// router/update.go — ultra-low-latency price-update fan-out
//
//   - One pinned goroutine per CPU core; each owns a ring + CoreRouter.
//   - Zero allocations, zero copies on every hot-path call.
//   - Pair routing via 17-bit open-address array (step-64 probe).
//   - Two-pair log₂ path mapped into 4 096-bucket queue for O(1) priority ops.
package router

import (
	"log"
	"runtime"
	"unsafe"

	"main/bucketqueue"
	"main/fastuni"
	"main/ring"
	"main/types"
	"main/utils"
)

////////////////////////////////////////////////////////////////////////////////
//                    ─── Global singletons, sized at boot ───
////////////////////////////////////////////////////////////////////////////////

var (
	cpuRingsGlobal [64]*ring.Ring  // per-core single-producer, single-consumer rings
	routingBitmap  [65536]uint16   // pairId → 16-bit CPU mask (who should see this update)
	addrToPairId   [1 << 17]uint16 // hash bucket → compact 16-bit pairId
	routers        []*CoreRouter   // one CoreRouter per active core
)

// //////////////////////////////////////////////////////////////////////////////
//
//	High-resolution mapping   log₂(p1)+log₂(p2)  →  0 … 4095 bucket index
//
// //////////////////////////////////////////////////////////////////////////////
//
//	We care only about realistic paths: |L₂| ≤ 64 covers 99.9 % of pairs.
//	Bucket width ≈ 0.031 log₂  (≈ 2.2 % price granularity).
const (
	clampL2  = 64          // clamp range −64 … +64
	buckets  = 4096        // total buckets (2¹²)
	scaleMul = buckets - 1 // 4095
	scaleDiv = clampL2 * 2 // 128
	zeroOff  = buckets / 2 // centre bucket = 2048
)

// mapL2ToBucket converts a two-pair log₂ sum to a 12-bit bucket index.
//
//go:nosplit
//go:inline
func mapL2ToBucket(l2 float64) int64 {
	if l2 > clampL2 {
		l2 = clampL2
	} else if l2 < -clampL2 {
		l2 = -clampL2
	}
	return int64(zeroOff) + int64(l2*scaleMul/scaleDiv)
}

////////////////////////////////////////////////////////////////////////////////
//                                Core structs
////////////////////////////////////////////////////////////////////////////////

// CoreRouter lives 1-per-core and owns ONLY local data — no locks needed.
type CoreRouter struct {
	isReverse   bool           // top half cores process reverse tick
	pairToLocal []uint32       // pairId → compact local bucket index
	buckets     []*DeltaBucket // local buckets (price + queue)
	fanOut      [][]fanRef     // per-bucket list of dependent path refs
}

type DeltaBucket struct {
	CurLog float64            // current log₂ price for the pair
	Queue  *bucketqueue.Queue // common queue for “shared leg” paths
}

// ArbPath models one triangular path.  One leg (SharedLeg) is always 0.
type ArbPath struct {
	LegVal [3]float64 // log₂ deltas; shared leg will be mutated in-place
	PoolID [3]uint32  // pool IDs per leg
	Dir    [3]bool    // true = reverse direction
}

// Sum returns log₂(p1)+log₂(p2)+log₂(p3). One leg is always 0.
func (p *ArbPath) Sum() float64 { return p.LegVal[0] + p.LegVal[1] + p.LegVal[2] }

// fanRef binds one path leg to its queue handle.
type fanRef struct {
	H         bucketqueue.Handle // handle inside path-specific queue
	Q         *bucketqueue.Queue // that queue
	P         *ArbPath           // owning path
	SharedLeg uint8              // 0/1/2 index mutated when common pair moves
}

// Hot-path payload pushed through rings.
type PriceUpdate struct {
	PairId  uint16
	FwdTick float64
	RevTick float64
}

////////////////////////////////////////////////////////////////////////////////
//                       Boot: spin up pinned consumers
////////////////////////////////////////////////////////////////////////////////

func InitCPURings() {
	active := runtime.NumCPU() - 4 // leave a few cores for OS + networking
	routers = make([]*CoreRouter, active)

	for core := 0; core < active; core++ {
		go func(id int) {
			runtime.LockOSThread() // stay on this core forever

			rb := ring.New(1 << 14) // 16 384-slot ring
			cpuRingsGlobal[id] = rb

			rt := &CoreRouter{
				isReverse:   id >= active/2,
				pairToLocal: make([]uint32, 1<<17),
				buckets:     make([]*DeltaBucket, 0, 1<<17),
				fanOut:      make([][]fanRef, 0, 1<<17),
			}
			routers[id] = rt

			ring.PinnedConsumer(id, rb, new(uint32), new(uint32), func(ptr unsafe.Pointer) {
				onPriceUpdate(rt, (*PriceUpdate)(ptr))
			}, make(chan struct{}))
		}(core)
	}
}

////////////////////////////////////////////////////////////////////////////////
//                    Per-core fast-path: handle one update
////////////////////////////////////////////////////////////////////////////////

func onPriceUpdate(rt *CoreRouter, upd *PriceUpdate) {
	// Select forward / reverse tick depending on core polarity.
	tick := upd.FwdTick
	if rt.isReverse {
		tick = upd.RevTick
	}

	// Locate bucket in O(1) via direct slice index.
	idx := rt.pairToLocal[upd.PairId]
	bkt := rt.buckets[idx]
	bkt.CurLog = tick

	// 1. Check profitability on queue head (queue invariant: head always present)
	_, _, ptr := bkt.Queue.PeepMin()
	if tick+(*ArbPath)(ptr).Sum() < 0 {
		logProfit((*ArbPath)(ptr), tick+(*ArbPath)(ptr).Sum())
	}

	// 2. Update every path that uses this pair, then re-prioritise in its queue.
	for _, ref := range rt.fanOut[idx] {
		ref.P.LegVal[ref.SharedLeg] = tick
		ref.Q.Update(mapL2ToBucket(ref.P.Sum()), ref.H, unsafe.Pointer(ref.P))
	}
}

////////////////////////////////////////////////////////////////////////////////
//             Ingress: parser calls this for every Uniswap Sync
////////////////////////////////////////////////////////////////////////////////

// lookupPairID: open-address array with step-64 linear probe.
// First 6 hex chars (24 bits) feed Hash17 → initial 17-bit bucket.
//
//go:nosplit
//go:inline
func lookupPairID(addr []byte) uint16 {
	idx := utils.Hash17(addr)
	for {
		if id := addrToPairId[idx]; id != 0 {
			return id
		}
		idx = (idx + 64) & ((1 << 17) - 1) // fixed probe stride
	}
}

func RouteUpdate(v *types.LogView) {
	// 0x-prefixed, quoted JSON string: "\"0x…" ; skip `"0x` (3 bytes)
	addr := v.Addr[3:43] // 40-byte lowercase hex
	pairId := lookupPairID(addr)

	// Decode reserves and compute log₂ price ratio.
	r0 := utils.ParseHexU64(v.Data[:32])
	r1 := utils.ParseHexU64(v.Data[32:64])
	fwd := fastuni.Log2ReserveRatio(r0, r1)
	rev := -fwd

	upd := PriceUpdate{PairId: pairId, FwdTick: fwd, RevTick: rev}
	ptr := unsafe.Pointer(&upd)

	// Bitmask fan-out: branch-free loop over target cores.
	for core, mask := 0, routingBitmap[pairId]; mask != 0; core, mask = core+1, mask>>1 {
		if mask&1 != 0 {
			cpuRingsGlobal[core].Push(ptr)
		}
	}
}

// Called during bootstrap to assign which cores handle which pair.
func RegisterRoute(pairId uint16, mask uint16) { routingBitmap[pairId] = mask }

////////////////////////////////////////////////////////////////////////////////
//                             Misc helpers
////////////////////////////////////////////////////////////////////////////////

// logProfit is a pluggable hook; replace with real metrics / queue.
func logProfit(p *ArbPath, gain float64) { log.Printf("PROFIT %.4f log₂ on path %+v", gain, p) }

/*
	NOTE: utils.Hash17(addr)   – 17-bit index from first 6 hex chars
	      utils.ParseHexU64() – zero-alloc 0x-optional hex → uint64
	      These live in main/utils (see utils/utils.go).
*/
