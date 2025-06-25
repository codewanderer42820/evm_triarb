// router/update.go — ultra‑tight, zero‑alloc fan‑out of Sync logs to per‑core bucketqueues.
//
//   - Static arrays + pinned threads  →  no locks, no GC work.
//   - Two‑pair log₂ mapped to 4 096 buckets for O(1) queue ops.
//   - Open‑address 17‑bit table with fixed stride‑64 probing — collision‑free in practice.
package router

import (
	"math/bits"
	"runtime"
	"unsafe"

	"main/bucketqueue"
	"main/fastuni"
	"main/ring"
	"main/types"
	"main/utils"
)

////////////////////////////////////////////////////////////////////////////////
//                      Global, hot‑path immutable tables                    //
////////////////////////////////////////////////////////////////////////////////

var (
	cpuRingsGlobal [64]*ring.Ring  // per‑core SPSC rings (index == coreID)
	routingBitmap  [65536]uint16   // pairId → 16‑bit CPU mask
	addrToPairId   [1 << 17]uint16 // 131 072‑slot open‑address table
	routers        []*CoreRouter   // len == active cores
)

////////////////////////////////////////////////////////////////////////////////
//            High‑res log₂ → bucket mapping (±64 → 0…4095)                 //
////////////////////////////////////////////////////////////////////////////////

const (
	clampL2  = 64
	buckets  = 4096
	scaleMul = buckets - 1 // 4095
	scaleDiv = clampL2 * 2 // 128
	zeroOff  = buckets / 2 // 2048
)

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
//                              Core structs                                  //
////////////////////////////////////////////////////////////////////////////////

type DeltaBucket struct {
	CurLog float64            // 8 B
	Queue  *bucketqueue.Queue // 8 B
}

type ArbPath struct {
	// 3 × 8 B floats first for aligned loads in Sum()
	LegVal [3]float64
	PoolID [3]uint32 // each 4 B; packed after floats
	Dir    [3]bool   // 3 B
	_      byte      // pad to 8‑byte boundary
}

//go:nosplit
//go:inline
func (p *ArbPath) Sum() float64 { return p.LegVal[0] + p.LegVal[1] + p.LegVal[2] }

type fanRef struct {
	P         *ArbPath           // 8 B  (payload first – accessed every Update)
	Q         *bucketqueue.Queue // 8 B
	H         bucketqueue.Handle // 4 B (uint32)
	SharedLeg uint8              // 1 B
	_         [3]byte            // pad to 8‑byte boundary
}

type CoreRouter struct {
	// fields ordered by read frequency
	buckets     []*DeltaBucket // 24 B slice header
	fanOut      [][]fanRef     // 24 B slice header
	pairToLocal []uint32       // 24 B slice header
	isReverse   bool           // 1 B flag (read once per update)
	_           [7]byte        // pad → 8‑byte multiple
}

////////////////////////////////////////////////////////////////////////////////
//                        Update payload (stack‑only)                         //
////////////////////////////////////////////////////////////////////////////////

type PriceUpdate struct {
	PairId  uint16
	_       uint16 // align tick to 8‑byte boundary
	FwdTick float64
	RevTick float64
}

////////////////////////////////////////////////////////////////////////////////
//                 Bootstrap: pin one consumer goroutine per core            //
////////////////////////////////////////////////////////////////////////////////

func InitCPURings() {
	active := runtime.NumCPU() - 4 // keep 4 cores for OS / I/O
	routers = make([]*CoreRouter, active)

	for core := 0; core < active; core++ {
		go func(id int) {
			runtime.LockOSThread()

			rb := ring.New(1 << 14) // 16 384‑slot ring
			cpuRingsGlobal[id] = rb

			rt := &CoreRouter{
				buckets:     make([]*DeltaBucket, 0, 1<<17),
				fanOut:      make([][]fanRef, 0, 1<<17),
				pairToLocal: make([]uint32, 1<<17),
				isReverse:   id >= active/2,
			}
			routers[id] = rt

			ring.PinnedConsumer(id, rb, new(uint32), new(uint32), func(p unsafe.Pointer) {
				onPriceUpdate(rt, (*PriceUpdate)(p))
			}, make(chan struct{}))
		}(core)
	}
}

////////////////////////////////////////////////////////////////////////////////
//                          Per‑core hot‑path logic                           //
////////////////////////////////////////////////////////////////////////////////

//go:nosplit
func onPriceUpdate(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.isReverse {
		tick = upd.RevTick
	}

	idx := rt.pairToLocal[upd.PairId]
	bkt := rt.buckets[idx]
	bkt.CurLog = tick

	// Profit check (queue invariant: head never empty)
	_, _, ptr := bkt.Queue.PeepMin()
	if tick+(*ArbPath)(ptr).Sum() < 0 {
		logProfit((*ArbPath)(ptr), tick)
	}

	// Fan‑out: mutate shared leg & reprioritise
	for _, ref := range rt.fanOut[idx] {
		ref.P.LegVal[ref.SharedLeg] = tick
		ref.Q.Update(mapL2ToBucket(ref.P.Sum()), ref.H, unsafe.Pointer(ref.P))
	}
}

////////////////////////////////////////////////////////////////////////////////
//                      Ingress from JSON‑parser thread                       //
////////////////////////////////////////////////////////////////////////////////

// lookupPairID performs stride‑64 open addressing. If we make a full circle
// without finding a populated slot the process panics — collisions must have
// been resolved at bootstrap time; runtime misses indicate data corruption.
//
//go:nosplit
//go:nosplit
func lookupPairID(addr []byte) uint16 {
	start := utils.Hash17(addr)
	idx := start
	for {
		if id := addrToPairId[idx]; id != 0 {
			return id // found
		}
		idx = (idx + 64) & ((1 << 17) - 1) // fixed stride probe
		if idx == start {
			panic("addrToPairId: exhausted 17‑bit table — collision or unregistered pool")
		}
	}
}

func RouteUpdate(v *types.LogView) {
	addr := v.Addr[3:43] // skip '"0x'
	pair := lookupPairID(addr)

	r0 := utils.ParseHexU64(v.Data[:32])
	r1 := utils.ParseHexU64(v.Data[32:64])
	fwd := fastuni.Log2ReserveRatio(r0, r1)

	upd := PriceUpdate{PairId: pair, FwdTick: fwd, RevTick: -fwd}
	ptr := unsafe.Pointer(&upd)

	for m := routingBitmap[pair]; m != 0; {
		core := bits.TrailingZeros16(m) // 0‥15  → first 1-bit
		cpuRingsGlobal[core].Push(ptr)  // <── one push for that core
		m &^= 1 << core                 // clear that bit
	}
}

////////////////////////////////////////////////////////////////////////////////
//                                  Helpers                                   //
////////////////////////////////////////////////////////////////////////////////

// RegisterPair inserts a pool → pairId mapping; panics on bucket collision.
func RegisterPair(addr40 []byte, pairId uint16) {
	idx := utils.Hash17(addr40)
	start := idx
	for {
		if addrToPairId[idx] == 0 {
			addrToPairId[idx] = pairId
			return
		}
		idx = (idx + 64) & ((1 << 17) - 1)
		if idx == start {
			panic("addrToPairId: table full unexpectedly while registering pool")
		}
	}
}

func RegisterRoute(id uint16, mask uint16) { routingBitmap[id] = mask }

// stub: replace with actual metric / alerting pipeline
func logProfit(p *ArbPath, gain float64) { _ = p; _ = gain }
