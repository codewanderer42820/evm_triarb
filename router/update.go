// router/update.go — core update + ingestion logic for tick propagation and fanout
package router

import (
	"math/bits"
	"runtime"
	"sync"
	"unsafe"

	"main/bucketqueue"
	"main/fastuni"
	"main/ring"
	"main/types"
	"main/utils"
)

// ─── Global runtime state ───
var (
	coreRings     [64]*ring.Ring  // per-core SPSC ring buffers
	routingBitmap [65536]uint64   // pairId → 64-bit CPU bitmask
	addrToPairId  [1 << 17]uint16 // 131072-entry open-address table
	coreRouters   []*CoreRouter   // router state per pinned core
)

// ─── Tick-to-bucket mapper ───
const (
	clampL2  = 64
	buckets  = 4096
	scaleMul = buckets - 1
	scaleDiv = clampL2 * 2
	zeroOff  = buckets / 2
)

func mapL2ToBucket(l2 float64) int64 {
	if l2 > clampL2 {
		l2 = clampL2
	} else if l2 < -clampL2 {
		l2 = -clampL2
	}
	return int64(zeroOff) + int64(l2*scaleMul/scaleDiv)
}

// ─── Data structs ───
type DeltaBucket struct {
	CurLog float64
	Queue  *bucketqueue.Queue
}

type ArbPath struct {
	Ticks   [3]float64
	PoolID  [3]uint32
	Reverse [3]bool
	_       byte
}

func (p *ArbPath) Sum() float64 {
	return p.Ticks[0] + p.Ticks[1] + p.Ticks[2]
}

type fanRef struct {
	P         *ArbPath
	Q         *bucketqueue.Queue
	H         bucketqueue.Handle
	SharedLeg uint8
	_         [3]byte
}

type CoreRouter struct {
	Routes    []*DeltaBucket
	Fanouts   [][]fanRef
	PairIndex []uint32
	IsReverse bool
	_         [7]byte
}

// ─── PriceUpdate payload ───
type PriceUpdate struct {
	PairId  uint16
	_       uint16
	FwdTick float64
	RevTick float64
}

// ─── Core ingestion loop ───
// InitCPURings spawns one pinned consumer goroutine per core, allocating all state on that OS thread.
func InitCPURings() {
	// clamp to [8,64]
	n := runtime.NumCPU() - 4
	if n < 8 {
		n = 8
	}
	if n > 64 {
		n = 64
	}

	// allocate slice for router metadata
	coreRouters = make([]*CoreRouter, n)

	var wg sync.WaitGroup
	wg.Add(n)

	for core := 0; core < n; core++ {
		go func(core int) {
			defer wg.Done()
			runtime.LockOSThread()

			// allocate the ring buffer first for core-local placement
			rb := ring.New(1 << 14)
			coreRings[core] = rb

			// allocate router state now
			rt := &CoreRouter{IsReverse: core >= n/2}
			rt.Fanouts = make([][]fanRef, 0, 1<<17)
			rt.Routes = make([]*DeltaBucket, 0, 1<<17)
			rt.PairIndex = make([]uint32, 1<<17)
			coreRouters[core] = rt

			// start the pinned consumer loop
			ring.PinnedConsumer(core, rb, new(uint32), new(uint32), func(p unsafe.Pointer) {
				onPriceUpdate(rt, (*PriceUpdate)(p))
			}, make(chan struct{}))
		}(core)
	}

	// wait for all goroutines to have done their allocations
	wg.Wait()
}

func onPriceUpdate(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.IsReverse {
		tick = upd.RevTick
	}

	idx := rt.PairIndex[upd.PairId]
	bkt := rt.Routes[idx]
	bkt.CurLog = tick

	_, _, ptr := bkt.Queue.PeepMin()
	if tick+(*ArbPath)(ptr).Sum() < 0 {
		onProfitablePath((*ArbPath)(ptr), tick)
	}

	for _, ref := range rt.Fanouts[idx] {
		ref.P.Ticks[ref.SharedLeg] = tick
		ref.Q.Update(mapL2ToBucket(ref.P.Sum()), ref.H, unsafe.Pointer(ref.P))
	}
}

// ─── Ingress from JSON log parser ───
func RouteUpdate(v *types.LogView) {
	addr := v.Addr[3:43]
	pair := lookupPairID(addr)

	r0 := utils.LoadBE64(v.Data[24:])
	r1 := utils.LoadBE64(v.Data[56:])
	fwd := fastuni.Log2ReserveRatio(r0, r1)

	upd := PriceUpdate{PairId: pair, FwdTick: fwd, RevTick: -fwd}
	ptr := unsafe.Pointer(&upd)

	for m := routingBitmap[pair]; m != 0; {
		core := bits.TrailingZeros64(m)
		coreRings[core].Push(ptr)
		m &^= 1 << core
	}
}

// ─── Address map setup ───
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
			panic("addrToPairId: table full while registering pool")
		}
	}
}

// RegisterRoute ORs a full 64-bit mask into routingBitmap.
func RegisterRoute(pairID uint16, mask uint64) {
	routingBitmap[pairID] |= mask
}

func lookupPairID(addr []byte) uint16 {
	start := utils.Hash17(addr)
	idx := start
	for {
		if id := addrToPairId[idx]; id != 0 {
			return id
		}
		idx = (idx + 64) & ((1 << 17) - 1)
		if idx == start {
			panic("addrToPairId: exhausted table — unregistered pool")
		}
	}
}

// onProfitablePath is a placeholder for arbitrage profit handling.
func onProfitablePath(p *ArbPath, tick float64) {
	// no-op
}
