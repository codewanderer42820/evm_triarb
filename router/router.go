// router/update.go — zero‑alloc, zero‑copy fan‑out of price updates to CPU‑pinned bucketqueues.
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

// -----------------------------------------------------------------------------
// Global dispatch tables
// -----------------------------------------------------------------------------

var cpuRingsGlobal [64]*ring.Ring // per‑core SPSC rings
var routingBitmap [65536]uint16   // pairId → 16‑bit CPU mask
var addrToPairId [1 << 17]uint16  // ASCII addr → compact pairId
var routers []*CoreRouter         // one router per active core

// -----------------------------------------------------------------------------
// Two‑pair log₂ mapping → 4 096‑bucket queue (high‑resolution centre band)
// -----------------------------------------------------------------------------

const (
	clampL2  = 64          // active half‑range (−64 … +64)
	buckets  = 4096        // total buckets (2¹²)
	scaleMul = buckets - 1 // 4095 numerator
	scaleDiv = clampL2 * 2 // 128 denominator
	zeroOff  = buckets / 2 // 2048 centre bucket
)

func mapL2ToBucket(l2 float64) int64 {
	if l2 > clampL2 {
		l2 = clampL2
	} else if l2 < -clampL2 {
		l2 = -clampL2
	}
	return int64(zeroOff) + int64(l2*scaleMul/scaleDiv)
}

// -----------------------------------------------------------------------------
// Core data structures
// -----------------------------------------------------------------------------

type CoreRouter struct {
	isReverse   bool
	pairToLocal []uint32       // pairId → compact index (flat slice)
	buckets     []*DeltaBucket // local index → bucket
	fanOut      [][]fanRef     // local index → refs that depend on bucket
}

type DeltaBucket struct {
	CurLog float64
	Queue  *bucketqueue.Queue
}

type ArbPath struct {
	LegVal [3]float64 // log₂ deltas (one will stay 0 for common leg)
	PoolID [3]uint32  // pool IDs per leg
	Dir    [3]bool    // direction flags per leg
}

func (p *ArbPath) Sum() float64 { return p.LegVal[0] + p.LegVal[1] + p.LegVal[2] }

type fanRef struct {
	H         bucketqueue.Handle // queue handle
	Q         *bucketqueue.Queue // path‑specific queue
	P         *ArbPath           // owning path metadata
	SharedLeg uint8              // which leg index (0/1/2) corresponds to shared pair
}

// -----------------------------------------------------------------------------
// PriceUpdate payload (stack‑allocated on RouteUpdate fast path)
// -----------------------------------------------------------------------------

type PriceUpdate struct {
	PairId  uint16
	FwdTick float64
	RevTick float64
}

// -----------------------------------------------------------------------------
// Bootstrapping: one pinned consumer per core
// -----------------------------------------------------------------------------

func InitCPURings() {
	active := runtime.NumCPU() - 4 // reserve 4 cores for OS / networking
	routers = make([]*CoreRouter, active)

	for coreID := 0; coreID < active; coreID++ {
		go func(id int) {
			runtime.LockOSThread()

			rb := ring.New(1 << 14) // 16 384 slots
			cpuRingsGlobal[id] = rb

			r := &CoreRouter{
				isReverse:   id >= active/2,
				pairToLocal: make([]uint32, 1<<17),
				buckets:     make([]*DeltaBucket, 0, 1<<17),
				fanOut:      make([][]fanRef, 0, 1<<17),
			}
			routers[id] = r

			ring.PinnedConsumer(id, rb, new(uint32), new(uint32), func(ptr unsafe.Pointer) {
				onPriceUpdate(r, (*PriceUpdate)(ptr))
			}, make(chan struct{}))
		}(coreID)
	}
}

// -----------------------------------------------------------------------------
// Fast‑path consumer logic (runs on pinned core)
// -----------------------------------------------------------------------------

func onPriceUpdate(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.isReverse {
		tick = upd.RevTick
	}

	idx := rt.pairToLocal[upd.PairId]
	bucket := rt.buckets[idx]
	bucket.CurLog = tick

	// Profitability check on common queue head (no guard; queue always populated)
	_, _, ptr := bucket.Queue.PeepMin()
	path := (*ArbPath)(ptr)
	if tick+path.Sum() < 0 {
		logProfit(path, tick+path.Sum())
	}

	// Update all dependent paths in‑place and re‑prioritise
	for _, ref := range rt.fanOut[idx] {
		ref.P.LegVal[ref.SharedLeg] = tick // mutate only the shared leg
		_ = ref.Q.Update(mapL2ToBucket(ref.P.Sum()), ref.H, unsafe.Pointer(ref.P))
	}
}

// -----------------------------------------------------------------------------
// Ingress: called by parser for each Sync log
// -----------------------------------------------------------------------------

func RouteUpdate(v *types.LogView) {
	// v.Addr: "0x" prefix + 40‑byte lowercase hex address (total 42 ASCII chars)
	key := v.Addr[3:43] // strip '"0x'
	pairId := addrToPairId[utils.HashAddress(key)]

	r0 := utils.ParseHexU64(v.Data[:32])
	r1 := utils.ParseHexU64(v.Data[32:64])
	fwd := fastuni.Log2ReserveRatio(r0, r1)
	rev := -fwd

	upd := PriceUpdate{PairId: pairId, FwdTick: fwd, RevTick: rev}
	ptr := unsafe.Pointer(&upd)

	mask := routingBitmap[pairId]
	for core := 0; mask != 0; core++ {
		if mask&1 != 0 {
			cpuRingsGlobal[core].Push(ptr)
		}
		mask >>= 1
	}
}

func RegisterRoute(pairId uint16, mask uint16) { routingBitmap[pairId] = mask }

// -----------------------------------------------------------------------------
// Stubs / helpers (must be provided elsewhere in project)
// -----------------------------------------------------------------------------

func logProfit(p *ArbPath, gain float64) { log.Printf("PROFIT %.4f %% path %+v", gain, p) }
