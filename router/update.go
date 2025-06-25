// router/update.go — core update + ingestion logic for tick propagation and fanout
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

// ───── Global runtime state ─────
var (
	cpuRingsGlobal [64]*ring.Ring  // per-core SPSC ring buffers
	routingBitmap  [65536]uint16   // pairId → 16-bit CPU bitmask
	addrToPairId   [1 << 17]uint16 // 131072-entry open-address table
	routers        []*CoreRouter   // router state per pinned core
)

// ───── Tick-to-bucket mapper ─────
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

// ───── Data structs ─────
type DeltaBucket struct {
	CurLog float64
	Queue  *bucketqueue.Queue
}

type ArbPath struct {
	LegVal [3]float64
	PoolID [3]uint32
	Dir    [3]bool
	_      byte
}

func (p *ArbPath) Sum() float64 {
	return p.LegVal[0] + p.LegVal[1] + p.LegVal[2]
}

type fanRef struct {
	P         *ArbPath
	Q         *bucketqueue.Queue
	H         bucketqueue.Handle
	SharedLeg uint8
	_         [3]byte
}

type CoreRouter struct {
	Buckets     []*DeltaBucket
	FanOut      [][]fanRef
	PairToLocal []uint32
	isReverse   bool
	_           [7]byte
}

// ───── PriceUpdate payload ─────
type PriceUpdate struct {
	PairId  uint16
	_       uint16
	FwdTick float64
	RevTick float64
}

// ───── Core ingestion loop ─────
func InitCPURings() {
	active := runtime.NumCPU() - 4
	if active > 64 {
		active = 64
	}
	routers = make([]*CoreRouter, active)

	for core := 0; core < active; core++ {
		go func(id int) {
			runtime.LockOSThread()

			rb := ring.New(1 << 14)
			cpuRingsGlobal[id] = rb

			rt := &CoreRouter{
				Buckets:     make([]*DeltaBucket, 0, 1<<17),
				FanOut:      make([][]fanRef, 0, 1<<17),
				PairToLocal: make([]uint32, 1<<17),
				isReverse:   id >= active/2,
			}
			routers[id] = rt

			ring.PinnedConsumer(id, rb, new(uint32), new(uint32), func(p unsafe.Pointer) {
				onPriceUpdate(rt, (*PriceUpdate)(p))
			}, make(chan struct{}))
		}(core)
	}
}

func onPriceUpdate(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.isReverse {
		tick = upd.RevTick
	}

	idx := rt.PairToLocal[upd.PairId]
	bkt := rt.Buckets[idx]
	bkt.CurLog = tick

	_, _, ptr := bkt.Queue.PeepMin()
	if tick+(*ArbPath)(ptr).Sum() < 0 {
		logProfit((*ArbPath)(ptr), tick)
	}

	for _, ref := range rt.FanOut[idx] {
		ref.P.LegVal[ref.SharedLeg] = tick
		ref.Q.Update(mapL2ToBucket(ref.P.Sum()), ref.H, unsafe.Pointer(ref.P))
	}
}

// ───── Ingress from JSON log parser ─────
func RouteUpdate(v *types.LogView) {
	addr := v.Addr[3:43]
	pair := lookupPairID(addr)

	r0 := utils.ParseHexU64(v.Data[:32])
	r1 := utils.ParseHexU64(v.Data[32:64])
	fwd := fastuni.Log2ReserveRatio(r0, r1)

	upd := PriceUpdate{PairId: pair, FwdTick: fwd, RevTick: -fwd}
	ptr := unsafe.Pointer(&upd)

	for m := routingBitmap[pair]; m != 0; {
		core := bits.TrailingZeros16(m)
		cpuRingsGlobal[core].Push(ptr)
		m &^= 1 << core
	}
}

// ───── Address map setup ─────
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

func RegisterRoute(id uint16, mask uint16) {
	routingBitmap[id] = mask
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

func logProfit(p *ArbPath, gain float64) {
	_ = p
	_ = gain
	// stub
}
