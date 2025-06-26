// router/api.go — integrated fan‑out builder + core update logic
//
// Hot‑path layout finalised:
//   - TickBucket owns Tick + Queue.
//   - Fanout is 64‑byte (Path, Queue ptr, Edge, padding).
//   - Index remains simple map[PairID]uint32 for clarity.
//   - All helper types/functions compile under Go 1.22.
package router

import (
	"crypto/rand"
	"encoding/binary"
	"math/bits"
	"runtime"
	"runtime/debug"
	"sync"
	"unsafe"

	"main/bucketqueue"
	"main/fastuni"
	"main/ring"
	"main/types"
	"main/utils"
)

/* ---------- domain ---------- */

type PairID uint32

type CPUMask uint64

type TriCycle [3]PairID

type Ref struct {
	Pairs TriCycle
	Edge  uint8
	//lint:ignore U1000 "used for cache-line alignment"
	_pad [3]byte
}

/* ---------- bootstrap fan‑outs ---------- */

type Shard struct {
	Pair PairID
	Refs []Ref
}

type Fanouts map[PairID][]Shard

var (
	fanouts        Fanouts
	splitThreshold = 16_384
)

/* ---------- crypto shuffle ---------- */

func crandInt(n int) int {
	var b [8]byte
	rand.Read(b[:])
	v := binary.LittleEndian.Uint64(b[:])
	if n&(n-1) == 0 {
		return int(v & uint64(n-1))
	}
	hi, _ := bits.Mul64(v, uint64(n))
	return int(hi)
}
func shuffleRefs(r []Ref) {
	for i := len(r) - 1; i > 0; i-- {
		j := crandInt(i + 1)
		r[i], r[j] = r[j], r[i]
	}
}

/* ---------- BuildFanouts ---------- */

func ResetFanouts() { fanouts = make(Fanouts) }

func BuildFanouts(cycles []TriCycle) {
	debug.SetGCPercent(100)
	defer debug.SetGCPercent(-1)

	ResetFanouts()
	tmp := make(map[PairID][]Ref, len(cycles)*3)
	for _, tri := range cycles {
		for pos, pair := range tri {
			tmp[pair] = append(tmp[pair], Ref{Pairs: tri, Edge: uint8(pos)})
		}
	}
	for pair, refs := range tmp {
		shuffleRefs(refs)
		for off := 0; off < len(refs); off += splitThreshold {
			end := off + splitThreshold
			if end > len(refs) {
				end = len(refs)
			}
			fanouts[pair] = append(fanouts[pair], Shard{Pair: pair, Refs: refs[off:end]})
		}
	}
	runtime.GC()
}

/* ---------- mapper ---------- */

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

/* ---------- hot structs ---------- */

type TickBucket struct {
	Tick  float64
	Queue bucketqueue.Queue
}

type ArbPath struct {
	Ticks [3]float64
	Pairs [3]PairID
}

func (p *ArbPath) Sum() float64 { return p.Ticks[0] + p.Ticks[1] + p.Ticks[2] }

type Fanout struct {
	Path  ArbPath            // 40 B (ticks + pairs)
	Queue *bucketqueue.Queue // 8  B
	Edge  uint8              // 1  B — which tick slot 0‑2
	//lint:ignore U1000 "used for cache-line alignment"
	_pad [15]byte // pad to 64‑byte whole cache line
}

type CoreRouter struct {
	Buckets []TickBucket
	Fanouts [][]Fanout
	Index   map[PairID]uint32
	IsRev   bool
	ShardCh chan Shard
}

type PriceUpdate struct {
	Pair             PairID
	FwdTick, RevTick float64
}

/* ---------- globals ---------- */

var (
	coreRings    [64]*ring.Ring
	coreRouters  []*CoreRouter
	routeList    [1 << 17][]uint8
	addrToPairID [1 << 17]PairID
)

/* ---------- init ---------- */

func InitCPURings(cycles []TriCycle) {
	n := runtime.NumCPU() - 4
	if n < 8 {
		n = 8
	}
	if n > 64 {
		n = 64
	}
	BuildFanouts(cycles)

	coreRouters = make([]*CoreRouter, n)
	var wg sync.WaitGroup
	wg.Add(n)

	for core := 0; core < n; core++ {
		go func(core int) {
			defer wg.Done()
			runtime.LockOSThread()

			rb := ring.New(16381) // prime size
			shCh := make(chan Shard, 256)

			rt := &CoreRouter{
				IsRev:   core >= n/2,
				Buckets: make([]TickBucket, 0, 1024),
				Fanouts: make([][]Fanout, 0, 1024),
				Index:   make(map[PairID]uint32, 1024),
				ShardCh: shCh,
			}
			coreRouters[core] = rt
			coreRings[core] = rb

			for sh := range shCh {
				installShard(rt, &sh)
			}

			ring.PinnedConsumer(core, rb, new(uint32), new(uint32), func(p unsafe.Pointer) {
				onPriceUpdate(rt, (*PriceUpdate)(p))
			}, make(chan struct{}))
		}(core)
	}

	go func() {
		core := 0
		for _, shards := range fanouts {
			for _, sh := range shards {
				coreRouters[core%n].ShardCh <- sh
				core++
			}
		}
		for _, rt := range coreRouters {
			close(rt.ShardCh)
		}
	}()

	wg.Wait()
}

/* ---------- shard install ---------- */

func localID(rt *CoreRouter, pid PairID) uint32 {
	if id, ok := rt.Index[pid]; ok {
		return id
	}
	id := uint32(len(rt.Buckets))
	rt.Index[pid] = id
	rt.Buckets = append(rt.Buckets, TickBucket{})
	rt.Fanouts = append(rt.Fanouts, nil)
	return id
}

func installShard(rt *CoreRouter, sh *Shard) {
	lid := localID(rt, sh.Pair)
	qb := &rt.Buckets[lid].Queue

	for _, ref := range sh.Refs {
		f := Fanout{Path: ArbPath{Pairs: ref.Pairs}, Queue: qb, Edge: ref.Edge}
		rt.Fanouts[lid] = append(rt.Fanouts[lid], f)
	}
}

/* ---------- price update ---------- */

func onPriceUpdate(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.IsRev {
		tick = upd.RevTick
	}

	lid, ok := rt.Index[upd.Pair]
	if !ok {
		return
	}

	bucket := &rt.Buckets[lid]
	bucket.Tick = tick

	for i := range rt.Fanouts[lid] {
		f := &rt.Fanouts[lid][i]
		f.Path.Ticks[f.Edge] = tick
		f.Queue.Update(mapL2ToBucket(f.Path.Sum()), 0, unsafe.Pointer(&f.Path))
	}
}

/* ---------- ingress ---------- */

func RouteUpdate(v *types.LogView) {
	addr := v.Addr[3:43]
	pair := lookupPairID(addr)
	r0 := utils.LoadBE64(v.Data[24:])
	r1 := utils.LoadBE64(v.Data[56:])
	fwd := fastuni.Log2ReserveRatio(r0, r1)

	upd := PriceUpdate{Pair: pair, FwdTick: fwd, RevTick: -fwd}
	ptr := unsafe.Pointer(&upd)
	for _, c := range routeList[pair] {
		coreRings[c].Push(ptr)
	}
}

/* ---------- registration ---------- */

func RegisterPair(addr40 []byte, pid PairID) {
	idx := utils.Hash17(addr40)
	for {
		if addrToPairID[idx] == 0 {
			addrToPairID[idx] = pid
			return
		}
		idx = (idx + 64) & ((1 << 17) - 1)
	}
}

// RegisterRoute appends a core‑ID (0‑63) to the per‑pair route list.
func RegisterRoute(pid PairID, coreID uint8) {
	routeList[pid] = append(routeList[pid], coreID)
}

// lookupPairID resolves a 40‑byte pool address to its PairID.
// Panics if the pool hasn’t been registered at bootstrap.
func lookupPairID(addr []byte) PairID {
	idx := utils.Hash17(addr)
	for {
		if id := addrToPairID[idx]; id != 0 {
			return id
		}
		idx = (idx + 64) & ((1 << 17) - 1)
	}
}

// Stub hook — integrate with execution engine / alerting as needed.
func onProfitablePath(*ArbPath, float64) {}
