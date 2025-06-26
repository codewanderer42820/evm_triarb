// router/api.go — fast fan-out router (SoA buckets + 64-byte Fanout)

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
	"main/pairidx"
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
	_pad  [3]byte
}

/* ---------- build-time fan-outs ---------- */

type Shard struct {
	Pair PairID
	Refs []Ref
}
type Fanouts map[PairID][]Shard

var (
	fanouts        Fanouts
	splitThreshold = 16_384
)

func ResetFanouts() { fanouts = make(Fanouts) }

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

/* ---------- buckets & fanouts ---------- */

type TickBucket = tickSoA // alias to SoA implementation (ticksoa.go)

type Fanout struct { // see fanout.go
	Pairs [3]PairID
	_pad0 [28]byte
	Queue *bucketqueue.Queue
	Edge  uint16
	Idx   uint16
	_pad1 [6]byte
}

/* ---------- core router ---------- */

type CoreRouter struct {
	Buckets []TickBucket // localID → bucket
	Fanouts []Fanout     // flat slice; Idx into bucket.t*
	Idx     pairidx.Hash // PairID → localID
	IsRev   bool
	ShardCh chan Shard
}

type PriceUpdate struct {
	Pair             PairID
	FwdTick, RevTick float64
}

/* ---------- global arrays ---------- */

var (
	coreRings    [64]*ring.Ring
	coreRouters  []*CoreRouter
	routingMask  [1 << 17]CPUMask
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

			rb := ring.New(15873)
			shCh := make(chan Shard, 256)
			rt := &CoreRouter{
				IsRev:   core >= n/2,
				Buckets: make([]TickBucket, 0, 1024),
				Fanouts: make([]Fanout, 0, 1<<17),
				Idx:     pairidx.New(1 << 16),
				ShardCh: shCh,
			}
			coreRouters[core] = rt
			coreRings[core] = rb

			for sh := range shCh {
				installShard(rt, &sh)
			}

			ring.PinnedConsumer(core, rb, new(uint32), new(uint32),
				func(p unsafe.Pointer) { onPrice(rt, (*PriceUpdate)(p)) },
				make(chan struct{}),
			)
		}(core)
	}

	go func() { // stripe shards
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

/* ---------- install shard ---------- */

func localID(rt *CoreRouter, pid PairID) uint32 {
	if id, ok := rt.Idx.Get(uint32(pid)); ok {
		return id
	}
	id, _ := rt.Idx.Put(uint32(pid), uint32(len(rt.Buckets)))
	rt.Buckets = append(rt.Buckets, TickBucket{})
	return id
}

func installShard(rt *CoreRouter, sh *Shard) {
	lid := localID(rt, sh.Pair)
	b := &rt.Buckets[lid]
	b.ensureCap(len(sh.Refs)) // ensure SoA slices fit

	for i, ref := range sh.Refs {
		rt.Fanouts = append(rt.Fanouts, Fanout{
			Pairs: ref.Pairs,
			Queue: &b.Queue,
			Edge:  uint16(ref.Edge),
			Idx:   uint16(i), // position inside slice
		})
	}
}

/* ---------- price update ---------- */

func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	t := upd.FwdTick
	if rt.IsRev {
		t = upd.RevTick
	}

	lid, ok := rt.Idx.Get(uint32(upd.Pair))
	if !ok {
		return
	} // pair not on this core

	b := &rt.Buckets[lid]
	b.Tick = t

	for i := range rt.Fanouts {
		f := &rt.Fanouts[i]
		if rt.Idx.Get(uint32(f.Pairs[0])) != lid &&
			rt.Idx.Get(uint32(f.Pairs[1])) != lid &&
			rt.Idx.Get(uint32(f.Pairs[2])) != lid {
			continue
		}
		switch f.Edge {
		case 0:
			b.t0[f.Idx] = t
		case 1:
			b.t1[f.Idx] = t
		default:
			b.t2[f.Idx] = t
		}
		sum := b.t0[f.Idx] + b.t1[f.Idx] + b.t2[f.Idx]
		f.Queue.Update(mapL2ToBucket(sum), 0, unsafe.Pointer(&f.Pairs))
	}
}

/* ---------- log ingress ---------- */

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

/* ---------- registration helpers ---------- */

var routeList [1 << 17][]uint8

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
func RegisterRoute(pid PairID, cpu uint8) { routeList[pid] = append(routeList[pid], cpu) }

func lookupPairID(addr []byte) PairID {
	idx := utils.Hash17(addr)
	for {
		if id := addrToPairID[idx]; id != 0 {
			return id
		}
		idx = (idx + 64) & ((1 << 17) - 1)
	}
}

func onProfitablePath(_ *ArbPath, _ float64) {} // stub
