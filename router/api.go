// api.go — 64-core bitmap router with slice-based Fanouts.
package router

import (
	"crypto/rand"
	"encoding/binary"
	"math/bits"
	"runtime"
	"unsafe"

	"main/bucketqueue"
	"main/fastuni"
	"main/localidx"
	"main/ring"
	"main/types"
	"main/utils"
)

/*───────────────────────────────────
   Types
───────────────────────────────────*/

type PairID uint32
type LocalPairID uint32
type CPUMask uint64
type TriCycle [3]PairID

type ArbPath struct {
	Ticks [3]float64
	Pairs TriCycle
}

type Ref struct {
	Pairs TriCycle
	Edge  uint16
	_pad  [2]byte
}

type Shard struct {
	Pair PairID
	Refs []Ref
}

type PriceUpdate struct {
	Pair             PairID
	FwdTick, RevTick float64
}

type CoreRouter struct {
	Buckets   []bucketqueue.Queue // per-pair tick + queue storage
	Fanouts   [][]Fanout          // index == LocalPairID (dense)
	Local     localidx.Hash       // global → local ID mapping
	IsReverse bool                // forward (false) or reverse (true) core
}

/*───────────────────────────────────
   Globals
───────────────────────────────────*/

var (
	coreRouters   [64]*CoreRouter // index == core ID
	coreRings     [64]*ring.Ring
	addrToPairID  [1 << 17]PairID  // open-addressed hash table
	routingBitmap [1 << 17]CPUMask // per-pair 64-bit CPU mask

	rawShards      map[PairID][]Shard
	splitThreshold = 16_384
)

/*───────────────────────────────────
   Fan-out planning helpers
───────────────────────────────────*/

func ResetFanouts() { rawShards = make(map[PairID][]Shard) }

func shuffleRefs(refs []Ref) {
	for i := len(refs) - 1; i > 0; i-- {
		j := crandInt(i + 1)
		refs[i], refs[j] = refs[j], refs[i]
	}
}

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

func BuildFanouts(cycles []TriCycle) {
	ResetFanouts()
	tmp := make(map[PairID][]Ref, len(cycles)*3)

	for _, tri := range cycles {
		for pos, pair := range tri {
			tmp[pair] = append(tmp[pair], Ref{Pairs: tri, Edge: uint16(pos)})
		}
	}

	for pair, refs := range tmp {
		shuffleRefs(refs)
		for off := 0; off < len(refs); off += splitThreshold {
			end := off + splitThreshold
			if end > len(refs) {
				end = len(refs)
			}
			rawShards[pair] = append(rawShards[pair], Shard{Pair: pair, Refs: refs[off:end]})
		}
	}
}

/*───────────────────────────────────
   Bootstrap
───────────────────────────────────*/

// InitCPURings creates two mirrored core sets.  Each shard is delivered by
// value to both the forward core and the matching reverse core.  Inside the
// per-core goroutine we deep-copy the Refs slice so *all* data is local to
// that core before installShard runs.

func InitCPURings(cycles []TriCycle) {
	/* 1. decide active cores (even) */
	n := runtime.NumCPU() - 4
	if n < 8 {
		n = 8
	}
	if n > 64 {
		n = 64
	}
	if n&1 != 0 {
		n--
	}
	half := n / 2

	/* 2. build global fan-out plan (fills rawShards) */
	BuildFanouts(cycles)

	/* 3. one shard channel per core, element type Shard (BY VALUE) */
	shardCh := make([]chan Shard, n)
	for i := range shardCh {
		shardCh[i] = make(chan Shard, 128)
	}

	/* 4. per-core goroutine: pin → local alloc → receive shards */
	for coreID := 0; coreID < n; coreID++ {
		go func(coreID, half int, in <-chan Shard) {
			runtime.LockOSThread() // pin to this CPU

			// ── local allocations (all faulted in by this core) ──
			rt := &CoreRouter{
				Buckets:   make([]bucketqueue.Queue, 0, 1024),
				Local:     localidx.New(1 << 16),
				IsReverse: coreID >= half,
			}
			coreRouters[coreID] = rt

			rb := ring.New(1 << 14)
			coreRings[coreID] = rb

			// 👉 THIS is where the slice goes
			paths := make([]ArbPath, 0, 1024) // owns every ArbPath on this core

			// ── install every shard sent to this core ──
			for sh := range in {
				localRefs := append([]Ref(nil), sh.Refs...) // deep copy
				local := Shard{Pair: sh.Pair, Refs: localRefs}
				installShard(rt, &local, &paths) // pass &paths
			}

			// ── all shards done: start the hot consumer ──
			ring.PinnedConsumer(
				coreID, rb,
				new(uint32), new(uint32),
				func(p unsafe.Pointer) { onPrice(rt, (*PriceUpdate)(p)) },
				make(chan struct{}),
			)
		}(coreID, half, shardCh[coreID])
	}

	/* 5. distribute each shard twice: forward core f, reverse core f+half */
	coreIdx := 0
	for _, shards := range rawShards {
		for _, s := range shards {
			fwd := coreIdx % half
			rev := fwd + half // mirrored partner

			shardCh[fwd] <- s // send BY VALUE
			shardCh[rev] <- s

			coreIdx++
		}
	}

	/* 6. close all channels so goroutines enter consumer loop */
	for _, ch := range shardCh {
		close(ch)
	}
}

/*───────────────────────────────────
   Shard installation
───────────────────────────────────*/

// installShard attaches every Ref as a Fanout and creates one ArbPath per Ref.
func installShard(rt *CoreRouter, sh *Shard, paths *[]ArbPath) {
	lid := rt.Local.Put(uint32(sh.Pair), uint32(len(rt.Buckets)))

	// bucket once per pair
	if int(lid) == len(rt.Buckets) {
		rt.Buckets = append(rt.Buckets, bucketqueue.Queue{})
		rt.Buckets = append(rt.Buckets, *bucketqueue.New())
	}
	// outer slice long enough
	if int(lid) >= len(rt.Fanouts) {
		rt.Fanouts = append(rt.Fanouts,
			make([][]Fanout, int(lid)-len(rt.Fanouts)+1)...)
	}

	for _, ref := range sh.Refs {
		*paths = append(*paths, ArbPath{Pairs: ref.Pairs})
		pPtr := &(*paths)[len(*paths)-1]

		rt.Fanouts[lid] = append(rt.Fanouts[lid], Fanout{
			Path:  pPtr,
			Queue: &rt.Buckets[lid],
			Edge:  ref.Edge,
		})
	}
}

/*───────────────────────────────────
   Registration helpers
───────────────────────────────────*/

func RegisterPair(addr40 []byte, pid PairID) {
	idx := utils.Hash17(addr40)
	for {
		if addrToPairID[idx] == 0 {
			addrToPairID[idx] = pid
			return
		}
		idx = (idx + 64) & (1<<17 - 1)
	}
}

func RegisterRoute(pid PairID, core uint8) {
	if core >= 64 {
		panic("core id out of range")
	}
	routingBitmap[pid] |= 1 << core
}

func lookupPairID(addr []byte) PairID {
	idx := utils.Hash17(addr)
	for {
		if id := addrToPairID[idx]; id != 0 {
			return id
		}
		idx = (idx + 64) & (1<<17 - 1)
	}
}

/*───────────────────────────────────
   Public entry point
───────────────────────────────────*/

func RouteUpdate(v *types.LogView) {
	addr := v.Addr[3:43] // skip "0x"
	pair := lookupPairID(addr)

	r0 := utils.LoadBE64(v.Data[24:])
	r1 := utils.LoadBE64(v.Data[56:])
	tick := fastuni.Log2ReserveRatio(r0, r1)

	upd := PriceUpdate{Pair: pair, FwdTick: tick, RevTick: -tick}
	ptr := unsafe.Pointer(&upd)

	for m := routingBitmap[pair]; m != 0; {
		core := bits.TrailingZeros64(uint64(m))
		coreRings[core].Push(ptr)
		m &^= 1 << core
	}
}

/*───────────────────────────────────
   Placeholder for execution layer
───────────────────────────────────*/

func onProfitablePath(_ *ArbPath, _ float64) {}
