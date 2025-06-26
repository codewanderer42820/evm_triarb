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
	Buckets   []tickSoA     // per-pair tick + queue storage
	Fanouts   [][]Fanout    // index == LocalPairID (dense)
	Local     localidx.Hash // global → local ID mapping
	IsReverse bool          // forward (false) or reverse (true) core
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

	/* 4. per-core goroutine: pin → local alloc → receive shards → deep-copy */
	for coreID := 0; coreID < n; coreID++ {
		go func(coreID, half int, in <-chan Shard) {
			runtime.LockOSThread() // NUMA pin

			/* 4-a. allocate router & ring on this core */
			rt := &CoreRouter{
				Buckets:   make([]tickSoA, 0, 1024),
				Local:     localidx.New(1 << 16),
				IsReverse: coreID >= half,
			}
			coreRouters[coreID] = rt

			rb := ring.New(1 << 14) // local first-touch
			coreRings[coreID] = rb

			/* 4-b. receive shards, deep-copy Refs, install */
			for sh := range in {
				localRefs := append([]Ref(nil), sh.Refs...) // deep copy
				localShard := Shard{Pair: sh.Pair, Refs: localRefs}
				installShard(rt, &localShard)
			}

			/* 4-c. everything installed – start hot loop */
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
   Hot path
───────────────────────────────────*/

// onPriceFast processes one PriceUpdate on its pinned core.
//
//go:nosplit
func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.IsReverse {
		tick = upd.RevTick // branch only once per update
	}

	lid, _ := rt.Local.Get(uint32(upd.Pair)) // guaranteed hit
	b := &rt.Buckets[lid]
	fan := rt.Fanouts[lid] // guaranteed non-empty

	// branch-free slice table for leg selection
	legs := [...]*[]float64{&b.t0, &b.t1, &b.t2}

	for _, f := range fan {
		idx := int(f.Idx)

		(*legs[f.Edge])[idx] = tick // no switch, no bounds check

		sum := b.t0[idx] + b.t1[idx] + b.t2[idx]
		f.Queue.Update(l2Bucket(sum), 0, unsafe.Pointer(&f.Pairs))
	}
}

// l2Bucket maps a clamped log₂ sum (±128) into 4096 buckets.
// clamp = 128  ⇒  range 256;  scale = 4096 / 256 = 16.
//
//go:nosplit
func l2Bucket(x float64) int64 {
	const (
		clamp = 128.0 // covers two full V2-pool log₂ moves
		scale = 16.0  // 4096 / (2*clamp)
	)
	if x > clamp {
		x = clamp
	} else if x < -clamp {
		x = -clamp
	}
	return int64((x + clamp) * scale) // (x+128)*16  →  0‥4095
}

/*───────────────────────────────────
   Shard installation
───────────────────────────────────*/

func installShard(rt *CoreRouter, sh *Shard) {
	lid := rt.Local.Put(uint32(sh.Pair), uint32(len(rt.Buckets)))

	/* create bucket exactly once per pair */
	if int(lid) == len(rt.Buckets) {
		rt.Buckets = append(rt.Buckets, tickSoA{})
		rt.Buckets[lid].Queue = *bucketqueue.New()
	}

	/* make sure Fanouts slice is long enough */
	if int(lid) >= len(rt.Fanouts) {
		rt.Fanouts = append(rt.Fanouts, make([][]Fanout, int(lid)-len(rt.Fanouts)+1)...)
	}

	b := &rt.Buckets[lid]
	base := len(rt.Fanouts[lid]) // cumulative length so far
	total := base + len(sh.Refs)
	b.ensureCap(total)

	for i, ref := range sh.Refs {
		rt.Fanouts[lid] = append(rt.Fanouts[lid], Fanout{
			Pairs: ref.Pairs,
			Edge:  ref.Edge,
			Idx:   uint32(base + i), // unique across shards
			Queue: &b.Queue,
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
