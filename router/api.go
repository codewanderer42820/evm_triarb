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

// InitCPURings sets up one forward-polarity core set and one mirrored
// reverse-polarity set.  Each shard is installed *twice*—first into the
// forward core, then into the matching reverse core (core+half).  All
// installation happens before any pinned-consumer goroutine starts, so the
// data path is race-free.

func InitCPURings(cycles []TriCycle) {
	// ── 1. Decide how many logical cores we’ll use ───────────────────────────
	n := runtime.NumCPU() - 4 // leave 4 for OS / networking
	if n < 8 {
		n = 8
	}
	if n > 64 {
		n = 64
	}
	if n&1 != 0 { // guarantee an even split
		n-- // e.g., 17 → 16
	}
	half := n / 2 // forward cores: [0 .. half-1]
	// reverse cores: [half .. n-1]

	// ── 2. Create an empty CoreRouter for every active core ─────────────────
	for i := 0; i < n; i++ {
		coreRouters[i] = &CoreRouter{
			Buckets:   make([]tickSoA, 0, 1024),
			Local:     localidx.New(1 << 16),
			IsReverse: i >= half, // second half = reverse polarity
		}
	}

	// ── 3. Build the global fan-out plan (no per-core mutation) ─────────────
	BuildFanouts(cycles)

	// ── 4. Install every shard twice: forward & mirrored reverse core ───────
	fwdCore := 0 // round-robin index into the forward set
	for _, shards := range rawShards {
		for _, sh := range shards {
			fwd := fwdCore % half // 0,1,2,…,half-1
			rev := fwd + half     // matching reverse core

			installShard(coreRouters[fwd], &sh)
			installShard(coreRouters[rev], &sh)

			fwdCore++
		}
	}

	// ── 5. Spin up pinned consumers after *all* shards are in place ─────────
	for i := 0; i < n; i++ {
		coreRings[i] = ring.New(1 << 14)
		rt := coreRouters[i]

		go func(coreID int, rt *CoreRouter) {
			ring.PinnedConsumer(
				coreID, coreRings[coreID],
				new(uint32), new(uint32),
				func(p unsafe.Pointer) { onPrice(rt, (*PriceUpdate)(p)) },
				make(chan struct{}),
			)
		}(i, rt)
	}
}

/*───────────────────────────────────
   Hot path
───────────────────────────────────*/

func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.IsReverse {
		tick = upd.RevTick
	}

	lid, ok := rt.Local.Get(uint32(upd.Pair))
	if !ok || lid >= uint32(len(rt.Fanouts)) {
		return
	}
	b := &rt.Buckets[lid]

	for _, f := range rt.Fanouts[lid] {
		idx := int(f.Idx)
		switch f.Edge {
		case 0:
			b.t0[idx] = tick
		case 1:
			b.t1[idx] = tick
		case 2:
			b.t2[idx] = tick
		}
		sum := b.t0[idx] + b.t1[idx] + b.t2[idx]
		f.Queue.Update(mapL2ToBucket(sum), 0, unsafe.Pointer(&f.Pairs))
	}
}

func mapL2ToBucket(x float64) int64 {
	const clamp = 64.0
	const buckets = 4096
	const half = buckets >> 1

	if x < -clamp {
		x = -clamp
	} else if x > clamp {
		x = clamp
	}
	return int64((x + clamp) * float64(half) / clamp)
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
