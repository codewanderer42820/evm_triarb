// router.go — 64-core fan-out router for real-time triangular-arbitrage tick propagation.
// Re-named, re-ordered & fully banner-commented ― 2025-06-27.
//
// ──────────────────────────────────────────────────────────────────────────────
//  Key structural changes
//  ─────────────────────────────────────────────────────────────────────────────
//  • TriPath        → PairTriplet      (semantic: three pair-IDs per cycle)
//  • PathState      → CycleState       (mutable tick state for one cycle)
//  • Ref            → EdgeBinding      (binds “which pair” inside a triplet)
//  • Shard          → PairShard        (batch of EdgeBinding for one pair)
//  • Fanout         → FanoutEntry      (per-core subscription to a CycleState)
//  • PriceUpdate    → TickUpdate       (32-byte ring message)
//  • CoreRouter     → CoreExecutor     (per-core state)
//  • RouteUpdate    → DispatchUpdate   (fast path from log view → ring)
//
//  Field re-ordering rules
//  ──────────────────────
//  1. 64-bit values first → minimise padding on arm64.
//  2. Slice / ptr headers are 24 B each; group them together.
//  3. Hot-path bools & uint16s last so they share trailing pad.
//
//  Resulting struct sizes:
//
//    CycleState   = 40 B   (24 tick + 12 pair + 4 pad → 40)
//    FanoutEntry  = 24 B   (p Path + p Queue + u32 Handle + u16 Edge + 2 pad)
//    TickUpdate   = 32 B   (ring message, compile-time check)
//
// ──────────────────────────────────────────────────────────────────────────────

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
	"main/ring32"
	"main/types"
	"main/utils"
)

/*─────────────────────────────  Constants  ─────────────────────────────*/

const (
	addrHexStart = 3  // LogView.Addr[3:43] → 40-byte ASCII hex
	addrHexEnd   = 43 // exclusive
)

/*────────────────────────────  Type Aliases  ───────────────────────────*/

type (
	PairID   uint32 // globally-unique pair identifier
	CoreMask uint64 // 64-bit CPU-bitmap for fan-out

	// PairTriplet holds the three PairID values that form one triangular cycle.
	PairTriplet [3]PairID
)

/*───────────────────────────  Core Data  ───────────────────────────────*/

// CycleState is the mutable payload stored in each bucket-queue entry.
// Tick values are placed first (hot - updated every price event).
type CycleState struct {
	Ticks [3]float64 // hot-path mutable tick cache
	Pairs PairTriplet
	_pad  uint32 // 8-byte alignment; keeps struct at 40 B
}

// EdgeBinding is a build-time helper that associates “which edge?” (0,1,2)
// with a particular pair inside a PairTriplet.
type EdgeBinding struct {
	Pairs   PairTriplet
	EdgeIdx uint16 // edge slot inside .Ticks
}

// PairShard is a batch of EdgeBinding that all reference the same PairID.
// They are attached to EXECUTORS in chunks to balance core fan-outs evenly.
type PairShard struct {
	Pair PairID
	Bins []EdgeBinding
}

/*
FanoutEntry (24 B) ― subscription record that lets a core:
  - overwrite ONE tick slot in its CycleState
  - update that CycleState’s min-heap key in O(log n)

Layout:

	 0-7   *CycleState         – shared payload
	 8-15  *bucketqueue.Queue  – owning min-heap
	16-19  bucketqueue.Handle  – slot inside that heap
	20-21  EdgeIdx (uint16)    – which Ticks[] slot to overwrite
	22-23  _pad                – alignment
*/
type FanoutEntry struct {
	State   *CycleState
	Queue   *bucketqueue.Queue
	Handle  bucketqueue.Handle
	EdgeIdx uint16
	//lint:ignore U1000 padding for cache alignment
	_pad [2]byte
}

// CoreExecutor owns all per-core state and runs on a pinned thread.
type CoreExecutor struct {
	Heaps     []bucketqueue.Queue // one min-heap per *local* pair
	Fanouts   [][]FanoutEntry     // Fanouts[localPairID]
	LocalIdx  localidx.Hash       // PairID → dense localPairID
	IsReverse bool                // true on reverse-direction cores
}

/*───────────────────────────  Ring Message  ───────────────────────────*/

// TickUpdate is the fixed-size (32 B) message pushed through ring32.
type TickUpdate struct {
	Pair PairID
	//lint:ignore U1000 padding for cache alignment
	_pad0            uint32 // aligns following floats
	FwdTick, RevTick float64
	//lint:ignore U1000 padding for cache alignment
	_pad1 [8]byte // pad to 32 B exactly
}

//lint:ignore U1000 Compile-time layout assertion
const _tickUpdateSize = unsafe.Sizeof(TickUpdate{})

//lint:ignore U1000 Compile-time layout assertion
var _TickUpdateSizeCheck [32 - int(_tickUpdateSize)]byte

/*──────────────────────  Package-level scratch  ───────────────────────*/

var (
	executors   [64]*CoreExecutor // index == logical core
	rings       [64]*ring32.Ring  // SPSC rings
	addr2pair   [1 << 17]PairID   // 128 Ki × open addressing
	pair2cores  [1 << 17]CoreMask // fan-out bitmap per pair
	shardBucket map[PairID][]PairShard

	splitThreshold = 16_384 // max EdgeBindings per PairShard
)

/*────────────────────────  Public Bootstrap  ──────────────────────────*/

// InitExecutors spins up one executor per logical core (minus 4 reserved for OS)
// and wires up fan-outs for every triangular path.
func InitExecutors(cycles []PairTriplet) {
	// 1️⃣ Decide core count (even, min 8, max 64)
	n := runtime.NumCPU() - 4
	switch {
	case n < 8:
		n = 8
	case n > 64:
		n = 64
	}
	if n&1 != 0 {
		n--
	}
	half := n / 2 // first half = forward, second half = reverse

	// 2️⃣ Build {pair → fan-out shards}
	buildFanoutShards(cycles)

	// 3️⃣ Spin shard workers (one per core)
	shardCh := make([]chan PairShard, n)
	for i := range shardCh {
		shardCh[i] = make(chan PairShard, 256)
		go shardWorker(i, half, shardCh[i])
	}

	// 4️⃣ Round-robin shards to cores, update pair2cores bitmap
	coreIdx := 0
	for _, shards := range shardBucket {
		for _, s := range shards {
			fwd := coreIdx % half // same index in forward half
			rev := fwd + half     // mirror index in reverse half
			shardCh[fwd] <- s
			shardCh[rev] <- s
			for _, eb := range s.Bins { // track *all* pairs in that cycle
				for _, pid := range eb.Pairs {
					pair2cores[pid] |= 1<<fwd | 1<<rev
				}
			}
			coreIdx++
		}
	}
	for _, ch := range shardCh {
		close(ch)
	}
}

// RegisterPair hashes the 40-byte hex address → PairID (inject at init).
func RegisterPair(addr40 []byte, pid PairID) {
	idx := utils.Hash17(addr40)
	for addr2pair[idx] != 0 {
		idx = (idx + 64) & (1<<17 - 1) // 64-slot stride ≈ λ cache sets
	}
	addr2pair[idx] = pid
}

// RegisterRoute lets external code add extra core mappings (rare).
func RegisterRoute(pid PairID, core uint8) { pair2cores[pid] |= 1 << core }

/*─────────────────────────  Fast-path Ingress  ────────────────────────*/

// DispatchUpdate converts a LogView → TickUpdate and multicasts into rings.
// Hot / branch-free except for bitmap walk.
func DispatchUpdate(v *types.LogView) {
	pid := lookupPairID(v.Addr[addrHexStart:addrHexEnd])
	if pid == 0 {
		return // unknown pair — silently drop
	}

	// Extract±log2(reserveRatio) using the user’s hand-tuned fastuni impl.
	r0 := utils.LoadBE64(v.Data[24:])
	r1 := utils.LoadBE64(v.Data[56:])
	tick := fastuni.Log2ReserveRatio(r0, r1)

	var msg [32]byte
	upd := (*TickUpdate)(unsafe.Pointer(&msg))
	upd.Pair, upd.FwdTick, upd.RevTick = pid, tick, -tick

	// Fan-out over ring32: TrailingZeros64 until bitmap empty.
	for m := pair2cores[pid]; m != 0; {
		core := bits.TrailingZeros64(uint64(m))
		rings[core].Push(&msg)
		m &^= 1 << core
	}
}

/*────────────────────────  Helper Look-ups  ───────────────────────────*/

func lookupPairID(addr40 []byte) PairID {
	idx := utils.Hash17(addr40)
	for pid := addr2pair[idx]; pid == 0; pid = addr2pair[idx] {
		idx = (idx + 64) & (1<<17 - 1)
	}
	return addr2pair[idx]
}

/*────────────────────  Fan-out shard construction  ────────────────────*/

func buildFanoutShards(cycles []PairTriplet) {
	shardBucket = make(map[PairID][]PairShard)

	tmp := make(map[PairID][]EdgeBinding, len(cycles)*3)
	for _, tri := range cycles {
		tmp[tri[0]] = append(tmp[tri[0]], EdgeBinding{Pairs: tri, EdgeIdx: 0})
		tmp[tri[1]] = append(tmp[tri[1]], EdgeBinding{Pairs: tri, EdgeIdx: 1})
		tmp[tri[2]] = append(tmp[tri[2]], EdgeBinding{Pairs: tri, EdgeIdx: 2})
	}

	for pid, bins := range tmp {
		shuffleBindings(bins)
		for off := 0; off < len(bins); off += splitThreshold {
			end := off + splitThreshold
			if end > len(bins) {
				end = len(bins)
			}
			shardBucket[pid] = append(
				shardBucket[pid],
				PairShard{Pair: pid, Bins: bins[off:end]},
			)
		}
	}
}

func shuffleBindings(b []EdgeBinding) {
	for i := len(b) - 1; i > 0; i-- {
		j := crandInt(i + 1)
		b[i], b[j] = b[j], b[i]
	}
}

// crandInt returns a uniform int ∈ [0,n) using crypto/rand.
func crandInt(n int) int {
	var buf [8]byte
	rand.Read(buf[:])
	v := binary.LittleEndian.Uint64(buf[:])
	if n&(n-1) == 0 {
		return int(v & uint64(n-1))
	}
	hi, _ := bits.Mul64(v, uint64(n))
	return int(hi)
}

/*──────────────────────────  Shard Worker  ────────────────────────────*/

// shardWorker pins to its OS thread, builds local indices, then spins a
// ring32.PinnedConsumer loop forever.
func shardWorker(coreID, half int, in <-chan PairShard) {
	runtime.LockOSThread()

	ex := &CoreExecutor{
		Heaps:     make([]bucketqueue.Queue, 0, 1024),
		Fanouts:   make([][]FanoutEntry, 0, 1024),
		LocalIdx:  localidx.New(1 << 16),
		IsReverse: coreID >= half,
	}
	executors[coreID] = ex

	rb := ring32.New(1 << 14) // 16 Ki entries per core
	rings[coreID] = rb

	cycleBuf := make([]CycleState, 0, 4096)
	for shard := range in {
		attachShard(ex, &shard, &cycleBuf)
	}

	ring32.PinnedConsumer(
		coreID, rb, new(uint32), new(uint32), // stats placeholders
		func(p *[32]byte) { handleTick(ex, (*TickUpdate)(unsafe.Pointer(p))) },
		make(chan struct{}), // never signals
	)
}

/*───────────────────────  Shard Attachment  ───────────────────────────*/

// attachShard wires one PairShard into a CoreExecutor (all local memory).
func attachShard(ex *CoreExecutor, shard *PairShard, buf *[]CycleState) {
	lid32 := ex.LocalIdx.Put(uint32(shard.Pair), uint32(len(ex.Heaps)))
	lid := uint32(lid32)

	// First time we see this localPairID → create heap & fanout slice.
	if int(lid32) == len(ex.Heaps) {
		ex.Heaps = append(ex.Heaps, *bucketqueue.New())
		ex.Fanouts = append(ex.Fanouts, nil)
	}
	hq := &ex.Heaps[lid]

	for _, eb := range shard.Bins {
		// Back the CycleState from a grow-only per-core buffer.
		*buf = append(*buf, CycleState{Pairs: eb.Pairs})
		cs := &(*buf)[len(*buf)-1]

		// Allocate a stable handle inside the pair-heap.
		h, _ := hq.Borrow()
		_ = hq.Push(4095, h, unsafe.Pointer(cs)) // initial key loosely “max”

		// Create fanout entries for the TWO edges *other* than eb.EdgeIdx.
		for _, edge := range []uint16{(eb.EdgeIdx + 1) % 3, (eb.EdgeIdx + 2) % 3} {
			ex.Fanouts[lid] = append(ex.Fanouts[lid], FanoutEntry{
				State: cs, Queue: hq, Handle: h, EdgeIdx: edge,
			})
		}
	}
}

/*──────────────────────────  Hot-path Loop  ───────────────────────────*/

// handleTick pops profitable paths, executes them once, parks the entry at the
// bottom of the heap, re-queues non-profitable ones with their real key, and
// finally propagates the fresh tick to every dependent path.
func handleTick(ex *CoreExecutor, upd *TickUpdate) {
	/* 1️⃣ Polarity */
	tick := upd.FwdTick
	if ex.IsReverse {
		tick = upd.RevTick
	}

	/* 2️⃣ Local lookup */
	lid32, _ := ex.LocalIdx.Get(uint32(upd.Pair))
	lid := uint32(lid32)
	hq := &ex.Heaps[lid]
	fans := ex.Fanouts[lid]

	/* 3️⃣ Profit-drain (PopMin → stash) */
	const maxDrain = 64
	type stashRec struct {
		h         bucketqueue.Handle
		cs        *CycleState
		wasProfit bool // true ⇒ executed this round
	}
	var stash [maxDrain]stashRec
	n := 0

	if h, _, ptr := hq.PopMin(); ptr != nil {
		for {
			cs := (*CycleState)(ptr)
			profit := tick + cs.Ticks[0] + cs.Ticks[1] + cs.Ticks[2]

			if profit >= 0 { // first non-profitable ⇒ stop draining
				stash[n] = stashRec{h, cs, false}
				n++
				break
			}

			// 🚀 one-shot execution, will be parked at bottom later
			onProfitable(cs)
			stash[n] = stashRec{h, cs, true}
			n++

			if n == maxDrain {
				break
			}
			h, _, ptr = hq.PopMin()
			if ptr == nil {
				break
			}
		}
	}

	/* 4️⃣ Re-push drained items BEFORE propagate */
	for i := 0; i < n; i++ {
		rec := stash[i]
		var key int64
		if rec.wasProfit {
			key = 4095 // park at bottom; will be updated after propagate
		} else {
			key = log2ToTick(rec.cs.Ticks[0] + rec.cs.Ticks[1] + rec.cs.Ticks[2])
		}
		_ = hq.Push(key, rec.h, unsafe.Pointer(rec.cs))
	}

	/* 5️⃣ Propagate new tick to all fan-outs */
	for _, f := range fans {
		cs := f.State
		cs.Ticks[f.EdgeIdx] = tick
		key := log2ToTick(cs.Ticks[0] + cs.Ticks[1] + cs.Ticks[2])
		_ = f.Queue.Update(key, f.Handle, unsafe.Pointer(cs))
	}
}

/*────────────────────────────  Helpers  ───────────────────────────────*/

func onProfitable(cs *CycleState) {
	// TODO: hook into execution engine.
	_, _ = cs.Ticks, cs.Pairs
}

// log2ToTick quantises a base-2 log-ratio into a 4 096-bucket histogram index.
func log2ToTick(r float64) int64 {
	const clamp, scale = 128.0, 16.0 // (±128)×16 → [0,4096)
	if r > clamp {
		r = clamp
	} else if r < -clamp {
		r = -clamp
	}
	return int64((r + clamp) * scale)
}
