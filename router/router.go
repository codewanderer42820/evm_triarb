// router.go — 64‑core fan‑out router for real‑time tri‑arbitrage tick propagation.
// Pointer‑accurate Fanout edition — 2025‑06‑27.
//
// Key tweaks in this drop:
//   • Fanout carries an **explicit pointer** to its owning bucketqueue.Queue so every
//     Update() call touches the correct heap even when a PathState has three live
//     handles (one per pair).
//   • struct size grows from 16 B → 24 B but remains two cache lines for eight
//     consecutive entries on arm64.
//
// Memory layout audits (Go 1.22, darwin/arm64):
//   PathState  := 48 B  (Ticks[3] + Pairs[3])
//   PriceUpdate:= 32 B  (compile‑time assert)
//   Fanout     := 24 B  (2×ptr + u32 + u16 + pad)

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

/*──────────────────────────  Constants  ──────────────────────────*/

const (
	addrHexStart = 3 // LogView.Addr[3:43] → 40‑byte ascii hex
	addrHexEnd   = 43
)

/*────────────────────────  Data Structures  ────────────────────────*/

type (
	PairID      uint32
	LocalPairID uint32
	CPUMask     uint64

	TriPath [3]PairID

	// PathState is the payload stored in bucketqueue.  Ticks first for locality.
	PathState struct {
		Ticks [3]float64 // hot‑path mutable
		Pairs TriPath    // rarely touched in on‑price
	}

	// Ref is a build‑time helper: binds one pair inside a TriPath.
	Ref struct {
		Pairs   TriPath
		EdgeIdx uint16 // 0‑2
	}

	// Shard groups many refs for one PairID.
	Shard struct {
		Pair PairID
		Refs []Ref
	}

	/*
	   Fanout (24 B):
	     0‑7   *PathState          – shared payload
	     8‑15  *bucketqueue.Queue  – owning min‑heap
	    16‑19  bucketqueue.Handle  – slot inside that heap
	    20‑21  EdgeIdx (uint16)    – which Ticks[] slot we overwrite
	    22‑23  _pad                – alignment
	*/
	Fanout struct {
		Path   *PathState
		Queue  *bucketqueue.Queue
		Handle bucketqueue.Handle
		Edge   uint16
		_pad   [2]byte
	}

	CoreRouter struct {
		Buckets   []bucketqueue.Queue // one per local pair
		Fanouts   [][]Fanout          // Fanouts[lid] slice
		Local     localidx.Hash       // PairID → LocalPairID
		IsReverse bool                // true on reverse‑direction cores
	}
)

/*────────────────────────  Ring Message  ────────────────────────*/

type PriceUpdate struct {
	Pair             PairID
	_pad             uint32
	FwdTick, RevTick float64
	_                [8]byte // pad to 32 B
}

const _priceUpdateSize = unsafe.Sizeof(PriceUpdate{})

func init() {
	const want = 32
	_ = [want - int(_priceUpdateSize)]byte{}
}

/*────────────────────────  Package Globals  ────────────────────────*/

var (
	coreRouters [64]*CoreRouter
	coreRings   [64]*ring32.Ring

	addrToPairID [1 << 17]PairID
	pairCoreMask [1 << 17]CPUMask
	pairShards   map[PairID][]Shard

	splitThreshold = 16_384
)

/*────────────────────────  Public API  ────────────────────────*/

func InitRouters(paths []TriPath) {
	n := runtime.NumCPU() - 4 // reserve 4 cores for OS / I/O
	switch {
	case n < 8:
		n = 8
	case n > 64:
		n = 64
	}
	if n&1 != 0 {
		n--
	}
	half := n / 2

	buildFanouts(paths)

	shardCh := make([]chan Shard, n)
	for i := range shardCh {
		shardCh[i] = make(chan Shard, 256)
	}
	for coreID := 0; coreID < n; coreID++ {
		go shardWorker(coreID, half, shardCh[coreID])
	}

	coreIdx := 0
	for _, shards := range pairShards {
		for _, s := range shards {
			fwd := coreIdx % half
			rev := fwd + half
			shardCh[fwd] <- s
			shardCh[rev] <- s
			for _, ref := range s.Refs {
				for _, pid := range ref.Pairs {
					pairCoreMask[pid] |= 1<<fwd | 1<<rev
				}
			}
			coreIdx++
		}
	}
	for _, ch := range shardCh {
		close(ch)
	}
}

func RegisterPair(addr40 []byte, pid PairID) {
	idx := utils.Hash17(addr40)
	for addrToPairID[idx] != 0 {
		idx = (idx + 64) & (1<<17 - 1)
	}
	addrToPairID[idx] = pid
}

func RegisterRoute(pid PairID, core uint8) { pairCoreMask[pid] |= 1 << core }

func RouteUpdate(v *types.LogView) {
	pair := lookupPairID(v.Addr[addrHexStart:addrHexEnd])

	r0 := utils.LoadBE64(v.Data[24:])
	r1 := utils.LoadBE64(v.Data[56:])
	tick := fastuni.Log2ReserveRatio(r0, r1)

	var msg [32]byte
	upd := (*PriceUpdate)(unsafe.Pointer(&msg))
	upd.Pair, upd.FwdTick, upd.RevTick = pair, tick, -tick

	for mask := pairCoreMask[pair]; mask != 0; {
		core := bits.TrailingZeros64(uint64(mask))
		coreRings[core].Push(&msg)
		mask &^= 1 << core
	}
}

/*────────────────────────  Helper Lookup  ────────────────────────*/

func lookupPairID(addr40 []byte) PairID {
	idx := utils.Hash17(addr40)
	for pid := addrToPairID[idx]; pid == 0; pid = addrToPairID[idx] {
		idx = (idx + 64) & (1<<17 - 1)
	}
	return addrToPairID[idx]
}

/*────────────────────────  Fan‑out Builder  ────────────────────────*/

func buildFanouts(paths []TriPath) {
	pairShards = make(map[PairID][]Shard)

	tmp := make(map[PairID][]Ref, len(paths)*3)
	for _, tri := range paths {
		tmp[tri[0]] = append(tmp[tri[0]], Ref{Pairs: tri, EdgeIdx: 0})
		tmp[tri[1]] = append(tmp[tri[1]], Ref{Pairs: tri, EdgeIdx: 1})
		tmp[tri[2]] = append(tmp[tri[2]], Ref{Pairs: tri, EdgeIdx: 2})
	}

	for pid, refs := range tmp {
		shuffleRefs(refs)
		for off := 0; off < len(refs); off += splitThreshold {
			end := off + splitThreshold
			if end > len(refs) {
				end = len(refs)
			}
			pairShards[pid] = append(pairShards[pid], Shard{Pair: pid, Refs: refs[off:end]})
		}
	}
}

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

/*────────────────────────  Bootstrap Worker  ────────────────────────*/

func shardWorker(coreID, half int, in <-chan Shard) {
	runtime.LockOSThread()

	rt := &CoreRouter{
		Buckets:   make([]bucketqueue.Queue, 0, 1024),
		Fanouts:   make([][]Fanout, 0, 1024),
		Local:     localidx.New(1 << 16),
		IsReverse: coreID >= half,
	}
	coreRouters[coreID] = rt

	rb := ring32.New(1 << 14)
	coreRings[coreID] = rb

	buf := make([]PathState, 0, 4096)
	for sh := range in {
		installShard(rt, &sh, &buf)
	}

	ring32.PinnedConsumer(
		coreID, rb, new(uint32), new(uint32),
		func(p *[32]byte) { onPrice(rt, (*PriceUpdate)(unsafe.Pointer(p))) },
		make(chan struct{}),
	)
}

/*────────────────────────  Shard Attach  ────────────────────────*/

// installShard attaches one shard (all refs for a single PairID) to a core.
func installShard(rt *CoreRouter, sh *Shard, buf *[]PathState) {
	lid32 := rt.Local.Put(uint32(sh.Pair), uint32(len(rt.Buckets)))
	lid := LocalPairID(lid32)

	// first encounter of this local‑pair id → allocate bucket & fanout slice
	if int(lid32) == len(rt.Buckets) {
		rt.Buckets = append(rt.Buckets, *bucketqueue.New())
		rt.Fanouts = append(rt.Fanouts, nil)
	}

	bq := &rt.Buckets[lid]

	for _, ref := range sh.Refs {
		// Persistent PathState lives in the per‑core buffer
		*buf = append(*buf, PathState{Pairs: ref.Pairs})
		ps := &(*buf)[len(*buf)-1]

		// Each PathState gets its own handle inside the pair’s min‑heap bucket
		handle, _ := bq.Borrow()
		_ = bq.Push(4095, handle, unsafe.Pointer(ps))

		// Insert fanouts for the two *other* tick slots in this triangle
		for _, edge := range []uint16{(ref.EdgeIdx + 1) % 3, (ref.EdgeIdx + 2) % 3} {
			rt.Fanouts[lid] = append(rt.Fanouts[lid], Fanout{
				Path: ps, Queue: bq, Handle: handle, Edge: edge,
			})
		}
	}
}

/*────────────────────────  Hot Path  ────────────────────────*/

// onPrice — PopMin profit‑drain (items re‑queued *before* propagate)
// -----------------------------------------------------------------------------
//   - All temporaries live on the stack (stash[64]) → zero heap traffic.
//   - Drained paths are re‑pushed with their original handles **prior** to the
//     fan‑out loop, guaranteeing that Update() can locate them.
//
// Queue may be empty; hard cap of 64 keeps the stack bounded.
func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	/* 1️⃣  polarity -------------------------------------------------------- */
	tick := upd.FwdTick
	if rt.IsReverse {
		tick = upd.RevTick
	}

	/* 2️⃣  local state ----------------------------------------------------- */
	lid32, _ := rt.Local.Get(uint32(upd.Pair))
	lid := LocalPairID(lid32)
	bq := &rt.Buckets[lid]
	fan := rt.Fanouts[lid]

	/* 3️⃣  profit‑drain phase --------------------------------------------- */
	const capDrain = 64
	type popped struct {
		h  bucketqueue.Handle
		ps *PathState
	}
	var stash [capDrain]popped
	n := 0

	if h, _, ptr := bq.PopMin(); ptr != nil { // queue could be empty
		for {
			ps := (*PathState)(ptr)
			if tick+ps.Ticks[0]+ps.Ticks[1]+ps.Ticks[2] >= 0 {
				stash[n] = popped{h, ps} // first non‑profitable
				n++
				break
			}
			onProfitablePath(ps)     // 🚀 fire trade
			stash[n] = popped{h, ps} // keep for re‑queue
			n++
			if n == capDrain {
				break // stack guard
			}
			h, _, ptr = bq.PopMin()
			if ptr == nil {
				break // queue drained
			}
		}
	}

	/* 4️⃣  push drained items back in BEFORE propagate -------------------- */
	for i := 0; i < n; i++ {
		ps := stash[i].ps
		h := stash[i].h
		sum := ps.Ticks[0] + ps.Ticks[1] + ps.Ticks[2]
		key := log2ToTick(sum)
		_ = bq.Push(key, h, unsafe.Pointer(ps)) // same handle, no alloc
	}

	/* 5️⃣  propagate fresh tick to every dependent path ------------------- */
	for _, f := range fan { // fan slice is core‑local
		ps := f.Path
		ps.Ticks[f.Edge] = tick
		sum := ps.Ticks[0] + ps.Ticks[1] + ps.Ticks[2]
		key := log2ToTick(sum)
		_ = f.Queue.Update(key, f.Handle, unsafe.Pointer(ps))
	}
}

func onProfitablePath(p *PathState) {
	// TODO: forward to execution engine.
	_ = p
}

// -----------------------------------------------------------------------------
// log2ToTick — Quantises a base‑2 log‑ratio into a 4 096‑bucket tick index
// -----------------------------------------------------------------------------
//
//	clamp = 128  → values beyond ±128 are saturated (covers worst‑case swings)
//	scale = 16   → (2×clamp)×scale == 4 096 (12‑bit histogram)
//
// -----------------------------------------------------------------------------
func log2ToTick(r float64) int64 {
	const clamp = 128.0 // ±range
	const scale = 16.0  // buckets per unit

	if r > clamp {
		r = clamp
	} else if r < -clamp {
		r = -clamp
	}
	return int64((r + clamp) * scale)
}
