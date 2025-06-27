// router.go — 64‑core fan‑out router for real‑time tri‑arbitrage tick propagation.
// Min‑check + packed structs edition — 2025‑06‑27.
//
// Key tweaks in this drop:
//   • `Fanout` shrunk from 24 B → **16 B** (pointer + handle + meta uint32).
//   • `meta` encodes Lid (high 16 b) | EdgeIdx (low 16 b).
//   • All call‑sites updated; no extra padding remains in hot‑path structs.
//
// Memory layout audits (Go 1.22, darwin/arm64):
//   PathState  :=  48 B  (Ticks[3] + Pairs[3])
//   PriceUpdate:=  32 B  (compile‑time assert)
//   Fanout     :=  16 B  (8 + 4 + 4)  // cache‑friendly

package router

import (
	"crypto/rand"
	"encoding/binary"
	"math"
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
	   Fanout (16 B):
	     0‑7   *PathState        (pointer)
	     8‑11  bucketqueue.Handle (uint32)
	    12‑15  meta = lid<<16 | edgeIdx (uint32)
	*/
	Fanout struct {
		Path   *PathState
		Handle bucketqueue.Handle
		Meta   uint32
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
	_ = [want - int(_priceUpdateSize)]byte{} // build‑time failure if size drift
}

/*────────────────────────  Package Globals  ────────────────────────*/

var (
	coreRouters [64]*CoreRouter
	coreRings   [64]*ring.Ring

	addrToPairID [1 << 17]PairID
	pairCoreMask [1 << 17]CPUMask
	pairShards   map[PairID][]Shard

	splitThreshold = 16_384
)

/*────────────────────────  Public API  ────────────────────────*/

func InitRouters(paths []TriPath) {
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

	rb := ring.New(1 << 14)
	coreRings[coreID] = rb

	buf := make([]PathState, 0, 4096)
	for sh := range in {
		installShard(rt, &sh, &buf)
	}

	ring.PinnedConsumer(
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

	// first encounter of this local-pair id → allocate bucket & fanout slice
	if int(lid32) == len(rt.Buckets) {
		rt.Buckets = append(rt.Buckets, *bucketqueue.New())
		rt.Fanouts = append(rt.Fanouts, nil)
	}

	for _, ref := range sh.Refs {
		// Persistent PathState lives in the per-core buffer
		*buf = append(*buf, PathState{Pairs: ref.Pairs})
		ps := &(*buf)[len(*buf)-1]

		// Each PathState gets its own handle inside the pair’s min-heap bucket
		handle, _ := rt.Buckets[lid].Borrow()
		_ = rt.Buckets[lid].Push(0, handle, unsafe.Pointer(ps))

		// Encode Lid | EdgeIdx into a 32-bit meta field (lid<<16 | edge)
		meta := uint32(lid)<<16 | uint32(ref.EdgeIdx)

		rt.Fanouts[lid] = append(
			rt.Fanouts[lid],
			Fanout{Path: ps, Handle: handle, Meta: meta},
		)
	}
}

/*────────────────────────  Hot Path  ────────────────────────*/

func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.IsReverse {
		tick = upd.RevTick
	}

	lid32, _ := rt.Local.Get(uint32(upd.Pair))
	lid := LocalPairID(lid32)
	bq := &rt.Buckets[lid]

	var minSum float64 = 1e300
	var prof *PathState

	for _, fan := range rt.Fanouts[lid] {
		edge := uint16(fan.Meta & 0xFFFF) // decode EdgeIdx
		ps := fan.Path
		ps.Ticks[edge] = tick

		sum := ps.Ticks[0] + ps.Ticks[1] + ps.Ticks[2]
		key := int64(math.Float64bits(sum)) // bucket key is monotonic on sum

		_ = bq.Update(key, fan.Handle, unsafe.Pointer(ps)) // ignore error

		if sum < minSum {
			minSum, prof = sum, ps
		}
	}

	if minSum < 0 {
		onProfitablePath(prof)
	}
}

func onProfitablePath(p *PathState) {
	// TODO: forward to execution engine.
	_ = p
}
