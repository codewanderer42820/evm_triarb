// router.go — 64-core parallelised fan-out router for real-time arbitrage updates.
// Core duties: dispatch price updates to per-core goroutines, manage per-pair
// tick queues, and keep arbitrage paths.

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

/*──────────────────────────────────────────────────────────────────────────────
   Data Structures
──────────────────────────────────────────────────────────────────────────────*/

// PairID: globally unique pair ID
type PairID uint32

// LocalPairID: dense per-core local ID (used as an index)
type LocalPairID uint32

// CPUMask: 64-bit bitmap of core targets for dispatch
type CPUMask uint64

// TriCycle: a 3-leg arbitrage path (three pair IDs)
type TriCycle [3]PairID

// ArbPath holds tick values and the cycle’s pair IDs
type ArbPath struct {
	Ticks [3]float64
	Pairs TriCycle
}

// Ref says: “for this pair I am edge <Edge> (0,1,2) of <Pairs>”.
// We emit ONE Ref per (pair, tri) and reconstruct the two update
// fan-outs locally during shard installation.
type Ref struct {
	Pairs TriCycle
	Edge  uint16 // the pair’s own position inside the triangle
}

// Shard groups refs by pair
type Shard struct {
	Pair PairID
	Refs []Ref
}

// Passed between threads via ring
type PriceUpdate struct {
	Pair             PairID
	FwdTick, RevTick float64
}

// Fanout links an ArbPath to its per-pair queue and tick slot to update
type Fanout struct {
	Path  *ArbPath           // shared triangle object
	Queue *bucketqueue.Queue // per-pair queue (PeepMin / Update)
	Edge  uint16             // tick slot we write on update (0-2)
}

// Per-core router state
type CoreRouter struct {
	Buckets   []bucketqueue.Queue // indexed by LocalPairID
	Fanouts   [][]Fanout          // fan-outs per local pair
	Local     localidx.Hash       // global PairID → LocalPairID
	IsReverse bool                // true if this core handles reverse ticks
}

/*──────────────────────────────────────────────────────────────────────────────
   Global Runtime Structures
──────────────────────────────────────────────────────────────────────────────*/

var (
	coreRouters   [64]*CoreRouter    // per-core logical routers
	coreRings     [64]*ring.Ring     // SPSC ring per core
	addrToPairID  [1 << 17]PairID    // fast address → pairID
	routingBitmap [1 << 17]CPUMask   // bitmap of cores per pairID
	rawShards     map[PairID][]Shard // pre-computed fan-out shards

	splitThreshold = 16_384 // max refs per shard
)

/*──────────────────────────────────────────────────────────────────────────────
   Fan-out Planner
──────────────────────────────────────────────────────────────────────────────*/

func ResetFanouts() { rawShards = make(map[PairID][]Shard) }

// Fisher-Yates shuffle to balance load
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

// BuildFanouts: one Ref per (pair, edge) → split into shards
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
			rawShards[pair] = append(rawShards[pair],
				Shard{Pair: pair, Refs: refs[off:end]},
			)
		}
	}
}

/*──────────────────────────────────────────────────────────────────────────────
   CPU Bootstrap (Router Initialization)
──────────────────────────────────────────────────────────────────────────────*/

func InitCPURings(cycles []TriCycle) {
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

	BuildFanouts(cycles)

	shardCh := make([]chan Shard, n)
	for i := range shardCh {
		shardCh[i] = make(chan Shard, 128)
	}

	for coreID := 0; coreID < n; coreID++ {
		go func(coreID, half int, in <-chan Shard) {
			runtime.LockOSThread()

			rt := &CoreRouter{
				Buckets:   make([]bucketqueue.Queue, 0, 1024),
				Local:     localidx.New(1 << 16),
				IsReverse: coreID >= half,
			}
			coreRouters[coreID] = rt

			rb := ring.New(1 << 14)
			coreRings[coreID] = rb

			paths := make([]ArbPath, 0, 1024)

			for sh := range in {
				localRefs := append([]Ref(nil), sh.Refs...)
				local := Shard{Pair: sh.Pair, Refs: localRefs}
				installShard(rt, &local, &paths)
			}

			ring.PinnedConsumer(
				coreID, rb,
				new(uint32), new(uint32),
				func(p unsafe.Pointer) { onPrice(rt, (*PriceUpdate)(p)) },
				make(chan struct{}),
			)
		}(coreID, half, shardCh[coreID])
	}

	coreIdx := 0
	for _, shards := range rawShards {
		for _, s := range shards {
			fwd := coreIdx % half
			rev := fwd + half
			shardCh[fwd] <- s
			shardCh[rev] <- s
			coreIdx++
		}
	}
	for _, ch := range shardCh {
		close(ch)
	}
}

/*──────────────────────────────────────────────────────────────────────────────
   Shard Attachment Logic
──────────────────────────────────────────────────────────────────────────────*/

// One bucket per pair; two Fanouts (edges) per Ref.
func installShard(rt *CoreRouter, sh *Shard, paths *[]ArbPath) {
	lid := rt.Local.Put(uint32(sh.Pair), uint32(len(rt.Buckets)))

	if int(lid) == len(rt.Buckets) {
		rt.Buckets = append(rt.Buckets, bucketqueue.Queue{})
	}
	if int(lid) >= len(rt.Fanouts) {
		rt.Fanouts = append(rt.Fanouts,
			make([][]Fanout, int(lid)-len(rt.Fanouts)+1)...)
	}

	for _, ref := range sh.Refs {
		*paths = append(*paths, ArbPath{Pairs: ref.Pairs})
		pPtr := &(*paths)[len(*paths)-1]

		// Reconstruct the two legs that change when THIS pair updates.
		a := (ref.Edge + 1) % 3
		b := (ref.Edge + 2) % 3

		rt.Fanouts[lid] = append(rt.Fanouts[lid],
			Fanout{Path: pPtr, Queue: &rt.Buckets[lid], Edge: uint16(a)},
			Fanout{Path: pPtr, Queue: &rt.Buckets[lid], Edge: uint16(b)},
		)
	}
}

/*──────────────────────────────────────────────────────────────────────────────
   Tick Processing & Arbitrage Execution
──────────────────────────────────────────────────────────────────────────────*/

func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.IsReverse {
		tick = upd.RevTick
	}

	lid, _ := rt.Local.Get(uint32(upd.Pair))
	b := &rt.Buckets[lid]
	fan := rt.Fanouts[lid]

	if _, _, ptr := b.PeepMin(); ptr != nil {
		p := (*ArbPath)(ptr)
		if profit := tick + p.Ticks[0] + p.Ticks[1] + p.Ticks[2]; profit < 0 {
			onProfitablePath(p, profit)
		}
	}

	for _, f := range fan {
		p := f.Path
		p.Ticks[f.Edge] = tick
		sum := p.Ticks[0] + p.Ticks[1] + p.Ticks[2]
		f.Queue.Update(l2Bucket(sum), 0, unsafe.Pointer(p))
	}
}

func l2Bucket(x float64) int64 {
	const clamp, scale = 128.0, 16.0
	if x > clamp {
		x = clamp
	} else if x < -clamp {
		x = -clamp
	}
	return int64((x + clamp) * scale)
}

/*──────────────────────────────────────────────────────────────────────────────
   Pair Registration & Lookup
──────────────────────────────────────────────────────────────────────────────*/

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

/*──────────────────────────────────────────────────────────────────────────────
   External Entry Point
──────────────────────────────────────────────────────────────────────────────*/

func RouteUpdate(v *types.LogView) {
	addr := v.Addr[3:43]
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

/*──────────────────────────────────────────────────────────────────────────────
   Arbitrage Action Hook
──────────────────────────────────────────────────────────────────────────────*/

// Called when a profitable path is discovered
func onProfitablePath(_ *ArbPath, _ float64) {}
