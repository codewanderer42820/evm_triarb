// router.go — High-performance triangular-arbitrage fan-out router (64 cores)
// -----------------------------------------------------------------------------
// • Zero-copy address→PairID hashing (stride-64, 5-word keys)
// • Per-core executors, each with lock-free *QuantumQueue* buckets
// • ring24 (56-byte) SPSC rings for cross-core TickUpdate dispatch
// • Zero heap allocations in every hot path
// -----------------------------------------------------------------------------
//
//	Layout legend
//	┌──────────────────────────────────────────────────────────────────────────┐
//	│ tx-ingress (ws_conn) ─▶ parser ─▶ DispatchUpdate ─▶ ring24 ─▶ executor │
//	│                                                        ▲               │
//	│                          cycle profit evaluation ◀─────┘               │
//	└──────────────────────────────────────────────────────────────────────────┘
package router

import (
	"crypto/rand"
	"encoding/binary"
	"math/bits"
	"runtime"
	"unsafe"

	"main/control"
	"main/fastuni"
	"main/localidx"
	"main/quantumqueue"
	"main/ring24"
	"main/types"
	"main/utils"
)

/*──────────────────── Address → PairID mapping (hot path) ───────────────────*/

const (
	addrHexStart = 3  // v.Addr[3:43] == 40-byte ASCII hex
	addrHexEnd   = 43 // exclusive
)

// wordKey: five unaligned 8-byte words (40 B)
type wordKey struct{ w [5]uint64 }

// Two fixed arrays (≈ 20 MiB + 2 MiB) for instant lookup.
var (
	pairKey  [1 << 20]wordKey // 131 072 cache-lines (stride-64)
	addr2pid [1 << 20]PairID  // 0 == empty
)

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func sliceToWordKey(b []byte) wordKey {
	return wordKey{w: [5]uint64{
		binary.LittleEndian.Uint64(b[0:8]),
		binary.LittleEndian.Uint64(b[8:16]),
		binary.LittleEndian.Uint64(b[16:24]),
		binary.LittleEndian.Uint64(b[24:32]),
		binary.LittleEndian.Uint64(b[32:40]),
	}}
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (a wordKey) equal(b wordKey) bool { return a.w == b.w }

//go:nosplit
//go:inline
func RegisterPair(addr40 []byte, pid PairID) {
	k := sliceToWordKey(addr40)
	idx := utils.Hash17(addr40) // 17-bit hash
	mask := uint32((1 << 20) - 1)
	for {
		if addr2pid[idx] == 0 {
			pairKey[idx] = k
			addr2pid[idx] = pid
			return
		}
		if pairKey[idx].equal(k) {
			addr2pid[idx] = pid
			return
		}
		idx = (idx + 64) & mask // stride-64 probing
	}
}

//go:nosplit
//go:inline
func lookupPairID(addr40 []byte) PairID {
	k := sliceToWordKey(addr40)
	idx := utils.Hash17(addr40)
	mask := uint32((1 << 20) - 1)
	for {
		pid := addr2pid[idx]
		if pid == 0 {
			return 0
		}
		if pairKey[idx].equal(k) {
			return pid
		}
		idx = (idx + 64) & mask
	}
}

/*──────────────────────────── Core data types ───────────────────────────────*/

type PairID uint32
type PairTriplet [3]PairID

//go:align 64
//go:notinheap
type CycleState struct { // 64 B
	Ticks [3]float64 // 24 B  ← hottest: used in every scoring op
	Pairs [3]PairID  // 12 B
	_     uint32
	_     [3]uint64 // pad → 64 B
} // fills exactly one line

// Always accessed via pointer → keep pointer first for early-deref
type FanoutEntry struct { // 32 B
	State   *CycleState                //  8 B  ← first cache miss pays for State prefetch
	Queue   *quantumqueue.QuantumQueue //  8 B
	Handle  quantumqueue.Handle        //  4 B
	EdgeIdx uint16                     //  2 B
	_       uint16                     //  2 B pad → 24
	_       uint64                     //     pad → 32 B
}

// PairShard is cold; but slice header first lets len/cap live in same line
type PairShard struct { // 32 B
	Bins []EdgeBinding // 24 B  ← slice header (ptr,len,cap)
	Pair PairID        //  4 B
	_    uint32        //  4 B pad → 32 B
}

//go:notinheap
type TickUpdate struct { // 24 B (ring slot)
	FwdTick float64 //  8 B  ← fast path
	RevTick float64 //  8 B
	Pair    PairID  //  4 B
	_       uint32  //  4 B pad → 24 B
}

// EdgeBinding only ever scanned in batch — keep array first for stride walk
type EdgeBinding struct { // 16 B
	Pairs   [3]PairID // 12 B
	EdgeIdx uint16    //  2 B
	_       uint16    //  2 B → 16
}

// CoreExecutor owns per-core queues and fan-out tables.
type CoreExecutor struct {
	// ── hot header: always-touched fields, all within first 64 B ──
	Heaps     []*quantumqueue.QuantumQueue // 24 B
	Fanouts   [][]FanoutEntry              // 24 B  (offset 24 → 47)
	IsReverse bool                         //  1 B  (offset 48)
	_         [7]byte                      // pad
	Done      <-chan struct{}              //  8 B  (offset 56 → 63)

	// ── second line: 64-byte local index header (pointers & mask) ──
	LocalIdx localidx.Hash // 64 B  (offset 64 → 127)
} // total size = 128 B

/*──────────────────────────── Global state ──────────────────────────────────*/

var (
	executors      [64]*CoreExecutor
	rings          [64]*ring24.Ring
	pair2cores     [1 << 20]uint64
	shardBucket    map[PairID][]PairShard
	splitThreshold = 1 << 16
)

const (
	clamp   = 128.0                       // domain −128 … +128
	maxTick = 262_143                     // 18-bit ceiling
	scale   = (maxTick - 1) / (2 * clamp) // 262142 / 256 ≈ 1023.9921875
)

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func log2ToTick(x float64) int64 {
	switch {
	case x <= -clamp:
		return 0
	case x >= clamp:
		return maxTick
	default:
		// (x + 128) ∈ (0‥256). Scale ensures result ≤ 262142, avoiding overflow.
		return int64((x + clamp) * scale)
	}
}

/*──────────────────────── Executor bootstrap ───────────────────────────────*/

func InitExecutors(cycles []PairTriplet) {
	n := runtime.NumCPU() - 1
	if n > 64 {
		n = 64
	}
	if n&1 == 1 {
		n--
	}
	half := n / 2

	buildFanoutShards(cycles)

	chs := make([]chan PairShard, n)
	for i := range chs {
		chs[i] = make(chan PairShard, 1<<10)
		go shardWorker(i, half, chs[i])
	}

	core := 0
	for _, shards := range shardBucket {
		for _, s := range shards {
			fwd := core % half
			rev := fwd + half
			chs[fwd] <- s
			chs[rev] <- s
			for _, eb := range s.Bins {
				for _, pid := range eb.Pairs {
					pair2cores[pid] |= 1<<fwd | 1<<rev
				}
			}
			core++
		}
	}
	for _, c := range chs {
		close(c)
	}
}

func shardWorker(coreID, half int, in <-chan PairShard) {
	runtime.LockOSThread()

	done := make(chan struct{})
	ex := &CoreExecutor{
		LocalIdx:  localidx.New(1 << 16),
		IsReverse: coreID >= half,
		Done:      done,
	}
	executors[coreID] = ex
	rings[coreID] = ring24.New(1 << 16)

	cycleBuf := make([]CycleState, 0)
	for sh := range in {
		attachShard(ex, &sh, &cycleBuf)
	}

	stop, hot := control.Flags()
	ring24.PinnedConsumer(coreID, rings[coreID], stop, hot,
		func(p *[24]byte) { handleTick(ex, (*TickUpdate)(unsafe.Pointer(p))) },
		done)
}

/*──────────────────── attachShard (fan-in construction) ─────────────────────*/

//go:nosplit
//go:inline
func attachShard(ex *CoreExecutor, sh *PairShard, buf *[]CycleState) {
	lid := ex.LocalIdx.Put(uint32(sh.Pair), uint32(len(ex.Heaps)))
	if int(lid) == len(ex.Heaps) { // first time we see pair on this core
		ex.Heaps = append(ex.Heaps, quantumqueue.New()) // returns *QuantumQueue
		ex.Fanouts = append(ex.Fanouts, nil)
	}
	hq := ex.Heaps[lid] // already a pointer

	for _, eb := range sh.Bins {
		*buf = append(*buf, CycleState{Pairs: eb.Pairs})
		cs := &(*buf)[len(*buf)-1]
		h, _ := hq.BorrowSafe()
		hq.Push(4095, h, (*[48]byte)(unsafe.Pointer(cs)))
		for _, edge := range [...]uint16{(eb.EdgeIdx + 1) % 3, (eb.EdgeIdx + 2) % 3} {
			ex.Fanouts[lid] = append(ex.Fanouts[lid],
				FanoutEntry{State: cs, Queue: hq, Handle: h, EdgeIdx: edge})
		}
	}
}

/*──────────────────────────── Tick handling ─────────────────────────────────*/

func handleTick(ex *CoreExecutor, upd *TickUpdate) {
	tick := upd.FwdTick
	if ex.IsReverse {
		tick = upd.RevTick
	}

	lid, _ := ex.LocalIdx.Get(uint32(upd.Pair))
	hq := ex.Heaps[lid]
	fans := ex.Fanouts[lid]

	type rec struct {
		h    quantumqueue.Handle
		cs   *CycleState
		prof bool
	}
	var stash [128]rec
	n := 0

	for {
		h, _, ptr := hq.PopMin()
		if ptr == nil {
			break
		}
		cs := (*CycleState)(unsafe.Pointer(ptr))
		prof := (tick + cs.Ticks[0] + cs.Ticks[1] + cs.Ticks[2]) < 0
		stash[n] = rec{h, cs, prof}
		n++
		if !prof || n == len(stash) {
			break
		}
	}

	for i := 0; i < n; i++ {
		r := stash[i]
		key := int64(4095)
		if !r.prof {
			key = log2ToTick(r.cs.Ticks[0] + r.cs.Ticks[1] + r.cs.Ticks[2])
		}
		hq.Push(key, r.h, (*[48]byte)(unsafe.Pointer(r.cs)))
	}

	for _, f := range fans {
		cs := f.State
		cs.Ticks[f.EdgeIdx] = tick
		key := log2ToTick(cs.Ticks[0] + cs.Ticks[1] + cs.Ticks[2])
		f.Queue.Update(key, f.Handle, (*[48]byte)(unsafe.Pointer(cs)))
	}
}

/*──────────────────── DispatchUpdate (entry from parser) ────────────────────*/

//go:nosplit
//go:inline
func RegisterRoute(pid PairID, core uint8) { pair2cores[pid] |= 1 << core }

//go:nosplit
func DispatchUpdate(v *types.LogView) {
	pid := lookupPairID(v.Addr[addrHexStart:addrHexEnd])
	if pid == 0 {
		return
	}

	r0 := utils.LoadBE64(v.Data[24:])
	r1 := utils.LoadBE64(v.Data[56:])
	tick, _ := fastuni.Log2ReserveRatio(r0, r1)

	var msg [24]byte
	upd := (*TickUpdate)(unsafe.Pointer(&msg))
	upd.Pair, upd.FwdTick, upd.RevTick = pid, tick, -tick

	for m := pair2cores[pid]; m != 0; {
		core := bits.TrailingZeros64(uint64(m))
		rings[core].Push(&msg)
		m &^= 1 << core
	}
}

/*──────────────────── Fan-out shard builders (cold path) ────────────────────*/

//go:nosplit
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
			shardBucket[pid] = append(shardBucket[pid],
				PairShard{Pair: pid, Bins: bins[off:end]})
		}
	}
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func shuffleBindings(b []EdgeBinding) {
	for i := len(b) - 1; i > 0; i-- {
		j := crandInt(i + 1)
		b[i], b[j] = b[j], b[i]
	}
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func crandInt(n int) int {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	v := binary.LittleEndian.Uint64(buf[:])
	if n&(n-1) == 0 {
		return int(v & uint64(n-1))
	}
	hi, _ := bits.Mul64(v, uint64(n))
	return int(hi)
}
