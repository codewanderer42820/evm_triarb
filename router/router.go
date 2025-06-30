// router.go — High-performance triangular-arbitrage fan-out router (64 cores)
// -----------------------------------------------------------------------------
// • Zero-copy address→PairID hashing (stride-64, 5-word keys)
// • Per-core executors, each with lock-free *QuantumQueue* buckets
// • ring56 (56-byte) SPSC rings for cross-core TickUpdate dispatch
// • Zero heap allocations in every hot path
// -----------------------------------------------------------------------------
//
//	Layout legend
//	┌──────────────────────────────────────────────────────────────────────────┐
//	│ tx-ingress (ws_conn) ─▶ parser ─▶ DispatchUpdate ─▶ ring56 ─▶ executor │
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

	"main/fastuni"
	"main/localidx"
	"main/quantumqueue"
	"main/ring56"
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
	pairKey  [1 << 17]wordKey // 131 072 cache-lines (stride-64)
	addr2pid [1 << 17]PairID  // 0 == empty
)

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

//go:nosplit
//go:inline
//go:registerparams
func (a wordKey) equal(b wordKey) bool { return a.w == b.w }

//go:nosplit
//go:inline
func RegisterPair(addr40 []byte, pid PairID) {
	k := sliceToWordKey(addr40)
	idx := utils.Hash17(addr40) // 17-bit hash
	mask := uint32((1 << 17) - 1)
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
	mask := uint32((1 << 17) - 1)
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

// CycleState = 56 B = two cache-lines, fits ring56 slot.
type CycleState struct {
	Ticks [3]float64
	Pairs PairTriplet
	_     [4]byte // align to 40
}

type EdgeBinding struct {
	Pairs   PairTriplet
	EdgeIdx uint16
	_       uint16
}
type PairShard struct {
	Pair PairID
	Bins []EdgeBinding
}

// FanoutEntry points to a CycleState in its owning QuantumQueue.
type FanoutEntry struct {
	State   *CycleState
	Queue   *quantumqueue.QuantumQueue
	Handle  quantumqueue.Handle
	EdgeIdx uint16
	_       [2]byte
}

// CoreExecutor owns all state for one OS thread / logical core.
type CoreExecutor struct {
	Heaps     []*quantumqueue.QuantumQueue // *pointer* slice (API requires ptr)
	Fanouts   [][]FanoutEntry
	LocalIdx  localidx.Hash
	IsReverse bool
}

// TickUpdate — 56-byte ring message (PairID + two ticks + padding)
type TickUpdate struct {
	FwdTick, RevTick float64
	Pair             PairID
}

var _ [56 - unsafe.Sizeof(TickUpdate{})]byte

/*──────────────────────────── Global state ──────────────────────────────────*/

var (
	executors      [64]*CoreExecutor
	rings          [64]*ring56.Ring
	pair2cores     [1 << 17]uint64
	shardBucket    map[PairID][]PairShard
	splitThreshold = 32_768
)

const (
	clamp   = 128.0
	scale   = 16.0
	maxTick = 4095
)

//go:nosplit
//go:inline
//go:registerparams
func log2ToTick(x float64) int64 {
	if x < -clamp {
		return 0
	}
	if x > clamp {
		return maxTick
	}
	t := int64((x + clamp) * scale)
	if t > maxTick {
		t = maxTick
	}
	return t
}

/*──────────────────────── Executor bootstrap ───────────────────────────────*/

func InitExecutors(cycles []PairTriplet) {
	n := runtime.NumCPU() - 1
	if n < 4 {
		n = 4
	}
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
		chs[i] = make(chan PairShard, 8192)
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

	ex := &CoreExecutor{
		LocalIdx:  localidx.New(1 << 16),
		IsReverse: coreID >= half,
	}
	executors[coreID] = ex
	rings[coreID] = ring56.New(1 << 16)

	cycleBuf := make([]CycleState, 0, 8192)
	for sh := range in {
		attachShard(ex, &sh, &cycleBuf)
	}

	ring56.PinnedConsumer(coreID, rings[coreID], new(uint32), new(uint32),
		func(p *[56]byte) { handleTick(ex, (*TickUpdate)(unsafe.Pointer(p))) },
		make(chan struct{}))
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

	var msg [56]byte
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

//go:nosplit
//go:inline
//go:registerparams
func shuffleBindings(b []EdgeBinding) {
	for i := len(b) - 1; i > 0; i-- {
		j := crandInt(i + 1)
		b[i], b[j] = b[j], b[i]
	}
}

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
