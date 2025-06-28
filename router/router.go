// router.go — High-performance triangular-arbitrage fan-out router for 64 cores.
// Combines zero-copy pool address mapping and hot-path tick dispatch.
// Field ordering and padding optimized for 64-bit architectures.
// Hot-path functions are marked for inlining and no-stack-splitting.

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

// Constants defining slice boundaries for zero-copy address lookup
const (
	addrHexStart = 3  // LogView.Addr[3:43] yields 40-byte ASCII hex
	addrHexEnd   = 43 // exclusive
)

// wordKey holds one 40-byte ASCII-hex address as five 8-byte words.
// Comparison is pure register equality: no heap, no slice copy.
// Layout: 5×uint64 = 40 bytes
//
//go:inline
type wordKey struct {
	w [5]uint64
}

// Global mapping arrays: 5 MiB for keys, 0.5 MiB for PairIDs.
// Zero pid indicates empty slot. Uses stride-64 linear probing.
var (
	pairKey  [1 << 17]wordKey
	addr2pid [1 << 17]PairID
)

// sliceToWordKey loads 40 bytes into a wordKey via five unaligned 8-byte reads.
//
//go:inline
//go:nosplit
func sliceToWordKey(addr40 []byte) wordKey {
	return wordKey{w: [5]uint64{
		binary.LittleEndian.Uint64(addr40[0:8]),
		binary.LittleEndian.Uint64(addr40[8:16]),
		binary.LittleEndian.Uint64(addr40[16:24]),
		binary.LittleEndian.Uint64(addr40[24:32]),
		binary.LittleEndian.Uint64(addr40[32:40]),
	}}
}

// equal tests for exact key equality via unrolled register comparisons.
//
//go:inline
//go:nosplit
func (k wordKey) equal(o wordKey) bool {
	return k.w[0] == o.w[0] &&
		k.w[1] == o.w[1] &&
		k.w[2] == o.w[2] &&
		k.w[3] == o.w[3] &&
		k.w[4] == o.w[4]
}

// RegisterPair maps a 40-byte address to a PairID, overwriting existing entries.
// Uses zero-copy, zero-alloc, stride-64 linear probing.
//
//go:inline
func RegisterPair(addr40 []byte, pid PairID) {
	k := sliceToWordKey(addr40)
	idx := utils.Hash17(addr40)
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
		idx = (idx + 64) & mask
	}
}

// lookupPairID resolves a 40-byte address to its PairID; returns 0 if missing.
//
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

// PairID is a globally unique identifier for a trading pair.
type PairID uint32

// CoreMask is a 64-bit bitmap representing target cores.
type CoreMask uint64

// PairTriplet holds three PairIDs forming an arbitrage cycle.
type PairTriplet [3]PairID

// CycleState stores mutable tick values and cycle membership.
// Layout: 3×float64 ticks (hot-path), PairTriplet, 4-byte pad for 40 B total.
type CycleState struct {
	Ticks [3]float64
	Pairs PairTriplet
	_pad  uint32
}

// EdgeBinding associates a cycle and which tick slot to update.
type EdgeBinding struct {
	Pairs   PairTriplet
	EdgeIdx uint16
	_pad    uint16
}

// PairShard groups EdgeBindings for one PairID, balancing fan-out.
type PairShard struct {
	Pair PairID
	Bins []EdgeBinding
}

// FanoutEntry links a CycleState to its heap and tick slot.
// Layout: ptr, ptr, handle, uint16 slot, 2-byte pad.
type FanoutEntry struct {
	State   *CycleState
	Queue   *bucketqueue.Queue
	Handle  bucketqueue.Handle
	EdgeIdx uint16
	_pad    [2]byte
}

// CoreExecutor holds all per-core data and tick queues.
type CoreExecutor struct {
	Heaps     []bucketqueue.Queue
	Fanouts   [][]FanoutEntry
	LocalIdx  localidx.Hash
	IsReverse bool
}

// TickUpdate is the fixed-size 32 B message carrying pair ID and ticks.
// Layout: PairID, pad, two float64, pad to 32 B.
type TickUpdate struct {
	Pair             PairID
	_pad0            uint32
	FwdTick, RevTick float64
	_pad1            [8]byte
}

// compile-time layout assertion
var _ [32 - unsafe.Sizeof(TickUpdate{})]byte

// Per-core executors and ring buffers.
var (
	executors   [64]*CoreExecutor
	rings       [64]*ring32.Ring
	pair2cores  [1 << 17]CoreMask
	shardBucket map[PairID][]PairShard

	// Twice the default split to reduce shards under load
	splitThreshold = 32_768
)

// InitExecutors creates one executor per core and distributes shards.
func InitExecutors(cycles []PairTriplet) {
	// Reserve just 1 core for OS/GC overhead
	n := runtime.NumCPU() - 1
	// Bound [4,64] and force even
	switch {
	case n < 4:
		n = 4
	case n > 64:
		n = 64
	}
	if n&1 != 0 {
		n--
	}
	half := n / 2

	buildFanoutShards(cycles)

	shardCh := make([]chan PairShard, n)
	for i := range shardCh {
		// Deep buffer for bursty fan-out
		shardCh[i] = make(chan PairShard, 8192)
		go shardWorker(i, half, shardCh[i])
	}

	coreIdx := 0
	for _, shards := range shardBucket {
		for _, s := range shards {
			fwd := coreIdx % half
			rev := fwd + half
			shardCh[fwd] <- s
			shardCh[rev] <- s
			for _, eb := range s.Bins {
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

// shardWorker pins to OS thread and processes PairShards.
//
//go:nosplit
func shardWorker(coreID, half int, in <-chan PairShard) {
	runtime.LockOSThread()
	ex := &CoreExecutor{
		Heaps:     make([]bucketqueue.Queue, 0, 1024),
		Fanouts:   make([][]FanoutEntry, 0, 1024),
		LocalIdx:  localidx.New(1 << 16),
		IsReverse: coreID >= half,
	}
	executors[coreID] = ex

	// Larger ring to buffer ~50 ms @1 M/sec bursts
	rb := ring32.New(1 << 16)
	rings[coreID] = rb

	// Pre-allocate per-shard cycle slice
	cycleBuf := make([]CycleState, 0, 8192)
	for shard := range in {
		attachShard(ex, &shard, &cycleBuf)
	}
	ring32.PinnedConsumer(coreID, rb, new(uint32), new(uint32),
		func(p *[32]byte) { handleTick(ex, (*TickUpdate)(unsafe.Pointer(p))) },
		make(chan struct{}))
}

// attachShard registers a PairShard with a CoreExecutor.
//
//go:inline
func attachShard(ex *CoreExecutor, shard *PairShard, buf *[]CycleState) {
	lid32 := ex.LocalIdx.Put(uint32(shard.Pair), uint32(len(ex.Heaps)))
	if int(lid32) == len(ex.Heaps) {
		ex.Heaps = append(ex.Heaps, *bucketqueue.New())
		ex.Fanouts = append(ex.Fanouts, nil)
	}
	hq := &ex.Heaps[lid32]
	for _, eb := range shard.Bins {
		*buf = append(*buf, CycleState{Pairs: eb.Pairs})
		cs := &(*buf)[len(*buf)-1]
		h, _ := hq.Borrow()
		_ = hq.Push(4095, h, unsafe.Pointer(cs)) // retains original key range
		for _, edge := range []uint16{(eb.EdgeIdx + 1) % 3, (eb.EdgeIdx + 2) % 3} {
			ex.Fanouts[lid32] = append(ex.Fanouts[lid32], FanoutEntry{State: cs, Queue: hq, Handle: h, EdgeIdx: edge})
		}
	}
}

// handleTick processes one TickUpdate: drains profitable cycles and propagates ticks.
func handleTick(ex *CoreExecutor, upd *TickUpdate) {
	tick := upd.FwdTick
	if ex.IsReverse {
		tick = upd.RevTick
	}
	lid32, _ := ex.LocalIdx.Get(uint32(upd.Pair))
	hq := &ex.Heaps[lid32]
	fans := ex.Fanouts[lid32]

	const maxDrain = 128
	type stashRec struct {
		h         bucketqueue.Handle
		cs        *CycleState
		wasProfit bool
	}
	var stash [maxDrain]stashRec

	n := 0
	if h, _, ptr := hq.PopMin(); ptr != nil {
		for {
			cs := (*CycleState)(ptr)
			profit := tick + cs.Ticks[0] + cs.Ticks[1] + cs.Ticks[2]
			if profit >= 0 {
				stash[n] = stashRec{h, cs, false}
				n++
				break
			}
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

	for i := 0; i < n; i++ {
		rec := stash[i]
		var key int64
		if rec.wasProfit {
			key = 4095
		} else {
			key = log2ToTick(rec.cs.Ticks[0] + rec.cs.Ticks[1] + rec.cs.Ticks[2])
		}
		_ = hq.Push(key, rec.h, unsafe.Pointer(rec.cs))
	}

	for _, f := range fans {
		cs := f.State
		cs.Ticks[f.EdgeIdx] = tick
		key := log2ToTick(cs.Ticks[0] + cs.Ticks[1] + cs.Ticks[2])
		_ = f.Queue.Update(key, f.Handle, unsafe.Pointer(cs))
	}
}

// onProfitable is a no-op hook for profitable cycles.
//
//go:inline
func onProfitable(cs *CycleState) {
	_, _ = cs.Ticks, cs.Pairs
}

const (
	clamp   = 128.0
	scale   = 16.0
	maxTick = 4095
)

// log2ToTick maps a log2(ratio) in [-clamp, clamp] to [0, maxTick].
//
//go:inline
//go:nosplit
func log2ToTick(ratio float64) int64 {
	if ratio < -clamp {
		return 0
	}
	if ratio > clamp {
		return maxTick
	}
	tick := int64((ratio + clamp) * scale)
	if tick > maxTick {
		tick = maxTick
	}
	return tick
}

// RegisterRoute adds an extra core mapping for a PairID.
//
//go:inline
func RegisterRoute(pid PairID, core uint8) {
	pair2cores[pid] |= 1 << core
}

// DispatchUpdate transforms a LogView into TickUpdate and multicasts.
//
//go:inline
//go:nosplit
func DispatchUpdate(v *types.LogView) {
	pid := lookupPairID(v.Addr[addrHexStart:addrHexEnd])
	if pid == 0 {
		return
	}
	r0 := utils.LoadBE64(v.Data[24:])
	r1 := utils.LoadBE64(v.Data[56:])
	tick := fastuni.Log2ReserveRatio(r0, r1)

	var msg [32]byte
	upd := (*TickUpdate)(unsafe.Pointer(&msg))
	upd.Pair, upd.FwdTick, upd.RevTick = pid, tick, -tick

	for m := pair2cores[pid]; m != 0; {
		core := bits.TrailingZeros64(uint64(m))
		rings[core].Push(&msg)
		m &^= 1 << core
	}
}

// buildFanoutShards prepares PairShard slices per PairID.
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
			shardBucket[pid] = append(shardBucket[pid], PairShard{Pair: pid, Bins: bins[off:end]})
		}
	}
}

// shuffleBindings randomizes EdgeBinding order via crypto/rand.
//
//go:inline
func shuffleBindings(b []EdgeBinding) {
	for i := len(b) - 1; i > 0; i-- {
		j := crandInt(i + 1)
		b[i], b[j] = b[j], b[i]
	}
}

// crandInt returns a uniform int in [0,n) via crypto/rand.
//
//go:inline
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
