// SPDX: UNLICENSED ─ Ultra-Low-Latency QuantumQueue
//
// QuantumQueue is a branch-free, cache-aware, duplicate-tolerant,
// time-bucket priority queue optimised for nanosecond-scale schedulers,
// HFT order books, and simulation kernels.
//
//  Window           : 2²⁶ buckets  (262 144 logical ticks)
//  Hierarchy        : 3-level reversed bitmaps
//      summary      : 64-bit ➜ non-empty group
//      l1Summary    : 64-bit ➜ non-empty lane
//      l2[lane]     : 64-bit ➜ non-empty bucket
//  Min lookup       : 3 × LZCNT  →  O(1)
//  Group block size : 576 B (9 × 64 B cache lines)
//  Duplicate ticks  : node.count > 1 (cheap counter bump)
//  All node & bitmap ops are branch-free except for API validation.

package quantumqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

/*──────────────────── Tunables ────────────────────*/

const (
	GroupCount  = 64                                 // top-level groups
	LaneCount   = 64                                 // lanes per group
	BucketCount = GroupCount * LaneCount * LaneCount // 64³ = 262 144
	CapItems    = 1 << 16                            // arena capacity
)

/*──────────────────── Error Surface ───────────────*/

var (
	ErrFull         = errors.New("QuantumQueue: no free handles")
	ErrPastWindow   = errors.New("QuantumQueue: tick too far in the past")
	ErrBeyondWindow = errors.New("QuantumQueue: tick too far in the future")
	ErrNotFound     = errors.New("QuantumQueue: invalid handle or removed")
)

/*──────────────────── Low-level Types ─────────────*/

type idx32 uint32

const nilIdx idx32 = ^idx32(0)

// Handle is the opaque identifier returned to callers.
type Handle idx32

// groupBlock packs l1 + l2 bitmaps into 9 cache lines.
type groupBlock struct {
	l1Summary uint64            // bit (63-lane) = 1 ➜ lane non-empty
	_pad0     [7]uint64         // pad to 64 B
	l2        [LaneCount]uint64 // bucket bitmaps (reversed)
}

// node holds per-handle metadata (32 B hot layout).
type node struct {
	tick  int64          // scheduled tick
	data  unsafe.Pointer // payload
	next  idx32          // next in bucket list
	prev  idx32          // previous in bucket list
	count uint32         // duplicate counter
	_pad  uint32         // align to 32 B
}

/*──────────────────── Queue Structure ─────────────*/

type QuantumQueue struct {
	// Hot (1 cache line)
	summary uint64
	_padG   [7]uint64

	// Warm
	groups [GroupCount]groupBlock

	// Cold
	buckets  [BucketCount]idx32
	arena    [CapItems]node
	freeHead idx32
	size     int
	baseTick uint64
}

/*──────────────────── Construction ───────────────*/

func NewQuantumQueue() *QuantumQueue {
	q := new(QuantumQueue)
	for i := CapItems - 1; i > 1; i-- {
		q.arena[i].next = idx32(i - 1)
	}
	q.arena[1].next = nilIdx
	q.freeHead = idx32(CapItems - 1)
	return q
}

/*──────────────────── Borrow / Free ──────────────*/

//go:inline
//go:nosplit
func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		return Handle(nilIdx), ErrFull
	}
	q.freeHead = q.arena[h].next
	n := &q.arena[h]
	n.next, n.prev, n.count = nilIdx, nilIdx, 0
	return Handle(h), nil
}

/*──────────────────── Min-lookup ──────────────────*/

//go:inline
//go:nosplit
func (q *QuantumQueue) peekIndexes() (g, lane, bit uint) {
	g = uint(bits.LeadingZeros64(q.summary))
	gb := &q.groups[g]
	lane = uint(bits.LeadingZeros64(gb.l1Summary))
	bit = uint(bits.LeadingZeros64(gb.l2[lane]))
	return
}

// PeepMin – O(1) read-only peek.
func (q *QuantumQueue) PeepMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 {
		return Handle(nilIdx), 0, nil
	}
	g, lane, bit := q.peekIndexes()
	b := (g << 12) | (lane << 6) | bit
	h := q.buckets[b]
	n := &q.arena[h]
	return Handle(h), n.tick, n.data
}

/*──────────────────── Delta Validation ───────────*/

// validateDelta maps delta to {0 ok, 1 past, 2 beyond} branch-free.
//
//go:inline
//go:nosplit
func validateDelta(delta uint64) uint {
	past := uint((delta >> 63) & 1)        // MSB set?
	diff := delta - BucketCount            // wrap uint
	beyond := uint(((diff >> 63) ^ 1) & 1) // invert
	return past | (beyond << 1)
}

/*──────────────────── Public API ──────────────────*/

//go:nosplit
func (q *QuantumQueue) Push(tick int64, h Handle, val unsafe.Pointer) error {
	idx := idx32(h)
	if idx == nilIdx || idx >= CapItems {
		return ErrNotFound
	}
	delta := uint64(tick) - q.baseTick
	switch validateDelta(delta) {
	case 1:
		return ErrPastWindow
	case 2:
		return ErrBeyondWindow
	}

	n := &q.arena[idx]
	if n.count > 0 && n.tick == tick { // duplicate fast-path
		n.count++
		n.data = val
		q.size++
		return nil
	}
	if n.count > 0 { // moving existing handle
		q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)
		q.size -= int(n.count)
	}
	n.count = 1
	return q.insertByDelta(idx, tick, delta, val)
}

//go:inline
//go:nosplit
func (q *QuantumQueue) Update(tick int64, h Handle, val unsafe.Pointer) error {
	idx := idx32(h)
	if idx == nilIdx || idx >= CapItems {
		return ErrNotFound
	}
	n := &q.arena[idx]
	if n.count == 0 {
		return ErrNotFound
	}

	delta := uint64(tick) - q.baseTick
	switch validateDelta(delta) {
	case 1:
		return ErrPastWindow
	case 2:
		return ErrBeyondWindow
	}

	if n.tick == tick { // same-tick payload patch
		n.data = val
		return nil
	}

	q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)
	if err := q.insertByDelta(idx, tick, delta, val); err != nil {
		return err
	}
	q.size-- // compensate insertByDelta internal +1
	return nil
}

//go:nosplit
func (q *QuantumQueue) PopMin() (Handle, int64, unsafe.Pointer) {
	h, t, v := q.PeepMin()
	if h == Handle(nilIdx) {
		return h, 0, nil
	}
	idx := idx32(h)
	n := &q.arena[idx]

	if n.count > 1 { // cheap duplicate decrement
		n.count--
		q.size--
		return h, t, n.data
	}

	q.unlinkByIndex(idx, uint64(t)-q.baseTick)
	q.size--
	n.next = q.freeHead
	q.freeHead = idx
	return h, t, v
}

//go:inline
//go:nosplit
func (q *QuantumQueue) Size() int { return q.size }

//go:inline
//go:nosplit
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

/*──────────────────── Internals ───────────────────*/

//go:inline
//go:nosplit
func (q *QuantumQueue) unlinkByIndex(idx idx32, delta uint64) {
	g, lane, bit := uint(delta>>12), uint((delta>>6)&0x3F), uint(delta&0x3F)
	b := (g << 12) | (lane << 6) | bit

	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	l2Mask := uint64(1) << (63 - bit)
	gb := &q.groups[g]

	newL2 := gb.l2[lane] &^ l2Mask
	gb.l2[lane] = newL2

	clearL1 := ((newL2 - 1) >> 63) & l2Mask
	gb.l1Summary &^= clearL1

	l1 := gb.l1Summary
	gMask := uint64(1) << (63 - g)
	clearG := ((l1 - 1) >> 63) & gMask
	q.summary &^= clearG
}

//go:inline
//go:nosplit
func (q *QuantumQueue) insertByDelta(idx idx32, tick int64, delta uint64, val unsafe.Pointer) error {
	g, lane, bit := uint(delta>>12), uint((delta>>6)&0x3F), uint(delta&0x3F)

	n := &q.arena[idx]
	n.next = q.buckets[delta]
	n.prev = nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx

	n.tick = tick
	n.data = val

	gb := &q.groups[g]
	l2Mask := uint64(1) << (63 - bit)
	l1Mask := uint64(1) << (63 - lane)
	gMask := uint64(1) << (63 - g)

	gb.l2[lane] |= l2Mask
	gb.l1Summary |= l1Mask
	q.summary |= gMask

	q.size++
	return nil
}
