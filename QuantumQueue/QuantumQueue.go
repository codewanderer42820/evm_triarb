package quantumqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

// ---------------------------------------------------------------------------
// QuantumQueue: Ultra-Optimized 3D Bitmap Scheduler (Group-Centric AoS + LZCNT + Branchless Clears)
// with low-overhead duplicate-tick handling
// - Each handle may be pushed multiple times to the same tick (count)
// - Fast-path for same-tick pushes avoids unlink/insert
// - 64 groups, each 512B cache-aligned block with reversed bitmaps
// - Three LeadingZeros64 calls for O(1) lookup
// - Branchless underflow trick for constant-time clears
// ---------------------------------------------------------------------------

const (
	GroupCount  = 64                                 // top-level groups
	LaneCount   = 64                                 // lanes per group
	BucketCount = GroupCount * LaneCount * LaneCount // 262,144 buckets
	CapItems    = 1 << 16                            // arena size
)

var (
	ErrFull         = errors.New("QuantumQueue: no free handles")
	ErrPastWindow   = errors.New("QuantumQueue: tick too far in the past")
	ErrBeyondWindow = errors.New("QuantumQueue: tick too far in the future")
	ErrNotFound     = errors.New("QuantumQueue: invalid handle or removed")
)

type idx32 uint32

const nilIdx idx32 = ^idx32(0)

type Handle idx32

// groupBlock: reversed-bitmaps per group, padded to 512B
type groupBlock struct {
	l1Summary uint64            // reversed: bit at pos (63-lane)
	_pad0     [7]uint64         // pad to 64B
	l2        [LaneCount]uint64 // reversed: bit at pos (63-bit)
	_pad1     [56]byte          // pad block to 512B
}

// node: per-handle metadata (hot fields first)
type node struct {
	tick  int64          // scheduled tick
	data  unsafe.Pointer // payload
	next  idx32          // next in bucket list
	prev  idx32          // prev in bucket list
	count uint32         // duplicate count for same-tick pushes
	_pad  uint32         // padding to 32 bytes
}

// QuantumQueue is the scheduler state.
type QuantumQueue struct {
	summary  uint64    // reversed global summary
	_padG    [7]uint64 // pad to 64B
	groups   [GroupCount]groupBlock
	buckets  [BucketCount]idx32 // head handle per bucket
	arena    [CapItems]node     // handle metadata pool
	freeHead idx32              // freelist head
	size     int                // entry count (includes duplicates)
	baseTick uint64             // offset for bucket 0
}

// NewQuantumQueue creates a zeroed, ready-to-use QuantumQueue.
func NewQuantumQueue() *QuantumQueue {
	q := new(QuantumQueue)
	// build freelist: indices CapItems-1 down to 1
	for i := CapItems - 1; i > 1; i-- {
		q.arena[i].next = idx32(i - 1)
	}
	// terminate at 1 so handle 0 is never given
	q.arena[1].next = nilIdx
	q.freeHead = idx32(CapItems - 1)
	return q
}

// Borrow retrieves a free handle.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		return Handle(nilIdx), ErrFull
	}
	n := &q.arena[h]
	q.freeHead = n.next
	// reset node
	n.next, n.prev, n.count = nilIdx, nilIdx, 0
	return Handle(h), nil
}

// peekIndexes: three LZCNTs on reversed bitmaps to find min bucket
//
//go:inline
//go:nosplit
func (q *QuantumQueue) peekIndexes() (g, lane, bit uint) {
	g = uint(bits.LeadingZeros64(q.summary))
	gb := &q.groups[g]
	lane = uint(bits.LeadingZeros64(gb.l1Summary))
	bit = uint(bits.LeadingZeros64(gb.l2[lane]))
	return
}

// PeepMin returns earliest handle, tick, and payload.
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

// Push enqueues handle h at tick with payload val.
// Fast-path for same-tick increments count.
//
//go:nosplit
func (q *QuantumQueue) Push(tick int64, h Handle, val unsafe.Pointer) error {
	idx := idx32(h)
	if idx == nilIdx || idx >= CapItems {
		return ErrNotFound
	}
	delta := uint64(tick) - q.baseTick
	switch {
	case delta >= 1<<63:
		return ErrPastWindow
	case delta >= BucketCount:
		return ErrBeyondWindow
	}
	n := &q.arena[idx]
	// same-tick fast path: bump count and update payload
	if n.count > 0 && n.tick == tick {
		n.count++
		n.data = val
		q.size++
		return nil
	}
	// unlink old if present
	if n.prev != nilIdx {
		old := uint64(n.tick) - q.baseTick
		q.unlinkByIndex(idx, old)
		q.size--
	}
	// insert new
	n.count = 1
	return q.insertByDelta(idx, tick, delta, val)
}

// PopMin removes and returns the earliest entry.
//
//go:nosplit
func (q *QuantumQueue) PopMin() (Handle, int64, unsafe.Pointer) {
	h, t, v := q.PeepMin()
	if h == Handle(nilIdx) {
		return h, 0, nil
	}
	idx := idx32(h)
	n := &q.arena[idx]
	// multi-count: just decrement
	if n.count > 1 {
		n.count--
		q.size--
		return h, t, n.data
	}
	// single: unlink and free
	delta := uint64(t) - q.baseTick
	q.unlinkByIndex(idx, delta)
	q.size--
	n.next = q.freeHead
	q.freeHead = idx
	return h, t, v
}

// Size returns entry count (includes duplicates).
//
//go:inline
//go:nosplit
func (q *QuantumQueue) Size() int { return q.size }

// Empty reports whether queue has no entries.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

// unlinkByIndex removes idx from bucket and updates bitmaps without branches.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) unlinkByIndex(idx idx32, delta uint64) {
	g := uint(delta >> 12)
	lane := uint((delta >> 6) & 0x3F)
	bit := uint(delta & 0x3F)
	b := (g << 12) | (lane << 6) | bit
	// unlink list
	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
	// branchless bitmap clears
	laneMask := uint64(1) << (63 - bit)
	slot := &q.groups[g]
	newL2 := slot.l2[lane] &^ laneMask
	slot.l2[lane] = newL2
	clearL1 := ((newL2 - 1) >> 63) & laneMask
	slot.l1Summary &^= clearL1
	l1 := slot.l1Summary
	gMask := uint64(1) << (63 - g)
	clearG := ((l1 - 1) >> 63) & gMask
	q.summary &^= clearG
}

// insertByDelta links idx into bucket and sets bitmaps (branchless).
//
//go:inline
//go:nosplit
func (q *QuantumQueue) insertByDelta(idx idx32, tick int64, delta uint64, val unsafe.Pointer) error {
	g := uint(delta >> 12)
	lane := uint((delta >> 6) & 0x3F)
	bit := uint(delta & 0x3F)
	// list insert
	n := &q.arena[idx]
	n.next = q.buckets[delta]
	n.prev = nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx
	// metadata
	n.tick = tick
	n.data = val
	// branchless bitmap sets
	laneMask := uint64(1) << (63 - bit)
	slot := &q.groups[g]
	slot.l2[lane] |= laneMask
	summaryMask := uint64(1) << (63 - lane)
	slot.l1Summary |= summaryMask
	q.summary |= uint64(1) << (63 - g)
	q.size++
	return nil
}
