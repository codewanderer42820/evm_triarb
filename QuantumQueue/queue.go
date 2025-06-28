// Package quantumqueue implements a branch-free, duplicate-tolerant,
// time-bucket priority queue with O(1) min lookup.  It is *single-threaded*;
// shard or wrap it externally if you need concurrency.
package quantumqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

/*──────────────── Tunables ───────────────*/

const (
	GroupCount  = 64                                 // top-level groups
	LaneCount   = 64                                 // lanes per group
	BucketCount = GroupCount * LaneCount * LaneCount // 64³ = 262 144 buckets
	CapItems    = 1 << 16                            // arena / freelist capacity
)

/*──────────────── Errors ──────────────────*/

var (
	ErrFull         = errors.New("QuantumQueue: no free handles")
	ErrPastWindow   = errors.New("QuantumQueue: tick too far in the past")
	ErrBeyondWindow = errors.New("QuantumQueue: tick too far in the future")
	ErrNotFound     = errors.New("QuantumQueue: invalid handle or removed")
)

/*──────────────── Low-level types ─────────*/

type idx32 uint32

const nilIdx idx32 = ^idx32(0) // −1 sentinel

// Handle is the opaque ID returned to callers.
type Handle idx32

/*──────────────── Internal structs ────────*/

type groupBlock struct {
	l1Summary uint64            // bit 63-lane == 1  → lane non-empty
	l2        [LaneCount]uint64 // per-lane bucket bitmaps (reversed)
}

type node struct {
	tick  int64
	data  unsafe.Pointer
	next  idx32
	prev  idx32
	count uint32 // duplicate counter
}

/*──────────────── Queue ───────────────────*/

type QuantumQueue struct {
	summary  uint64
	groups   [GroupCount]groupBlock
	buckets  [BucketCount]idx32
	arena    [CapItems]node
	freeHead idx32
	size     int
	baseTick uint64 // sliding-window origin (0 for now)
}

/*──────────────── Construction ───────────*/

func NewQuantumQueue() *QuantumQueue {
	q := new(QuantumQueue)

	// Build freelist: CapItems-1 → 1  (index 0 and nilIdx never given out)
	for i := CapItems - 1; i > 1; i-- {
		q.arena[i].next = idx32(i - 1)
	}
	q.arena[1].next = nilIdx
	q.freeHead = idx32(CapItems - 1)

	// All bucket heads start at nilIdx (not zero)
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

/*──────────────── Borrow / Free ──────────*/

func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		return Handle(nilIdx), ErrFull
	}
	q.freeHead = q.arena[h].next
	q.arena[h] = node{next: nilIdx, prev: nilIdx} // zero hot fields
	return Handle(h), nil
}

/*──────────────── Helpers ────────────────*/

func (q *QuantumQueue) peekIndexes() (g, lane, bit uint) {
	g = uint(bits.LeadingZeros64(q.summary))
	gb := &q.groups[g]
	lane = uint(bits.LeadingZeros64(gb.l1Summary))
	bit = uint(bits.LeadingZeros64(gb.l2[lane]))
	return
}

func validateDelta(delta uint64) uint {
	past := uint(delta >> 63) // MSB → 1 if past
	beyond := uint(((delta-BucketCount)>>63)^1) & 1
	return past | (beyond << 1) // 0 ok, 1 past, 2 beyond
}

/*──────────────── Public API ─────────────*/

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
	if n.count > 0 && n.tick == tick { // duplicate fast path
		n.count++
		n.data = val
		q.size++
		return nil
	}
	if n.count > 0 { // relocate
		q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)
		q.size -= int(n.count)
	}
	n.count = 1
	return q.insertByDelta(idx, tick, delta, val)
}

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

	if n.tick == tick { // same-tick → patch payload only
		n.data = val
		return nil
	}

	q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)
	if err := q.insertByDelta(idx, tick, delta, val); err != nil {
		return err
	}
	q.size-- // compensate internal +1
	return nil
}

func (q *QuantumQueue) PopMin() (Handle, int64, unsafe.Pointer) {
	h, t, v := q.PeepMin()
	if h == Handle(nilIdx) {
		return h, 0, nil
	}
	idx := idx32(h)
	n := &q.arena[idx]

	if n.count > 1 { // duplicate decrement
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

/*──────────────── Small helpers ──────────*/

func (q *QuantumQueue) Size() int   { return q.size }
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

/*──────────────── Internals ──────────────*/

func (q *QuantumQueue) unlinkByIndex(idx idx32, delta uint64) {
	g := uint(delta >> 12)
	lane := uint((delta >> 6) & 0x3F)
	bit := uint(delta & 0x3F)
	b := (g << 12) | (lane << 6) | bit

	// linked-list removal
	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// bucket not empty → keep bitmap bits
	if q.buckets[b] != nilIdx {
		return
	}

	// clear bitmap hierarchy
	l2Mask := uint64(1) << (63 - bit)
	l1Mask := uint64(1) << (63 - lane)
	gb := &q.groups[g]

	newL2 := gb.l2[lane] &^ l2Mask
	gb.l2[lane] = newL2

	gb.l1Summary &^= ((newL2 - 1) >> 63) & l1Mask

	l1 := gb.l1Summary
	gMask := uint64(1) << (63 - g)
	q.summary &^= ((l1 - 1) >> 63) & gMask
}

func (q *QuantumQueue) insertByDelta(idx idx32, tick int64, delta uint64, val unsafe.Pointer) error {
	g := uint(delta >> 12)
	lane := uint((delta >> 6) & 0x3F)
	bit := uint(delta & 0x3F)

	// linked-list prepend
	n := &q.arena[idx]
	n.next = q.buckets[delta]
	n.prev = nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx

	n.tick = tick
	n.data = val

	// bitmap sets
	gb := &q.groups[g]
	gb.l2[lane] |= uint64(1) << (63 - bit)
	gb.l1Summary |= uint64(1) << (63 - lane)
	q.summary |= uint64(1) << (63 - g)

	q.size++
	return nil
}
