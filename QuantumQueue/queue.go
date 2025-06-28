// Package quantumqueue implements a branch-free, duplicate-tolerant,
// time-bucket priority queue with O(1) min lookup.
package quantumqueue

import (
	"math/bits"
	"unsafe"
)

/*──────────────── Tunables ───────────────*/

const (
	GroupCount  = 64
	LaneCount   = 64
	BucketCount = GroupCount * LaneCount * LaneCount // 262 144
	CapItems    = 1 << 16
)

/*──────────────── Error sentinels ─────────*/

type queueErr struct{ msg string }

func (e queueErr) Error() string { return e.msg }

var (
	ErrFull         = queueErr{"QuantumQueue: full"}
	ErrPastWindow   = queueErr{"QuantumQueue: past window"}
	ErrBeyondWindow = queueErr{"QuantumQueue: beyond window"}
	ErrNotFound     = queueErr{"QuantumQueue: not found"}
)

/*──────────────── Types ───────────────────*/

type idx32 uint32

const nilIdx idx32 = ^idx32(0)

type Handle idx32

type groupBlock struct {
	l1Summary uint64
	l2        [LaneCount]uint64
}

type node struct {
	tick  int64
	data  unsafe.Pointer
	next  idx32
	prev  idx32
	count uint32
}

type QuantumQueue struct {
	summary  uint64
	groups   [GroupCount]groupBlock
	buckets  [BucketCount]idx32
	arena    [CapItems]node
	freeHead idx32
	size     int
	baseTick uint64
}

/*──────────────── Construction ───────────*/

func NewQuantumQueue() *QuantumQueue {
	q := new(QuantumQueue)
	for i := CapItems - 1; i > 1; i-- {
		q.arena[i].next = idx32(i - 1)
	}
	q.arena[1].next = nilIdx
	q.freeHead = idx32(CapItems - 1)
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
	q.arena[h] = node{next: nilIdx, prev: nilIdx}
	return Handle(h), nil
}

/*──────────────── Helpers ────────────────*/

func validateDelta(delta uint64) uint {
	past := uint(delta >> 63)
	beyond := uint(((delta - BucketCount) >> 63) ^ 1&1)
	return past | (beyond << 1)
}

/*──────────────── Public API ─────────────*/

func (q *QuantumQueue) PeepMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := uint(bits.LeadingZeros64(q.summary))
	gb := &q.groups[g]
	lane := uint(bits.LeadingZeros64(gb.l1Summary))
	bit := uint(bits.LeadingZeros64(gb.l2[lane]))
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

	/*–– inline insertByDelta ––*/
	g, lane, bit := uint(delta>>12), uint((delta>>6)&0x3F), uint(delta&0x3F)
	n.next = q.buckets[delta]
	n.prev = nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx
	n.tick, n.data, n.count = tick, val, 1
	gb := &q.groups[g]
	gb.l2[lane] |= 1 << (63 - bit)
	gb.l1Summary |= 1 << (63 - lane)
	q.summary |= 1 << (63 - g)
	q.size++
	return nil
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

	if n.tick == tick { // same-tick patch
		n.data = val
		return nil
	}

	q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)

	/*–– inline insertByDelta ––*/
	g, lane, bit := uint(delta>>12), uint((delta>>6)&0x3F), uint(delta&0x3F)
	n.next = q.buckets[delta]
	n.prev = nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx
	n.tick, n.data = tick, val
	gb := &q.groups[g]
	gb.l2[lane] |= 1 << (63 - bit)
	gb.l1Summary |= 1 << (63 - lane)
	q.summary |= 1 << (63 - g)
	/* size already correct (unlink + relink) */
	return nil
}

func (q *QuantumQueue) PopMin() (Handle, int64, unsafe.Pointer) {
	h, t, v := q.PeepMin()
	if h == Handle(nilIdx) {
		return h, 0, nil
	}
	idx := idx32(h)
	n := &q.arena[idx]
	if n.count > 1 {
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

	// list unlink
	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
	if q.buckets[b] != nilIdx { // bucket not empty
		return
	}

	// bitmap clear
	l2Mask := uint64(1) << (63 - bit)
	l1Mask := uint64(1) << (63 - lane)
	gb := &q.groups[g]

	newL2 := gb.l2[lane] &^ l2Mask
	gb.l2[lane] = newL2
	if newL2 == 0 {
		gb.l1Summary &^= l1Mask
		if gb.l1Summary == 0 {
			q.summary &^= 1 << (63 - g)
		}
	}
}
