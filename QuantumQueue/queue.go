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

/*──────────────── Compact errors ─────────*/

type queueErr struct{ msg string }

func (e queueErr) Error() string { return e.msg }

var (
	ErrFull         = queueErr{"full"}
	ErrPastWindow   = queueErr{"past"}
	ErrBeyondWindow = queueErr{"future"}
	ErrNotFound     = queueErr{"notfound"}
)

/*──────────────── Value types ────────────*/

type idx32 uint32

const nilIdx idx32 = ^idx32(0)

type Handle idx32

/*──────────────── Packed structs ─────────*/

type node struct {
	tick  int64
	data  unsafe.Pointer
	count uint32
	next  idx32
	prev  idx32
	_     [4]byte
}

type groupBlock struct {
	l1Summary uint64
	l2        [LaneCount]uint64
}

type QuantumQueue struct {
	/* hot header */
	summary  uint64
	baseTick uint64
	size     int
	freeHead idx32
	_        [4]byte

	/* cold / bulky */
	groups  [GroupCount]groupBlock
	buckets [BucketCount]idx32
	arena   [CapItems]node
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

/*──────────────── Helpers ───────────────*/

// validateDelta categorises the 64-bit unsigned distance between
// tick and baseTick:
//
//	0 → inside window         0 ≤ delta < BucketCount
//	1 → past (MSB set)        delta’s MSB = 1
//	2 → beyond future window  delta ≥ BucketCount
func validateDelta(delta uint64) uint {
	past := uint(delta >> 63) // 1 if MSB set

	// If delta < BucketCount:  (delta - BucketCount) underflows, MSB == 1
	// If delta ≥ BucketCount: (delta - BucketCount) MSB == 0
	beyond := uint(((delta-BucketCount)>>63)^1) & 1

	return past | beyond<<1 // 0 / 1 / 2
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
	if n.count > 0 && n.tick == tick {
		n.count++
		n.data = val
		q.size++
		return nil
	}
	if n.count > 0 {
		q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)
		q.size -= int(n.count)
	}

	g, lane, bit := uint(delta>>12), uint((delta>>6)&0x3F), uint(delta&0x3F)

	n.next, n.prev = q.buckets[delta], nilIdx
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

	if n.tick == tick {
		n.data = val
		return nil
	}
	q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)

	g, lane, bit := uint(delta>>12), uint((delta>>6)&0x3F), uint(delta&0x3F)

	n.next, n.prev = q.buckets[delta], nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx
	n.tick, n.data = tick, val

	gb := &q.groups[g]
	gb.l2[lane] |= 1 << (63 - bit)
	gb.l1Summary |= 1 << (63 - lane)
	q.summary |= 1 << (63 - g)
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

/*──────────────── Stats ────────────────*/

func (q *QuantumQueue) Size() int   { return q.size }
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

/*──────────────── Internal unlink ───────*/

// unlinkByIndex removes idx from its bucket list and updates the three-level
// bitmap hierarchy.  After this call the invariants hold:
//
//   - if any buckets remain in lane  → lane-bit is 1
//   - if any lanes   remain in group → group-bit is 1
//   - otherwise both bits are 0
func (q *QuantumQueue) unlinkByIndex(idx idx32, delta uint64) {
	/*──── decode bucket coordinates ─────────────────────────────*/
	g := uint(delta >> 12)            // group   0‥63
	lane := uint((delta >> 6) & 0x3F) // lane    0‥63
	bit := uint(delta & 0x3F)         // bucket  0‥63
	bIdx := (g << 12) | (lane << 6) | bit

	/*──── unlink node from intrusive list ───────────────────────*/
	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[bIdx] = n.next // idx was head
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// bucket still non-empty → bitmap already correct
	if q.buckets[bIdx] != nilIdx {
		return
	}

	/*──── bucket became empty → update bitmaps ──────────────────*/
	gb := &q.groups[g]

	// 1) clear bucket bit in L2
	gb.l2[lane] &^= 1 << (63 - bit)

	// 2) set/clear lane bit in L1 depending on lane occupancy
	if gb.l2[lane] == 0 {
		gb.l1Summary &^= 1 << (63 - lane) // lane now empty
	} else {
		gb.l1Summary |= 1 << (63 - lane) // lane still has buckets
	}

	// 3) set/clear group bit in summary depending on group occupancy
	if gb.l1Summary == 0 {
		q.summary &^= 1 << (63 - g) // group empty
	} else {
		q.summary |= 1 << (63 - g) // group non-empty
	}
}
