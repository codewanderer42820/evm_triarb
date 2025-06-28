// SPDX-Free:  Public-Domain
// Ultra-low-latency QuantumQueue (final tuned edition)
package quantumqueue

import (
	"math/bits"
	"unsafe"
)

const (
	GroupCount  = 64
	LaneCount   = 64
	BucketCount = GroupCount * LaneCount * LaneCount // 262 144
	CapItems    = 1 << 16
)

/*──────── errors (compact) ───────*/
type queueErr struct{ msg string }

func (e queueErr) Error() string { return e.msg }

var (
	ErrFull         = queueErr{"full"}
	ErrPastWindow   = queueErr{"past"}
	ErrBeyondWindow = queueErr{"future"}
	ErrNotFound     = queueErr{"notfound"}
)

/*──────── tiny types ─────────────*/
type idx32 uint32

const nilIdx idx32 = ^idx32(0)

type Handle idx32

/*──────── packed structs ─────────*/
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
	// hot header (fits in one 64 B line)
	summary, baseTick uint64
	size              int
	freeHead          idx32
	_                 [4]byte

	// cold
	groups  [GroupCount]groupBlock
	buckets [BucketCount]idx32
	arena   [CapItems]node
}

/*──────── construction ───────────*/
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

/*──────── freelist ───────────────*/
func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		return Handle(nilIdx), ErrFull
	}
	q.freeHead = q.arena[h].next
	q.arena[h] = node{next: nilIdx, prev: nilIdx}
	return Handle(h), nil
}

/*──────── helpers ───────────────*/
//go:nosplit
func validateDelta(d uint64) uint {
	past := uint(d >> 63)
	beyond := uint(^((d - BucketCount) >> 63) & 1) // 1 if ≥BucketCount
	return past | beyond<<1
}

/*──────── nano-hot path: min lookup ─────────*/

//go:inline
//go:nosplit
func (q *QuantumQueue) PeepMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 {
		return Handle(nilIdx), 0, nil
	}

	g := uint(bits.LeadingZeros64(q.summary)) // 0‥63 (summary ≠ 0)
	gb := &q.groups[g]
	l := uint(bits.LeadingZeros64(gb.l1Summary))
	b := uint(bits.LeadingZeros64(gb.l2[l]))
	idx := (g << 12) | (l << 6) | b

	h := q.buckets[idx]
	n := &q.arena[h]
	return Handle(h), n.tick, n.data
}

/*──────── Push ───────────────────*/

//go:inline
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
	if n.count > 0 { // relocate
		q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)
		q.size -= int(n.count)
	}

	g, l, b := uint(delta>>12), uint((delta>>6)&0x3F), uint(delta&0x3F)
	n.next, n.prev = q.buckets[delta], nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx
	n.tick, n.data, n.count = tick, val, 1

	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - b)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)
	q.size++
	return nil
}

/*──────── Update ─────────────────*/

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

	if n.tick == tick {
		n.data = val
		return nil
	}

	q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)

	g, l, b := uint(delta>>12), uint((delta>>6)&0x3F), uint(delta&0x3F)
	n.next, n.prev = q.buckets[delta], nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx
	n.tick, n.data = tick, val

	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - b)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)
	return nil
}

/*──────── PopMin ─────────────────*/

//go:inline
//go:nosplit
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

/*──────── stats ─────────────*/
func (q *QuantumQueue) Size() int   { return q.size }
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

/*──────── unlink & bitmap maintenance ─────*/

//go:inline
//go:nosplit
func (q *QuantumQueue) unlinkByIndex(idx idx32, delta uint64) {
	g := uint(delta >> 12)
	l := uint((delta >> 6) & 0x3F)
	b := uint(delta & 0x3F)
	bIdx := (g << 12) | (l << 6) | b

	// list unlink
	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[bIdx] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	if q.buckets[bIdx] != nilIdx {
		return
	}

	gb := &q.groups[g]
	gb.l2[l] &^= 1 << (63 - b) // clear bucket bit

	// synchronise lane bit
	if gb.l2[l] == 0 {
		gb.l1Summary &^= 1 << (63 - l)
	} else {
		gb.l1Summary |= 1 << (63 - l)
	}

	// synchronise group bit
	if gb.l1Summary == 0 {
		q.summary &^= 1 << (63 - g)
	} else {
		q.summary |= 1 << (63 - g)
	}
}
