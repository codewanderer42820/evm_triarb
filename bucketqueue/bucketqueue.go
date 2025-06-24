// Package bucketqueue is an ultra-low-latency, zero-alloc time-bucket priority
// queue. Two-level bitmaps give O(1) PopMin. This version does the bare minimum
// work required to maintain correctness, with no heap use and pure tick-window logic.
package bucketqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

const (
	numBuckets       = 4096
	groupSize        = 64
	numGroups        = numBuckets / groupSize
	capItems         = 1 << 16
	nilIdx     idx32 = ^idx32(0)
)

var _ [-int(numBuckets & (numBuckets - 1))]byte
var _ [-int(capItems % numBuckets)]byte

type idx32 uint32

type node struct {
	next, prev idx32
	tick       int64
	count      uint32
	data       unsafe.Pointer
	_          [64 - 2*4 - 8 - 4 - unsafe.Sizeof(unsafe.Pointer(nil))]byte
}

type Queue struct {
	arena     [capItems]node
	freeHead  idx32
	buckets   [numBuckets]idx32
	baseTick  uint64
	size      int
	summary   uint64
	groupBits [numGroups]uint64
}

var (
	ErrFull         = errors.New("bucketqueue: no free handles")
	ErrEmpty        = errors.New("bucketqueue: empty queue")
	ErrPastWindow   = errors.New("bucketqueue: tick too far in the past")
	ErrBeyondWindow = errors.New("bucketqueue: tick too far in the future")
	ErrItemNotFound = errors.New("bucketqueue: invalid handle")
)

var ErrPastTick = ErrPastWindow

type Handle idx32

func New() *Queue {
	q := &Queue{}
	for i := capItems - 1; i > 0; i-- {
		q.arena[i-1].next = idx32(i)
	}
	q.arena[capItems-1].next = nilIdx
	q.freeHead = 0
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

func (q *Queue) Borrow() (Handle, error) {
	if q.freeHead == nilIdx {
		return Handle(nilIdx), ErrFull
	}
	h := q.freeHead
	n := &q.arena[h]
	q.freeHead = n.next
	n.next, n.prev, n.count = nilIdx, nilIdx, 0
	return Handle(h), nil
}

func (q *Queue) Return(h Handle) error {
	if idx32(h) >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[idx32(h)]
	n.next = q.freeHead
	n.prev = nilIdx
	n.count = 0
	n.data = nil
	q.freeHead = idx32(h)
	return nil
}

func (q *Queue) release(h idx32) {
	n := &q.arena[h]
	n.next = q.freeHead
	n.prev = nilIdx
	n.count = 0
	n.data = nil
	q.freeHead = h
}

//go:nosplit
func (q *Queue) Push(tick int64, h Handle, val unsafe.Pointer) error {
	if h >= Handle(capItems) {
		return ErrItemNotFound
	}
	delta := uint64(tick) - q.baseTick
	switch {
	case delta >= 1<<63:
		return ErrPastWindow
	case delta >= numBuckets:
		return ErrBeyondWindow
	}
	idx := idx32(h)
	n := &q.arena[idx]
	if n.count != 0 && n.tick == tick {
		n.count++
		q.size++
		return nil
	}
	if n.count != 0 {
		if n.prev != nilIdx {
			q.arena[n.prev].next = n.next
		} else {
			old := uint64(n.tick) - q.baseTick
			q.buckets[old] = n.next
		}
		if n.next != nilIdx {
			q.arena[n.next].prev = n.prev
		}
		q.size -= int(n.count)
		old := uint64(n.tick) - q.baseTick
		if q.buckets[old] == nilIdx {
			g := old >> 6
			q.groupBits[g] &^= 1 << (old & 63)
			if q.groupBits[g] == 0 {
				q.summary &^= 1 << g
			}
		}
		n.next, n.prev = nilIdx, nilIdx
	}
	bkt := delta
	n.next, n.prev = q.buckets[bkt], nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[bkt] = idx
	n.tick, n.count, n.data = tick, 1, val
	g := bkt >> 6
	q.groupBits[g] |= 1 << (bkt & 63)
	q.summary |= 1 << g
	q.size++
	return nil
}

//go:nosplit
func (q *Queue) Update(tick int64, h Handle, val unsafe.Pointer) error {
	if h >= Handle(capItems) {
		return ErrItemNotFound
	}
	idx := idx32(h)
	n := &q.arena[idx]
	if n.count == 0 {
		return ErrItemNotFound
	}
	old := uint64(n.tick) - q.baseTick
	q.size -= int(n.count)
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[old] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
	if q.buckets[old] == nilIdx {
		g := old >> 6
		q.groupBits[g] &^= 1 << (old & 63)
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << g
		}
	}
	n.next, n.prev, n.count = nilIdx, nilIdx, 0
	return q.Push(tick, h, val)
}

//go:nosplit
func (q *Queue) PeepMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	b := bits.TrailingZeros64(q.groupBits[g])
	bkt := uint64(g<<6 | b)
	n := &q.arena[q.buckets[bkt]]
	return Handle(q.buckets[bkt]), n.tick, n.data
}

//go:nosplit
func (q *Queue) PopMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	b := bits.TrailingZeros64(q.groupBits[g])
	bkt := uint64(g<<6 | b)
	h := q.buckets[bkt]
	n := &q.arena[h]
	if n.count > 1 {
		n.count--
		q.size--
		return Handle(h), n.tick, n.data
	}
	q.buckets[bkt] = n.next
	if n.next != nilIdx {
		q.arena[n.next].prev = nilIdx
	}
	q.size--
	q.groupBits[g] &^= 1 << (bkt & 63)
	if q.groupBits[g] == 0 {
		q.summary &^= 1 << g
	}
	q.release(h)
	return Handle(h), n.tick, n.data
}

func (q *Queue) recycleStaleBuckets() {
	for g, bits64 := range q.groupBits {
		for bits64 != 0 {
			b := bits.TrailingZeros64(bits64)
			bkt := uint64(g<<6 | b)
			for idx := q.buckets[bkt]; idx != nilIdx; {
				next := q.arena[idx].next
				q.release(idx)
				idx = next
			}
			q.buckets[bkt] = nilIdx
			bits64 &^= 1 << b
		}
		q.groupBits[g] = 0
	}
	q.summary = 0
	q.size = 0
}

func (q *Queue) Size() int   { return q.size }
func (q *Queue) Empty() bool { return q.size == 0 }
