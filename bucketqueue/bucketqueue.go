// bucketqueue.go — arena‑backed, zero‑alloc min‑priority queue (fixed window)
// Fully patched per 2025‑06‑24 review: size accounting bug, error consistency,
// unused parameter removal, 32‑bit generation counter, and wrap‑around guard.
// -----------------------------------------------------------------------------
package bucketqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

const (
	numBuckets = 1 << 12 // ring size (4096)
	groupSize  = 64      // bits per summary word
	numGroups  = numBuckets / groupSize
	capItems   = 4 * numBuckets // arena capacity (16 384)
)

type idx32 = int32

const nilIdx idx32 = -1

// Handle is an opaque index into the arena returned by Borrow() and accepted by
// all queue operations. It is *not* goroutine‑safe.
type Handle = int

var (
	ErrPastTick     = errors.New("bucketqueue: tick is before window")
	ErrItemNotFound = errors.New("bucketqueue: handle not in queue")
	ErrFull         = errors.New("bucketqueue: arena exhausted")
)

// node stores arena metadata and user pointer.
// Callers own the pointed‑to value’s lifetime; the queue never frees it.
type node struct {
	next, prev idx32
	bucketIdx  int32
	count      int32
	data       unsafe.Pointer
}

// Queue is a fixed‑capacity, zero‑allocation, sliding‑window min‑priority queue keyed by tick.
type Queue struct {
	arena     [capItems]node
	freeHead  idx32
	buckets   [numBuckets]idx32
	bucketGen [numBuckets]uint32
	groupBits [numGroups]uint64
	summary   uint64
	baseTick  uint64
	gen       uint32
	size      int
}

func New() *Queue {
	q := &Queue{freeHead: 0}
	for i := 0; i < capItems-1; i++ {
		q.arena[i].next = idx32(i + 1)
		q.arena[i].bucketIdx = -1
	}
	q.arena[capItems-1].next = nilIdx
	q.arena[capItems-1].bucketIdx = -1
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

func (q *Queue) Borrow() (Handle, error) {
	if q.freeHead == nilIdx {
		return -1, ErrFull
	}
	idx := q.freeHead
	q.freeHead = q.arena[idx].next
	return Handle(idx), nil
}

func (q *Queue) Return(h Handle) error {
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]
	if n.count != 0 {
		return errors.New("bucketqueue: cannot return active handle")
	}
	n.prev, n.bucketIdx, n.count = nilIdx, -1, 0
	n.next = q.freeHead
	q.freeHead = idx32(h)
	return nil
}

// Push inserts (or increments) a handle at a given tick.
func (q *Queue) Push(tick int64, h Handle, val unsafe.Pointer) error {
	if tick < int64(q.baseTick) {
		return ErrPastTick
	}
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]

	if n.count > 0 && n.bucketIdx >= 0 && q.bucketGen[n.bucketIdx] == q.gen {
		if int64(q.baseTick)+int64(n.bucketIdx) == tick {
			n.count++
			q.size++
			return nil
		}
		q.size -= int(n.count)
		q.detach(n)
	}

	if d := uint64(tick) - q.baseTick; d >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = uint64(tick)
		q.gen++
		if q.gen == 0 {
			for i := range q.bucketGen {
				q.bucketGen[i] = 0
			}
		}
		q.summary = 0
		q.groupBits = [numGroups]uint64{}
	}

	bkt := int(uint64(tick)-q.baseTick) & (numBuckets - 1)
	if q.bucketGen[bkt] != q.gen {
		q.bucketGen[bkt] = q.gen
		q.buckets[bkt] = nilIdx
	}

	head := q.buckets[bkt]
	n.next, n.prev = head, nilIdx
	n.bucketIdx = int32(bkt)
	if n.count == 0 {
		n.count = 1
	}
	n.data = val
	if head != nilIdx {
		q.arena[head].prev = idx32(h)
	}
	q.buckets[bkt] = idx32(h)

	g := bkt / groupSize
	bit := uint(bkt % groupSize)
	q.groupBits[g] |= 1 << bit
	q.summary |= 1 << uint(g)

	q.size += int(n.count)
	return nil
}

func (q *Queue) Update(tick int64, h Handle, val unsafe.Pointer) error {
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	if tick < int64(q.baseTick) {
		return ErrPastTick
	}
	n := &q.arena[h]
	if n.count == 0 || n.bucketIdx < 0 || q.bucketGen[n.bucketIdx] != q.gen {
		return ErrItemNotFound
	}
	if int64(q.baseTick)+int64(n.bucketIdx) == tick {
		return nil
	}
	q.size -= int(n.count)
	q.detach(n)
	return q.Push(tick, h, val)
}

func (q *Queue) Remove(h Handle) error {
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]
	if n.count == 0 || n.bucketIdx < 0 || q.bucketGen[n.bucketIdx] != q.gen {
		return ErrItemNotFound
	}
	q.detach(n)
	q.size -= int(n.count)
	n.prev, n.bucketIdx, n.count = nilIdx, -1, 0
	n.next = q.freeHead
	q.freeHead = idx32(h)
	return nil
}

func (q *Queue) PopMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	w := q.groupBits[g]
	b := bits.TrailingZeros64(w)
	bkt := g*groupSize + int(b)
	tick := int64(q.baseTick) + int64(bkt)

	idx := q.buckets[bkt]
	n := &q.arena[idx]

	if n.count > 1 {
		n.count--
		q.size--
		return Handle(idx), tick, n.data
	}

	next := n.next
	q.buckets[bkt] = next
	if next != nilIdx {
		q.arena[next].prev = nilIdx
	}
	if next == nilIdx {
		q.groupBits[g] &^= 1 << uint(b)
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << uint(g)
		}
	}

	q.size--
	data := n.data
	n.prev, n.bucketIdx, n.count = nilIdx, -1, 0
	n.next = q.freeHead
	q.freeHead = idx32(idx)
	return Handle(idx), tick, data
}

func (q *Queue) PeepMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	w := q.groupBits[g]
	b := bits.TrailingZeros64(w)
	bkt := g*groupSize + int(b)
	tick := int64(q.baseTick) + int64(bkt)
	idx := q.buckets[bkt]
	return Handle(idx), tick, q.arena[idx].data
}

func (q *Queue) Size() int { return q.size }

func (q *Queue) Empty() bool { return q.size == 0 }

// internal helpers
func (q *Queue) detach(n *node) {
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[n.bucketIdx] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
	if q.buckets[n.bucketIdx] == nilIdx {
		g := int(n.bucketIdx) / groupSize
		bit := uint(int(n.bucketIdx) % groupSize)
		q.groupBits[g] &^= 1 << bit
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << uint(g)
		}
	}
	n.next, n.prev, n.bucketIdx = nilIdx, nilIdx, -1
}

func (q *Queue) recycleStaleBuckets() {
	if q.size == 0 || q.summary == 0 {
		return
	}
	for q.summary != 0 {
		g := bits.TrailingZeros64(q.summary)
		w := q.groupBits[g]
		for w != 0 {
			b := bits.TrailingZeros64(w)
			bkt := g*groupSize + int(b)
			for idx := q.buckets[bkt]; idx != nilIdx; {
				nxt := q.arena[idx].next
				q.release(idx)
				idx = nxt
			}
			q.buckets[bkt] = nilIdx
			q.bucketGen[bkt] = 0
			w &^= 1 << uint(b)
		}
		q.groupBits[g] = 0
		q.summary &^= 1 << uint(g)
	}
	q.size = 0
}

func (q *Queue) release(i idx32) {
	n := &q.arena[i]
	n.prev, n.bucketIdx, n.count = nilIdx, -1, 0
	n.next = q.freeHead
	q.freeHead = i
}
