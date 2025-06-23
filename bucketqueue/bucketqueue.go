// bucketqueue.go — arena-backed, zero-alloc min-priority queue (fixed window)
// 2025-06-24: 64-bit edition (cast-free)

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
	capItems   = 4 * numBuckets // arena capacity (16 384)
)

type idx = int64

const nilIdx idx = -1

type Handle = int64

var (
	ErrPastTick     = errors.New("bucketqueue: tick is before window")
	ErrItemNotFound = errors.New("bucketqueue: handle not in queue")
	ErrFull         = errors.New("bucketqueue: arena exhausted")
)

//go:align 64
type node struct {
	next, prev idx
	bucketIdx  idx
	count      int
	data       unsafe.Pointer
}

type Queue struct {
	arena     [capItems]node
	freeHead  idx
	buckets   [numBuckets]idx
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
		q.arena[i].next = int64(i + 1)
		q.arena[i].bucketIdx = nilIdx
	}
	q.arena[capItems-1].next = nilIdx
	q.arena[capItems-1].bucketIdx = nilIdx
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

func (q *Queue) Borrow() (Handle, error) {
	if q.freeHead == nilIdx {
		return nilIdx, ErrFull
	}
	i := q.freeHead
	q.freeHead = q.arena[i].next
	return i, nil
}

func (q *Queue) Return(h Handle) error {
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]
	if n.count != 0 {
		return errors.New("bucketqueue: cannot return active handle")
	}
	n.prev, n.bucketIdx, n.count = nilIdx, nilIdx, 0
	n.next = q.freeHead
	q.freeHead = h
	return nil
}

func (q *Queue) Push(tick int64, h Handle, val unsafe.Pointer) error {
	if tick < int64(q.baseTick) {
		return ErrPastTick
	}
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]

	if n.count > 0 && n.bucketIdx != nilIdx && q.bucketGen[n.bucketIdx] == q.gen {
		if int64(q.baseTick)+int64(n.bucketIdx) == tick {
			n.count++
			q.size++
			return nil
		}
		q.size -= n.count
		q.detach(n)
	}

	if delta := uint64(tick) - q.baseTick; delta >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = uint64(tick)
		if q.gen++; q.gen == 0 {
			q.bucketGen = [numBuckets]uint32{}
		}
		q.summary, q.groupBits = 0, [numGroups]uint64{}
	}

	bkt := int((uint64(tick) - q.baseTick) & (numBuckets - 1))
	if q.bucketGen[bkt] != q.gen {
		q.bucketGen[bkt] = q.gen
		q.buckets[bkt] = nilIdx
	}
	head := q.buckets[bkt]
	n.next, n.prev = head, nilIdx
	n.bucketIdx = int64(bkt)
	if n.count == 0 {
		n.count = 1
	}
	n.data = val
	if head != nilIdx {
		q.arena[head].prev = h
	}
	q.buckets[bkt] = h

	g := bkt / groupSize
	bit := uint(bkt % groupSize)
	q.groupBits[g] |= 1 << bit
	q.summary |= 1 << uint(g)

	q.size += n.count
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
	q.size -= n.count
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
	q.size -= n.count
	n.prev, n.bucketIdx, n.count = nilIdx, nilIdx, 0
	n.next = q.freeHead
	q.freeHead = h
	return nil
}

func (q *Queue) PopMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return nilIdx, 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	w := q.groupBits[g]
	b := bits.TrailingZeros64(w)
	bkt := g*groupSize + int(b)
	tick := int64(q.baseTick) + int64(bkt)

	i := q.buckets[bkt]
	n := &q.arena[i]

	if n.count > 1 {
		n.count--
		q.size--
		return i, tick, n.data
	}

	next := n.next
	q.buckets[bkt] = next
	if next != nilIdx {
		q.arena[next].prev = nilIdx
	}
	if next == nilIdx {
		g2 := bkt / groupSize
		q.groupBits[g2] &^= 1 << uint(b)
		if q.groupBits[g2] == 0 {
			q.summary &^= 1 << uint(g2)
		}
	}

	q.size--
	data := n.data
	n.prev, n.bucketIdx, n.count = nilIdx, nilIdx, 0
	n.next = q.freeHead
	q.freeHead = i
	return i, tick, data
}

func (q *Queue) PeepMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return nilIdx, 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	w := q.groupBits[g]
	b := bits.TrailingZeros64(w)
	bkt := g*groupSize + int(b)
	tick := int64(q.baseTick) + int64(bkt)
	i := q.buckets[bkt]
	return i, tick, q.arena[i].data
}

func (q *Queue) Size() int   { return q.size }
func (q *Queue) Empty() bool { return q.size == 0 }

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
		if q.groupBits[g]&^(1<<bit) == 0 {
			q.groupBits[g] = 0
			q.summary &^= 1 << uint(g)
		} else {
			q.groupBits[g] &^= 1 << bit
		}
	}
	n.next, n.prev, n.bucketIdx = nilIdx, nilIdx, nilIdx
}

func (q *Queue) recycleStaleBuckets() {
	if q.size == 0 || q.summary == 0 {
		return
	}
	summary := q.summary
	for summary != 0 {
		g := bits.TrailingZeros64(summary)
		summary &^= 1 << uint(g)
		w := q.groupBits[g]
		q.groupBits[g] = 0
		for w != 0 {
			b := bits.TrailingZeros64(w)
			w &^= 1 << uint(b)
			bkt := g*groupSize + int(b)
			for i := q.buckets[bkt]; i != nilIdx; {
				nxt := q.arena[i].next
				q.release(i)
				i = nxt
			}
			q.buckets[bkt] = nilIdx
		}
	}
	q.summary = 0
	q.size = 0
}

func (q *Queue) release(i idx) {
	n := &q.arena[i]
	n.prev, n.bucketIdx, n.count = nilIdx, nilIdx, 0
	n.next = q.freeHead
	q.freeHead = i
}
