// bucketqueue.go — fully uint64-purified, cast-free, zero-alloc min-priority queue
package bucketqueue

import (
	"errors"
	"math"
	"math/bits"
	"unsafe"
)

const (
	numBuckets uint64 = 1 << 12 // ring size (4096)
	groupSize  uint64 = 64      // bits per summary word
	numGroups         = numBuckets / groupSize
	capItems   uint64 = 4 * numBuckets // arena capacity (16 384)
)

type idx = uint64

const nilIdx idx = math.MaxUint64

type Handle = uint64

var (
	ErrPastTick     = errors.New("bucketqueue: tick is before window")
	ErrItemNotFound = errors.New("bucketqueue: handle not in queue")
	ErrFull         = errors.New("bucketqueue: arena exhausted")
)

type node struct {
	next, prev idx            // 16 B
	bucketIdx  idx            //  8 B
	count      uint64         //  8 B
	data       unsafe.Pointer //  8 B
	_          [24]byte       // padding to 64 B
}

type Queue struct {
	arena     [capItems]node
	freeHead  idx
	buckets   [numBuckets]idx
	bucketGen [numBuckets]uint64
	groupBits [numGroups]uint64
	summary   uint64
	baseTick  uint64
	gen       uint64
	size      uint64
}

func New() *Queue {
	q := &Queue{freeHead: 0}
	for i := uint64(0); i < capItems-1; i++ {
		q.arena[i].next = i + 1
		q.arena[i].bucketIdx = nilIdx
	}
	q.arena[capItems-1].next = nilIdx
	q.arena[capItems-1].bucketIdx = nilIdx
	for i := uint64(0); i < numBuckets; i++ {
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
	if h >= capItems {
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

func (q *Queue) Push(tick uint64, h Handle, val unsafe.Pointer) error {
	if tick < q.baseTick {
		return ErrPastTick
	}
	if h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]

	if n.count > 0 && n.bucketIdx != nilIdx && q.bucketGen[n.bucketIdx] == q.gen {
		if q.baseTick+n.bucketIdx == tick {
			n.count++
			q.size++
			return nil
		}
		q.size -= n.count
		q.detach(n)
	}

	if delta := tick - q.baseTick; delta >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = tick
		if q.gen++; q.gen == 0 {
			q.bucketGen = [numBuckets]uint64{}
		}
		q.summary = 0
		q.groupBits = [numGroups]uint64{}
	}

	bkt := (tick - q.baseTick) & (numBuckets - 1)
	if q.bucketGen[bkt] != q.gen {
		q.bucketGen[bkt] = q.gen
		q.buckets[bkt] = nilIdx
	}
	head := q.buckets[bkt]
	n.next, n.prev = head, nilIdx
	n.bucketIdx = bkt
	if n.count == 0 {
		n.count = 1
	}
	n.data = val
	if head != nilIdx {
		q.arena[head].prev = h
	}
	q.buckets[bkt] = h

	g := bkt / groupSize
	bit := bkt % groupSize
	q.groupBits[g] |= 1 << bit
	q.summary |= 1 << g

	q.size += n.count
	return nil
}

func (q *Queue) Update(tick uint64, h Handle, val unsafe.Pointer) error {
	if h >= capItems {
		return ErrItemNotFound
	}
	if tick < q.baseTick {
		return ErrPastTick
	}
	n := &q.arena[h]
	if n.count == 0 || n.bucketIdx == nilIdx || q.bucketGen[n.bucketIdx] != q.gen {
		return ErrItemNotFound
	}
	if q.baseTick+n.bucketIdx == tick {
		return nil
	}
	q.size -= n.count
	q.detach(n)
	return q.Push(tick, h, val)
}

func (q *Queue) Remove(h Handle) error {
	if h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]
	if n.count == 0 || n.bucketIdx == nilIdx || q.bucketGen[n.bucketIdx] != q.gen {
		return ErrItemNotFound
	}
	q.detach(n)
	q.size -= n.count
	n.prev, n.bucketIdx, n.count = nilIdx, nilIdx, 0
	n.next = q.freeHead
	q.freeHead = h
	return nil
}

func (q *Queue) PopMin() (Handle, uint64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return nilIdx, 0, nil
	}
	g := uint64(bits.TrailingZeros64(q.summary))
	w := q.groupBits[g]
	b := uint64(bits.TrailingZeros64(w))
	bkt := g*groupSize + b
	tick := q.baseTick + bkt

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
		bit := bkt % groupSize
		q.groupBits[g2] &^= 1 << bit
		if q.groupBits[g2] == 0 {
			q.summary &^= 1 << g2
		}
	}

	q.size--
	data := n.data
	n.prev, n.bucketIdx, n.count = nilIdx, nilIdx, 0
	n.next = q.freeHead
	q.freeHead = i
	return i, tick, data
}

func (q *Queue) PeepMin() (Handle, uint64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return nilIdx, 0, nil
	}
	g := uint64(bits.TrailingZeros64(q.summary))
	w := q.groupBits[g]
	b := uint64(bits.TrailingZeros64(w))
	bkt := g*groupSize + b
	tick := q.baseTick + bkt
	i := q.buckets[bkt]
	return i, tick, q.arena[i].data
}

func (q *Queue) Size() uint64 { return q.size }
func (q *Queue) Empty() bool  { return q.size == 0 }

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
		g := n.bucketIdx / groupSize
		bit := n.bucketIdx % groupSize
		tmp := q.groupBits[g] &^ (1 << bit)
		q.groupBits[g] = tmp
		if tmp == 0 {
			q.summary &^= 1 << g
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
		g := uint64(bits.TrailingZeros64(summary))
		summary &^= 1 << g
		w := q.groupBits[g]
		q.groupBits[g] = 0
		for w != 0 {
			b := uint64(bits.TrailingZeros64(w))
			w &^= 1 << b
			bkt := g*groupSize + b
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
