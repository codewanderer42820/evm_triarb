// SPDX-Free: Public-Domain
// Ultra-low-latency QuantumQueue — footgun edition (no safety checks)
// • Tick range assumed valid
// • Handle assumed valid and in-use
// • Full speed, no guards — caller must enforce constraints

package quantumqueue

import (
	"math/bits"
)

const (
	GroupCount  = 64
	LaneCount   = 64
	BucketCount = GroupCount * LaneCount * LaneCount
	CapItems    = 1 << 14
)

type Handle uint32

const nilIdx Handle = ^Handle(0)

type idx32 = Handle

type node struct {
	tick  int64
	data  [44]byte
	next  Handle
	prev  Handle
	count uint32
	_     [4]byte
}

type groupBlock struct {
	l1Summary uint64
	l2        [LaneCount]uint64
	_         [448]byte
}

type QuantumQueue struct {
	summary  uint64
	baseTick uint64
	size     int
	freeHead Handle
	_        [6]byte

	buckets [BucketCount]Handle
	groups  [GroupCount]groupBlock
	arena   [CapItems]node
}

func NewQuantumQueue() *QuantumQueue {
	q := &QuantumQueue{freeHead: 0}
	for i := Handle(0); i < CapItems-1; i++ {
		q.arena[i].next = i + 1
	}
	q.arena[CapItems-1].next = nilIdx
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	q.freeHead = q.arena[h].next
	q.arena[h] = node{next: nilIdx, prev: nilIdx}
	return h, nil
}

func (q *QuantumQueue) Size() int   { return q.size }
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

func (q *QuantumQueue) Push(tick int64, h Handle, val []byte) {
	n := &q.arena[h]
	if n.count > 0 && n.tick == tick {
		copy(n.data[:], val)
		n.count++
		return
	}

	if n.count > 0 {
		deltaOld := uint64(n.tick - int64(q.baseTick))
		q.unlinkByIndex(h, deltaOld)
		q.size -= int(n.count)
	}

	delta := uint64(tick) - q.baseTick
	g := delta >> 12
	l := (delta >> 6) & 63
	b := delta & 63
	bucket := idx32((g << 12) | (l << 6) | b)

	n.next = q.buckets[bucket]
	if n.next != nilIdx {
		q.arena[n.next].prev = h
	}
	n.prev = nilIdx
	q.buckets[bucket] = h

	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - b)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)

	n.tick = tick
	n.count = 1
	copy(n.data[:], val)
	q.size++
}

func (q *QuantumQueue) Update(tick int64, h Handle, val []byte) {
	n := &q.arena[h]
	if n.tick == tick {
		copy(n.data[:], val)
		return
	}

	deltaOld := uint64(n.tick - int64(q.baseTick))
	q.unlinkByIndex(h, deltaOld)
	q.size -= int(n.count)

	delta := uint64(tick) - q.baseTick
	g := delta >> 12
	l := (delta >> 6) & 63
	b := delta & 63
	bucket := idx32((g << 12) | (l << 6) | b)

	n.next = q.buckets[bucket]
	if n.next != nilIdx {
		q.arena[n.next].prev = h
	}
	n.prev = nilIdx
	q.buckets[bucket] = h

	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - b)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)

	n.tick = tick
	n.count = 1
	copy(n.data[:], val)
	q.size++
}

func (q *QuantumQueue) PeepMin() (Handle, int64, []byte) {
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	b := bits.LeadingZeros64(gb.l2[l])
	bucket := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(b))
	idx := q.buckets[bucket]
	n := &q.arena[idx]
	return idx, n.tick, n.data[:]
}

func (q *QuantumQueue) PopMin() (Handle, int64, []byte) {
	h, t, v := q.PeepMin()
	delta := uint64(t - int64(q.baseTick))
	q.unlinkByIndex(h, delta)
	q.size--
	return h, t, v
}

func (q *QuantumQueue) unlinkByIndex(idx Handle, delta uint64) {
	g := delta >> 12
	l := (delta >> 6) & 63
	b := delta & 63
	bucket := idx32((g << 12) | (l << 6) | b)

	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[bucket] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	if q.buckets[bucket] == nilIdx {
		gb := &q.groups[g]
		gb.l2[l] &^= 1 << (63 - b)
		if gb.l2[l] == 0 {
			gb.l1Summary &^= 1 << (63 - l)
			if gb.l1Summary == 0 {
				q.summary &^= 1 << (63 - g)
			}
		}
	}
	n.count = 0
	n.next = q.freeHead
	q.freeHead = idx
}
