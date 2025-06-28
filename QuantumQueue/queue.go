// SPDX‑Free: Public‑Domain
// Ultra‑low‑latency QuantumQueue — peak‑performance minimal version
// • Inline 44 B payload per node
// • Three‑LZCNT hot path, branch‑free bitmaps
// • Only essential sliding‑window & duplicate‑tick checks
package quantumqueue

import (
	"math/bits"
)

// error definitions
var (
	ErrFull         = queueErr{"full"}
	ErrPastWindow   = queueErr{"past"}
	ErrBeyondWindow = queueErr{"future"}
	ErrNotFound     = queueErr{"notfound"}
)

type queueErr struct{ msg string }

func (e queueErr) Error() string { return e.msg }

// constants
const (
	GroupCount  = 64
	LaneCount   = 64
	BucketCount = GroupCount * LaneCount * LaneCount // 262144
	CapItems    = 1 << 14
)

type Handle uint32

const nilIdx Handle = ^Handle(0)

type idx32 = Handle

type node struct {
	tick  int64    // 8 B
	count uint32   // 4 B
	prev  Handle   // 4 B
	next  Handle   // 4 B
	data  [44]byte // ← 44 B payload
}

type groupBlock struct {
	l1Summary uint64
	l2        [LaneCount]uint64
}

type QuantumQueue struct {
	summary  uint64
	baseTick uint64
	size     int
	freeHead Handle
	buckets  [BucketCount]Handle
	groups   [GroupCount]groupBlock
	arena    [CapItems]node
}

// NewQuantumQueue creates an empty queue.
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

// Borrow a handle from the free list.
func (q *QuantumQueue) Borrow() (Handle, error) {
	if q.freeHead == nilIdx {
		return nilIdx, ErrFull
	}
	h := q.freeHead
	q.freeHead = q.arena[h].next
	q.arena[h] = node{next: nilIdx, prev: nilIdx} // resets node
	return h, nil
}

func (q *QuantumQueue) Size() int   { return q.size }
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

// Push inserts or duplicates a handle at tick with payload.
func (q *QuantumQueue) Push(tick int64, h Handle, val []byte) error {
	if h >= CapItems {
		return ErrNotFound
	}
	idx := h
	n := &q.arena[idx]
	if n.count > 0 && n.tick == tick {
		copy(n.data[:], val)
		n.count++
		return nil
	}
	delta := uint64(tick) - q.baseTick
	if delta>>63 == 1 {
		return ErrPastWindow
	} else if (delta-BucketCount)>>63 == 0 {
		return ErrBeyondWindow
	}
	if n.count > 0 {
		q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)
		q.size -= int(n.count)
	}
	g := delta >> 12
	l := (delta >> 6) & 0x3F
	b := delta & 0x3F
	bucket := idx32((g << 12) | (l << 6) | b)
	n.next = q.buckets[bucket]
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	n.prev = nilIdx
	q.buckets[bucket] = idx
	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - b)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)
	n.tick = tick
	n.count = 1
	copy(n.data[:], val)
	q.size++
	return nil
}

// Update moves an active handle to a new tick with new payload or duplicates.
func (q *QuantumQueue) Update(tick int64, h Handle, val []byte) error {
	if h >= CapItems {
		return ErrNotFound
	}
	idx := h
	n := &q.arena[idx]
	if n.count == 0 {
		return ErrNotFound
	}
	if n.tick == tick {
		copy(n.data[:], val)
		return nil
	}
	q.unlinkByIndex(idx, uint64(n.tick)-q.baseTick)
	q.size -= int(n.count)
	delta := uint64(tick) - q.baseTick
	if delta>>63 == 1 {
		return ErrPastWindow
	} else if (delta-BucketCount)>>63 == 0 {
		return ErrBeyondWindow
	}
	g := delta >> 12
	l := (delta >> 6) & 0x3F
	b := delta & 0x3F
	bucket := idx32((g << 12) | (l << 6) | b)
	n.next = q.buckets[bucket]
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	n.prev = nilIdx
	q.buckets[bucket] = idx
	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - b)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)
	n.tick = tick
	n.count = 1
	copy(n.data[:], val)
	q.size++
	return nil
}

// PeepMin returns the minimum without removing.
func (q *QuantumQueue) PeepMin() (Handle, int64, []byte) {
	if q.size == 0 || q.summary == 0 {
		return nilIdx, 0, nil
	}
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	b := bits.LeadingZeros64(gb.l2[l])
	bucket := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(b))
	idx := q.buckets[bucket]
	n := &q.arena[idx]
	return idx, n.tick, n.data[:]
}

// PopMin removes and returns the minimum handle/value.
func (q *QuantumQueue) PopMin() (Handle, int64, []byte) {
	h, t, v := q.PeepMin()
	if h == nilIdx {
		return nilIdx, 0, nil
	}
	delta := uint64(t) - q.baseTick
	q.unlinkByIndex(h, delta)
	q.size--
	return h, t, v
}

// unlinkByIndex detaches a node from its bucket and clears bitmaps.
func (q *QuantumQueue) unlinkByIndex(idx Handle, delta uint64) {
	g := delta >> 12
	l := (delta >> 6) & 0x3F
	b := delta & 0x3F
	bucket := idx32((g << 12) | (l << 6) | b)
	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[bucket] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
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
