// SPDX-Free: Public-Domain
// Ultra-low-latency QuantumQueue — field-aligned and padded for optimal access
// • Struct layout re-ordered to match hot-path access patterns
// • Arena nodes padded for cacheline alignment and minimized false sharing
// • Fast field access (tick, data, next) grouped early in structs

package quantumqueue

import (
	"math/bits"
)

const (
	GroupCount  = 64
	LaneCount   = 64
	BucketCount = GroupCount * LaneCount * LaneCount // 262144
	CapItems    = 1 << 14                            // 16384
)

type Handle uint32

const nilIdx Handle = ^Handle(0)

type idx32 = Handle

type node struct {
	// Hot fields first for PopMin/Push/Update
	tick  int64    // 8 B - used in delta calc
	data  [44]byte // 44 B - value payload
	next  Handle   // 4 B - used for traversal
	prev  Handle   // 4 B - used for unlink
	count uint32   // 4 B - rarely used, after fast fields
	_     [4]byte  // pad to 64 B cacheline total
}

type groupBlock struct {
	l1Summary uint64            // hot-path access
	l2        [LaneCount]uint64 // sparse
	_         [448]byte         // pad to 512 B block size
}

type QuantumQueue struct {
	// Fast path summary
	summary  uint64
	baseTick uint64
	size     int
	freeHead Handle
	_        [6]byte // padding to align arena

	buckets [BucketCount]Handle    // 1:1 mapping of tick → head
	groups  [GroupCount]groupBlock // 64 groups × 512 B = 32 KB total
	arena   [CapItems]node         // intrusive handle allocator
}

var (
	ErrFull         = queueErr{"full"}
	ErrPastWindow   = queueErr{"past"}
	ErrBeyondWindow = queueErr{"future"}
	ErrNotFound     = queueErr{"notfound"}
)

type queueErr struct{ msg string }

func (e queueErr) Error() string { return e.msg }

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
	if q.freeHead == nilIdx {
		return nilIdx, ErrFull
	}
	h := q.freeHead
	q.freeHead = q.arena[h].next
	q.arena[h] = node{next: nilIdx, prev: nilIdx}
	return h, nil
}

func (q *QuantumQueue) Size() int   { return q.size }
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

func (q *QuantumQueue) Push(tick int64, h Handle, val []byte) error {
	if uint32(h) >= CapItems {
		return ErrNotFound
	}
	n := &q.arena[h]
	if n.count > 0 && n.tick == tick {
		copy(n.data[:], val)
		n.count++
		return nil // Fast path: duplicate tick
	}

	delta := uint64(tick) - q.baseTick
	if delta>>18 != 0 {
		if int64(delta) < 0 {
			return ErrPastWindow
		}
		return ErrBeyondWindow
	}

	// Handle relocation if already linked
	if n.count > 0 {
		deltaOld := uint64(n.tick - int64(q.baseTick))
		q.unlinkByIndex(h, deltaOld)
		q.size -= int(n.count)
	}

	// Fast-path bucket insertion
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
	return nil
}

func (q *QuantumQueue) Update(tick int64, h Handle, val []byte) error {
	if uint32(h) >= CapItems {
		return ErrNotFound
	}
	n := &q.arena[h]
	if n.count == 0 {
		return ErrNotFound
	}
	if n.tick == tick {
		copy(n.data[:], val)
		return nil // Fast path: in-place update
	}

	deltaOld := uint64(n.tick - int64(q.baseTick))
	q.unlinkByIndex(h, deltaOld)
	q.size -= int(n.count)

	delta := uint64(tick) - q.baseTick
	if delta>>18 != 0 {
		if int64(delta) < 0 {
			return ErrPastWindow
		}
		return ErrBeyondWindow
	}

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
	return nil
}

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

func (q *QuantumQueue) PopMin() (Handle, int64, []byte) {
	h, t, v := q.PeepMin()
	if h == nilIdx {
		return nilIdx, 0, nil
	}
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
