package quantumqueue

import (
	"errors"
	"math/bits"
)

// -----------------------------------------------------------------------------
// QuantumQueue: Ultra-low-latency fixed-range priority queue
// - Tick range: 0 .. 262143 (18-bit)
// - Fully arena-backed: 0 allocs, 0 GC pressure
// - Footgun mode: zero safety checks, insert-at-head only
// - O(1) Push, PeepMin, MoveTick, UnlinkMin (assuming caller discipline)
// -----------------------------------------------------------------------------

const (
	GroupCount  = 64                                 // top-level groups (4096 ticks each)
	LaneCount   = 64                                 // lanes per group (64 buckets per lane)
	BucketCount = GroupCount * LaneCount * LaneCount // 262144 buckets
	CapItems    = BucketCount                        // one node per tick slot
)

type Handle uint32

const nilIdx Handle = ^Handle(0)

type idx32 = Handle

// node is a single queue entry (64B cache-aligned).
// Stores tick key, payload, and doubly-linked pointers.
// Annotated for maximum performance:
//   - nosplit: avoid stack checks
//   - inline: encourage inlining
//   - noescape: pointers don't escape
//   - nocheckptr: disable pointer checks
//   - notinheap & align: ensure struct layout and no heap overhead
//
//go:notinheap
//go:align 64
//go:inline
type node struct {
	tick int64    // assigned tick value, -1 if unused
	data [48]byte // inline payload (zero-copy)
	prev Handle   // previous in same bucket
	next Handle   // next in same bucket
}

// groupBlock represents a 64x64 bucket bitmap (576B).
// Summary tree: summary → group.l1Summary → group.l2[lane]
// Use directives for alignment and inlining.
//
//go:align 576
//go:inline
type groupBlock struct {
	l1Summary uint64
	l2        [LaneCount]uint64
	_         [56]byte
}

// QuantumQueue is an arena-backed priority queue.
type QuantumQueue struct {
	arena   [CapItems]node         // static nodes
	buckets [BucketCount]Handle    // head pointers per tick
	groups  [GroupCount]groupBlock // 2-level summary bitmaps

	summary  uint64   // top-level group summary
	size     int      // total active entries
	freeHead Handle   // free list head
	_        [4]byte  // align
	_        [40]byte // pad header
}

// NewQuantumQueue initializes an empty queue.
//
//go:inline
//go:nosplit
//go:registerparams
func NewQuantumQueue() *QuantumQueue {
	q := &QuantumQueue{freeHead: 0}
	for i := Handle(0); i < CapItems-1; i++ {
		q.arena[i].next = i + 1
		q.arena[i].tick = -1
		q.arena[i].prev = nilIdx
	}
	q.arena[CapItems-1].next = nilIdx
	q.arena[CapItems-1].tick = -1
	q.arena[CapItems-1].prev = nilIdx
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// Borrow returns an unchecked arena handle.
// Caller must not exceed CapItems.
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	q.freeHead = q.arena[h].next
	n := &q.arena[h]
	n.tick = -1
	n.prev = nilIdx
	n.next = nilIdx
	return h, nil
}

// BorrowSafe returns a handle or error if exhausted.
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) BorrowSafe() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		return nilIdx, errors.New("QuantumQueue: arena exhausted")
	}
	q.freeHead = q.arena[h].next
	n := &q.arena[h]
	n.tick = -1
	n.prev = nilIdx
	n.next = nilIdx
	return h, nil
}

// Size returns the number of active entries.
//
//go:inline
//go:registerparams
func (q *QuantumQueue) Size() int { return q.size }

// Empty reports whether the queue is empty.
//
//go:inline
//go:registerparams
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

// unlink removes handle h from its bucket and recycles it.
// O(1) doubly-linked removal + bitmap maintenance.
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) unlink(h Handle) {
	n := &q.arena[h]
	b := idx32(n.tick)

	// stitch out of doubly-linked list
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// clear bitmaps if bucket emptied
	if q.buckets[b] == nilIdx {
		g := uint64(n.tick) >> 12
		l := (uint64(n.tick) >> 6) & 63
		bb := uint64(n.tick) & 63
		gb := &q.groups[g]
		gb.l2[l] &^= 1 << (63 - bb)
		if gb.l2[l] == 0 {
			gb.l1Summary &^= 1 << (63 - l)
			if gb.l1Summary == 0 {
				q.summary &^= 1 << (63 - g)
			}
		}
	}

	// recycle node
	n.next = q.freeHead
	n.prev = nilIdx
	n.tick = -1
	q.freeHead = h
	q.size--
}

// linkAtHead inserts h into bucket for tick and updates bitmaps.
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) linkAtHead(h Handle, tick int64) {
	n := &q.arena[h]
	b := idx32(uint64(tick))

	n.tick = tick
	n.prev = nilIdx
	n.next = q.buckets[b]
	if n.next != nilIdx {
		q.arena[n.next].prev = h
	}
	q.buckets[b] = h

	// set bitmaps
	g := uint64(tick) >> 12
	l := (uint64(tick) >> 6) & 63
	bb := uint64(tick) & 63
	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - bb)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)

	q.size++
}

// Push inserts or updates a handle at tick with payload val.
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) Push(tick int64, h Handle, val []byte) {
	n := &q.arena[h]
	if n.tick == tick {
		copy(n.data[:], val)
		return
	}
	if n.tick >= 0 {
		q.unlink(h)
	}
	q.linkAtHead(h, tick)
	copy(q.arena[h].data[:], val)
}

// PeepMin returns the handle, tick, and payload ptr of the smallest tick.
//
//go:inline
//go:registerparams
func (q *QuantumQueue) PeepMin() (Handle, int64, *[48]byte) {
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	t := bits.LeadingZeros64(gb.l2[l])
	b := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]
	return h, q.arena[h].tick, &q.arena[h].data
}

// MoveTick reassigns an existing handle to newTick.
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) MoveTick(h Handle, newTick int64) {
	n := &q.arena[h]
	if n.tick == newTick {
		return
	}
	q.unlink(h)
	q.linkAtHead(h, newTick)
}

// UnlinkMin removes handle h from its bucket.
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) UnlinkMin(h Handle, _ int64) {
	q.unlink(h)
}
