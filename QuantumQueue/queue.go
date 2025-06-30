// Package quantumqueue provides an ultra-low-latency, fixed-range priority queue
// implementation in Go. It leverages a static arena to avoid heap allocations
// and uses a two-level bitmap summary for O(1) operations: Push, PeepMin,
// MoveTick, and UnlinkMin. This "footgun" version omits safety checks for
// maximum performance, so the caller must uphold invariants (e.g., valid handles,
// correct ticks).
package quantumqueue

import (
	"errors"
	"math/bits"
)

// -----------------------------------------------------------------------------
// Constants defining the queue dimensions and capacity.
// -----------------------------------------------------------------------------
const (
	// GroupCount is the number of top-level groups; each covers 4096 ticks.
	GroupCount = 64
	// LaneCount is the number of lanes within each group; each lane covers 64 ticks.
	LaneCount = 64
	// BucketCount = total tick slots = GroupCount * LaneCount * LaneCount.
	// Here: 64 groups × 64 lanes × 64 ticks = 262,144 buckets.
	BucketCount = GroupCount * LaneCount * LaneCount
	// CapItems matches the number of buckets; each slot holds exactly one node.
	CapItems = BucketCount
)

// Handle is an index into the arena array. It must be in [0, CapItems).
type Handle uint32

// nilIdx represents the absence of a valid handle (e.g., list terminator).
const nilIdx Handle = ^Handle(0)

// idx32 is an alias for Handle, used when treating it as a bucket index.
type idx32 = Handle

// node represents one entry in the priority queue. It is cache-aligned
// and contains the tick key, inline payload, and doubly-linked pointers
// for efficient removal and insertion at the head.
// Compiler directives ensure inlining, alignment, and no heap allocation.

//go:notinheap
//go:align 64
//go:inline
type node struct {
	tick int64    // queue key or -1 if unused
	data [48]byte // inline payload
	prev Handle   // doubly-linked list: previous
	next Handle   // doubly-linked list: next or freelist next
}

// groupBlock holds a two-level bitmap summary for one group of 64 lanes,
// each lane covering 64 buckets. Summary bits allow fast traversal.

//go:align 576
//go:inline
type groupBlock struct {
	l1Summary uint64            // one bit per lane
	l2        [LaneCount]uint64 // one bit per bucket in lane
	_         [56]byte          // padding for cache line alignment
}

// QuantumQueue is the main priority queue structure.
//
// It includes:
//   - `arena`: node storage (fixed)
//   - `buckets`: one head per tick
//   - `groups`: bitmap summaries
//   - `summary`: top-level group bitmap
//   - `size`: active entry count
//   - `freeHead`: freelist head
//
// Padding is used to align hot fields for cache efficiency.
type QuantumQueue struct {
	arena   [CapItems]node         // statically allocated pool
	buckets [BucketCount]Handle    // tick-indexed list heads
	groups  [GroupCount]groupBlock // two-level bitmap summary

	summary  uint64   // top-level summary across groups
	size     int      // count of active items
	freeHead Handle   // free list head
	_        [4]byte  // alignment
	_        [40]byte // further cache alignment
}

// NewQuantumQueue constructs a zero-initialized queue with freelist populated.

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
	last := &q.arena[CapItems-1]
	last.next = nilIdx
	last.tick = -1
	last.prev = nilIdx

	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// Borrow retrieves a node from the freelist. No safety check.

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

// BorrowSafe does the same as Borrow, but fails gracefully if out of nodes.

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

// Size returns the current number of live entries.

//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) Size() int {
	return q.size
}

// Empty reports if the queue is currently empty.

//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) Empty() bool {
	return q.size == 0
}

// unlink removes a handle from its current bucket and updates bitmap summaries.

//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) unlink(h Handle) {
	n := &q.arena[h]
	b := idx32(n.tick)

	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
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

	n.next = q.freeHead
	n.prev = nilIdx
	n.tick = -1
	q.freeHead = h
	q.size--
}

// linkAtHead inserts handle at the head of the tick bucket and updates summaries.

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

	g := uint64(tick) >> 12
	l := (uint64(tick) >> 6) & 63
	bb := uint64(tick) & 63
	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - bb)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)

	q.size++
}

// Push inserts or updates a handle into the queue at the given tick. O(1).

//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) Push(tick int64, h Handle, val *[48]byte) {
	n := &q.arena[h]
	if n.tick == tick {
		n.data = *val
		return
	}
	if n.tick >= 0 {
		q.unlink(h)
	}
	q.linkAtHead(h, tick)
	n.data = *val
}

// PeepMin returns the minimum tick entry in the queue, without removing it. O(1).

//go:inline
//go:nosplit
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

// MoveTick relocates a handle to a new tick. No-op if unchanged. O(1).

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

// UnlinkMin removes a handle that was retrieved via PeepMin. Tick is unused.

//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) UnlinkMin(h Handle, _ int64) {
	q.unlink(h)
}
