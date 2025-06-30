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
// Compiler directives (go:inline, go:notinheap, etc.) ensure inlining,
// proper alignment, and no heap metadata.

//go:notinheap
//go:align 64
//go:inline
//go:align 64
type node struct {
	// tick holds the queue key (range 0..BucketCount-1) or -1 if unused/free.
	tick int64
	// data is the inline 48-byte payload, avoiding allocations.
	data [48]byte
	// prev and next form a doubly-linked list within a bucket.
	prev Handle
	next Handle
}

// groupBlock holds a two-level bitmap summary for one group of 64 lanes,
// each lane covering 64 buckets. l1Summary has one bit per lane, indicating
// which lanes are non-empty. l2 has one bitmap per lane, with one bit per
// bucket. The padding ensures the struct aligns as expected.

//go:align 576
//go:inline
type groupBlock struct {
	l1Summary uint64            // bit i = 1 if lane i has any buckets
	l2        [LaneCount]uint64 // per-lane bucket bitmaps
	_         [56]byte          // padding to 576 bytes
}

// QuantumQueue is the main data structure, containing:
// - arena: fixed-size array of nodes
// - buckets: head pointer per tick
// - groups: two-level summaries per 4096-tick group
// - summary: top-level summary across all groups
// - size: count of active entries
// - freeHead: head of the singly-linked free-list of nodes
// Padding fields align the struct for cache efficiency.

type QuantumQueue struct {
	arena   [CapItems]node         // static storage
	buckets [BucketCount]Handle    // head-of-list per tick index
	groups  [GroupCount]groupBlock // summary bitmaps per group

	summary  uint64   // top-level group usage bitmap
	size     int      // current number of active entries
	freeHead Handle   // head of free-list for unused nodes
	_        [4]byte  // padding for alignment
	_        [40]byte // further padding to align struct
}

// NewQuantumQueue constructs an empty queue, initializing the free list
// to contain all handles [0..CapItems), setting ticks to -1, and clearing
// the buckets and summary bitmaps. It returns a pointer to the queue.

//go:inline
//go:nosplit
//go:registerparams
func NewQuantumQueue() *QuantumQueue {
	q := &QuantumQueue{freeHead: 0}

	// Link all arena nodes into a free list, reset tick and prev pointers.
	for i := Handle(0); i < CapItems-1; i++ {
		n := &q.arena[i]
		n.next = i + 1
		n.tick = -1
		n.prev = nilIdx
	}
	// Last node terminates the free list.
	last := &q.arena[CapItems-1]
	last.next = nilIdx
	last.tick = -1
	last.prev = nilIdx

	// Initialize all buckets to empty.
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// Borrow retrieves the next free handle from the free list without bounds check.
// The caller must ensure at most CapItems handles are borrowed simultaneously.
// The returned node is reset to tick=-1 and cleared pointers.

//go:inline
//go:nosplit
func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	q.freeHead = q.arena[h].next
	// Reset node state
	n := &q.arena[h]
	n.tick = -1
	n.prev = nilIdx
	n.next = nilIdx
	return h, nil
}

// BorrowSafe behaves like Borrow but returns an error if no handles remain.

//go:inline
//go:nosplit
func (q *QuantumQueue) BorrowSafe() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		return nilIdx, errors.New("QuantumQueue: arena exhausted")
	}
	// Remove from free list and reset
	q.freeHead = q.arena[h].next
	n := &q.arena[h]
	n.tick = -1
	n.prev = nilIdx
	n.next = nilIdx
	return h, nil
}

// Size returns the number of active entries in the queue.

//go:inline
//go:nosplit
func (q *QuantumQueue) Size() int {
	return q.size
}

// Empty reports whether the queue contains no active entries.

//go:inline
//go:nosplit
func (q *QuantumQueue) Empty() bool {
	return q.size == 0
}

// unlink removes node h from its current bucket list, updates the summary
// bitmaps to reflect removal, and pushes the node back onto the free list.
// This is an O(1) operation involving pointer updates and bit clearing.

//go:inline
//go:nosplit
func (q *QuantumQueue) unlink(h Handle) {
	n := &q.arena[h]
	b := idx32(n.tick)

	// 1) Remove from doubly-linked bucket list
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		// Node was head of bucket list: update bucket pointer
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// 2) Clear bitmap bits if bucket is now empty
	if q.buckets[b] == nilIdx {
		// Compute group and lane indices from tick
		g := uint64(n.tick) >> 12
		l := (uint64(n.tick) >> 6) & 63
		bb := uint64(n.tick) & 63
		gb := &q.groups[g]
		// Clear the bucket bit
		gb.l2[l] &^= 1 << (63 - bb)
		// If no buckets remain in lane, clear lane bit
		if gb.l2[l] == 0 {
			gb.l1Summary &^= 1 << (63 - l)
			// If no lanes remain in group, clear group bit
			if gb.l1Summary == 0 {
				q.summary &^= 1 << (63 - g)
			}
		}
	}

	// 3) Recycle node: push onto free list, reset tick
	n.next = q.freeHead
	n.prev = nilIdx
	n.tick = -1
	q.freeHead = h
	q.size--
}

// linkAtHead inserts node h into the head of the bucket list for the given tick,
// and updates the summary bitmaps to reflect the addition. O(1) time.

//go:inline
//go:nosplit
func (q *QuantumQueue) linkAtHead(h Handle, tick int64) {
	n := &q.arena[h]
	b := idx32(uint64(tick))

	// 1) Set node fields
	n.tick = tick
	n.prev = nilIdx
	n.next = q.buckets[b] // chain into existing list
	if n.next != nilIdx {
		q.arena[n.next].prev = h
	}
	q.buckets[b] = h

	// 2) Set summary bits for group, lane, and bucket
	g := uint64(tick) >> 12       // group index
	l := (uint64(tick) >> 6) & 63 // lane index within group
	bb := uint64(tick) & 63       // bucket bit within lane
	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - bb)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)

	// 3) Increase active entry count
	q.size++
}

// Push inserts a handle at the specified tick with the given payload. If the handle
// is already in the queue at a different tick, it is first removed (unlink). If the
// tick is unchanged, only the payload is updated in-place.

//go:inline
//go:nosplit
func (q *QuantumQueue) Push(tick int64, h Handle, val *[48]byte) {
	n := &q.arena[h]
	// Fast path: update data if same tick
	if n.tick == tick {
		n.data = *val
		return
	}
	// If already enqueued, unlink from old bucket
	if n.tick >= 0 {
		q.unlink(h)
	}
	// Insert into new bucket and set payload
	q.linkAtHead(h, tick)
	n.data = *val
}

// PeepMin returns the handle, tick, and data pointer of the lowest-tick item
// in O(1) time by walking the summary bitmaps: first find the first set bit in
// the top-level summary, then at each lower level.

//go:inline
//go:nosplit
func (q *QuantumQueue) PeepMin() (Handle, int64, *[48]byte) {
	// 1) Find first active group
	g := bits.LeadingZeros64(q.summary)
	// 2) Find first active lane within group
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	// 3) Find first active bucket within lane
	t := bits.LeadingZeros64(gb.l2[l])
	// 4) Compute absolute bucket index
	b := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	// 5) Return the head handle and its data
	h := q.buckets[b]
	return h, q.arena[h].tick, &q.arena[h].data
}

// MoveTick reassigns an existing handle to a new tick. If the tick is unchanged,
// it does nothing. Otherwise, it unlinks and relinks the handle at O(1) cost.

//go:inline
//go:nosplit
func (q *QuantumQueue) MoveTick(h Handle, newTick int64) {
	n := &q.arena[h]
	if n.tick == newTick {
		// no-op if tick unchanged
		return
	}
	q.unlink(h)
	q.linkAtHead(h, newTick)
}

// UnlinkMin removes the handle h (which must be the current minimum) from the queue.
// The tick argument is unused but provided for symmetry with PeepMin.

//go:inline
//go:nosplit
func (q *QuantumQueue) UnlinkMin(h Handle, _ int64) {
	q.unlink(h)
}
