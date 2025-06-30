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
	// GroupCount = number of top-level summary groups. Each group spans 4096 ticks.
	GroupCount = 64

	// LaneCount = number of lanes per group. Each lane spans 64 ticks.
	LaneCount = 64

	// BucketCount = total number of unique tick slots (Group × Lane × Bucket)
	BucketCount = GroupCount * LaneCount * LaneCount // = 262,144

	// CapItems = maximum number of queue entries that can be held concurrently.
	// One node per tick is allowed.
	CapItems = BucketCount
)

// Handle is an opaque index into the node arena. Must remain within bounds [0, CapItems).
type Handle uint32

// nilIdx is a sentinel value used to indicate the absence of a handle.
// Used for freelist termination and doubly-linked list boundaries.
const nilIdx Handle = ^Handle(0)

// idx32 is a local alias used where semantic clarity is needed for bucket indices.
type idx32 = Handle

// -----------------------------------------------------------------------------
// Node: Individual queue element (tick key, payload, pointers)
// -----------------------------------------------------------------------------

// node represents a single entry in the queue.
// - Stored in a statically-allocated arena (no heap).
// - 64B aligned for cache performance.
// - Holds the tick key, inline 48-byte payload, and doubly-linked list pointers.

//go:notinheap     // Prevents heap metadata; ensures arena-pinning
//go:align 64      // Align each node to 64 bytes (cache-line aligned)
//go:inline        // Encourage inlining of accessors and helpers
type node struct {
	tick int64    // Tick index or -1 if unused (free)
	data [48]byte // Inline data payload (48 bytes = 3 cache lines)
	prev Handle   // Previous node in bucket (or nilIdx if head)
	next Handle   // Next node in bucket or freelist
}

// -----------------------------------------------------------------------------
// groupBlock: Bitmap summaries for one group of lanes
// -----------------------------------------------------------------------------

// groupBlock stores 64 lanes of tick metadata and summaries.
// Used for O(1) traversal and hierarchy tracking of tick population.

//go:notinheap     // Avoids heap overhead
//go:align 576     // Align to reduce false sharing and optimize cache fetches
//go:inline
type groupBlock struct {
	l1Summary uint64            // Summary of active lanes (1 bit per lane)
	l2        [LaneCount]uint64 // Each lane has a 64-bit bitmap (1 bit per bucket)
	_         [56]byte          // Explicit padding to avoid cross-line overlap
}

// -----------------------------------------------------------------------------
// QuantumQueue: Main queue structure
// -----------------------------------------------------------------------------

// QuantumQueue is the central footgun-mode queue.
// It avoids all allocations and performs all updates in O(1), but places
// full responsibility on the caller for maintaining safety and correctness.

//go:notinheap     // Avoid GC overhead or movement
//go:inline
type QuantumQueue struct {
	arena   [CapItems]node         // Static storage for all possible entries
	buckets [BucketCount]Handle    // Bucket array indexed by tick
	groups  [GroupCount]groupBlock // Per-group lane summaries

	summary  uint64   // Top-level summary across groups (1 bit per group)
	size     int      // Number of active (in-use) handles
	freeHead Handle   // Head of freelist for available handles
	_        [4]byte  // Padding for alignment (maintain 8-byte field spacing)
	_        [40]byte // Additional cache-line separation for hot fields
}

// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------

// NewQuantumQueue returns a new empty queue with all handles linked into the freelist.
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
	last := &q.arena[CapItems-1]
	last.next = nilIdx
	last.tick = -1
	last.prev = nilIdx

	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// -----------------------------------------------------------------------------
// Borrowing nodes from the freelist
// -----------------------------------------------------------------------------

// Borrow returns the next available handle from the freelist.
// No check for exhaustion. Unsafe in footgun mode.
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

// BorrowSafe is like Borrow but returns an error if the freelist is exhausted.
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

// -----------------------------------------------------------------------------
// Queue metadata queries
// -----------------------------------------------------------------------------

// Size returns the number of live entries in the queue.
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) Size() int {
	return q.size
}

// Empty reports whether the queue is currently empty.
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) Empty() bool {
	return q.size == 0
}

// -----------------------------------------------------------------------------
// Core operations: unlink / linkAtHead / Push / PeepMin / MoveTick / UnlinkMin
// -----------------------------------------------------------------------------

// unlink removes a handle from its bucket and updates the bitmaps.
// The node is returned to the freelist.
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) unlink(h Handle) {
	n := &q.arena[h]
	b := idx32(n.tick)

	// Detach node from doubly-linked list
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// If bucket became empty, clear bitmap hierarchy
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

	// Recycle the node into the freelist
	n.next = q.freeHead
	n.prev = nilIdx
	n.tick = -1
	q.freeHead = h
	q.size--
}

// linkAtHead inserts a node into the bucket at its new tick and updates the summary hierarchy.
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

	g := uint64(tick) >> 12
	l := (uint64(tick) >> 6) & 63
	bb := uint64(tick) & 63
	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - bb)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)

	q.size++
}

// Push inserts or updates the node at the specified tick.
// If the handle is already assigned and same-tick, only the payload is updated.
// Otherwise, it is unlinked from its current tick and reinserted at the new tick.
//
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

// PeepMin returns the head of the lexicographically minimum tick,
// without modifying the queue. O(1) via summary bitmaps.
//
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

// MoveTick relocates a handle to a new tick (if different).
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

// UnlinkMin removes the head of the minimum tick.
// The `tick` argument is unused (retained for call symmetry).
//
//go:inline
//go:nosplit
//go:registerparams
func (q *QuantumQueue) UnlinkMin(h Handle, _ int64) {
	q.unlink(h)
}
