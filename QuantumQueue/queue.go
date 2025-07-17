// ════════════════════════════════════════════════════════════════════════════════════════════════
// Hierarchical Bitmap Priority Queue
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Fixed-Capacity Priority Queue
//
// Description:
//   Fixed-capacity priority queue with constant-time operations using hierarchical bitmap indexing.
//   Supports 262,144 distinct priority levels with minimum extraction through
//   hardware-accelerated bit manipulation instructions.
//
// Features:
//   - Three-level bitmap hierarchy for efficient minimum finding
//   - Pre-allocated arena eliminates dynamic memory allocation
//   - Doubly-linked lists within priority buckets for constant-time updates
//   - Hardware CLZ instructions enable rapid priority scanning
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package quantumqueue

import (
	"errors"
	"math/bits"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONFIGURATION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// GroupCount defines the number of top-level summary groups in the bitmap hierarchy.
	// Each group summarizes 64 lanes of priority information.
	GroupCount = 64

	// LaneCount specifies lanes per group in the middle hierarchy level.
	// Each lane summarizes 64 individual priority buckets.
	LaneCount = 64

	// BucketCount represents total addressable priority levels.
	// Calculated as: GroupCount × LaneCount × 64 = 262,144 unique priorities.
	BucketCount = GroupCount * LaneCount * LaneCount

	// CapItems defines the maximum number of entries the queue can hold.
	// Set to 64K entries for efficient memory usage and addressing.
	CapItems = 1 << 16
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Handle represents an index into the queue's internal arena.
// Handles provide stable references to queue entries across operations.
type Handle uint32

// nilIdx serves as a sentinel value indicating no link or invalid handle.
// Set to maximum uint32 value to distinguish from valid indices.
const nilIdx Handle = ^Handle(0)

// idx32 provides a type alias for consistency in internal indexing.
type idx32 = Handle

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// NODE STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// node represents a single queue entry optimized for cache line efficiency.
// The 64-byte size ensures each node occupies exactly one cache line,
// minimizing false sharing and maximizing memory bandwidth utilization.
//
// Field Layout:
//   - tick: Priority value or -1 when free
//   - data: User payload consuming most of the cache line
//   - next/prev: Doubly-linked list pointers for constant-time operations
//
//go:notinheap
//go:align 64
type node struct {
	tick int64    // 8B - Active tick or -1 if free
	data [48]byte // 48B - User payload (3/4 cache line)
	next Handle   // 4B - Next in chain or freelist
	prev Handle   // 4B - Previous in chain
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// BITMAP HIERARCHY
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// groupBlock implements the middle level of the three-tier bitmap hierarchy.
// Each group tracks 64 lanes, with each lane tracking 64 buckets.
//
// Bitmap Organization:
//   - l1Summary: Single 64-bit mask indicating which lanes contain entries
//   - l2: Array of 64 lane masks, each indicating occupied buckets
//   - Padding ensures exclusive cache line ownership
//
//go:notinheap
//go:align 64
type groupBlock struct {
	l1Summary uint64            // 8B - Active lanes mask
	l2        [LaneCount]uint64 // 512B - Per-lane bucket masks
	_         [56]byte          // 56B - Cache line padding
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN QUEUE STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// QuantumQueue implements a static-capacity priority queue with hierarchical bitmap indexing.
// The structure is carefully organized to minimize cache misses during operations.
//
// Memory Layout:
//   - Hot metadata (summary, size, freeHead) fits in first cache line
//   - Large arrays (arena, buckets, groups) are cache-aligned
//   - Padding prevents false sharing between sections
//
//go:notinheap
//go:align 64
type QuantumQueue struct {
	// Hot path metadata (16 bytes) - accessed on every operation
	summary  uint64 // 8B - Global active groups mask
	size     int    // 4B - Current entry count
	freeHead Handle // 4B - Freelist head

	// Padding to cache line boundary (48 bytes)
	_ [48]byte // 48B - Cache isolation

	// Large data structures - accessed based on operation type
	arena   [CapItems]node         // Fixed allocation pool
	buckets [BucketCount]Handle    // Per-tick chain heads
	groups  [GroupCount]groupBlock // Hierarchical summaries
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// New creates an initialized queue with full capacity available.
// All entries start in the freelist, ready for allocation.
//
// Initialization Process:
//  1. Create freelist chain linking all nodes
//  2. Mark all nodes as unallocated (tick = -1)
//  3. Initialize all buckets as empty
//  4. Clear all bitmap summaries
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New() *QuantumQueue {
	q := &QuantumQueue{freeHead: 0}

	// Initialize freelist as a singly-linked chain
	// Each node points to the next available slot
	for i := Handle(0); i < CapItems-1; i++ {
		q.arena[i].next = i + 1
		q.arena[i].tick = -1     // Mark as free
		q.arena[i].prev = nilIdx // No previous in freelist
	}

	// Terminate the freelist at the last entry
	q.arena[CapItems-1].next = nilIdx
	q.arena[CapItems-1].tick = -1
	q.arena[CapItems-1].prev = nilIdx

	// Initialize all priority buckets as empty
	// Empty buckets have nilIdx as their head pointer
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}

	return q
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HANDLE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Borrow allocates a handle from the freelist without capacity checking.
// This variant assumes the caller has verified capacity availability.
//
// Operation:
//  1. Remove head node from freelist
//  2. Reset node to clean state
//  3. Return handle for caller use
//
// Safety Requirements:
//   - Caller must ensure queue has available capacity
//   - No validation performed for maximum speed
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) Borrow() (Handle, error) {
	// Take the first available node from freelist
	h := q.freeHead
	q.freeHead = q.arena[h].next

	// Reset node to clean state
	n := &q.arena[h]
	n.tick, n.prev, n.next = -1, nilIdx, nilIdx

	return h, nil
}

// BorrowSafe allocates a handle with exhaustion checking.
// This variant provides safety at the cost of an additional branch.
//
// Error Handling:
//
//	Returns error when arena is exhausted rather than corrupting state.
//	Caller should handle capacity exhaustion gracefully.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) BorrowSafe() (Handle, error) {
	// Check for arena exhaustion
	h := q.freeHead
	if h == nilIdx {
		return nilIdx, errors.New("QuantumQueue: arena exhausted")
	}

	// Remove from freelist
	q.freeHead = q.arena[h].next

	// Reset node state
	n := &q.arena[h]
	n.tick, n.prev, n.next = -1, nilIdx, nilIdx

	return h, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// QUERY OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Size returns the current number of entries in the queue.
// This is maintained incrementally for constant-time access.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) Size() int {
	return q.size
}

// Empty checks if the queue contains any entries.
// Provides a convenient boolean interface for queue state.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) Empty() bool {
	return q.size == 0
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INTERNAL OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// unlink removes an entry from its bucket and maintains bitmap consistency.
// This operation handles all the complexity of bitmap hierarchy updates.
//
// Algorithm:
//  1. Remove node from doubly-linked bucket chain
//  2. If bucket becomes empty, clear its bit in lane mask
//  3. If lane becomes empty, clear its bit in group summary
//  4. If group becomes empty, clear its bit in global summary
//  5. Return node to freelist for reuse
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) unlink(h Handle) {
	n := &q.arena[h]
	b := idx32(n.tick)

	// Remove from doubly-linked chain
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next // Update bucket head
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// Update hierarchical bitmap summaries if bucket is now empty
	if q.buckets[b] == nilIdx {
		// Decompose tick into hierarchical indices
		g := uint64(n.tick) >> 12       // Group index (top 6 bits)
		l := (uint64(n.tick) >> 6) & 63 // Lane index (middle 6 bits)
		bb := uint64(n.tick) & 63       // Bucket index (bottom 6 bits)

		gb := &q.groups[g]
		// Clear bucket bit in lane mask
		gb.l2[l] &^= 1 << (63 - bb)

		if gb.l2[l] == 0 { // Lane now empty
			// Clear lane bit in group summary
			gb.l1Summary &^= 1 << (63 - l)
			if gb.l1Summary == 0 { // Group now empty
				// Clear group bit in global summary
				q.summary &^= 1 << (63 - g)
			}
		}
	}

	// Return node to freelist
	n.next = q.freeHead
	n.prev = nilIdx
	n.tick = -1
	q.freeHead = h
	q.size--
}

// linkAtHead inserts an entry at the head of its priority bucket.
// Uses LIFO ordering within buckets for cache efficiency.
//
// Algorithm:
//  1. Insert node at head of bucket's doubly-linked list
//  2. Set bucket bit in lane mask
//  3. Set lane bit in group summary
//  4. Set group bit in global summary
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) linkAtHead(h Handle, tick int64) {
	n := &q.arena[h]
	b := idx32(uint64(tick))

	// Insert at head of bucket chain
	n.tick = tick
	n.prev = nilIdx
	n.next = q.buckets[b]
	if n.next != nilIdx {
		q.arena[n.next].prev = h
	}
	q.buckets[b] = h

	// Update hierarchical bitmap summaries
	g := uint64(tick) >> 12       // Group index
	l := (uint64(tick) >> 6) & 63 // Lane index
	bb := uint64(tick) & 63       // Bucket index

	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - bb)    // Set bucket bit
	gb.l1Summary |= 1 << (63 - l) // Set lane bit
	q.summary |= 1 << (63 - g)    // Set group bit

	q.size++
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Push inserts or updates an entry at the specified priority level.
// Efficiently handles both new insertions and priority updates.
//
// Optimization:
//
//	Same-priority updates only modify data without touching links.
//	This common case avoids expensive bitmap maintenance.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) Push(tick int64, h Handle, val *[48]byte) {
	n := &q.arena[h]

	// Hot path: same priority update
	if n.tick == tick {
		n.data = *val
		return
	}

	// Cold path: relocate to new priority
	if n.tick >= 0 {
		q.unlink(h) // Remove from current position
	}
	q.linkAtHead(h, tick)
	n.data = *val
}

// PeepMin returns the minimum entry without removing it.
// Uses CLZ (Count Leading Zeros) instructions for rapid scanning.
//
// Algorithm:
//  1. Find first set bit in global summary (leftmost = minimum)
//  2. Find first set bit in selected group's lane summary
//  3. Find first set bit in selected lane's bucket mask
//  4. Combine indices to locate minimum bucket
//  5. Return head entry from that bucket
//
// Safety Requirements:
//   - Queue must not be empty
//   - Undefined behavior if called on empty queue
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) PeepMin() (Handle, int64, *[48]byte) {
	// Find minimum through hierarchical bitmap traversal
	g := bits.LeadingZeros64(q.summary) // Find first group
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary) // Find first lane in group
	t := bits.LeadingZeros64(gb.l2[l])     // Find first bucket in lane

	// Reconstruct bucket index from hierarchical components
	b := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]

	// Return handle and associated data
	return h, q.arena[h].tick, &q.arena[h].data
}

// MoveTick efficiently relocates an entry to a new priority.
// Optimized for the common case where priority doesn't change.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) MoveTick(h Handle, newTick int64) {
	n := &q.arena[h]

	// No-op if priority unchanged
	if n.tick == newTick {
		return
	}

	// Relocate to new priority
	q.unlink(h)
	q.linkAtHead(h, newTick)
}

// UnlinkMin removes the minimum entry from the queue.
// Typically called after PeepMin to complete extraction.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) UnlinkMin(h Handle) {
	q.unlink(h)
}
