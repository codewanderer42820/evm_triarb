// ════════════════════════════════════════════════════════════════════════════════════════════════
// Shared Memory Priority Queue
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Zero-Allocation Priority Queue with External Memory Management
//
// Description:
//   Fixed-capacity priority queue with constant-time operations using hierarchical bitmap indexing.
//   Operates on externally-managed memory pools, enabling multiple queue instances to share a
//   single large allocation for optimal cache locality and reduced memory overhead.
//
// Features:
//   - Three-level bitmap hierarchy for efficient minimum finding
//   - Shared memory pools reduce total allocation overhead
//   - Hardware CLZ instructions enable rapid priority scanning
//   - Handle-based indirection enables flexible memory management
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package pooledquantumqueue

import (
	"math/bits"
	"unsafe"
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
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Handle represents an external pool index for queue entries.
// Unlike internal queues, handles are managed by the caller and must
// be allocated from a separate handle management system.
type Handle uint64

// nilIdx serves as a sentinel value indicating no link or invalid handle.
// Uses maximum uint64 value to distinguish from valid indices.
const nilIdx Handle = ^Handle(0)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SHARED MEMORY ENTRY
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Entry represents a single queue element within the shared memory pool.
// The 32-byte size ensures optimal memory alignment and cache utilization.
//
// Field Layout:
//   - Tick: Priority value or -1 when free
//   - Data: User payload for compact value storage
//   - Next/Prev: Doubly-linked list pointers for constant-time operations
//
//go:notinheap
//go:align 32
type Entry struct {
	Tick int64  // 8B - Active tick or -1 if free
	Data uint64 // 8B - Compact payload
	Next Handle // 8B - Next in chain
	Prev Handle // 8B - Previous in chain
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

// PooledQuantumQueue implements a static-capacity priority queue with hierarchical bitmap indexing.
// Operates on externally-managed memory pools for optimal resource utilization in multi-queue systems.
//
// Memory Layout:
//   - Hot metadata (summary, size, arena) fits in first cache line
//   - Large arrays (buckets, groups) are cache-aligned
//   - Padding prevents false sharing between sections
//
//go:notinheap
//go:align 64
type PooledQuantumQueue struct {
	// Hot path metadata (24 bytes) - accessed on every operation
	summary uint64  // 8B - Global active groups mask
	size    int     // 8B - Current entry count
	arena   uintptr // 8B - Base pointer to shared pool

	// Padding to cache line boundary (40 bytes)
	_ [40]byte // 40B - Cache isolation

	// Large data structures - accessed based on operation type
	buckets [BucketCount]Handle    // Per-tick chain heads
	groups  [GroupCount]groupBlock // Hierarchical summaries
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// New creates an initialized queue using the provided memory pool.
// The pool must be properly initialized before calling this function.
//
// Pool Initialization Requirements:
//  1. All entries must be marked as unlinked (Tick = -1)
//  2. All link pointers must be cleared (Next/Prev = nilIdx)
//  3. Pool must remain valid for queue lifetime
//  4. Multiple queues can share the same pool safely
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New(arena unsafe.Pointer) *PooledQuantumQueue {
	q := &PooledQuantumQueue{arena: uintptr(arena)}

	// Initialize all priority buckets as empty
	// Empty buckets have nilIdx as their head pointer
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}

	return q
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MEMORY ACCESS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// entry converts a handle to its corresponding entry pointer.
// Implements efficient address calculation for pool access.
//
// Address Calculation:
//
//	address = arena_base + (handle × sizeof(Entry))
//	Optimized using shift for 32-byte Entry size
//
// Safety Requirements:
//   - No bounds checking for maximum speed
//   - Caller must ensure handle validity
//   - Invalid handles cause memory corruption
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) entry(h Handle) *Entry {
	// Shift by 5 for 32-byte entries (2^5 = 32)
	return (*Entry)(unsafe.Pointer(q.arena + uintptr(h)<<5))
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
func (q *PooledQuantumQueue) Size() int {
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
func (q *PooledQuantumQueue) Empty() bool {
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
//  5. Mark entry as unlinked for reuse
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) unlink(h Handle) {
	entry := q.entry(h)
	b := Handle(entry.Tick)

	// Remove from doubly-linked chain
	if entry.Prev != nilIdx {
		q.entry(entry.Prev).Next = entry.Next
	} else {
		q.buckets[b] = entry.Next // Update bucket head
	}
	if entry.Next != nilIdx {
		q.entry(entry.Next).Prev = entry.Prev
	}

	// Update hierarchical bitmap summaries if bucket is now empty
	if q.buckets[b] == nilIdx {
		// Decompose tick into hierarchical indices
		g := uint64(entry.Tick) >> 12       // Group index (top 6 bits)
		l := (uint64(entry.Tick) >> 6) & 63 // Lane index (middle 6 bits)
		bb := uint64(entry.Tick) & 63       // Bucket index (bottom 6 bits)

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

	// Mark entry as unlinked
	entry.Next = nilIdx
	entry.Prev = nilIdx
	entry.Tick = -1
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
func (q *PooledQuantumQueue) linkAtHead(h Handle, tick int64) {
	entry := q.entry(h)
	b := Handle(uint64(tick))

	// Insert at head of bucket chain
	entry.Tick = tick
	entry.Prev = nilIdx
	entry.Next = q.buckets[b]
	if entry.Next != nilIdx {
		q.entry(entry.Next).Prev = h
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
func (q *PooledQuantumQueue) Push(tick int64, h Handle, val uint64) {
	entry := q.entry(h)

	// Hot path: same priority update
	if entry.Tick == tick {
		entry.Data = val
		return
	}

	// Cold path: relocate to new priority
	if entry.Tick >= 0 {
		q.unlink(h) // Remove from current position
	}
	q.linkAtHead(h, tick)
	entry.Data = val
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
func (q *PooledQuantumQueue) PeepMin() (Handle, int64, uint64) {
	// Find minimum through hierarchical bitmap traversal
	g := bits.LeadingZeros64(q.summary) // Find first group
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary) // Find first lane in group
	t := bits.LeadingZeros64(gb.l2[l])     // Find first bucket in lane

	// Reconstruct bucket index from hierarchical components
	b := Handle((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]

	// Return handle and associated data
	entry := q.entry(h)
	return h, entry.Tick, entry.Data
}

// MoveTick efficiently relocates an entry to a new priority.
// Optimized for the common case where priority doesn't change.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) MoveTick(h Handle, newTick int64) {
	entry := q.entry(h)

	// No-op if priority unchanged
	if entry.Tick == newTick {
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
func (q *PooledQuantumQueue) UnlinkMin(h Handle) {
	q.unlink(h)
}
