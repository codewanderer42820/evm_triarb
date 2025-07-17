// ════════════════════════════════════════════════════════════════════════════════════════════════
// Shared Memory Priority Queue
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Zero-Allocation Priority Queue with External Memory Management
//
// Description:
//   Priority queue implementation that operates on externally-managed memory pools. Multiple
//   queue instances can share a single large allocation, enabling optimal cache locality and
//   eliminating per-queue memory overhead in multi-queue systems.
//
// Features:
//   - Shared memory pools reduce total allocation overhead
//   - Improved cache locality when related queues share memory
//   - Zero internal allocations - all memory provided externally
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
	// GroupCount defines top-level groups in the bitmap hierarchy.
	// Determines the coarsest granularity of priority scanning.
	GroupCount = 64

	// LaneCount specifies lanes per group for medium granularity.
	// Each lane represents 64 individual priority buckets.
	LaneCount = 64

	// BucketCount calculates total addressable priority levels.
	// Supports 262,144 distinct priorities (64 × 64 × 64).
	BucketCount = GroupCount * LaneCount * LaneCount
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Handle represents an external pool index for queue entries.
// Unlike internal queues, handles are managed by the caller and must
// be allocated from a separate handle management system.
type Handle uint64

// nilIdx marks invalid handles and empty links.
// Uses maximum uint64 value as sentinel.
const nilIdx Handle = ^Handle(0)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SHARED MEMORY ENTRY
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Entry represents a queue element within the shared memory pool.
// This structure defines the fundamental unit of the external allocation.
//
// Memory Requirements:
//   - 32-byte alignment ensures optimal cache line usage
//   - All pool entries must be pre-initialized before use
//   - Initialization prevents segmentation faults from garbage memory
//
// Field Organization:
//   - Tick: Primary key for priority ordering (-1 indicates unlinked)
//   - Data: User payload for compact value storage
//   - Prev/Next: Doubly-linked list maintenance
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

// groupBlock maintains lane summaries for efficient minimum detection.
// Structure identical to other quantum queue variants for consistency.
//
//go:notinheap
//go:align 64
type groupBlock struct {
	l1Summary uint64            // 8B - Active lanes mask
	l2        [LaneCount]uint64 // 512B - Per-lane bucket masks
	_         [56]byte          // 56B - Pad to 64-byte boundary
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN QUEUE STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// PooledQuantumQueue provides priority queue operations on shared memory.
// Designed for systems where multiple queues share a common memory pool.
//
// Critical Design Point:
//
//	Unlike self-contained queues, this implementation requires proper
//	pool initialization. Failure to initialize pool entries results
//	in undefined behavior including segmentation faults.
//
//go:notinheap
//go:align 64
type PooledQuantumQueue struct {
	// Hot path metadata (24 bytes)
	summary uint64  // 8B - Active groups mask
	size    int     // 8B - Current entry count
	arena   uintptr // 8B - Base pointer to shared pool

	// Padding to cache line boundary (40 bytes)
	_ [40]byte // 40B - Cache isolation

	// Large data structures
	buckets [BucketCount]Handle    // Per-tick chain heads
	groups  [GroupCount]groupBlock // Hierarchical summaries
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// New creates a queue using the provided memory pool.
// The pool must be properly initialized before calling this function.
//
// Pool Initialization Pattern:
//
//	```
//	pool := make([]Entry, poolSize)
//	for i := range pool {
//	    pool[i].Tick = -1     // Mark unlinked
//	    pool[i].Prev = nilIdx // Clear prev
//	    pool[i].Next = nilIdx // Clear next
//	    pool[i].Data = 0      // Clear data
//	}
//	q := New(unsafe.Pointer(&pool[0]))
//	```
//
// Safety Requirements:
//   - Pool must be aligned for Entry structure
//   - All entries must be initialized to unlinked state
//   - Pool must remain valid for queue lifetime
//   - Multiple queues can share the same pool safely
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New(arena unsafe.Pointer) *PooledQuantumQueue {
	q := &PooledQuantumQueue{arena: uintptr(arena)}

	// Initialize all buckets to empty state
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
// Safety Notes:
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

// Size returns the current number of entries.
// Maintained incrementally during insertions and removals.
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
// Provides boolean interface for queue state queries.
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

// unlink removes an entry and maintains bitmap consistency.
// Updates the hierarchical bitmap structure when buckets become empty.
//
// Preconditions:
//   - Entry must be currently linked (Tick >= 0)
//   - Handle must be valid for the pool
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) unlink(h Handle) {
	entry := q.entry(h)
	b := Handle(entry.Tick)

	// Update doubly-linked list structure
	if entry.Prev != nilIdx {
		q.entry(entry.Prev).Next = entry.Next
	} else {
		q.buckets[b] = entry.Next
	}

	if entry.Next != nilIdx {
		q.entry(entry.Next).Prev = entry.Prev
	}

	// Maintain bitmap hierarchy if bucket becomes empty
	if q.buckets[b] == nilIdx {
		// Decompose tick into hierarchical indices
		g := uint64(entry.Tick) >> 12       // Group index
		l := (uint64(entry.Tick) >> 6) & 63 // Lane index
		bb := uint64(entry.Tick) & 63       // Bucket index

		gb := &q.groups[g]
		gb.l2[l] &^= 1 << (63 - bb) // Clear bucket bit

		if gb.l2[l] == 0 { // Lane became empty
			gb.l1Summary &^= 1 << (63 - l) // Clear lane bit
			if gb.l1Summary == 0 {         // Group became empty
				q.summary &^= 1 << (63 - g) // Clear group bit
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
// Updates bitmap hierarchy to reflect the new entry.
//
// Preconditions:
//   - Entry must be unlinked (Tick = -1)
//   - Handle must be valid for the pool
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

	// Update hierarchical bitmap
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

// Push inserts or updates an entry in the queue.
// Handles must be allocated and managed externally by the caller.
//
// Handle Management:
//
//	The caller is responsible for:
//	- Allocating handles from the shared pool
//	- Ensuring handles are not used in multiple queues simultaneously
//	- Properly initializing entries before first use
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) Push(tick int64, h Handle, val uint64) {
	entry := q.entry(h)

	// Hot path: update data for same priority
	if entry.Tick == tick {
		entry.Data = val
		return
	}

	// Cold path: relocate to new priority
	if entry.Tick >= 0 {
		q.unlink(h)
	}
	q.linkAtHead(h, tick)
	entry.Data = val
}

// PeepMin returns the minimum entry without removal.
// Uses hierarchical bitmap traversal with CLZ operations.
//
// Safety Requirements:
//   - Queue must not be empty
//   - Undefined behavior on empty queue
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) PeepMin() (Handle, int64, uint64) {
	// Find minimum via CLZ bitmap traversal
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	t := bits.LeadingZeros64(gb.l2[l])

	// Reconstruct bucket index
	b := Handle((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]

	// Return handle and entry data
	entry := q.entry(h)
	return h, entry.Tick, entry.Data
}

// MoveTick relocates an entry to a new priority.
// Optimized for the case where priority is unchanged.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) MoveTick(h Handle, newTick int64) {
	entry := q.entry(h)

	// No operation if priority unchanged
	if entry.Tick == newTick {
		return
	}

	// Relocate entry
	q.unlink(h)
	q.linkAtHead(h, newTick)
}

// UnlinkMin removes the minimum entry from the queue.
// The handle should typically come from a prior PeepMin call.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) UnlinkMin(h Handle) {
	q.unlink(h)
}
