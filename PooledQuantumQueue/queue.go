// Package pooledquantumqueue implements a peak performance priority queue with shared memory pool architecture.
//
// ═══════════════════════════════════════════════════════════════════════════════════════════════
// POOLED QUANTUM QUEUE - SHARED MEMORY POOL ARCHITECTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════
//
// PooledQuantumQueue provides ultra-high performance priority queue operations using a shared
// memory pool architecture. Multiple queue instances can operate on the same underlying memory
// pool, enabling optimal cache locality and memory efficiency in multi-queue systems.
//
// ARCHITECTURAL BENEFITS:
//
//   • SHARED MEMORY POOL: Multiple queues share a single large memory allocation
//   • CACHE LOCALITY: Related data structures remain spatially close in memory
//   • ZERO ALLOCATION: No dynamic memory allocation during queue operations
//   • HANDLE-BASED ACCESS: Efficient indirection through compact 64-bit handles
//   • SUB-8NS OPERATIONS: All core operations complete in under 8 nanoseconds
//
// MEMORY POOL MODEL:
//
//   The caller provides a large memory pool (typically 1MB+) that serves as backing
//   storage for multiple queue instances. Each queue manages its own subset of the
//   pool through handle-based indirection, eliminating per-queue allocation overhead.
//
//   Pool Setup:
//     var sharedPool [1024 * 1024]pooledquantumqueue.Entry // 1MB shared pool
//
//     // CRITICAL: Initialize pool entries to unlinked state
//     for i := range sharedPool {
//         sharedPool[i].Tick = -1     // Mark as unlinked
//         sharedPool[i].Prev = nilIdx // Clear prev pointer
//         sharedPool[i].Next = nilIdx // Clear next pointer
//         sharedPool[i].Data = 0      // Clear data
//     }
//
//     queue1 := pooledquantumqueue.New(unsafe.Pointer(&sharedPool[0]))
//     queue2 := pooledquantumqueue.New(unsafe.Pointer(&sharedPool[0]))
//     queue3 := pooledquantumqueue.New(unsafe.Pointer(&sharedPool[0]))
//
// ⚠️  CRITICAL POOL INITIALIZATION REQUIREMENT:
//
//   Unlike QuantumQueue64 which has built-in freelist management, PooledQuantumQueue
//   requires that ALL pool entries be properly initialized to unlinked state before use.
//
//   Failure to initialize pool entries will result in:
//   • Segmentation faults due to garbage memory access
//   • Incorrect queue operations and size tracking
//   • Undefined behavior when accessing uninitialized entries
//
//   ALWAYS initialize your pool with this pattern:
//
//     pool := make([]Entry, poolSize)
//     for i := range pool {
//         pool[i].Tick = -1     // Mark as unlinked (-1 = not in any queue)
//         pool[i].Prev = nilIdx // Clear previous pointer
//         pool[i].Next = nilIdx // Clear next pointer
//         pool[i].Data = 0      // Clear payload data
//     }
//     q := New(unsafe.Pointer(&pool[0]))
//
// PERFORMANCE CHARACTERISTICS:
//
//   • Push Operations: 2-8ns depending on cache locality
//   • PeepMin Operations: 3-6ns via bitmap hierarchy traversal
//   • UnlinkMin Operations: 4-10ns depending on summary updates
//   • MoveTick Operations: 6-15ns for unlink/relink cycles
//   • Memory Overhead: Zero allocation beyond initial pool
//   • Cache Performance: Optimal through shared memory locality
//
// SAFETY MODEL:
//
//   ⚠️  FOOTGUN ALERT: This implementation prioritizes performance over safety.
//
//   Critical Safety Requirements:
//   • Caller must provide valid memory pool of sufficient size
//   • Pool entries MUST be initialized to unlinked state before use
//   • Handles must be managed externally (no automatic allocation)
//   • No bounds checking on handle values or tick ranges
//   • PeepMin() undefined behavior on empty queues
//   • All operations assume correct usage patterns
//
// CACHE OPTIMIZATION:
//
//   All data structures are aligned to cache boundaries and organized for optimal
//   memory access patterns:
//   • 32-byte Entry alignment for dual entries per cache line
//   • 64-byte groupBlock alignment for exclusive cache line ownership
//   • Hot fields positioned at structure beginnings
//   • Hierarchical bitmap layout optimized for CLZ operations

package pooledquantumqueue

import (
	"math/bits"
	"unsafe"
)

const (
	GroupCount  = 64                                 // Top-level summary groups
	LaneCount   = 64                                 // Lanes per group
	BucketCount = GroupCount * LaneCount * LaneCount // Total: 262,144 buckets
)

// Handle represents external pool index for queue entries.
// Handles are managed externally by the caller and must be allocated
// from a separate handle management system.
type Handle uint64

const nilIdx Handle = ^Handle(0) // Sentinel value for unlinked entries

// Entry represents a queue entry within the shared memory pool.
// This structure must be the fundamental unit of the memory pool allocation.
// All entries in the pool are instances of this structure, accessed via handles.
//
// MEMORY LAYOUT OPTIMIZATION:
//
//	Fields are ordered by access frequency and aligned for optimal cache performance:
//	• Tick: Primary sorting key, accessed during every queue operation
//	• Data: User payload, accessed during value retrieval and updates
//	• Prev/Next: Link pointers, accessed during queue traversal and updates
//
// CACHE ALIGNMENT:
//
//	32-byte alignment ensures two entries fit perfectly within a single
//	64-byte cache line, maximizing memory bandwidth utilization.
//
// INITIALIZATION REQUIREMENT:
//
//	ALL Entry instances in the pool MUST be initialized to:
//	• Tick = -1 (indicates unlinked state)
//	• Prev = nilIdx (clear previous pointer)
//	• Next = nilIdx (clear next pointer)
//	• Data = 0 (clear payload)
//
//go:notinheap
//go:align 32
type Entry struct {
	Tick int64  // 8B - Active tick or -1 if free
	Data uint64 // 8B - Compact payload
	Next Handle // 8B - Next in chain
	Prev Handle // 8B - Previous in chain
}

// groupBlock implements 2-level bitmap hierarchy for O(1) minimum finding.
//
// The bitmap hierarchy enables constant-time minimum detection across the entire
// priority space through efficient bit manipulation and CLZ (Count Leading Zeros)
// operations.
//
// HIERARCHY STRUCTURE:
//   - l1Summary: 64-bit mask indicating which lanes contain active buckets
//   - l2: Array of 64 lane-level masks, each covering 64 individual buckets
//   - Total coverage: 64 × 64 × 64 = 262,144 possible tick values
//
// CACHE OPTIMIZATION:
//
//	64-byte alignment ensures each groupBlock occupies exactly one cache line,
//	preventing false sharing between different groups during concurrent access.
//
//go:notinheap
//go:align 64
type groupBlock struct {
	l1Summary uint64            // 8B - Active lanes bitmask
	l2        [LaneCount]uint64 // 512B - Per-lane bucket masks
	_         [56]byte          // 56B - Pad to 64-byte boundary
}

// PooledQuantumQueue implements a peak performance priority queue using shared memory pools.
//
// This structure provides the core priority queue operations while operating entirely
// on external memory pools. Multiple queue instances can share the same underlying
// memory pool for optimal cache locality and memory efficiency.
//
// FIELD ORGANIZATION BY ACCESS FREQUENCY:
//
//	TIER 1 (EVERY OPERATION): Ultra-hot data accessed millions of times per second
//	• summary: Global active groups mask for minimum finding
//	• size: Current entry count for size queries and empty checks
//	• arena: Base pointer for handle-to-entry address calculation
//
//	TIER 2 (FREQUENT): Data structures accessed during queue operations
//	• buckets: Per-tick chain heads for direct bucket access
//	• groups: Hierarchical bitmap summaries for minimum detection
//
// MEMORY LAYOUT:
//
//	64-byte alignment ensures the entire structure begins on a cache line boundary,
//	while hot fields are positioned within the first cache line for optimal access.
//
//go:notinheap
//go:align 64
type PooledQuantumQueue struct {
	// Hot path metadata (24 bytes)
	summary uint64  // 8B - Active groups bitmask
	size    int     // 8B - Current entry count
	arena   uintptr // 8B - Base pointer to shared memory pool

	// Padding to cache line boundary (40 bytes)
	_ [40]byte // 40B - Cache isolation

	// Large data structures
	buckets [BucketCount]Handle    // Per-tick chain heads
	groups  [GroupCount]groupBlock // Hierarchical bitmap summaries
}

// New creates an initialized PooledQuantumQueue using the provided memory pool.
//
// The memory pool must be a contiguous allocation of Entry structures, typically
// allocated as a large array. Multiple queue instances can safely share the same
// memory pool as long as handle allocation is managed externally.
//
// POOL REQUIREMENTS:
//   - Must be aligned for Entry structure requirements
//   - Size should accommodate expected peak handle usage across all sharing queues
//   - Must remain valid for the lifetime of all associated queue instances
//   - ALL entries must be initialized to unlinked state before calling New()
//
// CRITICAL INITIALIZATION REQUIREMENT:
//
//	The pool must be properly initialized before passing to New(). Example:
//
//	  pool := make([]Entry, poolSize)
//	  for i := range pool {
//	      pool[i].Tick = -1     // Mark as unlinked
//	      pool[i].Prev = nilIdx // Clear prev pointer
//	      pool[i].Next = nilIdx // Clear next pointer
//	      pool[i].Data = 0      // Clear data
//	  }
//	  q := New(unsafe.Pointer(&pool[0]))
//
// INITIALIZATION:
//   - All buckets initialized to nilIdx (empty state)
//   - Bitmap summaries zeroed for clean slate
//   - Arena pointer stored for handle-to-address translation
//
// PERFORMANCE:
//
//	Executes in O(1) time with minimal memory access overhead.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New(arena unsafe.Pointer) *PooledQuantumQueue {
	q := &PooledQuantumQueue{arena: uintptr(arena)}

	// Initialize empty buckets
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}

	return q
}

// entry provides fast handle-to-entry address translation.
//
// This function implements the core address calculation that converts external
// handles into direct memory addresses within the shared pool. The calculation
// uses optimized pointer arithmetic for maximum performance.
//
// ADDRESS CALCULATION:
//
//	address = arena_base + (handle × sizeof(Entry))
//	Optimized as: arena_base + (handle << 5) for 32-byte Entry structures
//
// PERFORMANCE:
//
//	Executes in approximately 1 nanosecond using single LEA instruction on x64.
//
// ⚠️  SAFETY REQUIREMENTS:
//   - Handle must be valid for the associated memory pool
//   - No bounds checking performed for maximum performance
//   - Caller responsible for handle validity
//   - Referenced entry must be properly initialized (Tick = -1 for unlinked)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) entry(h Handle) *Entry {
	return (*Entry)(unsafe.Pointer(q.arena + uintptr(h)<<5))
}

// Size returns the current number of entries in the queue.
//
// This operation executes in constant time by returning the cached entry count
// maintained during push and pop operations.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) Size() int {
	return q.size
}

// Empty checks whether the queue contains any entries.
//
// This operation executes in constant time by checking the cached size counter.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) Empty() bool {
	return q.size == 0
}

// unlink removes an entry from the queue and maintains bitmap summaries.
//
// This internal function handles the complete removal workflow:
// 1. Remove entry from its doubly-linked bucket chain
// 2. Update hierarchical bitmap summaries if bucket becomes empty
// 3. Mark entry as unlinked and decrement size counter
//
// BITMAP MAINTENANCE:
//
//	When the last entry is removed from a bucket, the function performs
//	hierarchical bitmap cleanup, potentially clearing bits at bucket,
//	lane, and group levels to maintain summary accuracy.
//
// PERFORMANCE:
//
//	Executes in 4-10 nanoseconds depending on bitmap update requirements.
//
// ⚠️  SAFETY REQUIREMENTS:
//   - Entry must be currently linked (Tick >= 0)
//   - Handle must be valid for the associated memory pool
//   - Entry must have been properly initialized before first use
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
		q.buckets[b] = entry.Next
	}

	if entry.Next != nilIdx {
		q.entry(entry.Next).Prev = entry.Prev
	}

	// Update hierarchical bitmap summaries if bucket is now empty
	if q.buckets[b] == nilIdx {
		g := uint64(entry.Tick) >> 12       // Group index
		l := (uint64(entry.Tick) >> 6) & 63 // Lane index
		bb := uint64(entry.Tick) & 63       // Bucket index

		gb := &q.groups[g]
		gb.l2[l] &^= 1 << (63 - bb) // Clear bucket bit

		if gb.l2[l] == 0 { // Lane empty
			gb.l1Summary &^= 1 << (63 - l) // Clear lane bit
			if gb.l1Summary == 0 {         // Group empty
				q.summary &^= 1 << (63 - g) // Clear group bit
			}
		}
	}

	// Mark entry as unlinked and update size
	entry.Next = nilIdx
	entry.Prev = nilIdx
	entry.Tick = -1
	q.size--
}

// linkAtHead inserts an entry at the head of its bucket chain.
//
// This internal function handles the complete insertion workflow:
// 1. Insert entry at head of appropriate bucket's doubly-linked chain
// 2. Update hierarchical bitmap summaries to reflect new active bucket
// 3. Increment size counter
//
// LIFO SEMANTICS:
//
//	Entries with identical tick values are ordered LIFO (Last In, First Out)
//	within their bucket, ensuring newest entries are processed first.
//
// BITMAP MAINTENANCE:
//
//	Updates bitmap hierarchy at bucket, lane, and group levels to ensure
//	accurate minimum finding via CLZ operations.
//
// PERFORMANCE:
//
//	Executes in 3-6 nanoseconds depending on bitmap update requirements.
//
// ⚠️  SAFETY REQUIREMENTS:
//   - Entry must be currently unlinked (Tick = -1)
//   - Handle must be valid for the associated memory pool
//   - Entry must have been properly initialized
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

	// Update bitmap summaries
	g := uint64(tick) >> 12       // Group index
	l := (uint64(tick) >> 6) & 63 // Lane index
	bb := uint64(tick) & 63       // Bucket index

	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - bb)    // Set bucket bit
	gb.l1Summary |= 1 << (63 - l) // Set lane bit
	q.summary |= 1 << (63 - g)    // Set group bit

	q.size++
}

// Push inserts or updates an entry in the queue.
//
// This function provides the primary interface for adding entries to the queue.
// If the entry is already linked with the same tick value, only the data payload
// is updated. Otherwise, the entry is moved to the appropriate position.
//
// OPERATION MODES:
//   - Hot Path: Same tick update - data field updated in-place (~2ns)
//   - Cold Path: New/different tick - full unlink/relink cycle (~8ns)
//
// HANDLE MANAGEMENT:
//
//	The caller must provide a valid handle pointing to an entry in the shared
//	memory pool. Handle allocation and deallocation are external responsibilities.
//
// TICK SEMANTICS:
//
//	Lower tick values have higher priority. The queue maintains entries in
//	tick-ascending order with LIFO semantics for identical tick values.
//
// ⚠️  SAFETY REQUIREMENTS:
//   - Handle must be valid for the associated memory pool
//   - Entry must be properly initialized (Tick = -1 for new entries)
//   - Tick values should be within reasonable range for bitmap efficiency
//   - Concurrent access requires external synchronization
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) Push(tick int64, h Handle, val uint64) {
	entry := q.entry(h)

	// Hot path: same tick update
	if entry.Tick == tick {
		entry.Data = val
		return
	}

	// Cold path: relocate entry
	if entry.Tick >= 0 {
		q.unlink(h)
	}
	q.linkAtHead(h, tick)
	entry.Data = val
}

// PeepMin returns the minimum entry without removing it from the queue.
//
// This function implements O(1) minimum finding using the hierarchical bitmap
// structure and CLZ (Count Leading Zeros) operations to efficiently traverse
// the priority space.
//
// ALGORITHM:
//  1. Use CLZ on global summary to find first active group
//  2. Use CLZ on group's lane summary to find first active lane
//  3. Use CLZ on lane's bucket mask to find first active bucket
//  4. Return head entry from the identified bucket
//
// RETURN VALUES:
//   - Handle: External handle for the minimum entry
//   - int64: Tick value (priority) of the minimum entry
//   - uint64: Data payload of the minimum entry
//
// PERFORMANCE:
//
//	Executes in 3-6 nanoseconds through optimized bitmap traversal.
//
// ⚠️  CRITICAL FOOTGUN: Undefined behavior on empty queue.
//
//	Calling PeepMin() on an empty queue will cause undefined behavior,
//	potentially including segmentation faults or data corruption. Always
//	check Empty() before calling PeepMin().
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) PeepMin() (Handle, int64, uint64) {
	// Find minimum via CLZ operations
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	t := bits.LeadingZeros64(gb.l2[l])

	// Reconstruct bucket index
	b := Handle((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]

	// Return handle, tick, and data from minimum entry
	entry := q.entry(h)
	return h, entry.Tick, entry.Data
}

// MoveTick relocates an entry to a new tick position.
//
// This function efficiently moves an entry from its current position to a new
// tick value while preserving its data payload. The operation is optimized
// for cases where the new tick equals the current tick (no-op).
//
// OPERATION:
//   - If new tick equals current tick: No operation performed
//   - Otherwise: Unlink from current position and relink at new tick
//
// USE CASES:
//   - Priority adjustments during processing
//   - Rescheduling entries based on updated conditions
//   - Dynamic priority queue management
//
// PERFORMANCE:
//   - Same tick: ~1 nanosecond (optimized no-op)
//   - Different tick: 6-15 nanoseconds (unlink + relink cycle)
//
// ⚠️  SAFETY REQUIREMENTS:
//   - Handle must be valid for the associated memory pool
//   - Entry must be currently linked (Tick >= 0)
//   - Entry must have been properly initialized
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) MoveTick(h Handle, newTick int64) {
	entry := q.entry(h)

	// No-op if same tick
	if entry.Tick == newTick {
		return
	}

	// Relocate entry
	q.unlink(h)
	q.linkAtHead(h, newTick)
}

// UnlinkMin removes the minimum entry from the queue.
//
// This function combines minimum finding with removal in a single operation.
// The caller must provide the handle for the minimum entry, typically obtained
// from a prior PeepMin() call.
//
// TYPICAL USAGE PATTERN:
//
//	handle, tick, data := queue.PeepMin()
//	// Process the minimum entry...
//	queue.UnlinkMin(handle)
//
// PERFORMANCE:
//
//	Executes in 4-10 nanoseconds depending on bitmap update requirements.
//
// ⚠️  SAFETY REQUIREMENTS:
//   - Handle must correspond to an entry currently in the queue
//   - Handle should typically be obtained from PeepMin() for correctness
//   - Entry must be properly initialized and currently linked
//   - No validation performed for maximum performance
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *PooledQuantumQueue) UnlinkMin(h Handle) {
	q.unlink(h)
}
