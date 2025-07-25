// ════════════════════════════════════════════════════════════════════════════════════════════════
// Hierarchical Bitmap Priority Queue - Compact 128-Priority Variant
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Memory-Optimized Priority Queue with Zero-Init Compatibility
//
// Description:
//   Fixed-capacity priority queue with constant-time operations using hierarchical bitmap indexing.
//   Supports 128 distinct priority levels with minimum extraction through hardware-accelerated
//   bit manipulation instructions. Optimized for true zero-initialization compatibility.
//
// Features:
//   - Three-level bitmap hierarchy for efficient minimum finding
//   - Zero-initialization compatible (Handle(0) = invalid, no setup required)
//   - Minimal memory footprint (1.1KB vs 37KB+ for full variant)
//   - Hardware CLZ instructions enable rapid priority scanning
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package compactqueue128

import (
	"math/bits"
	"unsafe"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONFIGURATION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// BucketCount represents total addressable priority levels.
	// Constrained to 128 priorities to enable dramatic memory footprint reduction
	// while maintaining the essential algorithmic properties of the hierarchical bitmap.
	BucketCount = 128

	// GroupCount defines the number of top-level summary groups in the bitmap hierarchy.
	// Preserved at 64 for bitmap compatibility, though only group 0 contains active data.
	// This sparse utilization strategy reduces memory while preserving addressing logic.
	GroupCount = 64

	// LaneCount specifies lanes per group in the middle hierarchy level.
	// Maintained at 64 for algorithmic consistency, with only lanes 0-1 utilized
	// to provide 64 buckets per lane × 2 active lanes = 128 total addressable buckets.
	LaneCount = 64
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Handle represents an index into the queue's external arena.
// Uses 1-based indexing where Handle(0) represents invalid/null for zero-initialization
// compatibility. Valid handles begin at Handle(1) mapping to pool[0].
type Handle uint64

// nilIdx serves as a sentinel value indicating no link or invalid handle.
// Set to zero to align with Go's zero-value semantics, enabling structures
// to be valid immediately upon zero-initialization without explicit setup.
const nilIdx Handle = 0

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COMPACT NODE STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Entry represents a single queue element with minimal memory footprint.
// The 32-byte size ensures optimal cache alignment and memory efficiency.
//
// Field Layout:
//   - Tick: Internal priority value (user_tick + 1) or 0 when free
//   - Data: Compact 64-bit payload for value storage or pointer indirection
//   - Next/Prev: Doubly-linked list pointers for constant-time operations
//
//go:notinheap
//go:align 32
type Entry struct {
	// Tick stores internal priority values offset by +1 from user priorities.
	// This offset scheme enables zero to represent the free state, aligning with
	// zero-initialization semantics where uninitialized entries are naturally free.
	Tick int64  // 8B - Internal tick (user_tick + 1) or 0 if free
	Data uint64 // 8B - Compact payload
	Next Handle // 8B - Next in chain (0 = nil, 1+ = valid)
	Prev Handle // 8B - Previous in chain (0 = nil, 1+ = valid)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// BITMAP HIERARCHY
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// groupBlock implements the middle level of the three-tier bitmap hierarchy.
// Optimized for memory efficiency by storing only the minimum lanes required
// for 128-priority support, rather than the full 64-lane capacity.
//
// Bitmap Organization:
//   - l1Summary: Only bits 63,62 used (lanes 0,1)
//   - l2: Only l2[0] and l2[1] used for 64 buckets each
//   - Padding ensures exclusive cache line ownership
//
//go:notinheap
//go:align 64
type groupBlock struct {
	l1Summary uint64 // 8B - Active lanes mask (only bits 63,62 used)
	// Reduced lane array stores only the two active lanes needed for 128 priorities.
	// This provides 89% memory reduction per group compared to the full 64-lane variant.
	l2 [2]uint64 // 16B - Per-lane bucket masks (only [0],[1] used)
	_  [40]byte  // 40B - Cache line padding
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN QUEUE STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// CompactQueue128 implements a static-capacity priority queue with hierarchical bitmap indexing.
// Optimized for minimal memory usage while maintaining constant-time operations.
//
// Memory Layout:
//   - Hot metadata (summary, size, arena) fits in first cache line
//   - Minimal arrays (buckets, groups) reduce total footprint
//   - True zero-initialization compatible - no setup required
//
//go:notinheap
//go:align 64
type CompactQueue128 struct {
	// Hot path metadata (24 bytes) - accessed on every operation
	summary uint64  // 8B - Global active groups mask (only bit 63 used)
	size    int     // 8B - Current entry count
	arena   uintptr // 8B - Base pointer to shared pool

	// Padding to cache line boundary (40 bytes)
	_ [40]byte // 40B - Cache isolation

	// Bucket array dimensioned specifically for 128 priorities, achieving
	// a 2MB+ memory reduction compared to the full-capacity variant.
	buckets [BucketCount]Handle // 128 buckets × 8B = 1024B (zero-init: all 0 = nil)
	// Single group sufficient for 128 priorities, eliminating 36KB of group storage
	// compared to the 64-group full-capacity design.
	groups [1]groupBlock // 1 group × 64B = 64B (zero-init: all empty)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// New creates an initialized queue using the provided memory pool.
// Leverages zero-initialization semantics to eliminate setup overhead.
// All buckets initialize to Handle(0) = nilIdx, representing empty buckets.
//
// Zero-Init Compatibility:
//  1. All buckets zero-init to Handle(0) = nilIdx (empty)
//  2. All bitmap summaries zero-init to 0 (empty)
//  3. Handle(0) is invalid - valid handles start at Handle(1)
//  4. Entry.Tick = 0 means free, >0 means active
//  5. No initialization loops needed
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New(arena unsafe.Pointer) *CompactQueue128 {
	return &CompactQueue128{arena: uintptr(arena)}
	// Zero-initialization provides valid empty state without explicit setup
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MEMORY ACCESS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// entry converts a handle to its corresponding entry pointer.
// Uses offset-based addressing where Handle(1) maps to pool[0], enabling
// Handle(0) to serve as a natural null sentinel without requiring special cases.
//
// Address Calculation:
//
//	address = arena_base + ((handle - 1) × sizeof(Entry))
//	Optimized using shift for 32-byte Entry size
//
// Safety Requirements:
//   - No bounds checking for maximum speed
//   - Caller must ensure handle validity (Handle >= 1)
//   - Invalid handles cause memory corruption
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) entry(h Handle) *Entry {
	// Offset addressing: Handle 1 = pool[0], Handle 2 = pool[1], etc.
	// Shift by 5 for 32-byte entries (2^5 = 32)
	return (*Entry)(unsafe.Pointer(q.arena + uintptr(h-1)<<5))
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
func (q *CompactQueue128) Size() int {
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
func (q *CompactQueue128) Empty() bool {
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
func (q *CompactQueue128) unlink(h Handle) {
	entry := q.entry(h)
	// Convert internal tick back to user tick for bucket indexing.
	// Internal ticks are offset by +1, so user_tick = internal_tick - 1.
	userTick := entry.Tick - 1
	b := Handle(userTick)

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
		// Decompose user tick for compact addressing scheme.
		// Group index is always 0 since all 128 priorities fit in single group.
		// Lane index uses bit 6 (values 0 or 1 for lanes 0-1).
		// Bucket index uses bottom 6 bits (values 0-63 within each lane).
		g := uint64(userTick) >> 12       // Group index (always 0 for 0-127)
		l := (uint64(userTick) >> 6) & 63 // Lane index (0 or 1 for 0-127)
		bb := uint64(userTick) & 63       // Bucket index (bottom 6 bits)

		// Reference the single active group in compact design.
		gb := &q.groups[g] // Always groups[0]
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

	// Mark entry as unlinked using zero-compatible sentinel values.
	// Tick = 0 represents free state, aligning with zero-initialization semantics.
	entry.Next = nilIdx
	entry.Prev = nilIdx
	entry.Tick = 0 // 0 = free (zero-init state)
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
func (q *CompactQueue128) linkAtHead(h Handle, userTick int64) {
	entry := q.entry(h)
	// Store internal tick (user + 1) while using user tick for bucket indexing.
	// This dual-tick approach enables zero to represent free state internally.
	internalTick := userTick + 1
	b := Handle(uint64(userTick))

	// Insert at head of bucket chain
	entry.Tick = internalTick
	entry.Prev = nilIdx
	entry.Next = q.buckets[b]
	if entry.Next != nilIdx {
		q.entry(entry.Next).Prev = h
	}
	q.buckets[b] = h

	// Update hierarchical bitmap summaries using compact addressing.
	// Decomposition optimized for single-group, dual-lane configuration.
	g := uint64(userTick) >> 12       // Group index (always 0 for 0-127)
	l := (uint64(userTick) >> 6) & 63 // Lane index (0 or 1 for 0-127)
	bb := uint64(userTick) & 63       // Bucket index (bottom 6 bits)

	// Reference the single active group in compact design.
	gb := &q.groups[g]            // Always groups[0]
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
func (q *CompactQueue128) Push(userTick int64, h Handle, val uint64) {
	entry := q.entry(h)
	// Convert to internal tick for comparison with stored entry state.
	// Internal tick = user tick + 1 enables zero to represent free entries.
	internalTick := userTick + 1

	// Hot path: same priority update
	if entry.Tick == internalTick {
		entry.Data = val
		return
	}

	// Cold path: relocate to new priority
	// Check for active state using internal tick > 0 (free state is 0).
	if entry.Tick > 0 { // Was linked (internal tick > 0 means active)
		q.unlink(h) // Remove from current position
	}
	q.linkAtHead(h, userTick)
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
func (q *CompactQueue128) PeepMin() (Handle, int64, uint64) {
	// Find minimum through hierarchical bitmap traversal
	g := bits.LeadingZeros64(q.summary) // Find first group
	// Reference the single active group in compact design.
	gb := &q.groups[g]                     // Always groups[0]
	l := bits.LeadingZeros64(gb.l1Summary) // Find first lane in group
	t := bits.LeadingZeros64(gb.l2[l])     // Find first bucket in lane

	// Reconstruct bucket index from hierarchical components
	b := Handle((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]

	// Convert internal tick back to user-visible tick for API consistency.
	// User tick = internal tick - 1 reverses the offset applied during storage.
	entry := q.entry(h)
	userTick := entry.Tick - 1 // Convert back to user tick
	return h, userTick, entry.Data
}

// MoveTick efficiently relocates an entry to a new priority.
// Optimized for the common case where priority doesn't change.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) MoveTick(h Handle, newUserTick int64) {
	entry := q.entry(h)
	// Convert to internal tick for comparison with stored entry state.
	newInternalTick := newUserTick + 1

	// No-op if priority unchanged
	if entry.Tick == newInternalTick {
		return
	}

	// Relocate to new priority
	q.unlink(h)
	q.linkAtHead(h, newUserTick)
}

// UnlinkMin removes the minimum entry from the queue.
// Typically called after PeepMin to complete extraction.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) UnlinkMin(h Handle) {
	q.unlink(h)
}
