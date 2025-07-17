// ════════════════════════════════════════════════════════════════════════════════════════════════
// Compact Quantum Priority Queue
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Memory-Optimized Priority Queue
//
// Description:
//   Compact variant of the quantum priority queue using 64-bit payloads instead of 48-byte
//   arrays. Reduces memory footprint while maintaining constant-time operation guarantees through
//   the same hierarchical bitmap indexing system.
//
// Design Trade-offs:
//   - Reduced payload size (8 bytes vs 48 bytes) for memory efficiency
//   - Same algorithmic complexity and bitmap hierarchy
//   - Optimized for scenarios where payload is a pointer or small value
//   - 32-byte nodes fit two per cache line vs one per line
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package quantumqueue64

import (
	"errors"
	"math/bits"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONFIGURATION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// GroupCount defines top-level groups in the three-tier bitmap hierarchy.
	// Each group summarizes the state of 64 lanes.
	GroupCount = 64

	// LaneCount specifies lanes per group in the hierarchy.
	// Each lane tracks 64 individual priority buckets.
	LaneCount = 64

	// BucketCount represents the total number of addressable priorities.
	// Provides 262,144 distinct priority levels (64 × 64 × 64).
	BucketCount = GroupCount * LaneCount * LaneCount

	// CapItems defines maximum queue capacity.
	// Limited to 64K entries for efficient 32-bit handle addressing.
	CapItems = 1 << 16
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Handle represents an index into the queue's arena.
// 32-bit handles provide sufficient addressing for 64K entries.
type Handle uint32

// nilIdx serves as a sentinel value for invalid handles and empty links.
// Uses maximum uint32 value to distinguish from valid indices.
const nilIdx Handle = ^Handle(0)

// idx32 provides type consistency for internal indexing operations.
type idx32 = Handle

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COMPACT NODE STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// node represents a memory-efficient queue entry at 32 bytes.
// Half the size of standard QuantumQueue nodes, allowing two nodes per cache line.
//
// Memory Layout:
//   - tick: Priority value or -1 when free
//   - data: Compact 64-bit payload (vs 48 bytes in standard variant)
//   - next/prev: Doubly-linked list pointers with padding for alignment
//
//go:notinheap
//go:align 32
type node struct {
	tick int64   // 8B - Active tick or -1 if free
	data uint64  // 8B - Compact payload
	next Handle  // 4B - Next in chain or freelist
	_    [4]byte // 4B - Alignment padding
	prev Handle  // 4B - Previous in chain
	_    [4]byte // 4B - Alignment padding
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// BITMAP HIERARCHY
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// groupBlock implements the hierarchical bitmap structure for efficient minimum finding.
// Identical to the standard QuantumQueue implementation for consistency.
//
//go:notinheap
//go:align 64
type groupBlock struct {
	l1Summary uint64            // 8B - Active lanes bitmask
	l2        [LaneCount]uint64 // 512B - Per-lane bucket masks
	_         [56]byte          // 56B - Cache line padding
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN QUEUE STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// QuantumQueue64 implements a memory-efficient priority queue with compact payloads.
// Uses smaller memory footprint than standard QuantumQueue while maintaining same performance.
//
// Memory Benefits:
//   - Node size: 32 bytes vs 64 bytes (50% reduction)
//   - Arena size: 2MB vs 4MB for 64K entries
//   - Better cache utilization with 2 nodes per cache line
//
//go:notinheap
//go:align 64
type QuantumQueue64 struct {
	// Hot path metadata (16 bytes)
	summary  uint64 // 8B - Active groups bitmask
	size     int    // 4B - Current entry count
	freeHead Handle // 4B - Freelist head

	// Padding to cache line boundary (48 bytes)
	_ [48]byte // 48B - Cache isolation

	// Large data structures
	arena   [CapItems]node         // Compact allocation pool
	buckets [BucketCount]Handle    // Per-tick chain heads
	groups  [GroupCount]groupBlock // Hierarchical bitmaps
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// New creates a queue with initialized freelist and empty buckets.
// Follows the same initialization pattern as standard QuantumQueue.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New() *QuantumQueue64 {
	q := &QuantumQueue64{freeHead: 0}

	// Build freelist chain through all nodes
	for i := Handle(0); i < CapItems-1; i++ {
		q.arena[i].next = i + 1
		q.arena[i].tick = -1
		q.arena[i].prev = nilIdx
	}

	// Terminate freelist
	q.arena[CapItems-1].next = nilIdx
	q.arena[CapItems-1].tick = -1
	q.arena[CapItems-1].prev = nilIdx

	// Clear all bucket heads
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}

	return q
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HANDLE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Borrow allocates a handle without capacity validation.
// Assumes caller has verified availability for maximum speed.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) Borrow() (Handle, error) {
	h := q.freeHead
	q.freeHead = q.arena[h].next

	// Initialize to clean state
	n := &q.arena[h]
	n.tick, n.prev, n.next = -1, nilIdx, nilIdx

	return h, nil
}

// BorrowSafe provides allocation with exhaustion checking.
// Safer variant that validates capacity before allocation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) BorrowSafe() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		return nilIdx, errors.New("QuantumQueue64: arena exhausted")
	}

	q.freeHead = q.arena[h].next

	// Reset node state
	n := &q.arena[h]
	n.tick, n.prev, n.next = -1, nilIdx, nilIdx

	return h, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// QUERY OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Size returns the current number of entries.
// Maintained incrementally for constant-time access.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) Size() int {
	return q.size
}

// Empty checks if the queue contains entries.
// Convenience method for state checking.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) Empty() bool {
	return q.size == 0
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INTERNAL OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// unlink removes an entry and maintains bitmap consistency.
// Handles the full complexity of hierarchical bitmap updates.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) unlink(h Handle) {
	n := &q.arena[h]
	b := idx32(n.tick)

	// Update doubly-linked list
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// Maintain bitmap hierarchy if bucket empty
	if q.buckets[b] == nilIdx {
		// Extract hierarchical indices
		g := uint64(n.tick) >> 12       // Group
		l := (uint64(n.tick) >> 6) & 63 // Lane
		bb := uint64(n.tick) & 63       // Bucket

		gb := &q.groups[g]
		gb.l2[l] &^= 1 << (63 - bb) // Clear bucket bit

		if gb.l2[l] == 0 { // Lane empty
			gb.l1Summary &^= 1 << (63 - l) // Clear lane bit
			if gb.l1Summary == 0 {         // Group empty
				q.summary &^= 1 << (63 - g) // Clear group bit
			}
		}
	}

	// Return to freelist
	n.next = q.freeHead
	n.prev = nilIdx
	n.tick = -1
	q.freeHead = h
	q.size--
}

// linkAtHead inserts at bucket head with LIFO ordering.
// Updates all levels of the bitmap hierarchy.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) linkAtHead(h Handle, tick int64) {
	n := &q.arena[h]
	b := idx32(uint64(tick))

	// Insert at bucket head
	n.tick = tick
	n.prev = nilIdx
	n.next = q.buckets[b]
	if n.next != nilIdx {
		q.arena[n.next].prev = h
	}
	q.buckets[b] = h

	// Update bitmap hierarchy
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

// Push inserts or updates an entry with a 64-bit payload.
// The compact payload enables efficient value storage or pointer indirection.
//
// Payload Options:
//   - Direct value storage (prices, indices, flags)
//   - Pointer to larger structures (with appropriate lifetime management)
//   - Packed data structures (two 32-bit values, etc.)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) Push(tick int64, h Handle, val uint64) {
	n := &q.arena[h]

	// Fast path for same-priority updates
	if n.tick == tick {
		n.data = val
		return
	}

	// Relocate to new priority
	if n.tick >= 0 {
		q.unlink(h)
	}
	q.linkAtHead(h, tick)
	n.data = val
}

// PeepMin returns the minimum entry using bitmap traversal.
// Leverages CLZ instructions for rapid hierarchical scanning.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) PeepMin() (Handle, int64, uint64) {
	// Hierarchical minimum finding via CLZ
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	t := bits.LeadingZeros64(gb.l2[l])

	// Reconstruct bucket index
	b := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]

	return h, q.arena[h].tick, q.arena[h].data
}

// MoveTick relocates an entry to a new priority level.
// Optimized for the common case of unchanged priority.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) MoveTick(h Handle, newTick int64) {
	n := &q.arena[h]

	// Skip if priority unchanged
	if n.tick == newTick {
		return
	}

	// Perform relocation
	q.unlink(h)
	q.linkAtHead(h, newTick)
}

// UnlinkMin removes the minimum entry from the queue.
// Completes the extraction started by PeepMin.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) UnlinkMin(h Handle) {
	q.unlink(h)
}
