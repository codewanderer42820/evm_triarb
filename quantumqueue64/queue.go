// quantumqueue64 - ISR-grade compact priority queue with O(1) operations
package quantumqueue64

import (
	"errors"
	"math/bits"
	"unsafe"
)

const (
	GroupCount  = 64                                 // Top-level summary groups
	LaneCount   = 64                                 // Lanes per group
	BucketCount = GroupCount * LaneCount * LaneCount // Total tick resolution: 262,144
	CapItems    = 1 << 16                            // Maximum concurrent entries
)

// Handle represents arena index for queue entries
type Handle uint32

const nilIdx Handle = ^Handle(0) // Sentinel for null links
type idx32 = Handle

// node - 32-byte queue entry optimized for cache efficiency
//
//go:notinheap
//go:align 32
type node struct {
	tick int64   // 8B - Active tick or -1 if free
	data uint64  // 8B - Compact payload (vs 48 bytes in QuantumQueue)
	prev Handle  // 4B - Previous in chain
	_    [4]byte // 4B - Alignment padding
	next Handle  // 4B - Next in chain or freelist
	_    [4]byte // 4B - Alignment padding
}

// groupBlock - 2-level bitmap for O(1) minimum finding
//
//go:notinheap
//go:align 64
type groupBlock struct {
	l1Summary uint64            // 8B - Active lanes bitmask
	l2        [LaneCount]uint64 // 512B - Per-lane bucket masks
	_         [56]byte          // 56B - Cache line padding
}

// QuantumQueue64 - High-performance priority queue with 50% smaller footprint
//
//go:notinheap
//go:align 64
type QuantumQueue64 struct {
	// Hot path metadata (16 bytes)
	summary  uint64 // 8B - Active groups bitmask
	size     int    // 4B - Current entry count
	freeHead Handle // 4B - Freelist head

	// Padding to cache line boundary (48 bytes)
	_ [6]uint64 // 48B - Cache isolation

	// Large data structures
	arena   [CapItems]node         // Fixed allocation pool (2MB vs 4MB)
	buckets [BucketCount]Handle    // Per-tick chain heads
	groups  [GroupCount]groupBlock // Hierarchical bitmaps
}

// New creates a queue with initialized freelist and empty buckets
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New() *QuantumQueue64 {
	q := &QuantumQueue64{freeHead: 0}

	// Initialize freelist chain
	for i := Handle(0); i < CapItems-1; i++ {
		q.arena[i].next = i + 1
		q.arena[i].tick = -1
		q.arena[i].prev = nilIdx
	}

	// Terminate freelist
	q.arena[CapItems-1].next = nilIdx
	q.arena[CapItems-1].tick = -1
	q.arena[CapItems-1].prev = nilIdx

	// Initialize empty buckets
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}

	return q
}

// Borrow allocates a handle without capacity checking
// ⚠️  FOOTGUN: No exhaustion validation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) Borrow() (Handle, error) {
	h := q.freeHead
	q.freeHead = q.arena[h].next

	// Reset node state
	n := &q.arena[h]
	n.tick, n.prev, n.next = -1, nilIdx, nilIdx

	return h, nil
}

// BorrowSafe allocates with exhaustion checking
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

// Size returns current entry count
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) Size() int {
	return q.size
}

// Empty checks if queue contains entries
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) Empty() bool {
	return q.size == 0
}

// unlink removes entry and updates bitmap summaries
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) unlink(h Handle) {
	n := &q.arena[h]
	b := idx32(n.tick)

	// Prefetch next node
	if n.next != nilIdx {
		_ = *(*node)(unsafe.Pointer(uintptr(unsafe.Pointer(&q.arena[0])) +
			uintptr(n.next)*unsafe.Sizeof(node{})))
	}

	// Remove from chain
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// Update summaries if bucket empty
	if q.buckets[b] == nilIdx {
		g := uint64(n.tick) >> 12       // Group index
		l := (uint64(n.tick) >> 6) & 63 // Lane index
		bb := uint64(n.tick) & 63       // Bucket index

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

// linkAtHead inserts at bucket head with LIFO ordering
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) linkAtHead(h Handle, tick int64) {
	n := &q.arena[h]
	b := idx32(uint64(tick))

	// Prefetch bucket head
	if q.buckets[b] != nilIdx {
		_ = *(*node)(unsafe.Pointer(uintptr(unsafe.Pointer(&q.arena[0])) +
			uintptr(q.buckets[b])*unsafe.Sizeof(node{})))
	}

	// Insert at head
	n.tick = tick
	n.prev = nilIdx
	n.next = q.buckets[b]
	if n.next != nilIdx {
		q.arena[n.next].prev = h
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

// Push inserts or updates entry at specified tick
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) Push(tick int64, h Handle, val uint64) {
	n := &q.arena[h]

	// Hot path: same tick update
	if n.tick == tick {
		n.data = val
		return
	}

	// Cold path: relocate entry
	if n.tick >= 0 {
		q.unlink(h)
	}
	q.linkAtHead(h, tick)
	n.data = val
}

// PeepMin returns minimum without removal using bitmap traversal
// ⚠️  FOOTGUN: Undefined on empty queue
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) PeepMin() (Handle, int64, uint64) {
	// Find minimum via CLZ operations
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	t := bits.LeadingZeros64(gb.l2[l])

	// Reconstruct bucket index
	b := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]

	// Prefetch minimum entry
	_ = *(*node)(unsafe.Pointer(uintptr(unsafe.Pointer(&q.arena[0])) +
		uintptr(h)*unsafe.Sizeof(node{})))

	return h, q.arena[h].tick, q.arena[h].data
}

// MoveTick relocates entry to new tick
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) MoveTick(h Handle, newTick int64) {
	n := &q.arena[h]

	// No-op if same tick
	if n.tick == newTick {
		return
	}

	// Relocate entry
	q.unlink(h)
	q.linkAtHead(h, newTick)
}

// UnlinkMin removes the minimum entry
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue64) UnlinkMin(h Handle) {
	q.unlink(h)
}
