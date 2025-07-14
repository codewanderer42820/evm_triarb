// quantumqueue - ISR-grade priority queue with O(1) operations
package quantumqueue

import (
	"errors"
	"math/bits"
)

const (
	GroupCount  = 64                                 // Top-level summary groups
	LaneCount   = 64                                 // Lanes per group
	BucketCount = GroupCount * LaneCount * LaneCount // Total: 262,144 buckets
	CapItems    = 1 << 16                            // Maximum entries: 65,536
)

// Handle represents arena index for queue entries
type Handle uint32

const nilIdx Handle = ^Handle(0) // Sentinel value
type idx32 = Handle

// node - 64-byte entry optimized for cache lines
//
//go:notinheap
//go:align 64
type node struct {
	tick int64    // 8B - Active tick or -1 if free
	data [48]byte // 48B - User payload (3/4 cache line)
	prev Handle   // 4B - Previous in chain
	next Handle   // 4B - Next in chain or freelist
}

// groupBlock - 2-level bitmap for O(1) minimum finding
//
//go:notinheap
//go:align 64
type groupBlock struct {
	l1Summary uint64            // 8B - Active lanes mask
	l2        [LaneCount]uint64 // 512B - Per-lane bucket masks
	_         [56]byte          // 56B - Cache line padding
}

// QuantumQueue - Static priority queue with hierarchical bitmaps
//
//go:notinheap
//go:align 64
type QuantumQueue struct {
	// Hot path metadata (16 bytes)
	summary  uint64 // 8B - Global active groups mask
	size     int    // 4B - Current entry count
	freeHead Handle // 4B - Freelist head

	// Padding to cache line boundary (48 bytes)
	_ [48]byte // 48B - Cache isolation

	// Large data structures
	arena   [CapItems]node         // Fixed allocation pool
	buckets [BucketCount]Handle    // Per-tick chain heads
	groups  [GroupCount]groupBlock // Hierarchical summaries
}

// New creates initialized queue with full capacity
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New() *QuantumQueue {
	q := &QuantumQueue{freeHead: 0}

	// Initialize freelist
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

// Borrow allocates handle without capacity check
// ⚠️  FOOTGUN: No exhaustion validation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	q.freeHead = q.arena[h].next

	// Reset node
	n := &q.arena[h]
	n.tick, n.prev, n.next = -1, nilIdx, nilIdx

	return h, nil
}

// BorrowSafe allocates with capacity checking
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) BorrowSafe() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		return nilIdx, errors.New("QuantumQueue: arena exhausted")
	}

	q.freeHead = q.arena[h].next

	// Reset node
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
func (q *QuantumQueue) Size() int {
	return q.size
}

// Empty checks if queue is empty
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) Empty() bool {
	return q.size == 0
}

// unlink removes entry and maintains bitmap summaries
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) unlink(h Handle) {
	n := &q.arena[h]
	b := idx32(n.tick)

	// Remove from chain
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// Update hierarchical summaries
	if q.buckets[b] == nilIdx {
		g := uint64(n.tick) >> 12       // Group index
		l := (uint64(n.tick) >> 6) & 63 // Lane index
		bb := uint64(n.tick) & 63       // Bucket index

		gb := &q.groups[g]
		gb.l2[l] &^= 1 << (63 - bb)

		if gb.l2[l] == 0 {
			gb.l1Summary &^= 1 << (63 - l)
			if gb.l1Summary == 0 {
				q.summary &^= 1 << (63 - g)
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

// linkAtHead inserts at bucket head
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) linkAtHead(h Handle, tick int64) {
	n := &q.arena[h]
	b := idx32(uint64(tick))

	// Insert at head
	n.tick = tick
	n.prev = nilIdx
	n.next = q.buckets[b]
	if n.next != nilIdx {
		q.arena[n.next].prev = h
	}
	q.buckets[b] = h

	// Update summaries
	g := uint64(tick) >> 12
	l := (uint64(tick) >> 6) & 63
	bb := uint64(tick) & 63

	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - bb)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)

	q.size++
}

// Push inserts or updates entry
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) Push(tick int64, h Handle, val *[48]byte) {
	n := &q.arena[h]

	// Hot path: update only
	if n.tick == tick {
		n.data = *val
		return
	}

	// Cold path: relocate
	if n.tick >= 0 {
		q.unlink(h)
	}
	q.linkAtHead(h, tick)
	n.data = *val
}

// PeepMin returns minimum using CLZ operations
// ⚠️  FOOTGUN: Undefined on empty queue
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) PeepMin() (Handle, int64, *[48]byte) {
	// Find minimum via bitmaps
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	t := bits.LeadingZeros64(gb.l2[l])

	// Get bucket head
	b := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]

	return h, q.arena[h].tick, &q.arena[h].data
}

// MoveTick relocates entry to new tick
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) MoveTick(h Handle, newTick int64) {
	n := &q.arena[h]

	if n.tick == newTick {
		return
	}

	q.unlink(h)
	q.linkAtHead(h, newTick)
}

// UnlinkMin removes minimum entry
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) UnlinkMin(h Handle) {
	q.unlink(h)
}
