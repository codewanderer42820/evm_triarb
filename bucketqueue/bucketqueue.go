// Package bucketqueue implements a zero-allocation, low-latency time-bucket priority queue.
// Items are distributed across a fixed-size sliding window of time-indexed buckets.
// A two-level bitmap structure allows O(1) retrieval of the earliest item.
//
// This implementation uses a fixed arena allocator, intrusive linked lists,
// and compact handle management for high-throughput applications such as
// schedulers, simulation engines, or event queues.
package bucketqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

// ---------------------------------------------------------------------------
// Tunable compile-time constants.
// ---------------------------------------------------------------------------

const (
	// numBuckets must be a power-of-two to allow cheap modulus operations via bitmasking.
	numBuckets = 4096

	// Each uint64 in groupBits maps to 64 consecutive buckets. This reduces scanning cost
	// during PeepMin and PopMin to a 2-level bitmap traversal.
	numGroups = numBuckets / 64

	// Max number of simultaneously active handles in the arena. Fits cleanly in a uint16 index.
	capItems = 1 << 16
)

// idx32 is the internal arena index type for tracking bucket nodes.
// A sentinel value of all-ones represents nil (invalid).
type idx32 uint32

const nilIdx idx32 = ^idx32(0)

// ---------------------------------------------------------------------------
// node stores metadata for each handle in the arena.
// Structured to occupy 32 bytes for cache locality (fits 2 per 64-byte line).
// ---------------------------------------------------------------------------
type node struct {
	tick  int64          // absolute tick this node is scheduled for
	data  unsafe.Pointer // user-supplied payload
	next  idx32          // linked-list next within bucket
	prev  idx32          // linked-list prev within bucket
	count uint32         // count of pushes at same tick (used for de-duplication)
	_pad  uint32         // padding for 8-byte alignment (U1000 ignored)
}

// ---------------------------------------------------------------------------
// Queue represents a fixed-capacity, lock-free, multi-bucket timing wheel.
// ---------------------------------------------------------------------------
type Queue struct {
	arena     [capItems]node    // preallocated arena for node handles
	buckets   [numBuckets]idx32 // circular bucket list heads
	groupBits [numGroups]uint64 // 64-bit bitmap per 64 buckets

	baseTick uint64 // absolute tick for bucket index 0
	summary  uint64 // top-level bitmap summarizing non-empty groups

	size     int   // total number of scheduled entries
	freeHead idx32 // index of next free node in arena
}

// ---------------------------------------------------------------------------
// Public error sentinels — do not rely on string contents.
// ---------------------------------------------------------------------------
var (
	ErrFull         = errors.New("bucketqueue: no free handles")
	ErrPastWindow   = errors.New("bucketqueue: tick too far in the past")
	ErrBeyondWindow = errors.New("bucketqueue: tick too far in the future")
	ErrItemNotFound = errors.New("bucketqueue: invalid handle")
)

// Handle is an opaque identifier for queued items.
// Zero value is invalid (reserved for nilIdx).
//
//go:inline
//go:nosplit
//nolint:revive
//lint:ignore U1000 exported type used by consumers

type Handle idx32

// New creates a fresh empty Queue.
// All internal memory is preallocated — no GC pressure.
func New() *Queue {
	q := &Queue{}

	// Initialize freelist backwards (skip Handle 0).
	for i := capItems - 1; i > 1; i-- {
		q.arena[i-1].next = idx32(i)
	}
	q.arena[capItems-1].next = nilIdx
	q.freeHead = 1

	// Clear all bucket heads.
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}

	return q
}

// Borrow acquires a free handle from the arena.
//
//go:nosplit
//go:inline
func (q *Queue) Borrow() (Handle, error) {
	if q.freeHead == nilIdx {
		return Handle(nilIdx), ErrFull
	}
	h := q.freeHead
	n := &q.arena[h]
	q.freeHead = n.next
	n.next, n.prev, n.count = nilIdx, nilIdx, 0
	return Handle(h), nil
}

// Push schedules a handle for execution at a specified tick.
// If the same handle is pushed to the same tick again, it increments a count.
//
//go:nosplit
func (q *Queue) Push(tick int64, h Handle, val unsafe.Pointer) error {
	if h >= Handle(capItems) {
		return ErrItemNotFound
	}
	delta := uint64(tick) - q.baseTick
	switch {
	case delta >= 1<<63:
		return ErrPastWindow
	case delta >= numBuckets:
		return ErrBeyondWindow
	}

	idx := idx32(h)
	n := &q.arena[idx]

	if n.count != 0 && n.tick == tick {
		n.count++
		q.size++
		n.data = val
		return nil
	}

	if n.count != 0 {
		if n.prev != nilIdx {
			q.arena[n.prev].next = n.next
		} else {
			old := uint64(n.tick) - q.baseTick
			q.buckets[old] = n.next
			if q.buckets[old] == nilIdx {
				g := old >> 6
				q.groupBits[g] &^= 1 << (old & 63)
				if q.groupBits[g] == 0 {
					q.summary &^= 1 << g
				}
			}
		}
		if n.next != nilIdx {
			q.arena[n.next].prev = n.prev
		}
		q.size -= int(n.count)
		n.next, n.prev = nilIdx, nilIdx
	}

	bkt := delta
	n.next, n.prev = q.buckets[bkt], nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[bkt] = idx
	n.tick, n.count, n.data = tick, 1, val

	g := bkt >> 6
	if (q.groupBits[g] & (1 << (bkt & 63))) == 0 {
		q.groupBits[g] |= 1 << (bkt & 63)
		q.summary |= 1 << g
	}

	q.size++
	return nil
}

// Update changes the tick of a handle that is already in the queue.
//
//go:nosplit
func (q *Queue) Update(tick int64, h Handle, val unsafe.Pointer) error {
	if h >= Handle(capItems) {
		return ErrItemNotFound
	}
	idx := idx32(h)
	n := &q.arena[idx]
	if n.count == 0 {
		return ErrItemNotFound
	}

	old := uint64(n.tick) - q.baseTick
	q.size -= int(n.count)
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[old] = n.next
		if q.buckets[old] == nilIdx {
			g := old >> 6
			q.groupBits[g] &^= 1 << (old & 63)
			if q.groupBits[g] == 0 {
				q.summary &^= 1 << g
			}
		}
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
	n.next, n.prev, n.count = nilIdx, nilIdx, 0
	return q.Push(tick, h, val)
}

// PeepMin reads the next earliest entry without removing it.
//
//go:nosplit
//go:inline
func (q *Queue) PeepMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	b := bits.TrailingZeros64(q.groupBits[g])
	bkt := uint64(g<<6 | b)

	h := q.buckets[bkt]
	n := &q.arena[h]
	return Handle(h), n.tick, n.data
}

// PopMin removes and returns the earliest enqueued entry.
//
//go:nosplit
func (q *Queue) PopMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	b := bits.TrailingZeros64(q.groupBits[g])
	bkt := uint64(g<<6 | b)

	h := q.buckets[bkt]
	n := &q.arena[h]

	if n.count > 1 {
		n.count--
		q.size--
		return Handle(h), n.tick, n.data
	}

	q.buckets[bkt] = n.next
	if n.next != nilIdx {
		q.arena[n.next].prev = nilIdx
	}
	q.size--

	if q.buckets[bkt] == nilIdx {
		q.groupBits[g] &^= 1 << (bkt & 63)
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << g
		}
	}

	retData := n.data
	retTick := n.tick

	n.next, n.prev, n.count, n.data = nilIdx, nilIdx, 0, nil
	q.freeHead, n.next = h, q.freeHead

	return Handle(h), retTick, retData
}

// Size returns the total number of entries in the queue.
//
//go:nosplit
//go:inline
func (q *Queue) Size() int { return q.size }

// Empty returns true if the queue contains no entries.
//
//go:nosplit
//go:inline
func (q *Queue) Empty() bool { return q.size == 0 }
