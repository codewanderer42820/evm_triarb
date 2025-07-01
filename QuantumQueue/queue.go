// ─────────────────────────────────────────────────────────────────────────────
// queue.go — QuantumQueue: ISR-grade static priority queue (footgun mode)
//
// Purpose:
//   - Provides O(1) Push/Pop for tick-based ISR scheduling
//   - Uses fixed arena with no heap, no atomics, no fences
//
// Architecture:
//   - 2-level bitmap summary: [Group (64)] → [Lane (64)] → [Bucket (64)]
//   - Each tick ∈ [0, 262143] indexes into a unique bucket
//   - Each bucket links to a per-tick LIFO doubly-linked list
//
// Safety Model:
//   - Footgun Grade: 10/10 — Absolutely unsafe without invariant adherence
//   - No bounds checks, no panics, no zeroing, no fencing
//
// Compiler Directives:
//   - //go:nosplit
//   - //go:inline
//   - //go:registerparams
//
// ⚠️ You are expected to be omniscient. Mistakes cause silent corruption.
// ─────────────────────────────────────────────────────────────────────────────

package quantumqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

/*─────────────────────────────────────────────────────────────────────────────*
 * Constants: tick range, bucket layout, arena capacity                        *
 *─────────────────────────────────────────────────────────────────────────────*/

const (
	GroupCount  = 64                                 // top-level summary groups
	LaneCount   = 64                                 // lanes per group
	BucketCount = GroupCount * LaneCount * LaneCount // total tick resolution
	CapItems    = 52428                              // max concurrent queue entries
)

type Handle uint32               // Opaque arena index
const nilIdx Handle = ^Handle(0) // Sentinel for freelist and linked list
type idx32 = Handle              // Used for bucket indexing and tick lookups

/*─────────────────────────────────────────────────────────────────────────────*
 * Node: 64-byte entry, contains tick key + payload + links                    *
 *─────────────────────────────────────────────────────────────────────────────*/

//go:notinheap
//go:align 64
type node struct {
	tick int64    // active tick index or -1 if free
	data [48]byte // 48-byte payload (3x cacheline)
	prev Handle   // previous node in bucket
	next Handle   // next node in bucket or freelist
}

/*─────────────────────────────────────────────────────────────────────────────*
 * groupBlock: 2-level bitmap summary per group                                *
 *─────────────────────────────────────────────────────────────────────────────*/

//go:notinheap
//go:align 64
type groupBlock struct {
	l1Summary uint64            // active lanes (1 bit per lane)
	l2        [LaneCount]uint64 // 64-bit bitmap for each lane
	_         [7]uint64         // padding to fill cachelines
}

/*─────────────────────────────────────────────────────────────────────────────*
 * QuantumQueue: Core structure                                                *
 *─────────────────────────────────────────────────────────────────────────────*/

//go:notinheap
//go:align 64
type QuantumQueue struct {
	arena   [CapItems]node         // all active entries
	buckets [BucketCount]Handle    // per-tick LIFO linked list
	groups  [GroupCount]groupBlock // summary hierarchy

	summary  uint64   // global summary (active groups)
	size     int      // active entries
	freeHead Handle   // freelist cursor
	_        [4]byte  // 8B alignment
	_        [40]byte // additional hot-path cache isolation
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Constructor                                                                *
 *─────────────────────────────────────────────────────────────────────────────*/

//go:nosplit
//go:inline
//go:registerparams
func NewQuantumQueue() *QuantumQueue {
	q := &QuantumQueue{freeHead: 0}
	for i := Handle(0); i < CapItems-1; i++ {
		q.arena[i].next = i + 1
		q.arena[i].tick = -1
		q.arena[i].prev = nilIdx
	}
	q.arena[CapItems-1].next = nilIdx
	q.arena[CapItems-1].tick = -1
	q.arena[CapItems-1].prev = nilIdx
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Freelist Borrowing                                                         *
 *─────────────────────────────────────────────────────────────────────────────*/

//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	q.freeHead = q.arena[h].next
	n := &q.arena[h]
	n.tick, n.prev, n.next = -1, nilIdx, nilIdx
	return h, nil
}

//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) BorrowSafe() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		return nilIdx, errors.New("QuantumQueue: arena exhausted")
	}
	q.freeHead = q.arena[h].next
	n := &q.arena[h]
	n.tick, n.prev, n.next = -1, nilIdx, nilIdx
	return h, nil
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Metadata Access                                                            *
 *─────────────────────────────────────────────────────────────────────────────*/

//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) Size() int {
	return q.size
}

//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) Empty() bool {
	return q.size == 0
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Internal Helpers                                                           *
 *─────────────────────────────────────────────────────────────────────────────*/

//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) unlink(h Handle) {
	n := &q.arena[h]
	b := idx32(n.tick)

	// Preload next node for perf
	if n.next != nilIdx {
		_ = *(*node)(unsafe.Pointer(uintptr(unsafe.Pointer(&q.arena[0])) + uintptr(n.next)*unsafe.Sizeof(node{})))
	}
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
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

	// Freelist return
	n.next = q.freeHead
	n.prev = nilIdx
	n.tick = -1
	q.freeHead = h
	q.size--
}

//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) linkAtHead(h Handle, tick int64) {
	n := &q.arena[h]
	b := idx32(uint64(tick))

	if q.buckets[b] != nilIdx {
		_ = *(*node)(unsafe.Pointer(uintptr(unsafe.Pointer(&q.arena[0])) + uintptr(q.buckets[b])*unsafe.Sizeof(node{})))
	}

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

/*─────────────────────────────────────────────────────────────────────────────*
 * API: Push / Peep / Move / Unlink                                           *
 *─────────────────────────────────────────────────────────────────────────────*/

//go:nosplit
//go:inline
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

//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) PeepMin() (Handle, int64, *[48]byte) {
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	t := bits.LeadingZeros64(gb.l2[l])
	b := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]
	_ = *(*node)(unsafe.Pointer(uintptr(unsafe.Pointer(&q.arena[0])) + uintptr(h)*unsafe.Sizeof(node{})))
	return h, q.arena[h].tick, &q.arena[h].data
}

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

//go:nosplit
//go:inline
//go:registerparams
func (q *QuantumQueue) UnlinkMin(h Handle, _ int64) {
	q.unlink(h)
}
