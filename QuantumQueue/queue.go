package quantumqueue

import (
	"errors"    // For safe BorrowSafe error handling
	"math/bits" // For fast leading-zero count
)

const (
	GroupCount  = 64                                 // Top-level groups (4096 ticks each)
	LaneCount   = 64                                 // Lanes per group (64 ticks each)
	BucketCount = GroupCount * LaneCount * LaneCount // 262144 ticks
	CapItems    = BucketCount                        // Max handles (one per tick)
)

type Handle uint32

const nilIdx Handle = ^Handle(0)

type idx32 = Handle

// node is a fixed-size arena entry for one tick.
// Layout is optimized for cache line alignment (64B).
//
//go:notinheap
//go:align 64
//go:nosplit
//go:inline
type node struct {
	tick int64    // 8B: active tick or -1 if unused
	data [52]byte // 52B: inline payload
	next Handle   // 4B: freelist or bucket link
}

// groupBlock holds two-level bitmaps for 4096 ticks.
// Layout is 9×64B cache lines (576 bytes total).
//
//go:notinheap
//go:nosplit
//go:inline
type groupBlock struct {
	l1Summary uint64            // 8B: summary of active lanes in group
	l2        [LaneCount]uint64 // 512B: per-lane bucket occupancy
	_         [56]byte          // 56B padding to reach 576B total
}

// QuantumQueue is the arena-backed fixed-priority queue.
// Field layout is optimized for hot-path locality:
// arena → buckets → groups → metadata.
//
//go:notinheap
//go:nosplit
//go:inline
type QuantumQueue struct {
	arena    [CapItems]node         // HOT: payload and tick storage
	buckets  [BucketCount]Handle    // HOT: direct tick→handle mapping
	groups   [GroupCount]groupBlock // MODERATE: bitmap summary layers
	summary  uint64                 // metadata: top-level group bitmap
	size     int                    // metadata: number of active entries
	freeHead Handle                 // metadata: freelist head
	_        [4]byte                // align to 8B
	_        [40]byte               // pad to full 64B header
}

// NewQuantumQueue initializes an empty queue.
//
//go:inline
//go:nosplit
func NewQuantumQueue() *QuantumQueue {
	q := &QuantumQueue{freeHead: 0}
	for i := Handle(0); i < CapItems-1; i++ {
		q.arena[i].next = i + 1
		q.arena[i].tick = -1
	}
	q.arena[CapItems-1].next = nilIdx
	q.arena[CapItems-1].tick = -1
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// Borrow retrieves a free handle (unsafe, caller must avoid exhaustion).
//
//go:inline
//go:nosplit
func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	q.freeHead = q.arena[h].next
	q.arena[h].tick = -1
	return h, nil
}

// BorrowSafe retrieves a free handle with exhaustion check.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) BorrowSafe() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		return nilIdx, errors.New("QuantumQueue: arena exhausted")
	}
	q.freeHead = q.arena[h].next
	q.arena[h].tick = -1
	return h, nil
}

// Size returns active entry count.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) Size() int {
	return q.size
}

// Empty reports if queue is empty.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) Empty() bool {
	return q.size == 0
}

// Push sets or updates handle to tick with payload.
// Caller must avoid tick collisions.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) Push(tick int64, h Handle, val []byte) {
	n := &q.arena[h]
	if n.tick == tick {
		copy(n.data[:], val)
		return
	}
	if n.tick >= 0 {
		d := uint64(n.tick)
		b := idx32(d)
		q.buckets[b] = nilIdx
		g, l, bb := d>>12, (d>>6)&63, d&63
		gb := &q.groups[g]
		gb.l2[l] &^= 1 << (63 - bb)
		if gb.l2[l] == 0 {
			gb.l1Summary &^= 1 << (63 - l)
			if gb.l1Summary == 0 {
				q.summary &^= 1 << (63 - g)
			}
		}
		q.size--
	}
	// link new
	b2 := idx32(uint64(tick))
	n.next = nilIdx
	n.tick = tick
	q.buckets[b2] = h
	g2, l2, bb2 := uint64(tick)>>12, (uint64(tick)>>6)&63, uint64(tick)&63
	gb2 := &q.groups[g2]
	gb2.l2[l2] |= 1 << (63 - bb2)
	gb2.l1Summary |= 1 << (63 - l2)
	q.summary |= 1 << (63 - g2)
	copy(n.data[:], val)
	q.size++
}

// MoveTick repositions handle to newTick.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) MoveTick(h Handle, newTick int64) {
	n := &q.arena[h]
	if n.tick == newTick {
		return
	}
	if n.tick >= 0 {
		d := uint64(n.tick)
		q.buckets[idx32(d)] = nilIdx
		g, l, bb := d>>12, (d>>6)&63, d&63
		gb := &q.groups[g]
		gb.l2[l] &^= 1 << (63 - bb)
		if gb.l2[l] == 0 {
			gb.l1Summary &^= 1 << (63 - l)
			if gb.l1Summary == 0 {
				q.summary &^= 1 << (63 - g)
			}
		}
	}
	b3 := idx32(uint64(newTick))
	n.next = nilIdx
	n.tick = newTick
	q.buckets[b3] = h
	g3, l3, bb3 := uint64(newTick)>>12, (uint64(newTick)>>6)&63, uint64(newTick)&63
	gb3 := &q.groups[g3]
	gb3.l2[l3] |= 1 << (63 - bb3)
	gb3.l1Summary |= 1 << (63 - l3)
	q.summary |= 1 << (63 - g3)
}

// PeepMin finds the earliest tick and returns its handle and payload.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) PeepMin() (Handle, int64, *[52]byte) {
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	t := bits.LeadingZeros64(gb.l2[l])
	b := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]
	n := &q.arena[h]
	return h, n.tick, &n.data
}

var zeroPayload [52]byte

// PeepMinSafe returns nilIdx and zero payload when empty.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) PeepMinSafe() (Handle, int64, *[52]byte) {
	if q.size == 0 {
		return nilIdx, 0, &zeroPayload
	}
	return q.PeepMin()
}

// UnlinkMin removes the min tick, clears bitmaps, and recycles handle.
//
//go:inline
//go:nosplit
func (q *QuantumQueue) UnlinkMin(h Handle, tick int64) {
	if tick < 0 {
		return
	}
	b := idx32(uint64(tick))
	q.buckets[b] = nilIdx
	g, l, bb := uint64(tick)>>12, (uint64(tick)>>6)&63, uint64(tick)&63
	gb := &q.groups[g]
	gb.l2[l] &^= 1 << (63 - bb)
	if gb.l2[l] == 0 {
		gb.l1Summary &^= 1 << (63 - l)
		if gb.l1Summary == 0 {
			q.summary &^= 1 << (63 - g)
		}
	}
	// recycle
	n := &q.arena[h]
	n.next = q.freeHead
	n.tick = -1
	q.freeHead = h
	q.size--
}
