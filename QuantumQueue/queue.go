// Ultra-low-latency QuantumQueue — Apex Edition
// • Adds PushIfFree(h) to skip push if handle already in use

package quantumqueue

import (
	"math/bits"
)

const (
	GroupCount  = 64
	LaneCount   = 64
	BucketCount = GroupCount * LaneCount * LaneCount
	CapItems    = 1 << 14
)

type Handle uint32

const nilIdx Handle = ^Handle(0)

type idx32 = Handle

type node struct {
	tick  int64    // 8 B: Tick timestamp (hot for delta math)
	data  [44]byte // 44 B: Inline payload (user-facing, copied)
	next  Handle   // 4 B: Arena next pointer
	prev  Handle   // 4 B: Arena prev pointer
	count uint32   // 4 B: Entry count for duplicate ticks
}

type groupBlock struct {
	l1Summary uint64            // 8 B: bitmap of active l2 entries
	l2        [LaneCount]uint64 // 512 B: per-lane bitmap of active buckets
	_         [56]byte          // 56 B: pad to 576 B (9 × 64B cachelines)
}

type QuantumQueue struct {
	summary  uint64 //  8 B  @0  — Top-level group bitmap (PeepMin root)
	baseTick uint64 //  8 B  @8  — Sliding window base for tick deltas
	size     int    //  8 B @16  — Active item count (PopMin/Push)

	freeHead Handle  //  4 B @24  — Arena free list head
	_        [4]byte //  4 B @28  — Align freeHead to 8B boundary

	_ [32]byte // 32 B @32–63 — Pad to full 64-byte struct header

	// ───── Hot arrays follow ─────

	buckets [BucketCount]Handle    // 1-entry-per-tick ring: 262144 × 4 B
	groups  [GroupCount]groupBlock // 64 × 576 B = 36 KB bitmap blocks
	arena   [CapItems]node         // 16384 × 64 B = 1 MB handle pool
}

func NewQuantumQueue() *QuantumQueue {
	q := &QuantumQueue{freeHead: 0}
	for i := Handle(0); i < CapItems-1; i++ {
		q.arena[i].next = i + 1
	}
	q.arena[CapItems-1].next = nilIdx
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	q.freeHead = q.arena[h].next
	q.arena[h] = node{next: nilIdx, prev: nilIdx}
	return h, nil
}

func (q *QuantumQueue) Size() int   { return q.size }
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

func (q *QuantumQueue) Push(tick int64, h Handle, val []byte) {
	n := &q.arena[h]
	if n.count > 0 && n.tick == tick {
		copy(n.data[:], val)
		n.count++
		return
	}
	if n.count > 0 {
		deltaOld := uint64(n.tick - int64(q.baseTick))
		q.unlinkByIndex(h, deltaOld)
		q.size -= int(n.count)
	}
	delta := uint64(tick) - q.baseTick
	g := delta >> 12
	l := (delta >> 6) & 63
	b := delta & 63
	bucket := idx32((g << 12) | (l << 6) | b)
	n.next = q.buckets[bucket]
	if n.next != nilIdx {
		q.arena[n.next].prev = h
	}
	n.prev = nilIdx
	q.buckets[bucket] = h
	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - b)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)
	n.tick = tick
	n.count = 1
	copy(n.data[:], val)
	q.size++
}

func (q *QuantumQueue) PushIfFree(tick int64, h Handle, val []byte) bool {
	n := &q.arena[h]
	if n.count > 0 {
		return false
	}
	q.Push(tick, h, val)
	return true
}

// MoveTick atomically “moves” an existing handle to a new tick.
// It captures the old count, unlinks the node, adjusts size,
// then relinks it into the correct bucket and restores size.
func (q *QuantumQueue) MoveTick(h Handle, newTick int64) {
	// 1) grab the node and its old count
	n := &q.arena[h]
	oldCount := n.count

	// 2) unlink from its old bucket
	//    compute delta = n.tick - baseTick
	deltaOld := uint64(n.tick) - q.baseTick
	q.unlinkByIndex(h, deltaOld)
	q.size -= int(oldCount)

	// 3) compute the new bucket index
	//    top‐level groups of 4096 ticks (2^12), lanes of 64 ticks (2^6)
	delta := uint64(newTick) - q.baseTick
	g := delta >> 12       // which of the 64 groups
	l := (delta >> 6) & 63 // which of the 64 lanes within that group
	b := delta & 63        // which of the 64 buckets within that lane
	idx := idx32((g << 12) | (l << 6) | b)

	// 4) splice into the head of the new bucket
	n.next = q.buckets[idx]
	if n.next != nilIdx {
		q.arena[n.next].prev = h
	}
	n.prev = nilIdx
	q.buckets[idx] = h

	// 5) flip the three summary bits
	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - b)
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)

	// 6) update the node and restore its size contribution
	n.tick = newTick
	n.count = 1
	q.size++
}

func (q *QuantumQueue) PeepMin() (Handle, int64, *[44]byte) {
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	b := bits.LeadingZeros64(gb.l2[l])
	bucket := idx32((uint64(g) << 12) | (uint64(l) << 6) | uint64(b))
	idx := q.buckets[bucket]
	n := &q.arena[idx]
	return idx, n.tick, &n.data
}

func (q *QuantumQueue) PeepMinSafe() (Handle, int64, *[44]byte) {
	if q.Size() == 0 || q.summary == 0 {
		return nilIdx, 0, nil
	}
	return q.PeepMin()
}

func (q *QuantumQueue) UnlinkMin(h Handle, tick int64) {
	delta := uint64(tick - int64(q.baseTick))
	q.unlinkByIndex(h, delta)
	q.size--
}

func (q *QuantumQueue) unlinkByIndex(idx Handle, delta uint64) {
	g := delta >> 12
	l := (delta >> 6) & 63
	b := delta & 63
	bucket := idx32((g << 12) | (l << 6) | b)
	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[bucket] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	if q.buckets[bucket] == nilIdx {
		gb := &q.groups[g]
		gb.l2[l] &^= 1 << (63 - b)
		if gb.l2[l] == 0 {
			gb.l1Summary &^= 1 << (63 - l)
			if gb.l1Summary == 0 {
				q.summary &^= 1 << (63 - g)
			}
		}
	}
	n.count = 0
	n.next = q.freeHead
	q.freeHead = idx
}
