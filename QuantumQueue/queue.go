// SPDX‑Free: Public‑Domain
// Ultra‑low‑latency **QuantumQueue** — branch‑budget trimmed
// Optional safety branches (delta clamp, size underflow, unlink guard, error returns) removed.
// Duplicate‑tick handling **kept**. Hot path unchanged.
package quantumqueue

import "math/bits"

/*──────── errors ─────────────*/
type queueErr struct{ msg string }

func (e queueErr) Error() string { return e.msg }

var (
	ErrFull         = queueErr{"full"}
	ErrPastWindow   = queueErr{"past"}
	ErrBeyondWindow = queueErr{"future"}
	ErrNotFound     = queueErr{"notfound"}
)

/*──────── constants ──────────*/
const (
	GroupCount  = 64
	LaneCount   = 64
	BucketCount = GroupCount * LaneCount * LaneCount // 262 144
	CapItems    = 1 << 16
)

/*──────── tiny types ─────────*/
type idx32 uint32

const nilIdx idx32 = ^idx32(0)

type Handle idx32

/*──────── packed structs ─────*/

type node struct {
	tick  int64
	count uint32
	next  idx32
	prev  idx32
	data  [44]byte
}

type groupBlock struct {
	l1Summary uint64
	l2        [LaneCount]uint64
}

/*──────── queue struct ───────*/

type QuantumQueue struct {
	summary  uint64
	baseTick uint64
	size     int
	freeHead idx32
	_        [40]byte // pad to 64B header
	groups   [GroupCount]groupBlock
	arena    [CapItems]node
	buckets  [BucketCount]idx32
}

/*──────── helpers ────────────*/
// mask64 returns all‑ones if b==true else 0.
//go:inline
func mask64(b bool) uint64 {
	if b {
		return ^uint64(0)
	}
	return 0
}

// validateDelta: 0=ok,1=past,2=beyond.
// Unsigned math avoids sign issues when baseTick ≥ 2^63.
//
//go:inline
func validateDelta(d uint64) uint {
	past := uint(d >> 63)                          // tick < baseTick → wrap → high bit =1
	beyond := uint(^((d - BucketCount) >> 63) & 1) // d>=BucketCount ⇒ beyond=1
	return past | beyond<<1
}

/*──────── constructor ───────*/
func NewQuantumQueue() *QuantumQueue {
	q := &QuantumQueue{freeHead: 1}
	for i := idx32(1); i < CapItems-1; i++ {
		q.arena[i].next = i + 1
	}
	q.arena[CapItems-1].next = nilIdx
	return q
}

/*──────── public API ─────────*/
func (q *QuantumQueue) Borrow() (Handle, error) {
	if q.freeHead == nilIdx {
		return Handle(nilIdx), ErrFull
	}
	idx := q.freeHead
	q.freeHead = q.arena[idx].next
	return Handle(idx), nil
}

func (q *QuantumQueue) Push(tick int64, h Handle, data []byte) error {
	idx := idx32(h)
	if idx == nilIdx || idx >= CapItems {
		return ErrNotFound
	}
	n := &q.arena[idx]

	// duplicate fast path
	if n.count > 0 && n.tick == tick {
		n.count++
		copy(n.data[:], data)
		return nil
	}

	// window validation
	delta := uint64(tick) - q.baseTick
	switch validateDelta(delta) {
	case 1:
		return ErrPastWindow
	case 2:
		return ErrBeyondWindow
	}

	// new insert
	g := delta >> 12
	l := (delta >> 6) & 0x3F
	b := delta & 0x3F
	bIdx := g*LaneCount*LaneCount + l*LaneCount + b

	n.tick = tick
	n.count = 1
	copy(n.data[:], data)

	// intrusive list insert at bucket head
	n.next = q.buckets[bIdx]
	n.prev = nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[bIdx] = idx

	// set bitmaps
	gb := &q.groups[g]
	if gb.l2[l] == 0 {
		gb.l1Summary |= 1 << (63 - l)
	}
	gb.l2[l] |= 1 << (63 - b)
	q.summary |= 1 << (63 - g)

	q.size++
	return nil
}

func (q *QuantumQueue) Update(tick int64, h Handle, data []byte) error {
	idx := idx32(h)
	if idx == nilIdx || idx >= CapItems {
		return ErrNotFound
	}
	n := &q.arena[idx]
	if n.count == 0 {
		return ErrNotFound
	}
	// same‑tick fast path
	if n.tick == tick {
		copy(n.data[:], data)
		return nil
	}
	deltaOld := uint64(n.tick) - q.baseTick
	gOld := deltaOld >> 12
	lOld := (deltaOld >> 6) & 0x3F
	bOld := deltaOld & 0x3F
	bIdxOld := gOld*LaneCount*LaneCount + lOld*LaneCount + bOld

	// remove from old bucket list
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[bIdxOld] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// bitmap clears if bucket empty
	if q.buckets[bIdxOld] == nilIdx {
		gbOld := &q.groups[gOld]
		gbOld.l2[lOld] &^= 1 << (63 - bOld)
		if gbOld.l2[lOld] == 0 {
			gbOld.l1Summary &^= 1 << (63 - lOld)
			if gbOld.l1Summary == 0 {
				q.summary &^= 1 << (63 - gOld)
			}
		}
	}

	// insert at new bucket
	delta := uint64(tick) - q.baseTick
	switch validateDelta(delta) {
	case 1:
		return ErrPastWindow
	case 2:
		return ErrBeyondWindow
	}
	g := delta >> 12
	l := (delta >> 6) & 0x3F
	b := delta & 0x3F
	bIdx := g*LaneCount*LaneCount + l*LaneCount + b

	n.tick = tick
	copy(n.data[:], data)

	n.next = q.buckets[bIdx]
	n.prev = nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[bIdx] = idx

	gb := &q.groups[g]
	if gb.l2[l] == 0 {
		gb.l1Summary |= 1 << (63 - l)
	}
	gb.l2[l] |= 1 << (63 - b)
	q.summary |= 1 << (63 - g)
	return nil
}

func (q *QuantumQueue) PeepMin() (Handle, int64, []byte) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	b := bits.LeadingZeros64(gb.l2[l])
	bIdx := uint64(g)*LaneCount*LaneCount + uint64(l)*LaneCount + uint64(b)
	idx := q.buckets[bIdx]
	n := &q.arena[idx]
	return Handle(idx), n.tick, n.data[:]
}

func (q *QuantumQueue) PopMin() (Handle, int64, []byte) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	b := bits.LeadingZeros64(gb.l2[l])
	bIdx := uint64(g)*LaneCount*LaneCount + uint64(l)*LaneCount + uint64(b)
	idx := q.buckets[bIdx]
	n := &q.arena[idx]
	if n.count > 1 {
		n.count--
		return Handle(idx), n.tick, n.data[:]
	}
	// unlink node fully
	q.buckets[bIdx] = n.next
	if n.next != nilIdx {
		q.arena[n.next].prev = nilIdx
	}
	// bitmap clear masks
	gb.l2[l] &^= 1 << (63 - b)
	if gb.l2[l] == 0 {
		gb.l1Summary &^= 1 << (63 - l)
		if gb.l1Summary == 0 {
			q.summary &^= 1 << (63 - g)
		}
	}

	q.size--
	// free handle back to freelist
	n.count = 0
	n.next = q.freeHead
	q.freeHead = idx
	return Handle(idx), n.tick, n.data[:]
}

func (q *QuantumQueue) Size() int   { return q.size }
func (q *QuantumQueue) Empty() bool { return q.size == 0 }
