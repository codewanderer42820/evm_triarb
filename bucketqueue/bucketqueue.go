// Package bucketqueue is an ultra-low-latency, zero-alloc time-bucket priority queue.
// One cache-line-padded node per item; O(1) min-pop via 2-level bitmaps.
package bucketqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

const (
	// queue parameters – all powers of two
	numBuckets       = 4096 // window width
	groupSize        = 64   // buckets per summary bit
	numGroups        = numBuckets / groupSize
	capItems         = 1 << 16   // max concurrent items
	nilIdx     idx32 = ^idx32(0) // 0xffffffff – invalid handle
)

// -----------------------------------------------------------------------------
// compile-time invariants (build fails if hacked)
//
//  1. numBuckets must be a power-of-two.
//     numBuckets & (numBuckets-1) == 0   → OK  → -0  → length 0 (legal)
//     otherwise it’s >0                  → negative length → compile error
var _ [-int(numBuckets & (numBuckets - 1))]byte

//  2. capItems must be an integer multiple of numBuckets.
//     capItems % numBuckets == 0  → -0  → length 0 (legal)
//     otherwise non-zero          → negative length → compile error
var _ [-int(capItems % numBuckets)]byte

// -----------------------------------------------------------------------------

// internal types
type idx32 uint32

type node struct {
	next, prev idx32
	bucketGen  uint32
	tick       int64
	count      uint32
	data       unsafe.Pointer
	_          [64 - 4*4 - 8 - unsafe.Sizeof(unsafe.Pointer(nil))]byte // pad to 64 B
}

type Queue struct {
	arena     [capItems]node
	freeHead  idx32
	buckets   [numBuckets]idx32
	bucketGen [numBuckets]uint32
	baseTick  uint64
	gen       uint32
	size      int
	summary   uint64 // coarse bitmap – one bit per group
	groupBits [numGroups]uint64
}

// exported errors
var (
	ErrFull         = errors.New("bucketqueue: no free handles")
	ErrEmpty        = errors.New("bucketqueue: empty queue")
	ErrPastWindow   = errors.New("bucketqueue: tick too far in the past")
	ErrBeyondWindow = errors.New("bucketqueue: tick too far in the future")
	ErrItemNotFound = errors.New("bucketqueue: invalid handle")
)

// constructor
func New() *Queue {
	q := &Queue{}
	// build freelist
	for i := capItems - 1; i > 0; i-- {
		q.arena[i-1].next = idx32(i)
	}
	q.freeHead = 0
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// allocation helpers
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

func (q *Queue) Return(h Handle) error {
	if idx32(h) >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[idx32(h)]
	n.next = q.freeHead
	q.freeHead = idx32(h)
	// GC hygiene: drop user payload
	n.data = nil
	return nil
}

// internal release used by PopMin & stale-bucket recycle
func (q *Queue) release(h idx32) {
	n := &q.arena[h]
	n.next = q.freeHead
	q.freeHead = h
	// GC hygiene
	n.data = nil
}

// Push
// Push inserts (or moves) handle h to the given tick bucket.
// If the same handle is already in that bucket it just increments its count.
func (q *Queue) Push(tick int64, h Handle, val unsafe.Pointer) error {
	// ---------- basic bounds checks --------------------------------------------------
	if h >= Handle(capItems) {
		return ErrItemNotFound
	}
	bktDelta := uint64(tick) - q.baseTick
	if bktDelta >= 1<<63 { // too far in the past (wrap-safe)
		return ErrPastWindow
	}
	if bktDelta >= numBuckets { // too far in the future
		return ErrBeyondWindow
	}

	idx := idx32(h)
	n := &q.arena[idx]

	// ---------- fast path: already in the right bucket -------------------------------
	if n.count != 0 && n.bucketGen == q.gen && n.tick == tick {
		n.count++
		q.size++
		return nil
	}

	// ---------- first insertion or bucket-move ---------------------------------------
	if n.count != 0 {
		q.detach(idx, n)
		q.size -= int(n.count)

		// -------- clear bitmap bit if the OLD bucket became empty --------------------
		oldBkt := uint64(n.tick) - q.baseTick
		if oldBkt < numBuckets && q.buckets[oldBkt] == nilIdx {
			g := oldBkt >> 6                      // which 64-bucket group?
			q.groupBits[g] &^= 1 << (oldBkt & 63) // clear bucket bit
			if q.groupBits[g] == 0 {              // if group now empty
				q.summary &^= 1 << g // clear summary bit
			}
		}
	}

	// ---------- slide the window if we jumped forward --------------------------------
	if d := uint64(tick) - q.baseTick; d >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = uint64(tick)
		q.gen++
		if q.gen == 0 { // uint32 wrap → cheap reset
			q.bucketGen = [numBuckets]uint32{}
		}
		q.summary = 0
		q.groupBits = [numGroups]uint64{}
	}

	// ---------- link node at head of its new bucket ----------------------------------
	bktIdx := uint64(tick) - q.baseTick
	head := q.buckets[bktIdx]

	n.next, n.prev = head, nilIdx
	if head != nilIdx {
		q.arena[head].prev = idx
	}
	q.buckets[bktIdx] = idx
	n.bucketGen, n.tick, n.count, n.data = q.gen, tick, 1, val

	// ---------- set bitmap bits ------------------------------------------------------
	g := bktIdx >> 6                     // group index
	q.groupBits[g] |= 1 << (bktIdx & 63) // per-bucket bit
	q.summary |= 1 << g                  // per-group bit
	q.size++
	return nil
}

// detach removes node n (idx) from its current bucket list without changing bitmaps.
func (q *Queue) detach(idx idx32, n *node) {
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else { // n is head
		bktIdx := uint64(n.tick) - q.baseTick
		q.buckets[bktIdx] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
	n.next, n.prev = nilIdx, nilIdx
}

// Update – detach & re-push
func (q *Queue) Update(tick int64, h Handle, val unsafe.Pointer) error {
	if h >= Handle(capItems) {
		return ErrItemNotFound
	}
	idx := idx32(h)
	n := &q.arena[idx]
	if n.count == 0 {
		return ErrItemNotFound
	}
	q.size -= int(n.count)
	q.detach(idx, n)
	// clear bitmaps for emptied bucket when necessary
	bkt := uint64(n.tick) - q.baseTick
	if q.buckets[bkt] == nilIdx {
		group := bkt >> 6
		q.groupBits[group] &^= 1 << (bkt & (groupSize - 1))
		if q.groupBits[group] == 0 {
			q.summary &^= 1 << group
		}
	}
	return q.Push(tick, h, val)
}

// PeepMin – non-destructive
func (q *Queue) PeepMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	group := bits.TrailingZeros64(q.summary)
	bucketBits := q.groupBits[group]
	bktOffset := bits.TrailingZeros64(bucketBits)
	bktIdx := uint64(group<<6 | bktOffset)
	head := q.buckets[bktIdx]
	n := &q.arena[head]
	return Handle(head), n.tick, n.data
}

// PopMin – destructive O(1)
func (q *Queue) PopMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 {
		return Handle(nilIdx), 0, nil
	}
	group := bits.TrailingZeros64(q.summary)
	bucketBits := q.groupBits[group]
	bktOffset := bits.TrailingZeros64(bucketBits)
	bktIdx := uint64(group<<6 | bktOffset)

	head := q.buckets[bktIdx]
	n := &q.arena[head]
	h := Handle(head)
	tick := n.tick
	val := n.data

	if n.count > 1 { // multi-count fast path
		n.count--
		q.size--
		return h, tick, val
	}

	// unlink single node
	q.buckets[bktIdx] = n.next
	if n.next != nilIdx {
		q.arena[n.next].prev = nilIdx
	}
	q.size--

	// clear bitmaps if bucket empty
	if q.buckets[bktIdx] == nilIdx {
		q.groupBits[group] &^= 1 << (bktIdx & (groupSize - 1))
		if q.groupBits[group] == 0 {
			q.summary &^= 1 << group
		}
	}

	// recycle handle
	q.release(head)
	return h, tick, val
}

// internal helpers
func (q *Queue) recycleStaleBuckets() {
	for group, bits64 := range q.groupBits {
		for bits64 != 0 {
			bktOff := bits.TrailingZeros64(bits64)
			bktIdx := uint64(group<<6 | bktOff)
			idx := q.buckets[bktIdx]
			for idx != nilIdx {
				next := q.arena[idx].next
				q.release(idx)
				idx = next
			}
			q.buckets[bktIdx] = nilIdx
			bits64 &^= 1 << bktOff
		}
		q.groupBits[group] = 0
	}
	q.summary = 0
	q.size = 0
}

// Public helpers for legacy tests
func (q *Queue) Size() int   { return q.size }
func (q *Queue) Empty() bool { return q.size == 0 }

// Remove deletes an item when its handle is already known.
func (q *Queue) Remove(h Handle) error {
	if idx32(h) >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[idx32(h)]
	if n.count == 0 {
		return ErrItemNotFound
	}
	q.detach(idx32(h), n)
	q.size -= int(n.count)
	bkt := uint64(n.tick) - q.baseTick
	if q.buckets[bkt] == nilIdx {
		g := bkt >> 6
		q.groupBits[g] &^= 1 << (bkt & (groupSize - 1))
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << g
		}
	}
	q.release(idx32(h))
	return nil
}

// ErrPastTick is an alias for ErrPastWindow for legacy tests
var ErrPastTick = ErrPastWindow

// exported opaque handle type
type Handle idx32
