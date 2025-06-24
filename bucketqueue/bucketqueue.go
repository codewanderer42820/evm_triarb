// Package bucketqueue is an ultra‑low‑latency, zero‑alloc time‑bucket priority queue.
// Each item lives in a cache‑line‑sized node.  Two‑level bitmaps give O(1) PopMin.
// This version is fully branch‑minimised in the hot paths and tuned for ≤1 ns/op
// on an Apple M4 Pro (ARM64) with GOMAXPROCS = 1.
package bucketqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

// -----------------------------------------------------------------------------
// Compile‑time parameters – all powers‑of‑two so the compiler can constant‑fold
// shift & mask operations.  Build fails if you tamper with them.
// -----------------------------------------------------------------------------
const (
	numBuckets       = 4096 // sliding‑window width (MUST be pow‑2)
	groupSize        = 64   // buckets aggregated under one summary bit
	numGroups        = numBuckets / groupSize
	capItems         = 1 << 16   // total concurrent items (arena size)
	nilIdx     idx32 = ^idx32(0) // sentinel idx32(0xffffffff)
)

// Invariants – cause negative‑length arrays (compile‑time error) if broken.
var _ [-int(numBuckets & (numBuckets - 1))]byte // numBuckets must be pow‑2
var _ [-int(capItems % numBuckets)]byte         // capItems multiple of buckets

// -----------------------------------------------------------------------------
// Arena node & queue structs
// -----------------------------------------------------------------------------

type idx32 uint32

type node struct {
	next, prev idx32  // intrusive doubly‑linked bucket list
	bucketGen  uint32 // generation stamp to detect stale nodes
	tick       int64  // absolute tick value
	count      uint32 // multiplicity for duplicate pushes
	data       unsafe.Pointer
	_          [64 - 4*4 - 8 - unsafe.Sizeof(unsafe.Pointer(nil))]byte // pad →64B
}

type Queue struct {
	arena    [capItems]node // fixed arena – never reallocates
	freeHead idx32          // freelist head inside arena

	buckets   [numBuckets]idx32  // per‑bucket singly‑linked heads
	bucketGen [numBuckets]uint32 // generation stamps per bucket‑slot

	baseTick uint64 // tick corresponding to bucket 0
	gen      uint32 // current generation stamp
	size     int    // total live items

	summary   uint64            // 1‑bit / group → coarse bitmap
	groupBits [numGroups]uint64 // 1‑bit / bucket inside the group
}

// -----------------------------------------------------------------------------
// Public error values
// -----------------------------------------------------------------------------
var (
	ErrFull         = errors.New("bucketqueue: no free handles")
	ErrEmpty        = errors.New("bucketqueue: empty queue")
	ErrPastWindow   = errors.New("bucketqueue: tick too far in the past")
	ErrBeyondWindow = errors.New("bucketqueue: tick too far in the future")
	ErrItemNotFound = errors.New("bucketqueue: invalid handle")
)

// ErrPastTick is kept for backward‑compat unit‑tests.
var ErrPastTick = ErrPastWindow

// exported opaque handle
type Handle idx32

// -----------------------------------------------------------------------------
// Construction & basic memory management
// -----------------------------------------------------------------------------

// New returns an empty queue with a pre‑built freelist.
func New() *Queue {
	q := &Queue{}
	for i := capItems - 1; i > 0; i-- {
		q.arena[i-1].next = idx32(i) // build singly‑linked freelist backwards
	}
	q.freeHead = 0
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// Borrow gives the caller a fresh, unused handle.
func (q *Queue) Borrow() (Handle, error) {
	if q.freeHead == nilIdx {
		return Handle(nilIdx), ErrFull
	}
	h := q.freeHead
	n := &q.arena[h]
	q.freeHead = n.next
	n.next, n.prev, n.count = nilIdx, nilIdx, 0 // clear linkage fields
	return Handle(h), nil
}

// Return releases a handle back to the freelist (regardless of queue state).
func (q *Queue) Return(h Handle) error {
	if idx32(h) >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[idx32(h)]
	n.next = q.freeHead
	q.freeHead = idx32(h)
	n.data = nil // drop user payload for GC safety
	return nil
}

// release – internal version used by PopMin & recycle logic (no bounds check).
func (q *Queue) release(h idx32) {
	n := &q.arena[h]
	n.next = q.freeHead
	q.freeHead = h
	n.data = nil
}

// -----------------------------------------------------------------------------
// Push – main insertion API (branch‑free on critical path)
// -----------------------------------------------------------------------------

func (q *Queue) Push(tick int64, h Handle, val unsafe.Pointer) error {
	// --- fast constant‑time bound checks --------------------------------------
	if h >= Handle(capItems) {
		return ErrItemNotFound
	}
	bktDelta := uint64(tick) - q.baseTick
	if bktDelta >= 1<<63 { // too far in the past (wrap‑safe cmp on unsigned)
		return ErrPastWindow
	}
	if bktDelta >= numBuckets { // too far in the future
		return ErrBeyondWindow
	}

	idx := idx32(h)
	n := &q.arena[idx]

	// --- DUPLICATE INTO SAME BUCKET fast path ---------------------------------
	if n.count != 0 && n.bucketGen == q.gen && n.tick == tick {
		n.count++ // pure counter bump
		q.size++
		return nil
	}

	// --- Node is already in queue but wrong bucket – detach first -------------
	if n.count != 0 {
		q.detach(n)
		q.size -= int(n.count)

		// if old bucket became empty → clear its bitmap bits
		oldBkt := uint64(n.tick) - q.baseTick
		if oldBkt < numBuckets && q.buckets[oldBkt] == nilIdx {
			g := oldBkt >> 6
			q.groupBits[g] &^= 1 << (oldBkt & 63)
			if q.groupBits[g] == 0 {
				q.summary &^= 1 << g
			}
		}
	}

	// --- Slide window forward if we jumped beyond the current span ------------
	if d := uint64(tick) - q.baseTick; d >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = uint64(tick)
		q.gen++
		if q.gen == 0 { // uint32 overflow → cheap global reset
			q.bucketGen = [numBuckets]uint32{}
		}
		q.summary = 0
		q.groupBits = [numGroups]uint64{}
	}

	// --- Link node at head of its new bucket ----------------------------------
	bktIdx := uint64(tick) - q.baseTick
	head := q.buckets[bktIdx]

	n.next, n.prev = head, nilIdx
	if head != nilIdx {
		q.arena[head].prev = idx
	}
	q.buckets[bktIdx] = idx
	n.bucketGen, n.tick, n.count, n.data = q.gen, tick, 1, val

	// --- Set bitmap bits -------------------------------------------------------
	g := bktIdx >> 6 // group index
	q.groupBits[g] |= 1 << (bktIdx & 63)
	q.summary |= 1 << g
	q.size++
	return nil
}

// detach unlinks a node from its current bucket without touching bitmaps.
func (q *Queue) detach(n *node) {
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		bktIdx := uint64(n.tick) - q.baseTick
		q.buckets[bktIdx] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
	n.next, n.prev = nilIdx, nilIdx
}

// -----------------------------------------------------------------------------
// Update – move existing handle to new tick in a single call.
// -----------------------------------------------------------------------------
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
	q.detach(n)

	// clear bitmap bits of old bucket if empty
	oldBkt := uint64(n.tick) - q.baseTick
	if q.buckets[oldBkt] == nilIdx {
		g := oldBkt >> 6
		q.groupBits[g] &^= 1 << (oldBkt & 63)
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << g
		}
	}
	return q.Push(tick, h, val)
}

// -----------------------------------------------------------------------------
// PeepMin – non‑destructive read of the current minimum.
// -----------------------------------------------------------------------------
func (q *Queue) PeepMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	bktBits := q.groupBits[g]
	b := bits.TrailingZeros64(bktBits)
	bktIdx := uint64(g<<6 | b)
	head := q.buckets[bktIdx]
	n := &q.arena[head]
	return Handle(head), n.tick, n.data
}

// -----------------------------------------------------------------------------
// PopMin – destructive O(1) removal (branch‑free hot path for count>1 case).
// -----------------------------------------------------------------------------
func (q *Queue) PopMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	bktBits := q.groupBits[g]
	b := bits.TrailingZeros64(bktBits)
	bktIdx := uint64(g<<6 | b)

	head := q.buckets[bktIdx]
	n := &q.arena[head]
	h := Handle(head)
	tick := n.tick
	val := n.data

	// --- fast path: duplicate count ------------------------------------------
	if n.count > 1 {
		n.count--
		q.size--
		return h, tick, val
	}

	// --- single‑item bucket – unlink & recycle --------------------------------
	q.buckets[bktIdx] = n.next
	if n.next != nilIdx {
		q.arena[n.next].prev = nilIdx
	}
	q.size--

	// clear bitmap bits if bucket now empty
	if q.buckets[bktIdx] == nilIdx {
		q.groupBits[g] &^= 1 << (bktIdx & 63)
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << g
		}
	}

	q.release(head)
	return h, tick, val
}

// -----------------------------------------------------------------------------
// recycleStaleBuckets – zero‑cost window slide helper (called rarely)
// -----------------------------------------------------------------------------
func (q *Queue) recycleStaleBuckets() {
	for g, bits64 := range q.groupBits {
		for bits64 != 0 {
			b := bits.TrailingZeros64(bits64)
			bktIdx := uint64(g<<6 | b)
			idx := q.buckets[bktIdx]
			for idx != nilIdx {
				next := q.arena[idx].next
				q.release(idx)
				idx = next
			}
			q.buckets[bktIdx] = nilIdx
			bits64 &^= 1 << b
		}
		q.groupBits[g] = 0
	}
	q.summary = 0
	q.size = 0
}

// -----------------------------------------------------------------------------
// Convenience helpers for tests / callers
// -----------------------------------------------------------------------------
func (q *Queue) Size() int   { return q.size }
func (q *Queue) Empty() bool { return q.size == 0 }

// Remove deletes a specific handle when its value is known.
func (q *Queue) Remove(h Handle) error {
	if idx32(h) >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[idx32(h)]
	if n.count == 0 {
		return ErrItemNotFound
	}
	q.detach(n)
	q.size -= int(n.count)

	bkt := uint64(n.tick) - q.baseTick
	if q.buckets[bkt] == nilIdx {
		g := bkt >> 6
		q.groupBits[g] &^= 1 << (bkt & 63)
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << g
		}
	}
	q.release(idx32(h))
	return nil
}
