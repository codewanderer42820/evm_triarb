// bucketqueue.go — fixed-capacity, allocation-free sliding-window min-priority queue
// ================================================================================
// • No heap allocations after New().
// • O(1) Push / PopMin / Remove / Update operations.
// • Duplicate pushes handled via per-node reference counters.
// • Sliding window of ticks implemented as a ring of buckets with generation stamps.
// • Arena of nodes fixed at compile time (capItems).

package bucketqueue

import (
	"errors"
	"math/bits"
)

// ───── compile-time parameters ─────────────────────────────────────────
const (
	numBuckets = 1 << 12 // 4096 tick buckets in the sliding window
	groupSize  = 64      // one uint64 covers 64 buckets
	numGroups  = numBuckets / groupSize
	nilIdx     = int32(-1)      // sentinel for "no node"
	capItems   = 4 * numBuckets // 16384 handles ≈ 256KiB arena
)

// ───── public error values ──────────────────────────────────────────────
var (
	ErrPastTick     = errors.New("bucketqueue: tick is before window")
	ErrItemNotFound = errors.New("bucketqueue: handle not in queue")
	ErrFull         = errors.New("bucketqueue: arena exhausted")
)

// Handle is the caller-visible alias for an internal node index
// (int32 to match arena indices).
type Handle = int32

// ───── internal node and arena ─────────────────────────────────────────
// node holds pointers for a doubly-linked list, bucket assignment, and
// a duplicate-push ref-count.
type Node struct {
	next, prev int32 // indices in the arena array, nilIdx for none
	bucketIdx  int32 // which bucket this node sits in
	count      int32 // number of duplicate pushes
}

// Queue encapsulates the sliding-window priority queue state:
//   - arena: fixed-size pool of nodes
//   - buckets: ring buffer of heads per tick bucket
//   - bitmaps: for fast min-bucket lookup via two-level bitmasks
//   - freelist: linked-list of unused handle indices
//   - baseTick/gen: track absolute tick origin and lazy bucket clearing
//   - size: current logical number of enqueued items
type Queue struct {
	Arena    [capItems]Node // node pool
	freeHead int32          // head of free-list

	buckets   [numBuckets]int32  // head index per tick bucket
	bucketGen [numBuckets]uint64 // generation stamp per bucket

	groupBits [numGroups]uint64 // bitmask per group of 64 buckets
	summary   uint64            // bitmask of which groups are non-empty

	baseTick uint64 // absolute tick at buckets[0]
	gen      uint64 // current generation ID
	size     int32  // logical queue length
}

// New constructs and returns an initialized Queue.
// All buckets start empty; the arena's freelist links all handles.
func New() *Queue {
	q := &Queue{freeHead: 0}
	// Build freelist through arena.next pointers
	for i := int32(0); i < capItems-1; i++ {
		q.Arena[i].next = i + 1
		q.Arena[i].bucketIdx = nilIdx
	}
	q.Arena[capItems-1].next = nilIdx
	q.Arena[capItems-1].bucketIdx = nilIdx

	// Mark all buckets empty
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// ───── freelist helpers ─────────────────────────────────────────────────
// borrow pops an index from the freelist or returns ErrFull.
func (q *Queue) borrow() (int32, error) {
	if q.freeHead == nilIdx {
		return nilIdx, ErrFull
	}
	idx := q.freeHead
	q.freeHead = q.Arena[idx].next
	return idx, nil
}

// release returns a node index to the freelist, zeroing its node.
func (q *Queue) release(idx int32) {
	n := &q.Arena[idx]
	*n = Node{}         // reset fields
	n.next = q.freeHead // push onto freelist
	n.bucketIdx = nilIdx
	q.freeHead = idx
}

// ───── public API: Borrow & Return handles ─────────────────────────────

// Borrow yields a fresh handle for subsequent Push calls.
func (q *Queue) Borrow() (Handle, error) {
	return q.borrow()
}

// Return recycles an unused handle back into the freelist.
// Fails if the handle is invalid or still enqueued.
func (q *Queue) Return(h Handle) error {
	idx := int32(h)
	if idx < 0 || idx >= capItems {
		return ErrItemNotFound
	}
	if q.Arena[idx].count != 0 {
		return errors.New("bucketqueue: cannot return active handle")
	}
	q.release(idx)
	return nil
}

// ───── public API: core queue operations ────────────────────────────────

// Push enqueues handle h at absolute tick. Errors on stale tick or invalid h.
// Uses O(1) bucket insertion, with fast path for duplicate pushes.
func (q *Queue) Push(tick int64, h Handle) error {
	if tick < int64(q.baseTick) {
		return ErrPastTick
	}
	idx := int32(h)
	if idx < 0 || idx >= capItems {
		return ErrItemNotFound
	}
	n := &q.Arena[idx]

	// Fast-path: same-bucket duplicate push bumps count only
	if n.count > 0 && n.bucketIdx >= 0 && q.bucketGen[n.bucketIdx] == q.gen {
		cur := int64(q.baseTick) + int64(n.bucketIdx)
		if cur == tick {
			n.count++
			q.size++
			return nil
		}
		// else: relocate to new tick
		q.removeInternal(idx, n)
	}

	// Slide window if tick lies beyond current ring
	d := uint64(tick) - q.baseTick
	if d >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = uint64(tick)
		q.gen++
		q.summary = 0
		q.groupBits = [numGroups]uint64{}
		q.size = 0
		d = 0
	}

	// Map absolute tick to ring index
	bkt := int(d) & (numBuckets - 1)

	// Lazy-clear on generation mismatch
	if q.bucketGen[bkt] != q.gen {
		q.bucketGen[bkt] = q.gen
		q.buckets[bkt] = nilIdx
	}

	// Splice node at front of bucket list (LIFO)
	head := q.buckets[bkt]
	n.next, n.prev = head, nilIdx
	n.bucketIdx = int32(bkt)
	n.count = 1
	if head != nilIdx {
		q.Arena[head].prev = idx
	}
	q.buckets[bkt] = idx

	// Set occupancy bit in group mask and summary
	g := bkt / groupSize
	bit := uint(bkt % groupSize)
	q.groupBits[g] |= 1 << bit
	q.summary |= 1 << uint(g)

	q.size++
	return nil
}

// PopMin removes and returns the handle with smallest tick, plus its tick.
// Runs in O(1) by scanning two-level bitmasks and unlinking the node.
func (q *Queue) PopMin() (Handle, int64) {
	if q.size == 0 || q.summary == 0 {
		return nilIdx, 0
	}
	// find first non-empty group and bucket
	g := bits.TrailingZeros64(q.summary)
	w := q.groupBits[g]
	b := bits.TrailingZeros64(w)
	bkt := g*groupSize + int(b)
	tick := int64(q.baseTick) + int64(bkt)

	// get head node
	idx := q.buckets[bkt]
	n := &q.Arena[idx]

	// duplicate count >1: just decrement counter
	if n.count > 1 {
		n.count--
		q.size--
		return Handle(idx), tick
	}

	// unlink single-copy node from list
	next := n.next
	q.buckets[bkt] = next
	if next != nilIdx {
		q.Arena[next].prev = nilIdx
	}
	q.size--

	// clear bit if bucket now empty
	if next == nilIdx {
		q.groupBits[g] &^= 1 << uint(b)
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << uint(g)
		}
	}

	q.release(idx)
	return Handle(idx), tick
}

// Remove expels handle h from its bucket, recycling the node.
func (q *Queue) Remove(h Handle) error {
	idx := int32(h)
	if idx < 0 || idx >= capItems {
		return ErrItemNotFound
	}
	n := &q.Arena[idx]
	if n.count == 0 || n.bucketIdx < 0 || q.bucketGen[n.bucketIdx] != q.gen {
		return ErrItemNotFound
	}
	q.removeInternal(idx, n)
	return nil
}

// Update moves an existing handle to a new tick. Equivalent to Remove+Push.
func (q *Queue) Update(newTick int64, h Handle) error {
	if err := q.Remove(h); err != nil {
		return err
	}
	return q.Push(newTick, h)
}

// Size returns the current number of enqueued items.
func (q *Queue) Size() int {
	return int(q.size)
}

// Empty reports whether the queue is empty.
func (q *Queue) Empty() bool {
	return q.size == 0
}

// ───── internals: unlink and recycling ─────────────────────────────────
// removeInternal unlinks a node from its bucket list and updates bitmasks.
func (q *Queue) removeInternal(idx int32, n *Node) {
	// detach from doubly-linked list
	if n.prev != nilIdx {
		q.Arena[n.prev].next = n.next
	} else {
		q.buckets[n.bucketIdx] = n.next
	}
	if n.next != nilIdx {
		q.Arena[n.next].prev = n.prev
	}
	// adjust size by all duplicates
	q.size -= n.count

	// clear occupancy if bucket empty
	if q.buckets[n.bucketIdx] == nilIdx {
		g := int(n.bucketIdx) / groupSize
		b := uint(int(n.bucketIdx) % groupSize)
		q.groupBits[g] &^= 1 << b
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << uint(g)
		}
	}
	q.release(idx)
}

// recycleStaleBuckets frees all nodes in every bucket when window jumps.
func (q *Queue) recycleStaleBuckets() {
	for i := 0; i < numBuckets; i++ {
		for n := q.buckets[i]; n != nilIdx; {
			next := q.Arena[n].next
			q.release(n)
			n = next
		}
		q.buckets[i] = nilIdx
	}
}

// compile-time size guard: arena should not exceed 32MiB
var _ [1 << 25]byte
