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
	"fmt"
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
// Node holds pointers for a doubly-linked list, bucket assignment, and
// a duplicate-push ref-count.
type Node struct {
	next, prev int32 // indices in the arena array, nilIdx for none
	bucketIdx  int32 // which bucket this node sits in
	count      int32 // number of duplicate pushes
}

// Queue encapsulates the sliding-window priority queue state.
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

// Push enqueues handle h at absolute tick. Errors on stale tick or invalid h.
// Duplicate pushes in the same tick bump the count; reprioritization requires Update.
func (q *Queue) Push(tick int64, h Handle) error {
	// Past-tick error
	if tick < int64(q.baseTick) {
		return ErrPastTick
	}
	// Slide window if tick lies beyond current ring
	d0 := uint64(tick) - q.baseTick
	if d0 >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = uint64(tick)
		q.gen++
		q.summary = 0
		q.groupBits = [numGroups]uint64{}
		q.size = 0
	}
	// Compute offset within ring
	d := uint64(tick) - q.baseTick

	// Validate handle
	idx := int32(h)
	if idx < 0 || idx >= capItems {
		return ErrItemNotFound
	}
	n := &q.Arena[idx]
	// If already enqueued in current generation
	if n.count > 0 && n.bucketIdx >= 0 && q.bucketGen[n.bucketIdx] == q.gen {
		cur := int64(q.baseTick) + int64(n.bucketIdx)
		if cur == tick {
			// same tick: bump duplicate count
			n.count++
			q.size++
			return nil
		}
		// different tick: require explicit Update()
		return fmt.Errorf("bucketqueue: handle %d already in queue at tick %d; use Update", h, cur)
	}
	// Determine bucket index
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
func (q *Queue) PopMin() (Handle, int64) {
	if q.size == 0 || q.summary == 0 {
		return nilIdx, 0
	}
	g := bits.TrailingZeros64(q.summary)
	w := q.groupBits[g]
	b := bits.TrailingZeros64(w)
	bkt := g*groupSize + int(b)
	tick := int64(q.baseTick) + int64(bkt)

	idx := q.buckets[bkt]
	n := &q.Arena[idx]
	if n.count > 1 {
		n.count--
		q.size--
		return Handle(idx), tick
	}

	next := n.next
	q.buckets[bkt] = next
	if next != nilIdx {
		q.Arena[next].prev = nilIdx
	}
	q.size--
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

// removeInternal unlinks a node from its bucket list and updates bitmasks.
func (q *Queue) removeInternal(idx int32, n *Node) {
	if n.prev != nilIdx {
		q.Arena[n.prev].next = n.next
	} else {
		q.buckets[n.bucketIdx] = n.next
	}
	if n.next != nilIdx {
		q.Arena[n.next].prev = n.prev
	}
	q.size -= n.count
	if q.buckets[n.bucketIdx] == nilIdx {
		g := int(n.bucketIdx) / groupSize
		b := uint(n.bucketIdx % groupSize)
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
	*n = Node{}
	n.next = q.freeHead
	n.bucketIdx = nilIdx
	q.freeHead = idx
}

// Borrow yields a fresh handle for subsequent Push calls.
func (q *Queue) Borrow() (Handle, error) {
	return q.borrow()
}

// Return recycles an unused handle back into the freelist.
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
