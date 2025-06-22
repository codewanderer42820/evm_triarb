// bucketqueue.go — fixed‑capacity, allocation‑free sliding‑window min‑priority queue
// ================================================================================
// * Zero heap allocations after New().
// * O(1) Push / PopMin / Remove / Update operations.
// * Duplicate pushes handled via per‑node reference counters.
// * Sliding window of ticks implemented as a ring of buckets with generation stamps.
// * Arena of nodes fixed at compile time (capItems).

package bucketqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

// ───── compile‑time parameters ─────────────────────────────────────────
const (
	numBuckets = 1 << 12 // 4096 tick buckets in the sliding window
	groupSize  = 64      // one uint64 covers 64 buckets
	numGroups  = numBuckets / groupSize
	nilIdx     = -1             // sentinel for "no node"
	capItems   = 4 * numBuckets // 16384 handles ≈ 512 KiB arena (ints)
)

// ───── public error values ──────────────────────────────────────────────
var (
	ErrPastTick     = errors.New("bucketqueue: tick is before window")
	ErrItemNotFound = errors.New("bucketqueue: handle not in queue")
	ErrFull         = errors.New("bucketqueue: arena exhausted")
)

// Handle is the caller‑visible alias for an internal node index (plain int).
// Using int avoids a handful of conversions on 64‑bit builds.
type Handle = int

// ───── internal node and arena ─────────────────────────────────────────
// node holds pointers for a doubly‑linked list, bucket assignment, and a
// duplicate‑push ref‑count.
// All ints fit comfortably within 32 bits (< 2^31 handles) but we keep them
// pointer‑sized to avoid conversions.
type node struct {
	next, prev int // indices in the arena array, nilIdx for none
	bucketIdx  int // which bucket this node sits in (‑1 when free)
	count      int // number of duplicate pushes (≥1 when enqueued)
}

// Queue encapsulates the sliding‑window priority queue state.
// All fields are private; callers interact only through the methods below.
type Queue struct {
	arena    [capItems]node // node pool (fixed‑size)
	freeHead int            // head of free‑list (indices)

	buckets   [numBuckets]int    // head index per tick bucket
	bucketGen [numBuckets]uint16 // generation stamp per bucket (wraps at 65 k)

	groupBits [numGroups]uint64 // bitmask per group of 64 buckets
	summary   uint64            // bitmask of which groups are non‑empty

	baseTick uint64 // absolute tick represented by buckets[0]
	gen      uint16 // current generation ID (increments on window shift)
	size     int    // logical queue length (#enqueued copies)
}

// compile‑time guard: arena must stay <32 MiB.
var _ [1 << 25]byte

// ───── constructors ────────────────────────────────────────────────────

// New constructs and returns an initialised Queue.
func New() *Queue {
	q := &Queue{freeHead: 0}

	// Build free‑list via arena.next pointers
	for i := 0; i < capItems-1; i++ {
		q.arena[i].next = i + 1
		q.arena[i].bucketIdx = nilIdx
	}
	q.arena[capItems-1].next = nilIdx
	q.arena[capItems-1].bucketIdx = nilIdx

	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// ───── freelist helpers (inlined by compiler) ─────────────────────────

//go:inline
func (q *Queue) borrow() (int, error) {
	if q.freeHead == nilIdx {
		return nilIdx, ErrFull
	}
	idx := q.freeHead
	q.freeHead = q.arena[idx].next
	return idx, nil
}

//go:inline
func (q *Queue) release(idx int) {
	n := &q.arena[idx]
	// Clear only what future readers inspect; avoid memclr of full struct.
	n.prev, n.bucketIdx, n.count = nilIdx, nilIdx, 0
	n.next = q.freeHead
	q.freeHead = idx
}

// ───── public API: Borrow & Return handles ─────────────────────────────
func (q *Queue) Borrow() (Handle, error) { return q.borrow() }

func (q *Queue) Return(h Handle) error {
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	if q.arena[h].count != 0 {
		return errors.New("bucketqueue: cannot return active handle")
	}
	q.release(h)
	return nil
}

// ───── core queue operations ───────────────────────────────────────────

// Push enqueues handle h at absolute tick. O(1).
func (q *Queue) Push(tick int64, h Handle) error {
	if tick < int64(q.baseTick) {
		return ErrPastTick
	}
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]

	// Fast‑path: same bucket duplicate → just bump count.
	if n.count > 0 && n.bucketIdx >= 0 && q.bucketGen[n.bucketIdx] == q.gen {
		cur := int64(q.baseTick) + int64(n.bucketIdx)
		if cur == tick {
			n.count++
			q.size++
			return nil
		}
		// relocate — detach but DON'T touch size yet (handled later)
		q.detach(h, n)
		q.size -= n.count
	}

	// Slide window if tick lies beyond current ring width.
	if d := uint64(tick) - q.baseTick; d >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = uint64(tick)
		q.gen++ // uint16 naturally wraps
		q.summary = 0
		q.groupBits = [numGroups]uint64{}
	}

	bkt := int(uint64(tick)-q.baseTick) & (numBuckets - 1)

	// Lazy‑initialise bucket for current generation
	if q.bucketGen[bkt] != q.gen {
		q.bucketGen[bkt] = q.gen
		q.buckets[bkt] = nilIdx
	}

	// Splice node at front (LIFO)
	head := q.buckets[bkt]
	n.next, n.prev = head, nilIdx
	n.bucketIdx = bkt
	if n.count == 0 {
		n.count = 1
	}
	if head != nilIdx {
		q.arena[head].prev = h
	}
	q.buckets[bkt] = h

	// Set occupancy bit
	g := bkt / groupSize
	bit := uint(bkt % groupSize)
	q.groupBits[g] |= 1 << bit
	q.summary |= 1 << uint(g)

	q.size += n.count
	return nil
}

// PopMin removes and returns the handle with the smallest tick. O(1).
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
	n := &q.arena[idx]

	if n.count > 1 {
		n.count--
		q.size--
		if n.next != nilIdx {
			prefetch(&q.arena[n.next])
		}
		return Handle(idx), tick
	}

	next := n.next
	if next != nilIdx {
		prefetch(&q.arena[next])
	}

	// unlink
	q.buckets[bkt] = next
	if next != nilIdx {
		q.arena[next].prev = nilIdx
	}

	if next == nilIdx {
		q.groupBits[g] &^= 1 << uint(b)
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << uint(g)
		}
	}

	q.size--
	q.release(idx)
	return Handle(idx), tick
}

// Remove expels handle h entirely. O(1).
func (q *Queue) Remove(h Handle) error {
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]
	if n.count == 0 || n.bucketIdx < 0 || q.bucketGen[n.bucketIdx] != q.gen {
		return ErrItemNotFound
	}
	q.detach(h, n)
	q.size -= n.count
	q.release(h)
	return nil
}

// Update changes the tick of an enqueued handle without churn. O(1).
func (q *Queue) Update(newTick int64, h Handle) error {
	if newTick < int64(q.baseTick) {
		return ErrPastTick
	}
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]
	if n.count == 0 || n.bucketIdx < 0 || q.bucketGen[n.bucketIdx] != q.gen {
		return ErrItemNotFound
	}

	curTick := int64(q.baseTick) + int64(n.bucketIdx)
	if curTick == newTick {
		return nil // already correct bucket
	}

	// Far future tick → recycle whole window and re‑push
	if d := uint64(newTick) - q.baseTick; d >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = uint64(newTick)
		q.gen++
		q.summary = 0
		q.groupBits = [numGroups]uint64{}
		// node becomes logically free, reuse via Push
		cnt := n.count
		n.count = 0
		q.size -= cnt
		return q.Push(newTick, h)
	}

	// detach from current bucket (size unchanged)
	q.detach(h, n)

	newBkt := int(uint64(newTick)-q.baseTick) & (numBuckets - 1)
	// ensure bucket current gen
	if q.bucketGen[newBkt] != q.gen {
		q.bucketGen[newBkt] = q.gen
		q.buckets[newBkt] = nilIdx
	}

	head := q.buckets[newBkt]
	n.next, n.prev = head, nilIdx
	n.bucketIdx = newBkt
	if head != nilIdx {
		q.arena[head].prev = h
	}
	q.buckets[newBkt] = h

	g := newBkt / groupSize
	bit := uint(newBkt % groupSize)
	q.groupBits[g] |= 1 << bit
	q.summary |= 1 << uint(g)
	return nil
}

// Size returns the total number of enqueued copies (duplicates counted).
func (q *Queue) Size() int { return q.size }

// Empty is true when no copies are in the queue.
func (q *Queue) Empty() bool { return q.size == 0 }

// ───── internal helpers ────────────────────────────────────────────────
// detach unlinks node idx from its bucket but does NOT mutate q.size.
func (q *Queue) detach(idx int, n *node) {
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[n.bucketIdx] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	// clear bucket bits if empty
	if q.buckets[n.bucketIdx] == nilIdx {
		g := n.bucketIdx / groupSize
		bit := uint(n.bucketIdx % groupSize)
		q.groupBits[g] &^= 1 << bit
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << uint(g)
		}
	}

	n.next, n.prev, n.bucketIdx = nilIdx, nilIdx, nilIdx
}

// recycleStaleBuckets frees all nodes when the window jumps far ahead.
func (q *Queue) recycleStaleBuckets() {
	for b := 0; b < numBuckets; b++ {
		for idx := q.buckets[b]; idx != nilIdx; {
			next := q.arena[idx].next
			q.release(idx)
			idx = next
		}
		q.buckets[b] = nilIdx
	}
	q.size = 0
}

//go:nosplit
func prefetch(ptr *node) {
	if ptr == nil {
		return
	}
	_ = *(*uintptr)(unsafe.Pointer(ptr))
}
