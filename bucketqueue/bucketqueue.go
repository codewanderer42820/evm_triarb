// bucketqueue.go — fixed‑capacity, allocation‑free sliding‑window min‑priority queue
// =============================================================================
// Pure‑Go, single‑thread, zero‑alloc min‑priority queue tuned for nanosecond‑scale
// latency.  This version applies two micro‑optimisations requested by the user:
//   1.  The freelist helper `release` now takes `idx32` (an int32 alias) directly,
//       so callers avoid an implicit widen‑shrink conversion.
//   2.  `recycleStaleBuckets` walks the two‑level bitmap and breaks early once a
//       local group is empty, guaranteeing forward progress without unnecessary
//       loop iterations.
// No platform‑specific assembly is used; everything stays 100 % portable.

package bucketqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

// ───── compile‑time parameters ─────────────────────────────────────────
const (
	numBuckets = 1 << 12 // 4 096 tick buckets per sliding window
	groupSize  = 64      // 64 buckets per bitmap word
	numGroups  = numBuckets / groupSize

	capItems = 4 * numBuckets // 16 384 handles (≈512 KiB arena)
)

// idx32 is the canonical index type used inside the queue.  Using int32 keeps
// structs tight while still covering the entire arena.
// The public Handle type remains an alias for int to avoid casts in user code.
type idx32 = int32

const nilIdx idx32 = -1

// Handle is the caller‑visible identifier for a borrowed node.
// It purposely aliases int for ergonomic use by external code.
type Handle = int

// ───── public error values ─────────────────────────────────────────────
var (
	ErrPastTick     = errors.New("bucketqueue: tick is before window")
	ErrItemNotFound = errors.New("bucketqueue: handle not in queue")
	ErrFull         = errors.New("bucketqueue: arena exhausted")
)

// node stores doubly‑linked‑list pointers plus a ref‑counter for duplicate
// pushes.  The bucket index is cached to avoid a lookup in Remove/Update.
// All indices are idx32 to maximise cache density.
// The struct fits perfectly into 16 bytes.
type node struct {
	next, prev idx32
	bucketIdx  int32 // -1 when the node is free
	count      int32 // ≥1 when enqueued, 0 when free
}

// Queue holds all state.  All fields are private; callers interact through the
// methods only.  size counts duplicate pushes so Push/PopMin stay O(1).
type Queue struct {
	arena    [capItems]node
	freeHead idx32

	buckets   [numBuckets]idx32
	bucketGen [numBuckets]uint16

	groupBits [numGroups]uint64
	summary   uint64

	baseTick uint64 // absolute tick represented by buckets[0]
	gen      uint16 // generation id; increments on window slides (wraps)
	size     int
}

// ───── low‑level helpers ───────────────────────────────────────────────

//go:nosplit
func prefetch(ptr *node) {
	// Portable SWAR prefetch: one speculative dummy read.
	_ = *(*uintptr)(unsafe.Pointer(ptr))
}

//go:inline
func (q *Queue) borrow() (idx32, error) {
	if q.freeHead == nilIdx {
		return nilIdx, ErrFull
	}
	idx := q.freeHead
	q.freeHead = q.arena[idx].next
	return idx, nil
}

//go:inline
func (q *Queue) release(i idx32) {
	n := &q.arena[i]
	// Clear only fields future readers consult; avoid a full memclr.
	n.prev, n.bucketIdx, n.count = nilIdx, -1, 0
	n.next = q.freeHead
	q.freeHead = i
}

// ───── constructors ───────────────────────────────────────────────────

// New returns an initialised, empty queue ready for use.
func New() *Queue {
	q := &Queue{freeHead: 0}
	// Build freelist inside the arena.
	for i := 0; i < capItems-1; i++ {
		q.arena[i].next = idx32(i + 1)
		q.arena[i].bucketIdx = -1
	}
	q.arena[capItems-1].next = nilIdx
	q.arena[capItems-1].bucketIdx = -1
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// ───── public API: Borrow / Return ─────────────────────────────────────

func (q *Queue) Borrow() (Handle, error) {
	idx, err := q.borrow()
	return Handle(idx), err
}

func (q *Queue) Return(h Handle) error {
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	if q.arena[h].count != 0 {
		return errors.New("bucketqueue: cannot return active handle")
	}
	q.release(idx32(h))
	return nil
}

// ───── core queue operations ──────────────────────────────────────────

func (q *Queue) Push(tick int64, h Handle) error {
	if tick < int64(q.baseTick) {
		return ErrPastTick
	}
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]

	// Duplicate‑push fast‑path.
	if n.count > 0 && n.bucketIdx >= 0 && q.bucketGen[n.bucketIdx] == q.gen {
		if int64(q.baseTick)+int64(n.bucketIdx) == tick {
			n.count++
			q.size++
			return nil
		}
		q.detach(idx32(h), n)
		q.size -= int(n.count)
	}

	// Window slide if necessary.
	if d := uint64(tick) - q.baseTick; d >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = uint64(tick)
		q.gen++
		q.summary = 0
		q.groupBits = [numGroups]uint64{}
	}

	bkt := int(uint64(tick)-q.baseTick) & (numBuckets - 1)
	if q.bucketGen[bkt] != q.gen {
		q.bucketGen[bkt] = q.gen
		q.buckets[bkt] = nilIdx
	}

	head := q.buckets[bkt]
	n.next, n.prev = head, nilIdx
	n.bucketIdx = int32(bkt)
	if n.count == 0 {
		n.count = 1
	}
	if head != nilIdx {
		q.arena[head].prev = idx32(h)
	}
	q.buckets[bkt] = idx32(h)

	g := bkt / groupSize
	bit := uint(bkt % groupSize)
	q.groupBits[g] |= 1 << bit
	q.summary |= 1 << uint(g)

	q.size += int(n.count)
	return nil
}

func (q *Queue) PopMin() (Handle, int64) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0
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

func (q *Queue) Remove(h Handle) error {
	if h < 0 || h >= capItems {
		return ErrItemNotFound
	}
	n := &q.arena[h]
	if n.count == 0 || n.bucketIdx < 0 || q.bucketGen[n.bucketIdx] != q.gen {
		return ErrItemNotFound
	}
	q.detach(idx32(h), n)
	q.size -= int(n.count)
	q.release(idx32(h))
	return nil
}

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
	if int64(q.baseTick)+int64(n.bucketIdx) == newTick {
		return nil
	}
	// Far‑future tick → force window slide and push.
	if d := uint64(newTick) - q.baseTick; d >= numBuckets {
		q.recycleStaleBuckets()
		q.baseTick = uint64(newTick)
		q.gen++
		q.summary = 0
		q.groupBits = [numGroups]uint64{}
		cnt := n.count
		n.count = 0
		q.size -= int(cnt)
		return q.Push(newTick, h)
	}
	q.detach(idx32(h), n)
	return q.Push(newTick, h)
}

// ───── inspectors ───────────────────────────────────────────────────────
func (q *Queue) Size() int   { return q.size }
func (q *Queue) Empty() bool { return q.size == 0 }

// ───── internal helpers ────────────────────────────────────────────────

func (q *Queue) detach(i idx32, n *node) {
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[n.bucketIdx] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
	if q.buckets[n.bucketIdx] == nilIdx {
		g := int(n.bucketIdx) / groupSize
		bit := uint(int(n.bucketIdx) % groupSize)
		q.groupBits[g] &^= 1 << bit
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << uint(g)
		}
	}
	n.next, n.prev, n.bucketIdx = nilIdx, nilIdx, -1
}

// recycleStaleBuckets frees every live node when the window jumps far ahead.
//
//go:nosplit
func (q *Queue) recycleStaleBuckets() {
	if q.size == 0 || q.summary == 0 {
		return
	}
	for q.summary != 0 {
		g := bits.TrailingZeros64(q.summary)
		w := q.groupBits[g]
		for w != 0 {
			b := bits.TrailingZeros64(w)
			bkt := g*groupSize + int(b)
			// Drain linked list.
			for idx := q.buckets[bkt]; idx != nilIdx; {
				nxt := q.arena[idx].next
				q.release(idx)
				idx = nxt
			}
			q.buckets[bkt] = nilIdx
			q.bucketGen[bkt] = 0
			w &^= 1 << uint(b)
			if w == 0 {
				break // early exit once this group is empty
			}
		}
		q.groupBits[g] = 0
		q.summary &^= 1 << uint(g)
	}
	q.size = 0
}
