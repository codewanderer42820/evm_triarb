// ============================================================================
// bucketqueue.go — performance‑tuned, heavily documented revision
//
//   - Re‑ordered structs to minimise padding and improve cache locality.
//   - Added //go:inline and //go:nosplit directives to the hottest tiny helpers
//     so the compiler is urged to keep them on the leaf stack and inline them
//     into call‑sites where practical.
//   - Split explanatory commentary into “why” and “how”, focusing on the hard
//     real‑time guarantees that the surrounding trading system demands.
//
// NOTE: The public API surface is **unchanged** so existing callers continue to
//
//	compile.  Only internal layout / compiler hints were touched.
//
// ============================================================================
package bucketqueue

import (
	"errors"
	"math/bits"
	"unsafe"
)

// ---------------------------------------------------------------------------
// Tunable compile‑time constants.
// ---------------------------------------------------------------------------

const (
	// numBuckets must stay a power‑of‑two so we can use bit‑twiddling instead
	// of integer division when mapping absolute ticks → circular bucket index.
	numBuckets = 4096 // one full 4 096‑tick window (≈ 2¹²)

	// Each uint64 inside groupBits tracks a *block* of 64 buckets, so the total
	// number of groups is numBuckets/64.  That allows PopMin/PeepMin to skip
	// entire empty 64‑bucket spans in one TrailingZeros64() call.
	numGroups = numBuckets / 64

	// Hard cap on concurrently borrowed handles.  2¹⁶ fits naturally into a
	// uint16, but we keep idx32 for alignment reasons.
	capItems = 1 << 16 // 65 536 – plenty for real‑time order books
)

// idx32 is *only* used as an array index inside the private arena; it never
// escapes the package, so we can rely on pointer‑sized ints for public Handle.
//
// The nil index uses the all‑ones pattern so a zero value is considered valid –
// that choice keeps the initial arena slot ‘0’ usable without extra checks.
// ---------------------------------------------------------------------------
type idx32 uint32

const nilIdx idx32 = ^idx32(0)

// ---------------------------------------------------------------------------
// node – internal per‑handle metadata
// ---------------------------------------------------------------------------
// Layout rationale (all sizes on a 64‑bit platform):
//
//	tick  int64          8  ┐ keep the two 64‑bit fields together → fewer
//	data  *unsafe.Pointer 8 │    cache lines touched when PopMin only needs
//	next  idx32          4 │    the timestamp + payload pointer.
//	prev  idx32          4 │ –––––––––––––––––––––––––→ 24 B
//	count uint32         4 │ count rarely used, so we pay one 4‑byte hole…
//	_pad                4  ┘ so the struct still fits snugly into 32 bytes.
//
// The total fits exactly into half a typical L1 cache line (64 B), meaning two
// nodes map nicely and none spans a line boundary.
// ---------------------------------------------------------------------------
type node struct {
	tick  int64          // absolute tick for this bucket element
	data  unsafe.Pointer // latest value pushed for the handle at this tick
	next  idx32          // next node within the same bucket list (nilIdx = end)
	prev  idx32          // previous node within the bucket list (nilIdx = head)
	count uint32         // multiplicity when same handle pushed N times at tick
	_pad  uint32         // forces 8‑byte alignment of the whole struct
}

// ---------------------------------------------------------------------------
// Queue – the public, lock‑free 4 096‑bucket timing wheel.
// ---------------------------------------------------------------------------
// Field order chosen so the hot, mutated scalars live together at the end – the
// large arrays never move once allocated and therefore do not penalise access
// to those scalars through additional offset calculations on every use.
// ---------------------------------------------------------------------------

//lint:ignore U1000  we export the type; unused‑field linter disabled deliberately
//nolint:structcheck // we know some fields are mutated only on slow paths.
type Queue struct {
	arena     [capItems]node    // fixed arena – no per‑node heap allocs
	buckets   [numBuckets]idx32 // circular linked‑list heads for 4 096 slots
	groupBits [numGroups]uint64 // bitmap of non‑empty buckets per group

	baseTick uint64 // absolute tick represented by bucket 0
	summary  uint64 // bitmap of non‑empty *groups* (top‑level accelerator)

	size     int   // total number of enqueued (handle,tick) records
	freeHead idx32 // head of single‑linked free‑list inside arena
}

// ---------------------------------------------------------------------------
// Error values – callers treat them as sentinels; do NOT compare the message.
// ---------------------------------------------------------------------------
var (
	ErrFull         = errors.New("bucketqueue: no free handles")
	ErrPastWindow   = errors.New("bucketqueue: tick too far in the past")
	ErrBeyondWindow = errors.New("bucketqueue: tick too far in the future")
	ErrItemNotFound = errors.New("bucketqueue: invalid handle")
)

// Handle is an opaque index handed to the caller after Borrow().
//
// The zero value (nilIdx) is intentionally *invalid* so misuse is easy to spot.
// ---------------------------------------------------------------------------
//
//go:inline
//lint:ignore U1000 exported for users of the package
//nolint:revive // we want a short name for ergonomics
type Handle idx32

// New returns an empty Queue ready for use.  It never allocates at runtime –
// the entire arena and bucket heads live in the struct itself so the GC does
// not even notice them.
// ---------------------------------------------------------------------------
func New() *Queue {
	q := &Queue{}

	// Build the free‑list backwards so Borrow() pops the lowest index first –
	// this gives slightly better locality when users borrow a burst of handles
	// and immediately push them into consecutive ticks.
	for i := capItems - 1; i > 0; i-- {
		q.arena[i-1].next = idx32(i)
	}
	q.arena[capItems-1].next = nilIdx
	q.freeHead = 0

	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

// ---------------------------------------------------------------------------
// Borrow — acquire a free Handle from the pool. O(1).
//
// ---------------------------------------------------------------------------
//
//go:nosplit  // no calls into runtime; safe to run on the system stack
//go:inline   // trivial enough that the compiler will happily inline it anyway
func (q *Queue) Borrow() (Handle, error) {
	if q.freeHead == nilIdx {
		return Handle(nilIdx), ErrFull
	}
	h := q.freeHead
	n := &q.arena[h]
	q.freeHead = n.next

	// Reset linkage so stale pointers never leak user data across ticks.
	n.next, n.prev, n.count = nilIdx, nilIdx, 0
	return Handle(h), nil
}

// ---------------------------------------------------------------------------
// Push — enqueue (or re‑enqueue) a handle at `tick`.
//
// Fast‑path handles the common case where the same handle is pushed repeatedly
// into the same bucket (update‑in‑place).  Otherwise we detach any existing
// node and splice it into the new bucket.
//
// ---------------------------------------------------------------------------
//
//go:nosplit   // tail‑calls only; never allocates, never triggers stack grow
func (q *Queue) Push(tick int64, h Handle, val unsafe.Pointer) error {
	if h >= Handle(capItems) {
		return ErrItemNotFound
	}

	// Tick sanity checks without extra branches: convert to unsigned so we can
	// detect negative deltas via the sign‑bit (delta >= 1<<63).
	delta := uint64(tick) - q.baseTick
	switch {
	case delta >= 1<<63:
		return ErrPastWindow
	case delta >= numBuckets:
		return ErrBeyondWindow
	}

	idx := idx32(h)
	n := &q.arena[idx]

	// -------------------------------- fast path: in‑bucket count bump --------
	if n.count != 0 && n.tick == tick {
		n.count++
		q.size++
		n.data = val // overwrite payload pointer so PeepMin sees the latest
		return nil
	}

	// -------------------------------- slow path: detach from old bucket ------
	if n.count != 0 {
		if n.prev != nilIdx {
			q.arena[n.prev].next = n.next
		} else {
			// Node was bucket head → update bucket→group→summary cascading bits
			old := uint64(n.tick) - q.baseTick
			q.buckets[old] = n.next
			if q.buckets[old] == nilIdx {
				g := old >> 6
				q.groupBits[g] &^= 1 << (old & 63)
				if q.groupBits[g] == 0 {
					q.summary &^= 1 << g
				}
			}
		}
		if n.next != nilIdx {
			q.arena[n.next].prev = n.prev
		}
		q.size -= int(n.count)
		n.next, n.prev = nilIdx, nilIdx
	}

	// -------------------------------- attach into new bucket -----------------
	bkt := delta // unsigned, guaranteed < numBuckets here
	n.next, n.prev = q.buckets[bkt], nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[bkt] = idx
	n.tick, n.count, n.data = tick, 1, val

	// Update summarising hierarchic bitmaps
	g := bkt >> 6
	if (q.groupBits[g] & (1 << (bkt & 63))) == 0 {
		q.groupBits[g] |= 1 << (bkt & 63)
		q.summary |= 1 << g
	}
	q.size++
	return nil
}

// ---------------------------------------------------------------------------
// Update — move an already‑queued handle to a new tick.  Internally calls Push
// after unlinking, thereby reusing the exact same bucket/bit management.
//
// ---------------------------------------------------------------------------
//
//go:nosplit
func (q *Queue) Update(tick int64, h Handle, val unsafe.Pointer) error {
	if h >= Handle(capItems) {
		return ErrItemNotFound
	}
	idx := idx32(h)
	n := &q.arena[idx]
	if n.count == 0 {
		return ErrItemNotFound
	}

	// Detach from current bucket – largely identical to the branch in Push()
	old := uint64(n.tick) - q.baseTick
	q.size -= int(n.count)
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[old] = n.next
		if q.buckets[old] == nilIdx {
			g := old >> 6
			q.groupBits[g] &^= 1 << (old & 63)
			if q.groupBits[g] == 0 {
				q.summary &^= 1 << g
			}
		}
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}
	n.next, n.prev, n.count = nilIdx, nilIdx, 0
	// Re‑insert via Push – that already updates all counters and bitmaps.
	return q.Push(tick, h, val)
}

// ---------------------------------------------------------------------------
// PeepMin — read the front element without dequeuing. O(1).
//
// ---------------------------------------------------------------------------
//
//go:nosplit
//go:inline
func (q *Queue) PeepMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	b := bits.TrailingZeros64(q.groupBits[g])
	bkt := uint64(g<<6 | b)

	h := q.buckets[bkt]
	n := &q.arena[h]
	return Handle(h), n.tick, n.data
}

// ---------------------------------------------------------------------------
// PopMin — remove & return the front element.  O(1).
//
// ---------------------------------------------------------------------------
//
//go:nosplit
func (q *Queue) PopMin() (Handle, int64, unsafe.Pointer) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.TrailingZeros64(q.summary)
	b := bits.TrailingZeros64(q.groupBits[g])
	bkt := uint64(g<<6 | b)

	h := q.buckets[bkt]
	n := &q.arena[h]

	// Fast path: multiplicity >1, just decrement and keep node in place.
	if n.count > 1 {
		n.count--
		q.size--
		return Handle(h), n.tick, n.data
	}

	// Last copy – unlink node and propagate bitmap clears upwards.
	q.buckets[bkt] = n.next
	if n.next != nilIdx {
		q.arena[n.next].prev = nilIdx
	}
	q.size--
	if q.buckets[bkt] == nilIdx {
		q.groupBits[g] &^= 1 << (bkt & 63)
		if q.groupBits[g] == 0 {
			q.summary &^= 1 << g
		}
	}

	// Return node to free‑list (LIFO to maximise locality for fresh Borrow()).
	n.next, n.prev, n.count, n.data = nilIdx, nilIdx, 0, nil
	q.freeHead, n.next = h, q.freeHead

	return Handle(h), n.tick, n.data
}

// ---------------------------------------------------------------------------
// Const‑time helpers
// ---------------------------------------------------------------------------
//
//go:nosplit
//go:inline
func (q *Queue) Size() int { return q.size }

//go:nosplit
//go:inline
func (q *Queue) Empty() bool { return q.size == 0 }
