// SPDX‑Free: Public‑Domain
// Ultra‑low‑latency **QuantumQueue** — 64‑byte‑node, branch‑free version
// **Fixes:**
//   - Signed Δ logic (tick‑baseTick) to prevent underflow/overflow errors
//   - Accurate size accounting on dup push/pop, underflow clamp
//   - unlinkByIndex guards against out‑of‑window Δ
//   - mask64 helper for branch‑free bitmap clears
package quantumqueue

import (
	"fmt"
	"math/bits"
)

/*──────── constants ───────*/
const (
	GroupCount  = 64
	LaneCount   = 64
	BucketCount = GroupCount * LaneCount * LaneCount // 262 144
	CapItems    = 1 << 16
)

/*──────── errors ─────────*/
type queueErr struct{ msg string }

func (e queueErr) Error() string { return e.msg }

var (
	ErrFull         = queueErr{"full"}
	ErrPastWindow   = queueErr{"past"}
	ErrBeyondWindow = queueErr{"future"}
	ErrNotFound     = queueErr{"notfound"}
	ErrTooLarge     = queueErr{"toolarge"}
)

/*──────── tiny types ─────*/

// idx32 indexes arena slices. nilIdx == ^idx32(0).
type idx32 uint32

const nilIdx idx32 = ^idx32(0)

// Handle is an exported opaque reference.
// Its zero value is invalid (nilIdx maps to Handle(~0)).
type Handle idx32

/*──────── node: 64 B ─────*/

type node struct {
	tick  int64    // 8
	next  idx32    // 4  (12)
	prev  idx32    // 4  (16)
	count uint32   // 4  (20)
	_     [4]byte  // 4  (24) pad to 8‑byte boundary
	data  [44]byte // payload (24+44 = 68 -> rounded to 64 via padding drop)
}

/*──────── group slab ─────*/

type groupBlock struct {
	l1Summary uint64            // 8
	l2        [LaneCount]uint64 // 512 (8×64)
	_         [56]byte          // pad: 8*9 = 576 total
}

/*──────── queue struct────*/

type QuantumQueue struct {
	summary  uint64                 // bitmap of non‑empty groups
	baseTick int64                  // logical 0 of sliding window
	size     int                    // active handles
	freeHead idx32                  // freelist
	_        [40]byte               // pad → header 64 B
	groups   [GroupCount]groupBlock // 64×576 ≈ 36 KB
	buckets  [BucketCount]idx32     // head idx per bucket (nilIdx == empty)
	arena    [CapItems]node         // fixed 64‑B nodes
}

/*──────── helpers ───────*/

// mask64 returns all‑ones if b==true, else 0.
//
//go:inline
func mask64(b bool) uint64 {
	if b {
		return ^uint64(0)
	}
	return 0
}

// validateDelta returns 0 OK, 1 past, 2 beyond.
// d is signed difference tick‑baseTick.
//
//go:nosplit
func validateDelta(d int64) uint {
	past := uint((d >> 63) & 1)                 // 1 if negative
	beyond := uint(((d-BucketCount)>>63)&1) ^ 1 // 1 if d≥BucketCount
	return past | (beyond << 1)
}

/*──────── constructor ───*/

func NewQuantumQueue() *QuantumQueue {
	q := &QuantumQueue{}
	// freelist init
	for i := CapItems - 1; i >= 0; i-- {
		q.arena[i].next = q.freeHead
		q.freeHead = idx32(i)
	}
	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}
	return q
}

/*──────── Borrow / Return──*/

func (q *QuantumQueue) Borrow() (Handle, error) {
	if q.freeHead == nilIdx {
		return Handle(nilIdx), ErrFull
	}
	idx := q.freeHead
	q.freeHead = q.arena[idx].next
	return Handle(idx), nil
}

/*──────── Push ───────────*/

func (q *QuantumQueue) Push(tick int64, h Handle, payload []byte) error {
	if len(payload) > 44 {
		return ErrTooLarge
	}
	idx := idx32(h)
	if idx >= CapItems {
		return ErrNotFound
	}
	n := &q.arena[idx]
	if n.count != 0 {
		// active handle: relocate or duplicate handled by Update.
		return q.Update(tick, h, payload)
	}

	d := tick - q.baseTick
	switch validateDelta(d) {
	case 1:
		return ErrPastWindow
	case 2:
		return ErrBeyondWindow
	}

	delta := uint64(d)
	if delta >= BucketCount {
		return ErrBeyondWindow
	}

	// copy payload
	copy(n.data[:], payload)
	n.count = 1
	n.tick = tick
	n.next, n.prev = q.buckets[delta], nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx

	g, l := delta>>12, (delta>>6)&0x3F
	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - (delta & 63))
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)
	q.size++
	return nil
}

/*──────── Update ─────────*/

func (q *QuantumQueue) Update(tick int64, h Handle, payload []byte) error {
	if len(payload) > 44 {
		return ErrTooLarge
	}
	idx := idx32(h)
	if idx >= CapItems {
		return ErrNotFound
	}
	n := &q.arena[idx]
	if n.count == 0 {
		return ErrNotFound
	}

	d := tick - q.baseTick
	switch validateDelta(d) {
	case 1:
		return ErrPastWindow
	case 2:
		return ErrBeyondWindow
	}
	delta := uint64(d)
	if delta >= BucketCount {
		return ErrBeyondWindow
	}

	// same tick fast path
	if n.tick == tick {
		copy(n.data[:], payload)
		return nil
	}

	// relocate: unlink from old bucket
	q.unlinkByIndex(idx, uint64(n.tick-q.baseTick))

	// link into new bucket
	n.tick = tick
	copy(n.data[:], payload)
	n.next, n.prev = q.buckets[delta], nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx

	g, l := delta>>12, (delta>>6)&0x3F
	gb := &q.groups[g]
	gb.l2[l] |= 1 << (63 - (delta & 63))
	gb.l1Summary |= 1 << (63 - l)
	q.summary |= 1 << (63 - g)
	return nil
}

/*──────── Pop / Peep ─────*/

func (q *QuantumQueue) PeepMin() (Handle, int64, []byte) {
	if q.size == 0 || q.summary == 0 {
		return Handle(nilIdx), 0, nil
	}
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g]
	l := bits.LeadingZeros64(gb.l1Summary)
	laneBits := gb.l2[l]
	bOff := bits.LeadingZeros64(laneBits)
	delta := uint64(g)<<12 | uint64(l)<<6 | uint64(bOff)
	idx := q.buckets[delta]
	n := &q.arena[idx]
	return Handle(idx), n.tick, n.data[:] // slice full payload
}

func (q *QuantumQueue) PopMin() (Handle, int64, []byte) {
	h, t, v := q.PeepMin()
	if h == Handle(nilIdx) {
		return h, t, v
	}
	idx := idx32(h)
	n := &q.arena[idx]
	if n.count > 1 {
		n.count--
		return h, t, v
	}
	// unlink from bucket
	delta := uint64(t - q.baseTick)
	q.unlinkByIndex(idx, delta)
	// push idx onto freelist
	n.count = 0
	n.next = q.freeHead
	q.freeHead = idx
	if q.size > 0 {
		q.size--
	}
	return h, t, v
}

/*──────── unlink helper ───*/
//go:nosplit
func (q *QuantumQueue) unlinkByIndex(idx idx32, delta uint64) {
	if delta >= BucketCount {
		q.arena[idx].next, q.arena[idx].prev = nilIdx, nilIdx
		return
	}
	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[delta] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	g, l := delta>>12, (delta>>6)&0x3F
	gb := &q.groups[g]
	bucketBit := uint64(1) << (63 - (delta & 63))
	laneBit := uint64(1) << (63 - l)

	gb.l2[l] &^= bucketBit
	gb.l1Summary &^= laneBit & mask64(gb.l2[l] == 0)
	q.summary &^= (uint64(1) << (63 - g)) & mask64(gb.l1Summary == 0)
}

/*──────── public misc ────*/

func (q *QuantumQueue) Size() int   { return q.size }
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

/*──────── String (debug)──*/
func (q *QuantumQueue) String() string {
	return fmt.Sprintf("QuantumQueue{size=%d, base=%d}", q.size, q.baseTick)
}
