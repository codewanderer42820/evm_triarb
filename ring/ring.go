// -----------------------------------------------------------------------------
// ring.go — Lock‑free single‑producer / single‑consumer (SPSC) queue
// -----------------------------------------------------------------------------
//
//  This extensively‑commented edition expands on the design notes already present
//  in the original file so that every decision is explicit.  Nothing in the
//  executable logic has been changed — only explanatory comments have been
//  added.  All ASCII‑art diagrams have been preserved and augmented.
// -----------------------------------------------------------------------------

// SPDX-License-Identifier: MIT

package ring

/*───────────────────────── slot & ring ─────────────────────────*/

// slot holds one fixed‑width payload (32 bytes) together with a monotonically
// increasing sequence number.  The sequence functions as a *ticket* that tells
// the producer/consumer whose turn it is to own the slot:
//
//   - Producer owns the slot    when seq == tail
//
//   - Consumer owns the slot    when seq == head+1
//
//   - Neither side owns (full)  when seq == tail+1  (visible to consumer)
//
//   - Neither side owns (empty) when seq == head    (visible to producer)
//
//     Using a uint64 allows >4 billion wraps before re‑use even on tiny rings.
//     Since this is an SPSC queue there is *never* any contention on the slot
//     itself: the producer writes only val+seq, the consumer reads val and then
//     writes seq.
type slot struct {
	val [32]byte // in‑place payload (caller chooses its own encoding)
	seq uint64   // coordination ticket (see above)
}

// Ring is a fixed‑capacity circular buffer.  The layout deliberately separates
// head and tail onto independent cache lines (64‑byte paddings) so that the
// producer and consumer never false‑share a line even when the queue is hot.
//
// mask is used instead of modulo:   index = cursor & mask
// step is cached len(buf) so the consumer can recycle a slot in one store.
//
// NOTE: head *and* tail are manipulated *without atomics* because the producer
//
//	and consumer each owns exactly one variable.
type Ring struct {
	//lint:ignore U1000 cache-line padding
	_pad0 [64]byte // ----- 1st cache line (producer never touches)
	head  uint64   // cursor read exclusively by consumer
	//lint:ignore U1000 cache-line padding
	_pad1 [64]byte // ----- 2nd cache line (consumer never touches)
	tail  uint64   // cursor written exclusively by producer
	//lint:ignore U1000 cache-line padding
	_pad2 [64]byte // ----- 3rd cache line

	mask uint64 // == len(buf)-1  (power‑of‑two invariant)
	step uint64 // == len(buf)    (cached to avoid an extra uint64->int conv.)
	buf  []slot // backing storage
}

/*──────────────────────── constructor ──────────────────────────*/

// New allocates an empty ring of the requested *size*.
//
//   - *size* MUST be >0 AND a power‑of‑two or we panic immediately.  The power
//     of two restriction lets us replace the more expensive `%` operator with
//     a bit‑mask.
//   - Each slot.seq is initialised so that   seq == index   (slot is empty).
//
// Complexity: O(size)
func New(size int) *Ring {
	if size <= 0 || size&(size-1) != 0 {
		panic("ring: size must be >0 and a power of two")
	}
	// Allocate & initialise.
	r := &Ring{
		mask: uint64(size - 1),
		step: uint64(size),
		buf:  make([]slot, size),
	}
	for i := range r.buf {
		r.buf[i].seq = uint64(i)
	}
	return r
}

/*────────────────────────── Push ───────────────────────────────*
| Producer algorithm (non‑blocking)                               |
|   slot = buf[tail & mask]                                        |
|   if slot.seq == tail            => slot is free                |
|       slot.val = input                                             |
|       slot.seq = tail + 1        => publish to consumer          |
|       tail++                                                     |
|   else: queue is full (return false)                             |
*────────────────────────────────────────────────────────────────*/

// Push writes one element into the queue.  It is *wait‑free* and never blocks.
// Returns false immediately if the ring is full.
//
// Memory ordering:
//
//	loadAcquireUint64 guarantees we read the latest seq written by the
//	consumer, and storeReleaseUint64 publishes the value before seq is
//	incremented so the consumer never observes a partially written payload.
func (r *Ring) Push(val *[32]byte) bool {
	t := r.tail
	s := &r.buf[t&r.mask]

	if loadAcquireUint64(&s.seq) != t {
		return false // queue is full
	}

	s.val = *val                    // copy 32 byte payload (inline)
	storeReleaseUint64(&s.seq, t+1) // publish ownership to consumer
	r.tail = t + 1                  // advance cursor (no atomic needed)
	return true
}

/*────────────────────────── Pop ────────────────────────────────*
| Consumer algorithm (non‑blocking)                               |
|   slot = buf[head & mask]                                        |
|   if slot.seq == head+1        => item ready                    |
|       output = slot.val                                         |
|       slot.seq = head + step     => recycle for next wrap       |
|       head++                                                     |
|   else: queue is empty (return nil)                             |
*────────────────────────────────────────────────────────────────*/

// Pop removes and returns a pointer to the next element, or nil when the queue
// is empty.
//
// Memory ordering mirrors Push: the acquire load ensures we observe the
// producer's write **after** it published the slot (release).
func (r *Ring) Pop() *[32]byte {
	h := r.head
	s := &r.buf[h&r.mask]

	if loadAcquireUint64(&s.seq) != h+1 {
		return nil // empty
	}

	val := &s.val                        // take address *before* recycling
	storeReleaseUint64(&s.seq, h+r.step) // recycle slot for next reuse cycle
	r.head = h + 1
	return val
}

/*─────────────────────── PopWait helper ────────────────────────*/

// PopWait is a convenience wrapper that busy‑waits (with a polite PAUSE) until
// an item is available.  This is suitable when the consumer is dedicated to
// the queue (e.g. pinned to a core) and extremely low latency is required.
// The outer loop is small so the branch predictor locks on quickly.
func (r *Ring) PopWait() *[32]byte {
	for {
		if p := r.Pop(); p != nil {
			return p
		}
		cpuRelax() // emit PAUSE (amd64) or no‑op elsewhere
	}
}
