// ring_56byte.go — Lock‑free single‑producer/single‑consumer (SPSC) ring queue (56-byte payload)
//
// ⚠️ Footgun-Grade Ring: Engineered for absolute performance under full trust model.
//
// Assumptions:
//   - Single writer, single reader (SPSC).
//   - All payloads are fixed-size [56]byte aligned.
//   - Capacity is power-of-two. Overflows must be handled externally.
//   - Atomic counters are used for head/tail synchronization.
//
// Footgun Warnings:
//   - No bounds checks. No fallback. Caller responsibility enforced.
//   - Push() returns false if full. Pop() returns nil if empty.
//   - Ring is never dynamically resized. Construction must be correct.
//
// Use only with full memory and CPU core discipline.

package ring32

import (
	"sync/atomic"
)

// slot is the payload container for one entry in the ring.
// Payload is 56 bytes, followed by a uint64 ticket for strict position control.
type slot struct {
	val [56]byte // fixed-size payload (matches fastuni, quantumqueue, etc.)
	seq uint64   // slot ticket number
}

// Ring is a lock-free single-producer/single-consumer queue.
// Designed for pinned-thread hot-loop dispatch.
type Ring struct {
	_    [64]byte // cache-line isolation (consumer head)
	head uint64   // consumer cursor

	_    [64]byte // cache-line isolation (producer tail)
	tail uint64   // producer cursor

	_ [64]byte // further isolation from neighbors

	mask uint64 // == len(buf) - 1 (bitmask for modulo)
	step uint64 // == len(buf)     (precomputed stride for wraparound)
	buf  []slot // backing ring buffer
}

// New constructs a ring with power-of-two size.
// Panics if size is not valid. Caller must ensure sizing discipline.
//
//go:nosplit
//go:inline
func New(size int) *Ring {
	if size <= 0 || size&(size-1) != 0 {
		panic("ring: size must be >0 and power of two")
	}
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

// Push attempts to enqueue a [56]byte payload.
// Returns false if slot is not yet ready (queue full). No backoff logic.
//
//go:nosplit
//go:inline
func (r *Ring) Push(val *[56]byte) bool {
	t := r.tail
	s := &r.buf[t&r.mask]
	if atomic.LoadUint64(&s.seq) != t {
		return false
	}
	s.val = *val
	atomic.StoreUint64(&s.seq, t+1)
	r.tail = t + 1
	return true
}

// Pop returns the next available payload pointer.
// If empty, returns nil. Payload is valid until overwritten.
//
//go:nosplit
//go:inline
func (r *Ring) Pop() *[56]byte {
	h := r.head
	s := &r.buf[h&r.mask]
	if atomic.LoadUint64(&s.seq) != h+1 {
		return nil
	}
	val := &s.val
	atomic.StoreUint64(&s.seq, h+r.step)
	r.head = h + 1
	return val
}

// PopWait spins until a value is available and returns it.
// Uses cpuRelax() for polite spin-loop yielding.
//
//go:nosplit
func (r *Ring) PopWait() *[56]byte {
	for {
		if p := r.Pop(); p != nil {
			return p
		}
		cpuRelax()
	}
}
