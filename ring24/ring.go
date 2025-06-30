// ring.go — Lock‑free single‑producer/single‑consumer (SPSC) ring queue (24-byte payload)
//
// ⚠️ Footgun-Grade Ring: Engineered for absolute performance under full trust model.
//
// Assumptions:
//   - Single writer, single reader (SPSC).
//   - All payloads are fixed-size [24]byte aligned.
//   - Capacity is power-of-two. Overflows must be handled externally.
//   - Atomic counters are used for head/tail synchronization.
//
// Footgun Warnings:
//   - No bounds checks. No fallback. Caller responsibility enforced.
//   - Push() returns false if full. Pop() returns nil if empty.
//   - Ring is never dynamically resized. Construction must be correct.
//
// Use only with full memory and CPU core discipline.

package ring24

import (
	"sync/atomic"
)

// slot holds one **24-byte** payload plus an 8-byte sequence counter.
// Total struct size: 32 B (half of a 64-byte cache line).
//
//go:notinheap
//go:align 64
type slot struct {
	val [24]byte // user payload
	seq uint64   // sequence number for ownership tracking
}

// Ring is an ultra-fast, cache-friendly, single-producer single-consumer ring buffer.
//
// Layout ensures:
// - `head` and `tail` each sit on separate 64-byte cachelines
// - No false sharing across producer/consumer lanes
// - struct size = 272 bytes (multiple of 64)
//
//go:notinheap
//go:align 64
type Ring struct {
	_    [64]byte // bytes 0–63: pad to isolate head
	head uint64   // bytes 64–71: read cursor (consumer)

	_    [56]byte // bytes 72–135: pad to isolate tail
	tail uint64   // bytes 136–143: write cursor (producer)

	_ [56]byte // bytes 144–207: additional separation or future metadata

	mask uint64 // bytes 208–215
	step uint64 // bytes 216–223
	buf  []slot // bytes 224–247 (slice header: ptr, len, cap)

	_ [3]uint64
}

// New constructs a ring with power-of-two size.
// Panics if size is invalid.
//
//go:nosplit
//go:inline
//go:registerparams
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

// Push attempts to enqueue a [24]byte payload.
// Returns false if full (slot not ready).
//
//go:nosplit
//go:inline
//go:registerparams
func (r *Ring) Push(val *[24]byte) bool {
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

// Pop returns the next available payload, or nil if empty.
//
//go:nosplit
//go:inline
//go:registerparams
func (r *Ring) Pop() *[24]byte {
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

// PopWait blocks (spins) until a value is available.
//
//go:nosplit
//go:inline
//go:registerparams
func (r *Ring) PopWait() *[24]byte {
	for {
		if p := r.Pop(); p != nil {
			return p
		}
		cpuRelax()
	}
}
