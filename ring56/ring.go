// ring.go — Lock‑free single‑producer/single‑consumer (SPSC) ring queue (56-byte payload)
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

package ring56

import (
	"sync/atomic"
)

// slot holds one 56-byte payload and its sequence number for tracking ownership.
//
//go:notinheap
type slot struct {
	val [56]byte
	seq uint64
}

// Ring is an ultra-fast, cache-friendly, single-producer single-consumer ring buffer.
//
//go:notinheap
type Ring struct {
	_    [64]byte // consumer head cacheline
	head uint64

	_    [56]byte // producer tail cacheline
	tail uint64

	_ [56]byte // extra padding

	mask uint64
	step uint64
	buf  []slot
	_    [3]uint64
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

// Push attempts to enqueue a [56]byte payload.
// Returns false if full (slot not ready).
//
//go:nosplit
//go:inline
//go:registerparams
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

// Pop returns the next available payload, or nil if empty.
//
//go:nosplit
//go:inline
//go:registerparams
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

// PopWait blocks (spins) until a value is available.
//
//go:nosplit
//go:inline
//go:registerparams
func (r *Ring) PopWait() *[56]byte {
	for {
		if p := r.Pop(); p != nil {
			return p
		}
		cpuRelax()
	}
}
