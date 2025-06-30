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

// slot carries a 24-byte payload plus an 8-byte sequence counter.
// Total: 32 B  (= one-half of a 64-byte cache-line).
//
//go:notinheap
//go:align 64
type slot struct {
	val [24]byte // user payload (bytes 0–23)
	seq uint64   // sequence number (bytes 24–31)
}

// Ring is a single-producer / single-consumer ring buffer.
//
// Layout guarantees
//   - `head` and `tail` live on distinct 64-byte lines → no false sharing
//   - read-only fields (`mask`, `step`, slice header) share one cold line
//   - Total struct size = **256 B** (exactly 4 cache-lines, multiple of 64)
//
//go:notinheap
//go:align 64
type Ring struct {
	// cache-line 0 — pad so `head` starts on its own line
	_    [64]byte // bytes   0–63
	head uint64   // bytes  64–71 : consumer read cursor

	// cache-line 1 — isolate `tail`
	_    [56]byte // bytes  72–127
	tail uint64   // bytes 128–135 : producer write cursor

	// cache-line 2 — spare / future metadata
	_ [56]byte // bytes 136–191

	// cache-line 3 — read-only config
	mask uint64 // bytes 192–199
	step uint64 // bytes 200–207
	buf  []slot // bytes 208–231 : slice header (ptr,len,cap)

	// still cache-line 3 — trailing pad keeps total divisible by 64
	_ [3]uint64 // bytes 232–255
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
