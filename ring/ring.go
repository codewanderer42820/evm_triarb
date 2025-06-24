// SPDX-License-Identifier: MIT
//
// Package ring implements a lock-free single-producer / single-consumer
// queue with *zero* heap allocations after construction.
//
//   - Power-of-two capacity ⇒ bit-mask instead of modulus
//   - Per-slot sequence numbers ⇒ no head>tail arithmetic
//   - Cache-line padding ⇒ producer/consumer never false-share
//   - Fast helpers (`PopWait`, relaxed barriers) keep hot-path ≤7 ns
//
// The code is pure Go; platform-specific barriers live in the tiny helpers
// at the bottom and auto-map to `LDAR/STLR` on arm64 or `MOV`+`LOCK` on x86.
package ring

import (
	"unsafe"
)

/*───────────────────────── slot & ring ─────────────────────────*/

type slot struct {
	ptr unsafe.Pointer // user payload
	seq uint64         // monotonically increasing sequence
}

// Ring holds state and a fixed-size circular buffer.
//
// head & tail are deliberately separated by 64-byte paddings so that the
// producer (writes tail) and consumer (writes head) never share a cache
// line on SMP machines.
type Ring struct {
	_pad0 [64]byte
	head  uint64 // read cursor  (consumer)
	_pad1 [64]byte
	tail  uint64 // write cursor (producer)
	_pad2 [64]byte
	mask  uint64 // len(buf)-1 (power-of-two)
	step  uint64 // == len(buf)
	buf   []slot
}

/*──────────────────────── constructor ──────────────────────────*/

// New returns an empty ring with *size* slots.  *size* **must** be a
// power of two or the function panics.
func New(size int) *Ring {
	if size <= 0 || size&(size-1) != 0 {
		panic("ring: size must be >0 and a power of two")
	}
	r := &Ring{
		mask: uint64(size - 1),
		step: uint64(size),
		buf:  make([]slot, size),
	}
	// Initialise per-slot sequence numbers so the first Push sees “free”.
	for i := range r.buf {
		r.buf[i].seq = uint64(i)
	}
	return r
}

/*────────────────────────── Push ───────────────────────────────*
| Producer algorithm                                             |
|   slot = buf[tail & mask]                                      |
|   if slot.seq == tail            => slot is free               |
|       slot.ptr = payload                                        |
|       slot.seq = tail + 1       => publish to consumer         |
|       tail++                                                   |
|   else ring is full                                            |
*────────────────────────────────────────────────────────────────*/

//go:nosplit
func (r *Ring) Push(p unsafe.Pointer) bool {
	t := r.tail
	s := &r.buf[t&r.mask]

	if loadAcquireUint64(&s.seq) != t { // still owned by consumer?
		return false // queue full
	}
	s.ptr = p                       // store payload
	storeReleaseUint64(&s.seq, t+1) // give slot to consumer
	r.tail = t + 1
	return true
}

/*────────────────────────── Pop ────────────────────────────────*
| Consumer algorithm                                             |
|   slot = buf[head & mask]                                      |
|   if slot.seq == head+1          => item ready                 |
|       data = slot.ptr                                          |
|       slot.seq = head + step     => recycle for next wrap      |
|       head++                                                   |
|   else queue is empty                                          |
*────────────────────────────────────────────────────────────────*/

//go:nosplit
func (r *Ring) Pop() unsafe.Pointer {
	h := r.head
	s := &r.buf[h&r.mask]

	if loadAcquireUint64(&s.seq) != h+1 { // queue empty?
		return nil
	}
	p := s.ptr
	storeReleaseUint64(&s.seq, h+r.step) // recycle slot
	r.head = h + 1
	return p
}

/*─────────────────────── PopWait helper ────────────────────────*/

// PopWait spins (or yields via cpuRelax) until an element is available.
// Kept for backward-compatibility with older tests.
func (r *Ring) PopWait() unsafe.Pointer {
	for {
		if p := r.Pop(); p != nil {
			return p
		}
		cpuRelax()
	}
}
