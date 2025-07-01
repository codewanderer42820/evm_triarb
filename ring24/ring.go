// ring.go — Lock-free single-producer/single-consumer (SPSC) ring queue (24-byte payload)
//
// ⚠️ Footgun Mode: ISR-grade ring queue with zero safety net.
// Designed for extreme throughput and predictability in tick routing, matching engine pipelines,
// or real-time ISR systems operating under full trust.
//
// Assumptions:
//   - Single writer, single reader (SPSC)
//   - Fixed-size [24]byte payloads only
//   - Power-of-two capacity, no dynamic resize
//   - External overflow management required
//
// Invariants:
//   - Push fails if full (returns false), Pop returns nil if empty
//   - Ring must be sized correctly at construction
//   - All operations are wait-free under correct use
//
//go:build amd64 || arm64 || riscv64

package ring24

import (
	"sync/atomic"
)

/*───────────────────────────── Slot Definition ─────────────────────────────*/

// slot holds one **24-byte** payload plus an 8-byte sequence counter.
// Total struct size: 32 B (half of a 64-byte cache line).
//
// Compiler directives:
//   - notinheap: prevent GC tagging
//   - align 64: ensure slot alignment for predictable cache placement
//
//go:notinheap
//go:align 64
//go:inline

type slot struct {
	val [24]byte // payload (bytes 0–23)
	seq uint64   // sequence number (bytes 24–31)
}

/*────────────────────────────── Ring Structure ──────────────────────────────*/

// Ring is a cacheline-padded SPSC ring buffer with:
//   - Producer/consumer cursors on isolated cache lines
//   - Fully lock-free, polling-friendly design
//   - Fixed [24]byte payloads
//   - Power-of-two sized backing array
//
// Compiler directives:
//   - notinheap: for arena/pinned usage
//   - align 64: force cacheline alignment
//
//go:notinheap
//go:align 64
//go:inline

type Ring struct {
	_    [64]byte // pad to isolate head
	head uint64   // consumer read cursor (cacheline #1)

	_    [56]byte // pad to isolate tail
	tail uint64   // producer write cursor (cacheline #2)

	_ [56]byte // reserved for future use (cacheline #3)

	mask uint64 // ring size-1 (must be power of 2)
	step uint64 // ring size (used in Pop to reset seq)
	buf  []slot // backing buffer

	_ [3]uint64 // tail padding to 256 bytes (4x cache lines)
}

/*──────────────────────────── Constructor ─────────────────────────────*/

// New constructs a ring buffer of size `size` (must be power of two).
// Panics if size is zero or not a power of two.
//
// Compiler directives:
//   - inline: enable full inlining at call sites
//   - nosplit: safe due to bounded frame and no GC
//
//go:inline
//go:nosplit
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

/*──────────────────────────── Push Operation ─────────────────────────────*/

// Push attempts to write a new value into the ring.
// Returns false if the ring is full (slot not ready).
//
// Caller must NOT retain `val` beyond Pop.
//
// Compiler directives:
//   - registerparams: pass pointer in register
//   - nosplit: suitable for ISR-safe environments
//
//go:inline
//go:nosplit
//go:registerparams
func (r *Ring) Push(val *[24]byte) bool {
	t := r.tail
	s := &r.buf[t&r.mask]
	if atomic.LoadUint64(&s.seq) != t {
		return false // slot not available
	}
	s.val = *val
	atomic.StoreUint64(&s.seq, t+1) // signal availability
	r.tail = t + 1
	return true
}

/*──────────────────────────── Pop Operation ─────────────────────────────*/

// Pop attempts to retrieve the next value from the ring.
// Returns nil if the ring is empty.
//
// The returned pointer is only valid until next Push or Pop.
//
// Compiler directives:
//   - registerparams: avoid stack spill for argument passing
//   - nosplit: avoids preemption or stack growth
//
//go:inline
//go:nosplit
//go:registerparams
func (r *Ring) Pop() *[24]byte {
	h := r.head
	s := &r.buf[h&r.mask]
	if atomic.LoadUint64(&s.seq) != h+1 {
		return nil // no data
	}

	val := &s.val
	atomic.StoreUint64(&s.seq, h+r.step) // mark slot reusable
	r.head = h + 1
	return val
}

/*──────────────────────────── PopWait (Spin) ─────────────────────────────*/

// PopWait spins until a value is available.
// Used for latency-critical ISR routines with active polling.
//
//go:inline
//go:nosplit
//go:registerparams
func (r *Ring) PopWait() *[24]byte {
	for {
		if p := r.Pop(); p != nil {
			return p
		}
		cpuRelax() // pause to yield HT/thread slot
	}
}
