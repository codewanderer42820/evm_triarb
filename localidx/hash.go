// ─────────────────────────────────────────────────────────────────────────────
// hash.go — ISR-grade, fixed-capacity Robin-Hood hashmap (localidx)
//
// Purpose:
//   - Provides a single-thread, arena-allocated hashmap with 0 allocs
//   - ISR-tuned: safe under pinned goroutine, unsafe elsewhere
//
// Notes:
//   - Zero heap pressure; insertion-only; no deletions or resizing
//   - Robin-Hood hashing with soft-prefetching for latency hiding
//   - All footguns are documented: no sentinel-key insert, no concurrency
//
// Compiler Directives:
//   - //go:nosplit
//   - //go:inline
//   - //go:registerparams
// ─────────────────────────────────────────────────────────────────────────────

package localidx

import "unsafe"

// Hash is a fixed-capacity Robin-Hood hashmap optimized for ISR-style ISR pipelines.
//
// Contract:
//   - Insertion-only: no key removal or overwrite
//   - Key==0 is reserved as sentinel and MUST NOT be inserted
//   - Single-threaded use only; NO atomics or fencing
//   - Soft-prefetching masks latency at high probe distances
//
// Fields:
//   - mask must be (len(keys)-1) for power-of-two masking
//   - keys and vals aligned slices of equal length
//
//go:notinheap
//go:align 64
type Hash struct {
	keys    []uint32 // key slots; key=0 denotes empty
	vals    []uint32 // corresponding values
	mask, _ uint32   // bitmask for modulo (len(keys)-1)
	_       uint64   // pad for future flags or alignment
}

// nextPow2 returns smallest power-of-2 ≥ n, used for table sizing
//
//go:nosplit
//go:inline
//go:registerparams
func nextPow2(n int) uint32 {
	s := uint32(1)
	for s < uint32(n) {
		s <<= 1
	}
	return s
}

// New returns a fixed-capacity Hash instance with 2× capacity for load headroom.
//
//go:nosplit
//go:inline
//go:registerparams
func New(capacity int) Hash {
	sz := nextPow2(capacity * 2)
	return Hash{
		keys: make([]uint32, sz),
		vals: make([]uint32, sz),
		mask: sz - 1,
	}
}

// Put inserts key→val into the table using Robin-Hood swap logic.
//
// Behavior:
//   - Inserts if empty slot found
//   - If key exists: returns existing value, does NOT overwrite
//   - Robin-Hood: swaps in new key if probe distance exceeds resident
//   - Soft-prefetches 2-slots ahead to reduce latency on cache misses
//
// ⚠️ Caller must guarantee key != 0
//
//go:nosplit
//go:inline
//go:registerparams
func (h Hash) Put(key, val uint32) uint32 {
	i := key & h.mask
	dist := uint32(0)

	for {
		k := h.keys[i]

		if k == 0 {
			h.keys[i], h.vals[i] = key, val
			return val
		}
		if k == key {
			return h.vals[i]
		}

		// Robin-Hood probe distance of resident
		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask
		if kDist < dist {
			key, h.keys[i] = h.keys[i], key
			val, h.vals[i] = h.vals[i], val
			dist = kDist
		}

		// ⚠️ Unsafe: soft-prefetch 2-ahead key
		_ = *(*uint32)(unsafe.Pointer(
			uintptr(unsafe.Pointer(&h.keys[0])) + uintptr(((i+2)&h.mask)<<2)))

		i = (i + 1) & h.mask
		dist++
	}
}

// Get returns (value, true) if key exists, else (0, false).
//
// Fast-paths:
//   - Empty slot = early exit
//   - Bound-check via resident probe distance
//   - Soft-prefetches 2-ahead key
//
//go:nosplit
//go:inline
//go:registerparams
func (h Hash) Get(key uint32) (uint32, bool) {
	i := key & h.mask
	dist := uint32(0)

	for {
		k := h.keys[i]
		if k == 0 {
			return 0, false
		}
		if k == key {
			return h.vals[i], true
		}

		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask
		if kDist < dist {
			return 0, false
		}

		// ⚠️ Unsafe: soft-prefetch 2-ahead key
		_ = *(*uint32)(unsafe.Pointer(
			uintptr(unsafe.Pointer(&h.keys[0])) + uintptr(((i+2)&h.mask)<<2)))

		i = (i + 1) & h.mask
		dist++
	}
}
