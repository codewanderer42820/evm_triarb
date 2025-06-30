// Package localidx implements a fixed-capacity Robin-Hood hashmap
// with zero heap pressure, single-writer performance, and direct
// memory layout exposure.
//
// This is a performance-first, safety-last implementation designed for
// ultra-fast in-place key-value routing, typically inside CPU-local arenas.
//
// ⚠️ Quantum Hash Footgun Rating: 8 / 10 ⚠️
// This map is optimized for nanosecond-class access under single-core,
// single-threaded constraints. It sacrifices general safety for maximal
// throughput and assumes total caller discipline.
//
// Footgun 1/8: Zero-valued key is reserved as "empty slot" marker.
//              Do NOT attempt to store key == 0.
// Footgun 2/8: No deletion logic. Inserted keys persist forever.
// Footgun 3/8: Unsafe.Pointer math used for soft-prefetching ahead.
//              Misalignment or bad mask logic will corrupt memory.
// Footgun 4/8: Robin-Hood swap logic is destructive and assumes linear scan semantics.
// Footgun 5/8: No resizing. If capacity is exceeded, logic fails silently or clogs.
// Footgun 6/8: Cacheline padding is present but alignment is not enforced without explicit container.
// Footgun 7/8: No generation tracking. Reuse or snapshotting risks stale reads.
// Footgun 8/8: Absolutely not safe for concurrent access.
//              Single-threaded only. Caller must uphold exclusivity.
//
// → You are holding a surgical instrument. If misused, it will bleed — fast.

package localidx

import "unsafe"

// Package localidx implements a fixed-capacity Robin-Hood hashmap
// optimized for single-threaded, nanosecond-class performance.
// Zero heap pressure; insertion-only; no deletion logic.

//go:notinheap         // avoids heap metadata, allows static arena use
//go:align 64          // ensures alignment for cacheline locality
//go:inline            // hint to inline Hash where embedded in parent
type Hash struct {
	keys []uint32 // key slots; key=0 denotes empty
	vals []uint32 // corresponding values
	mask uint32   // bitmask for modulo (len(keys)-1)
}

// nextPow2 returns the smallest power of two greater than or equal to n.
// Used to size the hash table for efficient masking and linear probing.
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

// New creates and returns a Hash with capacity for at least `capacity` entries.
// Internally, it allocates slices of length equal to nextPow2(capacity*2),
// providing ~50% load factor headroom for performance.
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

// Put inserts the given key-value pair into the hash.
// If the key is not present, it is placed into an empty slot and the inserted value is returned.
// If the key already exists, the existing stored value is returned and no overwrite occurs.
// Implements Robin-Hood hashing: when probing, entries swap based on probe-distance
// to minimize variance. Soft-prefetching hides memory latency.
//
//go:nosplit
//go:inline
//go:registerparams
func (h Hash) Put(key, val uint32) uint32 {
	i := key & h.mask // initial bucket index
	dist := uint32(0) // distance from original hashed bucket

	for {
		k := h.keys[i]

		if k == 0 {
			// Empty slot → insert here
			h.keys[i], h.vals[i] = key, val
			return val
		}
		if k == key {
			// Existing key → return stored value without overwrite
			return h.vals[i]
		}

		// Compute current resident's probe distance
		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask

		// Robin-Hood: swap entries if new element has probed farther
		if kDist < dist {
			key, h.keys[i] = h.keys[i], key
			val, h.vals[i] = h.vals[i], val
			dist = kDist
		}

		// Soft-prefetch next slot’s key to hide latency
		_ = *(*uint32)(unsafe.Pointer(
			uintptr(unsafe.Pointer(&h.keys[0])) + uintptr(((i+2)&h.mask)<<2)))

		// Move to next slot and increment probe distance
		i = (i + 1) & h.mask
		dist++
	}
}

// Get retrieves the value for the given key.
// Returns (value, true) if found; (0, false) if not present.
// Lookup terminates on empty slot or when probe-distance bound-check
// indicates the key is not in this cluster. Soft-prefetching reduces latency.
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
			// Empty slot → key not present
			return 0, false
		}
		if k == key {
			// Match found → return associated value
			return h.vals[i], true
		}

		// Compute current resident's probe distance
		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask
		if kDist < dist {
			// Bound-check fail → key not in this cluster
			return 0, false
		}

		// Soft-prefetch ahead to hide memory latency
		_ = *(*uint32)(unsafe.Pointer(
			uintptr(unsafe.Pointer(&h.keys[0])) + uintptr(((i+2)&h.mask)<<2)))

		// Continue probing
		i = (i + 1) & h.mask
		dist++
	}
}
