package localidx

import "unsafe"

// Hash implements a fixed-capacity Robin-Hood hashmap.
//
// Design:
//   - Key and value slices are allocated in parallel.
//   - Capacity is always a power-of-two ×2 the requested capacity.
//   - Mask allows modulo via bitmasking.
//   - Single writer; no sync needed.
//   - Zero-valued key marks empty slot (so key==0 is disallowed).
//
// Optimized for:
//   - Tightest probe latency under load (Robin-Hood policy equalizes path).
//   - Memory layout ensures fast vectorization and prefetch friendliness.
type Hash struct {
	keys []uint32 // key slots; key=0 denotes empty
	vals []uint32 // corresponding values
	mask uint32   // bitmask for modulo (len(keys)-1)
	_pad [56]byte // cacheline isolation for high-throughput (was [4]byte)
}

// nextPow2 rounds an integer up to the next power-of-two.
// Used to compute table capacity and mask.
//
//go:nosplit
//go:inline
func nextPow2(n int) uint32 {
	s := uint32(1)
	for s < uint32(n) {
		s <<= 1
	}
	return s
}

// New creates a new Hash with enough capacity to store 'capacity' entries
// with a ~50% load factor. Underlying slices are allocated as power-of-two.
//
//go:nosplit
//go:inline
func New(capacity int) Hash {
	sz := nextPow2(capacity * 2) // doubled → leaves 50% headroom
	return Hash{
		keys: make([]uint32, sz),
		vals: make([]uint32, sz),
		mask: sz - 1,
	}
}

// Put inserts a key with associated value.
// If the key already exists, it returns the existing value.
// Otherwise, it stores 'val' and returns it.
//
// Algorithm:
//   - Robin-Hood probing with displacement comparison.
//   - If incoming key's probe distance is larger than occupant’s, it swaps.
//   - Probing continues until an empty slot or match is found.
//   - Empty slot → insert and return.
//   - Match → return current value (no overwrite).
//
//go:nosplit
func (h Hash) Put(key, val uint32) uint32 {
	i := key & h.mask // initial bucket index
	dist := uint32(0) // distance from original hashed bucket

	for {
		k := h.keys[i]

		// Empty slot → insert here
		if k == 0 {
			h.keys[i], h.vals[i] = key, val
			return val
		}

		// Existing key → return stored value
		if k == key {
			return h.vals[i]
		}

		// Compute current resident's probe distance
		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask

		// Robin-Hood: swap if we're "poorer" (i.e., have higher dist)
		if kDist < dist {
			key, h.keys[i] = h.keys[i], key
			val, h.vals[i] = h.vals[i], val
			dist = kDist
		}

		// Soft-prefetch next slot’s key to overlap memory latency
		_ = *(*uint32)(unsafe.Pointer(
			uintptr(unsafe.Pointer(&h.keys[0])) + uintptr(((i+2)&h.mask)<<2)))

		// Linear probe to next slot
		i = (i + 1) & h.mask
		dist++
	}
}

// Get returns the value associated with the key if present.
// Otherwise, it returns (0, false).
//
// Optimizations:
//   - Early exit via “bound check”: if current slot's probe distance is
//     smaller than ours, key must be missing (Robin-Hood guarantee).
//
//go:nosplit
func (h Hash) Get(key uint32) (uint32, bool) {
	i := key & h.mask
	dist := uint32(0)

	for {
		k := h.keys[i]

		if k == 0 {
			return 0, false // empty slot = not present
		}
		if k == key {
			return h.vals[i], true // match
		}

		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask
		if kDist < dist {
			return 0, false // bound-check fail → key not in cluster
		}

		// Soft-prefetch ahead
		_ = *(*uint32)(unsafe.Pointer(
			uintptr(unsafe.Pointer(&h.keys[0])) + uintptr(((i+2)&h.mask)<<2)))

		i = (i + 1) & h.mask
		dist++
	}
}
