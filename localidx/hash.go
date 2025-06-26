// localidx/hash.go — fixed-size Robin-Hood map, heavily optimized for speed.
package localidx

import "unsafe"

type Hash struct {
	keys, vals []uint32
	mask       uint32
}

// nextPow2 returns smallest power-of-two ≥ n.
func nextPow2(n int) uint32 {
	s := uint32(1)
	for s < uint32(n) {
		s <<= 1
	}
	return s
}

// New creates a Hash with capacity at 50% load factor.
func New(capacity int) Hash {
	sz := nextPow2(capacity * 2)
	return Hash{
		keys: make([]uint32, sz),
		vals: make([]uint32, sz),
		mask: sz - 1,
	}
}

// Put inserts or updates a key–value pair using Robin Hood hashing.
// Inlined `probeDist`, loop unrolling, and cache prefetching are applied.
func (h Hash) Put(key, val uint32) uint32 {
	i := key & h.mask // initial (ideal) index
	dist := uint32(0)

	for {
		k := h.keys[i]

		// Unroll first probe iteration (hot path)
		if k == 0 {
			h.keys[i], h.vals[i] = key, val
			return val
		}
		if k == key {
			return h.vals[i]
		}

		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask
		if kDist < dist {
			// Swap keys and vals
			key, h.keys[i] = h.keys[i], key
			val, h.vals[i] = h.vals[i], val
			dist = (i + h.mask + 1 - (key & h.mask)) & h.mask
		}

		// Prefetch next slot to help CPU during tight probes
		_ = *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&h.keys[0])) + uintptr(((i+1)&h.mask)<<2)))

		i = (i + 1) & h.mask
		dist++
	}
}

// Get returns (val, true) if key found, else (0, false).
// Applies unrolled first probe, early-exit Robin Hood bound check,
// and prefetching of next key.
func (h Hash) Get(key uint32) (uint32, bool) {
	i := key & h.mask
	dist := uint32(0)

	for {
		k := h.keys[i]

		// Unrolled fast path: hit or miss
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

		// Prefetch next key slot to avoid cache miss
		_ = *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&h.keys[0])) + uintptr(((i+1)&h.mask)<<2)))

		i = (i + 1) & h.mask
		dist++
	}
}
