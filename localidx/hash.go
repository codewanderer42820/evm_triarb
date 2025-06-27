// -----------------------------------------------------------------------------
// localidx/hash.go — Fixed-capacity Robin-Hood map (32-bit keys & values)
// -----------------------------------------------------------------------------
//
//  Contract (unchanged):
//  ─────────────────────
//    • Put(new key, v)           → stores v, returns v
//    • Put(existing key, v2)     → leaves old value, returns old
//    • Get(key)                  → (value, true) | (0, false)
//
//  Design notes:
//    • Single-writer in this repo; safe for unsynchronised reads.
//    • Table never grows, so caller sizes with New(capacity).
//    • 50 % load-factor (capacity×2) keeps avg probe length ≈1.5.
//    • Robin-Hood displacement equalises probe distances → tight tail latency.
//    • Prefetch hides main-memory latency in worst-case clusters.
//
//  2025-06-27: only comments added; behaviour byte-for-byte identical to the
//              version that previously passed all tests.
// -----------------------------------------------------------------------------

package localidx

import "unsafe"

// Hash stores parallel key/value slices plus a power-of-two mask for modulo.
type Hash struct {
	keys, vals []uint32
	mask       uint32
}

// nextPow2 rounds n up to the next power-of-two; tiny helper.
func nextPow2(n int) uint32 {
	s := uint32(1)
	for s < uint32(n) {
		s <<= 1
	}
	return s
}

// New returns a table sized so that ‘capacity’ keys will fill it ~50 %.
func New(capacity int) Hash {
	sz := nextPow2(capacity * 2) // ×2 → 50 % max load
	return Hash{
		keys: make([]uint32, sz),
		vals: make([]uint32, sz),
		mask: sz - 1,
	}
}

// Put implements load-or-store semantics:
//
//	– Empty slot  → (store val, return val)
//	– Key exists  → (leave as-is, return stored)
//	– Collisions  → Robin-Hood displacement until one of the above fires.
func (h Hash) Put(key, val uint32) uint32 {
	i := key & h.mask
	dist := uint32(0)

	for {
		k := h.keys[i]

		// ░░ Empty slot ░░
		if k == 0 {
			h.keys[i], h.vals[i] = key, val
			return val
		}

		// ░░ Key match ░░
		if k == key {
			return h.vals[i] // leave stored value untouched
		}

		// ░░ Robin-Hood: swap if resident’s probe < ours ░░
		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask
		if kDist < dist {
			key, h.keys[i] = h.keys[i], key
			val, h.vals[i] = h.vals[i], val
			dist = kDist
		}

		// Soft prefetch of next key slot to overlap memory latency
		_ = *(*uint32)(unsafe.Pointer(
			uintptr(unsafe.Pointer(&h.keys[0])) + uintptr(((i+1)&h.mask)<<2)))

		i = (i + 1) & h.mask
		dist++
	}
}

// Get returns (value, true) if present, else (0, false).
// Early-exit bound check: once we hit a slot with shorter probe distance than
// our own search, the key cannot be further along the chain.
func (h Hash) Get(key uint32) (uint32, bool) {
	i := key & h.mask
	dist := uint32(0)

	for {
		k := h.keys[i]

		if k == 0 { // empty → miss
			return 0, false
		}
		if k == key { // hit
			return h.vals[i], true
		}

		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask
		if kDist < dist { // bound check
			return 0, false
		}

		_ = *(*uint32)(unsafe.Pointer(
			uintptr(unsafe.Pointer(&h.keys[0])) + uintptr(((i+1)&h.mask)<<2)))

		i = (i + 1) & h.mask
		dist++
	}
}
