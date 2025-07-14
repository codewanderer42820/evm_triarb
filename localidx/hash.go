// localidx - ISR-grade Robin Hood hashmap with zero allocations
package localidx

// Hash - Fixed-capacity Robin Hood hashmap for single-threaded ISR processing
//
//go:notinheap
//go:align 64
type Hash struct {
	keys []uint32 // Key array (0 = empty sentinel)
	vals []uint32 // Value array (parallel to keys)
	mask uint32   // Size mask for modulo
	_    [12]byte // Padding to 64-byte cache line boundary
}

// nextPow2 finds smallest power-of-2 ≥ n
//
//go:norace
//go:nocheckptr
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

// New creates hashmap with 2× capacity for load factor headroom
//
//go:norace
//go:nocheckptr
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

// Put inserts key-value using Robin Hood displacement
// Returns: new value for insertions, existing value for duplicates
// ⚠️  SAFETY: key must not be 0 (reserved sentinel)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h Hash) Put(key, val uint32) uint32 {
	i := key & h.mask
	dist := uint32(0)

	for {
		k := h.keys[i]

		// Empty slot - insert
		if k == 0 {
			h.keys[i], h.vals[i] = key, val
			return val
		}

		// Key exists - return current value
		if k == key {
			return h.vals[i]
		}

		// Robin Hood displacement check
		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask

		// Swap if incoming traveled farther
		if kDist < dist {
			key, h.keys[i] = h.keys[i], key
			val, h.vals[i] = h.vals[i], val
			dist = kDist
		}

		// Advance
		i = (i + 1) & h.mask
		dist++
	}
}

// Get retrieves value with Robin Hood early termination
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h Hash) Get(key uint32) (uint32, bool) {
	i := key & h.mask
	dist := uint32(0)

	for {
		k := h.keys[i]

		// Empty slot - not found
		if k == 0 {
			return 0, false
		}

		// Key found
		if k == key {
			return h.vals[i], true
		}

		// Robin Hood early termination
		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask
		if kDist < dist {
			return 0, false
		}

		// Advance
		i = (i + 1) & h.mask
		dist++
	}
}
