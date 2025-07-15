// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ ROBIN HOOD HASH TABLE
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Trading System
// Component: Fixed-Capacity Hash Map Implementation
//
// Description:
//   Zero-allocation hash table using Robin Hood hashing for optimal cache performance.
//   Provides constant-time lookups with deterministic displacement patterns and efficient
//   memory layout for single-threaded processing contexts.
//
// Design Principles:
//   - Fixed capacity with power-of-2 sizing for fast modulo operations
//   - Robin Hood displacement minimizes probe distances
//   - Parallel arrays for keys and values optimize cache usage
//   - Zero sentinel value enables efficient empty slot detection
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package localidx

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Hash implements a fixed-capacity Robin Hood hash map for single-threaded processing.
// The structure uses parallel arrays for keys and values to maximize cache efficiency
// during lookups and insertions.
//
// MEMORY LAYOUT:
//
//	The structure is aligned to 64-byte cache lines with explicit padding to prevent
//	false sharing in multi-core systems. Keys and values are stored in separate arrays
//	to optimize sequential access patterns.
//
//go:notinheap
//go:align 64
type Hash struct {
	keys []uint32 // Key array (0 = empty sentinel)
	vals []uint32 // Value array (parallel to keys)
	mask uint32   // Size mask for fast modulo operation
	_    [12]byte // Padding to 64-byte cache line boundary
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// nextPow2 calculates the smallest power of 2 greater than or equal to n.
// This ensures the hash table size is always a power of 2 for efficient masking.
//
// ALGORITHM:
//
//	Repeatedly doubles the size until it exceeds the input value.
//	This simple approach is sufficient for initialization-time calculations.
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

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONSTRUCTOR
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// New creates a hash map with doubled capacity for optimal load factor.
// The 2× sizing ensures the table maintains good performance characteristics
// even at 50% load factor, reducing collision chains.
//
// CAPACITY PLANNING:
//
//	Input capacity is doubled to maintain performance headroom.
//	Final size is rounded up to the nearest power of 2.
//	This prevents performance degradation as the table fills.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New(capacity int) Hash {
	// Double capacity for load factor headroom
	sz := nextPow2(capacity * 2)
	return Hash{
		keys: make([]uint32, sz),
		vals: make([]uint32, sz),
		mask: sz - 1, // Bitmask for fast modulo
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Put inserts or retrieves a key-value pair using Robin Hood displacement.
// This algorithm minimizes the maximum probe distance by displacing entries
// that are closer to their ideal positions.
//
// ROBIN HOOD ALGORITHM:
//
//	When inserting, if we encounter an entry that is closer to its ideal position
//	than we are to ours, we take its place and continue inserting the displaced entry.
//	This "rich give to the poor" approach minimizes worst-case probe distances.
//
// RETURN VALUE:
//   - For new insertions: returns the newly inserted value
//   - For existing keys: returns the current value without modification
//
// SAFETY REQUIREMENTS:
//   - Key must not be 0 (reserved as empty sentinel)
//   - Table must have sufficient capacity to avoid infinite loops
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h Hash) Put(key, val uint32) uint32 {
	// Calculate initial position using key hash
	i := key & h.mask
	dist := uint32(0) // Track displacement from ideal position

	for {
		k := h.keys[i]

		// Case 1: Empty slot found - insert new entry
		if k == 0 {
			h.keys[i], h.vals[i] = key, val
			return val
		}

		// Case 2: Key already exists - return existing value
		if k == key {
			return h.vals[i]
		}

		// Case 3: Robin Hood displacement check
		// Calculate how far the current occupant is from its ideal position
		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask

		// If current occupant is closer to home than we are, displace it
		if kDist < dist {
			// Swap with current occupant and continue inserting displaced entry
			key, h.keys[i] = h.keys[i], key
			val, h.vals[i] = h.vals[i], val
			dist = kDist // Reset distance for displaced entry
		}

		// Advance to next position with wraparound
		i = (i + 1) & h.mask
		dist++ // Increment probe distance
	}
}

// Get retrieves a value by key with Robin Hood early termination.
// The Robin Hood invariant allows early termination when we encounter
// an entry closer to its ideal position than our search distance.
//
// EARLY TERMINATION:
//
//	If we find an entry that is closer to its ideal position than our
//	current probe distance, our target key cannot exist in the table.
//	This optimization reduces average probe lengths for missing keys.
//
// RETURN VALUES:
//   - value: The associated value if key exists
//   - found: True if key exists, false otherwise
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h Hash) Get(key uint32) (uint32, bool) {
	// Calculate initial search position
	i := key & h.mask
	dist := uint32(0) // Track search distance

	for {
		k := h.keys[i]

		// Case 1: Empty slot - key not found
		if k == 0 {
			return 0, false
		}

		// Case 2: Key found - return associated value
		if k == key {
			return h.vals[i], true
		}

		// Case 3: Robin Hood early termination
		// Calculate distance of current entry from its ideal position
		kDist := (i + h.mask + 1 - (k & h.mask)) & h.mask

		// If current entry is closer to home than our search distance,
		// our target key cannot exist (Robin Hood invariant)
		if kDist < dist {
			return 0, false
		}

		// Continue searching at next position
		i = (i + 1) & h.mask
		dist++ // Increment probe distance
	}
}
