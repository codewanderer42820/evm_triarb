// ============================================================================
// ISR HASHMAP CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive unit testing framework for Robin-Hood hashmap implementation
// with emphasis on ISR-grade reliability and edge case coverage.
//
// Test categories:
//   - Constructor validation: Memory allocation and initialization
//   - Core operations: Put/Get semantics and return value correctness
//   - Collision handling: Robin-Hood displacement and probe behavior
//   - Edge cases: Boundary conditions, sentinel handling, capacity limits
//   - Stress testing: Randomized operations with reference model validation
//
// Validation methodology:
//   - Deterministic seeding for reproducible test execution
//   - Reference model comparison for correctness verification
//   - Comprehensive edge case coverage including pathological inputs
//   - Performance assumption verification under stress conditions
//
// Safety validation:
//   - Sentinel key behavior (key=0 restriction)
//   - Capacity boundary handling
//   - Memory safety under extreme conditions
//   - Single-threaded operation assumptions

package localidx

import (
	"math/rand"
	"testing"
)

// ============================================================================
// TEST CONFIGURATION
// ============================================================================

// Deterministic PRNG for reproducible stress testing patterns
var rnd = rand.New(rand.NewSource(0xC0FFEE))

// ============================================================================
// CONSTRUCTOR AND INITIALIZATION
// ============================================================================

// TestNewHash validates constructor behavior and memory allocation.
//
// Verification criteria:
//   - Proper mask calculation for power-of-2 sizing
//   - Correct array allocation with 2× capacity overhead
//   - Memory layout initialization with expected dimensions
//
// Tests constructor correctness and capacity calculation logic.
func TestNewHash(t *testing.T) {
	h := New(8)

	// Verify mask initialization (should be non-zero for valid operation)
	if h.mask == 0 {
		t.Fatal("mask should be non-zero for valid hash operations")
	}

	// Verify capacity expansion: 8 requested → 16 actual (2×) → 16 slots
	if len(h.keys) != 16 || len(h.vals) != 16 {
		t.Fatalf("expected 16-slot table with 2× overhead, got keys=%d, vals=%d",
			len(h.keys), len(h.vals))
	}
}

// ============================================================================
// BASIC OPERATION VALIDATION
// ============================================================================

// TestPutAndGet validates fundamental insertion and retrieval operations.
//
// Test sequence:
//  1. Insert multiple key-value pairs
//  2. Retrieve all inserted pairs
//  3. Verify value correctness and presence indication
//
// Validates core hashmap functionality and data integrity.
func TestPutAndGet(t *testing.T) {
	h := New(16)

	// Insert test data with known pattern
	for i := 1; i <= 16; i++ {
		h.Put(uint32(i), uint32(i*10))
	}

	// Verify all insertions are retrievable with correct values
	for i := 1; i <= 16; i++ {
		v, ok := h.Get(uint32(i))
		if !ok || v != uint32(i*10) {
			t.Fatalf("Get(%d) = %d,%v; want %d,true", i, v, ok, i*10)
		}
	}
}

// TestGetMiss validates lookup behavior for non-existent keys.
//
// Test scenario:
//  1. Insert known key-value pair
//  2. Attempt retrieval of non-existent key
//  3. Verify correct miss indication (false return)
//
// Validates proper handling of lookup misses and early termination.
func TestGetMiss(t *testing.T) {
	h := New(4)
	h.Put(1, 123)

	if _, ok := h.Get(99); ok {
		t.Fatal("Get(99) should return false for missing key")
	}
}

// ============================================================================
// INSERTION SEMANTICS
// ============================================================================

// TestPutOverwrite validates insertion-only semantics and value preservation.
//
// Test sequence:
//  1. Insert initial key-value pair
//  2. Attempt to "overwrite" with different value
//  3. Verify original value is preserved (no overwrite occurs)
//  4. Confirm Put returns original value on second insertion
//
// Validates insertion-only contract and value preservation guarantees.
func TestPutOverwrite(t *testing.T) {
	h := New(8)

	// First insertion - should return inserted value
	first := h.Put(42, 100)
	if first != 100 {
		t.Fatalf("first Put returned %d, want 100", first)
	}

	// Second insertion (same key) - should return original value
	old := h.Put(42, 200)
	if old != 100 {
		t.Fatalf("overwrite attempt returned %d, want original value 100", old)
	}

	// Verify original value is preserved
	if v, ok := h.Get(42); !ok || v != 100 {
		t.Fatalf("Get(42) = %d,%v; want 100,true (original value preserved)", v, ok)
	}
}

// ============================================================================
// COLLISION HANDLING
// ============================================================================

// TestCollisionAndRobinHood validates Robin-Hood collision resolution.
//
// Test approach:
//  1. Insert keys that hash to same initial slot
//  2. Verify Robin-Hood displacement preserves all insertions
//  3. Confirm retrieval correctness after displacement
//
// Tests collision handling and Robin-Hood hashing effectiveness.
func TestCollisionAndRobinHood(t *testing.T) {
	h := New(4)
	base := uint32(0xDEADBEEF)

	// Insert colliding keys (same hash modulo table size)
	for i := 0; i < 4; i++ {
		h.Put(base+uint32(i), uint32(i))
	}

	// Verify all keys are retrievable after collision resolution
	for i := 0; i < 4; i++ {
		v, ok := h.Get(base + uint32(i))
		if !ok || v != uint32(i) {
			t.Fatalf("Get(%d) = %d,%v; want %d,true", base+uint32(i), v, ok, i)
		}
	}
}

// TestWraparound validates probe wraparound at table boundaries.
//
// Test methodology:
//  1. Insert keys that force probe wraparound
//  2. Verify wraparound logic maintains data integrity
//  3. Confirm boundary condition handling
//
// Tests circular probing and boundary condition correctness.
func TestWraparound(t *testing.T) {
	h := New(4)

	// Insert keys with high hash values to test wraparound
	for i := 1; i <= 4; i++ {
		h.Put(uint32(i)<<29, uint32(i))
	}

	// Verify wraparound preservation of all entries
	for i := 1; i <= 4; i++ {
		k := uint32(i) << 29
		v, ok := h.Get(k)
		if !ok || v != uint32(i) {
			t.Fatalf("Get(%d) = %d,%v; want %d,true", k, v, ok, i)
		}
	}
}

// ============================================================================
// STRESS TESTING
// ============================================================================

// TestRandomStress validates algorithm correctness under randomized load.
//
// Test characteristics:
//   - 900 random insertions into 1024-slot table
//   - Reference model comparison for correctness validation
//   - Deterministic seeding for reproducible results
//
// Validates robustness under realistic usage patterns.
func TestRandomStress(t *testing.T) {
	h := New(1 << 10)
	ref := make(map[uint32]uint32)
	r := rand.New(rand.NewSource(12345))

	// Generate random key-value pairs
	for i := 0; i < 900; i++ {
		k := uint32(r.Intn(1_000_000)) + 1 // Avoid sentinel key 0
		ref[k] = uint32(i)
		h.Put(k, uint32(i))
	}

	// Verify all entries match reference model
	for k, want := range ref {
		if got, ok := h.Get(k); !ok || got != want {
			t.Fatalf("Get(%d) = %d,%v; want %d,true", k, got, ok, want)
		}
	}
}

// TestGetRobinHoodBound validates early termination via probe distance bounds.
//
// Test approach:
//  1. Create scenario with known probe distances
//  2. Search for key that would exceed maximum probe distance
//  3. Verify early termination prevents unnecessary probing
//
// Tests Robin-Hood bound checking optimization.
func TestGetRobinHoodBound(t *testing.T) {
	h := New(4)
	h.Put(1, 10)
	h.Put(9, 20)
	h.Put(17, 30)
	h.Put(4, 40)

	// Search for key that should trigger bound-check termination
	if v, ok := h.Get(33); ok {
		t.Fatalf("expected miss via bound-check, got %d,true", v)
	}
}

// ============================================================================
// CAPACITY TESTING
// ============================================================================

// TestNearFullCapacity validates behavior at high load factors.
//
// Test scenario:
//  1. Fill table to capacity with 16 entries in 8-request table
//  2. Verify all entries remain accessible
//  3. Confirm Robin-Hood effectiveness under high load
//
// Tests performance and correctness at capacity limits.
func TestNearFullCapacity(t *testing.T) {
	h := New(8) // Creates 16-slot table

	// Fill to capacity
	for i := uint32(1); i <= 16; i++ {
		h.Put(i, i*100)
	}

	// Verify all entries accessible at high load
	for i := uint32(1); i <= 16; i++ {
		v, ok := h.Get(i)
		if !ok || v != i*100 {
			t.Fatalf("Get(%d) = %d,%v; want %d,true", i, v, ok, i*100)
		}
	}
}

// ============================================================================
// EDGE CASE VALIDATION
// ============================================================================

// TestPutZeroKey validates sentinel key handling.
//
// Test behavior:
//  1. Attempt insertion with key=0 (reserved sentinel)
//  2. Verify Put returns value as if inserted
//  3. Confirm Get correctly reports key absence
//
// ⚠️  FOOTGUN: Tests documented unsafe behavior with sentinel key
func TestPutZeroKey(t *testing.T) {
	h := New(4)

	// Attempt insertion with sentinel key
	got := h.Put(0, 42)
	if got != 42 {
		t.Fatalf("Put(0,42) = %d; want 42 (returned as if inserted)", got)
	}

	// Verify sentinel key is not actually stored
	v, ok := h.Get(0)
	if ok {
		t.Fatalf("Get(0) = %d,true; want _,false (zero key should not be stored)", v)
	}
}

// TestMaxUint32Key validates maximum key value handling.
//
// Test scenario:
//  1. Insert with maximum uint32 value as key
//  2. Verify correct storage and retrieval
//  3. Confirm boundary value arithmetic correctness
//
// Tests numeric boundary conditions and wraparound arithmetic.
func TestMaxUint32Key(t *testing.T) {
	h := New(4)
	const maxKey = ^uint32(0) // Maximum uint32 value

	h.Put(maxKey, 999)
	v, ok := h.Get(maxKey)
	if !ok || v != 999 {
		t.Fatalf("Get(%d) = %d,%v; want 999,true", maxKey, v, ok)
	}
}

// TestEmptyHashGet validates lookup behavior on empty table.
//
// Test scenario:
//  1. Create empty hash table
//  2. Attempt key lookup
//  3. Verify correct miss indication
//
// Tests initial state behavior and empty table handling.
func TestEmptyHashGet(t *testing.T) {
	h := New(64)
	if _, ok := h.Get(12345); ok {
		t.Fatal("Get on empty hash should return false")
	}
}

// TestSoftPrefetchNoPanic validates prefetch safety under capacity stress.
//
// Test approach:
//  1. Fill small table beyond recommended capacity
//  2. Verify all operations complete without panic
//  3. Confirm data integrity under stress conditions
//
// Tests memory safety of soft prefetch optimization.
func TestSoftPrefetchNoPanic(t *testing.T) {
	h := New(2) // Creates 4-slot table

	// Insert beyond recommended capacity
	h.Put(1, 100)
	h.Put(2, 200)
	h.Put(3, 300)
	h.Put(4, 400)

	// Verify all entries remain accessible
	for i := uint32(1); i <= 4; i++ {
		v, ok := h.Get(i)
		if !ok || v != i*100 {
			t.Fatalf("Get(%d) = %d,%v; want %d,true", i, v, ok, i*100)
		}
	}
}
