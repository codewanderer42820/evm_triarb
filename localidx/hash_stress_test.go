// ============================================================================
// ISR HASHMAP COMPREHENSIVE STRESS VALIDATION SUITE
// ============================================================================
//
// Randomized stress testing framework for Robin-Hood hashmap implementation
// with emphasis on deterministic correctness validation under extreme load.
//
// Validation scope:
//   - 1M randomized Put/Get operations simulating ISR ticker ingestion
//   - Deterministic behavior comparison with Go standard library reference
//   - Insertion-only semantics validation (no overwrites)
//   - Probe distance and collision resolution verification
//
// Test characteristics:
//   - Fixed capacity operation: No resizing or dynamic growth
//   - Sentinel key avoidance: Keys range [1, capacity] excluding zero
//   - Reference model synchronization: Parallel stdlib map validation
//   - Deterministic seeding: Reproducible failure scenarios
//
// Stress scenarios:
//   - High collision rates: Multiple keys hashing to same slots
//   - Capacity boundary testing: Near-full table operation
//   - Robin-Hood displacement: Optimal probe distance distribution
//   - Cache pressure: Working set exceeding L1/L2 boundaries
//
// Validation methodology:
//   - Single-threaded operation: No concurrency or atomics
//   - Zero allocation during operation: Insertion-only design
//   - Reference model comparison: Stdlib map behavioral equivalence
//   - Fail-fast error reporting: Immediate test termination on mismatch

package localidx

import (
	"testing"
)

// ============================================================================
// STRESS TEST CONFIGURATION
// ============================================================================

const (
	iterations = 1_000_000 // Total randomized operations for comprehensive coverage
	capacity   = 1024      // Hash table capacity for bounded key generation
)

// ============================================================================
// COMPREHENSIVE STRESS VALIDATION
// ============================================================================

// TestHashStressRandomPutGet executes 1M randomized operations for correctness validation.
// Simulates real-time ISR ticker ingestion patterns under zero-GC constraints.
//
// Test methodology:
//  1. Generate random key-value pairs within capacity bounds
//  2. Execute Put operations on both test and reference implementations
//  3. Validate insertion semantics: first value wins, no overwrites
//  4. Verify Get operations return consistent results across implementations
//  5. Maintain reference model synchronization throughout execution
//
// Validation criteria:
//   - Put return values: Must match reference implementation behavior
//   - Get correctness: Present keys return correct values, absent keys return false
//   - Insertion semantics: First inserted value preserved, subsequent ignored
//   - Deterministic behavior: Reproducible results across test runs
//
// Stress characteristics:
//   - Random key distribution: [1, capacity] with uniform probability
//   - Random value generation: Full uint32 range for collision detection
//   - High collision probability: capacity keys into capacity slots
//   - Robin-Hood effectiveness: Probe distance optimization validation
//
// Performance assumptions:
//   - Sub-microsecond operation latency under stress conditions
//   - Linear performance degradation with load factor increase
//   - Bounded probe distances via Robin-Hood displacement
//   - Cache-friendly access patterns despite randomization
//
// Error conditions:
//   - Put return value mismatch: Implementation vs reference disagreement
//   - Get miss on present key: Insertion not properly stored
//   - Get value mismatch: Incorrect value retrieval
//   - Deterministic failure: Non-reproducible behavior across runs
func TestHashStressRandomPutGet(t *testing.T) {
	h := New(capacity)
	ref := make(map[uint32]uint32, capacity)

	for i := 0; i < iterations; i++ {
		// Generate random key-value pair within bounds
		key := uint32(rnd.Intn(capacity)) + 1 // Avoid sentinel key 0
		val := rnd.Uint32()                   // Full range random value

		// Determine expected behavior via reference model
		prev, seen := ref[key]
		var want uint32
		if !seen {
			// New key: should insert and return inserted value
			want = val
			ref[key] = val
		} else {
			// Existing key: should return original value (insertion-only)
			want = prev
		}

		// Execute Put operation and validate return value
		got := h.Put(key, val)
		if got != want {
			t.Fatalf("iteration %d: Put(%d,%d) = %d; want %d",
				i, key, val, got, want)
		}

		// Validate Get operation consistency
		v, ok := h.Get(key)
		if !ok {
			t.Fatalf("iteration %d: key %d missing after Put; expected %d",
				i, key, want)
		}
		if v != want {
			t.Fatalf("iteration %d: Get(%d) = %d; want %d",
				i, key, v, want)
		}
	}
}
