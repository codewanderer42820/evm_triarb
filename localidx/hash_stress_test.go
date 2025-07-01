// ─────────────────────────────────────────────────────────────────────────────
// hash_stress_test.go — Randomized ISR Stress Test for localidx.Hash
//
// Purpose:
//   - Applies 1 million randomized Put/Get operations on ISR hashmap
//   - Validates probe-bound logic, insertion freezing, and prefetch safety
//   - Confirms deterministic behavior matches Go stdlib reference map
//
// Notes:
//   - Keys range from [1,capacity] to avoid sentinel (0)
//   - Fixed-capacity: no resizing or deletions
//
// Compiler Constraints:
//   - Executed under single-core, single-threaded conditions only
// ─────────────────────────────────────────────────────────────────────────────

package localidx

import (
	"testing"
)

const (
	iterations = 1_000_000 // total random operations to perform
	capacity   = 1024      // hash capacity; keys drawn from [1,capacity]
)

// -----------------------------------------------------------------------------
// ░░ Stress Test: Randomized Put/Get within capacity ░░
// -----------------------------------------------------------------------------

// TestHashStressRandomPutGet performs 1M randomized Put/Get operations
// to simulate real-time ISR ticker ingestion under zero-GC load.
//
// Validates:
//   - Deterministic retention of first-inserted values (no overwrite)
//   - Return value of Put matches reference state
//   - Get returns presence and correct value per insertion logic
func TestHashStressRandomPutGet(t *testing.T) {
	h := New(capacity)
	ref := make(map[uint32]uint32, capacity)

	for i := 0; i < iterations; i++ {
		key := uint32(rnd.Intn(capacity)) + 1
		val := rnd.Uint32()

		prev, seen := ref[key]
		var want uint32
		if !seen {
			want = val
			ref[key] = val
		} else {
			want = prev
		}

		got := h.Put(key, val)
		if got != want {
			t.Fatalf("iteration %d: Put(%d,%d) = %d; want %d", i, key, val, got, want)
		}

		v, ok := h.Get(key)
		if !ok {
			t.Fatalf("iteration %d: key %d missing; expected %d", i, key, want)
		}
		if v != want {
			t.Fatalf("iteration %d: Get(%d) = %d; want %d", i, key, v, want)
		}
	}
}
