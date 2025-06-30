// Package localidx provides stress testing for the fixed-capacity,
// Robin-Hood "footgun" hashmap defined in hash.go. This test
// verifies correctness under heavy randomized Put/Get load within table bounds.
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
// TestHashStressRandomPutGet performs a million randomized Put/Get operations
// on keys within the table bounds, ensuring no overflow of the fixed-capacity
// Hash. Compares against a Go map reference to validate:
//   - First-insert wins on duplicates (Put returns existing value)
//   - Get returns correct presence and value
func TestHashStressRandomPutGet(t *testing.T) {
	// initialize hash and reference map
	h := New(capacity)
	ref := make(map[uint32]uint32, capacity)

	for i := 0; i < iterations; i++ {
		// key in [1,capacity] to avoid sentinel and overflow
		key := uint32(rnd.Intn(capacity)) + 1
		val := rnd.Uint32()

		// compute expected Put return: first-insert or frozen value
		prev, seen := ref[key]
		var want uint32
		if !seen {
			want = val
			ref[key] = val
		} else {
			want = prev
		}

		// Execute Put and verify return
		got := h.Put(key, val)
		if got != want {
			t.Fatalf("iteration %d: Put(%d,%d) = %d; want %d", i, key, val, got, want)
		}

		// Verify via Get
		v, ok := h.Get(key)
		if !ok {
			t.Fatalf("iteration %d: key %d missing; expected %d", i, key, want)
		}
		if v != want {
			t.Fatalf("iteration %d: Get(%d) = %d; want %d", i, key, v, want)
		}
	}
}
