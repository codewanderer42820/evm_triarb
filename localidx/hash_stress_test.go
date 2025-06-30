// Package localidx provides stress testing for the fixed-capacity,
// Robin-Hood "footgun" hashmap defined in hash.go. This test
// verifies correctness under heavy randomized Put/Get load.
package localidx

import (
	"testing"
)

const (
	iterations = 1_000_000 // total random operations to perform
	capacity   = 1024      // initial hash capacity for the test
)

// -----------------------------------------------------------------------------
// ░░ Stress Test: Randomized Put/Get ░░
// -----------------------------------------------------------------------------
// TestHashStressRandomPutGet performs a million randomized Put/Get operations
// against our Hash and compares results to a Go map reference. Ensures:
//   - First-insert wins on duplicates (Put returns existing value)
//   - Get returns correct presence and value
//   - Mixed hit/miss lookups remain consistent under load
func TestHashStressRandomPutGet(t *testing.T) {
	// initialize hash and reference map
	h := New(capacity)
	ref := make(map[uint32]uint32, capacity)

	for i := 0; i < iterations; i++ {
		key := rnd.Uint32()
		val := rnd.Uint32()

		// Determine expected return value from Put
		// first time: expect val, then freeze to that
		prev, seen := ref[key]
		var want uint32
		if !seen {
			want = val
			ref[key] = val
		} else {
			want = prev
		}

		// Execute Put and verify its return
		got := h.Put(key, val)
		if got != want {
			t.Fatalf("iteration %d: Put(%d,%d) = %d; want %d", i, key, val, got, want)
		}

		// Immediately check Get for correctness
		if v, ok := h.Get(key); !ok {
			t.Fatalf("iteration %d: Get(%d) missing; expected %d", i, key, want)
		} else if v != want {
			t.Fatalf("iteration %d: Get(%d) = %d; want %d", i, key, v, want)
		}

		// Periodically probe random keys for hit/miss fidelity
		if i%10000 == 0 {
			rk := rnd.Uint32()
			refV, refOK := ref[rk]
			v, ok := h.Get(rk)
			if refOK {
				if !ok || v != refV {
					t.Fatalf("iter %d: Get existing %d = (%d,%v); want (%d,true)",
						i, rk, v, ok, refV)
				}
			} else {
				if ok {
					t.Fatalf("iter %d: Get missing %d = (%d,true); want (_,false)",
						i, rk, v)
				}
			}
		}
	}
}
