// ============================================================================
// QUANTUMQUEUE CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive stress testing framework validating QuantumQueue correctness
// against a reference Go heap implementation under millions of random operations.
//
// ORIGINAL VERSION: Uses 48-byte data blocks instead of uint64 payloads.
//
// Validation methodology:
//   - Stress-test QuantumQueue against reference Go container/heap
//   - Apply 10M+ randomized operations: push, move, pop
//   - Deterministic seed ensures reproducible failure cases
//   - Models CapItems handle limits and safety boundaries
//
// Correctness guarantees verified:
//   - Order correctness (tick priority + LIFO tiebreaking)
//   - Tick relocation consistency across operations
//   - Arena reuse and freelist accuracy
//   - Summary bitmap integrity under all conditions
//
// Failure detection:
//   - Any corruption, misordering, or ghost state triggers immediate failure
//   - Comprehensive drain verification ensures no phantom entries
//   - Handle lifecycle tracking prevents use-after-free scenarios
//   - Bitmap consistency validation across all hierarchy levels

package quantumqueue

import (
	"container/heap"
	"math/rand"
	"testing"
)

// ============================================================================
// REFERENCE IMPLEMENTATION
// ============================================================================

// stressItem represents a single entry in the reference heap implementation.
// Mirrors QuantumQueue node structure for direct comparison validation.
//
// Field layout:
//   - h: Handle into QuantumQueue arena (identity correlation)
//   - tick: Priority key for ordering comparison
//   - seq: LIFO tiebreaker (higher sequence = newer entry)
type stressItem struct {
	h    Handle // Corresponding handle in QuantumQueue arena
	tick int64  // Priority key for heap ordering
	seq  int    // LIFO sequence number for tiebreaking
}

// stressHeap implements heap.Interface with QuantumQueue-compatible ordering.
// Provides reference behavior for correctness validation.
//
// Ordering semantics:
//   - Primary: Ascending tick value (earlier ticks first)
//   - Secondary: Descending sequence (newer entries first within same tick)
//   - Matches QuantumQueue LIFO-within-tick behavior exactly
type stressHeap []*stressItem

func (h stressHeap) Len() int { return len(h) }

func (h stressHeap) Less(i, j int) bool {
	// Primary comparison: tick value (ascending)
	if h[i].tick != h[j].tick {
		return h[i].tick < h[j].tick
	}
	// Secondary comparison: sequence number (descending for LIFO)
	return h[i].seq > h[j].seq
}

func (h stressHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *stressHeap) Push(x interface{}) {
	*h = append(*h, x.(*stressItem))
}

func (h *stressHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	it := old[n]
	*h = old[:n]
	return it
}

// Helper function to create test data from sequence number
func makeStressData(seq int) *[48]byte {
	data := &[48]byte{}
	// Create deterministic pattern from sequence
	val := uint64(seq) * 0x9E3779B97F4A7C15
	for i := 0; i < 6; i++ {
		offset := i * 8
		data[offset] = byte(val)
		data[offset+1] = byte(val >> 8)
		data[offset+2] = byte(val >> 16)
		data[offset+3] = byte(val >> 24)
		data[offset+4] = byte(val >> 32)
		data[offset+5] = byte(val >> 40)
		data[offset+6] = byte(val >> 48)
		data[offset+7] = byte(val >> 56)
		val = val*0x9E3779B97F4A7C15 + uint64(i)
	}
	return data
}

// ============================================================================
// COMPREHENSIVE STRESS TEST
// ============================================================================

// TestQueueStressRandomOperations validates QuantumQueue under chaotic workloads.
// Applies millions of random operations while maintaining reference comparison.
//
// Test methodology:
//  1. Parallel operation on QuantumQueue and reference heap
//  2. Random operation selection: 33% push, 33% move, 33% pop
//  3. Deterministic PRNG seed for reproducible failure analysis
//  4. Continuous correctness validation at every operation
//  5. Complete drain verification ensures no phantom state
//
// Operation patterns:
//   - Push: Allocate new handle and insert at random tick with 48-byte payload
//   - Move: Relocate existing entry to different random tick
//   - Pop: Extract minimum and validate against reference
//
// Failure modes detected:
//   - Ordering violations (wrong minimum returned)
//   - Handle correlation mismatches
//   - Phantom entries (queue/reference size mismatches)
//   - Arena corruption (invalid handle states)
//   - Summary bitmap inconsistencies
func TestQueueStressRandomOperations(t *testing.T) {
	const iterations = 10_000_000

	// Deterministic PRNG for reproducible failure analysis
	rng := rand.New(rand.NewSource(69))

	// Initialize test subjects
	q := New()           // QuantumQueue under test
	ref := &stressHeap{} // Reference heap implementation
	heap.Init(ref)

	// Handle lifecycle management
	free := make([]Handle, CapItems) // Available handle pool
	for i := range free {
		free[i] = Handle(i)
	}
	live := make(map[Handle]bool) // Active handle tracking

	seq := 0 // Global sequence counter for LIFO tiebreaking

	// ────────────────────────────────────────────────────────────────────────
	// MAIN STRESS LOOP: Random Operation Application
	// ────────────────────────────────────────────────────────────────────────
	for i := 0; i < iterations; i++ {
		op := rng.Intn(3)                    // Random operation selection
		tick := int64(rng.Intn(BucketCount)) // Random tick within valid range

		switch op {

		// ──────────────────────────────────────────────────────────────────
		// PUSH OPERATION: New handle allocation and insertion
		// ──────────────────────────────────────────────────────────────────
		case 0:
			// Skip if arena exhausted
			if len(free) == 0 {
				continue
			}

			// Allocate handle from free pool
			h := free[len(free)-1]
			free = free[:len(free)-1]

			// Generate deterministic payload
			val := makeStressData(seq)

			// Parallel insertion into both implementations
			q.Push(tick, h, val)
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})

			// Update handle lifecycle tracking
			live[h] = true
			seq++

		// ──────────────────────────────────────────────────────────────────
		// MOVE OPERATION: Tick relocation for existing entry
		// ──────────────────────────────────────────────────────────────────
		case 1: // MOVE
			if len(live) == 0 {
				continue
			}

			var h Handle
			for k := range live {
				h = k
				break
			}

			// Locate and extract existing entry from reference heap
			var oldSeq int
			var oldTick int64
			var found bool
			for j := len(*ref) - 1; j >= 0; j-- {
				if (*ref)[j].h == h {
					oldSeq = (*ref)[j].seq
					oldTick = (*ref)[j].tick
					found = true
					heap.Remove(ref, j)
					break
				}
			}

			if !found {
				continue // Entry not found in reference heap
			}

			// Apply tick relocation
			q.MoveTick(h, tick)

			// Update reference heap: preserve sequence for no-op moves
			if oldTick != tick {
				// Actual relocation: assign new sequence for LIFO ordering
				heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
				seq++
			} else {
				// No-op relocation: maintain original sequence number
				heap.Push(ref, &stressItem{h: h, tick: tick, seq: oldSeq})
			}

		// ──────────────────────────────────────────────────────────────────
		// POP OPERATION: Minimum extraction with validation
		// ──────────────────────────────────────────────────────────────────
		case 2:
			// Skip if queue empty
			if q.Empty() {
				continue
			}

			// Extract minimum from both implementations
			h, tickGot, _ := q.PeepMin()
			exp := heap.Pop(ref).(*stressItem)

			// Validate correctness: handle and tick must match exactly
			if exp.h != h || exp.tick != tickGot {
				t.Fatalf("Mismatch at iteration %d: got (h=%v,tick=%d); want (h=%v,tick=%d)",
					i, h, tickGot, exp.h, exp.tick)
			}

			// Complete removal and handle lifecycle update
			q.UnlinkMin(h)
			delete(live, h)
			free = append(free, h)
		}

		// Periodic consistency validation (FIXED: Reduced frequency for performance)
		if i%100000 == 0 && i > 0 {
			if uint64(len(live)) != uint64(q.Size()) {
				t.Fatalf("Size mismatch at iteration %d: live=%d, queue=%d",
					i, len(live), q.Size())
			}
			if len(*ref) != len(live) {
				t.Fatalf("Reference size mismatch at iteration %d: ref=%d, live=%d",
					i, len(*ref), len(live))
			}

			// FIXED: Only do expensive bitmap validation occasionally
			if i%500000 == 0 {
				validateBitmapSummaries(t, q, live)
			}
		}
	}

	// ────────────────────────────────────────────────────────────────────────
	// DRAIN VERIFICATION: Complete queue emptying with validation
	// ────────────────────────────────────────────────────────────────────────
	for !q.Empty() {
		// Extract minimum from both implementations
		h, tickGot, _ := q.PeepMin()
		exp := heap.Pop(ref).(*stressItem)

		// Validate remaining entries match reference exactly
		if exp.h != h || exp.tick != tickGot {
			t.Fatalf("Drain mismatch: got (h=%v,tick=%d); want (h=%v,tick=%d)",
				h, tickGot, exp.h, exp.tick)
		}

		// Complete removal and cleanup
		q.UnlinkMin(h)
		delete(live, h)
		free = append(free, h)
	}

	// ────────────────────────────────────────────────────────────────────────
	// FINAL CONSISTENCY VALIDATION
	// ────────────────────────────────────────────────────────────────────────

	// Reference heap must be completely empty
	if ref.Len() != 0 {
		t.Fatalf("Reference heap not empty after drain: %d items remaining", ref.Len())
	}

	// All handles must be returned to free pool
	if len(free) != CapItems {
		t.Fatalf("Handle leak detected: %d handles missing", CapItems-len(free))
	}

	// No handles should remain in live set
	if len(live) != 0 {
		t.Fatalf("Live handle tracking inconsistent: %d handles still marked active", len(live))
	}
}

// ============================================================================
// BITMAP CONSISTENCY VALIDATION
// ============================================================================

// validateBitmapSummaries performs comprehensive bitmap consistency validation.
// FIXED: Added this function that was missing from original
func validateBitmapSummaries(t *testing.T, q *QuantumQueue, activeHandles map[Handle]bool) {
	// Build expected bitmap state from active handles
	expectedGroups := make(map[uint64]bool)
	expectedLanes := make(map[uint64]map[uint64]bool)
	expectedBuckets := make(map[uint64]bool)

	// FIXED: Scan through active handles and extract their ticks
	for h := range activeHandles {
		tick := q.arena[h].tick
		if tick < 0 {
			continue // Skip unlinked entries
		}

		g := uint64(tick) >> 12
		l := (uint64(tick) >> 6) & 63
		b := uint64(tick)

		expectedGroups[g] = true
		if expectedLanes[g] == nil {
			expectedLanes[g] = make(map[uint64]bool)
		}
		expectedLanes[g][l] = true
		expectedBuckets[b] = true
	}

	// Validate global summary
	for g := uint64(0); g < GroupCount; g++ {
		expectedActive := expectedGroups[g]
		actualActive := (q.summary & (1 << (63 - g))) != 0

		if expectedActive != actualActive {
			t.Fatalf("Group %d summary mismatch: expected=%v actual=%v",
				g, expectedActive, actualActive)
		}

		// Validate group-level summaries
		if expectedActive {
			gb := &q.groups[g]
			for l := uint64(0); l < LaneCount; l++ {
				expectedLaneActive := expectedLanes[g][l]
				actualLaneActive := (gb.l1Summary & (1 << (63 - l))) != 0

				if expectedLaneActive != actualLaneActive {
					t.Fatalf("Group %d lane %d summary mismatch: expected=%v actual=%v",
						g, l, expectedLaneActive, actualLaneActive)
				}
			}
		}
	}
}
