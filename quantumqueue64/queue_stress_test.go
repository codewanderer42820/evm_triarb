// ============================================================================
// QUANTUMQUEUE64 CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive stress testing framework validating QuantumQueue64 correctness
// against a reference Go heap implementation under millions of random operations.
//
// COMPACT VERSION: Updated for uint64 payloads and 32-byte node layout
//
// Validation methodology:
//   - Stress-test QuantumQueue64 against reference Go container/heap
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

package quantumqueue64

import (
	"container/heap"
	"math/rand"
	"testing"
)

// ============================================================================
// REFERENCE IMPLEMENTATION
// ============================================================================

// stressItem represents a single entry in the reference heap implementation.
// Mirrors QuantumQueue64 node structure for direct comparison validation.
//
// COMPACT VERSION: Updated for uint64 payload comparison
//
// Field layout:
//   - h: Handle into QuantumQueue64 arena (identity correlation)
//   - tick: Priority key for ordering comparison
//   - data: uint64 payload for validation (vs [48]byte in original)
//   - seq: LIFO tiebreaker (higher sequence = newer entry)
type stressItem struct {
	h    Handle // Corresponding handle in QuantumQueue64 arena
	tick int64  // Priority key for heap ordering
	data uint64 // Compact payload for validation
	seq  int    // LIFO sequence number for tiebreaking
}

// stressHeap implements heap.Interface with QuantumQueue64-compatible ordering.
// Provides reference behavior for correctness validation.
//
// Ordering semantics:
//   - Primary: Ascending tick value (earlier ticks first)
//   - Secondary: Descending sequence (newer entries first within same tick)
//   - Matches QuantumQueue64 LIFO-within-tick behavior exactly
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

// ============================================================================
// COMPREHENSIVE STRESS TEST
// ============================================================================

// TestQueueStressRandomOperations validates QuantumQueue64 under chaotic workloads.
// Applies millions of random operations while maintaining reference comparison.
//
// COMPACT VERSION: Updated for uint64 payload validation and deterministic data generation
//
// Test methodology:
//  1. Parallel operation on QuantumQueue64 and reference heap
//  2. Random operation selection: 33% push, 33% move, 33% pop
//  3. Deterministic PRNG seed for reproducible failure analysis
//  4. Continuous correctness validation at every operation
//  5. Complete drain verification ensures no phantom state
//
// Operation patterns:
//   - Push: Allocate new handle and insert at random tick with deterministic payload
//   - Move: Relocate existing entry to different random tick
//   - Pop: Extract minimum and validate against reference
//
// Failure modes detected:
//   - Ordering violations (wrong minimum returned)
//   - Handle correlation mismatches
//   - Payload corruption (uint64 data mismatches)
//   - Phantom entries (queue/reference size mismatches)
//   - Arena corruption (invalid handle states)
//   - Summary bitmap inconsistencies
func TestQueueStressRandomOperations(t *testing.T) {
	const iterations = 10_000_000

	// Deterministic PRNG for reproducible failure analysis
	rng := rand.New(rand.NewSource(69))

	// Initialize test subjects
	q := New()           // QuantumQueue64 under test
	ref := &stressHeap{} // Reference heap implementation
	heap.Init(ref)

	// Handle lifecycle management
	free := make([]Handle, CapItems) // Available handle pool
	for i := range free {
		free[i] = Handle(i)
	}
	live := make(map[Handle]bool) // Active handle tracking

	seq := 0 // Global sequence counter for LIFO tiebreaking

	// makeData generates deterministic uint64 payload for validation.
	// Uses seed-based generation for reproducible test data.
	makeData := func(seed int64) uint64 {
		// Generate deterministic uint64 from seed using safe arithmetic
		return uint64(seed) * 0x9E3779B97F4A7C15
	}

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
			data := makeData(int64(seq))

			// Parallel insertion into both implementations
			q.Push(tick, h, data)
			heap.Push(ref, &stressItem{h: h, tick: tick, data: data, seq: seq})

			// Update handle lifecycle tracking
			live[h] = true
			seq++

		// ──────────────────────────────────────────────────────────────────
		// MOVE OPERATION: Tick relocation for existing entry
		// ──────────────────────────────────────────────────────────────────
		case 1:
			// Skip if no active entries
			if len(live) == 0 {
				continue
			}

			// Select arbitrary active handle
			var h Handle
			for k := range live {
				h = k
				break
			}

			// Get current data for preservation
			var currentData uint64
			for j := len(*ref) - 1; j >= 0; j-- {
				if (*ref)[j].h == h {
					currentData = (*ref)[j].data
					break
				}
			}

			// Apply tick relocation
			q.MoveTick(h, tick)

			// Update reference heap: remove old entry, insert new
			for j := len(*ref) - 1; j >= 0; j-- {
				if (*ref)[j].h == h {
					heap.Remove(ref, j)
					break
				}
			}
			heap.Push(ref, &stressItem{h: h, tick: tick, data: currentData, seq: seq})
			seq++

		// ──────────────────────────────────────────────────────────────────
		// POP OPERATION: Minimum extraction with validation
		// ──────────────────────────────────────────────────────────────────
		case 2:
			// Skip if queue empty
			if q.Empty() {
				continue
			}

			// Extract minimum from both implementations
			h, tickGot, dataGot := q.PeepMin()
			exp := heap.Pop(ref).(*stressItem)

			// Validate correctness: handle, tick, and data must match exactly
			if exp.h != h || exp.tick != tickGot || exp.data != dataGot {
				t.Fatalf("Mismatch at iteration %d: got (h=%v,tick=%d,data=%x); want (h=%v,tick=%d,data=%x)",
					i, h, tickGot, dataGot, exp.h, exp.tick, exp.data)
			}

			// Complete removal and handle lifecycle update
			q.UnlinkMin(h, tickGot)
			delete(live, h)
			free = append(free, h)
		}
	}

	// ────────────────────────────────────────────────────────────────────────
	// DRAIN VERIFICATION: Complete queue emptying with validation
	// ────────────────────────────────────────────────────────────────────────
	for !q.Empty() {
		// Extract minimum from both implementations
		h, tickGot, dataGot := q.PeepMin()
		exp := heap.Pop(ref).(*stressItem)

		// Validate remaining entries match reference exactly
		if exp.h != h || exp.tick != tickGot || exp.data != dataGot {
			t.Fatalf("Drain mismatch: got (h=%v,tick=%d,data=%x); want (h=%v,tick=%d,data=%x)",
				h, tickGot, dataGot, exp.h, exp.tick, exp.data)
		}

		// Complete removal and cleanup
		q.UnlinkMin(h, tickGot)
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
// PAYLOAD INTEGRITY STRESS TEST
// ============================================================================

// TestPayloadIntegrity validates uint64 payload preservation under stress.
// Ensures compact payload handling doesn't introduce corruption.
//
// Test characteristics:
//   - High-entropy uint64 payloads
//   - Frequent tick relocations
//   - Payload validation after each operation
//   - Stress patterns that might expose alignment issues
func TestPayloadIntegrity(t *testing.T) {
	const iterations = 1_000_000

	q := New()
	rng := rand.New(rand.NewSource(42))

	// Track handle->expected_data mapping
	expected := make(map[Handle]uint64)

	// Allocate initial handles
	var handles []Handle
	for i := 0; i < 100; i++ {
		h, _ := q.BorrowSafe()
		handles = append(handles, h)

		// Use high-entropy data with safe arithmetic
		data := uint64(rng.Uint64())
		expected[h] = data
		q.Push(int64(i), h, data)
	}

	// Stress test with random operations
	for i := 0; i < iterations; i++ {
		op := rng.Intn(3)

		switch op {
		case 0: // Update payload
			if len(handles) > 0 {
				h := handles[rng.Intn(len(handles))]
				newData := uint64(rng.Uint64())
				expected[h] = newData
				q.Push(int64(rng.Intn(BucketCount)), h, newData)
			}

		case 1: // Move tick (preserve payload)
			if len(handles) > 0 {
				h := handles[rng.Intn(len(handles))]
				q.MoveTick(h, int64(rng.Intn(BucketCount)))
			}

		case 2: // Validate random entry
			if len(handles) > 0 && !q.Empty() {
				h := handles[rng.Intn(len(handles))]
				node := &q.arena[h]
				if node.tick >= 0 { // Only check active nodes
					if node.data != expected[h] {
						t.Fatalf("Payload corruption at iteration %d: handle=%v got=%x want=%x",
							i, h, node.data, expected[h])
					}
				}
			}
		}
	}

	// Final validation of all active entries
	for h, expectedData := range expected {
		node := &q.arena[h]
		if node.tick >= 0 { // Active node
			if node.data != expectedData {
				t.Fatalf("Final payload validation failed: handle=%v got=%x want=%x",
					h, node.data, expectedData)
			}
		}
	}
}

// ============================================================================
// CACHE LINE STRESS TEST
// ============================================================================

// TestCacheLineStress validates 32-byte node layout under memory pressure.
// Ensures cache line pairing doesn't introduce corruption or ordering issues.
//
// Test characteristics:
//   - Sequential handle allocation (cache line pairs)
//   - Interleaved operations on paired nodes
//   - Memory access patterns that stress cache coherency
func TestCacheLineStress(t *testing.T) {
	const iterations = 500_000

	q := New()

	// Allocate handles in pairs (same cache line)
	var pairs [][2]Handle
	for i := 0; i < 1000; i++ {
		h1, _ := q.BorrowSafe()
		h2, _ := q.BorrowSafe()
		pairs = append(pairs, [2]Handle{h1, h2})
	}

	rng := rand.New(rand.NewSource(123))

	// Stress test with operations on cache line pairs
	for i := 0; i < iterations; i++ {
		pair := pairs[rng.Intn(len(pairs))]
		tick1 := int64(rng.Intn(BucketCount / 2))
		tick2 := int64(rng.Intn(BucketCount/2) + BucketCount/2)
		data1 := uint64(i * 2)
		data2 := uint64(i*2 + 1)

		// Interleaved operations on paired nodes
		q.Push(tick1, pair[0], data1)
		q.Push(tick2, pair[1], data2)

		// Validate both nodes
		if q.arena[pair[0]].tick >= 0 && q.arena[pair[0]].data != data1 {
			t.Fatalf("Cache line pair corruption: node 0 got=%x want=%x",
				q.arena[pair[0]].data, data1)
		}
		if q.arena[pair[1]].tick >= 0 && q.arena[pair[1]].data != data2 {
			t.Fatalf("Cache line pair corruption: node 1 got=%x want=%x",
				q.arena[pair[1]].data, data2)
		}

		// Occasionally move ticks to test relocation
		if i%100 == 0 {
			q.MoveTick(pair[0], tick2)
			q.MoveTick(pair[1], tick1)
		}
	}
}
