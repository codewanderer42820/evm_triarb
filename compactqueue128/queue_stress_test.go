// ============================================================================
// COMPACTQUEUE128 CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive stress testing framework validating CompactQueue128 correctness
// against a reference Go heap implementation under millions of random operations.

package compactqueue128

import (
	"container/heap"
	"fmt"
	"math/rand"
	"testing"
	"unsafe"
)

// ============================================================================
// REFERENCE IMPLEMENTATION
// ============================================================================

// stressItem represents a single entry in the reference heap implementation.
type stressItem struct {
	h    Handle // Corresponding handle in CompactQueue128 pool
	tick int64  // Priority key for heap ordering
	seq  int    // LIFO sequence number for tiebreaking
}

// stressHeap implements heap.Interface with CompactQueue128-compatible ordering.
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

// TestQueueStressRandomOperations validates CompactQueue128 under chaotic workloads.
func TestQueueStressRandomOperations(t *testing.T) {
	const iterations = 5_000_000 // Reduced for smaller queue
	const maxHandles = 25000     // Reduced for smaller pool

	// Deterministic PRNG for reproducible failure analysis
	rng := rand.New(rand.NewSource(69))

	// Initialize external pool
	pool := make([]Entry, maxHandles)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Initialize reference heap
	ref := &stressHeap{}
	heap.Init(ref)

	// Handle lifecycle management - FIXED: Start handles from 1
	availableHandles := make([]Handle, maxHandles)
	for i := range availableHandles {
		availableHandles[i] = Handle(i + 1) // FIXED: 1, 2, 3, ... instead of 0, 1, 2, ...
	}
	activeHandles := make(map[Handle]bool)

	seq := 0

	makeVal := func(seed int64) uint64 {
		return uint64(seed) * 0x9E3779B97F4A7C15
	}

	// Debug tracking for the failing case
	debugOps := make([]string, 0, 1000) // Store recent operations
	debugSeq := make(map[Handle]int)    // Track sequence number for each handle
	debugTick := make(map[Handle]int64) // Track current tick for each handle

	for i := 0; i < iterations; i++ {
		op := rng.Intn(3)
		tick := int64(rng.Intn(BucketCount)) // Keep within 0-127 range

		// Add debug info for recent operations
		if len(debugOps) >= 1000 {
			debugOps = debugOps[1:] // Keep only last 1000 operations
		}

		switch op {
		case 0: // PUSH
			if len(availableHandles) == 0 {
				continue
			}

			h := availableHandles[len(availableHandles)-1]
			availableHandles = availableHandles[:len(availableHandles)-1]

			val := makeVal(int64(seq))

			debugOps = append(debugOps, fmt.Sprintf("PUSH iter=%d h=%d tick=%d seq=%d", i, h, tick, seq))

			q.Push(tick, h, val)
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})

			activeHandles[h] = true
			debugSeq[h] = seq
			debugTick[h] = tick
			seq++

		case 1: // MOVE
			if len(activeHandles) == 0 {
				continue
			}

			var h Handle
			for k := range activeHandles {
				h = k
				break
			}

			// Find and remove the old entry, preserving its sequence number
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
				debugOps = append(debugOps, fmt.Sprintf("MOVE SKIP iter=%d h=%d tick=%d (not found in ref)", i, h, tick))
				continue // Handle not found in reference, skip
			}

			debugOps = append(debugOps, fmt.Sprintf("MOVE iter=%d h=%d oldTick=%d->newTick=%d oldSeq=%d newSeq=%d", i, h, oldTick, tick, oldSeq, seq))

			q.MoveTick(h, tick)

			// Only increment sequence if tick actually changed (matching queue behavior)
			if oldTick != tick {
				// MoveTick acts like a new insertion at the destination tick
				// So we need a new sequence number to represent LIFO at the new tick
				heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
				debugSeq[h] = seq
				debugTick[h] = tick
				seq++
			} else {
				// No-op move, reinsert with same sequence number
				heap.Push(ref, &stressItem{h: h, tick: tick, seq: oldSeq})
				debugTick[h] = tick
				// Keep same sequence number from debugSeq[h]
			}

		case 2: // POP
			if q.Empty() {
				continue
			}

			h, tickGot, _ := q.PeepMin()
			exp := heap.Pop(ref).(*stressItem)

			debugOps = append(debugOps, fmt.Sprintf("POP iter=%d queueGot=(h=%d,tick=%d,seq=%d) refExp=(h=%d,tick=%d,seq=%d)",
				i, h, tickGot, debugSeq[h], exp.h, exp.tick, exp.seq))

			if exp.h != h || exp.tick != tickGot {
				// DETAILED DEBUG OUTPUT ON FAILURE
				t.Logf("=== MISMATCH DETECTED AT ITERATION %d ===", i)
				t.Logf("Queue returned: h=%d, tick=%d", h, tickGot)
				t.Logf("Reference expected: h=%d, tick=%d", exp.h, exp.tick)
				t.Logf("Queue handle seq: %d", debugSeq[h])
				t.Logf("Reference handle seq: %d", exp.seq)

				// Print recent operations
				t.Logf("\n=== RECENT OPERATIONS ===")
				start := len(debugOps) - 50
				if start < 0 {
					start = 0
				}
				for j := start; j < len(debugOps); j++ {
					t.Logf("%s", debugOps[j])
				}

				// Print current state of same tick entries
				t.Logf("\n=== CURRENT STATE FOR TICK %d ===", tickGot)
				sameTickHandles := make([]Handle, 0)
				for handle, currentTick := range debugTick {
					if currentTick == tickGot && activeHandles[handle] {
						sameTickHandles = append(sameTickHandles, handle)
					}
				}
				t.Logf("Active handles at tick %d: %v", tickGot, sameTickHandles)
				for _, handle := range sameTickHandles {
					t.Logf("  h=%d seq=%d", handle, debugSeq[handle])
				}

				// Print reference heap state for same tick
				t.Logf("\n=== REFERENCE HEAP STATE FOR TICK %d ===", tickGot)
				refSameTick := make([]*stressItem, 0)
				for _, item := range *ref {
					if item.tick == tickGot {
						refSameTick = append(refSameTick, item)
					}
				}
				t.Logf("Reference entries at tick %d:", tickGot)
				for _, item := range refSameTick {
					t.Logf("  h=%d seq=%d", item.h, item.seq)
				}

				// Check queue's bucket state directly - FIXED: Only if tickGot is valid
				if tickGot >= 0 && tickGot < BucketCount {
					t.Logf("\n=== QUEUE BUCKET STATE ===")
					b := Handle(uint64(tickGot))
					t.Logf("Bucket[%d] head: %d", b, q.buckets[b])

					// Walk the linked list
					current := q.buckets[b]
					listPos := 0
					for current != nilIdx && listPos < 10 { // Prevent infinite loop
						entry := q.entry(current)
						t.Logf("  [%d] h=%d tick=%d data=%d next=%d prev=%d",
							listPos, current, entry.Tick, entry.Data, entry.Next, entry.Prev)
						current = entry.Next
						listPos++
					}
				}

				t.Fatalf("Mismatch at iteration %d: got (h=%v,tick=%d); want (h=%v,tick=%d)",
					i, h, tickGot, exp.h, exp.tick)
			}

			q.UnlinkMin(h)
			delete(activeHandles, h)
			delete(debugSeq, h)
			delete(debugTick, h)

			// Reset the pool entry to unlinked state when returning handle
			// FIXED: Convert handle to pool index (h-1) for CompactQueue128
			poolIndex := h - 1
			pool[poolIndex].Tick = -1
			pool[poolIndex].Prev = nilIdx
			pool[poolIndex].Next = nilIdx
			pool[poolIndex].Data = 0

			availableHandles = append(availableHandles, h)
		}

		// Periodic consistency validation
		if i%100000 == 0 {
			if len(activeHandles) != q.Size() {
				t.Fatalf("Size mismatch at iteration %d: active=%d, queue=%d",
					i, len(activeHandles), q.Size())
			}
			if len(*ref) != len(activeHandles) {
				t.Fatalf("Reference size mismatch at iteration %d: ref=%d, active=%d",
					i, len(*ref), len(activeHandles))
			}

			// Check if all active handles are in reference heap
			refHandles := make(map[Handle]bool)
			for _, item := range *ref {
				refHandles[item.h] = true
			}

			for handle := range activeHandles {
				if !refHandles[handle] {
					t.Fatalf("Handle %d in activeHandles but missing from reference heap at iteration %d", handle, i)
				}
			}

			for handle := range refHandles {
				if !activeHandles[handle] {
					t.Fatalf("Handle %d in reference heap but missing from activeHandles at iteration %d", handle, i)
				}
			}
		}
	}

	// Drain verification
	for !q.Empty() {
		h, tickGot, _ := q.PeepMin()
		exp := heap.Pop(ref).(*stressItem)

		if exp.h != h || exp.tick != tickGot {
			t.Fatalf("Drain mismatch: got (h=%v,tick=%d); want (h=%v,tick=%d)",
				h, tickGot, exp.h, exp.tick)
		}

		q.UnlinkMin(h)
		delete(activeHandles, h)

		// Reset pool entry state - FIXED: Convert handle to pool index
		poolIndex := h - 1
		pool[poolIndex].Tick = -1
		pool[poolIndex].Prev = nilIdx
		pool[poolIndex].Next = nilIdx
		pool[poolIndex].Data = 0

		availableHandles = append(availableHandles, h)
	}

	// Final validation
	if ref.Len() != 0 {
		t.Fatalf("Reference heap not empty after drain: %d items remaining", ref.Len())
	}
	if len(availableHandles) != maxHandles {
		t.Fatalf("Handle leak detected: %d handles missing", maxHandles-len(availableHandles))
	}
	if len(activeHandles) != 0 {
		t.Fatalf("Active handle tracking inconsistent: %d handles still marked active", len(activeHandles))
	}
	if q.Size() != 0 {
		t.Fatalf("Queue size not zero after complete drain: %d", q.Size())
	}
}

// ============================================================================
// SHARED POOL STRESS TESTS
// ============================================================================

// TestSharedPoolStress validates multiple queues sharing a single pool under stress.
func TestSharedPoolStress(t *testing.T) {
	const operations = 1_000_000
	const poolSize = 10000
	const queueCount = 3

	// Shared pool for all queues
	pool := make([]Entry, poolSize)
	InitializePool(pool)

	// Create multiple queues sharing the pool
	queues := make([]*CompactQueue128, queueCount)
	for i := range queues {
		queues[i] = New(unsafe.Pointer(&pool[0]))
	}

	// Partition handle space between queues - FIXED: Start from 1
	handlesPerQueue := poolSize / queueCount

	// Track which handles are active in each queue
	activeHandles := make([]map[Handle]bool, queueCount)
	for i := range activeHandles {
		activeHandles[i] = make(map[Handle]bool)
	}

	rng := rand.New(rand.NewSource(42))

	for i := 0; i < operations; i++ {
		queueIdx := rng.Intn(queueCount)
		queue := queues[queueIdx]

		// Use handles from this queue's partition - FIXED: Start from 1
		handleBase := Handle(queueIdx*handlesPerQueue + 1)
		h := handleBase + Handle(rng.Intn(handlesPerQueue))

		tick := int64(rng.Intn(BucketCount)) // Keep within 0-127 range
		val := uint64(rng.Uint64())

		// Random operation
		switch rng.Intn(3) {
		case 0: // Push
			queue.Push(tick, h, val)
			activeHandles[queueIdx][h] = true

		case 1: // MoveTick - only move if handle is active in this queue
			if activeHandles[queueIdx][h] {
				newTick := int64(rng.Intn(BucketCount))
				queue.MoveTick(h, newTick)
			}

		case 2: // Pop (if queue not empty)
			if !queue.Empty() {
				popH, _, _ := queue.PeepMin()
				queue.UnlinkMin(popH)
				// Remove from active set - determine which queue it belonged to
				for qIdx := range activeHandles {
					delete(activeHandles[qIdx], popH)
				}
			}
		}

		// Periodic validation of queue independence
		if i%50000 == 0 {
			// Verify each queue operates independently
			for j, q := range queues {
				size := q.Size()
				empty := q.Empty()

				// Size consistency
				if (size == 0) != empty {
					t.Fatalf("Queue %d size/empty inconsistency: size=%d empty=%v",
						j, size, empty)
				}

				// If not empty, should be able to peek
				if !empty {
					_, _, _ = q.PeepMin()
				}
			}
		}
	}

	// Final cleanup and validation
	for i, queue := range queues {
		// Drain each queue completely
		for !queue.Empty() {
			h, _, _ := queue.PeepMin()
			queue.UnlinkMin(h)
		}

		// Verify completely empty
		if queue.Size() != 0 || !queue.Empty() {
			t.Errorf("Queue %d not empty after drain: size=%d empty=%v",
				i, queue.Size(), queue.Empty())
		}
	}
}

// ============================================================================
// POOL BOUNDARY STRESS TESTS
// ============================================================================

// TestPoolBoundaryStress validates handle bounds within pool capacity.
func TestPoolBoundaryStress(t *testing.T) {
	const poolSize = 10000
	const operations = 500000

	pool := make([]Entry, poolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	rng := rand.New(rand.NewSource(123))
	activeHandles := make(map[Handle]bool)

	for i := 0; i < operations; i++ {
		// FIXED: Use handles 1 to poolSize (instead of 0 to poolSize-1)
		h := Handle(rng.Intn(poolSize) + 1)
		tick := int64(rng.Intn(BucketCount)) // Keep within 0-127 range
		val := uint64(rng.Uint64())

		switch rng.Intn(4) {
		case 0: // Push
			q.Push(tick, h, val)
			activeHandles[h] = true

		case 1: // MoveTick - only move if handle is active
			if activeHandles[h] {
				newTick := int64(rng.Intn(BucketCount))
				q.MoveTick(h, newTick)
			}

		case 2: // Pop minimum
			if !q.Empty() {
				popH, _, _ := q.PeepMin()
				q.UnlinkMin(popH)
				delete(activeHandles, popH)
			}

		case 3: // Verify pool entry directly
			entry := q.entry(h)
			poolIndex := h - 1 // FIXED: Convert handle to pool index
			poolEntry := &pool[poolIndex]

			if entry != poolEntry {
				t.Fatalf("Entry access mismatch: handle=%d got=%p want=%p",
					h, entry, poolEntry)
			}
		}

		if i%25000 == 0 {
			expectedActive := 0
			for h := range activeHandles {
				poolIndex := h - 1 // FIXED: Convert handle to pool index
				if pool[poolIndex].Tick >= 0 {
					expectedActive++
				}
			}

			actualSize := int(q.Size())
			if actualSize != expectedActive {
				t.Fatalf("Size mismatch at iteration %d: queue=%d expected=%d",
					i, actualSize, expectedActive)
			}
		}
	}

	// Drain and verify
	drainCount := 0
	for !q.Empty() {
		h, _, _ := q.PeepMin()
		q.UnlinkMin(h)
		drainCount++
	}

	if drainCount != len(activeHandles) {
		t.Errorf("Drain count mismatch: drained=%d expected=%d",
			drainCount, len(activeHandles))
	}
}

// ============================================================================
// BITMAP CONSISTENCY STRESS TESTS
// ============================================================================

// TestBitmapConsistencyUnderStress validates bitmap integrity under intensive load.
func TestBitmapConsistencyUnderStress(t *testing.T) {
	const poolSize = 25000 // Reduced for smaller queue
	const operations = 1_000_000

	pool := make([]Entry, poolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	rng := rand.New(rand.NewSource(456))
	handleTracker := make(map[Handle]int64) // Handle -> tick mapping

	for i := 0; i < operations; i++ {
		// FIXED: Use handles 1 to poolSize (instead of 0 to poolSize-1)
		h := Handle(rng.Intn(poolSize) + 1)
		tick := int64(rng.Intn(BucketCount)) // Keep within 0-127 range
		val := uint64(rng.Uint64())

		switch rng.Intn(3) {
		case 0: // Push
			q.Push(tick, h, val)
			handleTracker[h] = tick

		case 1: // MoveTick - only move if handle is tracked (active)
			if _, exists := handleTracker[h]; exists {
				newTick := int64(rng.Intn(BucketCount))
				q.MoveTick(h, newTick)
				handleTracker[h] = newTick
			}

		case 2: // Pop
			if !q.Empty() {
				popH, popTick, _ := q.PeepMin()
				q.UnlinkMin(popH)

				// Verify popped handle had correct tick
				if expectedTick, exists := handleTracker[popH]; exists {
					if expectedTick != popTick {
						t.Fatalf("Popped tick mismatch: handle=%d got=%d want=%d",
							popH, popTick, expectedTick)
					}
				}
				delete(handleTracker, popH)

				// Reset pool entry state - FIXED: Convert handle to pool index
				poolIndex := popH - 1
				pool[poolIndex].Tick = -1
				pool[poolIndex].Prev = nilIdx
				pool[poolIndex].Next = nilIdx
				pool[poolIndex].Data = 0
			}
		}

		// Intensive bitmap validation every 50k operations (more frequent due to smaller range)
		if i%50000 == 0 && !q.Empty() {
			h, tick, _ := q.PeepMin()

			// Find actual minimum from handle tracker
			actualMinTick := int64(BucketCount)
			for _, trackedTick := range handleTracker {
				if trackedTick < actualMinTick {
					actualMinTick = trackedTick
				}
			}

			if tick != actualMinTick {
				t.Fatalf("Bitmap minimum tick inconsistent at iteration %d: bitmap=%d actual=%d",
					i, tick, actualMinTick)
			}

			// Verify the returned handle corresponds to an entry with the minimum tick
			if handleTick, exists := handleTracker[h]; !exists || handleTick != actualMinTick {
				t.Fatalf("Handle validation failed at iteration %d: handle=%d tick=%d, expected tick=%d",
					i, h, handleTick, actualMinTick)
			}

			// Verify bitmap summary consistency
			validateBitmapSummaries(t, q, handleTracker)
		}
	}
}

// validateBitmapSummaries performs comprehensive bitmap consistency validation.
func validateBitmapSummaries(t *testing.T, q *CompactQueue128, handleTracker map[Handle]int64) {
	// Build expected bitmap state from handle tracker
	expectedGroups := make(map[uint64]bool)
	expectedLanes := make(map[uint64]map[uint64]bool)
	expectedBuckets := make(map[uint64]bool)

	for _, tick := range handleTracker {
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

	// Validate global summary (only check group 0 since ticks 0-127 only use group 0)
	for g := uint64(0); g < 1; g++ { // Only validate group 0
		expectedActive := expectedGroups[g]
		actualActive := (q.summary & (1 << (63 - g))) != 0

		if expectedActive != actualActive {
			t.Fatalf("Group %d summary mismatch: expected=%v actual=%v",
				g, expectedActive, actualActive)
		}

		// Validate group-level summaries (only check lanes 0-1 since ticks 0-127 only use lanes 0-1)
		if expectedActive {
			gb := &q.groups[g]
			for l := uint64(0); l < 2; l++ { // Only validate lanes 0 and 1
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
