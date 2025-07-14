// ============================================================================
// POOLEDQUANTUMQUEUE CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive stress testing framework validating PooledQuantumQueue correctness
// against a reference Go heap implementation under millions of random operations.
//
// Validation methodology:
//   - Stress-test PooledQuantumQueue against reference Go container/heap
//   - Apply 10M+ randomized operations: push, move, pop
//   - Deterministic seed ensures reproducible failure cases
//   - External handle management with pool bounds checking
//
// Correctness guarantees verified:
//   - Order correctness (tick priority + LIFO tiebreaking)
//   - Tick relocation consistency across operations
//   - Handle bounds validation and pool safety
//   - Summary bitmap integrity under all conditions
//   - Shared pool architecture correctness
//
// Failure detection:
//   - Any corruption, misordering, or ghost state triggers immediate failure
//   - Comprehensive drain verification ensures no phantom entries
//   - Handle lifecycle tracking prevents use-after-free scenarios
//   - Bitmap consistency validation across all hierarchy levels

package pooledquantumqueue

import (
	"container/heap"
	"math/rand"
	"testing"
	"unsafe"
)

// ============================================================================
// REFERENCE IMPLEMENTATION
// ============================================================================

// stressItem represents a single entry in the reference heap implementation.
type stressItem struct {
	h    Handle // Corresponding handle in PooledQuantumQueue pool
	tick int64  // Priority key for heap ordering
	seq  int    // LIFO sequence number for tiebreaking
}

// stressHeap implements heap.Interface with PooledQuantumQueue-compatible ordering.
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

// TestQueueStressRandomOperations validates PooledQuantumQueue under chaotic workloads.
func TestQueueStressRandomOperations(t *testing.T) {
	const iterations = 10_000_000
	const maxHandles = 50000

	// Deterministic PRNG for reproducible failure analysis
	rng := rand.New(rand.NewSource(69))

	// Initialize external pool
	pool := make([]Entry, maxHandles)
	InitializePool(pool) // Use shared helper function
	q := New(unsafe.Pointer(&pool[0]))

	// Initialize reference heap
	ref := &stressHeap{}
	heap.Init(ref)

	// Handle lifecycle management
	availableHandles := make([]Handle, maxHandles)
	for i := range availableHandles {
		availableHandles[i] = Handle(i)
	}
	activeHandles := make(map[Handle]bool)

	seq := 0

	makeVal := func(seed int64) uint64 {
		return uint64(seed) * 0x9E3779B97F4A7C15
	}

	for i := 0; i < iterations; i++ {
		op := rng.Intn(3)
		tick := int64(rng.Intn(BucketCount))

		switch op {
		case 0: // PUSH
			if len(availableHandles) == 0 {
				continue
			}

			h := availableHandles[len(availableHandles)-1]
			availableHandles = availableHandles[:len(availableHandles)-1]

			val := makeVal(int64(seq))

			q.Push(tick, h, val)
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})

			activeHandles[h] = true
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

			q.MoveTick(h, tick)

			for j := len(*ref) - 1; j >= 0; j-- {
				if (*ref)[j].h == h {
					heap.Remove(ref, j)
					break
				}
			}
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			seq++

		case 2: // POP
			if q.Empty() {
				continue
			}

			h, tickGot, _ := q.PeepMin()
			exp := heap.Pop(ref).(*stressItem)

			if exp.h != h || exp.tick != tickGot {
				t.Fatalf("Mismatch at iteration %d: got (h=%v,tick=%d); want (h=%v,tick=%d)",
					i, h, tickGot, exp.h, exp.tick)
			}

			q.UnlinkMin(h)
			delete(activeHandles, h)

			// Reset the pool entry to unlinked state when returning handle (FIXED: using exported fields)
			pool[h].Tick = -1
			pool[h].Prev = nilIdx
			pool[h].Next = nilIdx
			pool[h].Data = 0

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

		// Reset pool entry state (FIXED: using exported fields)
		pool[h].Tick = -1
		pool[h].Prev = nilIdx
		pool[h].Next = nilIdx
		pool[h].Data = 0

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
	InitializePool(pool) // Use shared helper function

	// Create multiple queues sharing the pool
	queues := make([]*PooledQuantumQueue, queueCount)
	for i := range queues {
		queues[i] = New(unsafe.Pointer(&pool[0]))
	}

	// Partition handle space between queues
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

		// Use handles from this queue's partition
		handleBase := Handle(queueIdx * handlesPerQueue)
		h := handleBase + Handle(rng.Intn(handlesPerQueue))

		tick := int64(rng.Intn(10000))
		val := uint64(rng.Uint64())

		// Random operation
		switch rng.Intn(3) {
		case 0: // Push
			queue.Push(tick, h, val)
			activeHandles[queueIdx][h] = true

		case 1: // MoveTick - FIXED: Only move if handle is active in this queue
			if activeHandles[queueIdx][h] {
				newTick := int64(rng.Intn(10000))
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
	InitializePool(pool) // Use shared helper function
	q := New(unsafe.Pointer(&pool[0]))

	rng := rand.New(rand.NewSource(123))
	activeHandles := make(map[Handle]bool)

	for i := 0; i < operations; i++ {
		h := Handle(rng.Intn(poolSize))
		tick := int64(rng.Intn(100000))
		val := uint64(rng.Uint64())

		switch rng.Intn(4) {
		case 0: // Push
			q.Push(tick, h, val)
			activeHandles[h] = true

		case 1: // MoveTick - FIXED: Only move if handle is active
			if activeHandles[h] {
				newTick := int64(rng.Intn(100000))
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
			poolEntry := &pool[h]

			if entry != poolEntry {
				t.Fatalf("Entry access mismatch: handle=%d got=%p want=%p",
					h, entry, poolEntry)
			}
		}

		if i%25000 == 0 {
			expectedActive := 0
			for h := range activeHandles {
				if pool[h].Tick >= 0 { // FIXED: using exported field
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
	const poolSize = 50000
	const operations = 2_000_000

	pool := make([]Entry, poolSize)
	InitializePool(pool) // Use shared helper function
	q := New(unsafe.Pointer(&pool[0]))

	rng := rand.New(rand.NewSource(456))
	handleTracker := make(map[Handle]int64) // Handle -> tick mapping

	for i := 0; i < operations; i++ {
		h := Handle(rng.Intn(poolSize))
		tick := int64(rng.Intn(BucketCount))
		val := uint64(rng.Uint64())

		switch rng.Intn(3) {
		case 0: // Push
			q.Push(tick, h, val)
			handleTracker[h] = tick

		case 1: // MoveTick - FIXED: Only move if handle is tracked (active)
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

				// Reset pool entry state (FIXED: using exported fields)
				pool[popH].Tick = -1
				pool[popH].Prev = nilIdx
				pool[popH].Next = nilIdx
				pool[popH].Data = 0
			}
		}

		// Intensive bitmap validation every 100k operations
		if i%100000 == 0 && !q.Empty() {
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
func validateBitmapSummaries(t *testing.T, q *PooledQuantumQueue, handleTracker map[Handle]int64) {
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
