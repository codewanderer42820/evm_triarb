// ============================================================================
// QUANTUMQUEUE MICROBENCHMARK SUITE
// ============================================================================
//
// Comprehensive performance measurement suite for QuantumQueue core operations.
// Validates sub-8ns operation latency under realistic ISR workload patterns.
//
// ORIGINAL VERSION: Uses 48-byte data blocks instead of uint64 payloads.
//
// Benchmark methodology:
//   - All benchmarks use pre-filled arenas (CapItems) for stress realism
//   - Edge tick usage (0, max) included for performance consistency validation
//   - Hot/cold path distinction models real-world ISR usage patterns
//   - Bursty update patterns simulate interrupt coalescing scenarios
//
// Performance patterns tested:
//   - Hot path: Same tick repeated updates (cache-friendly)
//   - Cold path: Random/spread tick patterns (cache-hostile)
//   - Summary collapse: Bitmap maintenance under sparse loads
//   - Arena exhaustion: Full capacity stress testing
//
// Expected results:
//   - Push operations: 2-8ns depending on cache locality
//   - PeepMin operations: 3-6ns via bitmap hierarchy traversal
//   - UnlinkMin operations: 4-10ns depending on summary updates
//   - MoveTick operations: 6-15ns for unlink/relink cycles

package quantumqueue

import (
	"math/rand"
	"testing"
)

// ============================================================================
// BENCHMARK CONFIGURATION
// ============================================================================

const benchSize = CapItems // Use full arena capacity for realistic stress testing

// Helper function to create benchmark data
func makeBenchData(seed uint64) *[48]byte {
	data := &[48]byte{}
	// Fill with pattern based on seed
	for i := 0; i < 6; i++ {
		val := seed + uint64(i)
		data[i*8] = byte(val)
		data[i*8+1] = byte(val >> 8)
		data[i*8+2] = byte(val >> 16)
		data[i*8+3] = byte(val >> 24)
		data[i*8+4] = byte(val >> 32)
		data[i*8+5] = byte(val >> 40)
		data[i*8+6] = byte(val >> 48)
		data[i*8+7] = byte(val >> 56)
	}
	return data
}

// ============================================================================
// METADATA ACCESS BENCHMARKS
// ============================================================================

// BenchmarkEmpty measures the cost of queue emptiness checking.
// Expected performance: <1ns (single field load from hot cache line).
//
// Operation tested: q.Empty()
// Typical use case: ISR guard condition before queue processing
func BenchmarkEmpty(b *testing.B) {
	q := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Empty()
	}
}

// BenchmarkSize measures the cost of queue size retrieval.
// Expected performance: <1ns (single field load from hot cache line).
//
// Operation tested: q.Size()
// Typical use case: Load balancing decisions in multi-queue systems
func BenchmarkSize(b *testing.B) {
	q := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Size()
	}
}

// ============================================================================
// HANDLE ALLOCATION BENCHMARKS
// ============================================================================

// BenchmarkBorrow measures unchecked handle allocation performance.
// Expected performance: 1-3ns (freelist pop with minimal validation).
//
// ⚠️  FOOTGUN GRADE 8/10: No exhaustion checking
// Operation tested: q.Borrow()
// Typical use case: High-frequency allocation in ISR contexts
// DANGER: Undefined behavior when freelist exhausted
func BenchmarkBorrow(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)

	// Pre-allocate to avoid exhaustion during benchmark
	for i := range handles {
		h, _ := q.Borrow()
		handles[i] = h
	}

	// Return all handles to reset freelist
	for _, h := range handles {
		q.arena[h].next = q.freeHead
		q.freeHead = h
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		// Return immediately to maintain freelist
		q.arena[h].next = q.freeHead
		q.freeHead = h
	}
}

// BenchmarkBorrowSafe measures checked handle allocation performance.
// Expected performance: 1-4ns (includes exhaustion validation).
//
// Operation tested: q.BorrowSafe()
// Typical use case: Safe allocation with error handling
func BenchmarkBorrowSafe(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)

	// Pre-allocate to avoid exhaustion during benchmark
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}

	// Return all handles to reset freelist
	for _, h := range handles {
		q.arena[h].next = q.freeHead
		q.freeHead = h
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.BorrowSafe()
		// Return immediately to maintain freelist
		q.arena[h].next = q.freeHead
		q.freeHead = h
	}
}

// ============================================================================
// PUSH OPERATION BENCHMARKS
// ============================================================================

// BenchmarkPushUnique measures cold path performance for new tick insertions.
// Each operation targets a unique tick, maximizing cache misses and bitmap updates.
//
// Performance characteristics:
//   - New bucket creation per operation
//   - Complete bitmap hierarchy updates
//   - Maximum memory access scatter
//   - Expected latency: 8-15ns per operation
func BenchmarkPushUnique(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	data := makeBenchData(0x12345678) // Pre-allocate data outside timing loop
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, data)
	}
}

// BenchmarkPushUpdate measures hot path performance for tick updates.
// All operations target existing ticks, minimizing structural changes.
//
// Performance characteristics:
//   - In-place payload updates only
//   - No bitmap summary modifications
//   - Optimal cache locality with 64-byte nodes
//   - Expected latency: 2-5ns per operation
func BenchmarkPushUpdate(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	initData := makeBenchData(0x11111111)
	updateData := makeBenchData(0x22222222)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, initData)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, updateData)
	}
}

// BenchmarkPushSameTickZero measures performance for edge case tick value (0).
// Validates consistent performance across the tick range boundary conditions.
//
// ⚠️  FOOTGUN NOTE: No bounds validation on tick values
// Edge case validation:
//   - Minimum tick value handling
//   - Bitmap index computation accuracy
//   - Expected latency: 3-6ns per operation
func BenchmarkPushSameTickZero(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	data := makeBenchData(0x33333333)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(0, h, data)
	}
}

// BenchmarkPushSameTickMax measures performance for maximum tick value.
// Validates bitmap hierarchy performance at upper boundary conditions.
//
// Edge case validation:
//   - Maximum tick value handling
//   - High-order bitmap group performance
//   - Expected latency: 3-6ns per operation
func BenchmarkPushSameTickMax(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	maxTick := int64(BucketCount - 1)
	data := makeBenchData(0x44444444)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(maxTick, h, data)
	}
}

// BenchmarkPushRandom measures worst-case performance under chaotic tick patterns.
// Simulates completely random ISR arrival patterns with maximum cache hostility.
//
// Stress test characteristics:
//   - Uniformly random tick distribution
//   - Maximum bitmap summary churn
//   - Worst-case memory access patterns
//   - Expected latency: 10-20ns per operation
func BenchmarkPushRandom(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	data := makeBenchData(0x55555555)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}

	// Pre-generate deterministic random sequence for reproducibility
	rand.Seed(42) // Fixed seed for consistent results
	ticks := make([]int64, benchSize)
	for i := range ticks {
		ticks[i] = rand.Int63n(BucketCount)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(ticks[i%benchSize], h, data)
	}
}

// BenchmarkPushBursty measures interrupt coalescing patterns.
// Simulates real ISR behavior where updates cluster around recent ticks.
//
// Burst characteristics:
//   - 80% updates within 16-tick window
//   - 20% random scatter for cache pressure
//   - Models temporal locality in real systems
//   - Expected latency: 4-10ns per operation
func BenchmarkPushBursty(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	data := makeBenchData(0x66666666)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}

	rand.Seed(42)
	baseTick := int64(1000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		var tick int64

		if rand.Float32() < 0.8 {
			// 80% within burst window
			tick = baseTick + rand.Int63n(16)
		} else {
			// 20% random scatter
			tick = rand.Int63n(BucketCount)
		}

		// Ensure tick is within bounds
		if tick >= BucketCount {
			tick = BucketCount - 1
		}

		q.Push(tick, h, data)

		// Shift burst window occasionally
		if i%1000 == 0 {
			baseTick = (baseTick + 100) % (BucketCount - 100)
		}
	}
}

// ============================================================================
// MINIMUM EXTRACTION BENCHMARKS
// ============================================================================

// BenchmarkPeepMin measures minimum finding performance via bitmap hierarchy.
// Tests O(1) minimum extraction using CLZ-based bitmap traversal.
//
// ⚠️  FOOTGUN GRADE 10/10: Undefined behavior on empty queue
// Algorithm performance:
//   - 3-level bitmap hierarchy traversal
//   - CLZ instruction utilization
//   - Cache-optimized data structure access
//   - Expected latency: 4-8ns per operation
//   - DANGER: Crashes or corrupts on empty queue
func BenchmarkPeepMin(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		data := makeBenchData(uint64(i))
		q.Push(int64(i), h, data)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.PeepMin()
	}
}

// BenchmarkPeepMinSparse measures minimum finding in sparse queues.
// Tests performance when only few buckets are populated.
//
// Sparse characteristics:
//   - Only 1% of buckets populated
//   - Large gaps between active ticks
//   - Tests bitmap efficiency under sparsity
//   - Expected latency: 3-6ns per operation
func BenchmarkPeepMinSparse(b *testing.B) {
	q := New()
	handles := make([]Handle, 100) // Only 100 handles for sparsity
	data := makeBenchData(0xAAAA)

	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		// Spread entries across tick space for sparsity
		tick := int64(i * (BucketCount / 100))
		q.Push(tick, h, data)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.PeepMin()
	}
}

// BenchmarkPeepMinDense measures minimum finding in dense queues.
// Tests performance when most buckets are populated.
//
// Dense characteristics:
//   - 90%+ bucket utilization
//   - High bitmap population
//   - Stress tests bitmap traversal
//   - Expected latency: 4-8ns per operation
func BenchmarkPeepMinDense(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	data := makeBenchData(0xBBBB)

	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		// Pack entries densely
		tick := int64(i % (BucketCount * 9 / 10))
		q.Push(tick, h, data)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.PeepMin()
	}
}

// ============================================================================
// UNLINK OPERATION BENCHMARKS
// ============================================================================

// BenchmarkUnlinkMin_StableBucket measures unlink performance without summary updates.
// Tests pure linked list manipulation without bitmap hierarchy changes.
//
// Performance isolation:
//   - No bitmap summary modifications
//   - Pure doubly-linked list operations
//   - Minimal cache line access with 64-byte nodes
//   - Expected latency: 3-6ns per operation
func BenchmarkUnlinkMin_StableBucket(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	data := makeBenchData(0x1234)
	q.Push(2048, h, data)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.UnlinkMin(h)
		q.Push(2048, h, data)
	}
}

// BenchmarkUnlinkMin_DenseBucket measures unlink performance in populated buckets.
// Tests linked list manipulation with multiple entries per bucket.
//
// Multi-entry scenarios:
//   - 3 handles sharing single bucket
//   - Chain traversal and pointer updates
//   - No summary collapse triggers
//   - Expected latency: 4-7ns per operation
func BenchmarkUnlinkMin_DenseBucket(b *testing.B) {
	q := New()
	var hs [3]Handle
	data := makeBenchData(0x7777)
	for i := 0; i < 3; i++ {
		hs[i], _ = q.BorrowSafe()
		q.Push(1234, hs[i], data)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := hs[i%3]
		q.UnlinkMin(h)
		q.Push(1234, h, data)
	}
}

// BenchmarkUnlinkMin_BitmapCollapse measures summary update overhead.
// Each unlink operation triggers complete bitmap hierarchy collapse.
//
// Summary maintenance stress:
//   - Every operation empties a bucket
//   - Complete bitmap hierarchy updates
//   - Maximum summary maintenance overhead
//   - Expected latency: 8-15ns per operation
func BenchmarkUnlinkMin_BitmapCollapse(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	data := makeBenchData(0x8888)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tick := int64(i % BucketCount)
		q.Push(tick, h, data)
		q.UnlinkMin(h)
	}
}

// BenchmarkUnlinkMin_ScatterCollapse measures random collapse performance.
// Combines random tick distribution with frequent summary collapses.
//
// Chaotic collapse patterns:
//   - Fully random tick selection
//   - Unpredictable summary update patterns
//   - Maximum bitmap churn
//   - Expected latency: 10-20ns per operation
func BenchmarkUnlinkMin_ScatterCollapse(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	data := makeBenchData(0x9999)

	// Pre-generate random tick sequence for reproducibility
	rand.Seed(42)
	ticks := make([]int64, b.N)
	for i := range ticks {
		ticks[i] = int64(rand.Intn(BucketCount))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tick := ticks[i]
		q.Push(tick, h, data)
		q.UnlinkMin(h)
	}
}

// BenchmarkUnlinkMin_ReinsertAfterCollapse measures collapse/refill cycle performance.
// Tests repeated bucket empty/fill patterns at fixed tick position.
//
// Cycle performance measurement:
//   - Deterministic collapse/refill pattern
//   - Fixed tick for cache optimization
//   - Summary update cycle overhead
//   - Expected latency: 6-12ns per operation
func BenchmarkUnlinkMin_ReinsertAfterCollapse(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	const tick = 4095
	data := makeBenchData(0xAAAA)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(tick, h, data)
		q.UnlinkMin(h)
	}
}

// BenchmarkUnlinkMin_ChainTraversal measures performance with long chains.
// Tests linked list traversal when multiple entries share the same bucket.
//
// Chain stress characteristics:
//   - 10 entries per bucket
//   - Random position removals
//   - Chain pointer maintenance
//   - Expected latency: 5-12ns per operation
func BenchmarkUnlinkMin_ChainTraversal(b *testing.B) {
	q := New()
	const chainLength = 10
	handles := make([]Handle, chainLength)
	data := makeBenchData(0xBBBB)

	// Setup chain
	for i := 0; i < chainLength; i++ {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(1000, h, data)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%chainLength]
		q.UnlinkMin(h)
		q.Push(1000, h, data)
	}
}

// ============================================================================
// TICK MOVEMENT BENCHMARKS
// ============================================================================

// BenchmarkMoveTick measures tick relocation performance.
// Tests combined unlink/relink operations for entry repositioning.
//
// Movement operation characteristics:
//   - Atomic unlink from current position
//   - Relink at new tick position
//   - Double bitmap summary updates
//   - Expected latency: 8-18ns per operation
func BenchmarkMoveTick(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		data := makeBenchData(uint64(i))
		q.Push(int64(i), h, data)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.MoveTick(h, int64((i+1)%benchSize))
	}
}

// BenchmarkMoveTickNoop measures no-op movement optimization.
// Tests performance when moving to the same tick (should be optimized).
//
// No-op characteristics:
//   - Same source and destination tick
//   - Early return optimization
//   - Minimal overhead validation
//   - Expected latency: 1-3ns per operation
func BenchmarkMoveTickNoop(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		data := makeBenchData(uint64(i))
		q.Push(int64(i), h, data)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		currentTick := int64(i % benchSize)
		q.MoveTick(h, currentTick) // Same tick - should be no-op
	}
}

// BenchmarkMoveTickRandom measures worst-case movement performance.
// Tests random tick relocation with maximum cache hostility.
//
// Random movement characteristics:
//   - Completely random source/destination pairs
//   - Maximum bitmap churn
//   - Worst-case memory access patterns
//   - Expected latency: 12-25ns per operation
func BenchmarkMoveTickRandom(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		data := makeBenchData(uint64(i))
		q.Push(int64(i), h, data)
	}

	// Pre-generate random destinations
	rand.Seed(42)
	destinations := make([]int64, benchSize)
	for i := range destinations {
		destinations[i] = rand.Int63n(BucketCount)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.MoveTick(h, destinations[i%benchSize])
	}
}

// ============================================================================
// MIXED WORKLOAD BENCHMARKS
// ============================================================================

// BenchmarkMixedOperations measures realistic workload performance.
// Combines all operations in patterns typical of ISR usage.
//
// ⚠️  FOOTGUN AWARENESS: Assumes correct operation sequencing
// Mixed workload characteristics:
//   - 40% Push operations (new entries)
//   - 30% PeepMin operations (scheduling queries)
//   - 20% UnlinkMin operations (task completion)
//   - 10% MoveTick operations (priority updates)
//   - Expected latency: 5-15ns per operation
//   - DANGER: No validation of operation preconditions
func BenchmarkMixedOperations(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	data := makeBenchData(0xCCCC)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}

	// Initialize some entries
	for i := 0; i < benchSize/2; i++ {
		h := handles[i]
		q.Push(int64(i), h, data)
	}

	rand.Seed(42)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		op := rand.Float32()

		switch {
		case op < 0.4: // 40% Push
			h := handles[i%benchSize]
			tick := rand.Int63n(BucketCount)
			q.Push(tick, h, data)

		case op < 0.7: // 30% PeepMin
			if !q.Empty() {
				q.PeepMin()
			}

		case op < 0.9: // 20% UnlinkMin
			if !q.Empty() {
				h, _, _ := q.PeepMin()
				q.UnlinkMin(h)
			}

		default: // 10% MoveTick
			if !q.Empty() {
				h, _, _ := q.PeepMin()
				newTick := rand.Int63n(BucketCount)
				q.MoveTick(h, newTick)
			}
		}
	}
}
