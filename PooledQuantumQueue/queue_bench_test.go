// ============================================================================
// POOLEDQUANTUMQUEUE MICROBENCHMARK SUITE
// ============================================================================
//
// Comprehensive performance measurement suite for PooledQuantumQueue core operations.
// Validates sub-8ns operation latency under realistic shared pool workload patterns.
//
// Benchmark methodology:
//   - Shared memory pools pre-allocated for stress realism
//   - Edge tick usage (0, max) included for performance consistency validation
//   - Hot/cold path distinction models real-world usage patterns
//   - Bursty update patterns simulate coalesced operations
//   - Pre-computed data values eliminate timing loop overhead
//   - External handle management simulates production usage
//
// Performance patterns tested:
//   - Hot path: Same tick repeated updates (cache-friendly)
//   - Cold path: Random/spread tick patterns (cache-hostile)
//   - Summary collapse: Bitmap maintenance under sparse loads
//   - Pool utilization: Shared pool access patterns
//
// Expected results:
//   - Push operations: 2-8ns depending on cache locality
//   - PeepMin operations: 3-6ns via bitmap hierarchy traversal
//   - UnlinkMin operations: 4-10ns depending on summary updates
//   - MoveTick operations: 6-15ns for unlink/relink cycles

package pooledquantumqueue

import (
	"math/rand"
	"testing"
	"unsafe"
)

// ============================================================================
// BENCHMARK CONFIGURATION
// ============================================================================

const (
	benchPoolSize = 100000 // Shared pool size for benchmarks
	benchHandles  = 10000  // Number of handles to use in tests
)

// ============================================================================
// METADATA ACCESS BENCHMARKS
// ============================================================================

// BenchmarkEmpty measures the cost of queue emptiness checking.
// Expected performance: <1ns (single field load from hot cache line).
//
// Operation tested: q.Empty()
// Typical use case: Guard condition before queue processing
func BenchmarkEmpty(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	var result bool // FIXED: Capture result to prevent optimization
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = q.Empty() // FIXED: Use the result
	}
	_ = result // FIXED: Prevent compiler from optimizing away the call
}

// BenchmarkSize measures the cost of queue size retrieval.
// Expected performance: <1ns (single field load from hot cache line).
//
// Operation tested: q.Size()
// Typical use case: Load balancing decisions in multi-queue systems
func BenchmarkSize(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	var result int // FIXED: Capture result to prevent optimization
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = q.Size() // FIXED: Use the result
	}
	_ = result // FIXED: Prevent compiler from optimizing away the call
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	const testValue = uint64(0x123456789ABCDEF0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		q.Push(int64(i%benchHandles), h, testValue)
	}
}

// BenchmarkPushUpdate measures hot path performance for tick updates.
// All operations target existing ticks, minimizing structural changes.
//
// Performance characteristics:
//   - In-place payload updates only
//   - No bitmap summary modifications
//   - Optimal cache locality
//   - Expected latency: 2-5ns per operation
func BenchmarkPushUpdate(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	const initValue = uint64(0x1111111111111111)
	const updateValue = uint64(0x2222222222222222)

	// Pre-populate with initial values
	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		q.Push(int64(i), h, initValue)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		q.Push(int64(i%benchHandles), h, updateValue)
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	const testValue = uint64(0x3333333333333333)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(0, h, testValue)
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	maxTick := int64(BucketCount - 1)
	const testValue = uint64(0x4444444444444444)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(maxTick, h, testValue)
	}
}

// BenchmarkPushRandom measures worst-case performance under chaotic tick patterns.
// Simulates completely random access patterns with maximum cache hostility.
//
// Stress test characteristics:
//   - Uniformly random tick distribution
//   - Maximum bitmap summary churn
//   - Worst-case memory access patterns
//   - Expected latency: 10-20ns per operation
func BenchmarkPushRandom(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	const testValue = uint64(0x5555555555555555)

	// Pre-generate deterministic random sequence for reproducibility
	rand.Seed(42) // Fixed seed for consistent results
	ticks := make([]int64, benchHandles)
	for i := range ticks {
		ticks[i] = rand.Int63n(BucketCount)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		q.Push(ticks[i%benchHandles], h, testValue)
	}
}

// BenchmarkPushBursty measures coalesced operation patterns.
// Simulates real usage where updates cluster around recent ticks.
//
// Burst characteristics:
//   - 80% updates within 16-tick window
//   - 20% random scatter for cache pressure
//   - Models temporal locality in real systems
//   - Expected latency: 4-10ns per operation
func BenchmarkPushBursty(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	const testValue = uint64(0x6666666666666666)

	rand.Seed(42)
	baseTick := int64(1000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
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

		q.Push(tick, h, testValue)

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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))

	// Pre-populate queue
	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		q.Push(int64(i), h, uint64(i))
	}

	var h Handle    // FIXED: Capture all results
	var tick int64  // FIXED: Capture all results
	var data uint64 // FIXED: Capture all results
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h, tick, data = q.PeepMin() // FIXED: Use all return values
	}
	_, _, _ = h, tick, data // FIXED: Prevent optimization
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	const sparseCount = 100 // Only 100 entries for sparsity

	for i := 0; i < sparseCount; i++ {
		h := Handle(i)
		// Spread entries across tick space for sparsity
		tick := int64(i * (BucketCount / sparseCount))
		q.Push(tick, h, uint64(i))
	}

	var h Handle    // FIXED: Capture all results
	var tick int64  // FIXED: Capture all results
	var data uint64 // FIXED: Capture all results
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h, tick, data = q.PeepMin() // FIXED: Use all return values
	}
	_, _, _ = h, tick, data // FIXED: Prevent optimization
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))

	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		// Pack entries densely
		tick := int64(i % (BucketCount * 9 / 10))
		q.Push(tick, h, uint64(i))
	}

	var h Handle    // FIXED: Capture all results
	var tick int64  // FIXED: Capture all results
	var data uint64 // FIXED: Capture all results
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h, tick, data = q.PeepMin() // FIXED: Use all return values
	}
	_, _, _ = h, tick, data // FIXED: Prevent optimization
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
//   - Minimal cache line access
//   - Expected latency: 3-6ns per operation
func BenchmarkUnlinkMin_StableBucket(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	const testValue = uint64(0x1234567890ABCDEF)
	q.Push(2048, h, testValue)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.UnlinkMin(h)
		q.Push(2048, h, testValue)
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	hs := [3]Handle{0, 1, 2}
	const testValue = uint64(0x7777777777777777)

	for i := 0; i < 3; i++ {
		q.Push(1234, hs[i], testValue)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := hs[i%3]
		q.UnlinkMin(h)
		q.Push(1234, h, testValue)
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	const testValue = uint64(0x8888888888888888)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tick := int64(i % BucketCount)
		q.Push(tick, h, testValue)
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	const testValue = uint64(0x9999999999999999)

	// Pre-generate random tick sequence for reproducibility
	rand.Seed(42)
	ticks := make([]int64, b.N)
	for i := range ticks {
		ticks[i] = int64(rand.Intn(BucketCount))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tick := ticks[i]
		q.Push(tick, h, testValue)
		q.UnlinkMin(h)
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))

	// Pre-populate with handles
	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		q.Push(int64(i), h, uint64(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		q.MoveTick(h, int64((i+1)%benchHandles))
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))

	// Pre-populate with handles
	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		q.Push(int64(i), h, uint64(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		currentTick := int64(i % benchHandles)
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
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))

	// Pre-populate with handles
	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		q.Push(int64(i), h, uint64(i))
	}

	// Pre-generate random destinations
	rand.Seed(42)
	destinations := make([]int64, benchHandles)
	for i := range destinations {
		destinations[i] = rand.Int63n(BucketCount)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		q.MoveTick(h, destinations[i%benchHandles])
	}
}

// ============================================================================
// SHARED POOL BENCHMARKS
// ============================================================================

// BenchmarkSharedPoolMultiQueue measures performance with multiple queues sharing a pool.
// Tests the core shared pool architecture benefit under realistic usage.
//
// Shared pool characteristics:
//   - 3 queues sharing single memory pool
//   - Independent handle ranges per queue
//   - Concurrent operations on shared memory
//   - Cache locality benefits measurement
//   - Expected latency: 3-8ns per operation
func BenchmarkSharedPoolMultiQueue(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	q1 := New(unsafe.Pointer(&pool[0]))
	q2 := New(unsafe.Pointer(&pool[0]))
	q3 := New(unsafe.Pointer(&pool[0]))

	const testValue = uint64(0xAAAAAAAAAAAAAAAA)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Round-robin across queues with different handle ranges
		switch i % 3 {
		case 0:
			h := Handle(i%1000 + 1000) // Range 1000-1999
			q1.Push(int64(i%1000), h, testValue)
		case 1:
			h := Handle(i%1000 + 2000) // Range 2000-2999
			q2.Push(int64(i%1000), h, testValue)
		case 2:
			h := Handle(i%1000 + 3000) // Range 3000-3999
			q3.Push(int64(i%1000), h, testValue)
		}
	}
}

// BenchmarkPoolUtilization measures memory pool utilization efficiency.
// Tests how effectively the shared pool memory is utilized across operations.
//
// Pool utilization characteristics:
//   - Handle distribution across pool space
//   - Memory access pattern measurement
//   - Cache line utilization analysis
//   - Expected latency: 2-6ns per operation
func BenchmarkPoolUtilization(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))

	const testValue = uint64(0xBBBBBBBBBBBBBBBB)

	// Use widely distributed handles across pool
	handles := make([]Handle, 1000)
	for i := range handles {
		handles[i] = Handle(i * (benchPoolSize / 1000))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%len(handles)]
		q.Push(int64(i%1000), h, testValue)
	}
}

// ============================================================================
// MIXED WORKLOAD BENCHMARKS
// ============================================================================

// BenchmarkMixedOperations measures realistic workload performance.
// Combines all operations in patterns typical of production usage.
//
// ⚠️  FOOTGUN AWARENESS: Assumes correct operation sequencing
// Mixed workload characteristics:
//   - 40% Push operations (new entries)
//   - 30% PeepMin operations (priority queries)
//   - 20% UnlinkMin operations (removal)
//   - 10% MoveTick operations (priority updates)
//   - Expected latency: 6-15ns per operation
//   - DANGER: No validation of operation preconditions
func BenchmarkMixedOperations(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	const testValue = uint64(0xCCCCCCCCCCCCCCCC)

	// Initialize some entries
	for i := 0; i < benchHandles/2; i++ {
		h := Handle(i)
		q.Push(int64(i), h, uint64(i))
	}

	rand.Seed(42)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		op := rand.Float32()

		switch {
		case op < 0.4: // 40% Push
			h := Handle(i % benchHandles)
			tick := rand.Int63n(BucketCount)
			q.Push(tick, h, testValue)

		case op < 0.7: // 30% PeepMin
			if !q.Empty() {
				_, _, _ = q.PeepMin() // FIXED: Use return values to prevent optimization
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

// BenchmarkHighContention measures performance under high contention scenarios.
// Tests behavior when multiple operations target the same buckets/cache lines.
//
// Contention characteristics:
//   - All operations within narrow tick range
//   - Maximum cache line sharing
//   - Bitmap update contention simulation
//   - Expected latency: 4-12ns per operation
func BenchmarkHighContention(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	q := New(unsafe.Pointer(&pool[0]))
	const testValue = uint64(0xDDDDDDDDDDDDDDDD)

	// All operations within narrow range for maximum contention
	const contentionRange = 64

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		tick := int64(i % contentionRange) // Force contention
		q.Push(tick, h, testValue)
	}
}
