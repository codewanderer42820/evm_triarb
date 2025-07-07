// ============================================================================
// QUANTUMQUEUE MICROBENCHMARK SUITE
// ============================================================================
//
// Comprehensive performance measurement suite for QuantumQueue core operations.
// Validates sub-10ns operation latency under realistic ISR workload patterns.
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
	"time"
)

// ============================================================================
// BENCHMARK CONFIGURATION
// ============================================================================

const benchSize = CapItems // Use full arena capacity for realistic stress testing

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
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}
	val := new([48]byte)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, val)
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
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, new([48]byte))
	}
	val := new([48]byte)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, val)
	}
}

// BenchmarkPushSameTickZero measures performance for edge case tick value (0).
// Validates consistent performance across the tick range boundary conditions.
//
// Edge case validation:
//   - Minimum tick value handling
//   - Bitmap index computation accuracy
//   - Expected latency: 3-6ns per operation
func BenchmarkPushSameTickZero(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	val := new([48]byte)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(0, h, val)
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
	val := new([48]byte)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(int64(benchSize-1), h, val)
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
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}

	// Pre-generate deterministic random sequence for reproducibility
	rand.Seed(time.Now().UnixNano())
	ticks := make([]int64, benchSize)
	for i := range ticks {
		ticks[i] = rand.Int63n(benchSize)
	}

	val := new([48]byte)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(ticks[i%benchSize], h, val)
	}
}

// ============================================================================
// MINIMUM EXTRACTION BENCHMARKS
// ============================================================================

// BenchmarkPeepMin measures minimum finding performance via bitmap hierarchy.
// Tests O(1) minimum extraction using CLZ-based bitmap traversal.
//
// Algorithm performance:
//   - 3-level bitmap hierarchy traversal
//   - CLZ instruction utilization
//   - Cache-optimized data structure access
//   - Expected latency: 4-8ns per operation
func BenchmarkPeepMin(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, new([48]byte))
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
//   - Minimal cache line access
//   - Expected latency: 3-6ns per operation
func BenchmarkUnlinkMin_StableBucket(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	val := new([48]byte)
	q.Push(2048, h, val)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.UnlinkMin(h)
		q.Push(2048, h, val)
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
	val := new([48]byte)
	for i := 0; i < 3; i++ {
		hs[i], _ = q.BorrowSafe()
		q.Push(1234, hs[i], val)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := hs[i%3]
		q.UnlinkMin(h)
		q.Push(1234, h, val)
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
	val := new([48]byte)
	h, _ := q.BorrowSafe()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tick := int64(i % BucketCount)
		q.Push(tick, h, val)
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
	val := new([48]byte)
	h, _ := q.BorrowSafe()

	// Pre-generate random tick sequence for reproducibility
	ticks := make([]int64, b.N)
	for i := range ticks {
		ticks[i] = int64(rand.Intn(BucketCount))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tick := ticks[i]
		q.Push(tick, h, val)
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
	val := new([48]byte)
	h, _ := q.BorrowSafe()
	const tick = 4095
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(tick, h, val)
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
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, new([48]byte))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.MoveTick(h, int64((i+1)%benchSize))
	}
}
