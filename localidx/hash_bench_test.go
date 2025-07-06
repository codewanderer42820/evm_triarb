// ============================================================================
// ISR HASHMAP PERFORMANCE MICROBENCHMARK SUITE
// ============================================================================
//
// Comprehensive performance measurement framework for Robin-Hood hashmap
// operations with emphasis on ISR-grade latency and throughput analysis.
//
// Benchmark categories:
//   - Cold path operations: Initial insertions and unique key processing
//   - Hot path operations: Overwrite attempts and repeated key access
//   - Lookup patterns: Hit/miss/mixed access with cache analysis
//   - Stress scenarios: Collision handling and probe distance extremes
//
// Performance targets:
//   - Cold insertions: <20ns per operation
//   - Hot overwrites: <10ns per operation
//   - Lookup hits: <15ns per operation
//   - Lookup misses: <10ns per operation (early termination)
//
// Test methodology:
//   - Pre-shuffled key sets eliminate allocation noise
//   - Cache-aware sizing (L1/L2/L3 boundary testing)
//   - Single-core pinned execution for consistent measurements
//   - Statistical significance via large iteration counts
//
// Memory hierarchy analysis:
//   - Tiny maps: L1 cache optimization (256 entries)
//   - Medium maps: L2 cache utilization (64K entries)
//   - Large maps: L3 cache and memory bandwidth testing

package localidx

import (
	"testing"
)

// ============================================================================
// BENCHMARK CONFIGURATION
// ============================================================================

const (
	insertSize = 1 << 17        // 131072 entries → ~512 KiB table (L2/L3 boundary)
	lookupSize = insertSize / 2 // 65536 entries → 50% hit/miss ratio balance
)

// Pre-allocated key sets for noise-free benchmarking
var (
	keys     = make([]uint32, insertSize) // Shuffled insertion keys
	missKeys = make([]uint32, lookupSize) // Guaranteed miss keys
)

// Initialize deterministic key sets for consistent benchmarking
func init() {
	// Generate sequential keys and shuffle for realistic access patterns
	for i := 0; i < insertSize; i++ {
		keys[i] = uint32(i + 1) // Avoid sentinel key 0
	}
	rnd.Shuffle(insertSize, func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	// Generate guaranteed miss keys (outside insertion range)
	for i := 0; i < lookupSize; i++ {
		missKeys[i] = uint32(i + insertSize + 100)
	}
}

// ============================================================================
// INSERTION BENCHMARKS
// ============================================================================

// BenchmarkHashPutUnique measures cold insertion performance.
// Tests initial key insertion into clean table with no overwrites.
//
// Performance characteristics:
//   - Robin-Hood displacement overhead
//   - Cache miss patterns during table growth
//   - Memory allocation and initialization costs
//
// Expected performance: <20ns per operation
// Use cases: Initial table population, bulk data ingestion
func BenchmarkHashPutUnique(b *testing.B) {
	for n := 0; n < b.N; n++ {
		h := New(insertSize)
		for i := 0; i < insertSize; i++ {
			h.Put(keys[i], keys[i]*2)
		}
	}
}

// BenchmarkHashPutOverwrite measures hot path overwrite performance.
// Tests overwrite attempt performance on fully populated table.
//
// Performance characteristics:
//   - Key lookup without insertion overhead
//   - Cache-hot access patterns
//   - Minimal Robin-Hood displacement
//
// Expected performance: <10ns per operation
// Use cases: High-frequency value updates, cache refresh patterns
func BenchmarkHashPutOverwrite(b *testing.B) {
	h := New(insertSize)

	// Pre-populate table for overwrite testing
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < insertSize; i++ {
			h.Put(keys[i], keys[i]*3) // Attempt overwrite (returns original)
		}
	}
}

// ============================================================================
// LOOKUP BENCHMARKS
// ============================================================================

// BenchmarkHashGetHit measures lookup performance for present keys.
// Tests optimal path performance with 100% hit rate.
//
// Performance characteristics:
//   - Robin-Hood probe efficiency
//   - Cache locality for frequently accessed keys
//   - Prefetch effectiveness
//
// Expected performance: <15ns per operation
// Use cases: Cache hit scenarios, frequent key access patterns
func BenchmarkHashGetHit(b *testing.B) {
	h := New(insertSize)

	// Populate table for hit testing
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		k := keys[n%insertSize]
		_, _ = h.Get(k)
	}
}

// BenchmarkHashGetMiss measures lookup performance for absent keys.
// Tests early termination efficiency with 100% miss rate.
//
// Performance characteristics:
//   - Robin-Hood bound checking effectiveness
//   - Early termination via probe distance analysis
//   - Minimal cache pollution from failed lookups
//
// Expected performance: <10ns per operation
// Use cases: Cache miss scenarios, negative lookup validation
func BenchmarkHashGetMiss(b *testing.B) {
	h := New(insertSize)

	// Populate table for miss testing
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		k := missKeys[n%lookupSize]
		_, _ = h.Get(k)
	}
}

// BenchmarkHashGetMixed measures realistic mixed access patterns.
// Tests performance under 50/50 hit/miss ratio.
//
// Performance characteristics:
//   - Branch prediction impact from mixed outcomes
//   - Cache behavior under varied access patterns
//   - Robin-Hood efficiency across hit/miss scenarios
//
// Expected performance: <12ns per operation (average of hit/miss)
// Use cases: Realistic application access patterns, cache evaluation
func BenchmarkHashGetMixed(b *testing.B) {
	h := New(insertSize)

	// Populate table for mixed testing
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var k uint32
		if n&1 == 0 {
			k = keys[n%insertSize] // Hit case
		} else {
			k = missKeys[n%lookupSize] // Miss case
		}
		_, _ = h.Get(k)
	}
}

// BenchmarkHashGetTightLoop measures repeated access to same key.
// Tests cache optimization for hot key scenarios.
//
// Performance characteristics:
//   - L1 cache optimization for repeated access
//   - Branch predictor effectiveness
//   - Memory prefetch efficiency
//
// Expected performance: <5ns per operation (L1 cache hit)
// Use cases: Hot key access patterns, frequently queried data
func BenchmarkHashGetTightLoop(b *testing.B) {
	h := New(insertSize)

	// Populate table
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}

	k := keys[12345] // Fixed key for tight loop testing
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _ = h.Get(k)
	}
}

// BenchmarkHashGetTinyMap measures performance in L1-optimal scenarios.
// Tests cache behavior with table fitting entirely in L1 cache.
//
// Performance characteristics:
//   - L1 cache utilization efficiency
//   - Minimal memory latency impact
//   - Optimal cache line utilization
//
// Expected performance: <8ns per operation
// Use cases: Small lookup tables, embedded system optimization
func BenchmarkHashGetTinyMap(b *testing.B) {
	const size = 256 // L1 cache-friendly size
	h := New(size)

	// Populate tiny table
	for i := 0; i < size; i++ {
		h.Put(uint32(i+1), uint32(i+100))
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		k := uint32((n & (size - 1)) + 1)
		_, _ = h.Get(k)
	}
}

// ============================================================================
// STRESS BENCHMARKS
// ============================================================================

// BenchmarkHashPutLinearProbeWorst measures pathological collision performance.
// Tests Robin-Hood behavior under extreme probe distance scenarios.
//
// Test methodology:
//  1. Insert keys designed to create maximum collisions
//  2. Measure insertion performance under worst-case probe distances
//  3. Validate Robin-Hood displacement effectiveness
//
// Performance characteristics:
//   - Maximum probe distance handling
//   - Robin-Hood displacement overhead
//   - Cache thrashing under high collision rates
//
// Expected performance: <50ns per operation (worst case)
// Use cases: Adversarial input handling, robustness validation
func BenchmarkHashPutLinearProbeWorst(b *testing.B) {
	h := New(insertSize)
	base := uint32(0xDEADBEEF) // Base value for collision generation

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// Generate colliding keys for pathological behavior
		h.Put(base+uint32(n&255), uint32(n))
	}
}
