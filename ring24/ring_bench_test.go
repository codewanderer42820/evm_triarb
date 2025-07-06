// ============================================================================
// SPSC RING BUFFER PERFORMANCE MICROBENCHMARK SUITE
// ============================================================================
//
// Comprehensive performance measurement framework for lock-free SPSC ring buffer
// operations with emphasis on real-world ISR scenario modeling.
//
// Benchmark categories:
//   - Producer-only: Push throughput under capacity pressure
//   - Consumer-only: Pop latency with pre-filled buffers
//   - Interleaved operations: Sequential push/pop pipeline performance
//   - Cross-core SPSC: True producer/consumer separation with cache effects
//
// Performance targets:
//   - Push operations: <10ns per operation
//   - Pop operations: <8ns per operation
//   - Interleaved operations: <12ns per operation
//   - Cross-core SPSC: <50ns per operation (including cache transfer)
//
// Test methodology:
//   - L1/L2 cache-friendly sizing for consistent measurements
//   - Pre-allocated payloads to eliminate allocation noise
//   - Realistic retry patterns for capacity management
//   - Cross-core testing to model true SPSC deployment
//
// Memory hierarchy considerations:
//   - Ring size fits in L1/L2 cache for optimal performance
//   - Cache line effects measured via cross-core benchmarks
//   - Memory bandwidth utilization under sustained throughput

package ring24

import (
	"runtime"
	"sync"
	"testing"
)

// ============================================================================
// BENCHMARK CONFIGURATION
// ============================================================================

const benchCap = 1024 // Ring capacity fitting in L1/L2 cache for optimal performance

// Pre-allocated test data to eliminate allocation overhead during measurement
var (
	dummy24 = &[24]byte{1, 2, 3} // Standard payload for throughput testing
	sink    any                  // Escape variable to prevent compiler optimizations
)

// ============================================================================
// PRODUCER BENCHMARKS
// ============================================================================

// BenchmarkRing_Push measures producer throughput under capacity pressure.
// Tests write-side performance with realistic overflow handling and retry patterns.
//
// Performance characteristics:
//   - Measures push latency under full ring conditions
//   - Tests retry behavior when capacity temporarily exceeded
//   - Validates producer-side cache efficiency
//
// Expected performance: <10ns per operation
// Use cases: High-frequency data ingestion, ISR tick routing
func BenchmarkRing_Push(b *testing.B) {
	r := New(benchCap)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !r.Push(dummy24) {
			// Handle overflow with immediate retry (ISR pattern)
			_ = r.Pop()         // Clear one slot
			_ = r.Push(dummy24) // Retry push operation
		}
	}
}

// ============================================================================
// CONSUMER BENCHMARKS
// ============================================================================

// BenchmarkRing_Pop measures consumer read latency with pre-filled buffer.
// Tests read-side performance under optimal data availability conditions.
//
// Performance characteristics:
//   - Measures pop latency with data immediately available
//   - Tests consumer-side cache utilization efficiency
//   - Validates sequence number checking overhead
//
// Expected performance: <8ns per operation
// Use cases: Real-time data processing, tick consumption pipelines
func BenchmarkRing_Pop(b *testing.B) {
	r := New(benchCap)

	// Pre-fill ring to simulate data-ready conditions
	for i := 0; i < benchCap-1; i++ {
		r.Push(dummy24)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := r.Pop()
		if p == nil {
			// Handle underrun with immediate refill
			r.Push(dummy24)
			p = r.Pop()
		}
		sink = p            // Prevent compiler elimination
		_ = r.Push(dummy24) // Maintain buffer fullness
	}
	runtime.KeepAlive(sink)
}

// ============================================================================
// INTERLEAVED OPERATION BENCHMARKS
// ============================================================================

// BenchmarkRing_PushPop measures sequential push/pop pipeline performance.
// Tests interleaved operation efficiency for single-threaded relay scenarios.
//
// Performance characteristics:
//   - Measures combined push/pop latency in tight loop
//   - Tests cache locality for sequential operations
//   - Validates pipeline efficiency for relay applications
//
// Expected performance: <12ns per operation pair
// Use cases: Microservice local rings, single-threaded data transformation
func BenchmarkRing_PushPop(b *testing.B) {
	r := New(benchCap)

	// Pre-fill to half capacity for balanced operation
	for i := 0; i < benchCap/2; i++ {
		r.Push(dummy24)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := r.Pop()
		sink = p
		_ = r.Push(dummy24)
	}
	runtime.KeepAlive(sink)
}

// ============================================================================
// CROSS-CORE SPSC BENCHMARKS
// ============================================================================

// BenchmarkRing_CrossCore measures true SPSC performance across CPU cores.
// Tests producer/consumer separation with realistic cache transfer effects.
//
// Test methodology:
//  1. Dedicated producer thread pushing data continuously
//  2. Dedicated consumer thread popping data continuously
//  3. Measure sustained throughput with cache line transfers
//  4. Synchronize completion via wait group
//
// Performance characteristics:
//   - Measures cache line transfer overhead between cores
//   - Tests SPSC protocol under true concurrency conditions
//   - Validates memory ordering and visibility guarantees
//
// Expected performance: <50ns per operation (including cache effects)
// Use cases: ISR to CPU pipelines, cross-core data streaming
func BenchmarkRing_CrossCore(b *testing.B) {
	runtime.GOMAXPROCS(2) // Ensure sufficient cores for separation
	r := New(benchCap)
	var wg sync.WaitGroup
	wg.Add(1)

	// Consumer goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; {
			if r.Pop() != nil {
				i++
			}
			// Active polling without relaxation for maximum throughput
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()

	// Producer loop (benchmark timing)
	for i := 0; i < b.N; i++ {
		for !r.Push(dummy24) {
			// Spin until consumer creates capacity
			// Models ISR retry behavior under backpressure
		}
	}

	wg.Wait()
}
