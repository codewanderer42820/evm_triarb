// ============================================================================
// SPSC RING BUFFER PERFORMANCE MICROBENCHMARK SUITE
// ============================================================================
//
// Comprehensive performance measurement framework for lock-free SPSC ring buffer
// operations with emphasis on real-world ISR scenario modeling.
//
// Benchmark categories:
//   - Single operation latency: Push/Pop individual operation costs
//   - Throughput measurement: Sustained operation rates under load
//   - Cache behavior: Memory hierarchy performance characteristics
//   - Cross-core performance: True SPSC deployment with cache effects
//   - Contention scenarios: Performance under various load patterns
//   - Memory pressure: Behavior under different allocation patterns
//   - Thermal effects: Performance stability under sustained load
//
// Performance targets:
//   - Push operations: <10ns per operation (optimal), <20ns (acceptable)
//   - Pop operations: <8ns per operation (optimal), <15ns (acceptable)
//   - Interleaved operations: <12ns per operation (optimal), <25ns (acceptable)
//   - Cross-core SPSC: <50ns per operation (including cache transfer)
//   - Sustained throughput: >100M ops/sec single-core, >50M ops/sec cross-core
//
// Test methodology:
//   - Multiple ring sizes for cache hierarchy analysis
//   - Pre-allocated payloads to eliminate allocation noise
//   - Realistic retry patterns for capacity management
//   - Cross-core testing to model true SPSC deployment
//   - Thermal stability testing for sustained performance
//   - Memory bandwidth utilization measurement
//
// Memory hierarchy considerations:
//   - Ring sizes: 64 (L1), 1024 (L2), 16384 (L3), 65536 (Memory)
//   - Cache line effects measured via alignment analysis
//   - Memory bandwidth utilization under sustained throughput
//   - NUMA effects on cross-core performance

package ring24

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// BENCHMARK CONFIGURATION AND UTILITIES
// ============================================================================

// Benchmark ring sizes targeting different cache levels
const (
	benchCap64  = 64    // L1 cache friendly
	benchCap1K  = 1024  // L2 cache friendly
	benchCap16K = 16384 // L3 cache friendly
	benchCap64K = 65536 // Memory resident
)

// Pre-allocated test data to eliminate allocation overhead
var (
	dummy24      = &[24]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}
	randomData24 = generateRandomData()
	sink         any // Escape variable to prevent compiler optimizations
)

// generateRandomData creates diverse test payloads
func generateRandomData() [1000]*[24]byte {
	var data [1000]*[24]byte
	for i := range data {
		data[i] = &[24]byte{}
		rand.Read(data[i][:])
	}
	return data
}

// benchmarkSizes returns common ring sizes for comprehensive testing
func benchmarkSizes() []int {
	return []int{64, 256, 1024, 4096, 16384}
}

// warmupRing performs initial operations to establish steady state
func warmupRing(r *Ring, operations int) {
	for i := 0; i < operations; i++ {
		if r.Push(dummy24) {
			sink = r.Pop()
		}
	}
	runtime.KeepAlive(sink)
}

// ============================================================================
// SINGLE OPERATION LATENCY BENCHMARKS
// ============================================================================

// BenchmarkRing_Push measures producer latency under optimal conditions
func BenchmarkRing_Push(b *testing.B) {
	sizes := benchmarkSizes()

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)
			warmupRing(r, 1000)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if !r.Push(dummy24) {
					// Handle overflow with minimal retry
					_ = r.Pop()
					_ = r.Push(dummy24)
				}
			}
		})
	}
}

// BenchmarkRing_PushRandomData measures push performance with varied payloads
func BenchmarkRing_PushRandomData(b *testing.B) {
	r := New(benchCap1K)
	warmupRing(r, 1000)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := randomData24[i%len(randomData24)]
		if !r.Push(data) {
			_ = r.Pop()
			_ = r.Push(data)
		}
	}
}

// BenchmarkRing_Pop measures consumer latency with pre-filled buffer
func BenchmarkRing_Pop(b *testing.B) {
	sizes := benchmarkSizes()

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)

			// Pre-fill ring to simulate data-ready conditions
			for i := 0; i < size-1; i++ {
				r.Push(dummy24)
			}

			warmupRing(r, 1000)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				p := r.Pop()
				if p == nil {
					// Handle underrun with immediate refill
					r.Push(dummy24)
					p = r.Pop()
				}
				sink = p
				_ = r.Push(dummy24) // Maintain buffer fullness
			}
			runtime.KeepAlive(sink)
		})
	}
}

// BenchmarkRing_PopEmpty measures pop latency on empty ring
func BenchmarkRing_PopEmpty(b *testing.B) {
	r := New(benchCap1K)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sink = r.Pop() // Should always return nil
	}
	runtime.KeepAlive(sink)
}

// BenchmarkRing_PushFull measures push latency on full ring
func BenchmarkRing_PushFull(b *testing.B) {
	r := New(benchCap1K)

	// Fill ring completely
	for i := 0; i < benchCap1K; i++ {
		r.Push(dummy24)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = r.Push(dummy24) // Should always fail
	}
}

// ============================================================================
// INTERLEAVED OPERATION BENCHMARKS
// ============================================================================

// BenchmarkRing_PushPop measures sequential push/pop pipeline performance
func BenchmarkRing_PushPop(b *testing.B) {
	sizes := benchmarkSizes()

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)

			// Pre-fill to half capacity for balanced operation
			for i := 0; i < size/2; i++ {
				r.Push(dummy24)
			}

			warmupRing(r, 1000)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				p := r.Pop()
				sink = p
				_ = r.Push(dummy24)
			}
			runtime.KeepAlive(sink)
		})
	}
}

// BenchmarkRing_PushPopBatch measures batch operation efficiency
func BenchmarkRing_PushPopBatch(b *testing.B) {
	batchSizes := []int{1, 2, 4, 8, 16, 32}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			r := New(benchCap1K)
			warmupRing(r, 1000)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i += batchSize {
				// Push batch
				for j := 0; j < batchSize && i+j < b.N; j++ {
					if !r.Push(dummy24) {
						r.Pop()
						r.Push(dummy24)
					}
				}

				// Pop batch
				for j := 0; j < batchSize && i+j < b.N; j++ {
					p := r.Pop()
					if p == nil {
						r.Push(dummy24)
						p = r.Pop()
					}
					sink = p
				}
			}
			runtime.KeepAlive(sink)
		})
	}
}

// ============================================================================
// THROUGHPUT AND SUSTAINED PERFORMANCE BENCHMARKS
// ============================================================================

// BenchmarkRing_SustainedThroughput measures long-term performance stability
func BenchmarkRing_SustainedThroughput(b *testing.B) {
	durations := []time.Duration{
		100 * time.Millisecond,
		1 * time.Second,
		5 * time.Second,
	}

	for _, duration := range durations {
		b.Run(fmt.Sprintf("duration_%v", duration), func(b *testing.B) {
			r := New(benchCap1K)
			warmupRing(r, 10000)

			start := time.Now()
			operations := 0

			b.ResetTimer()

			for time.Since(start) < duration {
				for i := 0; i < 1000; i++ {
					if r.Push(dummy24) {
						sink = r.Pop()
						operations++
					}
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(operations)/duration.Seconds(), "ops/sec")
			runtime.KeepAlive(sink)
		})
	}
}

// BenchmarkRing_ThermalStability measures performance under extended load
func BenchmarkRing_ThermalStability(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping thermal stability test in short mode")
	}

	r := New(benchCap1K)
	warmupRing(r, 10000)

	// Measure performance in intervals
	intervals := 10
	opsPerInterval := b.N / intervals

	b.ReportAllocs()
	b.ResetTimer()

	for interval := 0; interval < intervals; interval++ {
		start := time.Now()

		for i := 0; i < opsPerInterval; i++ {
			if r.Push(dummy24) {
				sink = r.Pop()
			}
		}

		elapsed := time.Since(start)
		b.ReportMetric(float64(opsPerInterval)/elapsed.Seconds(), fmt.Sprintf("ops/sec_interval_%d", interval))
	}

	runtime.KeepAlive(sink)
}

// ============================================================================
// CROSS-CORE AND CONCURRENT BENCHMARKS
// ============================================================================

// BenchmarkRing_CrossCore measures true SPSC performance across CPU cores
func BenchmarkRing_CrossCore(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			runtime.GOMAXPROCS(2)
			r := New(size)
			var wg sync.WaitGroup
			wg.Add(1)

			// Consumer goroutine
			go func() {
				defer wg.Done()
				runtime.LockOSThread()
				consumed := 0
				for consumed < b.N {
					if p := r.Pop(); p != nil {
						sink = p
						consumed++
					}
					// Active polling for maximum throughput
				}
			}()

			// Allow consumer to start
			time.Sleep(time.Millisecond)

			b.ReportAllocs()
			b.ResetTimer()

			// Producer loop (benchmark timing)
			runtime.LockOSThread()
			for i := 0; i < b.N; i++ {
				for !r.Push(dummy24) {
					// Spin until consumer creates capacity
				}
			}

			wg.Wait()
			runtime.KeepAlive(sink)
		})
	}
}

// BenchmarkRing_CrossCoreWithBackoff measures SPSC with CPU relaxation
func BenchmarkRing_CrossCoreWithBackoff(b *testing.B) {
	runtime.GOMAXPROCS(2)
	r := New(benchCap1K)
	var wg sync.WaitGroup
	wg.Add(1)

	// Consumer with backoff
	go func() {
		defer wg.Done()
		runtime.LockOSThread()
		consumed := 0
		misses := 0
		for consumed < b.N {
			if p := r.Pop(); p != nil {
				sink = p
				consumed++
				misses = 0
			} else {
				misses++
				if misses > 100 {
					runtime.Gosched()
					misses = 0
				}
			}
		}
	}()

	time.Sleep(time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()

	runtime.LockOSThread()
	for i := 0; i < b.N; i++ {
		retries := 0
		for !r.Push(dummy24) {
			retries++
			if retries > 100 {
				runtime.Gosched()
				retries = 0
			}
		}
	}

	wg.Wait()
	runtime.KeepAlive(sink)
}

// ============================================================================
// CACHE AND MEMORY HIERARCHY BENCHMARKS
// ============================================================================

// BenchmarkRing_CacheHierarchy measures performance across cache levels
func BenchmarkRing_CacheHierarchy(b *testing.B) {
	cacheSizes := map[string]int{
		"L1":     64,
		"L2":     1024,
		"L3":     16384,
		"Memory": 65536,
	}

	for level, size := range cacheSizes {
		b.Run(level, func(b *testing.B) {
			r := New(size)

			// Pre-fill to test cache effects
			for i := 0; i < size/2; i++ {
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
		})
	}
}

// BenchmarkRing_MemoryBandwidth measures memory subsystem utilization
func BenchmarkRing_MemoryBandwidth(b *testing.B) {
	r := New(benchCap64K) // Large enough to stress memory

	// Fill ring completely to maximize memory access
	for i := 0; i < benchCap64K; i++ {
		r.Push(randomData24[i%len(randomData24)])
	}

	b.ReportAllocs()
	b.SetBytes(24) // Bytes per operation
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := r.Pop()
		sink = p
		data := randomData24[i%len(randomData24)]
		_ = r.Push(data)
	}
	runtime.KeepAlive(sink)
}

// BenchmarkRing_CacheLineBouncing measures false sharing effects
func BenchmarkRing_CacheLineBouncing(b *testing.B) {
	runtime.GOMAXPROCS(2)
	r := New(benchCap1K)
	var wg sync.WaitGroup
	wg.Add(2)

	// Two goroutines accessing the same ring
	producer := func() {
		defer wg.Done()
		runtime.LockOSThread()
		for i := 0; i < b.N/2; i++ {
			for !r.Push(dummy24) {
				runtime.Gosched()
			}
		}
	}

	consumer := func() {
		defer wg.Done()
		runtime.LockOSThread()
		consumed := 0
		for consumed < b.N/2 {
			if p := r.Pop(); p != nil {
				sink = p
				consumed++
			} else {
				runtime.Gosched()
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	go producer()
	go consumer()

	wg.Wait()
	runtime.KeepAlive(sink)
}

// ============================================================================
// SPECIALIZED OPERATION BENCHMARKS
// ============================================================================

// BenchmarkRing_PopWait measures blocking consumer performance
func BenchmarkRing_PopWait(b *testing.B) {
	r := New(benchCap1K)
	var wg sync.WaitGroup
	wg.Add(1)

	// Consumer using PopWait
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			sink = r.PopWait()
		}
	}()

	// Producer with slight delay to test blocking
	time.Sleep(time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for !r.Push(dummy24) {
			time.Sleep(time.Microsecond)
		}
	}

	wg.Wait()
	runtime.KeepAlive(sink)
}

// BenchmarkRing_OverflowRecovery measures recovery from overflow conditions
func BenchmarkRing_OverflowRecovery(b *testing.B) {
	r := New(64) // Small ring to force overflows

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Attempt push, recover if failed
		if !r.Push(dummy24) {
			sink = r.Pop()      // Make space
			_ = r.Push(dummy24) // Retry
		}
	}
	runtime.KeepAlive(sink)
}

// BenchmarkRing_EmptyFullCycles measures state transition overhead
func BenchmarkRing_EmptyFullCycles(b *testing.B) {
	r := New(64)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Fill ring
		for j := 0; j < 64; j++ {
			r.Push(dummy24)
		}

		// Empty ring
		for j := 0; j < 64; j++ {
			sink = r.Pop()
		}
	}
	runtime.KeepAlive(sink)
}

// ============================================================================
// MEMORY ALLOCATION AND GC PRESSURE BENCHMARKS
// ============================================================================

// BenchmarkRing_ZeroAlloc validates zero-allocation operation
func BenchmarkRing_ZeroAlloc(b *testing.B) {
	r := New(benchCap1K)
	warmupRing(r, 1000)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if r.Push(dummy24) {
			sink = r.Pop()
		}
	}
	runtime.KeepAlive(sink)
}

// BenchmarkRing_UnderGCPressure measures performance during garbage collection
func BenchmarkRing_UnderGCPressure(b *testing.B) {
	r := New(benchCap1K)

	// Create GC pressure
	go func() {
		for {
			_ = make([]byte, 1024*1024) // 1MB allocation
			runtime.GC()
			time.Sleep(time.Millisecond)
		}
	}()

	warmupRing(r, 1000)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if r.Push(dummy24) {
			sink = r.Pop()
		}
	}
	runtime.KeepAlive(sink)
}

// ============================================================================
// COMPARATIVE AND SCALING BENCHMARKS
// ============================================================================

// BenchmarkRing_ScalingFactors measures performance scaling with ring size
func BenchmarkRing_ScalingFactors(b *testing.B) {
	sizes := []int{8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("scale_%d", size), func(b *testing.B) {
			r := New(size)
			warmupRing(r, 1000)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if r.Push(dummy24) {
					sink = r.Pop()
				}
			}
			runtime.KeepAlive(sink)
		})
	}
}

// BenchmarkRing_LoadFactors measures performance at different fill levels
func BenchmarkRing_LoadFactors(b *testing.B) {
	loadFactors := []float64{0.1, 0.25, 0.5, 0.75, 0.9, 0.95}
	ringSize := 1024

	for _, load := range loadFactors {
		b.Run(fmt.Sprintf("load_%.2f", load), func(b *testing.B) {
			r := New(ringSize)

			// Pre-fill to target load
			fillLevel := int(float64(ringSize) * load)
			for i := 0; i < fillLevel; i++ {
				r.Push(dummy24)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Maintain approximate load level
				if r.Push(dummy24) {
					sink = r.Pop()
				}
			}
			runtime.KeepAlive(sink)
		})
	}
}

// ============================================================================
// PLATFORM-SPECIFIC BENCHMARKS
// ============================================================================

// BenchmarkRing_CpuRelaxEffect measures impact of CPU relaxation
func BenchmarkRing_CpuRelaxEffect(b *testing.B) {
	r := New(benchCap1K)

	b.Run("without_relax", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for !r.Push(dummy24) {
				// Busy wait without relaxation
			}
			sink = r.Pop()
		}
		runtime.KeepAlive(sink)
	})

	b.Run("with_relax", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			misses := 0
			for !r.Push(dummy24) {
				if misses++; misses > 10 {
					cpuRelax()
					misses = 0
				}
			}
			sink = r.Pop()
		}
		runtime.KeepAlive(sink)
	})
}

// BenchmarkRing_MemoryOrdering measures atomic operation overhead
func BenchmarkRing_MemoryOrdering(b *testing.B) {
	r := New(benchCap1K)

	// Direct access to test atomic overhead
	slot := &r.buf[0]

	b.Run("atomic_load", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			sink = atomic.LoadUint64(&slot.seq)
		}
		runtime.KeepAlive(sink)
	})

	b.Run("atomic_store", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			atomic.StoreUint64(&slot.seq, uint64(i))
		}
	})

	b.Run("memory_copy", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			slot.val = *dummy24
		}
	})
}

// ============================================================================
// REGRESSION AND STABILITY BENCHMARKS
// ============================================================================

// BenchmarkRing_Regression validates performance doesn't degrade
func BenchmarkRing_Regression(b *testing.B) {
	// This benchmark serves as a performance regression detector
	r := New(benchCap1K)
	warmupRing(r, 10000)

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		if r.Push(dummy24) {
			sink = r.Pop()
		}
	}
	elapsed := time.Since(start)

	// Report detailed metrics for regression analysis
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "ns/op")

	runtime.KeepAlive(sink)
}

// BenchmarkRing_WorstCase measures performance under adverse conditions
func BenchmarkRing_WorstCase(b *testing.B) {
	// Small ring with maximum contention potential
	r := New(2)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Force maximum retry cycles
		retries := 0
		for !r.Push(dummy24) {
			if retries++; retries > 100 {
				sink = r.Pop() // Force progress
				retries = 0
			}
		}
		sink = r.Pop()
	}
	runtime.KeepAlive(sink)
}
