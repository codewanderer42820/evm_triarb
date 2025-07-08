// ============================================================================
// SPSC RING BUFFER PRECISION BENCHMARK SUITE
// ============================================================================
//
// High-precision performance measurement framework for lock-free SPSC ring buffer
// operations with emphasis on eliminating measurement overhead and focusing on
// actual hardware-level performance characteristics.
//
// Benchmark categories:
//   - Core operations: Pure Push/Pop latency without measurement artifacts
//   - Cross-core performance: True SPSC deployment with cache coherency effects
//   - Comparative analysis: Fair comparison against Go channels and mutex approaches
//   - Regression detection: Performance threshold validation for ISR-grade code
//
// Performance targets (ISR-grade):
//   - Push operations: <15ns per operation (optimal), <30ns (acceptable)
//   - Pop operations: <12ns per operation (optimal), <25ns (acceptable)
//   - Push+Pop pair: <25ns per operation (optimal), <50ns (acceptable)
//   - Cross-core SPSC: <50ns per operation (including cache transfer overhead)
//   - Sustained throughput: >20M ops/sec single-core, >10M ops/sec cross-core
//
// Measurement methodology:
//   - Elimination of time.Now() overhead in critical paths
//   - Pre-warmed steady-state operation for accurate results
//   - Cache-friendly test patterns to avoid measurement artifacts
//   - Statistical validation with performance regression thresholds

package ring24

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// CORE RING BUFFER OPERATION BENCHMARKS
// ============================================================================

// BenchmarkRing_PushOnly measures pure producer latency under optimal conditions
func BenchmarkRing_PushOnly(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)
			data := &[24]byte{}

			// Warmup phase
			for i := 0; i < 1000; i++ {
				if r.Push(data) {
					r.Pop()
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if !r.Push(data) {
					// Ring capacity exceeded, create space and retry
					r.Pop()
					r.Push(data)
				}
			}
		})
	}
}

// BenchmarkRing_PopOnly measures pure consumer latency with pre-filled buffer
func BenchmarkRing_PopOnly(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)
			data := &[24]byte{}

			// Pre-fill ring to simulate data-ready conditions
			for i := 0; i < size; i++ {
				r.Push(data)
			}

			// Warmup phase
			for i := 0; i < 1000; i++ {
				if p := r.Pop(); p != nil {
					r.Push(data)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				p := r.Pop()
				if p == nil {
					// Ring underrun, refill and retry
					r.Push(data)
					p = r.Pop()
				}
				_ = p // Prevent compiler optimization
			}
		})
	}
}

// BenchmarkRing_PushPopPair measures combined push+pop operation latency
func BenchmarkRing_PushPopPair(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)
			data := &[24]byte{}

			// Warmup phase
			for i := 0; i < 1000; i++ {
				r.Push(data)
				r.Pop()
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				r.Push(data)
				p := r.Pop()
				_ = p
			}
		})
	}
}

// ============================================================================
// CROSS-CORE SPSC DEPLOYMENT BENCHMARKS
// ============================================================================

// BenchmarkRing_CrossCoreSPSC measures true SPSC performance across CPU cores
func BenchmarkRing_CrossCoreSPSC(b *testing.B) {
	runtime.GOMAXPROCS(2)

	sizes := []int{64, 256, 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)
			data := &[24]byte{}

			var wg sync.WaitGroup
			wg.Add(1)

			// Consumer goroutine pinned to dedicated core
			go func() {
				defer wg.Done()
				runtime.LockOSThread()

				consumed := 0
				for consumed < b.N {
					if p := r.Pop(); p != nil {
						consumed++
						_ = p // Prevent compiler optimization
					}
				}
			}()

			// Brief delay to allow consumer initialization
			time.Sleep(time.Millisecond)

			b.ReportAllocs()
			b.ResetTimer()

			// Producer goroutine - measurement target
			runtime.LockOSThread()
			for i := 0; i < b.N; i++ {
				for !r.Push(data) {
					// Active polling until consumer creates capacity
				}
			}

			wg.Wait()
		})
	}
}

// ============================================================================
// SUSTAINED THROUGHPUT BENCHMARKS
// ============================================================================

// BenchmarkRing_MaxThroughput measures pure sustained throughput capability
func BenchmarkRing_MaxThroughput(b *testing.B) {
	runtime.GOMAXPROCS(2)

	r := New(2048) // Large ring buffer to minimize stalls
	data := &[24]byte{}

	var wg sync.WaitGroup
	wg.Add(1)

	// Consumer goroutine
	go func() {
		defer wg.Done()
		runtime.LockOSThread()

		consumed := 0
		for consumed < b.N {
			if p := r.Pop(); p != nil {
				consumed++
				_ = p
			}
		}
	}()

	// Warmup phase for steady-state operation
	for i := 0; i < 1000; i++ {
		if r.Push(data) {
			// Let consumer drain
		}
	}
	time.Sleep(10 * time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()

	// Push at maximum sustainable rate
	runtime.LockOSThread()
	for i := 0; i < b.N; i++ {
		for !r.Push(data) {
			// Active polling
		}
	}

	elapsed := time.Since(start)
	wg.Wait()

	// Report performance metrics
	throughput := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(throughput, "ops/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "ns/op")
}

// ============================================================================
// COMPARATIVE PERFORMANCE ANALYSIS
// ============================================================================

// BenchmarkComparison_RingVsChannel provides fair performance comparison
func BenchmarkComparison_RingVsChannel(b *testing.B) {
	runtime.GOMAXPROCS(2)

	b.Run("ring_buffer", func(b *testing.B) {
		r := New(1024)
		data := &[24]byte{}

		var wg sync.WaitGroup
		wg.Add(1)

		// Consumer goroutine
		go func() {
			defer wg.Done()
			consumed := 0
			for consumed < b.N {
				if p := r.Pop(); p != nil {
					consumed++
					_ = p // Prevent optimization
				}
			}
		}()

		b.ReportAllocs()
		b.ResetTimer()

		// Producer measurement
		for i := 0; i < b.N; i++ {
			for !r.Push(data) {
			}
		}

		wg.Wait()
	})

	b.Run("go_channel", func(b *testing.B) {
		ch := make(chan [24]byte, 1024)
		data := [24]byte{}

		var wg sync.WaitGroup
		wg.Add(1)

		// Consumer goroutine
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				d := <-ch
				_ = d // Prevent optimization
			}
		}()

		b.ReportAllocs()
		b.ResetTimer()

		// Producer measurement
		for i := 0; i < b.N; i++ {
			ch <- data
		}

		wg.Wait()
	})
}

// ============================================================================
// PERFORMANCE REGRESSION DETECTION
// ============================================================================

// BenchmarkRegression_Core provides automated performance regression detection
func BenchmarkRegression_Core(b *testing.B) {
	r := New(1024)
	data := &[24]byte{}

	// Warmup phase to establish steady-state operation
	for i := 0; i < 1000; i++ {
		r.Push(data)
		r.Pop()
	}

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		r.Push(data)
		p := r.Pop()
		_ = p // Prevent compiler optimization
	}
	elapsed := time.Since(start)

	avgLatency := float64(elapsed.Nanoseconds()) / float64(b.N)
	throughput := float64(b.N) / elapsed.Seconds()

	b.ReportMetric(avgLatency, "ns/op")
	b.ReportMetric(throughput, "ops/sec")

	// Performance thresholds for ISR-grade operation
	if avgLatency > 50 { // 50ns per push+pop pair maximum
		b.Errorf("Performance regression: %.1f ns/op (threshold: 50ns)", avgLatency)
	}
	if throughput < 20_000_000 { // 20M ops/sec minimum throughput
		b.Errorf("Throughput regression: %.0f ops/sec (threshold: 20M)", throughput)
	}
}
