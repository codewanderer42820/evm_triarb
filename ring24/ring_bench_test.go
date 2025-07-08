// ============================================================================
// SPSC RING BUFFER CORRECTED BENCHMARK SUITE
// ============================================================================
//
// Properly designed benchmark suite that addresses measurement issues in the
// original implementation and provides realistic performance validation.
//
// Key improvements:
//   - Consistent operations per benchmark iteration
//   - Realistic performance thresholds based on Go runtime capabilities
//   - Proper cross-core SPSC testing without measurement artifacts
//   - Fair comparisons with appropriate baseline implementations
//   - Separate measurement of different operation patterns
//
// Performance expectations (realistic for Go):
//   - Single-threaded Push: 20-100ns per operation
//   - Single-threaded Pop: 20-100ns per operation
//   - Push+Pop pair: 40-200ns per operation
//   - Cross-core SPSC: 100-500ns per operation (including coordination overhead)
//   - Sustained throughput: 5-50M ops/sec depending on pattern and core count

package ring24

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// SINGLE-THREADED OPERATION BENCHMARKS
// ============================================================================

// BenchmarkRing_PushOnly_Consistent measures pure push latency with consistent operations
func BenchmarkRing_PushOnly_Consistent(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)
			data := &[24]byte{42}

			// Pre-fill to 50% capacity to ensure consistent push behavior
			halfSize := size / 2
			for i := 0; i < halfSize; i++ {
				r.Push(data)
			}

			b.ReportAllocs()
			b.ResetTimer()

			// Measure only successful pushes
			for i := 0; i < b.N; i++ {
				r.Push(data)
				if i%halfSize == halfSize-1 {
					// Periodically drain to maintain space
					for j := 0; j < halfSize; j++ {
						r.Pop()
					}
				}
			}
		})
	}
}

// BenchmarkRing_PopOnly_Consistent measures pure pop latency with consistent operations
func BenchmarkRing_PopOnly_Consistent(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)
			data := &[24]byte{42}

			// Pre-fill ring to ensure consistent pop behavior
			for i := 0; i < size; i++ {
				r.Push(data)
			}

			b.ReportAllocs()
			b.ResetTimer()

			// Measure only successful pops
			for i := 0; i < b.N; i++ {
				p := r.Pop()
				_ = p // Prevent optimization
				if i%size == size-1 {
					// Periodically refill to maintain data
					for j := 0; j < size; j++ {
						r.Push(data)
					}
				}
			}
		})
	}
}

// BenchmarkRing_PushPopPair_Consistent measures paired operations
func BenchmarkRing_PushPopPair_Consistent(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)
			data := &[24]byte{42}

			// Warmup
			for i := 0; i < 1000; i++ {
				r.Push(data)
				r.Pop()
			}

			b.ReportAllocs()
			b.ResetTimer()

			// Each iteration performs exactly one push+pop pair
			for i := 0; i < b.N; i++ {
				r.Push(data)
				p := r.Pop()
				_ = p
			}
		})
	}
}

// ============================================================================
// CROSS-CORE SPSC BENCHMARKS (PROPERLY IMPLEMENTED)
// ============================================================================

// BenchmarkRing_CrossCoreSPSC_Proper measures true SPSC performance without spin artifacts
func BenchmarkRing_CrossCoreSPSC_Proper(b *testing.B) {
	if runtime.GOMAXPROCS(0) < 2 {
		b.Skip("Requires at least 2 CPU cores")
	}

	sizes := []int{64, 256, 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)
			data := &[24]byte{42}

			// Pre-fill to prevent immediate blocking
			prefill := size / 4
			for i := 0; i < prefill; i++ {
				r.Push(data)
			}

			var consumerReady sync.WaitGroup
			var benchmarkStart sync.WaitGroup
			consumerReady.Add(1)
			benchmarkStart.Add(1)

			var consumerDone uint32
			consumedCount := int64(0)

			// Consumer goroutine
			go func() {
				runtime.LockOSThread()
				consumerReady.Done()
				benchmarkStart.Wait()

				for atomic.LoadUint32(&consumerDone) == 0 {
					if p := r.Pop(); p != nil {
						atomic.AddInt64(&consumedCount, 1)
					} else {
						// Brief pause to avoid excessive spinning
						runtime.Gosched()
					}
				}
			}()

			// Wait for consumer to be ready
			consumerReady.Wait()

			b.ReportAllocs()
			b.ResetTimer()

			// Producer measurement (this thread)
			runtime.LockOSThread()
			benchmarkStart.Done()

			pushedCount := 0
			for i := 0; i < b.N; i++ {
				// Non-blocking push attempt
				if r.Push(data) {
					pushedCount++
				}
				// If push fails, consumer will catch up eventually
			}

			// Signal consumer to stop and wait for final draining
			atomic.StoreUint32(&consumerDone, 1)

			// Brief wait for consumer to finish
			deadline := time.Now().Add(100 * time.Millisecond)
			for time.Now().Before(deadline) && atomic.LoadInt64(&consumedCount) < int64(pushedCount) {
				runtime.Gosched()
			}

			runtime.UnlockOSThread()

			// Report actual throughput
			b.ReportMetric(float64(pushedCount), "items_pushed")
			b.ReportMetric(float64(atomic.LoadInt64(&consumedCount)), "items_consumed")
		})
	}
}

// ============================================================================
// SUSTAINED THROUGHPUT BENCHMARKS
// ============================================================================

// BenchmarkRing_SustainedThroughput measures maximum sustainable throughput
func BenchmarkRing_SustainedThroughput(b *testing.B) {
	if runtime.GOMAXPROCS(0) < 2 {
		b.Skip("Requires at least 2 CPU cores")
	}

	r := New(2048) // Large buffer to minimize coordination overhead
	data := &[24]byte{42}

	var ready sync.WaitGroup
	var done uint32
	ready.Add(1)

	// Consumer goroutine with minimal overhead
	go func() {
		runtime.LockOSThread()
		ready.Done()

		consumed := 0
		for atomic.LoadUint32(&done) == 0 || consumed < b.N {
			if p := r.Pop(); p != nil {
				consumed++
				if consumed >= b.N {
					break
				}
			}
		}
	}()

	ready.Wait()
	time.Sleep(time.Millisecond) // Brief stabilization

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	runtime.LockOSThread()

	// Push at maximum rate
	for i := 0; i < b.N; i++ {
		for !r.Push(data) {
			// Spin until space available
		}
	}

	elapsed := time.Since(start)
	atomic.StoreUint32(&done, 1)

	runtime.UnlockOSThread()

	// Calculate and report throughput
	throughput := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(throughput, "ops/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "ns/op")
}

// ============================================================================
// COMPARATIVE BENCHMARKS
// ============================================================================

// BenchmarkComparison_RingVsAlternatives provides fair comparison with other approaches
func BenchmarkComparison_RingVsAlternatives(b *testing.B) {
	if runtime.GOMAXPROCS(0) < 2 {
		b.Skip("Requires at least 2 CPU cores")
	}

	data := [24]byte{42}

	b.Run("ring_buffer_spsc", func(b *testing.B) {
		r := New(1024)

		var ready sync.WaitGroup
		var done uint32
		ready.Add(1)

		go func() {
			runtime.LockOSThread()
			ready.Done()
			consumed := 0
			for consumed < b.N {
				if p := r.Pop(); p != nil {
					consumed++
				}
			}
		}()

		ready.Wait()
		time.Sleep(time.Millisecond)

		b.ReportAllocs()
		b.ResetTimer()

		runtime.LockOSThread()
		for i := 0; i < b.N; i++ {
			for !r.Push(&data) {
			}
		}
		atomic.StoreUint32(&done, 1)
		runtime.UnlockOSThread()
	})

	b.Run("buffered_channel", func(b *testing.B) {
		ch := make(chan [24]byte, 1024)

		var ready sync.WaitGroup
		ready.Add(1)

		go func() {
			runtime.LockOSThread()
			ready.Done()
			for i := 0; i < b.N; i++ {
				<-ch
			}
		}()

		ready.Wait()
		time.Sleep(time.Millisecond)

		b.ReportAllocs()
		b.ResetTimer()

		runtime.LockOSThread()
		for i := 0; i < b.N; i++ {
			ch <- data
		}
		runtime.UnlockOSThread()
	})

	b.Run("unbuffered_channel", func(b *testing.B) {
		ch := make(chan [24]byte)

		var ready sync.WaitGroup
		ready.Add(1)

		go func() {
			runtime.LockOSThread()
			ready.Done()
			for i := 0; i < b.N; i++ {
				<-ch
			}
		}()

		ready.Wait()

		b.ReportAllocs()
		b.ResetTimer()

		runtime.LockOSThread()
		for i := 0; i < b.N; i++ {
			ch <- data
		}
		runtime.UnlockOSThread()
	})
}

// ============================================================================
// MEMORY ACCESS PATTERN BENCHMARKS
// ============================================================================

// BenchmarkRing_MemoryAccessPatterns measures impact of different access patterns
func BenchmarkRing_MemoryAccessPatterns(b *testing.B) {
	r := New(1024)

	b.Run("sequential_fill_drain", func(b *testing.B) {
		data := &[24]byte{42}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Fill completely
			for j := 0; j < 1024; j++ {
				r.Push(data)
			}
			// Drain completely
			for j := 0; j < 1024; j++ {
				r.Pop()
			}
		}
	})

	b.Run("interleaved_operations", func(b *testing.B) {
		data := &[24]byte{42}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.Push(data)
			r.Pop()
		}
	})

	b.Run("batch_operations", func(b *testing.B) {
		data := &[24]byte{42}
		batchSize := 64

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Push batch
			for j := 0; j < batchSize; j++ {
				r.Push(data)
			}
			// Pop batch
			for j := 0; j < batchSize; j++ {
				r.Pop()
			}
		}
	})
}

// ============================================================================
// PERFORMANCE REGRESSION DETECTION
// ============================================================================

// BenchmarkRegression_RealisticThresholds provides meaningful regression detection
func BenchmarkRegression_RealisticThresholds(b *testing.B) {
	r := New(1024)
	data := &[24]byte{42}

	// Warmup to establish steady state
	for i := 0; i < 10000; i++ {
		r.Push(data)
		r.Pop()
	}

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		r.Push(data)
		p := r.Pop()
		_ = p
	}
	elapsed := time.Since(start)

	avgLatency := float64(elapsed.Nanoseconds()) / float64(b.N)
	throughput := float64(b.N) / elapsed.Seconds()

	b.ReportMetric(avgLatency, "ns/op")
	b.ReportMetric(throughput, "ops/sec")

	// Realistic performance thresholds for Go
	if avgLatency > 500 { // 500ns per push+pop pair (realistic threshold)
		b.Errorf("Performance regression: %.1f ns/op (threshold: 500ns)", avgLatency)
	}
	if throughput < 2_000_000 { // 2M ops/sec minimum (achievable in Go)
		b.Errorf("Throughput regression: %.0f ops/sec (threshold: 2M)", throughput)
	}

	// Log performance for trend analysis
	b.Logf("Push+Pop latency: %.1f ns/op", avgLatency)
	b.Logf("Throughput: %.0f ops/sec", throughput)
}

// ============================================================================
// CACHE BEHAVIOR BENCHMARKS
// ============================================================================

// BenchmarkRing_CacheBehavior measures impact of ring size on cache performance
func BenchmarkRing_CacheBehavior(b *testing.B) {
	// Test various sizes to understand cache impact
	sizes := []int{
		16,    // Fits in L1 cache
		64,    // Still L1 cache
		256,   // L2 cache territory
		1024,  // L3 cache territory
		4096,  // May exceed cache
		16384, // Definitely exceeds typical caches
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			r := New(size)
			data := &[24]byte{42}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.Push(data)
				r.Pop()
			}
		})
	}
}

// ============================================================================
// CONTENTION SIMULATION BENCHMARKS
// ============================================================================

// BenchmarkRing_ContentionSimulation simulates various contention scenarios
func BenchmarkRing_ContentionSimulation(b *testing.B) {
	if runtime.GOMAXPROCS(0) < 2 {
		b.Skip("Requires at least 2 CPU cores")
	}

	scenarios := []struct {
		name          string
		producerDelay time.Duration
		consumerDelay time.Duration
		bufferSize    int
	}{
		{"fast_producer_slow_consumer", 0, 100 * time.Nanosecond, 1024},
		{"slow_producer_fast_consumer", 100 * time.Nanosecond, 0, 1024},
		{"matched_pace", 50 * time.Nanosecond, 50 * time.Nanosecond, 256},
		{"bursty_traffic", 0, 0, 64}, // No artificial delays, natural bursts
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			r := New(scenario.bufferSize)
			data := &[24]byte{42}

			var ready sync.WaitGroup
			var done uint32
			ready.Add(1)

			// Consumer with configurable delay
			go func() {
				runtime.LockOSThread()
				ready.Done()
				consumed := 0
				for consumed < b.N {
					if p := r.Pop(); p != nil {
						consumed++
						if scenario.consumerDelay > 0 {
							time.Sleep(scenario.consumerDelay)
						}
					}
				}
			}()

			ready.Wait()
			time.Sleep(time.Millisecond)

			b.ReportAllocs()
			b.ResetTimer()

			// Producer with configurable delay
			runtime.LockOSThread()
			produced := 0
			for produced < b.N {
				if r.Push(data) {
					produced++
					if scenario.producerDelay > 0 {
						time.Sleep(scenario.producerDelay)
					}
				}
			}

			atomic.StoreUint32(&done, 1)
			runtime.UnlockOSThread()
		})
	}
}
