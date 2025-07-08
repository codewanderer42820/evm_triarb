// ============================================================================
// PINNED CONSUMER PERFORMANCE BENCHMARK SUITE
// ============================================================================
//
// Comprehensive performance measurement framework for ISR-grade pinned consumer
// goroutines with emphasis on real-world deployment scenarios.
//
// Benchmark categories:
//   - Callback latency: Handler invocation overhead measurement
//   - Throughput analysis: Sustained processing rates under load
//   - CPU affinity impact: Performance effects of core pinning
//   - Hot/cold transitions: State change performance characteristics
//   - Shutdown latency: Graceful termination timing
//   - Cross-core effects: Cache coherency and NUMA impacts
//   - Backoff behavior: CPU relaxation effectiveness
//   - Memory access patterns: Cache utilization efficiency
//
// Performance targets:
//   - Callback latency: <1μs (optimal), <5μs (acceptable)
//   - Throughput: >1M callbacks/sec sustained
//   - Shutdown latency: <10ms under load
//   - CPU utilization: >95% efficiency during hot periods
//   - Memory bandwidth: Efficient cache line utilization
//
// Test methodology:
//   - Real core pinning with affinity validation
//   - Statistical analysis of timing distributions
//   - Thermal stability under sustained load
//   - Resource utilization monitoring
//   - Comparative analysis across configurations

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
// BENCHMARK CONFIGURATION AND UTILITIES
// ============================================================================

// benchConsumerState holds benchmark execution state
type benchConsumerState struct {
	ring         *Ring
	stop         *uint32
	hot          *uint32
	done         chan struct{}
	callCount    *uint64
	totalLatency *uint64
	maxLatency   *uint64
	startTime    time.Time
}

// newBenchConsumerState creates initialized benchmark state
func newBenchConsumerState(ringSize int) *benchConsumerState {
	return &benchConsumerState{
		ring:         New(ringSize),
		stop:         new(uint32),
		hot:          new(uint32),
		done:         make(chan struct{}),
		callCount:    new(uint64),
		totalLatency: new(uint64),
		maxLatency:   new(uint64),
		startTime:    time.Now(),
	}
}

// latencyTrackingHandler creates a handler that measures callback latency
func (s *benchConsumerState) latencyTrackingHandler() func(*[24]byte) {
	return func(data *[24]byte) {
		start := time.Now()

		// Simulate minimal processing
		_ = data[0] + data[23] // Touch first and last bytes

		latency := time.Since(start)
		atomic.AddUint64(s.callCount, 1)
		atomic.AddUint64(s.totalLatency, uint64(latency.Nanoseconds()))

		// Update max latency (approximate due to races, but good enough for benchmarks)
		for {
			current := atomic.LoadUint64(s.maxLatency)
			if uint64(latency.Nanoseconds()) <= current {
				break
			}
			if atomic.CompareAndSwapUint64(s.maxLatency, current, uint64(latency.Nanoseconds())) {
				break
			}
		}
	}
}

// noopHandler creates a minimal overhead handler
func (s *benchConsumerState) noopHandler() func(*[24]byte) {
	return func(_ *[24]byte) {
		atomic.AddUint64(s.callCount, 1)
	}
}

// processingHandler creates a handler with configurable processing time
func (s *benchConsumerState) processingHandler(processingNanos int) func(*[24]byte) {
	return func(data *[24]byte) {
		start := time.Now()

		// Busy wait for specified duration
		for time.Since(start).Nanoseconds() < int64(processingNanos) {
			_ = data[0] // Keep memory access
		}

		atomic.AddUint64(s.callCount, 1)
	}
}

// launchConsumer starts a pinned consumer for benchmarking
func (s *benchConsumerState) launchConsumer(core int, handler func(*[24]byte)) {
	PinnedConsumer(core, s.ring, s.stop, s.hot, handler, s.done)
}

// launchConsumerWithCooldown starts a pinned consumer with cooldown
func (s *benchConsumerState) launchConsumerWithCooldown(core int, handler func(*[24]byte)) {
	PinnedConsumerWithCooldown(core, s.ring, s.stop, s.hot, handler, s.done)
}

// shutdown gracefully stops the consumer
func (s *benchConsumerState) shutdown() {
	atomic.StoreUint32(s.stop, 1)
	<-s.done
}

// getStats returns performance statistics
func (s *benchConsumerState) getStats() (calls uint64, avgLatencyNs float64, maxLatencyNs uint64) {
	calls = atomic.LoadUint64(s.callCount)
	totalLatency := atomic.LoadUint64(s.totalLatency)
	maxLatencyNs = atomic.LoadUint64(s.maxLatency)

	if calls > 0 {
		avgLatencyNs = float64(totalLatency) / float64(calls)
	}

	return
}

// warmupConsumer performs initial operations to reach steady state
func warmupConsumer(s *benchConsumerState, operations int) {
	atomic.StoreUint32(s.hot, 1)
	for i := 0; i < operations; i++ {
		data := testData(byte(i))
		for !s.ring.Push(data) {
			time.Sleep(time.Microsecond)
		}
	}
	time.Sleep(10 * time.Millisecond) // Allow processing
	atomic.StoreUint32(s.hot, 0)
}

// ============================================================================
// CALLBACK LATENCY BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_CallbackLatency measures handler invocation overhead
func BenchmarkPinnedConsumer_CallbackLatency(b *testing.B) {
	if runtime.GOMAXPROCS(0) < 2 {
		runtime.GOMAXPROCS(2)
	}

	ringSizes := []int{64, 256, 1024}

	for _, ringSize := range ringSizes {
		b.Run(fmt.Sprintf("ring_size_%d", ringSize), func(b *testing.B) {
			s := newBenchConsumerState(ringSize)
			handler := s.latencyTrackingHandler()
			s.launchConsumer(0, handler)

			warmupConsumer(s, 1000)

			b.ReportAllocs()
			b.ResetTimer()

			atomic.StoreUint32(s.hot, 1)

			for i := 0; i < b.N; i++ {
				data := testData(byte(i))
				for !s.ring.Push(data) {
					// Spin until space available
				}
			}

			// Wait for all callbacks to complete
			for atomic.LoadUint64(s.callCount) < uint64(b.N) {
				time.Sleep(time.Microsecond)
			}

			b.StopTimer()
			atomic.StoreUint32(s.hot, 0)
			s.shutdown()

			// Report latency statistics
			calls, avgLatency, maxLatency := s.getStats()
			b.ReportMetric(avgLatency, "ns/callback_avg")
			b.ReportMetric(float64(maxLatency), "ns/callback_max")
			b.ReportMetric(float64(calls), "callbacks")
		})
	}
}

// BenchmarkPinnedConsumer_MinimalLatency measures absolute minimum overhead
func BenchmarkPinnedConsumer_MinimalLatency(b *testing.B) {
	runtime.GOMAXPROCS(2)

	s := newBenchConsumerState(1024)
	handler := s.noopHandler()
	s.launchConsumer(0, handler)

	warmupConsumer(s, 1000)

	b.ReportAllocs()
	b.ResetTimer()

	atomic.StoreUint32(s.hot, 1)
	start := time.Now()

	for i := 0; i < b.N; i++ {
		data := testData(byte(i))
		for !s.ring.Push(data) {
			// Busy wait
		}
	}

	// Wait for completion
	for atomic.LoadUint64(s.callCount) < uint64(b.N) {
		time.Sleep(time.Microsecond)
	}

	elapsed := time.Since(start)
	b.StopTimer()

	s.shutdown()

	avgLatency := float64(elapsed.Nanoseconds()) / float64(b.N)
	b.ReportMetric(avgLatency, "ns/op_total")
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/sec")
}

// BenchmarkPinnedConsumer_ProcessingLatency measures impact of processing time
func BenchmarkPinnedConsumer_ProcessingLatency(b *testing.B) {
	runtime.GOMAXPROCS(2)

	processingTimes := []int{0, 100, 500, 1000, 5000} // nanoseconds

	for _, procTime := range processingTimes {
		b.Run(fmt.Sprintf("processing_%dns", procTime), func(b *testing.B) {
			s := newBenchConsumerState(256)
			handler := s.processingHandler(procTime)
			s.launchConsumer(0, handler)

			warmupConsumer(s, 100)

			b.ReportAllocs()
			b.ResetTimer()

			atomic.StoreUint32(s.hot, 1)

			for i := 0; i < b.N; i++ {
				data := testData(byte(i))
				for !s.ring.Push(data) {
					time.Sleep(time.Microsecond)
				}
			}

			// Wait for completion
			for atomic.LoadUint64(s.callCount) < uint64(b.N) {
				time.Sleep(time.Millisecond)
			}

			b.StopTimer()
			s.shutdown()
		})
	}
}

// ============================================================================
// THROUGHPUT BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_SustainedThroughput measures long-term processing rates
func BenchmarkPinnedConsumer_SustainedThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping sustained throughput test in short mode")
	}

	runtime.GOMAXPROCS(2)

	durations := []time.Duration{
		100 * time.Millisecond,
		1 * time.Second,
		10 * time.Second,
	}

	for _, duration := range durations {
		b.Run(fmt.Sprintf("duration_%v", duration), func(b *testing.B) {
			s := newBenchConsumerState(1024)
			handler := s.noopHandler()
			s.launchConsumer(0, handler)

			warmupConsumer(s, 10000)

			b.ResetTimer()

			atomic.StoreUint32(s.hot, 1)
			start := time.Now()
			operations := 0

			for time.Since(start) < duration {
				for i := 0; i < 1000 && time.Since(start) < duration; i++ {
					data := testData(byte(operations))
					if s.ring.Push(data) {
						operations++
					}
				}
			}

			elapsed := time.Since(start)

			// Wait for final processing
			time.Sleep(100 * time.Millisecond)

			b.StopTimer()
			s.shutdown()

			processed := atomic.LoadUint64(s.callCount)
			throughput := float64(processed) / elapsed.Seconds()
			efficiency := float64(processed) / float64(operations)

			b.ReportMetric(throughput, "callbacks/sec")
			b.ReportMetric(efficiency*100, "efficiency_%")
			b.ReportMetric(float64(operations), "attempted_ops")
			b.ReportMetric(float64(processed), "completed_callbacks")
		})
	}
}

// BenchmarkPinnedConsumer_BurstThroughput measures peak processing rates
func BenchmarkPinnedConsumer_BurstThroughput(b *testing.B) {
	runtime.GOMAXPROCS(2)

	burstSizes := []int{100, 1000, 10000}

	for _, burstSize := range burstSizes {
		b.Run(fmt.Sprintf("burst_%d", burstSize), func(b *testing.B) {
			s := newBenchConsumerState(2048)
			handler := s.noopHandler()
			s.launchConsumer(0, handler)

			warmupConsumer(s, 1000)

			b.ReportAllocs()
			b.ResetTimer()

			for burst := 0; burst < b.N; burst += burstSize {
				currentBurst := burstSize
				if burst+burstSize > b.N {
					currentBurst = b.N - burst
				}

				atomic.StoreUint32(s.hot, 1)

				// Push burst
				for i := 0; i < currentBurst; i++ {
					data := testData(byte(burst + i))
					for !s.ring.Push(data) {
						time.Sleep(time.Microsecond)
					}
				}

				// Wait for burst completion
				target := atomic.LoadUint64(s.callCount) + uint64(currentBurst)
				for atomic.LoadUint64(s.callCount) < target {
					time.Sleep(time.Microsecond)
				}

				atomic.StoreUint32(s.hot, 0)
				time.Sleep(time.Millisecond) // Brief pause between bursts
			}

			b.StopTimer()
			s.shutdown()

			processed := atomic.LoadUint64(s.callCount)
			b.ReportMetric(float64(processed), "total_callbacks")
		})
	}
}

// ============================================================================
// CPU AFFINITY AND CORE PINNING BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_CoreAffinity measures impact of core pinning
func BenchmarkPinnedConsumer_CoreAffinity(b *testing.B) {
	if runtime.NumCPU() < 4 {
		b.Skip("Insufficient CPU cores for affinity testing")
	}

	runtime.GOMAXPROCS(4)

	cores := []int{0, 1, 2, 3}

	for _, core := range cores {
		b.Run(fmt.Sprintf("core_%d", core), func(b *testing.B) {
			s := newBenchConsumerState(512)
			handler := s.noopHandler()
			s.launchConsumer(core, handler)

			warmupConsumer(s, 1000)

			b.ReportAllocs()
			b.ResetTimer()

			atomic.StoreUint32(s.hot, 1)

			for i := 0; i < b.N; i++ {
				data := testData(byte(i))
				for !s.ring.Push(data) {
					time.Sleep(time.Microsecond)
				}
			}

			// Wait for completion
			for atomic.LoadUint64(s.callCount) < uint64(b.N) {
				time.Sleep(time.Microsecond)
			}

			b.StopTimer()
			s.shutdown()
		})
	}
}

// BenchmarkPinnedConsumer_CrossCore measures cross-core performance
func BenchmarkPinnedConsumer_CrossCore(b *testing.B) {
	if runtime.NumCPU() < 2 {
		b.Skip("Insufficient CPU cores for cross-core testing")
	}

	runtime.GOMAXPROCS(2)

	s := newBenchConsumerState(1024)
	handler := s.noopHandler()

	// Pin consumer to core 1
	s.launchConsumer(1, handler)
	warmupConsumer(s, 1000)

	b.ReportAllocs()
	b.ResetTimer()

	// Producer on core 0 (different from consumer)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	atomic.StoreUint32(s.hot, 1)

	for i := 0; i < b.N; i++ {
		data := testData(byte(i))
		for !s.ring.Push(data) {
			// Spin on producer core
		}
	}

	// Wait for completion
	for atomic.LoadUint64(s.callCount) < uint64(b.N) {
		time.Sleep(time.Microsecond)
	}

	b.StopTimer()
	s.shutdown()
}

// ============================================================================
// HOT/COLD STATE TRANSITION BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_HotTransitions measures hot flag responsiveness
func BenchmarkPinnedConsumer_HotTransitions(b *testing.B) {
	runtime.GOMAXPROCS(2)

	s := newBenchConsumerState(256)
	handler := s.noopHandler()
	s.launchConsumer(0, handler)

	warmupConsumer(s, 100)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Set hot flag
		atomic.StoreUint32(s.hot, 1)

		// Push data
		data := testData(byte(i))
		for !s.ring.Push(data) {
			time.Sleep(time.Microsecond)
		}

		// Clear hot flag
		atomic.StoreUint32(s.hot, 0)

		// Brief pause
		time.Sleep(time.Microsecond)
	}

	// Wait for completion
	for atomic.LoadUint64(s.callCount) < uint64(b.N) {
		time.Sleep(time.Microsecond)
	}

	b.StopTimer()
	s.shutdown()
}

// BenchmarkPinnedConsumer_ColdResume measures idle to active transition
func BenchmarkPinnedConsumer_ColdResume(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping cold resume test in short mode")
	}

	runtime.GOMAXPROCS(2)

	idlePeriods := []time.Duration{
		100 * time.Millisecond,
		1 * time.Second,
		5 * time.Second,
	}

	for _, idlePeriod := range idlePeriods {
		b.Run(fmt.Sprintf("idle_%v", idlePeriod), func(b *testing.B) {
			s := newBenchConsumerState(64)
			handler := s.latencyTrackingHandler()
			s.launchConsumer(0, handler)

			// Initial warmup
			warmupConsumer(s, 100)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Idle period
				time.Sleep(idlePeriod)

				// Resume with data
				atomic.StoreUint32(s.hot, 1)
				data := testData(byte(i))

				start := time.Now()
				for !s.ring.Push(data) {
					time.Sleep(time.Microsecond)
				}

				// Wait for callback
				targetCount := atomic.LoadUint64(s.callCount) + 1
				for atomic.LoadUint64(s.callCount) < targetCount {
					time.Sleep(time.Microsecond)
				}

				resumeLatency := time.Since(start)
				b.ReportMetric(float64(resumeLatency.Nanoseconds()), fmt.Sprintf("resume_ns_idle_%v", idlePeriod))

				atomic.StoreUint32(s.hot, 0)
			}

			b.StopTimer()
			s.shutdown()
		})
	}
}

// ============================================================================
// SHUTDOWN PERFORMANCE BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_ShutdownLatency measures termination timing
func BenchmarkPinnedConsumer_ShutdownLatency(b *testing.B) {
	runtime.GOMAXPROCS(2)

	loadLevels := []int{0, 10, 100, 1000} // Number of pending items

	for _, load := range loadLevels {
		b.Run(fmt.Sprintf("load_%d", load), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s := newBenchConsumerState(1024)
				handler := s.noopHandler()
				s.launchConsumer(0, handler)

				// Create load
				atomic.StoreUint32(s.hot, 1)
				for j := 0; j < load; j++ {
					data := testData(byte(j))
					s.ring.Push(data)
				}

				// Measure shutdown time
				start := time.Now()
				atomic.StoreUint32(s.stop, 1)
				<-s.done
				shutdownDuration := time.Since(start)

				b.ReportMetric(float64(shutdownDuration.Nanoseconds()), fmt.Sprintf("shutdown_ns_load_%d", load))
			}
		})
	}
}

// BenchmarkPinnedConsumer_ShutdownUnderLoad measures shutdown with active processing
func BenchmarkPinnedConsumer_ShutdownUnderLoad(b *testing.B) {
	runtime.GOMAXPROCS(2)

	s := newBenchConsumerState(2048)

	// Handler with processing delay
	handler := func(data *[24]byte) {
		time.Sleep(100 * time.Microsecond) // Simulate processing
		atomic.AddUint64(s.callCount, 1)
	}

	s.launchConsumer(0, handler)

	// Create sustained load
	go func() {
		atomic.StoreUint32(s.hot, 1)
		for i := 0; atomic.LoadUint32(s.stop) == 0; i++ {
			data := testData(byte(i))
			s.ring.Push(data)
			time.Sleep(10 * time.Microsecond)
		}
	}()

	// Allow load to build
	time.Sleep(100 * time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		atomic.StoreUint32(s.stop, 1)
		<-s.done
		shutdownDuration := time.Since(start)

		b.ReportMetric(float64(shutdownDuration.Nanoseconds()), "shutdown_under_load_ns")

		// Reset for next iteration
		s = newBenchConsumerState(2048)
		s.launchConsumer(0, handler)
		time.Sleep(50 * time.Millisecond) // Brief setup time
	}
}

// ============================================================================
// COOLDOWN VARIANT BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumerWithCooldown_Performance measures cooldown impact
func BenchmarkPinnedConsumerWithCooldown_Performance(b *testing.B) {
	runtime.GOMAXPROCS(2)

	variants := map[string]func(*benchConsumerState, int, func(*[24]byte)){
		"standard": (*benchConsumerState).launchConsumer,
		"cooldown": (*benchConsumerState).launchConsumerWithCooldown,
	}

	for variant, launcher := range variants {
		b.Run(variant, func(b *testing.B) {
			s := newBenchConsumerState(512)
			handler := s.noopHandler()
			launcher(s, 0, handler)

			warmupConsumer(s, 1000)

			b.ReportAllocs()
			b.ResetTimer()

			atomic.StoreUint32(s.hot, 1)

			for i := 0; i < b.N; i++ {
				data := testData(byte(i))
				for !s.ring.Push(data) {
					time.Sleep(time.Microsecond)
				}
			}

			// Wait for completion
			for atomic.LoadUint64(s.callCount) < uint64(b.N) {
				time.Sleep(time.Microsecond)
			}

			b.StopTimer()
			s.shutdown()
		})
	}
}

// ============================================================================
// MEMORY ACCESS PATTERN BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_CacheEfficiency measures memory access patterns
func BenchmarkPinnedConsumer_CacheEfficiency(b *testing.B) {
	runtime.GOMAXPROCS(2)

	// Different access patterns in handler
	accessPatterns := map[string]func(*[24]byte){
		"sequential": func(data *[24]byte) {
			sum := byte(0)
			for i := 0; i < 24; i++ {
				sum += data[i]
			}
			_ = sum
		},
		"random": func(data *[24]byte) {
			indices := []int{0, 5, 12, 23, 7, 19, 3, 15}
			sum := byte(0)
			for _, i := range indices {
				sum += data[i]
			}
			_ = sum
		},
		"minimal": func(data *[24]byte) {
			_ = data[0] + data[23] // Touch only endpoints
		},
	}

	for pattern, accessFunc := range accessPatterns {
		b.Run(pattern, func(b *testing.B) {
			s := newBenchConsumerState(1024)

			handler := func(data *[24]byte) {
				accessFunc(data)
				atomic.AddUint64(s.callCount, 1)
			}

			s.launchConsumer(0, handler)
			warmupConsumer(s, 1000)

			b.ReportAllocs()
			b.SetBytes(24) // Bytes per operation
			b.ResetTimer()

			atomic.StoreUint32(s.hot, 1)

			for i := 0; i < b.N; i++ {
				data := testData(byte(i))
				for !s.ring.Push(data) {
					time.Sleep(time.Microsecond)
				}
			}

			// Wait for completion
			for atomic.LoadUint64(s.callCount) < uint64(b.N) {
				time.Sleep(time.Microsecond)
			}

			b.StopTimer()
			s.shutdown()
		})
	}
}

// ============================================================================
// SCALING AND RESOURCE UTILIZATION BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_ResourceScaling measures performance scaling
func BenchmarkPinnedConsumer_ResourceScaling(b *testing.B) {
	maxCores := runtime.NumCPU()
	if maxCores > 8 {
		maxCores = 8 // Limit for practical testing
	}

	for cores := 1; cores <= maxCores; cores++ {
		b.Run(fmt.Sprintf("cores_%d", cores), func(b *testing.B) {
			runtime.GOMAXPROCS(cores)

			s := newBenchConsumerState(1024)
			handler := s.noopHandler()

			// Pin consumer to last available core
			consumerCore := cores - 1
			s.launchConsumer(consumerCore, handler)

			warmupConsumer(s, 1000)

			b.ReportAllocs()
			b.ResetTimer()

			atomic.StoreUint32(s.hot, 1)

			for i := 0; i < b.N; i++ {
				data := testData(byte(i))
				for !s.ring.Push(data) {
					time.Sleep(time.Microsecond)
				}
			}

			// Wait for completion
			for atomic.LoadUint64(s.callCount) < uint64(b.N) {
				time.Sleep(time.Microsecond)
			}

			b.StopTimer()
			s.shutdown()

			processed := atomic.LoadUint64(s.callCount)
			b.ReportMetric(float64(processed), "callbacks_completed")
		})
	}
}

// BenchmarkPinnedConsumer_MultipleConsumers measures multiple consumer performance
func BenchmarkPinnedConsumer_MultipleConsumers(b *testing.B) {
	if runtime.NumCPU() < 4 {
		b.Skip("Insufficient cores for multiple consumer testing")
	}

	runtime.GOMAXPROCS(4)

	consumerCounts := []int{1, 2, 3, 4}

	for _, numConsumers := range consumerCounts {
		b.Run(fmt.Sprintf("consumers_%d", numConsumers), func(b *testing.B) {
			// Create separate ring and consumer for each
			consumers := make([]*benchConsumerState, numConsumers)

			for i := 0; i < numConsumers; i++ {
				consumers[i] = newBenchConsumerState(512)
				handler := consumers[i].noopHandler()
				consumers[i].launchConsumer(i, handler)
				warmupConsumer(consumers[i], 100)
			}

			b.ReportAllocs()
			b.ResetTimer()

			opsPerConsumer := b.N / numConsumers
			var wg sync.WaitGroup

			for i := 0; i < numConsumers; i++ {
				wg.Add(1)
				go func(consumerIdx int) {
					defer wg.Done()

					s := consumers[consumerIdx]
					atomic.StoreUint32(s.hot, 1)

					for j := 0; j < opsPerConsumer; j++ {
						data := testData(byte(consumerIdx*256 + j))
						for !s.ring.Push(data) {
							time.Sleep(time.Microsecond)
						}
					}

					// Wait for this consumer's completion
					target := uint64(opsPerConsumer)
					for atomic.LoadUint64(s.callCount) < target {
						time.Sleep(time.Microsecond)
					}
				}(i)
			}

			wg.Wait()
			b.StopTimer()

			// Shutdown all consumers
			totalProcessed := uint64(0)
			for i := 0; i < numConsumers; i++ {
				consumers[i].shutdown()
				totalProcessed += atomic.LoadUint64(consumers[i].callCount)
			}

			b.ReportMetric(float64(totalProcessed), "total_callbacks")
			b.ReportMetric(float64(totalProcessed)/float64(numConsumers), "avg_callbacks_per_consumer")
		})
	}
}

// ============================================================================
// STRESS AND STABILITY BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_StressTest measures performance under extreme load
func BenchmarkPinnedConsumer_StressTest(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping stress test in short mode")
	}

	runtime.GOMAXPROCS(2)

	s := newBenchConsumerState(4096) // Large ring for stress

	// Handler with variable processing time
	handler := func(data *[24]byte) {
		// Variable delay based on data content
		delay := time.Duration(data[0]%10) * time.Microsecond
		if delay > 0 {
			time.Sleep(delay)
		}
		atomic.AddUint64(s.callCount, 1)
	}

	s.launchConsumer(0, handler)
	warmupConsumer(s, 1000)

	b.ReportAllocs()
	b.ResetTimer()

	// Multiple producer goroutines for stress
	var producerWg sync.WaitGroup
	numProducers := 4
	opsPerProducer := b.N / numProducers

	atomic.StoreUint32(s.hot, 1)

	for p := 0; p < numProducers; p++ {
		producerWg.Add(1)
		go func(producerID int) {
			defer producerWg.Done()

			for i := 0; i < opsPerProducer; i++ {
				data := testData(byte(producerID*64 + i%256))

				retries := 0
				for !s.ring.Push(data) {
					retries++
					if retries > 1000 {
						time.Sleep(time.Microsecond)
						retries = 0
					}
				}
			}
		}(p)
	}

	producerWg.Wait()

	// Wait for all processing to complete
	expectedCallbacks := uint64(numProducers * opsPerProducer)
	for atomic.LoadUint64(s.callCount) < expectedCallbacks {
		time.Sleep(time.Millisecond)
	}

	b.StopTimer()
	s.shutdown()

	processed := atomic.LoadUint64(s.callCount)
	b.ReportMetric(float64(processed), "callbacks_under_stress")
}

// BenchmarkPinnedConsumer_MemoryPressure measures performance under GC pressure
func BenchmarkPinnedConsumer_MemoryPressure(b *testing.B) {
	runtime.GOMAXPROCS(3) // Consumer + Producer + GC pressure

	s := newBenchConsumerState(1024)
	handler := s.noopHandler()
	s.launchConsumer(0, handler)

	// Create memory pressure
	stopGC := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopGC:
				return
			default:
				// Allocate and release memory rapidly
				data := make([]byte, 1024*1024) // 1MB
				_ = data
				runtime.GC()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	warmupConsumer(s, 1000)

	b.ReportAllocs()
	b.ResetTimer()

	atomic.StoreUint32(s.hot, 1)

	for i := 0; i < b.N; i++ {
		data := testData(byte(i))
		for !s.ring.Push(data) {
			time.Sleep(time.Microsecond)
		}
	}

	// Wait for completion
	for atomic.LoadUint64(s.callCount) < uint64(b.N) {
		time.Sleep(time.Microsecond)
	}

	b.StopTimer()
	close(stopGC)
	s.shutdown()
}

// ============================================================================
// PLATFORM-SPECIFIC BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_CpuRelaxImpact measures CPU relaxation effectiveness
func BenchmarkPinnedConsumer_CpuRelaxImpact(b *testing.B) {
	runtime.GOMAXPROCS(2)

	scenarios := map[string]time.Duration{
		"no_idle":     0,
		"short_idle":  time.Millisecond,
		"medium_idle": 10 * time.Millisecond,
		"long_idle":   100 * time.Millisecond,
	}

	for scenario, idleTime := range scenarios {
		b.Run(scenario, func(b *testing.B) {
			s := newBenchConsumerState(256)
			handler := s.noopHandler()
			s.launchConsumer(0, handler)

			warmupConsumer(s, 100)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if idleTime > 0 {
					time.Sleep(idleTime)
				}

				atomic.StoreUint32(s.hot, 1)
				data := testData(byte(i))

				start := time.Now()
				for !s.ring.Push(data) {
					time.Sleep(time.Microsecond)
				}

				// Wait for callback
				target := atomic.LoadUint64(s.callCount) + 1
				for atomic.LoadUint64(s.callCount) < target {
					time.Sleep(time.Microsecond)
				}

				responseTime := time.Since(start)
				b.ReportMetric(float64(responseTime.Nanoseconds()), fmt.Sprintf("response_ns_%s", scenario))

				atomic.StoreUint32(s.hot, 0)
			}

			b.StopTimer()
			s.shutdown()
		})
	}
}

// BenchmarkPinnedConsumer_AtomicOperations measures atomic operation overhead
func BenchmarkPinnedConsumer_AtomicOperations(b *testing.B) {
	runtime.GOMAXPROCS(2)

	s := newBenchConsumerState(512)

	// Handler that performs various atomic operations
	atomicCounter := new(uint64)
	handler := func(data *[24]byte) {
		// Simulate various atomic operations typical in ISR contexts
		atomic.AddUint64(atomicCounter, 1)
		atomic.LoadUint64(atomicCounter)
		atomic.StoreUint64(atomicCounter, atomic.LoadUint64(atomicCounter))
		atomic.AddUint64(s.callCount, 1)
	}

	s.launchConsumer(0, handler)
	warmupConsumer(s, 1000)

	b.ReportAllocs()
	b.ResetTimer()

	atomic.StoreUint32(s.hot, 1)

	for i := 0; i < b.N; i++ {
		data := testData(byte(i))
		for !s.ring.Push(data) {
			time.Sleep(time.Microsecond)
		}
	}

	// Wait for completion
	for atomic.LoadUint64(s.callCount) < uint64(b.N) {
		time.Sleep(time.Microsecond)
	}

	b.StopTimer()
	s.shutdown()

	finalCounter := atomic.LoadUint64(atomicCounter)
	b.ReportMetric(float64(finalCounter), "atomic_operations")
}

// ============================================================================
// COMPARATIVE BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_vsChannel compares with Go channel performance
func BenchmarkPinnedConsumer_vsChannel(b *testing.B) {
	runtime.GOMAXPROCS(2)

	b.Run("pinned_consumer", func(b *testing.B) {
		s := newBenchConsumerState(1024)
		handler := s.noopHandler()
		s.launchConsumer(0, handler)

		warmupConsumer(s, 1000)

		b.ReportAllocs()
		b.ResetTimer()

		atomic.StoreUint32(s.hot, 1)

		for i := 0; i < b.N; i++ {
			data := testData(byte(i))
			for !s.ring.Push(data) {
				time.Sleep(time.Microsecond)
			}
		}

		for atomic.LoadUint64(s.callCount) < uint64(b.N) {
			time.Sleep(time.Microsecond)
		}

		b.StopTimer()
		s.shutdown()
	})

	b.Run("go_channel", func(b *testing.B) {
		ch := make(chan [24]byte, 1024)
		var callCount uint64
		done := make(chan struct{})

		// Consumer goroutine
		go func() {
			for data := range ch {
				_ = data
				atomic.AddUint64(&callCount, 1)
			}
			close(done)
		}()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			data := testData(byte(i))
			ch <- *data
		}

		close(ch)
		<-done

		b.StopTimer()

		processed := atomic.LoadUint64(&callCount)
		b.ReportMetric(float64(processed), "channel_callbacks")
	})
}

// BenchmarkPinnedConsumer_vsSync compares with sync-based approaches
func BenchmarkPinnedConsumer_vsSync(b *testing.B) {
	runtime.GOMAXPROCS(2)

	b.Run("pinned_consumer", func(b *testing.B) {
		s := newBenchConsumerState(1024)
		handler := s.noopHandler()
		s.launchConsumer(0, handler)

		warmupConsumer(s, 1000)

		b.ReportAllocs()
		b.ResetTimer()

		atomic.StoreUint32(s.hot, 1)

		for i := 0; i < b.N; i++ {
			data := testData(byte(i))
			for !s.ring.Push(data) {
				time.Sleep(time.Microsecond)
			}
		}

		for atomic.LoadUint64(s.callCount) < uint64(b.N) {
			time.Sleep(time.Microsecond)
		}

		b.StopTimer()
		s.shutdown()
	})

	b.Run("mutex_queue", func(b *testing.B) {
		var mu sync.Mutex
		var queue [][24]byte
		var callCount uint64
		done := make(chan struct{})

		// Consumer goroutine
		go func() {
			for {
				mu.Lock()
				if len(queue) == 0 {
					mu.Unlock()
					select {
					case <-done:
						return
					default:
						time.Sleep(time.Microsecond)
						continue
					}
				}

				data := queue[0]
				queue = queue[1:]
				mu.Unlock()

				_ = data
				atomic.AddUint64(&callCount, 1)
			}
		}()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			data := testData(byte(i))

			mu.Lock()
			queue = append(queue, *data)
			mu.Unlock()
		}

		// Wait for completion
		for atomic.LoadUint64(&callCount) < uint64(b.N) {
			time.Sleep(time.Microsecond)
		}

		close(done)
		b.StopTimer()

		processed := atomic.LoadUint64(&callCount)
		b.ReportMetric(float64(processed), "mutex_callbacks")
	})
}

// ============================================================================
// REGRESSION AND VALIDATION BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_Regression validates performance doesn't degrade
func BenchmarkPinnedConsumer_Regression(b *testing.B) {
	runtime.GOMAXPROCS(2)

	// This benchmark serves as a performance regression detector
	s := newBenchConsumerState(1024)
	handler := s.latencyTrackingHandler()
	s.launchConsumer(0, handler)

	warmupConsumer(s, 10000)

	b.ReportAllocs()
	b.ResetTimer()

	atomic.StoreUint32(s.hot, 1)
	start := time.Now()

	for i := 0; i < b.N; i++ {
		data := testData(byte(i))
		for !s.ring.Push(data) {
			time.Sleep(time.Microsecond)
		}
	}

	// Wait for completion
	for atomic.LoadUint64(s.callCount) < uint64(b.N) {
		time.Sleep(time.Microsecond)
	}

	elapsed := time.Since(start)
	b.StopTimer()
	s.shutdown()

	// Report comprehensive metrics for regression analysis
	calls, avgLatency, maxLatency := s.getStats()
	throughput := float64(calls) / elapsed.Seconds()

	b.ReportMetric(throughput, "callbacks/sec")
	b.ReportMetric(avgLatency, "ns/callback_avg")
	b.ReportMetric(float64(maxLatency), "ns/callback_max")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "ns/op_total")

	// Performance thresholds for regression detection
	if throughput < 500000 { // 500K callbacks/sec minimum
		b.Errorf("Throughput regression: %.0f callbacks/sec", throughput)
	}
	if avgLatency > 5000 { // 5μs average latency maximum
		b.Errorf("Latency regression: %.0f ns average", avgLatency)
	}
}

// BenchmarkPinnedConsumer_WorstCase measures performance under adverse conditions
func BenchmarkPinnedConsumer_WorstCase(b *testing.B) {
	runtime.GOMAXPROCS(2)

	// Small ring with slow processing to create worst-case conditions
	s := newBenchConsumerState(4)

	handler := func(data *[24]byte) {
		// Slow processing
		time.Sleep(100 * time.Microsecond)
		atomic.AddUint64(s.callCount, 1)
	}

	s.launchConsumer(0, handler)

	b.ReportAllocs()
	b.ResetTimer()

	atomic.StoreUint32(s.hot, 1)

	for i := 0; i < b.N; i++ {
		data := testData(byte(i))

		// Force maximum retry cycles due to ring being full
		retries := 0
		for !s.ring.Push(data) {
			retries++
			if retries > 1000 {
				time.Sleep(time.Microsecond)
				retries = 0
			}
		}
	}

	// Wait for completion
	for atomic.LoadUint64(s.callCount) < uint64(b.N) {
		time.Sleep(time.Millisecond)
	}

	b.StopTimer()
	s.shutdown()

	processed := atomic.LoadUint64(s.callCount)
	b.ReportMetric(float64(processed), "worst_case_callbacks")
}

// ============================================================================
// PROFILING AND ANALYSIS HELPERS
// ============================================================================

// BenchmarkPinnedConsumer_ProfilerFriendly provides a long-running benchmark for profiling
func BenchmarkPinnedConsumer_ProfilerFriendly(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping profiler-friendly test in short mode")
	}

	runtime.GOMAXPROCS(2)

	s := newBenchConsumerState(1024)

	// Realistic handler for profiling
	handler := func(data *[24]byte) {
		// Simulate real processing work
		checksum := uint32(0)
		for i := 0; i < 24; i++ {
			checksum = checksum*31 + uint32(data[i])
		}

		// Store result to prevent optimization
		sink = checksum
		atomic.AddUint64(s.callCount, 1)
	}

	s.launchConsumer(0, handler)
	warmupConsumer(s, 10000)

	b.ReportAllocs()
	b.ResetTimer()

	atomic.StoreUint32(s.hot, 1)

	// Run for extended period for profiling
	duration := 10 * time.Second
	start := time.Now()
	operations := 0

	for time.Since(start) < duration {
		data := testData(byte(operations))
		if s.ring.Push(data) {
			operations++
		} else {
			time.Sleep(time.Microsecond)
		}

		// Periodic progress check
		if operations%10000 == 0 {
			runtime.Gosched()
		}
	}

	elapsed := time.Since(start)

	// Wait for final processing
	time.Sleep(100 * time.Millisecond)

	b.StopTimer()
	s.shutdown()

	processed := atomic.LoadUint64(s.callCount)
	throughput := float64(processed) / elapsed.Seconds()

	b.ReportMetric(throughput, "profiler_throughput")
	b.ReportMetric(float64(operations), "total_operations")
	b.ReportMetric(float64(processed), "total_callbacks")

	runtime.KeepAlive(sink)
}
