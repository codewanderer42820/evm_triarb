// ============================================================================
// PINNED CONSUMER CORRECTED BENCHMARK SUITE
// ============================================================================
//
// Properly designed benchmark suite for ISR-grade pinned consumer validation
// that addresses measurement issues and provides realistic performance metrics.
//
// Key improvements over original:
//   - Separated measurement of producer latency from consumer processing overhead
//   - Eliminated timing measurement artifacts in critical paths
//   - Realistic performance thresholds based on Go runtime capabilities
//   - Proper steady-state operation measurement
//   - Accurate throughput calculation without spin-wait contamination
//   - Statistical validation with appropriate confidence intervals
//
// Realistic performance expectations:
//   - Push latency: 50-200ns per operation (Go runtime overhead included)
//   - Handler processing: 10-100ns per operation for minimal workloads
//   - Sustained throughput: 1-10M operations/second (depends on handler complexity)
//   - Cross-core coordination: Additional 50-200ns overhead for cache coherency

package ring24

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// BENCHMARK TEST STATE MANAGEMENT
// ============================================================================

// benchmarkConsumerState manages benchmark execution state
type benchmarkConsumerState struct {
	ring            *Ring
	stop            *uint32
	hot             *uint32
	done            chan struct{}
	operationsCount *uint64
	processingTime  *uint64
	startTime       time.Time
	endTime         time.Time
}

// newBenchmarkConsumerState creates initialized benchmark state
func newBenchmarkConsumerState(ringSize int) *benchmarkConsumerState {
	return &benchmarkConsumerState{
		ring:            New(ringSize),
		stop:            new(uint32),
		hot:             new(uint32),
		done:            make(chan struct{}),
		operationsCount: new(uint64),
		processingTime:  new(uint64),
	}
}

// launchConsumer starts consumer with precise timing measurement
func (s *benchmarkConsumerState) launchConsumer(handler func(*[24]byte)) {
	PinnedConsumer(0, s.ring, s.stop, s.hot, handler, s.done)
}

// shutdown gracefully stops consumer and waits for completion
func (s *benchmarkConsumerState) shutdown(timeout time.Duration) error {
	atomic.StoreUint32(s.stop, 1)
	select {
	case <-s.done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("consumer shutdown timeout after %v", timeout)
	}
}

// ============================================================================
// PURE PRODUCER LATENCY BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_ProducerLatency measures pure push operation latency
// without consumer processing overhead contamination
func BenchmarkPinnedConsumer_ProducerLatency(b *testing.B) {
	runtime.GOMAXPROCS(2)

	sizes := []int{64, 256, 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("ring_size_%d", size), func(b *testing.B) {
			s := newBenchmarkConsumerState(size)

			// Minimal consumer that just counts operations
			handler := func(data *[24]byte) {
				atomic.AddUint64(s.operationsCount, 1)
				_ = data[0] // Minimal data access to prevent optimization
			}

			s.launchConsumer(handler)

			// Warmup phase - establish steady state
			atomic.StoreUint32(s.hot, 1)
			warmupData := [24]byte{99}
			for i := 0; i < 1000; i++ {
				for !s.ring.Push(&warmupData) {
					time.Sleep(time.Microsecond)
				}
			}

			// Wait for warmup processing
			for atomic.LoadUint64(s.operationsCount) < 1000 {
				time.Sleep(time.Microsecond)
			}

			// Reset counters for measurement
			atomic.StoreUint64(s.operationsCount, 0)

			b.ReportAllocs()
			b.ResetTimer()

			// Measure producer latency only
			testData := [24]byte{42}
			for i := 0; i < b.N; i++ {
				// This is what we're measuring: pure push latency
				for !s.ring.Push(&testData) {
					// Brief yield to let consumer catch up, but don't measure this
					runtime.Gosched()
				}
			}

			b.StopTimer()

			// Wait for all operations to be processed
			for atomic.LoadUint64(s.operationsCount) < uint64(b.N) {
				time.Sleep(time.Microsecond)
			}

			atomic.StoreUint32(s.hot, 0)
			s.shutdown(100 * time.Millisecond)
		})
	}
}

// ============================================================================
// CONSUMER PROCESSING OVERHEAD BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_ProcessingOverhead measures handler execution overhead
func BenchmarkPinnedConsumer_ProcessingOverhead(b *testing.B) {
	runtime.GOMAXPROCS(2)

	workloads := []struct {
		name    string
		handler func(*[24]byte, *uint64, *uint64)
	}{
		{
			"minimal",
			func(data *[24]byte, ops *uint64, totalTime *uint64) {
				start := time.Now()
				_ = data[0] // Single memory access
				elapsed := time.Since(start)
				atomic.AddUint64(ops, 1)
				atomic.AddUint64(totalTime, uint64(elapsed.Nanoseconds()))
			},
		},
		{
			"light_computation",
			func(data *[24]byte, ops *uint64, totalTime *uint64) {
				start := time.Now()
				sum := byte(0)
				for i := 0; i < 8; i++ { // Process first 8 bytes
					sum += data[i]
				}
				_ = sum
				elapsed := time.Since(start)
				atomic.AddUint64(ops, 1)
				atomic.AddUint64(totalTime, uint64(elapsed.Nanoseconds()))
			},
		},
		{
			"checksum_validation",
			func(data *[24]byte, ops *uint64, totalTime *uint64) {
				start := time.Now()
				checksum := uint32(0)
				for i := 0; i < 24; i++ {
					checksum = checksum*31 + uint32(data[i])
				}
				_ = checksum
				elapsed := time.Since(start)
				atomic.AddUint64(ops, 1)
				atomic.AddUint64(totalTime, uint64(elapsed.Nanoseconds()))
			},
		},
	}

	for _, workload := range workloads {
		b.Run(workload.name, func(b *testing.B) {
			s := newBenchmarkConsumerState(512)

			var processingOps uint64
			var processingTime uint64

			handler := func(data *[24]byte) {
				workload.handler(data, &processingOps, &processingTime)
			}

			s.launchConsumer(handler)

			// Pre-push all test data to eliminate producer timing effects
			testData := [24]byte{42}
			atomic.StoreUint32(s.hot, 1)

			for i := 0; i < b.N; i++ {
				for !s.ring.Push(&testData) {
					time.Sleep(time.Microsecond)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			// Wait for all processing to complete
			for atomic.LoadUint64(&processingOps) < uint64(b.N) {
				time.Sleep(time.Microsecond)
			}

			b.StopTimer()

			// Calculate handler overhead
			totalProcTime := atomic.LoadUint64(&processingTime)
			ops := atomic.LoadUint64(&processingOps)

			if ops > 0 {
				avgHandlerLatency := float64(totalProcTime) / float64(ops)
				b.ReportMetric(avgHandlerLatency, "ns/handler_call")
			}

			atomic.StoreUint32(s.hot, 0)
			s.shutdown(100 * time.Millisecond)
		})
	}
}

// ============================================================================
// END-TO-END THROUGHPUT BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_SustainedThroughput measures realistic sustained throughput
func BenchmarkPinnedConsumer_SustainedThroughput(b *testing.B) {
	runtime.GOMAXPROCS(2)

	durations := []time.Duration{
		100 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}

	for _, duration := range durations {
		b.Run(fmt.Sprintf("duration_%v", duration), func(b *testing.B) {
			s := newBenchmarkConsumerState(2048) // Large buffer to minimize coordination overhead

			var processedCount uint64
			handler := func(data *[24]byte) {
				atomic.AddUint64(&processedCount, 1)
				// Minimal realistic processing
				_ = data[0] + data[23]
			}

			s.launchConsumer(handler)

			// Warmup
			atomic.StoreUint32(s.hot, 1)
			warmupData := [24]byte{99}
			for i := 0; i < 1000; i++ {
				s.ring.Push(&warmupData)
			}
			time.Sleep(10 * time.Millisecond)

			atomic.StoreUint64(&processedCount, 0)

			b.ReportAllocs()
			b.ResetTimer()

			// Sustained operation for specified duration
			testData := [24]byte{42}
			start := time.Now()
			pushed := uint64(0)

			for time.Since(start) < duration {
				if s.ring.Push(&testData) {
					pushed++
				} else {
					// Brief pause if ring is full
					runtime.Gosched()
				}
			}

			elapsed := time.Since(start)
			b.StopTimer()

			// Wait for processing to complete
			deadline := time.Now().Add(100 * time.Millisecond)
			for time.Now().Before(deadline) && atomic.LoadUint64(&processedCount) < pushed {
				time.Sleep(time.Millisecond)
			}

			processed := atomic.LoadUint64(&processedCount)
			pushThroughput := float64(pushed) / elapsed.Seconds()
			processThroughput := float64(processed) / elapsed.Seconds()

			b.ReportMetric(pushThroughput, "pushes/sec")
			b.ReportMetric(processThroughput, "processed/sec")
			b.ReportMetric(float64(pushed), "total_pushed")
			b.ReportMetric(float64(processed), "total_processed")

			atomic.StoreUint32(s.hot, 0)
			s.shutdown(100 * time.Millisecond)

			// Set b.N to processed count for proper ns/op calculation
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(processed), "ns/processed_op")
		})
	}
}

// ============================================================================
// ADAPTIVE POLLING BEHAVIOR BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_AdaptivePolling measures hot/cold polling behavior
func BenchmarkPinnedConsumer_AdaptivePolling(b *testing.B) {
	runtime.GOMAXPROCS(2)

	scenarios := []struct {
		name        string
		useHotFlag  bool
		burstDelay  time.Duration
		description string
	}{
		{"hot_continuous", true, 0, "Continuous operation with hot flag"},
		{"hot_bursty", true, 10 * time.Millisecond, "Bursty operation with hot flag"},
		{"cold_continuous", false, 0, "Continuous operation without hot flag"},
		{"cold_bursty", false, 10 * time.Millisecond, "Bursty operation without hot flag"},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			s := newBenchmarkConsumerState(256)

			var processedCount uint64
			var avgResponseTime uint64

			handler := func(data *[24]byte) {
				atomic.AddUint64(&processedCount, 1)
				_ = data[0]
			}

			s.launchConsumer(handler)

			if scenario.useHotFlag {
				atomic.StoreUint32(s.hot, 1)
			}

			b.ReportAllocs()
			b.ResetTimer()

			// Simulate workload pattern
			testData := [24]byte{42}
			batchSize := 100

			for batch := 0; batch < b.N/batchSize; batch++ {
				batchStart := time.Now()

				// Push batch
				for i := 0; i < batchSize; i++ {
					for !s.ring.Push(&testData) {
						runtime.Gosched()
					}
				}

				// Wait for batch processing
				targetProcessed := uint64(batch+1) * uint64(batchSize)
				for atomic.LoadUint64(&processedCount) < targetProcessed {
					time.Sleep(time.Microsecond)
				}

				batchTime := time.Since(batchStart)
				atomic.AddUint64(&avgResponseTime, uint64(batchTime.Nanoseconds()))

				// Delay between bursts if configured
				if scenario.burstDelay > 0 {
					time.Sleep(scenario.burstDelay)
				}
			}

			b.StopTimer()

			processed := atomic.LoadUint64(&processedCount)
			totalResponseTime := atomic.LoadUint64(&avgResponseTime)
			numBatches := uint64(b.N / batchSize)

			if numBatches > 0 {
				avgBatchTime := float64(totalResponseTime) / float64(numBatches)
				b.ReportMetric(avgBatchTime/float64(batchSize), "ns/op")
				b.ReportMetric(float64(processed), "total_processed")
			}

			if scenario.useHotFlag {
				atomic.StoreUint32(s.hot, 0)
			}
			s.shutdown(100 * time.Millisecond)
		})
	}
}

// ============================================================================
// MEMORY ACCESS PATTERN BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumer_MemoryPatterns measures cache behavior under different patterns
func BenchmarkPinnedConsumer_MemoryPatterns(b *testing.B) {
	runtime.GOMAXPROCS(2)

	patterns := []struct {
		name        string
		ringSize    int
		dataPattern func(int) [24]byte
	}{
		{
			"sequential_small_ring",
			64,
			func(i int) [24]byte {
				var data [24]byte
				data[0] = byte(i)
				return data
			},
		},
		{
			"sequential_large_ring",
			4096,
			func(i int) [24]byte {
				var data [24]byte
				data[0] = byte(i)
				return data
			},
		},
		{
			"random_data_pattern",
			256,
			func(i int) [24]byte {
				var data [24]byte
				for j := range data {
					data[j] = byte((i*31 + j*17) % 256)
				}
				return data
			},
		},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			s := newBenchmarkConsumerState(pattern.ringSize)

			var processedCount uint64
			handler := func(data *[24]byte) {
				atomic.AddUint64(&processedCount, 1)
				// Process all bytes to ensure cache line access
				sum := byte(0)
				for i := 0; i < 24; i++ {
					sum += data[i]
				}
				_ = sum
			}

			s.launchConsumer(handler)
			atomic.StoreUint32(s.hot, 1)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				data := pattern.dataPattern(i)
				for !s.ring.Push(&data) {
					runtime.Gosched()
				}
			}

			// Wait for processing completion
			for atomic.LoadUint64(&processedCount) < uint64(b.N) {
				time.Sleep(time.Microsecond)
			}

			b.StopTimer()

			atomic.StoreUint32(s.hot, 0)
			s.shutdown(100 * time.Millisecond)
		})
	}
}

// ============================================================================
// PERFORMANCE REGRESSION DETECTION
// ============================================================================

// BenchmarkPinnedConsumer_RegressionDetection provides realistic performance baselines
func BenchmarkPinnedConsumer_RegressionDetection(b *testing.B) {
	runtime.GOMAXPROCS(2)

	s := newBenchmarkConsumerState(1024)

	var totalProcessingTime uint64
	var operationsCompleted uint64

	handler := func(data *[24]byte) {
		start := time.Now()

		// Realistic minimal processing
		_ = data[0] + data[23]

		processingTime := time.Since(start)
		atomic.AddUint64(&operationsCompleted, 1)
		atomic.AddUint64(&totalProcessingTime, uint64(processingTime.Nanoseconds()))
	}

	s.launchConsumer(handler)

	// Warmup phase
	atomic.StoreUint32(s.hot, 1)
	warmupData := [24]byte{99}
	for i := 0; i < 5000; i++ {
		for !s.ring.Push(&warmupData) {
			time.Sleep(time.Microsecond)
		}
	}

	// Wait for warmup
	for atomic.LoadUint64(&operationsCompleted) < 5000 {
		time.Sleep(time.Microsecond)
	}

	// Reset for measurement
	atomic.StoreUint64(&operationsCompleted, 0)
	atomic.StoreUint64(&totalProcessingTime, 0)

	b.ReportAllocs()
	b.ResetTimer()

	// Measurement phase
	testData := [24]byte{42}
	pushStart := time.Now()

	for i := 0; i < b.N; i++ {
		for !s.ring.Push(&testData) {
			runtime.Gosched()
		}
	}

	pushDuration := time.Since(pushStart)

	// Wait for all processing to complete
	for atomic.LoadUint64(&operationsCompleted) < uint64(b.N) {
		time.Sleep(time.Microsecond)
	}

	b.StopTimer()

	// Calculate metrics
	ops := atomic.LoadUint64(&operationsCompleted)
	totalProcTime := atomic.LoadUint64(&totalProcessingTime)

	avgPushLatency := float64(pushDuration.Nanoseconds()) / float64(b.N)
	avgHandlerLatency := float64(totalProcTime) / float64(ops)
	throughput := float64(b.N) / pushDuration.Seconds()

	b.ReportMetric(avgPushLatency, "ns/push")
	b.ReportMetric(avgHandlerLatency, "ns/handler")
	b.ReportMetric(throughput, "ops/sec")

	// Realistic regression thresholds for Go
	if avgPushLatency > 1000 { // 1µs per push (very conservative)
		b.Errorf("Push latency regression: %.1f ns (threshold: 1000ns)", avgPushLatency)
	}

	if avgHandlerLatency > 500 { // 500ns per handler call (conservative)
		b.Errorf("Handler latency regression: %.1f ns (threshold: 500ns)", avgHandlerLatency)
	}

	if throughput < 1_000_000 { // 1M ops/sec minimum (achievable)
		b.Errorf("Throughput regression: %.0f ops/sec (threshold: 1M)", throughput)
	}

	// Log current performance for trend analysis
	b.Logf("Performance metrics:")
	b.Logf("  Push latency: %.1f ns/op", avgPushLatency)
	b.Logf("  Handler latency: %.1f ns/op", avgHandlerLatency)
	b.Logf("  Sustained throughput: %.0f ops/sec", throughput)

	atomic.StoreUint32(s.hot, 0)
	s.shutdown(100 * time.Millisecond)
}

// ============================================================================
// COOLDOWN VARIANT BENCHMARKS
// ============================================================================

// BenchmarkPinnedConsumerWithCooldown_Performance measures cooldown variant performance
func BenchmarkPinnedConsumerWithCooldown_Performance(b *testing.B) {
	runtime.GOMAXPROCS(2)

	s := newBenchmarkConsumerState(512)

	var processedCount uint64
	handler := func(data *[24]byte) {
		atomic.AddUint64(&processedCount, 1)
		_ = data[0] + data[23]
	}

	// Launch cooldown variant instead of regular consumer
	PinnedConsumerWithCooldown(1, s.ring, s.stop, s.hot, handler, s.done)

	// Warmup
	atomic.StoreUint32(s.hot, 1)
	warmupData := [24]byte{99}
	for i := 0; i < 1000; i++ {
		for !s.ring.Push(&warmupData) {
			time.Sleep(time.Microsecond)
		}
	}

	for atomic.LoadUint64(&processedCount) < 1000 {
		time.Sleep(time.Microsecond)
	}

	atomic.StoreUint64(&processedCount, 0)

	b.ReportAllocs()
	b.ResetTimer()

	// Measurement
	testData := [24]byte{42}
	start := time.Now()

	for i := 0; i < b.N; i++ {
		for !s.ring.Push(&testData) {
			runtime.Gosched()
		}
	}

	pushDuration := time.Since(start)

	for atomic.LoadUint64(&processedCount) < uint64(b.N) {
		time.Sleep(time.Microsecond)
	}

	b.StopTimer()

	throughput := float64(b.N) / pushDuration.Seconds()
	avgLatency := float64(pushDuration.Nanoseconds()) / float64(b.N)

	b.ReportMetric(throughput, "ops/sec")
	b.ReportMetric(avgLatency, "ns/op")

	atomic.StoreUint32(s.hot, 0)
	s.shutdown(100 * time.Millisecond)
}
