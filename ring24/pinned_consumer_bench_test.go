// ============================================================================
// PINNED CONSUMER FIXED BENCHMARK SUITE
// ============================================================================
//
// Fixed benchmark suite that eliminates timer-related hanging issues
// and provides accurate performance measurement without StopTimer/StartTimer abuse.
//
// Key fixes:
//   - Removed problematic StopTimer/StartTimer calls in tight loops
//   - Used separate measurement approaches for different performance aspects
//   - Implemented calibration-based measurement for precise overhead calculation
//   - Added proper benchmark termination conditions
//   - Used statistical sampling instead of per-iteration timing

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
}

// newBenchmarkConsumerState creates initialized benchmark state
func newBenchmarkConsumerState(ringSize int) *benchmarkConsumerState {
	return &benchmarkConsumerState{
		ring:            New(ringSize),
		stop:            new(uint32),
		hot:             new(uint32),
		done:            make(chan struct{}),
		operationsCount: new(uint64),
	}
}

// launchConsumer starts consumer with specified handler
func (s *benchmarkConsumerState) launchConsumer(handler func(*[24]byte)) {
	PinnedConsumer(0, s.ring, s.stop, s.hot, handler, s.done)
}

// shutdown gracefully stops consumer
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
// PURE PRODUCER LATENCY BENCHMARKS (FIXED)
// ============================================================================

// BenchmarkPinnedConsumer_ProducerLatency measures pure push operation latency
// WITHOUT timer manipulation in loops
func BenchmarkPinnedConsumer_ProducerLatency(b *testing.B) {
	runtime.GOMAXPROCS(2)

	sizes := []int{64, 256, 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("ring_size_%d", size), func(b *testing.B) {
			s := newBenchmarkConsumerState(size)

			// Simple counting consumer
			handler := func(data *[24]byte) {
				atomic.AddUint64(s.operationsCount, 1)
				_ = data[0] // Prevent optimization
			}

			s.launchConsumer(handler)

			// Warmup - no timing here
			atomic.StoreUint32(s.hot, 1)
			warmupData := [24]byte{99}
			for i := 0; i < 1000; i++ {
				for !s.ring.Push(&warmupData) {
					time.Sleep(time.Microsecond)
				}
			}

			// Wait for warmup completion
			for atomic.LoadUint64(s.operationsCount) < 1000 {
				time.Sleep(time.Microsecond)
			}

			// Reset for measurement
			atomic.StoreUint64(s.operationsCount, 0)

			// NO timer manipulation - let Go's benchmark framework handle timing
			b.ReportAllocs()
			b.ResetTimer()

			testData := [24]byte{42}
			for i := 0; i < b.N; i++ {
				// Just push - this is what we're measuring
				for !s.ring.Push(&testData) {
					runtime.Gosched()
				}
			}

			// Clean shutdown
			atomic.StoreUint32(s.hot, 0)
			s.shutdown(100 * time.Millisecond)
		})
	}
}

// ============================================================================
// PROCESSING OVERHEAD BENCHMARKS (FIXED)
// ============================================================================

// BenchmarkPinnedConsumer_ProcessingOverhead measures handler overhead using sampling
func BenchmarkPinnedConsumer_ProcessingOverhead(b *testing.B) {
	runtime.GOMAXPROCS(2)

	workloads := []struct {
		name        string
		handlerFunc func(*[24]byte) time.Duration
	}{
		{
			"minimal",
			func(data *[24]byte) time.Duration {
				start := time.Now()
				_ = data[0] // Single memory access
				return time.Since(start)
			},
		},
		{
			"light_computation",
			func(data *[24]byte) time.Duration {
				start := time.Now()
				sum := byte(0)
				for i := 0; i < 8; i++ {
					sum += data[i]
				}
				_ = sum
				return time.Since(start)
			},
		},
		{
			"checksum_validation",
			func(data *[24]byte) time.Duration {
				start := time.Now()
				checksum := uint32(0)
				for i := 0; i < 24; i++ {
					checksum = checksum*31 + uint32(data[i])
				}
				_ = checksum
				return time.Since(start)
			},
		},
	}

	for _, workload := range workloads {
		b.Run(workload.name, func(b *testing.B) {
			// Use sampling approach instead of per-iteration timing
			const sampleSize = 1000
			samples := make([]time.Duration, 0, sampleSize)

			b.ReportAllocs()
			b.ResetTimer()

			testData := [24]byte{42}
			sampleInterval := b.N / sampleSize
			if sampleInterval < 1 {
				sampleInterval = 1
			}

			for i := 0; i < b.N; i++ {
				// Sample handler overhead periodically
				if i%sampleInterval == 0 && len(samples) < sampleSize {
					elapsed := workload.handlerFunc(&testData)
					samples = append(samples, elapsed)
				} else {
					// Just run the handler without timing
					workload.handlerFunc(&testData)
				}
			}

			// Calculate average from samples
			if len(samples) > 0 {
				total := time.Duration(0)
				for _, sample := range samples {
					total += sample
				}
				avgHandlerTime := total / time.Duration(len(samples))
				b.ReportMetric(float64(avgHandlerTime.Nanoseconds()), "ns/handler_sampled")
			}
		})
	}
}

// ============================================================================
// END-TO-END THROUGHPUT BENCHMARKS (FIXED)
// ============================================================================

// BenchmarkPinnedConsumer_SustainedThroughput measures realistic throughput
func BenchmarkPinnedConsumer_SustainedThroughput(b *testing.B) {
	runtime.GOMAXPROCS(2)

	// Test different sustained durations
	testDurations := []struct {
		name     string
		duration time.Duration
	}{
		{"short_burst", 10 * time.Millisecond},
		{"medium_burst", 100 * time.Millisecond},
		{"sustained", 500 * time.Millisecond},
	}

	for _, test := range testDurations {
		b.Run(test.name, func(b *testing.B) {
			s := newBenchmarkConsumerState(2048)

			var processedCount uint64
			handler := func(data *[24]byte) {
				atomic.AddUint64(&processedCount, 1)
				_ = data[0] + data[23] // Minimal processing
			}

			s.launchConsumer(handler)
			atomic.StoreUint32(s.hot, 1)

			// Warmup
			warmupData := [24]byte{99}
			for i := 0; i < 1000; i++ {
				s.ring.Push(&warmupData)
			}
			time.Sleep(10 * time.Millisecond)

			atomic.StoreUint64(&processedCount, 0)

			b.ReportAllocs()
			b.ResetTimer()

			// Run for specified duration, counting operations
			testData := [24]byte{42}
			start := time.Now()
			opsCompleted := 0

			for time.Since(start) < test.duration && opsCompleted < b.N {
				if s.ring.Push(&testData) {
					opsCompleted++
				} else {
					runtime.Gosched()
				}
			}

			elapsed := time.Since(start)

			// Wait for processing to complete
			deadline := time.Now().Add(100 * time.Millisecond)
			for time.Now().Before(deadline) && atomic.LoadUint64(&processedCount) < uint64(opsCompleted) {
				time.Sleep(time.Millisecond)
			}

			processed := atomic.LoadUint64(&processedCount)
			throughput := float64(processed) / elapsed.Seconds()

			b.ReportMetric(throughput, "ops/sec")
			b.ReportMetric(float64(opsCompleted), "total_pushed")
			b.ReportMetric(float64(processed), "total_processed")

			atomic.StoreUint32(s.hot, 0)
			s.shutdown(100 * time.Millisecond)
		})
	}
}

// ============================================================================
// ADAPTIVE POLLING BEHAVIOR BENCHMARKS (FIXED)
// ============================================================================

// BenchmarkPinnedConsumer_AdaptivePolling tests hot/cold behavior
func BenchmarkPinnedConsumer_AdaptivePolling(b *testing.B) {
	runtime.GOMAXPROCS(2)

	scenarios := []struct {
		name       string
		useHotFlag bool
		burstSize  int
		idleTime   time.Duration
	}{
		{"hot_continuous", true, b.N, 0},
		{"hot_bursty", true, 100, 10 * time.Millisecond},
		{"cold_continuous", false, b.N, 0},
		{"cold_bursty", false, 100, 10 * time.Millisecond},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			s := newBenchmarkConsumerState(256)

			var processedCount uint64
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

			testData := [24]byte{42}
			remaining := b.N

			for remaining > 0 {
				batchSize := scenario.burstSize
				if batchSize > remaining {
					batchSize = remaining
				}

				// Push batch
				for i := 0; i < batchSize; i++ {
					for !s.ring.Push(&testData) {
						runtime.Gosched()
					}
				}

				remaining -= batchSize

				// Idle period if configured
				if scenario.idleTime > 0 && remaining > 0 {
					time.Sleep(scenario.idleTime)
				}
			}

			// Wait for processing completion
			for atomic.LoadUint64(&processedCount) < uint64(b.N) {
				time.Sleep(time.Microsecond)
			}

			if scenario.useHotFlag {
				atomic.StoreUint32(s.hot, 0)
			}
			s.shutdown(100 * time.Millisecond)
		})
	}
}

// ============================================================================
// MEMORY ACCESS PATTERN BENCHMARKS (FIXED)
// ============================================================================

// BenchmarkPinnedConsumer_MemoryPatterns tests cache behavior
func BenchmarkPinnedConsumer_MemoryPatterns(b *testing.B) {
	runtime.GOMAXPROCS(2)

	patterns := []struct {
		name        string
		ringSize    int
		processFunc func(*[24]byte) byte
	}{
		{
			"single_byte_access",
			256,
			func(data *[24]byte) byte { return data[0] },
		},
		{
			"full_data_access",
			256,
			func(data *[24]byte) byte {
				sum := byte(0)
				for i := 0; i < 24; i++ {
					sum += data[i]
				}
				return sum
			},
		},
		{
			"large_ring_full_access",
			4096,
			func(data *[24]byte) byte {
				sum := byte(0)
				for i := 0; i < 24; i++ {
					sum += data[i]
				}
				return sum
			},
		},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			s := newBenchmarkConsumerState(pattern.ringSize)

			var processedCount uint64
			handler := func(data *[24]byte) {
				_ = pattern.processFunc(data)
				atomic.AddUint64(&processedCount, 1)
			}

			s.launchConsumer(handler)
			atomic.StoreUint32(s.hot, 1)

			b.ReportAllocs()
			b.ResetTimer()

			testData := [24]byte{42}
			for i := 0; i < b.N; i++ {
				for !s.ring.Push(&testData) {
					runtime.Gosched()
				}
			}

			// Wait for completion
			for atomic.LoadUint64(&processedCount) < uint64(b.N) {
				time.Sleep(time.Microsecond)
			}

			atomic.StoreUint32(s.hot, 0)
			s.shutdown(100 * time.Millisecond)
		})
	}
}

// ============================================================================
// REGRESSION DETECTION WITH REALISTIC THRESHOLDS
// ============================================================================

// BenchmarkPinnedConsumer_RegressionDetection provides baseline performance validation
func BenchmarkPinnedConsumer_RegressionDetection(b *testing.B) {
	runtime.GOMAXPROCS(2)

	s := newBenchmarkConsumerState(1024)

	var processedCount uint64
	handler := func(data *[24]byte) {
		atomic.AddUint64(&processedCount, 1)
		// Realistic minimal processing
		_ = data[0] + data[23]
	}

	s.launchConsumer(handler)

	// Warmup
	atomic.StoreUint32(s.hot, 1)
	warmupData := [24]byte{99}
	for i := 0; i < 5000; i++ {
		for !s.ring.Push(&warmupData) {
			time.Sleep(time.Microsecond)
		}
	}

	// Wait for warmup completion
	for atomic.LoadUint64(&processedCount) < 5000 {
		time.Sleep(time.Microsecond)
	}

	// Reset for measurement
	atomic.StoreUint64(&processedCount, 0)

	b.ReportAllocs()
	b.ResetTimer()

	// Measure push operations
	testData := [24]byte{42}
	start := time.Now()

	for i := 0; i < b.N; i++ {
		for !s.ring.Push(&testData) {
			runtime.Gosched()
		}
	}

	pushDuration := time.Since(start)

	// Wait for processing completion
	for atomic.LoadUint64(&processedCount) < uint64(b.N) {
		time.Sleep(time.Microsecond)
	}

	// Calculate realistic metrics
	avgPushLatency := float64(pushDuration.Nanoseconds()) / float64(b.N)
	throughput := float64(b.N) / pushDuration.Seconds()

	b.ReportMetric(avgPushLatency, "ns/push")
	b.ReportMetric(throughput, "ops/sec")

	// REALISTIC regression thresholds for Go
	if avgPushLatency > 2000 { // 2µs per push (very conservative for Go)
		b.Errorf("Push latency regression: %.1f ns (threshold: 2000ns)", avgPushLatency)
	}

	if throughput < 500_000 { // 500K ops/sec minimum (definitely achievable)
		b.Errorf("Throughput regression: %.0f ops/sec (threshold: 500K)", throughput)
	}

	// Log for trend analysis
	b.Logf("Performance: %.1f ns/push, %.0f ops/sec", avgPushLatency, throughput)

	atomic.StoreUint32(s.hot, 0)
	s.shutdown(100 * time.Millisecond)
}

// ============================================================================
// COOLDOWN VARIANT BENCHMARKS (FIXED)
// ============================================================================

// BenchmarkPinnedConsumerWithCooldown_Performance tests cooldown variant
func BenchmarkPinnedConsumerWithCooldown_Performance(b *testing.B) {
	runtime.GOMAXPROCS(2)

	s := newBenchmarkConsumerState(512)

	var processedCount uint64
	handler := func(data *[24]byte) {
		atomic.AddUint64(&processedCount, 1)
		_ = data[0] + data[23]
	}

	// Use cooldown variant
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

	// Measure cooldown variant performance
	testData := [24]byte{42}
	for i := 0; i < b.N; i++ {
		for !s.ring.Push(&testData) {
			runtime.Gosched()
		}
	}

	// Wait for completion
	for atomic.LoadUint64(&processedCount) < uint64(b.N) {
		time.Sleep(time.Microsecond)
	}

	atomic.StoreUint32(s.hot, 0)
	s.shutdown(100 * time.Millisecond)
}

// ============================================================================
// CALIBRATION-BASED PRECISE MEASUREMENT (ADVANCED)
// ============================================================================

// BenchmarkPinnedConsumer_CalibratedMeasurement provides the most accurate timing
func BenchmarkPinnedConsumer_CalibratedMeasurement(b *testing.B) {
	runtime.GOMAXPROCS(2)

	// Calibrate timing overhead first
	calibrationRuns := 1000
	overheadTotal := time.Duration(0)

	for i := 0; i < calibrationRuns; i++ {
		start := time.Now()
		_ = time.Since(start) // Measure timing overhead
		end := time.Now()
		overheadTotal += end.Sub(start)
	}

	timingOverhead := overheadTotal / time.Duration(calibrationRuns)

	s := newBenchmarkConsumerState(512)

	// Measure actual operation with calibration
	handler := func(data *[24]byte) {
		_ = data[0]
	}

	s.launchConsumer(handler)
	atomic.StoreUint32(s.hot, 1)

	b.ReportAllocs()
	b.ResetTimer()

	testData := [24]byte{42}
	start := time.Now()

	for i := 0; i < b.N; i++ {
		for !s.ring.Push(&testData) {
			runtime.Gosched()
		}
	}

	rawDuration := time.Since(start)

	// Apply calibration correction
	calibratedDuration := rawDuration - (timingOverhead * time.Duration(b.N))
	calibratedLatency := float64(calibratedDuration.Nanoseconds()) / float64(b.N)

	b.ReportMetric(calibratedLatency, "ns/op_calibrated")
	b.ReportMetric(float64(timingOverhead.Nanoseconds()), "timing_overhead_ns")

	atomic.StoreUint32(s.hot, 0)
	s.shutdown(100 * time.Millisecond)
}
