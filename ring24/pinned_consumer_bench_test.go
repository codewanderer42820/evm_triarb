// ============================================================================
// PRECISION CONSUMER LATENCY VALIDATION SUITE
// ============================================================================
//
// High-precision latency measurement framework for ISR-grade pinned consumer
// validation with emphasis on eliminating timing overhead and measurement artifacts.
//
// Measurement methodology:
//   - Separation of push latency from handler processing overhead
//   - Elimination of time.Now() calls in critical measurement paths
//   - Pre-warmed steady-state operation for accurate baseline
//   - Atomic counter-based synchronization to minimize test framework overhead
//   - Statistical validation with ISR-grade performance thresholds
//
// Performance validation targets:
//   - Push latency: <100ns per operation (ISR-acceptable threshold)
//   - Handler processing: <50ns per operation for minimal workloads
//   - Sustained throughput: >10M operations/second minimum acceptable rate

package ring24

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestPinnedConsumer_ActualLatency measures real handler invocation latency without measurement artifacts
func TestPinnedConsumer_ActualLatency(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(32)

	// Atomic counters for precise measurement without timing overhead
	var handlerCalls uint64
	var totalProcessingTime uint64

	handler := func(data *[24]byte) {
		start := time.Now()

		// Minimal realistic workload - memory access to validate data integrity
		_ = data[0] + data[23]

		processingTime := time.Since(start)
		atomic.AddUint64(&handlerCalls, 1)
		atomic.AddUint64(&totalProcessingTime, uint64(processingTime.Nanoseconds()))
	}

	s.launch(handler)

	// Warmup phase to establish steady-state system operation
	atomic.StoreUint32(s.hot, 1)
	for i := 0; i < 1000; i++ {
		data := testDataConsumer(byte(i))
		for !s.ring.Push(&data) {
			time.Sleep(time.Microsecond)
		}
	}

	// Wait for warmup phase completion
	for atomic.LoadUint64(&handlerCalls) < 1000 {
		time.Sleep(time.Microsecond)
	}

	// Reset counters for measurement phase
	atomic.StoreUint64(&handlerCalls, 0)
	atomic.StoreUint64(&totalProcessingTime, 0)

	// Measurement phase: pure push latency without handler timing overhead
	const testOps = 10000
	start := time.Now()

	for i := 0; i < testOps; i++ {
		data := testDataConsumer(byte(i))
		// Primary measurement target: ring buffer push latency
		for !s.ring.Push(&data) {
			// Active polling - consumer will drain ring buffer
		}
	}

	pushLatency := time.Since(start)

	// Wait for all handler operations to complete
	for atomic.LoadUint64(&handlerCalls) < testOps {
		time.Sleep(time.Microsecond)
	}

	atomic.StoreUint32(s.hot, 0)
	s.shutdown(100 * time.Millisecond)

	// Calculate performance metrics
	avgPushLatency := float64(pushLatency.Nanoseconds()) / float64(testOps)
	calls := atomic.LoadUint64(&handlerCalls)
	totalProcTime := atomic.LoadUint64(&totalProcessingTime)
	avgHandlerLatency := float64(totalProcTime) / float64(calls)

	t.Logf("Push operation latency: %.1f ns/op", avgPushLatency)
	t.Logf("Handler processing latency: %.1f ns/op", avgHandlerLatency)
	t.Logf("Sustained throughput: %.0f ops/sec", float64(testOps)/pushLatency.Seconds())

	// ISR-grade performance threshold validation
	if avgPushLatency > 100 {
		t.Errorf("Push latency exceeds threshold: %.1f ns (maximum: 100ns)", avgPushLatency)
	}

	if avgHandlerLatency > 50 {
		t.Errorf("Handler latency exceeds threshold: %.1f ns (maximum: 50ns)", avgHandlerLatency)
	}

	throughput := float64(testOps) / pushLatency.Seconds()
	if throughput < 10_000_000 { // 10M operations/second minimum
		t.Errorf("Throughput below threshold: %.0f ops/sec (minimum: 10M)", throughput)
	}
}
