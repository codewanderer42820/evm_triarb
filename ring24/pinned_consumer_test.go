// ============================================================================
// PINNED CONSUMER LIFECYCLE VALIDATION SUITE
// ============================================================================
//
// Comprehensive testing framework for ISR-grade pinned consumer goroutines
// with emphasis on lifecycle management and adaptive polling behavior.
//
// Test categories:
//   - Basic functionality: Callback triggering and data delivery
//   - Shutdown coordination: Stop flag handling and graceful termination
//   - Hot window behavior: Activity-based polling state management
//   - Cold resume logic: Recovery from idle periods and backoff behavior
//   - Delayed startup: Pre-pushed data handling and consumer initialization
//   - Error conditions: Invalid parameters and edge cases
//   - Performance characteristics: Latency and throughput validation
//   - Memory safety: Goroutine cleanup and resource management
//   - Concurrency safety: Thread-safe operation validation
//
// Validation methodology:
//   - Multi-core environment setup for realistic testing
//   - Pure memory side-effect callbacks for deterministic validation
//   - Atomic operations for thread-safe state coordination
//   - Time-based testing for temporal behavior verification
//   - Resource leak detection and cleanup validation
//   - Statistical analysis of timing behavior
//
// Performance assumptions:
//   - Sub-microsecond callback invocation latency
//   - Immediate response to producer activity signals
//   - Graceful degradation during idle periods
//   - Deterministic shutdown within bounded time limits
//   - Zero memory leaks during normal operation

package ring24

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// ============================================================================
// TEST UTILITIES AND HELPERS
// ============================================================================

// testConsumerConfig holds configuration for consumer tests
type testConsumerConfig struct {
	core      int
	ringSize  int
	timeout   time.Duration
	warmup    time.Duration
	validator func(*testing.T, *consumerTestState)
}

// consumerTestState tracks test execution state
type consumerTestState struct {
	ring         *Ring
	stop         *uint32
	hot          *uint32
	done         chan struct{}
	callCount    *uint32
	lastData     *[24]byte
	callTimes    []time.Time
	startTime    time.Time
	mu           sync.Mutex
	dataReceived map[[24]byte]int
}

// newConsumerTestState creates initialized test state
func newConsumerTestState(ringSize int) *consumerTestState {
	return &consumerTestState{
		ring:         New(ringSize),
		stop:         new(uint32),
		hot:          new(uint32),
		done:         make(chan struct{}),
		callCount:    new(uint32),
		startTime:    time.Now(),
		dataReceived: make(map[[24]byte]int),
	}
}

// trackingHandler creates a callback that tracks all invocations
func (s *consumerTestState) trackingHandler() func(*[24]byte) {
	return func(data *[24]byte) {
		atomic.AddUint32(s.callCount, 1)

		s.mu.Lock()
		s.lastData = data
		s.callTimes = append(s.callTimes, time.Now())
		if data != nil {
			s.dataReceived[*data]++
		}
		s.mu.Unlock()
	}
}

// countingHandler creates a simple counting callback
func (s *consumerTestState) countingHandler() func(*[24]byte) {
	return func(_ *[24]byte) {
		atomic.AddUint32(s.callCount, 1)
	}
}

// launch starts a pinned consumer with the given handler
func (s *consumerTestState) launch(handler func(*[24]byte)) {
	PinnedConsumer(0, s.ring, s.stop, s.hot, handler, s.done)
}

// launchWithCooldown starts a pinned consumer with cooldown
func (s *consumerTestState) launchWithCooldown(handler func(*[24]byte)) {
	PinnedConsumerWithCooldown(1, s.ring, s.stop, s.hot, handler, s.done)
}

// shutdown gracefully stops the consumer
func (s *consumerTestState) shutdown(timeout time.Duration) error {
	atomic.StoreUint32(s.stop, 1)

	select {
	case <-s.done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("consumer shutdown timeout after %v", timeout)
	}
}

// waitForCalls waits for a specific number of callback invocations
func (s *consumerTestState) waitForCalls(expected uint32, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if atomic.LoadUint32(s.callCount) >= expected {
			return nil
		}
		time.Sleep(time.Millisecond)
	}

	return fmt.Errorf("expected %d calls, got %d within %v",
		expected, atomic.LoadUint32(s.callCount), timeout)
}

// getCallCount returns the current callback count
func (s *consumerTestState) getCallCount() uint32 {
	return atomic.LoadUint32(s.callCount)
}

// getLastData returns the last received data (thread-safe)
func (s *consumerTestState) getLastData() *[24]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastData
}

// getDataStats returns statistics about received data
func (s *consumerTestState) getDataStats() map[[24]byte]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[[24]byte]int)
	for k, v := range s.dataReceived {
		result[k] = v
	}
	return result
}

// testDataConsumer creates deterministic test payload for consumer tests
func testDataConsumer(seed byte) [24]byte {
	var data [24]byte
	for i := range data {
		data[i] = seed + byte(i)
	}
	return data
}

// pushTestData pushes test data with hot signal management
func pushTestData(s *consumerTestState, data [24]byte, useHot bool) bool {
	if useHot {
		atomic.StoreUint32(s.hot, 1)
	}

	success := s.ring.Push(&data)

	if useHot {
		atomic.StoreUint32(s.hot, 0)
	}

	return success
}

// ============================================================================
// BASIC FUNCTIONALITY VALIDATION
// ============================================================================

// TestPinnedConsumerBasicOperation validates core consumer functionality
func TestPinnedConsumerBasicOperation(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		runtime.GOMAXPROCS(2) // Ensure sufficient cores
	}

	s := newConsumerTestState(8)
	handler := s.trackingHandler()

	// Launch consumer
	s.launch(handler)

	// Test data delivery
	want := testDataConsumer(42)
	if !pushTestData(s, want, true) {
		t.Fatal("Failed to push test data")
	}

	// Wait for callback execution
	if err := s.waitForCalls(1, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Validate data integrity
	got := s.getLastData()
	if got == nil || *got != want {
		t.Fatalf("Expected %v, got %v", want, got)
	}

	// Test graceful shutdown
	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Verify final call count
	if count := s.getCallCount(); count != 1 {
		t.Fatalf("Expected 1 callback, got %d", count)
	}
}

// TestPinnedConsumerMultipleItems validates handling of multiple data items
func TestPinnedConsumerMultipleItems(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(16)
	handler := s.trackingHandler()
	s.launch(handler)

	// Push multiple items
	testItems := []byte{1, 2, 3, 4, 5}
	atomic.StoreUint32(s.hot, 1)

	for _, item := range testItems {
		data := testDataConsumer(item)
		if !s.ring.Push(&data) {
			t.Fatalf("Failed to push item %d", item)
		}
	}

	atomic.StoreUint32(s.hot, 0)

	// Wait for all callbacks
	if err := s.waitForCalls(uint32(len(testItems)), 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Verify all data was received
	stats := s.getDataStats()
	for _, item := range testItems {
		expected := testDataConsumer(item)
		if count, exists := stats[expected]; !exists || count != 1 {
			t.Errorf("Item %v: expected 1 occurrence, got %d", expected, count)
		}
	}

	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// TestPinnedConsumerWithCooldownBasic validates cooldown variant functionality
func TestPinnedConsumerWithCooldownBasic(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(8)
	handler := s.trackingHandler()

	// Launch cooldown variant
	s.launchWithCooldown(handler)

	// Test data delivery
	want := testDataConsumer(99)
	if !pushTestData(s, want, true) {
		t.Fatal("Failed to push test data")
	}

	if err := s.waitForCalls(1, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	got := s.getLastData()
	if got == nil || *got != want {
		t.Fatalf("Expected %v, got %v", want, got)
	}

	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// ============================================================================
// SHUTDOWN COORDINATION VALIDATION
// ============================================================================

// TestPinnedConsumerImmediateShutdown validates immediate termination
func TestPinnedConsumerImmediateShutdown(t *testing.T) {
	s := newConsumerTestState(4)
	handler := s.countingHandler()

	s.launch(handler)

	// Immediate shutdown without any data
	if err := s.shutdown(50 * time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Verify no spurious callbacks
	if count := s.getCallCount(); count != 0 {
		t.Fatalf("Expected 0 callbacks, got %d", count)
	}
}

// TestPinnedConsumerShutdownDuringProcessing validates shutdown during activity
func TestPinnedConsumerShutdownDuringProcessing(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(32)
	var processingMu sync.Mutex
	processingCount := 0

	handler := func(data *[24]byte) {
		processingMu.Lock()
		processingCount++
		processingMu.Unlock()

		// Simulate processing time
		time.Sleep(time.Millisecond)
		atomic.AddUint32(s.callCount, 1)
	}

	s.launch(handler)

	// Push data continuously
	atomic.StoreUint32(s.hot, 1)
	for i := 0; i < 10; i++ {
		data := testDataConsumer(byte(i))
		s.ring.Push(&data)
	}

	// Allow some processing
	time.Sleep(5 * time.Millisecond)

	// Shutdown during processing
	shutdownStart := time.Now()
	if err := s.shutdown(200 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	shutdownDuration := time.Since(shutdownStart)

	// Verify shutdown was reasonably prompt
	if shutdownDuration > 150*time.Millisecond {
		t.Errorf("Shutdown took %v, expected < 150ms", shutdownDuration)
	}

	// Verify some processing occurred
	if s.getCallCount() == 0 {
		t.Fatal("No callbacks executed before shutdown")
	}
}

// TestPinnedConsumerMultipleShutdownCalls validates idempotent shutdown
func TestPinnedConsumerMultipleShutdownCalls(t *testing.T) {
	s := newConsumerTestState(4)
	handler := s.countingHandler()

	s.launch(handler)

	// Multiple shutdown attempts
	for i := 0; i < 5; i++ {
		atomic.StoreUint32(s.stop, 1)
	}

	// Should shutdown cleanly despite multiple stop signals
	select {
	case <-s.done:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Consumer did not shutdown after multiple stop signals")
	}
}

// ============================================================================
// HOT WINDOW BEHAVIOR VALIDATION
// ============================================================================

// TestPinnedConsumerHotWindowPersistence validates hot polling duration
func TestPinnedConsumerHotWindowPersistence(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(8)
	handler := s.trackingHandler()
	s.launch(handler)

	// Process item to activate hot window
	data := testDataConsumer(42)
	if !pushTestData(s, data, true) {
		t.Fatal("Failed to push initial data")
	}

	if err := s.waitForCalls(1, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Wait within hot window (less than 5 seconds)
	time.Sleep(2 * time.Second)

	// Consumer should still be active (not shut down)
	select {
	case <-s.done:
		t.Fatal("Consumer shut down prematurely during hot window")
	default:
		// Expected: consumer still running
	}

	// Push another item to verify responsiveness
	data2 := testDataConsumer(43)
	if !pushTestData(s, data2, false) {
		t.Fatal("Failed to push second data during hot window")
	}

	if err := s.waitForCalls(2, 50*time.Millisecond); err != nil {
		t.Fatal("Consumer not responsive during hot window")
	}

	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// TestPinnedConsumerHotFlagBehavior validates hot flag influence
func TestPinnedConsumerHotFlagBehavior(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(8)
	handler := s.trackingHandler()
	s.launch(handler)

	// Test with hot flag set
	atomic.StoreUint32(s.hot, 1)

	// Even without data, consumer should remain active when hot flag is set
	time.Sleep(100 * time.Millisecond)

	select {
	case <-s.done:
		t.Fatal("Consumer shut down while hot flag was set")
	default:
		// Expected: consumer still running
	}

	// Clear hot flag
	atomic.StoreUint32(s.hot, 0)

	// Now push data to verify consumer is still responsive
	data := testDataConsumer(55)
	if !s.ring.Push(&data) {
		t.Fatal("Failed to push data after clearing hot flag")
	}

	if err := s.waitForCalls(1, 50*time.Millisecond); err != nil {
		t.Fatal("Consumer not responsive after hot flag cleared")
	}

	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// TestPinnedConsumerHotWindowExpiry validates transition to idle state
func TestPinnedConsumerHotWindowExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping hot window expiry test in short mode")
	}

	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(8)

	handler := func(data *[24]byte) {
		atomic.AddUint32(s.callCount, 1)
	}

	s.launch(handler)

	// Process initial item
	data := testDataConsumer(77)
	if !pushTestData(s, data, true) {
		t.Fatal("Failed to push initial data")
	}

	if err := s.waitForCalls(1, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Wait for hot window to expire (hotWindow = 5 seconds)
	time.Sleep(6 * time.Second)

	// Push new data after hot window expiry
	data2 := testDataConsumer(78)
	if !pushTestData(s, data2, false) {
		t.Fatal("Failed to push data after hot window expiry")
	}

	// Should still be responsive, but may have brief delay due to CPU relaxation
	if err := s.waitForCalls(2, 100*time.Millisecond); err != nil {
		t.Fatal("Consumer not responsive after hot window expiry")
	}

	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// ============================================================================
// COLD RESUME AND BACKOFF VALIDATION
// ============================================================================

// TestPinnedConsumerColdResume validates recovery from idle periods
func TestPinnedConsumerColdResume(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cold resume test in short mode")
	}

	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(8)
	handler := s.trackingHandler()
	s.launch(handler)

	// Initial processing
	data1 := testDataConsumer(10)
	if !pushTestData(s, data1, true) {
		t.Fatal("Failed to push initial data")
	}

	if err := s.waitForCalls(1, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Extended idle period (beyond hot window + spin budget)
	time.Sleep(6*time.Second + 100*time.Millisecond)

	// Resume activity
	data2 := testDataConsumer(11)
	resumeStart := time.Now()

	if !pushTestData(s, data2, true) {
		t.Fatal("Failed to push data after idle period")
	}

	// Measure resume latency
	if err := s.waitForCalls(2, 200*time.Millisecond); err != nil {
		t.Fatal("Consumer failed to resume after idle period")
	}

	resumeLatency := time.Since(resumeStart)

	// Resume should be reasonably quick despite idle period
	if resumeLatency > 100*time.Millisecond {
		t.Logf("Resume latency: %v (may indicate CPU relaxation working)", resumeLatency)
	}

	// Verify data integrity after resume
	lastData := s.getLastData()
	if lastData == nil || *lastData != data2 {
		t.Fatalf("Data corruption after resume: got %v, want %v", lastData, data2)
	}

	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// TestPinnedConsumerSpinBudgetBehavior validates CPU relaxation timing
func TestPinnedConsumerSpinBudgetBehavior(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(8)

	// Track system behavior during spinning
	startTime := time.Now()
	responseLatencies := make([]time.Duration, 0)

	handler := func(data *[24]byte) {
		responseLatencies = append(responseLatencies, time.Since(startTime))
		atomic.AddUint32(s.callCount, 1)
		startTime = time.Now()
	}

	s.launch(handler)

	// Allow spin budget to be exercised
	time.Sleep(100 * time.Millisecond)

	// Push data after potential CPU relaxation
	data := testDataConsumer(99)
	if !pushTestData(s, data, false) {
		t.Fatal("Failed to push data after spin period")
	}

	if err := s.waitForCalls(1, 100*time.Millisecond); err != nil {
		t.Fatal("Consumer not responsive after spin period")
	}

	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Log latency characteristics for analysis
	if len(responseLatencies) > 0 {
		t.Logf("Response latency after potential relaxation: %v", responseLatencies[0])
	}
}

// ============================================================================
// DELAYED STARTUP VALIDATION
// ============================================================================

// TestPinnedConsumerDelayedStartup validates pre-pushed data handling
func TestPinnedConsumerDelayedStartup(t *testing.T) {
	s := newConsumerTestState(8)

	// Push data before consumer starts
	prePushedData := []byte{1, 2, 3}
	for _, item := range prePushedData {
		data := testDataConsumer(item)
		if !s.ring.Push(&data) {
			t.Fatalf("Failed to pre-push data %d", item)
		}
	}

	// Now start consumer
	handler := s.trackingHandler()
	s.launch(handler)

	// Consumer should process pre-existing data
	expectedCalls := uint32(len(prePushedData))
	if err := s.waitForCalls(expectedCalls, 100*time.Millisecond); err != nil {
		t.Fatal("Consumer failed to process pre-pushed data")
	}

	// Verify all pre-pushed data was received
	stats := s.getDataStats()
	for _, item := range prePushedData {
		expected := testDataConsumer(item)
		if count, exists := stats[expected]; !exists || count != 1 {
			t.Errorf("Pre-pushed item %v: expected 1, got %d", expected, count)
		}
	}

	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// TestPinnedConsumerStartupRace validates startup timing robustness
func TestPinnedConsumerStartupRace(t *testing.T) {
	for iteration := 0; iteration < 10; iteration++ {
		t.Run(fmt.Sprintf("iteration_%d", iteration), func(t *testing.T) {
			s := newConsumerTestState(4)

			// Rapidly push data and start consumer
			var wg sync.WaitGroup
			wg.Add(2)

			// Producer goroutine
			go func() {
				defer wg.Done()
				for i := 0; i < 5; i++ {
					data := testDataConsumer(byte(i))
					s.ring.Push(&data)
					time.Sleep(time.Microsecond)
				}
			}()

			// Consumer startup
			go func() {
				defer wg.Done()
				time.Sleep(time.Microsecond) // Slight delay
				handler := s.trackingHandler()
				s.launch(handler)
			}()

			wg.Wait()

			// Allow processing
			time.Sleep(50 * time.Millisecond)

			// Should have processed some or all data
			count := s.getCallCount()
			if count == 0 {
				t.Error("Consumer processed no data in startup race")
			}
			if count > 5 {
				t.Errorf("Consumer processed more data than expected: %d", count)
			}

			if err := s.shutdown(100 * time.Millisecond); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// ============================================================================
// ERROR CONDITIONS AND EDGE CASES
// ============================================================================

// TestPinnedConsumerNilHandler validates nil handler handling
func TestPinnedConsumerNilHandler(t *testing.T) {
	s := newConsumerTestState(4)

	// This should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Consumer with nil handler panicked: %v", r)
		}
	}()

	PinnedConsumer(0, s.ring, s.stop, s.hot, nil, s.done)

	// Push data
	data := testDataConsumer(42)
	pushTestData(s, data, true)

	// Allow some time for potential nil dereference
	time.Sleep(10 * time.Millisecond)

	// Should shutdown cleanly
	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// TestPinnedConsumerNilPointers validates nil pointer handling
func TestPinnedConsumerNilPointers(t *testing.T) {
	ring := New(4)
	done := make(chan struct{})

	// Test with nil stop pointer
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic with nil stop pointer")
		}
	}()

	PinnedConsumer(0, ring, nil, new(uint32), func(*[24]byte) {}, done)
}

// TestPinnedConsumerInvalidCore validates core parameter handling
func TestPinnedConsumerInvalidCore(t *testing.T) {
	s := newConsumerTestState(4)

	// Test with invalid core numbers
	invalidCores := []int{-1, 999}

	for _, core := range invalidCores {
		t.Run(fmt.Sprintf("core_%d", core), func(t *testing.T) {
			handler := s.countingHandler()

			// Should not panic with invalid core
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Consumer panicked with invalid core %d: %v", core, r)
				}
			}()

			PinnedConsumer(core, s.ring, s.stop, s.hot, handler, s.done)

			// Should still function despite invalid affinity
			data := testDataConsumer(42)
			pushTestData(s, data, true)

			if err := s.waitForCalls(1, 100*time.Millisecond); err != nil {
				t.Errorf("Consumer with invalid core %d failed: %v", core, err)
			}

			if err := s.shutdown(100 * time.Millisecond); err != nil {
				t.Fatal(err)
			}

			// Reset for next iteration
			s = newConsumerTestState(4)
		})
	}
}

// ============================================================================
// PERFORMANCE CHARACTERISTICS VALIDATION
// ============================================================================

// TestPinnedConsumerLatency validates callback latency
func TestPinnedConsumerLatency(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(32)

	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0)

	handler := func(data *[24]byte) {
		end := time.Now()
		// Extract timestamp from data (if embedded)
		latencyMu.Lock()
		latencies = append(latencies, end.Sub(s.startTime))
		latencyMu.Unlock()
		atomic.AddUint32(s.callCount, 1)
	}

	s.launch(handler)

	// Measure multiple operation latencies
	const numOps = 100
	atomic.StoreUint32(s.hot, 1)

	for i := 0; i < numOps; i++ {
		s.startTime = time.Now()
		data := testDataConsumer(byte(i))
		if !s.ring.Push(&data) {
			t.Fatalf("Push %d failed", i)
		}
		time.Sleep(time.Microsecond) // Small delay between operations
	}

	atomic.StoreUint32(s.hot, 0)

	if err := s.waitForCalls(numOps, 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Analyze latencies
	latencyMu.Lock()
	defer latencyMu.Unlock()

	if len(latencies) == 0 {
		t.Fatal("No latency measurements collected")
	}

	var total time.Duration
	var max time.Duration
	min := time.Hour

	for _, lat := range latencies {
		total += lat
		if lat > max {
			max = lat
		}
		if lat < min {
			min = lat
		}
	}

	avg := total / time.Duration(len(latencies))

	t.Logf("Latency stats: avg=%v, min=%v, max=%v, samples=%d",
		avg, min, max, len(latencies))

	// Validate reasonable latency bounds
	if avg > 100*time.Microsecond {
		t.Errorf("Average latency too high: %v", avg)
	}
	if max > time.Millisecond {
		t.Errorf("Maximum latency too high: %v", max)
	}
}

// TestPinnedConsumerThroughput validates sustained throughput
func TestPinnedConsumerThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput test in short mode")
	}

	runtime.GOMAXPROCS(2)

	s := newConsumerTestState(1024)
	handler := s.countingHandler()
	s.launch(handler)

	// Sustained operation for measurement
	duration := 1 * time.Second
	start := time.Now()
	atomic.StoreUint32(s.hot, 1)

	operations := 0
	for time.Since(start) < duration {
		data := testDataConsumer(byte(operations % 256))
		if s.ring.Push(&data) {
			operations++
		} else {
			// Ring full, brief yield
			time.Sleep(time.Microsecond)
		}
	}

	atomic.StoreUint32(s.hot, 0)
	elapsed := time.Since(start)

	// Allow final processing
	time.Sleep(100 * time.Millisecond)

	if err := s.shutdown(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}

	processed := s.getCallCount()
	throughput := float64(processed) / elapsed.Seconds()

	t.Logf("Throughput: %.0f ops/sec (%d ops in %v)",
		throughput, processed, elapsed)

	// Validate minimum throughput
	if throughput < 100000 { // 100K ops/sec minimum
		t.Errorf("Throughput too low: %.0f ops/sec", throughput)
	}

	// Validate processing efficiency
	efficiency := float64(processed) / float64(operations)
	if efficiency < 0.8 {
		t.Errorf("Processing efficiency too low: %.2f", efficiency)
	}
}

// ============================================================================
// MEMORY SAFETY AND RESOURCE MANAGEMENT
// ============================================================================

// TestPinnedConsumerGoroutineCleanup validates proper goroutine lifecycle
func TestPinnedConsumerGoroutineCleanup(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	// Start and stop multiple consumers
	for i := 0; i < 10; i++ {
		s := newConsumerTestState(4)
		handler := s.countingHandler()
		s.launch(handler)

		// Brief activity
		data := testDataConsumer(byte(i))
		pushTestData(s, data, true)
		s.waitForCalls(1, 50*time.Millisecond)

		if err := s.shutdown(100 * time.Millisecond); err != nil {
			t.Fatalf("Iteration %d: %v", i, err)
		}
	}

	// Allow goroutine cleanup
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()

	// Should not have significant goroutine leak
	leaked := finalGoroutines - initialGoroutines
	if leaked > 2 { // Allow some tolerance for test framework
		t.Errorf("Potential goroutine leak: %d additional goroutines", leaked)
	}

	t.Logf("Goroutines: initial=%d, final=%d, delta=%d",
		initialGoroutines, finalGoroutines, leaked)
}

// TestPinnedConsumerMemoryLeaks validates absence of memory leaks
func TestPinnedConsumerMemoryLeaks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Perform many consumer operations
	for i := 0; i < 100; i++ {
		s := newConsumerTestState(16)
		handler := s.countingHandler()
		s.launch(handler)

		// Push some data
		for j := 0; j < 10; j++ {
			data := testDataConsumer(byte(i*10 + j))
			pushTestData(s, data, true)
		}

		s.waitForCalls(10, 100*time.Millisecond)
		s.shutdown(100 * time.Millisecond)
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	allocated := m2.TotalAlloc - m1.TotalAlloc
	heapGrowth := int64(m2.HeapInuse) - int64(m1.HeapInuse)

	t.Logf("Memory usage: allocated=%d bytes, heap_growth=%d bytes",
		allocated, heapGrowth)

	// Validate reasonable memory usage
	if heapGrowth > 1024*1024 { // 1MB growth limit
		t.Errorf("Excessive heap growth: %d bytes", heapGrowth)
	}
}

// ============================================================================
// CONCURRENCY SAFETY VALIDATION
// ============================================================================

// TestPinnedConsumerConcurrentAccess validates thread safety
func TestPinnedConsumerConcurrentAccess(t *testing.T) {
	runtime.GOMAXPROCS(4)

	s := newConsumerTestState(64)

	var accessMu sync.Mutex
	accessCount := make(map[uintptr]int)

	handler := func(data *[24]byte) {
		// Track concurrent access to data
		addr := uintptr(unsafe.Pointer(data))
		accessMu.Lock()
		accessCount[addr]++
		accessMu.Unlock()

		atomic.AddUint32(s.callCount, 1)
		time.Sleep(time.Microsecond) // Simulate processing
	}

	s.launch(handler)

	// Concurrent producers
	var wg sync.WaitGroup
	numProducers := 4
	opsPerProducer := 50

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			atomic.StoreUint32(s.hot, 1)

			for i := 0; i < opsPerProducer; i++ {
				data := testDataConsumer(byte(producerID*100 + i))
				for !s.ring.Push(&data) {
					time.Sleep(time.Microsecond)
				}
			}
		}(p)
	}

	wg.Wait()
	atomic.StoreUint32(s.hot, 0)

	expectedCalls := uint32(numProducers * opsPerProducer)
	if err := s.waitForCalls(expectedCalls, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	if err := s.shutdown(200 * time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Analyze concurrent access patterns
	accessMu.Lock()
	defer accessMu.Unlock()

	totalAccesses := 0
	for _, count := range accessCount {
		totalAccesses += count
		if count > 1 {
			t.Errorf("Concurrent access detected to same memory address: %d accesses", count)
		}
	}

	if totalAccesses != int(expectedCalls) {
		t.Errorf("Access count mismatch: got %d, expected %d", totalAccesses, expectedCalls)
	}
}

// TestPinnedConsumerStopFlagRace validates stop flag race conditions
func TestPinnedConsumerStopFlagRace(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		t.Run(fmt.Sprintf("iteration_%d", iteration), func(t *testing.T) {
			s := newConsumerTestState(8)
			handler := s.countingHandler()
			s.launch(handler)

			// Concurrent stop flag manipulation
			var wg sync.WaitGroup
			wg.Add(3)

			// Multiple stop signalers
			go func() {
				defer wg.Done()
				time.Sleep(time.Duration(iteration) * time.Millisecond)
				atomic.StoreUint32(s.stop, 1)
			}()

			go func() {
				defer wg.Done()
				time.Sleep(time.Duration(iteration+1) * time.Millisecond)
				atomic.StoreUint32(s.stop, 1)
			}()

			// Data producer
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					data := testDataConsumer(byte(i))
					if !s.ring.Push(&data) {
						break
					}
					time.Sleep(time.Microsecond)
				}
			}()

			wg.Wait()

			// Should shutdown despite race conditions
			select {
			case <-s.done:
				// Expected
			case <-time.After(200 * time.Millisecond):
				t.Fatal("Consumer did not shutdown despite stop signal race")
			}
		})
	}
}
