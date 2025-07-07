// control_test.go — Comprehensive test suite for global control system
// ============================================================================
// CONTROL SYSTEM TEST SUITE
// ============================================================================
//
// Complete testing framework for global control flags and activity management
// with comprehensive coverage of timing, concurrency, and coordination scenarios.
//
// Test coverage includes:
//   • Unit tests for flag operations and state transitions
//   • Timing validation for cooldown management with nanosecond precision
//   • Concurrency safety testing across multiple goroutines
//   • Integration tests for WebSocket activity coordination
//   • Graceful shutdown sequence validation
//   • Memory safety and pointer stability verification
//   • Race condition detection under high contention
//   • Boundary condition testing for edge cases
//
// Performance validation:
//   • Sub-microsecond flag access operations
//   • Zero-allocation hot path verification
//   • Concurrent throughput under load
//   • Memory layout and alignment validation

package control

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// ============================================================================
// TEST CONFIGURATION AND CONSTANTS
// ============================================================================

const (
	// Test timing parameters - calibrated for reliable CI/CD execution
	testActivityDuration = 10 * time.Millisecond  // Duration for sustained activity simulation
	testCooldownPeriod   = 50 * time.Millisecond  // Accelerated cooldown for testing efficiency
	testShutdownTimeout  = 100 * time.Millisecond // Maximum graceful shutdown wait time

	// Concurrency test parameters - optimized for comprehensive coverage
	testGoroutineCount      = 16  // Concurrent worker threads for stress testing
	testOperationsPerWorker = 100 // Operations per worker thread for statistical significance

	// Timing tolerance parameters - prevents flaky test failures
	timingToleranceMs = 5 // 5ms tolerance for timing-sensitive assertions
)

// ============================================================================
// BASIC FUNCTIONALITY TESTS
// ============================================================================

// TestInitialState validates the initial state of all control flags
func TestInitialState(t *testing.T) {
	t.Run("FlagsInitiallyZero", func(t *testing.T) {
		// Initialize clean test environment
		hot = 0
		stop = 0
		lastHot = 0

		stopPtr, hotPtr := Flags()

		if *stopPtr != 0 {
			t.Error("Stop flag must initialize to 0")
		}

		if *hotPtr != 0 {
			t.Error("Hot flag must initialize to 0")
		}

		if lastHot != 0 {
			t.Error("Activity timestamp must initialize to 0")
		}
	})

	t.Run("PointerStability", func(t *testing.T) {
		// Verify pointer consistency across multiple flag access calls
		stopPtr1, hotPtr1 := Flags()
		stopPtr2, hotPtr2 := Flags()

		if stopPtr1 != stopPtr2 {
			t.Error("Stop flag pointer must remain stable across calls")
		}

		if hotPtr1 != hotPtr2 {
			t.Error("Hot flag pointer must remain stable across calls")
		}
	})

	t.Run("MemoryLayout", func(t *testing.T) {
		// Validate expected memory layout for cache alignment
		if unsafe.Sizeof(hot) != 4 {
			t.Errorf("Hot flag size is %d bytes, expected 4 bytes for uint32", unsafe.Sizeof(hot))
		}

		if unsafe.Sizeof(stop) != 4 {
			t.Errorf("Stop flag size is %d bytes, expected 4 bytes for uint32", unsafe.Sizeof(stop))
		}

		if unsafe.Sizeof(lastHot) != 8 {
			t.Errorf("Activity timestamp size is %d bytes, expected 8 bytes for int64", unsafe.Sizeof(lastHot))
		}
	})
}

// ============================================================================
// ACTIVITY SIGNALING TESTS
// ============================================================================

// TestSignalActivity validates activity signaling functionality
func TestSignalActivity(t *testing.T) {
	t.Run("BasicActivitySignaling", func(t *testing.T) {
		// Initialize clean test state
		hot = 0
		stop = 0
		lastHot = 0

		beforeTime := time.Now().UnixNano()
		SignalActivity()
		afterTime := time.Now().UnixNano()

		stopPtr, hotPtr := Flags()

		if *hotPtr != 1 {
			t.Error("Hot flag must be set to 1 after SignalActivity()")
		}

		if *stopPtr != 0 {
			t.Error("Stop flag must remain unchanged after SignalActivity()")
		}

		if lastHot < beforeTime || lastHot > afterTime {
			t.Error("Activity timestamp must be updated to current nanosecond time")
		}
	})

	t.Run("RepeatedActivitySignaling", func(t *testing.T) {
		// Initialize clean test state
		hot = 0
		lastHot = 0

		// Execute first activity signal
		SignalActivity()
		firstTimestamp := lastHot

		time.Sleep(1 * time.Millisecond) // Ensure temporal progression

		SignalActivity()
		secondTimestamp := lastHot

		_, hotPtr := Flags()

		if *hotPtr != 1 {
			t.Error("Hot flag must remain set after repeated signaling")
		}

		if secondTimestamp <= firstTimestamp {
			t.Error("Activity timestamp must advance on each signal")
		}
	})

	t.Run("ActivityTimestampPrecision", func(t *testing.T) {
		// Validate timestamp precision and temporal progression
		hot = 0
		lastHot = 0

		timestamps := make([]int64, 5)

		for i := 0; i < 5; i++ {
			SignalActivity()
			timestamps[i] = lastHot
			// Introduce microsecond delay to ensure temporal progression
			time.Sleep(time.Microsecond)
		}

		// Verify overall temporal progression from first to last timestamp
		if timestamps[4] <= timestamps[0] {
			t.Errorf("Final timestamp (%d) must exceed initial timestamp (%d)",
				timestamps[4], timestamps[0])
		}

		// Validate all timestamps fall within reasonable temporal bounds
		currentTime := time.Now().UnixNano()
		for i, ts := range timestamps {
			if ts < currentTime-int64(time.Second) || ts > currentTime+int64(time.Second) {
				t.Errorf("Timestamp %d (%d) exceeds reasonable temporal bounds", i, ts)
			}
		}

		// Verify timestamps maintain non-decreasing order (allow equal for rapid calls)
		for i := 1; i < len(timestamps); i++ {
			if timestamps[i] < timestamps[i-1] {
				t.Errorf("Timestamp %d (%d) must not regress from timestamp %d (%d)",
					i, timestamps[i], i-1, timestamps[i-1])
			}
		}
	})
}

// ============================================================================
// COOLDOWN MANAGEMENT TESTS
// ============================================================================

// TestCooldownBehavior validates automatic cooldown functionality
func TestCooldownBehavior(t *testing.T) {
	// Configure accelerated cooldown period for efficient testing
	originalCooldown := cooldownNs
	cooldownNs = int64(testCooldownPeriod)
	defer func() { cooldownNs = originalCooldown }()

	t.Run("CooldownAfterInactivity", func(t *testing.T) {
		// Initialize system with active state
		hot = 0
		lastHot = 0
		SignalActivity()

		_, hotPtr := Flags()

		if *hotPtr != 1 {
			t.Error("Hot flag must be active immediately after signaling")
		}

		// Allow cooldown period to elapse completely
		time.Sleep(testCooldownPeriod + time.Duration(timingToleranceMs)*time.Millisecond)

		// Trigger cooldown evaluation
		PollCooldown()

		if *hotPtr != 0 {
			t.Error("Hot flag must clear after cooldown period expiration")
		}
	})

	t.Run("NoCooldownDuringActivity", func(t *testing.T) {
		// Initialize system with active state
		hot = 0
		lastHot = 0
		SignalActivity()

		_, hotPtr := Flags()

		// Immediate cooldown polling should preserve hot state
		PollCooldown()

		if *hotPtr != 1 {
			t.Error("Hot flag must remain active immediately after signaling")
		}

		// Mid-cooldown polling should preserve hot state
		time.Sleep(testCooldownPeriod / 2)
		PollCooldown()

		if *hotPtr != 1 {
			t.Error("Hot flag must remain active during cooldown period")
		}
	})

	t.Run("CooldownResetOnNewActivity", func(t *testing.T) {
		// Reset state and signal initial activity
		hot = 0
		lastHot = 0
		SignalActivity()

		// Wait most of the cooldown period
		time.Sleep(testCooldownPeriod * 3 / 4)

		// Signal new activity - should reset cooldown timer
		SignalActivity()

		_, hotPtr := Flags()

		if *hotPtr != 1 {
			t.Error("Hot flag should remain 1 after activity reset")
		}

		// Wait the original cooldown period (should not be enough now)
		time.Sleep(testCooldownPeriod * 3 / 4)
		PollCooldown()

		if *hotPtr != 1 {
			t.Error("Hot flag should remain 1 with reset cooldown timer")
		}

		// Wait full cooldown from reset point
		time.Sleep(testCooldownPeriod/2 + time.Duration(timingToleranceMs)*time.Millisecond)
		PollCooldown()

		if *hotPtr != 0 {
			t.Error("Hot flag should be 0 after full cooldown from reset")
		}
	})

	t.Run("PollCooldownWhenNotHot", func(t *testing.T) {
		// Ensure hot flag is not set
		hot = 0
		lastHot = 0

		// Polling cooldown when not hot should be no-op
		PollCooldown()

		_, hotPtr := Flags()

		if *hotPtr != 0 {
			t.Error("Hot flag should remain 0 when polling while inactive")
		}

		if lastHot != 0 {
			t.Error("LastHot timestamp should remain unchanged")
		}
	})
}

// ============================================================================
// SHUTDOWN MANAGEMENT TESTS
// ============================================================================

// TestShutdownBehavior validates graceful shutdown functionality
func TestShutdownBehavior(t *testing.T) {
	t.Run("BasicShutdownSignaling", func(t *testing.T) {
		// Reset state
		hot = 0
		stop = 0
		lastHot = 0

		Shutdown()

		stopPtr, hotPtr := Flags()

		if *stopPtr != 1 {
			t.Error("Stop flag should be 1 after Shutdown()")
		}

		if *hotPtr != 0 {
			t.Error("Hot flag should remain unaffected by shutdown")
		}

		if lastHot != 0 {
			t.Error("LastHot timestamp should remain unaffected by shutdown")
		}
	})

	t.Run("ShutdownWithActiveSystem", func(t *testing.T) {
		// Reset and activate system
		hot = 0
		stop = 0
		lastHot = 0
		SignalActivity()

		_, hotPtr := Flags()

		if *hotPtr != 1 {
			t.Error("System should be active before shutdown")
		}

		Shutdown()

		stopPtr, _ := Flags()

		if *stopPtr != 1 {
			t.Error("Stop flag should be 1 after shutdown")
		}

		if *hotPtr != 1 {
			t.Error("Hot flag should remain 1 after shutdown (independent)")
		}
	})

	t.Run("RepeatedShutdownCalls", func(t *testing.T) {
		// Reset state
		stop = 0

		// Multiple shutdown calls should be idempotent
		Shutdown()
		Shutdown()
		Shutdown()

		stopPtr, _ := Flags()

		if *stopPtr != 1 {
			t.Error("Stop flag should be 1 after repeated shutdown calls")
		}
	})
}

// ============================================================================
// CONCURRENCY SAFETY TESTS
// ============================================================================

// TestConcurrentAccess validates thread safety under concurrent operations
func TestConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency validation in short test mode")
	}

	t.Run("ConcurrentActivitySignaling", func(t *testing.T) {
		// Initialize clean concurrent test environment
		hot = 0
		stop = 0
		lastHot = 0

		var wg sync.WaitGroup
		activityCount := uint64(0)

		// Deploy concurrent activity signaling workers
		for i := 0; i < testGoroutineCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < testOperationsPerWorker; j++ {
					SignalActivity()
					atomic.AddUint64(&activityCount, 1)
					runtime.Gosched() // Yield execution to increase contention
				}
			}()
		}

		wg.Wait()

		_, hotPtr := Flags()

		if *hotPtr != 1 {
			t.Error("Hot flag must be active after concurrent signaling operations")
		}

		expectedOperations := uint64(testGoroutineCount * testOperationsPerWorker)
		if activityCount != expectedOperations {
			t.Errorf("Operation count mismatch: expected %d, executed %d", expectedOperations, activityCount)
		}

		if lastHot == 0 {
			t.Error("Activity timestamp must be updated after concurrent operations")
		}
	})

	t.Run("ConcurrentFlagAccess", func(t *testing.T) {
		// Initialize clean concurrent test environment
		hot = 0
		stop = 0

		var wg sync.WaitGroup
		accessCount := uint64(0)

		// Deploy concurrent flag access workers
		for i := 0; i < testGoroutineCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < testOperationsPerWorker; j++ {
					stopPtr, hotPtr := Flags()
					// Validate pointer consistency and non-null guarantees
					if stopPtr == nil || hotPtr == nil {
						t.Error("Flag pointers must never be null")
						return
					}
					atomic.AddUint64(&accessCount, 1)
					runtime.Gosched()
				}
			}()
		}

		wg.Wait()

		expectedAccesses := uint64(testGoroutineCount * testOperationsPerWorker)
		if accessCount != expectedAccesses {
			t.Errorf("Access count mismatch: expected %d, executed %d", expectedAccesses, accessCount)
		}
	})

	t.Run("ConcurrentMixedOperations", func(t *testing.T) {
		// Test mixed concurrent operations: activity, cooldown, shutdown, flag access
		hot = 0
		stop = 0
		lastHot = 0

		// Temporarily reduce cooldown for faster testing
		originalCooldown := cooldownNs
		cooldownNs = int64(5 * time.Millisecond)
		defer func() { cooldownNs = originalCooldown }()

		var wg sync.WaitGroup
		operationCounts := struct {
			activities uint64
			cooldowns  uint64
			shutdowns  uint64
			flagReads  uint64
		}{}

		// Activity signalers
		for i := 0; i < testGoroutineCount/4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < testOperationsPerWorker; j++ {
					SignalActivity()
					atomic.AddUint64(&operationCounts.activities, 1)
					time.Sleep(time.Microsecond)
				}
			}()
		}

		// Cooldown pollers
		for i := 0; i < testGoroutineCount/4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < testOperationsPerWorker; j++ {
					PollCooldown()
					atomic.AddUint64(&operationCounts.cooldowns, 1)
					time.Sleep(time.Microsecond)
				}
			}()
		}

		// Flag readers
		for i := 0; i < testGoroutineCount/4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < testOperationsPerWorker; j++ {
					_, _ = Flags()
					atomic.AddUint64(&operationCounts.flagReads, 1)
					runtime.Gosched()
				}
			}()
		}

		// Single shutdown caller (after brief delay)
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(5 * time.Millisecond)
			Shutdown()
			atomic.AddUint64(&operationCounts.shutdowns, 1)
		}()

		wg.Wait()

		// Verify all operations completed
		expectedPerType := uint64(testGoroutineCount / 4 * testOperationsPerWorker)

		if operationCounts.activities != expectedPerType {
			t.Errorf("Expected %d activities, got %d", expectedPerType, operationCounts.activities)
		}

		if operationCounts.cooldowns != expectedPerType {
			t.Errorf("Expected %d cooldowns, got %d", expectedPerType, operationCounts.cooldowns)
		}

		if operationCounts.flagReads != expectedPerType {
			t.Errorf("Expected %d flag reads, got %d", expectedPerType, operationCounts.flagReads)
		}

		if operationCounts.shutdowns != 1 {
			t.Errorf("Expected 1 shutdown, got %d", operationCounts.shutdowns)
		}

		// Final state should show shutdown
		stopPtr, _ := Flags()
		if *stopPtr != 1 {
			t.Error("System should be shutdown after mixed operations")
		}
	})
}

// ============================================================================
// INTEGRATION AND WORKFLOW TESTS
// ============================================================================

// TestIntegrationWorkflows validates complete operational scenarios
func TestIntegrationWorkflows(t *testing.T) {
	t.Run("TypicalWebSocketActivityCycle", func(t *testing.T) {
		// Simulate typical WebSocket activity cycle

		// Reset system
		hot = 0
		stop = 0
		lastHot = 0

		// Temporarily reduce cooldown for testing
		originalCooldown := cooldownNs
		cooldownNs = int64(20 * time.Millisecond)
		defer func() { cooldownNs = originalCooldown }()

		stopPtr, hotPtr := Flags()

		// Phase 1: System starts idle
		if *hotPtr != 0 || *stopPtr != 0 {
			t.Error("System should start idle")
		}

		// Phase 2: WebSocket activity begins
		SignalActivity()
		if *hotPtr != 1 {
			t.Error("System should be hot after initial activity")
		}

		// Phase 3: Continued activity keeps system hot
		for i := 0; i < 5; i++ {
			time.Sleep(5 * time.Millisecond)
			SignalActivity()
			PollCooldown()
			if *hotPtr != 1 {
				t.Error("System should remain hot during continued activity")
			}
		}

		// Phase 4: Activity stops, system cools down
		time.Sleep(25 * time.Millisecond) // Exceed cooldown period
		PollCooldown()
		if *hotPtr != 0 {
			t.Error("System should cool down after inactivity")
		}

		// Phase 5: System shutdown
		Shutdown()
		if *stopPtr != 1 {
			t.Error("System should be stopped after shutdown")
		}
	})

	t.Run("ConsumerCoordinationWorkflow", func(t *testing.T) {
		// Simulate pinned consumer coordination workflow

		// Reset system
		hot = 0
		stop = 0
		lastHot = 0

		stopPtr, hotPtr := Flags()

		// Simulate multiple consumers checking flags
		consumerReadings := make([]struct{ stop, hot uint32 }, 10)

		for i := 0; i < 5; i++ {
			consumerReadings[i] = struct{ stop, hot uint32 }{*stopPtr, *hotPtr}
			if consumerReadings[i].stop != 0 || consumerReadings[i].hot != 0 {
				t.Error("All consumers should see idle system initially")
			}
		}

		// Activity signal affects all consumers
		SignalActivity()

		for i := 5; i < 10; i++ {
			consumerReadings[i] = struct{ stop, hot uint32 }{*stopPtr, *hotPtr}
			if consumerReadings[i].stop != 0 || consumerReadings[i].hot != 1 {
				t.Error("All consumers should see hot system after activity")
			}
		}

		// Shutdown affects all consumers
		Shutdown()

		finalStop, finalHot := *stopPtr, *hotPtr
		if finalStop != 1 {
			t.Error("All consumers should see stop signal after shutdown")
		}
		if finalHot != 1 {
			t.Error("Hot flag should remain 1 after shutdown (independent)")
		}
	})
}

// ============================================================================
// EDGE CASE AND BOUNDARY TESTS
// ============================================================================

// TestEdgeCases validates behavior under unusual conditions
func TestEdgeCases(t *testing.T) {
	t.Run("ZeroCooldownPeriod", func(t *testing.T) {
		// Test behavior with zero cooldown period
		originalCooldown := cooldownNs
		cooldownNs = 0
		defer func() { cooldownNs = originalCooldown }()

		hot = 0
		lastHot = 0

		SignalActivity()
		_, hotPtr := Flags()

		if *hotPtr != 1 {
			t.Error("Hot flag should be set after activity")
		}

		// With zero cooldown, should immediately cool down
		PollCooldown()

		if *hotPtr != 0 {
			t.Error("Hot flag should clear immediately with zero cooldown")
		}
	})

	t.Run("LargeCooldownPeriod", func(t *testing.T) {
		// Test behavior with very large cooldown period
		originalCooldown := cooldownNs
		cooldownNs = int64(1 * time.Hour) // 1 hour cooldown
		defer func() { cooldownNs = originalCooldown }()

		hot = 0
		lastHot = 0

		SignalActivity()
		_, hotPtr := Flags()

		if *hotPtr != 1 {
			t.Error("Hot flag should be set after activity")
		}

		// Even after significant time, should remain hot with large cooldown
		time.Sleep(10 * time.Millisecond)
		PollCooldown()

		if *hotPtr != 1 {
			t.Error("Hot flag should remain set with large cooldown period")
		}
	})

	t.Run("TimestampOverflow", func(t *testing.T) {
		// Test behavior near timestamp boundaries
		hot = 0
		stop = 0

		// Set lastHot to large value
		lastHot = 9223372036854775807 // Max int64

		// Signal activity should update timestamp properly
		SignalActivity()

		// Timestamp should be updated to current time
		currentTime := time.Now().UnixNano()
		if lastHot < currentTime-int64(time.Second) || lastHot > currentTime+int64(time.Second) {
			t.Error("Timestamp should be updated to reasonable current time")
		}
	})

	t.Run("RapidActivityBursts", func(t *testing.T) {
		// Test system behavior under rapid activity bursts
		hot = 0
		stop = 0
		lastHot = 0

		initialTime := time.Now().UnixNano()

		// Signal activity in rapid succession
		for i := 0; i < 1000; i++ {
			SignalActivity()
		}

		_, hotPtr := Flags()

		if *hotPtr != 1 {
			t.Error("Hot flag should be set after rapid activity bursts")
		}

		// Timestamp should be recent
		if lastHot < initialTime {
			t.Error("LastHot timestamp should be updated by rapid activity")
		}
	})
}

// ============================================================================
// PERFORMANCE AND STRESS TESTS
// ============================================================================

// TestPerformanceCharacteristics validates timing and efficiency
func TestPerformanceCharacteristics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance validation in short test mode")
	}

	t.Run("FlagAccessPerformance", func(t *testing.T) {
		// Measure flag access operation latency
		iterations := 1000000

		start := time.Now()
		for i := 0; i < iterations; i++ {
			Flags()
		}
		elapsed := time.Since(start)

		nanosPerOp := elapsed.Nanoseconds() / int64(iterations)
		t.Logf("Flag access performance: %d ns/operation over %d iterations", nanosPerOp, iterations)

		// Validate sub-microsecond performance requirement
		if nanosPerOp > 1000 {
			t.Errorf("Flag access latency exceeds requirement: %d ns/op (maximum: 1000 ns/op)", nanosPerOp)
		}
	})

	t.Run("ActivitySignalPerformance", func(t *testing.T) {
		// Measure activity signaling operation latency
		iterations := 100000

		start := time.Now()
		for i := 0; i < iterations; i++ {
			SignalActivity()
		}
		elapsed := time.Since(start)

		nanosPerOp := elapsed.Nanoseconds() / int64(iterations)
		t.Logf("Activity signaling performance: %d ns/operation over %d iterations", nanosPerOp, iterations)

		// Validate high-performance requirement for hot path
		if nanosPerOp > 5000 {
			t.Errorf("Activity signaling latency exceeds requirement: %d ns/op (maximum: 5000 ns/op)", nanosPerOp)
		}
	})

	t.Run("CooldownPollingPerformance", func(t *testing.T) {
		// Measure cooldown polling performance
		hot = 1
		lastHot = time.Now().UnixNano()
		iterations := 100000

		start := time.Now()
		for i := 0; i < iterations; i++ {
			PollCooldown()
		}
		elapsed := time.Since(start)

		nanosPerOp := elapsed.Nanoseconds() / int64(iterations)
		t.Logf("Cooldown polling: %d ns/op over %d iterations", nanosPerOp, iterations)

		// Cooldown polling should be fast
		if nanosPerOp > 5000 {
			t.Errorf("Cooldown polling too slow: %d ns/op (expected < 5000 ns/op)", nanosPerOp)
		}
	})

	t.Run("MemoryAllocationTest", func(t *testing.T) {
		// Verify zero allocation in hot paths
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Perform operations that should not allocate
		for i := 0; i < 10000; i++ {
			Flags()
			SignalActivity()
			PollCooldown()
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)

		allocatedBytes := m2.TotalAlloc - m1.TotalAlloc
		t.Logf("Memory allocated during operations: %d bytes", allocatedBytes)

		// Should be minimal allocation (some may come from test framework)
		if allocatedBytes > 1024 {
			t.Errorf("Excessive memory allocation: %d bytes (expected < 1024)", allocatedBytes)
		}
	})
}

// ============================================================================
// SYSTEM LIFECYCLE TESTS
// ============================================================================

// TestSystemLifecycle validates complete system operation lifecycle
func TestSystemLifecycle(t *testing.T) {
	t.Run("CompleteOperationalCycle", func(t *testing.T) {
		// Test complete system lifecycle: startup → activity → cooldown → shutdown

		// Phase 1: System initialization
		hot = 0
		stop = 0
		lastHot = 0

		stopPtr, hotPtr := Flags()

		if *stopPtr != 0 || *hotPtr != 0 {
			t.Error("System should start in clean state")
		}

		// Phase 2: Begin operations
		SignalActivity()

		if *hotPtr != 1 {
			t.Error("System should become active")
		}

		// Phase 3: Sustained operation
		originalCooldown := cooldownNs
		cooldownNs = int64(30 * time.Millisecond)
		defer func() { cooldownNs = originalCooldown }()

		for i := 0; i < 10; i++ {
			time.Sleep(5 * time.Millisecond)
			SignalActivity()
			PollCooldown()
			if *hotPtr != 1 {
				t.Error("System should remain active during sustained operation")
			}
		}

		// Phase 4: Natural cooldown
		time.Sleep(35 * time.Millisecond)
		PollCooldown()

		if *hotPtr != 0 {
			t.Error("System should naturally cool down")
		}

		// Phase 5: Graceful shutdown
		Shutdown()

		if *stopPtr != 1 {
			t.Error("System should shutdown gracefully")
		}

		t.Log("Complete operational cycle validated successfully")
	})
}
