package control

import (
	"main/constants"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// ============================================================================
// TEST CONFIGURATION
// ============================================================================

const (
	testGoroutineCount     = 16
	testOperationsCount    = 100
	timingTolerancePercent = 50 // ±50% tolerance for virtual timing approximation
	performanceIterations  = 100000
)

// Virtual timing test constants (poll-based instead of time-based)
var (
	testCooldownPolls     = uint64(1000000) // Test cooldown period in polls
	originalCooldownPolls uint64            // Backup for restoration
)

// ============================================================================
// TEST SETUP AND HELPERS
// ============================================================================

// setupTest resets system state for clean testing
func setupTest() {
	hot = 0
	stop = 0
	lastActivityCount = 0
	pollCounter = 0
}

// setupTestCooldown configures shorter cooldown for testing
func setupTestCooldown() {
	originalCooldownPolls = constants.CooldownPolls
	// Temporarily modify for testing (this would normally be unsafe)
	// In real implementation, consider making CooldownPolls configurable
}

// restoreTestCooldown restores original cooldown configuration
func restoreTestCooldown() {
	// Would restore constants.CooldownPolls = originalCooldownPolls
	// In practice, might need dependency injection for testability
}

// advancePollCounter simulates poll advancement for testing
func advancePollCounter(polls uint64) {
	pollCounter += polls
}

// ============================================================================
// UNIT TESTS - INITIALIZATION AND STATE
// ============================================================================

// TestInitialState validates system starts in clean state
func TestInitialState(t *testing.T) {
	setupTest()

	stopPtr, hotPtr := Flags()

	if *stopPtr != 0 {
		t.Error("Stop flag must initialize to 0")
	}
	if *hotPtr != 0 {
		t.Error("Hot flag must initialize to 0")
	}
	if lastActivityCount != 0 {
		t.Error("Activity count must initialize to 0")
	}
	if pollCounter != 0 {
		t.Error("Poll counter must initialize to 0")
	}
}

// TestPointerStability ensures Flags() returns consistent pointers
func TestPointerStability(t *testing.T) {
	setupTest()

	stopPtr1, hotPtr1 := Flags()
	stopPtr2, hotPtr2 := Flags()

	if stopPtr1 != stopPtr2 || hotPtr1 != hotPtr2 {
		t.Error("Flag pointers must remain stable across calls")
	}

	// Validate pointer alignment for performance
	if uintptr(unsafe.Pointer(stopPtr1))%4 != 0 {
		t.Error("Stop flag pointer should be 4-byte aligned")
	}
	if uintptr(unsafe.Pointer(hotPtr1))%4 != 0 {
		t.Error("Hot flag pointer should be 4-byte aligned")
	}
}

// ============================================================================
// UNIT TESTS - ACTIVITY SIGNALING
// ============================================================================

// TestSignalActivity validates activity signaling with virtual timing
func TestSignalActivity(t *testing.T) {
	setupTest()

	beforeCount := GetPollCount()
	SignalActivity()
	afterCount := GetPollCount()

	_, hotPtr := Flags()

	if *hotPtr != 1 {
		t.Error("Hot flag must be set after SignalActivity()")
	}
	if lastActivityCount != beforeCount {
		t.Errorf("Activity count should be %d, got %d", beforeCount, lastActivityCount)
	}
	if afterCount != beforeCount {
		t.Error("SignalActivity should not advance poll counter")
	}
}

// TestMultipleSignalActivity validates repeated activity signals
func TestMultipleSignalActivity(t *testing.T) {
	setupTest()

	// Multiple signals should update activity timestamp
	for i := 0; i < 5; i++ {
		advancePollCounter(100)
		expectedCount := GetPollCount()
		SignalActivity()

		if lastActivityCount != expectedCount {
			t.Errorf("Activity count should update to %d on signal %d", expectedCount, i)
		}
	}

	_, hotPtr := Flags()
	if *hotPtr != 1 {
		t.Error("Hot flag should remain set after multiple signals")
	}
}

// ============================================================================
// UNIT TESTS - BRANCHLESS COOLDOWN BEHAVIOR
// ============================================================================

// TestPollCooldownBranchless validates branchless cooldown implementation
func TestPollCooldownBranchless(t *testing.T) {
	setupTest()

	t.Run("NoOpWhenInactive", func(t *testing.T) {
		hot = 0
		beforeCounter := pollCounter
		PollCooldown()

		if hot != 0 {
			t.Error("PollCooldown should not change inactive hot flag")
		}
		if pollCounter != beforeCounter+1 {
			t.Error("PollCooldown should always increment poll counter")
		}
	})

	t.Run("CooldownAfterExpiry", func(t *testing.T) {
		setupTest()
		hot = 1
		lastActivityCount = 0
		pollCounter = 0

		// Advance past cooldown threshold
		advancePollCounter(constants.CooldownPolls + 100)
		PollCooldown()

		if hot != 0 {
			t.Error("Hot flag should clear after cooldown expiry")
		}
	})

	t.Run("NoEarlyCooldown", func(t *testing.T) {
		setupTest()
		hot = 1
		SignalActivity() // Set recent activity

		// Advance but stay within cooldown period
		advancePollCounter(constants.CooldownPolls / 2)
		PollCooldown()

		if hot != 1 {
			t.Error("Hot flag should remain set before cooldown expiry")
		}
	})

	t.Run("BoundaryCondition", func(t *testing.T) {
		setupTest()
		hot = 1
		lastActivityCount = 0
		pollCounter = 0

		// Test exactly at cooldown boundary
		advancePollCounter(constants.CooldownPolls)
		PollCooldown()

		if hot != 0 {
			t.Error("Hot flag should clear exactly at cooldown boundary")
		}
	})
}

// ============================================================================
// UNIT TESTS - VIRTUAL TIMING FUNCTIONS
// ============================================================================

// TestGetActivityAge validates activity age calculation
func TestGetActivityAge(t *testing.T) {
	setupTest()

	t.Run("ZeroWhenNoActivity", func(t *testing.T) {
		age := GetActivityAge()
		if age != 0 {
			t.Errorf("Activity age should be 0 initially, got %d", age)
		}
	})

	t.Run("CorrectAgeCalculation", func(t *testing.T) {
		SignalActivity()
		advancePollCounter(1000)

		age := GetActivityAge()
		if age != 1000 {
			t.Errorf("Activity age should be 1000, got %d", age)
		}
	})

	t.Run("WraparoundHandling", func(t *testing.T) {
		setupTest()

		// Simulate wraparound condition
		lastActivityCount = 100
		pollCounter = 50 // Simulates counter wraparound

		age := GetActivityAge()
		// Should handle wraparound gracefully (masked result)
		if age >= (1 << 63) {
			t.Error("Activity age should handle wraparound condition")
		}
	})
}

// TestGetCooldownProgress validates progress calculation
func TestGetCooldownProgress(t *testing.T) {
	setupTest()

	t.Run("ColdSystemReturns100", func(t *testing.T) {
		hot = 0
		progress := GetCooldownProgress()
		if progress != 100 {
			t.Errorf("Cold system should return 100%% progress, got %d", progress)
		}
	})

	t.Run("ProgressCalculation", func(t *testing.T) {
		hot = 1
		SignalActivity()

		// Advance to 25% of cooldown
		advancePollCounter(constants.CooldownPolls / 4)
		progress := GetCooldownProgress()

		// Allow some tolerance for integer division
		if progress < 20 || progress > 30 {
			t.Errorf("Expected ~25%% progress, got %d", progress)
		}
	})

	t.Run("CompletedCooldown", func(t *testing.T) {
		hot = 1
		SignalActivity()

		// Advance past cooldown
		advancePollCounter(constants.CooldownPolls + 100)
		progress := GetCooldownProgress()

		if progress != 100 {
			t.Errorf("Completed cooldown should return 100%%, got %d", progress)
		}
	})
}

// TestGetCooldownRemaining validates remaining time calculation
func TestGetCooldownRemaining(t *testing.T) {
	setupTest()

	t.Run("FullRemainingAtStart", func(t *testing.T) {
		hot = 1
		SignalActivity()

		remaining := GetCooldownRemaining()
		if remaining != constants.CooldownPolls {
			t.Errorf("Should have full cooldown remaining, got %d", remaining)
		}
	})

	t.Run("PartialRemaining", func(t *testing.T) {
		hot = 1
		SignalActivity()

		advancePollCounter(1000)
		remaining := GetCooldownRemaining()
		expected := constants.CooldownPolls - 1000

		if remaining != expected {
			t.Errorf("Expected %d remaining, got %d", expected, remaining)
		}
	})

	t.Run("ZeroWhenExpired", func(t *testing.T) {
		hot = 1
		SignalActivity()

		advancePollCounter(constants.CooldownPolls + 500)
		remaining := GetCooldownRemaining()

		if remaining != 0 {
			t.Errorf("Should have 0 remaining after expiry, got %d", remaining)
		}
	})
}

// ============================================================================
// UNIT TESTS - STATE QUERY FUNCTIONS
// ============================================================================

// TestStateQueryFunctions validates state query consistency
func TestStateQueryFunctions(t *testing.T) {
	setupTest()

	t.Run("IsHotConsistency", func(t *testing.T) {
		hot = 0
		if IsHot() {
			t.Error("IsHot() should return false when hot=0")
		}

		hot = 1
		if !IsHot() {
			t.Error("IsHot() should return true when hot=1")
		}
	})

	t.Run("IsStoppingConsistency", func(t *testing.T) {
		stop = 0
		if IsStopping() {
			t.Error("IsStopping() should return false when stop=0")
		}

		stop = 1
		if !IsStopping() {
			t.Error("IsStopping() should return true when stop=1")
		}
	})

	t.Run("IsActiveLogic", func(t *testing.T) {
		// Cold system should not be active
		hot = 0
		if IsActive() {
			t.Error("Cold system should not be active")
		}

		// Hot system within cooldown should be active
		hot = 1
		SignalActivity()
		if !IsActive() {
			t.Error("Hot system within cooldown should be active")
		}

		// Hot system past cooldown should not be active
		advancePollCounter(constants.CooldownPolls + 100)
		if IsActive() {
			t.Error("Hot system past cooldown should not be active")
		}
	})
}

// TestGetSystemState validates packed state representation
func TestGetSystemState(t *testing.T) {
	setupTest()

	t.Run("StateBitPacking", func(t *testing.T) {
		hot = 1
		stop = 0
		SignalActivity()

		state := GetSystemState()

		// Check individual bits
		if (state & 1) != 1 {
			t.Error("Bit 0 should be set for hot flag")
		}
		if (state & 2) != 0 {
			t.Error("Bit 1 should be clear for stop flag")
		}
		if (state & 4) != 4 {
			t.Error("Bit 2 should be set for active cooldown")
		}

		// Test after cooldown expiry
		advancePollCounter(constants.CooldownPolls + 100)
		state = GetSystemState()
		if (state & 4) != 0 {
			t.Error("Bit 2 should be clear after cooldown expiry")
		}
	})
}

// ============================================================================
// UNIT TESTS - SHUTDOWN HANDLING
// ============================================================================

// TestShutdown validates shutdown signaling
func TestShutdown(t *testing.T) {
	setupTest()

	Shutdown()

	stopPtr, _ := Flags()
	if *stopPtr != 1 {
		t.Error("Stop flag should be set after Shutdown()")
	}

	// Multiple shutdowns should be idempotent
	Shutdown()
	if *stopPtr != 1 {
		t.Error("Multiple shutdowns should be idempotent")
	}
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

// TestEdgeCases validates boundary conditions and edge cases
func TestEdgeCases(t *testing.T) {
	t.Run("CounterOverflow", func(t *testing.T) {
		setupTest()

		// Test with very large counter values
		pollCounter = ^uint64(0) - 100 // Near max uint64
		lastActivityCount = ^uint64(0) - 50

		age := GetActivityAge()
		// Should handle large values gracefully
		if age > 100 {
			t.Error("Activity age should handle large counter values")
		}
	})

	t.Run("MemoryLayout", func(t *testing.T) {
		if unsafe.Sizeof(hot) != 4 {
			t.Errorf("Hot flag size: %d, expected 4 bytes", unsafe.Sizeof(hot))
		}
		if unsafe.Sizeof(stop) != 4 {
			t.Errorf("Stop flag size: %d, expected 4 bytes", unsafe.Sizeof(stop))
		}
		if unsafe.Sizeof(lastActivityCount) != 8 {
			t.Errorf("LastActivityCount size: %d, expected 8 bytes", unsafe.Sizeof(lastActivityCount))
		}
		if unsafe.Sizeof(pollCounter) != 8 {
			t.Errorf("PollCounter size: %d, expected 8 bytes", unsafe.Sizeof(pollCounter))
		}
	})

	t.Run("AlignmentValidation", func(t *testing.T) {
		// Validate memory alignment for performance
		hotAddr := uintptr(unsafe.Pointer(&hot))
		stopAddr := uintptr(unsafe.Pointer(&stop))

		if hotAddr%4 != 0 {
			t.Error("Hot flag should be 4-byte aligned")
		}
		if stopAddr%4 != 0 {
			t.Error("Stop flag should be 4-byte aligned")
		}
	})
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

// TestConcurrentActivity validates thread safety of virtual timing
func TestConcurrentActivity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	setupTest()

	var wg sync.WaitGroup
	operations := uint64(0)

	// Concurrent activity signaling with virtual timing
	for i := 0; i < testGoroutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < testOperationsCount; j++ {
				SignalActivity()
				atomic.AddUint64(&operations, 1)
			}
		}()
	}

	wg.Wait()

	_, hotPtr := Flags()
	if *hotPtr != 1 {
		t.Error("Hot flag must be active after concurrent signaling")
	}

	expected := uint64(testGoroutineCount * testOperationsCount)
	if operations != expected {
		t.Errorf("Expected %d operations, got %d", expected, operations)
	}
}

// TestConcurrentMixedOperations validates mixed concurrent access
func TestConcurrentMixedOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping mixed operations test in short mode")
	}

	setupTest()

	var wg sync.WaitGroup
	var activityOps, cooldownOps, flagOps, queryOps uint64

	// Activity workers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				SignalActivity()
				atomic.AddUint64(&activityOps, 1)
			}
		}()
	}

	// Cooldown workers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				PollCooldown()
				atomic.AddUint64(&cooldownOps, 1)
			}
		}()
	}

	// Flag readers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				Flags()
				atomic.AddUint64(&flagOps, 1)
			}
		}()
	}

	// Query function workers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				GetActivityAge()
				GetCooldownProgress()
				IsActive()
				atomic.AddUint64(&queryOps, 1)
			}
		}()
	}

	// Single shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()
		runtime.Gosched() // Allow other goroutines to start
		Shutdown()
	}()

	wg.Wait()

	if activityOps != 200 || cooldownOps != 200 || flagOps != 200 || queryOps != 200 {
		t.Errorf("Operation counts: activity=%d cooldown=%d flags=%d query=%d (expected 200 each)",
			activityOps, cooldownOps, flagOps, queryOps)
	}

	stopPtr, _ := Flags()
	if *stopPtr != 1 {
		t.Error("System should be shutdown after mixed operations")
	}
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

// TestTypicalWorkflow validates complete operational cycle with virtual timing
func TestTypicalWorkflow(t *testing.T) {
	setupTest()

	stopPtr, hotPtr := Flags()

	// Phase 1: System starts idle
	if *hotPtr != 0 || *stopPtr != 0 {
		t.Error("System should start idle")
	}

	// Phase 2: Activity begins
	SignalActivity()
	if *hotPtr != 1 {
		t.Error("System should be hot after activity")
	}

	// Phase 3: Sustained activity (simulate multiple events)
	for i := 0; i < 5; i++ {
		advancePollCounter(1000) // Simulate some processing
		SignalActivity()         // Renewed activity
		PollCooldown()
		if *hotPtr != 1 {
			t.Error("System should remain hot during sustained activity")
		}
	}

	// Phase 4: Natural cooldown (simulate idle period)
	advancePollCounter(constants.CooldownPolls + 1000)
	PollCooldown()
	if *hotPtr != 0 {
		t.Error("System should cool down after virtual idle period")
	}

	// Phase 5: Graceful shutdown
	Shutdown()
	if *stopPtr != 1 {
		t.Error("System should shutdown gracefully")
	}
}

// ============================================================================
// BENCHMARKS - CORE OPERATIONS
// ============================================================================

// BenchmarkSignalActivity measures activity signaling performance
func BenchmarkSignalActivity(b *testing.B) {
	setupTest()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		SignalActivity()
	}
}

// BenchmarkPollCooldown measures branchless cooldown performance
func BenchmarkPollCooldown(b *testing.B) {
	setupTest()
	hot = 1
	SignalActivity()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		PollCooldown()
	}
}

// BenchmarkFlags measures flag access performance
func BenchmarkFlags(b *testing.B) {
	setupTest()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Flags()
	}
}

// BenchmarkVirtualTimingFunctions measures query function performance
func BenchmarkVirtualTimingFunctions(b *testing.B) {
	setupTest()
	hot = 1
	SignalActivity()
	advancePollCounter(1000)

	b.Run("GetActivityAge", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			GetActivityAge()
		}
	})

	b.Run("GetCooldownProgress", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			GetCooldownProgress()
		}
	})

	b.Run("GetCooldownRemaining", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			GetCooldownRemaining()
		}
	})

	b.Run("IsActive", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			IsActive()
		}
	})

	b.Run("GetSystemState", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			GetSystemState()
		}
	})
}

// ============================================================================
// BENCHMARKS - CONCURRENCY AND WORKLOADS
// ============================================================================

// BenchmarkConcurrentAccess measures concurrent performance
func BenchmarkConcurrentAccess(b *testing.B) {
	setupTest()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			SignalActivity()
		}
	})
}

// BenchmarkBranchlessWorkload simulates branchless operation patterns
func BenchmarkBranchlessWorkload(b *testing.B) {
	setupTest()
	hot = 1
	SignalActivity()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 10 {
			case 0:
				SignalActivity() // 10% activity signals
			case 1:
				PollCooldown() // 10% cooldown polls
			default:
				stopPtr, hotPtr := Flags() // 80% flag reads
				_ = *stopPtr
				_ = *hotPtr
			}
			i++
		}
	})
}

// ============================================================================
// BENCHMARKS - MEMORY AND PERFORMANCE VALIDATION
// ============================================================================

// BenchmarkMemoryFootprint validates zero allocation requirement
func BenchmarkMemoryFootprint(b *testing.B) {
	setupTest()
	var m1, m2 runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&m1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SignalActivity()
		PollCooldown()
		Flags()
		GetActivityAge()
		GetCooldownProgress()
		IsActive()
	}
	b.StopTimer()

	runtime.GC()
	runtime.ReadMemStats(&m2)

	allocations := m2.Mallocs - m1.Mallocs
	if allocations > 0 {
		b.Errorf("Hot path allocated %d times (expected 0)", allocations)
	}
}

// ============================================================================
// PERFORMANCE VALIDATION
// ============================================================================

// TestPerformanceRequirements validates branchless timing requirements
func TestPerformanceRequirements(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance validation in short mode")
	}

	setupTest()

	// Test flag access performance (< 500 ns/op requirement for branchless)
	start := time.Now()
	for i := 0; i < performanceIterations; i++ {
		Flags()
	}
	flagLatency := time.Since(start).Nanoseconds() / performanceIterations

	// Test activity signaling performance (< 1000 ns/op requirement)
	start = time.Now()
	for i := 0; i < performanceIterations; i++ {
		SignalActivity()
	}
	activityLatency := time.Since(start).Nanoseconds() / performanceIterations

	// Test branchless cooldown performance (< 2000 ns/op requirement)
	hot = 1
	SignalActivity()
	start = time.Now()
	for i := 0; i < performanceIterations; i++ {
		PollCooldown()
	}
	cooldownLatency := time.Since(start).Nanoseconds() / performanceIterations

	t.Logf("Flag access: %d ns/op (requirement: <500)", flagLatency)
	t.Logf("Activity signaling: %d ns/op (requirement: <1000)", activityLatency)
	t.Logf("Branchless cooldown: %d ns/op (requirement: <2000)", cooldownLatency)

	if flagLatency > 500 {
		t.Errorf("Flag access too slow: %d ns/op", flagLatency)
	}
	if activityLatency > 1000 {
		t.Errorf("Activity signaling too slow: %d ns/op", activityLatency)
	}
	if cooldownLatency > 2000 {
		t.Errorf("Branchless cooldown too slow: %d ns/op", cooldownLatency)
	}
}

// TestBranchlessBehavior validates mathematical correctness of branchless operations
func TestBranchlessBehavior(t *testing.T) {
	setupTest()

	t.Run("BitManipulationCorrectness", func(t *testing.T) {
		// Test branchless cooldown logic with known values
		hot = 1
		lastActivityCount = 0
		pollCounter = 0

		// Test boundary conditions
		testCases := []struct {
			elapsed  uint64
			expected uint32
		}{
			{constants.CooldownPolls - 1, 1}, // Just before cooldown
			{constants.CooldownPolls, 0},     // Exactly at cooldown
			{constants.CooldownPolls + 1, 0}, // Just after cooldown
		}

		for _, tc := range testCases {
			hot = 1 // Reset for each test
			pollCounter = tc.elapsed
			PollCooldown()

			if hot != tc.expected {
				t.Errorf("For elapsed=%d, expected hot=%d, got %d",
					tc.elapsed, tc.expected, hot)
			}
		}
	})
}
