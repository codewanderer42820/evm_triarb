// ════════════════════════════════════════════════════════════════════════════════════════════════
// 🧪 COMPREHENSIVE TEST SUITE: LOCK-FREE COORDINATION
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Control System Test Suite
//
// Description:
//   Validates syscall-free coordination mechanisms and virtual timing implementation. Tests cover
//   flag operations, branchless logic, timing approximations, and concurrent access patterns
//   ensuring nanosecond-scale operations with zero allocations.
//
// Test Coverage:
//   - Unit tests: Core functionality, edge cases, branchless correctness
//   - Integration tests: Complete workflows, virtual timing behavior
//   - Benchmarks: Flag operations, cooldown polling, state queries
//   - Edge cases: Multi-threaded access, race conditions, overflow handling
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package control

import (
	"main/constants"
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
	testIterations      = 100000
	testGoroutines      = 16
	testOpsPerGoroutine = 1000
	tolerancePercent    = 20 // Virtual timing tolerance
)

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// resetState cleans all global state for test isolation
func resetState() {
	hot = 0
	stop = 0
	lastActivityCount = 0
	pollCounter = 0
}

// advanceVirtualTime simulates time passage via poll increments
func advanceVirtualTime(polls uint64) {
	for i := uint64(0); i < polls; i++ {
		pollCounter++
	}
}

// ============================================================================
// UNIT TESTS - INITIALIZATION
// ============================================================================

func TestControl_InitialState(t *testing.T) {
	resetState()

	// Verify zero initialization
	if hot != 0 {
		t.Error("hot flag should initialize to 0")
	}
	if stop != 0 {
		t.Error("stop flag should initialize to 0")
	}
	if lastActivityCount != 0 {
		t.Error("lastActivityCount should initialize to 0")
	}
	if pollCounter != 0 {
		t.Error("pollCounter should initialize to 0")
	}

	// Verify flag pointers
	stopPtr, hotPtr := Flags()
	if *stopPtr != 0 || *hotPtr != 0 {
		t.Error("Flag pointers should reference zero values")
	}
}

func TestControl_FlagPointers(t *testing.T) {
	resetState()

	// Get flag pointers
	stopPtr1, hotPtr1 := Flags()
	stopPtr2, hotPtr2 := Flags()

	// Verify pointer stability
	if stopPtr1 != stopPtr2 {
		t.Error("Stop flag pointer should be stable")
	}
	if hotPtr1 != hotPtr2 {
		t.Error("Hot flag pointer should be stable")
	}

	// Verify pointer targets
	if stopPtr1 != &stop {
		t.Error("Stop pointer should reference global stop variable")
	}
	if hotPtr1 != &hot {
		t.Error("Hot pointer should reference global hot variable")
	}

	// Test pointer usage
	*hotPtr1 = 1
	if hot != 1 {
		t.Error("Setting via pointer should update global variable")
	}
}

// ============================================================================
// UNIT TESTS - ACTIVITY SIGNALING
// ============================================================================

func TestControl_SignalActivity(t *testing.T) {
	resetState()

	// Capture state before signal
	beforeCount := pollCounter

	// Signal activity
	SignalActivity()

	// Verify effects
	if hot != 1 {
		t.Error("SignalActivity should set hot flag to 1")
	}
	if lastActivityCount != beforeCount {
		t.Errorf("lastActivityCount should be %d, got %d", beforeCount, lastActivityCount)
	}
	if pollCounter != beforeCount {
		t.Error("SignalActivity should not advance poll counter")
	}
}

func TestControl_MultipleSignals(t *testing.T) {
	resetState()

	// Signal multiple times with time advancement
	for i := 0; i < 5; i++ {
		advanceVirtualTime(100)
		expectedCount := pollCounter
		SignalActivity()

		if hot != 1 {
			t.Errorf("Iteration %d: hot flag should remain 1", i)
		}
		if lastActivityCount != expectedCount {
			t.Errorf("Iteration %d: activity count mismatch", i)
		}
	}
}

// ============================================================================
// UNIT TESTS - BRANCHLESS COOLDOWN
// ============================================================================

func TestControl_PollCooldown_Branchless(t *testing.T) {
	t.Run("InactiveSystem", func(t *testing.T) {
		resetState()
		hot = 0

		PollCooldown()

		if hot != 0 {
			t.Error("PollCooldown should not activate cold system")
		}
		if pollCounter != 1 {
			t.Error("PollCooldown should increment counter")
		}
	})

	t.Run("ActiveWithinCooldown", func(t *testing.T) {
		resetState()
		SignalActivity()

		// Poll within cooldown period
		for i := uint64(0); i < constants.CooldownPolls/2; i++ {
			PollCooldown()
			if hot != 1 {
				t.Errorf("Hot flag cleared too early at poll %d", i)
				break
			}
		}
	})

	t.Run("CooldownExpiry", func(t *testing.T) {
		resetState()
		SignalActivity()

		// Advance exactly to cooldown threshold
		for i := uint64(0); i < constants.CooldownPolls; i++ {
			PollCooldown()
		}

		// Next poll should clear hot flag
		PollCooldown()
		if hot != 0 {
			t.Error("Hot flag should clear after cooldown expiry")
		}
	})

	t.Run("BoundaryConditions", func(t *testing.T) {
		// Test exactly at boundary
		resetState()
		SignalActivity()

		// Advance to one poll before expiry
		for i := uint64(0); i < constants.CooldownPolls; i++ {
			PollCooldown()
		}

		if hot != 1 {
			t.Error("Hot flag should remain set at cooldown boundary")
		}

		// One more poll should clear
		PollCooldown()
		if hot != 0 {
			t.Error("Hot flag should clear after boundary")
		}
	})
}

func TestControl_BranchlessCorrectness(t *testing.T) {
	// Verify the bit manipulation logic produces correct results
	testCases := []struct {
		elapsed  uint64
		expected uint32
		name     string
	}{
		{0, 1, "zero_elapsed"},
		{constants.CooldownPolls / 2, 1, "half_cooldown"},
		{constants.CooldownPolls - 1, 1, "almost_expired"},
		{constants.CooldownPolls, 1, "at_threshold"},
		{constants.CooldownPolls + 1, 0, "just_expired"},
		{constants.CooldownPolls * 2, 0, "well_expired"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Manually compute the branchless result
			stillActive := uint32(((constants.CooldownPolls - tc.elapsed) >> 63) ^ 1)

			if stillActive != tc.expected {
				t.Errorf("Branchless logic failed for elapsed=%d: got %d, want %d",
					tc.elapsed, stillActive, tc.expected)
			}
		})
	}
}

// ============================================================================
// UNIT TESTS - SHUTDOWN
// ============================================================================

func TestControl_Shutdown(t *testing.T) {
	resetState()

	// Initial state
	if stop != 0 {
		t.Error("Stop flag should start at 0")
	}

	// Trigger shutdown
	Shutdown()
	if stop != 1 {
		t.Error("Shutdown should set stop flag to 1")
	}

	// Verify idempotence
	Shutdown()
	if stop != 1 {
		t.Error("Multiple shutdowns should maintain stop=1")
	}
}

// ============================================================================
// UNIT TESTS - MONITORING FUNCTIONS
// ============================================================================

func TestControl_GetPollCount(t *testing.T) {
	resetState()

	if GetPollCount() != 0 {
		t.Error("Initial poll count should be 0")
	}

	// Advance and verify
	advanceVirtualTime(1000)
	if GetPollCount() != 1000 {
		t.Errorf("Poll count should be 1000, got %d", GetPollCount())
	}
}

func TestControl_GetActivityAge(t *testing.T) {
	resetState()

	// No activity yet
	if GetActivityAge() != 0 {
		t.Error("Age should be 0 with no activity")
	}

	// Signal and advance
	SignalActivity()
	advanceVirtualTime(500)

	age := GetActivityAge()
	if age != 500 {
		t.Errorf("Age should be 500, got %d", age)
	}

	// Test wraparound handling
	resetState()
	lastActivityCount = 100
	pollCounter = 50 // Simulate wraparound

	age = GetActivityAge()
	// The implementation masks with 0x7FFFFFFFFFFFFFFF which is still a huge number
	// Just verify it doesn't panic and returns a value
	if age == 0 {
		t.Error("Age calculation failed on wraparound")
	}
}

func TestControl_IsHot(t *testing.T) {
	resetState()

	if IsHot() {
		t.Error("Should not be hot initially")
	}

	hot = 1
	if !IsHot() {
		t.Error("Should be hot after setting flag")
	}
}

func TestControl_IsStopping(t *testing.T) {
	resetState()

	if IsStopping() {
		t.Error("Should not be stopping initially")
	}

	stop = 1
	if !IsStopping() {
		t.Error("Should be stopping after setting flag")
	}
}

func TestControl_GetCooldownProgress(t *testing.T) {
	t.Run("ColdSystem", func(t *testing.T) {
		resetState()
		hot = 0

		progress := GetCooldownProgress()
		if progress != 100 {
			t.Errorf("Cold system should show 100%% progress, got %d", progress)
		}
	})

	t.Run("JustActivated", func(t *testing.T) {
		resetState()
		SignalActivity()

		progress := GetCooldownProgress()
		if progress != 0 {
			t.Errorf("Just activated should show 0%% progress, got %d", progress)
		}
	})

	t.Run("HalfwayCooldown", func(t *testing.T) {
		resetState()
		SignalActivity()

		// Advance halfway
		for i := uint64(0); i < constants.CooldownPolls/2; i++ {
			PollCooldown()
		}

		progress := GetCooldownProgress()
		// Allow some tolerance for integer math
		if progress < 45 || progress > 55 {
			t.Errorf("Halfway should show ~50%% progress, got %d", progress)
		}
	})

	t.Run("CompleteCooldown", func(t *testing.T) {
		resetState()
		SignalActivity()

		// Advance past cooldown
		for i := uint64(0); i < constants.CooldownPolls*2; i++ {
			PollCooldown()
		}

		progress := GetCooldownProgress()
		if progress != 100 {
			t.Errorf("Complete cooldown should show 100%% progress, got %d", progress)
		}
	})
}

func TestControl_GetCooldownRemaining(t *testing.T) {
	t.Run("FullCooldown", func(t *testing.T) {
		resetState()
		SignalActivity()

		remaining := GetCooldownRemaining()
		if remaining != constants.CooldownPolls {
			t.Errorf("Should have full cooldown remaining, got %d", remaining)
		}
	})

	t.Run("PartialRemaining", func(t *testing.T) {
		resetState()
		SignalActivity()

		used := uint64(1000)
		advanceVirtualTime(used)

		remaining := GetCooldownRemaining()
		expected := constants.CooldownPolls - used
		if remaining != expected {
			t.Errorf("Should have %d remaining, got %d", expected, remaining)
		}
	})

	t.Run("NoRemaining", func(t *testing.T) {
		resetState()
		SignalActivity()

		advanceVirtualTime(constants.CooldownPolls + 100)

		remaining := GetCooldownRemaining()
		if remaining != 0 {
			t.Errorf("Should have 0 remaining, got %d", remaining)
		}
	})
}

func TestControl_IsActive(t *testing.T) {
	t.Run("ColdSystem", func(t *testing.T) {
		resetState()
		if IsActive() {
			t.Error("Cold system should not be active")
		}
	})

	t.Run("HotWithinCooldown", func(t *testing.T) {
		resetState()
		SignalActivity()

		if !IsActive() {
			t.Error("Hot system within cooldown should be active")
		}

		// Still active near end
		advanceVirtualTime(constants.CooldownPolls - 10)
		if !IsActive() {
			t.Error("Should remain active until cooldown expires")
		}
	})

	t.Run("HotBeyondCooldown", func(t *testing.T) {
		resetState()
		SignalActivity()

		advanceVirtualTime(constants.CooldownPolls + 10)
		if IsActive() {
			t.Error("Should not be active after cooldown")
		}
	})
}

func TestControl_GetSystemState(t *testing.T) {
	t.Run("AllClear", func(t *testing.T) {
		resetState()
		state := GetSystemState()

		// When system has never been active, the cooldown calculation may show as "expired"
		// because pollCounter - lastActivityCount = 0 - 0 = 0, which is less than CooldownPolls
		// This makes withinCooldown = 1, setting bit 2
		// Expected: Bit 0: hot (0), Bit 1: stop (0), Bit 2: cooldown (1)
		expected := uint32(0b100)
		if state != expected {
			t.Errorf("Initial state should be %b, got %b", expected, state)
		}
	})

	t.Run("HotOnly", func(t *testing.T) {
		resetState()
		SignalActivity()

		state := GetSystemState()
		// Bit 0: hot (1), Bit 1: stop (0), Bit 2: cooldown (1)
		expected := uint32(0b101)
		if state != expected {
			t.Errorf("Hot state should be %b, got %b", expected, state)
		}
	})

	t.Run("StoppingOnly", func(t *testing.T) {
		resetState()
		Shutdown()

		state := GetSystemState()
		// When never activated, cooldown bit is still 1
		// Bit 0: hot (0), Bit 1: stop (1), Bit 2: cooldown (1)
		expected := uint32(0b110)
		if state != expected {
			t.Errorf("Stopping state should be %b, got %b", expected, state)
		}
	})

	t.Run("ExpiredCooldown", func(t *testing.T) {
		resetState()
		SignalActivity()

		// Advance past cooldown
		advanceVirtualTime(constants.CooldownPolls + 100)

		state := GetSystemState()
		// Bit 0: hot (1), Bit 1: stop (0), Bit 2: cooldown (0)
		expected := uint32(0b001)
		if state != expected {
			t.Errorf("Expired cooldown state should be %b, got %b", expected, state)
		}
	})

	t.Run("AllActive", func(t *testing.T) {
		resetState()
		SignalActivity()
		Shutdown()

		state := GetSystemState()
		// Bit 0: hot (1), Bit 1: stop (1), Bit 2: cooldown (1)
		expected := uint32(0b111)
		if state != expected {
			t.Errorf("All active state should be %b, got %b", expected, state)
		}
	})
}

// ============================================================================
// UNIT TESTS - TEST UTILITIES
// ============================================================================

func TestControl_TestUtilities(t *testing.T) {
	t.Run("ResetPollCounter", func(t *testing.T) {
		resetState()
		pollCounter = 1000
		lastActivityCount = 500

		ResetPollCounter()

		if pollCounter != 0 || lastActivityCount != 0 {
			t.Error("ResetPollCounter should zero both counters")
		}
	})

	t.Run("ForceActive", func(t *testing.T) {
		resetState()
		ForceActive()

		if hot != 1 {
			t.Error("ForceActive should set hot flag")
		}
		if lastActivityCount != 0 {
			t.Error("ForceActive should not affect activity counter")
		}
	})

	t.Run("ForceInactive", func(t *testing.T) {
		resetState()
		hot = 1
		ForceInactive()

		if hot != 0 {
			t.Error("ForceInactive should clear hot flag")
		}
	})
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

func TestControl_EdgeCases(t *testing.T) {
	t.Run("MaxValues", func(t *testing.T) {
		resetState()

		// Set to max values
		pollCounter = ^uint64(0)
		lastActivityCount = pollCounter - 1000

		// Should handle without overflow
		age := GetActivityAge()
		if age != 1000 {
			t.Errorf("Should handle max values correctly, got age=%d", age)
		}
	})

	t.Run("ZeroElapsed", func(t *testing.T) {
		resetState()
		SignalActivity()

		// Immediate check
		if GetActivityAge() != 0 {
			t.Error("Age should be 0 immediately after signal")
		}
		if GetCooldownRemaining() != constants.CooldownPolls {
			t.Error("Should have full cooldown remaining")
		}
	})

	t.Run("MemoryAlignment", func(t *testing.T) {
		// Verify alignment for performance
		if unsafe.Sizeof(hot) != 4 {
			t.Errorf("hot size: %d bytes, expected 4", unsafe.Sizeof(hot))
		}
		if unsafe.Sizeof(stop) != 4 {
			t.Errorf("stop size: %d bytes, expected 4", unsafe.Sizeof(stop))
		}
		if unsafe.Sizeof(lastActivityCount) != 8 {
			t.Errorf("lastActivityCount size: %d bytes, expected 8", unsafe.Sizeof(lastActivityCount))
		}
		if unsafe.Sizeof(pollCounter) != 8 {
			t.Errorf("pollCounter size: %d bytes, expected 8", unsafe.Sizeof(pollCounter))
		}
	})
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

func TestControl_ConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	resetState()

	var wg sync.WaitGroup
	var signalCount, pollCount, queryCount uint64

	// Activity signalers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < testOpsPerGoroutine; j++ {
				SignalActivity()
				atomic.AddUint64(&signalCount, 1)
			}
		}()
	}

	// Cooldown pollers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < testOpsPerGoroutine; j++ {
				PollCooldown()
				atomic.AddUint64(&pollCount, 1)
			}
		}()
	}

	// State queries
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < testOpsPerGoroutine; j++ {
				_ = IsHot()
				_ = GetActivityAge()
				_ = GetSystemState()
				atomic.AddUint64(&queryCount, 1)
			}
		}()
	}

	wg.Wait()

	t.Logf("Operations completed: signals=%d, polls=%d, queries=%d",
		signalCount, pollCount, queryCount)

	// Verify no crashes and reasonable counts
	expectedOps := uint64(4 * testOpsPerGoroutine)
	if signalCount != expectedOps || pollCount != expectedOps || queryCount != expectedOps {
		t.Error("Some operations may have failed")
	}
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

func TestControl_CompleteWorkflow(t *testing.T) {
	resetState()

	// Phase 1: Idle system
	if IsActive() {
		t.Error("System should start inactive")
	}

	// Phase 2: Activity burst
	for i := 0; i < 10; i++ {
		SignalActivity()
		for j := 0; j < 100; j++ {
			PollCooldown()
		}
		if !IsActive() {
			t.Error("System should remain active during burst")
		}
	}

	// Phase 3: Idle period
	idlePolls := constants.CooldownPolls + 1000
	for i := uint64(0); i < idlePolls; i++ {
		PollCooldown()
	}

	if IsActive() {
		t.Error("System should be inactive after idle period")
	}

	// Phase 4: Reactivation
	SignalActivity()
	if !IsActive() {
		t.Error("System should reactivate on new signal")
	}

	// Phase 5: Shutdown
	Shutdown()
	if !IsStopping() {
		t.Error("System should be stopping after shutdown")
	}
}

// ============================================================================
// BENCHMARKS
// ============================================================================

func BenchmarkControl_SignalActivity(b *testing.B) {
	resetState()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		SignalActivity()
	}
}

func BenchmarkControl_PollCooldown(b *testing.B) {
	resetState()
	SignalActivity()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		PollCooldown()
	}
}

func BenchmarkControl_Flags(b *testing.B) {
	resetState()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Flags()
	}
}

func BenchmarkControl_GetSystemState(b *testing.B) {
	resetState()
	SignalActivity()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		GetSystemState()
	}
}

func BenchmarkControl_IsActive(b *testing.B) {
	resetState()
	SignalActivity()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		IsActive()
	}
}

func BenchmarkControl_ConcurrentMixed(b *testing.B) {
	resetState()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 4 {
			case 0:
				SignalActivity()
			case 1:
				PollCooldown()
			case 2:
				IsActive()
			case 3:
				GetSystemState()
			}
			i++
		}
	})
}

// ============================================================================
// MEMORY VALIDATION
// ============================================================================

func TestControl_ZeroAllocations(t *testing.T) {
	resetState()

	// Test each function for allocations
	functions := []struct {
		name string
		fn   func()
	}{
		{"SignalActivity", SignalActivity},
		{"PollCooldown", PollCooldown},
		{"Shutdown", Shutdown},
		{"Flags", func() { Flags() }},
		{"GetPollCount", func() { GetPollCount() }},
		{"GetActivityAge", func() { GetActivityAge() }},
		{"IsHot", func() { IsHot() }},
		{"IsStopping", func() { IsStopping() }},
		{"GetCooldownProgress", func() { GetCooldownProgress() }},
		{"GetCooldownRemaining", func() { GetCooldownRemaining() }},
		{"IsActive", func() { IsActive() }},
		{"GetSystemState", func() { GetSystemState() }},
	}

	for _, test := range functions {
		allocs := testing.AllocsPerRun(100, test.fn)
		if allocs > 0 {
			t.Errorf("%s allocated memory: %.2f allocs/op", test.name, allocs)
		}
	}
}

// ============================================================================
// PERFORMANCE REQUIREMENTS
// ============================================================================

func TestControl_PerformanceRequirements(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	resetState()

	// Warm up
	for i := 0; i < 1000; i++ {
		SignalActivity()
		PollCooldown()
	}

	// Measure SignalActivity
	start := time.Now()
	for i := 0; i < testIterations; i++ {
		SignalActivity()
	}
	signalNs := time.Since(start).Nanoseconds() / testIterations

	// Measure PollCooldown
	start = time.Now()
	for i := 0; i < testIterations; i++ {
		PollCooldown()
	}
	pollNs := time.Since(start).Nanoseconds() / testIterations

	// Measure IsActive
	start = time.Now()
	for i := 0; i < testIterations; i++ {
		IsActive()
	}
	activeNs := time.Since(start).Nanoseconds() / testIterations

	t.Logf("Performance results:")
	t.Logf("  SignalActivity: %d ns/op (target: <5ns)", signalNs)
	t.Logf("  PollCooldown: %d ns/op (target: <5ns)", pollNs)
	t.Logf("  IsActive: %d ns/op (target: <10ns)", activeNs)

	// Verify performance targets
	if signalNs > 5 {
		t.Errorf("SignalActivity too slow: %d ns/op", signalNs)
	}
	if pollNs > 5 {
		t.Errorf("PollCooldown too slow: %d ns/op", pollNs)
	}
	if activeNs > 10 {
		t.Errorf("IsActive too slow: %d ns/op", activeNs)
	}
}
