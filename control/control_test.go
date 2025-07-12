// control_test.go — Comprehensive test suite for global control system
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
// TEST CONFIGURATION
// ============================================================================

const (
	testCooldownPeriod  = 50 * time.Millisecond
	testGoroutineCount  = 16
	testOperationsCount = 100
	timingToleranceMs   = 5
)

// ============================================================================
// UNIT TESTS
// ============================================================================

// TestInitialState validates system starts in clean state
func TestInitialState(t *testing.T) {
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
}

// TestPointerStability ensures Flags() returns consistent pointers
func TestPointerStability(t *testing.T) {
	stopPtr1, hotPtr1 := Flags()
	stopPtr2, hotPtr2 := Flags()

	if stopPtr1 != stopPtr2 || hotPtr1 != hotPtr2 {
		t.Error("Flag pointers must remain stable across calls")
	}
}

// TestSignalActivity validates activity signaling
func TestSignalActivity(t *testing.T) {
	hot = 0
	lastHot = 0

	beforeTime := time.Now().UnixNano()
	SignalActivity()
	afterTime := time.Now().UnixNano()

	_, hotPtr := Flags()

	if *hotPtr != 1 {
		t.Error("Hot flag must be set after SignalActivity()")
	}
	if lastHot < beforeTime || lastHot > afterTime {
		t.Error("Activity timestamp must be updated to current time")
	}
}

// TestPollCooldown validates cooldown behavior
func TestPollCooldown(t *testing.T) {
	originalCooldown := cooldownNs
	cooldownNs = int64(testCooldownPeriod)
	defer func() { cooldownNs = originalCooldown }()

	t.Run("NoOpWhenInactive", func(t *testing.T) {
		hot = 0
		PollCooldown()
		if hot != 0 {
			t.Error("PollCooldown should be no-op when hot=0")
		}
	})

	t.Run("CooldownAfterExpiry", func(t *testing.T) {
		hot = 1
		lastHot = time.Now().UnixNano() - int64(testCooldownPeriod) - 1000000 // Past expiry

		PollCooldown()

		if hot != 0 {
			t.Error("Hot flag should clear after cooldown expiry")
		}
	})

	t.Run("NoEarlyCooldown", func(t *testing.T) {
		hot = 1
		lastHot = time.Now().UnixNano() // Recent activity

		PollCooldown()

		if hot != 1 {
			t.Error("Hot flag should remain set before cooldown expiry")
		}
	})
}

// TestShutdown validates shutdown signaling
func TestShutdown(t *testing.T) {
	stop = 0

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
// CONCURRENCY TESTS
// ============================================================================

// TestConcurrentActivity validates thread safety
func TestConcurrentActivity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	hot = 0
	stop = 0
	lastHot = 0

	var wg sync.WaitGroup
	operations := uint64(0)

	// Concurrent activity signaling
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

	hot = 0
	stop = 0
	lastHot = 0

	originalCooldown := cooldownNs
	cooldownNs = int64(5 * time.Millisecond)
	defer func() { cooldownNs = originalCooldown }()

	var wg sync.WaitGroup
	var activityOps, cooldownOps, flagOps uint64

	// Activity workers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				SignalActivity()
				atomic.AddUint64(&activityOps, 1)
				time.Sleep(time.Microsecond)
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
				time.Sleep(time.Microsecond)
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
				runtime.Gosched()
			}
		}()
	}

	// Single shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		Shutdown()
	}()

	wg.Wait()

	if activityOps != 200 || cooldownOps != 200 || flagOps != 200 {
		t.Errorf("Operation counts: activity=%d cooldown=%d flags=%d (expected 200 each)",
			activityOps, cooldownOps, flagOps)
	}

	stopPtr, _ := Flags()
	if *stopPtr != 1 {
		t.Error("System should be shutdown after mixed operations")
	}
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

// TestTypicalWorkflow validates complete operational cycle
func TestTypicalWorkflow(t *testing.T) {
	// Reset system
	hot = 0
	stop = 0
	lastHot = 0

	originalCooldown := cooldownNs
	cooldownNs = int64(20 * time.Millisecond)
	defer func() { cooldownNs = originalCooldown }()

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

	// Phase 3: Sustained activity
	for i := 0; i < 3; i++ {
		time.Sleep(5 * time.Millisecond)
		SignalActivity()
		PollCooldown()
		if *hotPtr != 1 {
			t.Error("System should remain hot during sustained activity")
		}
	}

	// Phase 4: Natural cooldown
	time.Sleep(25 * time.Millisecond)
	PollCooldown()
	if *hotPtr != 0 {
		t.Error("System should cool down after inactivity")
	}

	// Phase 5: Graceful shutdown
	Shutdown()
	if *stopPtr != 1 {
		t.Error("System should shutdown gracefully")
	}
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

// TestEdgeCases validates boundary conditions
func TestEdgeCases(t *testing.T) {
	t.Run("ZeroCooldown", func(t *testing.T) {
		originalCooldown := cooldownNs
		cooldownNs = 0
		defer func() { cooldownNs = originalCooldown }()

		hot = 1
		lastHot = time.Now().UnixNano() - 1

		PollCooldown()

		if hot != 0 {
			t.Error("Zero cooldown should immediately clear hot flag")
		}
	})

	t.Run("FutureTimestamp", func(t *testing.T) {
		originalCooldown := cooldownNs
		cooldownNs = int64(10 * time.Millisecond)
		defer func() { cooldownNs = originalCooldown }()

		hot = 1
		lastHot = time.Now().UnixNano() + int64(time.Hour) // Future time

		PollCooldown()

		if hot != 1 {
			t.Error("Future timestamp should not trigger cooldown")
		}
	})

	t.Run("MemoryLayout", func(t *testing.T) {
		if unsafe.Sizeof(hot) != 4 {
			t.Errorf("Hot flag size: %d, expected 4 bytes", unsafe.Sizeof(hot))
		}
		if unsafe.Sizeof(stop) != 4 {
			t.Errorf("Stop flag size: %d, expected 4 bytes", unsafe.Sizeof(stop))
		}
		if unsafe.Sizeof(lastHot) != 8 {
			t.Errorf("LastHot size: %d, expected 8 bytes", unsafe.Sizeof(lastHot))
		}
	})
}

// ============================================================================
// BENCHMARKS
// ============================================================================

// BenchmarkSignalActivity measures activity signaling performance
func BenchmarkSignalActivity(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		SignalActivity()
	}
}

// BenchmarkPollCooldown measures cooldown polling performance
func BenchmarkPollCooldown(b *testing.B) {
	hot = 1
	lastHot = time.Now().UnixNano()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		PollCooldown()
	}
}

// BenchmarkFlags measures flag access performance
func BenchmarkFlags(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Flags()
	}
}

// BenchmarkFlagsWithRead measures flag access + read performance
func BenchmarkFlagsWithRead(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stopPtr, hotPtr := Flags()
		_ = *stopPtr
		_ = *hotPtr
	}
}

// BenchmarkConcurrentAccess measures concurrent performance
func BenchmarkConcurrentAccess(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			SignalActivity()
		}
	})
}

// BenchmarkTypicalWorkload simulates realistic usage patterns
func BenchmarkTypicalWorkload(b *testing.B) {
	originalCooldown := cooldownNs
	cooldownNs = int64(100 * time.Millisecond)
	defer func() { cooldownNs = originalCooldown }()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 20 {
			case 0:
				SignalActivity() // 5% activity signals
			case 1:
				PollCooldown() // 5% cooldown polls
			default:
				stopPtr, hotPtr := Flags() // 90% flag reads
				_ = *stopPtr
				_ = *hotPtr
			}
			i++
		}
	})
}

// BenchmarkMemoryFootprint validates zero allocation requirement
func BenchmarkMemoryFootprint(b *testing.B) {
	var m1, m2 runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&m1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SignalActivity()
		PollCooldown()
		Flags()
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

// TestPerformanceRequirements validates timing requirements
func TestPerformanceRequirements(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance validation in short mode")
	}

	const iterations = 100000

	// Test flag access performance (< 1000 ns/op requirement)
	start := time.Now()
	for i := 0; i < iterations; i++ {
		Flags()
	}
	flagLatency := time.Since(start).Nanoseconds() / iterations

	// Test activity signaling performance (< 5000 ns/op requirement)
	start = time.Now()
	for i := 0; i < iterations; i++ {
		SignalActivity()
	}
	activityLatency := time.Since(start).Nanoseconds() / iterations

	t.Logf("Flag access: %d ns/op (requirement: <1000)", flagLatency)
	t.Logf("Activity signaling: %d ns/op (requirement: <5000)", activityLatency)

	if flagLatency > 1000 {
		t.Errorf("Flag access too slow: %d ns/op", flagLatency)
	}
	if activityLatency > 5000 {
		t.Errorf("Activity signaling too slow: %d ns/op", activityLatency)
	}
}
