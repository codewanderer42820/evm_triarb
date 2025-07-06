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
//
// Validation methodology:
//   - Multi-core environment setup for realistic testing
//   - Pure memory side-effect callbacks for deterministic validation
//   - Atomic operations for thread-safe state coordination
//   - Time-based testing for temporal behavior verification
//
// Performance assumptions:
//   - Sub-microsecond callback invocation latency
//   - Immediate response to producer activity signals
//   - Graceful degradation during idle periods
//   - Deterministic shutdown within bounded time limits

package ring24

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// TEST UTILITIES
// ============================================================================

// launch creates and starts a pinned consumer for testing purposes.
// Provides convenient access to control flags and completion signaling.
//
// Returns:
//
//	stop: Atomic flag for shutdown signaling (set to 1 to terminate)
//	hot: Atomic flag for producer activity indication
//	done: Channel closed when consumer completes shutdown
func launch(r *Ring, fn func(*[24]byte)) (stop, hot *uint32, done chan struct{}) {
	stop = new(uint32)
	hot = new(uint32)
	done = make(chan struct{})
	PinnedConsumer(0, r, stop, hot, fn, done)
	return
}

// ============================================================================
// BASIC FUNCTIONALITY VALIDATION
// ============================================================================

// TestPinnedConsumerDeliversItem validates core consumer functionality.
// Tests callback invocation, data delivery, and graceful shutdown coordination.
//
// Test sequence:
//  1. Launch pinned consumer with data capture callback
//  2. Signal producer activity and push test data
//  3. Wait for callback execution and data capture
//  4. Initiate shutdown and verify clean termination
//  5. Validate delivered data matches pushed payload
//
// Validates fundamental consumer operation and data integrity.
func TestPinnedConsumerDeliversItem(t *testing.T) {
	runtime.GOMAXPROCS(2) // Ensure sufficient cores for producer/consumer
	r := New(8)

	want := [24]byte{1, 2, 3, 4}
	var got [24]byte
	var zero [24]byte

	// Launch consumer with data capture callback
	stop, hot, done := launch(r, func(p *[24]byte) { got = *p })

	// Signal producer activity and push data
	atomic.StoreUint32(hot, 1)
	if !r.Push(&want) {
		t.Fatal("Push failed unexpectedly")
	}
	atomic.StoreUint32(hot, 0)

	// Wait for callback execution with timeout
	wait := time.NewTimer(20 * time.Millisecond)
	for got == zero {
		select {
		case <-wait.C:
			t.Fatal("callback never executed within timeout")
		default:
			runtime.Gosched() // Yield to allow consumer processing
		}
	}

	// Initiate shutdown and verify completion
	atomic.StoreUint32(stop, 1)
	select {
	case <-done:
		// Clean shutdown achieved
	case <-time.After(100 * time.Millisecond):
		t.Fatal("consumer shutdown timeout exceeded")
	}

	// Validate data integrity
	if got != want {
		t.Fatalf("handler received %v, expected %v", got, want)
	}
}

// TestPinnedConsumerStopsNoWork validates idle shutdown behavior.
// Tests consumer termination when no data processing occurs.
//
// Test approach:
//  1. Launch consumer without any data activity
//  2. Immediately signal shutdown
//  3. Verify prompt termination without processing
//
// Validates shutdown responsiveness under idle conditions.
func TestPinnedConsumerStopsNoWork(t *testing.T) {
	r := New(4)
	stop, _, done := launch(r, func(_ *[24]byte) {})

	// Immediate shutdown signal
	atomic.StoreUint32(stop, 1)

	// Verify prompt termination
	select {
	case <-done:
		// Expected immediate termination
	case <-time.After(50 * time.Millisecond):
		t.Fatal("consumer did not terminate promptly")
	}
}

// ============================================================================
// HOT WINDOW BEHAVIOR VALIDATION
// ============================================================================

// TestPinnedConsumerHotWindow validates hot polling state persistence.
// Tests consumer behavior during hot window period after data processing.
//
// Test sequence:
//  1. Process single data item to activate hot window
//  2. Wait for hot window duration
//  3. Verify consumer remains active (no premature shutdown)
//  4. Confirm callback execution count accuracy
//  5. Validate graceful shutdown after hot window
//
// Tests hot window timing and activity state management.
func TestPinnedConsumerHotWindow(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ *[24]byte) { hits.Add(1) })

	// Process data to activate hot window
	atomic.StoreUint32(hot, 1)
	_ = r.Push(&[24]byte{9})
	atomic.StoreUint32(hot, 0)

	// Wait within hot window period
	time.Sleep(1 * time.Second)

	// Verify single callback execution
	if v := hits.Load(); v != 1 {
		t.Fatalf("callback count = %d, expected 1", v)
	}

	// Consumer should remain active during hot window
	select {
	case <-done:
		t.Fatal("consumer exited prematurely during hot window")
	default:
		// Expected: consumer still active
	}

	// Clean shutdown after validation
	atomic.StoreUint32(stop, 1)
	<-done
}

// TestPinnedConsumerBackoffThenWake validates cold resume behavior.
// Tests consumer recovery from extended idle periods and backoff states.
//
// Test methodology:
//  1. Process initial data item
//  2. Wait for extended idle period (beyond hot window)
//  3. Resume activity with new data
//  4. Verify consumer responsiveness after idle period
//  5. Validate total callback execution count
//
// Tests idle state management and activity resumption.
func TestPinnedConsumerBackoffThenWake(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ *[24]byte) { hits.Add(1) })

	// Initial data processing
	atomic.StoreUint32(hot, 1)
	r.Push(&[24]byte{7})
	atomic.StoreUint32(hot, 0)

	// Extended idle period (beyond hot window)
	time.Sleep(15*time.Second + 100*time.Millisecond)

	// Resume activity after idle period
	atomic.StoreUint32(hot, 1)
	r.Push(&[24]byte{8})
	time.Sleep(10 * time.Millisecond)

	// Verify both callbacks executed
	if v := hits.Load(); v != 2 {
		t.Fatalf("expected 2 callbacks after resume, got %d", v)
	}

	// Clean shutdown
	atomic.StoreUint32(stop, 1)
	<-done
}

// ============================================================================
// DELAYED STARTUP VALIDATION
// ============================================================================

// TestDelayedConsumerStart validates pre-pushed data handling.
// Tests consumer behavior when data exists before consumer initialization.
//
// Test sequence:
//  1. Push data into ring before consumer starts
//  2. Launch consumer after data is available
//  3. Verify consumer processes pre-existing data
//  4. Confirm callback execution for delayed startup scenario
//
// Tests initialization timing and pre-existing data consumption.
func TestDelayedConsumerStart(t *testing.T) {
	r := New(4)
	var seen atomic.Bool

	// Push data before consumer starts
	r.Push(&[24]byte{11})

	// Launch consumer after data availability
	stop := new(uint32)
	hot := new(uint32)
	done := make(chan struct{})
	go PinnedConsumer(0, r, stop, hot, func(*[24]byte) {
		seen.Store(true)
	}, done)

	// Allow consumer initialization and processing
	time.Sleep(10 * time.Millisecond)

	// Initiate shutdown
	atomic.StoreUint32(stop, 1)
	<-done

	// Verify pre-pushed data was processed
	if !seen.Load() {
		t.Fatal("consumer did not process pre-pushed data")
	}
}
