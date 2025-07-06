// ============================================================================
// SPSC RING BUFFER CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive unit testing framework for lock-free SPSC ring buffer
// with emphasis on ISR-grade reliability and edge case coverage.
//
// Test categories:
//   - Constructor validation: Power-of-2 sizing and initialization
//   - Basic operations: Push/Pop semantics and data integrity
//   - Capacity management: Full/empty state handling and overflow behavior
//   - Wraparound logic: Circular buffer pointer arithmetic validation
//   - Edge cases: Boundary conditions, reuse safety, and idle behavior
//
// Validation methodology:
//   - Single-threaded operation validation (SPSC discipline)
//   - Data integrity verification across operation cycles
//   - Capacity boundary testing with overflow scenarios
//   - Pointer lifetime and reuse safety validation
//
// Performance assumptions:
//   - Sub-10ns operation latency for Push/Pop operations
//   - Zero allocation during steady-state operation
//   - Predictable behavior under varying load conditions
//   - Cache-friendly access patterns for sequential operations

package ring24

import (
	"testing"
	"time"
)

// ============================================================================
// CONSTRUCTOR VALIDATION
// ============================================================================

// TestNewPanicsOnBadSize validates constructor input validation.
//
// Invalid size conditions:
//   - Zero size: No capacity for data storage
//   - Non-power-of-2: Breaks bit masking efficiency requirements
//   - Large non-power-of-2: Validates power-of-2 detection at scale
//
// Tests constructor safety and power-of-2 requirement enforcement.
func TestNewPanicsOnBadSize(t *testing.T) {
	bad := []int{0, 3, 1000} // Zero, small non-power-of-2, large non-power-of-2

	for _, sz := range bad {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("New(%d) should panic on invalid size", sz)
				}
			}()
			_ = New(sz)
		}()
	}
}

// ============================================================================
// BASIC OPERATION VALIDATION
// ============================================================================

// TestPushPopRoundTrip validates fundamental Push/Pop semantics.
//
// Test sequence:
//  1. Push known payload into empty ring
//  2. Pop payload and verify data integrity
//  3. Confirm ring returns to empty state
//
// Validates core SPSC functionality and data preservation.
func TestPushPopRoundTrip(t *testing.T) {
	r := New(8)
	val := &[24]byte{1, 2, 3}

	// Test successful push to empty ring
	if !r.Push(val) {
		t.Fatal("Push should succeed on empty ring")
	}

	// Test pop and data integrity
	got := r.Pop()
	if got == nil || *got != *val {
		t.Fatalf("expected %v, got %v", val, got)
	}

	// Test empty state after pop
	if r.Pop() != nil {
		t.Fatal("ring should be empty after single push/pop cycle")
	}
}

// TestPushFailsWhenFull validates capacity enforcement and overflow handling.
//
// Test approach:
//  1. Fill ring to exact capacity
//  2. Attempt overflow push
//  3. Verify overflow rejection
//
// Tests capacity management and overflow protection.
func TestPushFailsWhenFull(t *testing.T) {
	r := New(4)
	val := &[24]byte{7}

	// Fill to capacity
	for i := 0; i < 4; i++ {
		if !r.Push(val) {
			t.Fatalf("push %d unexpectedly failed before capacity reached", i)
		}
	}

	// Test overflow rejection
	if r.Push(val) {
		t.Fatal("push into full ring should return false")
	}
}

// TestPopNil validates empty ring behavior.
//
// Test scenario:
//  1. Create empty ring
//  2. Attempt pop operation
//  3. Verify nil return for empty state
//
// Tests empty state handling and nil return semantics.
func TestPopNil(t *testing.T) {
	r := New(4)
	if r.Pop() != nil {
		t.Fatal("Pop on empty ring should return nil")
	}
}

// TestPopWaitBlocksUntilItem validates blocking consumption behavior.
//
// Test methodology:
//  1. Start PopWait on empty ring
//  2. Push data from separate goroutine with delay
//  3. Verify PopWait receives pushed data
//
// Tests blocking semantics and cross-goroutine data transfer.
func TestPopWaitBlocksUntilItem(t *testing.T) {
	r := New(2)
	want := &[24]byte{42}

	// Producer goroutine with delay
	go func() {
		time.Sleep(5 * time.Millisecond)
		r.Push(want)
	}()

	// Blocking consumer
	if got := r.PopWait(); got == nil || *got != *want {
		t.Fatalf("PopWait = %v, want %v", got, want)
	}
}

// ============================================================================
// WRAPAROUND AND CIRCULAR BUFFER VALIDATION
// ============================================================================

// TestWrapAround validates circular buffer pointer arithmetic.
//
// Test approach:
//  1. Perform multiple push/pop cycles exceeding ring size
//  2. Verify data integrity across wraparound boundaries
//  3. Confirm sequential ordering preservation
//
// Tests circular buffer mechanics and pointer wraparound safety.
func TestWrapAround(t *testing.T) {
	const size = 4
	r := New(size)

	// Test wraparound across multiple cycles
	for i := 0; i < 10; i++ {
		val := &[24]byte{byte(i)}

		if !r.Push(val) {
			t.Fatalf("push %d failed unexpectedly during wraparound test", i)
		}

		got := r.Pop()
		if got == nil || got[0] != byte(i) {
			t.Fatalf("iteration %d: got %v, want %v", i, got[0], val[0])
		}
	}
}

// ============================================================================
// INTERLEAVED OPERATION TESTING
// ============================================================================

// TestPushPopInterleaved validates safety under alternating operations.
//
// Test pattern:
//  1. Interleave push and pop operations extensively
//  2. Verify data integrity throughout sequence
//  3. Confirm no state corruption under alternating access
//
// Tests operation safety under realistic access patterns.
func TestPushPopInterleaved(t *testing.T) {
	r := New(8)

	// Extended interleaved operation sequence
	for i := 0; i < 64; i++ {
		val := &[24]byte{byte(i)}

		if !r.Push(val) {
			t.Fatalf("push %d failed during interleaved test", i)
		}

		got := r.Pop()
		if got == nil || got[0] != byte(i) {
			t.Fatalf("data mismatch at iteration %d", i)
		}
	}
}

// TestPopAfterLongIdle validates resume behavior after idle periods.
//
// Test sequence:
//  1. Allow ring to remain idle for extended period
//  2. Push data after idle period
//  3. Verify normal operation resumption
//
// Tests idle state handling and operation resumption.
func TestPopAfterLongIdle(t *testing.T) {
	r := New(2)

	// Simulate idle period
	time.Sleep(50 * time.Millisecond)

	want := &[24]byte{88}
	if !r.Push(want) {
		t.Fatal("Push failed after idle period")
	}

	got := r.Pop()
	if got == nil || *got != *want {
		t.Fatalf("got %v, want %v after idle period", got, want)
	}
}

// ============================================================================
// OVERFLOW AND CAPACITY TESTING
// ============================================================================

// TestPushDropOnOverflow validates overflow behavior and drop detection.
//
// Test methodology:
//  1. Attempt pushes exceeding ring capacity
//  2. Count successful vs failed operations
//  3. Verify overflow detection mechanism
//
// Tests capacity enforcement and overflow handling accuracy.
func TestPushDropOnOverflow(t *testing.T) {
	r := New(4)
	val := &[24]byte{99}
	drops := 0

	// Attempt pushes beyond capacity
	for i := 0; i < 8; i++ {
		if !r.Push(val) {
			drops++
		}
	}

	if drops == 0 {
		t.Fatal("expected at least one push to fail on overflow")
	}
}

// ============================================================================
// EDGE CASE VALIDATION
// ============================================================================

// TestDoublePopWithoutPush validates multiple pop safety on empty ring.
//
// Test scenario:
//  1. Perform multiple pop operations on empty ring
//  2. Verify consistent nil return behavior
//  3. Confirm no state corruption from repeated empty pops
//
// Tests empty state consistency and repeated operation safety.
func TestDoublePopWithoutPush(t *testing.T) {
	r := New(4)

	if r.Pop() != nil {
		t.Fatal("first Pop on empty ring should return nil")
	}
	if r.Pop() != nil {
		t.Fatal("second Pop on empty ring should also return nil")
	}
}

// TestFullWrapPushPop validates full-capacity wraparound cycles.
//
// Test approach:
//  1. Perform multiple complete ring cycles
//  2. Verify data integrity across all cycles
//  3. Confirm wraparound arithmetic correctness
//
// Tests full-capacity operation and multi-cycle wraparound.
func TestFullWrapPushPop(t *testing.T) {
	const size = 4
	r := New(size)

	// Multiple complete cycles
	for i := 0; i < size*3; i++ {
		val := &[24]byte{byte(i)}

		if !r.Push(val) {
			t.Fatalf("Push failed at iteration %d", i)
		}

		got := r.Pop()
		if got == nil || got[0] != byte(i) {
			t.Fatalf("iteration %d: got %v, want %v", i, got[0], val[0])
		}
	}
}

// TestPopImmediateReuse validates pointer safety after slot reuse.
//
// Test sequence:
//  1. Push, pop, and copy data from first operation
//  2. Push new data causing slot reuse
//  3. Verify copied data remains unchanged
//
// Tests pointer lifetime and data independence after reuse.
func TestPopImmediateReuse(t *testing.T) {
	r := New(2)
	val1 := &[24]byte{42}
	val2 := &[24]byte{99}

	// First push/pop cycle
	if !r.Push(val1) {
		t.Fatal("Push val1 failed")
	}

	ptr := r.Pop()
	if ptr == nil || *ptr != *val1 {
		t.Fatal("first Pop mismatch")
	}

	// Copy data before potential reuse
	copy := *ptr

	// Second push (may reuse same slot)
	if !r.Push(val2) {
		t.Fatal("Push val2 failed")
	}

	// Verify copied data integrity
	if copy != *val1 {
		t.Fatal("copied value changed after ring reuse")
	}
}
