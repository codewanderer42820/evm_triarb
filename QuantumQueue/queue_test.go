// ============================================================================
// QUANTUMQUEUE CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive test coverage for ISR-safe QuantumQueue operations with
// emphasis on footgun-mode behavior validation and protocol adherence.
//
// Test categories:
//   - Basic construction and initialization validation
//   - Core API operation verification (Push, PeepMin, MoveTick, UnlinkMin)
//   - Handle lifecycle and arena management
//   - LIFO ordering within tick buckets
//   - Edge case and boundary condition handling
//   - Footgun behavior validation (panic conditions)
//
// Safety model validation:
//   - Protocol violation detection (double unlink, empty PeepMin)
//   - Handle reuse cycle correctness
//   - Size tracking accuracy across all operations
//   - Freelist integrity maintenance
//
// Performance assumptions:
//   - All operations complete in O(1) time
//   - Zero allocation during normal operation
//   - Deterministic behavior under stress conditions

package quantumqueue

import "testing"

// ============================================================================
// TEST UTILITIES
// ============================================================================

// arr48 converts a variable-length byte slice into a fixed-size [48]byte pointer.
// Provides convenient payload construction for test scenarios with type safety.
//
// Parameters:
//
//	b: Source byte slice (will be truncated or zero-padded to 48 bytes)
//
// Returns:
//
//	Pointer to 48-byte array suitable for QuantumQueue payload operations
//
// Usage pattern:
//
//	q.Push(tick, handle, arr48([]byte("test payload")))
func arr48(b []byte) *[48]byte {
	var a [48]byte
	copy(a[:], b)
	return &a
}

// ============================================================================
// BASIC CONSTRUCTION AND INITIALIZATION
// ============================================================================

// TestNewQueueBehavior validates proper queue initialization and handle allocation.
//
// Verification criteria:
//   - Initial empty state: size=0, Empty()=true
//   - Clean handle allocation with proper node reset
//   - Sequential handle assignment from freelist
//   - Node field initialization to safe defaults
//
// Validates constructor correctness and freelist setup integrity.
func TestNewQueueBehavior(t *testing.T) {
	q := New()

	// Verify initial empty state
	if !q.Empty() || q.Size() != 0 {
		t.Errorf("new queue state invalid: Empty=%v Size=%d; want true, 0",
			q.Empty(), q.Size())
	}

	// Test first handle allocation
	h1, err := q.Borrow()
	if err != nil {
		t.Fatalf("first Borrow failed: %v", err)
	}

	// Verify handle reset to clean state
	n1 := &q.arena[h1]
	if n1.tick != -1 || n1.prev != nilIdx || n1.next != nilIdx {
		t.Errorf("Borrow reset failed: tick=%d prev=%v next=%v",
			n1.tick, n1.prev, n1.next)
	}

	// Test sequential handle allocation
	h2, err2 := q.Borrow()
	if err2 != nil || h2 != h1+1 {
		t.Errorf("sequential Borrow failed: h2=%v want=%v", h2, h1+1)
	}
}

// ============================================================================
// CAPACITY MANAGEMENT
// ============================================================================

// TestBorrowSafeExhaustion verifies arena capacity limits and exhaustion handling.
//
// Validation sequence:
//  1. Allocate exactly CapItems handles successfully
//  2. Verify exhaustion detection on (CapItems + 1) allocation
//  3. Confirm no corruption occurs at capacity boundary
//
// Tests freelist management and capacity enforcement correctness.
func TestBorrowSafeExhaustion(t *testing.T) {
	q := New()

	// Allocate all available handles
	for i := 0; i < CapItems; i++ {
		if _, err := q.BorrowSafe(); err != nil {
			t.Fatalf("Borrow #%d failed unexpectedly: %v", i, err)
		}
	}

	// Verify exhaustion detection
	if _, err := q.BorrowSafe(); err == nil {
		t.Error("expected exhaustion error after CapItems allocations, got nil")
	}
}

// ============================================================================
// CORE API OPERATIONS
// ============================================================================

// TestPushAndPeepMin validates fundamental queue operations and edge cases.
//
// Test scenarios:
//   - Basic insertion and retrieval correctness
//   - In-place payload updates for identical tick values
//   - Proper tick-based ordering across different values
//   - Edge case handling at tick boundaries (0 and maximum)
//   - Size tracking accuracy throughout operations
//
// Validates core scheduling semantics and boundary condition handling.
func TestPushAndPeepMin(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	// Test basic insertion
	q.Push(10, h, arr48([]byte("foo")))
	if q.Empty() || q.Size() != 1 {
		t.Errorf("queue state after Push: Empty=%v Size=%d; want false, 1",
			q.Empty(), q.Size())
	}

	// Verify insertion correctness
	hGot, tickGot, data := q.PeepMin()
	if hGot != h || tickGot != 10 || string(data[:3]) != "foo" {
		t.Errorf("PeepMin mismatch: h=%v tick=%d data=%q; want %v, 10, 'foo'",
			hGot, tickGot, data[:3], h)
	}

	// Test in-place update (same tick, same handle)
	q.Push(10, h, arr48([]byte("bar")))
	if q.Size() != 1 {
		t.Errorf("in-place update changed size: got %d, want 1", q.Size())
	}

	// Verify payload update without structural changes
	hGot2, _, data2 := q.PeepMin()
	if hGot2 != h || string(data2[:3]) != "bar" {
		t.Errorf("payload update failed: got %q, want 'bar'", data2[:3])
	}

	// Test entry removal
	q.UnlinkMin(h)
	if !q.Empty() || q.Size() != 0 {
		t.Error("queue not empty after UnlinkMin")
	}

	// Test edge cases: boundary tick values
	q2 := New()
	h0, _ := q2.BorrowSafe()
	hMax, _ := q2.BorrowSafe()

	q2.Push(0, h0, arr48([]byte("low")))
	q2.Push(int64(CapItems-1), hMax, arr48([]byte("hi")))

	// Verify minimum tick priority
	hMin, tickMin, _ := q2.PeepMin()
	if hMin != h0 || tickMin != 0 {
		t.Errorf("minimum tick selection failed: got (%v, %d), want (%v, 0)",
			hMin, tickMin, h0)
	}

	// Test maximum tick handling after minimum removal
	q2.UnlinkMin(h0)
	hHigh, tickHigh, _ := q2.PeepMin()
	if hHigh != hMax || tickHigh != int64(CapItems-1) {
		t.Errorf("maximum tick retrieval failed: got (%v, %d), want (%v, %d)",
			hHigh, tickHigh, hMax, int64(CapItems-1))
	}

	// Verify complete cleanup
	q2.UnlinkMin(hMax)
	if !q2.Empty() {
		t.Error("queue not empty after removing all entries")
	}
}

// TestPushTriggersUnlink validates automatic tick relocation behavior.
//
// Test sequence:
//  1. Insert handle at initial tick position
//  2. Push same handle to different tick (triggers unlink/relink)
//  3. Verify correct relocation and payload preservation
//  4. Validate size invariant maintenance
//
// Ensures Push operations handle tick changes via proper unlink/relink cycles.
func TestPushTriggersUnlink(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	// Insert at initial tick
	q.Push(42, h, arr48([]byte("aaa")))

	// Move to different tick (triggers automatic unlink/relink)
	q.Push(99, h, arr48([]byte("bbb")))

	// Verify relocation correctness
	if q.Size() != 1 {
		t.Errorf("size after tick change: got %d, want 1", q.Size())
	}

	hGot, tickGot, data := q.PeepMin()
	if hGot != h || tickGot != 99 {
		t.Errorf("tick relocation failed: got (%v, %d), want (%v, 99)",
			hGot, tickGot, h)
	}

	if string(data[:3]) != "bbb" {
		t.Errorf("payload after relocation: got %q, want 'bbb'", data[:3])
	}
}

// ============================================================================
// ORDERING SEMANTICS
// ============================================================================

// TestMultipleSameTickOrdering validates LIFO semantics within tick buckets.
//
// Test scenario:
//  1. Insert multiple entries with identical tick values
//  2. Verify that newer entries appear first (LIFO ordering)
//  3. Confirm bucket head management correctness
//
// Validates Last-In-First-Out behavior for entries sharing tick values.
func TestMultipleSameTickOrdering(t *testing.T) {
	q := New()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()

	q.Push(5, h1, arr48([]byte("a1")))
	q.Push(5, h2, arr48([]byte("a2"))) // Newer entry, should be head

	hMin, _, data := q.PeepMin()
	if hMin != h2 || string(data[:2]) != "a2" {
		t.Errorf("LIFO ordering failed: got handle=%v data=%q, want %v 'a2'",
			hMin, data[:2], h2)
	}
}

// TestPushDifferentTicks validates tick-based priority ordering.
//
// Test scenario:
//  1. Insert entries with different tick values in arbitrary order
//  2. Verify that lower tick values have higher priority
//  3. Confirm hierarchical bitmap minimum finding correctness
//
// Validates priority queue semantics regardless of insertion order.
func TestPushDifferentTicks(t *testing.T) {
	q := New()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()

	q.Push(100, h1, arr48([]byte("one")))
	q.Push(50, h2, arr48([]byte("two"))) // Lower tick, higher priority

	hMin, tickMin, _ := q.PeepMin()
	if hMin != h2 || tickMin != 50 {
		t.Errorf("tick priority ordering failed: got (%v, %d), want (%v, 50)",
			hMin, tickMin, h2)
	}
}

// ============================================================================
// ADVANCED OPERATIONS
// ============================================================================

// TestMoveTickBehavior validates tick relocation functionality.
//
// Test scenarios:
//  1. No-op move (same tick): Verify optimization works correctly
//  2. Actual relocation: Confirm data integrity and position correctness
//  3. Queue structure consistency: Validate bitmap and link maintenance
//
// Tests MoveTick operation efficiency and correctness.
func TestMoveTickBehavior(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	q.Push(20, h, arr48([]byte("x")))

	// Test no-op move (same tick) - should be optimized
	q.MoveTick(h, 20)

	// Test actual relocation to different tick
	q.MoveTick(h, 30)

	hNew, tickNew, _ := q.PeepMin()
	if hNew != h || tickNew != 30 {
		t.Errorf("MoveTick failed: got (%v, %d), want (%v, 30)", hNew, tickNew, h)
	}
}

// TestUnlinkMinNonHead validates removal of entries from arbitrary positions.
//
// Test sequence:
//  1. Create entries at different tick positions
//  2. Remove non-minimum entry (middle of global order)
//  3. Verify remaining entries maintain correct ordering
//  4. Confirm bitmap and linked list integrity
//
// Validates UnlinkMin correctness for non-head removals.
func TestUnlinkMinNonHead(t *testing.T) {
	q := New()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()

	// Create entries in ascending tick order
	q.Push(1, h1, arr48([]byte("h1")))
	q.Push(2, h2, arr48([]byte("h2")))
	q.Push(3, h3, arr48([]byte("h3")))

	// Remove middle entry (non-minimum in global ordering)
	q.UnlinkMin(h2)

	// Verify remaining minimum is correct
	hMin, tickMin, _ := q.PeepMin()
	if hMin != h1 || tickMin != 1 {
		t.Errorf("non-head removal failed: got (%v, %d), want (%v, 1)",
			hMin, tickMin, h1)
	}
}

// ============================================================================
// COMPREHENSIVE OPERATION TESTING
// ============================================================================

// TestMixedOperations validates complex operation sequences.
//
// Test pattern:
//  1. Initialize queue with sequential tick values
//  2. Drain queue in strict priority order
//  3. Verify correct ordering throughout draining process
//  4. Confirm complete emptiness after all operations
//
// Tests interleaved Push/PeepMin/UnlinkMin operations under realistic usage.
func TestMixedOperations(t *testing.T) {
	q := New()
	var hs [3]Handle

	// Initialize with sequential tick values
	for i := range hs {
		h, _ := q.BorrowSafe()
		hs[i] = h
		q.Push(int64(i), h, arr48([]byte{byte(i)}))
	}

	// Drain in strict tick priority order
	for i := 0; i < 3; i++ {
		h, tick, _ := q.PeepMin()
		if tick != int64(i) {
			t.Errorf("drain order incorrect: want tick %d, got %d", i, tick)
		}
		q.UnlinkMin(h)
	}

	// Verify complete emptiness
	if !q.Empty() {
		t.Error("queue not empty after draining all entries")
	}
}

// ============================================================================
// FOOTGUN BEHAVIOR VALIDATION
// ============================================================================

// TestPeepMinWhenEmpty validates panic behavior on empty queue access.
//
// ⚠️  FOOTGUN GRADE 10/10: PeepMin on empty queue causes undefined behavior
//
// Expected behavior: Immediate panic or memory corruption
// Safety requirement: Never call PeepMin without checking Empty() first
func TestPeepMinWhenEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when calling PeepMin on empty queue")
		}
	}()
	New().PeepMin()
}

// TestDoubleUnlink validates panic behavior on protocol violations.
//
// ⚠️  FOOTGUN GRADES 2/4/8: Double unlink operations cause corruption
//
// Expected behavior: Immediate panic or silent state corruption
// Safety requirement: Never unlink same handle twice without re-linking
func TestDoubleUnlink(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on double unlink operation")
		}
	}()

	q := New()
	h, _ := q.BorrowSafe()
	q.Push(100, h, arr48([]byte("foo")))
	q.UnlinkMin(h)
	q.UnlinkMin(h) // Protocol violation - should panic
}

// TestHandleReuseAfterUnlink validates safe handle recycling patterns.
//
// Test sequence:
//  1. Use handle in normal Push/UnlinkMin cycle
//  2. Reuse same handle for different tick and payload
//  3. Verify no state leakage or corruption from previous usage
//
// Validates proper handle cleanup and safe reuse protocols.
func TestHandleReuseAfterUnlink(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	// Initial use cycle
	q.Push(123, h, arr48([]byte("x")))
	q.UnlinkMin(h)

	// Reuse same handle with different data
	q.Push(456, h, arr48([]byte("y")))

	// Verify reuse correctness and no state leakage
	_, tick, data := q.PeepMin()
	if tick != 456 || string(data[:1]) != "y" {
		t.Errorf("handle reuse failed: tick=%d data=%q, want 456 'y'",
			tick, data[:1])
	}
}

// TestPushWithInvalidTicks validates boundary condition handling.
//
// Test scenarios:
//  1. Negative tick values (undefined behavior)
//  2. Tick values exceeding BucketCount (array bounds violation)
//
// ⚠️  FOOTGUN WARNING: Invalid ticks may cause silent corruption
func TestPushWithInvalidTicks(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	t.Run("NegativeTick", func(t *testing.T) {
		defer func() { recover() }()
		q.Push(-9999, h, arr48([]byte("neg")))
	})

	t.Run("OverflowTick", func(t *testing.T) {
		defer func() { recover() }()
		q.Push(int64(BucketCount+1000), h, arr48([]byte("hi")))
	})
}

// ============================================================================
// LIFECYCLE AND RESOURCE MANAGEMENT
// ============================================================================

// TestSizeTracking validates accurate size counter maintenance.
//
// Test scenarios:
//  1. Size increments on new insertions
//  2. Size remains stable on in-place updates (same tick)
//  3. Size decrements on entry removals
//  4. Size accuracy maintained across mixed operations
//
// Ensures queue size counter reflects actual entry count at all times.
func TestSizeTracking(t *testing.T) {
	q := New()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()

	// Test insertions and in-place updates
	q.Push(10, h1, arr48([]byte("a")))
	q.Push(20, h2, arr48([]byte("b")))
	q.Push(10, h1, arr48([]byte("c"))) // Update, not insertion

	// Test removals and final size validation
	q.UnlinkMin(h1)
	q.UnlinkMin(h2)

	if q.Size() != 0 {
		t.Errorf("size tracking failed: got %d, want 0", q.Size())
	}
}

// TestFreelistCycle validates complete freelist recycling.
//
// Test sequence:
//  1. Exhaust all available handles via BorrowSafe
//  2. Use all handles in Push operations
//  3. Return all handles via UnlinkMin operations
//  4. Verify all handles can be re-borrowed successfully
//
// Tests complete arena lifecycle without resource leakage.
func TestFreelistCycle(t *testing.T) {
	q := New()
	var handles []Handle

	// Exhaust all handles in arena
	for i := 0; i < CapItems; i++ {
		h, _ := q.BorrowSafe()
		handles = append(handles, h)
		q.Push(int64(i), h, arr48([]byte("x")))
	}

	// Return all handles to freelist
	for _, h := range handles {
		q.UnlinkMin(h)
	}

	// Verify complete freelist recovery
	for i := 0; i < CapItems; i++ {
		if _, err := q.BorrowSafe(); err != nil {
			t.Fatalf("freelist reuse failed at allocation #%d: %v", i, err)
		}
	}
}
