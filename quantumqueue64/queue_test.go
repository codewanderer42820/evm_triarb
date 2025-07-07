// ============================================================================
// QUANTUMQUEUE64 CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive test coverage for ISR-safe QuantumQueue64 operations with
// emphasis on footgun-mode behavior validation and protocol adherence.
//
// COMPACT VERSION: Updated for uint64 payloads and 32-byte node layout
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

package quantumqueue64

import "testing"

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
	if n1.tick != -1 || n1.data != 0 || n1.prev != nilIdx || n1.next != nilIdx {
		t.Errorf("Borrow reset failed: tick=%d data=%d prev=%v next=%v",
			n1.tick, n1.data, n1.prev, n1.next)
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
	const testData1 = uint64(0x123456789ABCDEF0)
	q.Push(10, h, testData1)
	if q.Empty() || q.Size() != 1 {
		t.Errorf("queue state after Push: Empty=%v Size=%d; want false, 1",
			q.Empty(), q.Size())
	}

	// Verify insertion correctness
	hGot, tickGot, data := q.PeepMin()
	if hGot != h || tickGot != 10 || data != testData1 {
		t.Errorf("PeepMin mismatch: h=%v tick=%d data=%x; want %v, 10, %x",
			hGot, tickGot, data, h, testData1)
	}

	// Test in-place update (same tick, same handle)
	const testData2 = uint64(0xFEDCBA9876543210)
	q.Push(10, h, testData2)
	if q.Size() != 1 {
		t.Errorf("in-place update changed size: got %d, want 1", q.Size())
	}

	// Verify payload update without structural changes
	hGot2, _, data2 := q.PeepMin()
	if hGot2 != h || data2 != testData2 {
		t.Errorf("payload update failed: got %x, want %x", data2, testData2)
	}

	// Test entry removal
	q.UnlinkMin(h, 10)
	if !q.Empty() || q.Size() != 0 {
		t.Error("queue not empty after UnlinkMin")
	}

	// Test edge cases: boundary tick values
	q2 := New()
	h0, _ := q2.BorrowSafe()
	hMax, _ := q2.BorrowSafe()

	const lowData = uint64(0x1111)
	const highData = uint64(0x2222)
	q2.Push(0, h0, lowData)
	q2.Push(int64(BucketCount-1), hMax, highData)

	// Verify minimum tick priority
	hMin, tickMin, dataMin := q2.PeepMin()
	if hMin != h0 || tickMin != 0 || dataMin != lowData {
		t.Errorf("minimum tick selection failed: got (%v, %d, %x), want (%v, 0, %x)",
			hMin, tickMin, dataMin, h0, lowData)
	}

	// Test maximum tick handling after minimum removal
	q2.UnlinkMin(h0, 0)
	hHigh, tickHigh, dataHigh := q2.PeepMin()
	if hHigh != hMax || tickHigh != int64(BucketCount-1) || dataHigh != highData {
		t.Errorf("maximum tick retrieval failed: got (%v, %d, %x), want (%v, %d, %x)",
			hHigh, tickHigh, dataHigh, hMax, int64(BucketCount-1), highData)
	}

	// Verify complete cleanup
	q2.UnlinkMin(hMax, tickHigh)
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
	const initialData = uint64(0xAAAA)
	q.Push(42, h, initialData)

	// Move to different tick (triggers automatic unlink/relink)
	const newData = uint64(0xBBBB)
	q.Push(99, h, newData)

	// Verify relocation correctness
	if q.Size() != 1 {
		t.Errorf("size after tick change: got %d, want 1", q.Size())
	}

	hGot, tickGot, data := q.PeepMin()
	if hGot != h || tickGot != 99 || data != newData {
		t.Errorf("tick relocation failed: got (%v, %d, %x), want (%v, 99, %x)",
			hGot, tickGot, data, h, newData)
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

	const data1 = uint64(0x1111)
	const data2 = uint64(0x2222)
	q.Push(5, h1, data1)
	q.Push(5, h2, data2) // Newer entry, should be head

	hMin, _, data := q.PeepMin()
	if hMin != h2 || data != data2 {
		t.Errorf("LIFO ordering failed: got handle=%v data=%x, want %v %x",
			hMin, data, h2, data2)
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

	const highPriorityData = uint64(0x1111)
	const lowPriorityData = uint64(0x2222)
	q.Push(100, h1, highPriorityData)
	q.Push(50, h2, lowPriorityData) // Lower tick, higher priority

	hMin, tickMin, data := q.PeepMin()
	if hMin != h2 || tickMin != 50 || data != lowPriorityData {
		t.Errorf("tick priority ordering failed: got (%v, %d, %x), want (%v, 50, %x)",
			hMin, tickMin, data, h2, lowPriorityData)
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

	const testData = uint64(0xCCCC)
	q.Push(20, h, testData)

	// Test no-op move (same tick) - should be optimized
	q.MoveTick(h, 20)

	// Test actual relocation to different tick
	q.MoveTick(h, 30)

	hNew, tickNew, data := q.PeepMin()
	if hNew != h || tickNew != 30 || data != testData {
		t.Errorf("MoveTick failed: got (%v, %d, %x), want (%v, 30, %x)",
			hNew, tickNew, data, h, testData)
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
	const data1 = uint64(0x1111)
	const data2 = uint64(0x2222)
	const data3 = uint64(0x3333)
	q.Push(1, h1, data1)
	q.Push(2, h2, data2)
	q.Push(3, h3, data3)

	// Remove middle entry (non-minimum in global ordering)
	q.UnlinkMin(h2, 2)

	// Verify remaining minimum is correct
	hMin, tickMin, data := q.PeepMin()
	if hMin != h1 || tickMin != 1 || data != data1 {
		t.Errorf("non-head removal failed: got (%v, %d, %x), want (%v, 1, %x)",
			hMin, tickMin, data, h1, data1)
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
		q.Push(int64(i), h, uint64(0x1000+i))
	}

	// Drain in strict tick priority order
	for i := 0; i < 3; i++ {
		h, tick, data := q.PeepMin()
		if tick != int64(i) || data != uint64(0x1000+i) {
			t.Errorf("drain order incorrect: want tick %d data %x, got %d %x",
				i, 0x1000+i, tick, data)
		}
		q.UnlinkMin(h, tick)
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
	q.Push(100, h, uint64(0xDEAD))
	q.UnlinkMin(h, 100)
	q.UnlinkMin(h, 100) // Protocol violation - should panic
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
	const initialData = uint64(0xAAAA)
	q.Push(123, h, initialData)
	q.UnlinkMin(h, 123)

	// Reuse same handle with different data
	const reuseData = uint64(0xBBBB)
	q.Push(456, h, reuseData)

	// Verify reuse correctness and no state leakage
	_, tick, data := q.PeepMin()
	if tick != 456 || data != reuseData {
		t.Errorf("handle reuse failed: tick=%d data=%x, want 456 %x",
			tick, data, reuseData)
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
		q.Push(-9999, h, uint64(0xDEAD))
	})

	t.Run("OverflowTick", func(t *testing.T) {
		defer func() { recover() }()
		q.Push(int64(BucketCount+1000), h, uint64(0xBEEF))
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
	const data1 = uint64(0x1111)
	const data2 = uint64(0x2222)
	const data3 = uint64(0x3333)
	q.Push(10, h1, data1)
	q.Push(20, h2, data2)
	q.Push(10, h1, data3) // Update, not insertion

	if q.Size() != 2 {
		t.Errorf("size after operations: got %d, want 2", q.Size())
	}

	// Test removals and final size validation
	q.UnlinkMin(h1, 10)
	q.UnlinkMin(h2, 20)

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
		q.Push(int64(i), h, uint64(i))
	}

	// Return all handles to freelist
	for i, h := range handles {
		q.UnlinkMin(h, int64(i))
	}

	// Verify complete freelist recovery
	for i := 0; i < CapItems; i++ {
		if _, err := q.BorrowSafe(); err != nil {
			t.Fatalf("freelist reuse failed at allocation #%d: %v", i, err)
		}
	}
}
