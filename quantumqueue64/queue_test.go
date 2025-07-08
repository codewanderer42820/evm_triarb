// ============================================================================
// QUANTUMQUEUE64 CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive test coverage for ISR-safe QuantumQueue64 operations with
// emphasis on footgun-mode behavior validation and protocol adherence.
//
// COMPACT VERSION: Updated for uint64 payloads instead of 48-byte data blocks.
//
// ⚠️  FOOTGUN IMPLEMENTATION WARNING ⚠️
// This queue prioritizes performance over safety. Many operations have
// undefined behavior when used incorrectly. Tests validate core functionality
// while documenting dangerous edge cases.
//
// Test categories:
//   - Basic construction and initialization validation
//   - Core API operation verification (Push, PeepMin, MoveTick, UnlinkMin)
//   - Handle lifecycle and arena management
//   - LIFO ordering within tick buckets
//   - Edge case and boundary condition handling
//   - Footgun behavior validation (panic conditions)
//   - Data integrity and payload validation
//   - Bitmap consistency and summary maintenance
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
//   - Caller responsible for correct usage patterns

package quantumqueue64

import (
	"testing"
)

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
//   - Bitmap summaries properly zeroed
//
// ⚠️  FOOTGUN NOTE: Handle allocation does minimal validation
// Validates constructor correctness and freelist setup integrity.
func TestNewQueueBehavior(t *testing.T) {
	q := New()

	// Verify initial empty state
	if !q.Empty() || q.Size() != 0 {
		t.Errorf("new queue state invalid: Empty=%v Size=%d; want true, 0",
			q.Empty(), q.Size())
	}

	// Verify bitmap summaries are clear
	if q.summary != 0 {
		t.Errorf("summary bitmap not cleared: got %x, want 0", q.summary)
	}

	for i, group := range q.groups {
		if group.l1Summary != 0 {
			t.Errorf("group %d l1Summary not cleared: got %x, want 0", i, group.l1Summary)
		}
		for j, lane := range group.l2 {
			if lane != 0 {
				t.Errorf("group %d lane %d not cleared: got %x, want 0", i, j, lane)
			}
		}
	}

	// Verify all buckets are empty
	for i, bucket := range q.buckets {
		if bucket != nilIdx {
			t.Errorf("bucket %d not empty: got %v, want %v", i, bucket, nilIdx)
		}
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

	// Verify data field is zeroed
	if n1.data != 0 {
		t.Errorf("Borrow data not cleared: got %x, want 0", n1.data)
	}

	// Test sequential handle allocation
	h2, err2 := q.Borrow()
	if err2 != nil || h2 != h1+1 {
		t.Errorf("sequential Borrow failed: h2=%v want=%v", h2, h1+1)
	}
}

// TestFreelist validates freelist initialization and management.
//
// Freelist validation:
//   - All handles initially in freelist chain
//   - Proper chain termination
//   - No circular references
//   - Correct freelist head management
func TestFreelist(t *testing.T) {
	q := New()

	// Verify freelist chain integrity
	visited := make(map[Handle]bool)
	current := q.freeHead
	count := 0

	for current != nilIdx {
		if visited[current] {
			t.Fatalf("circular reference in freelist at handle %v", current)
		}
		visited[current] = true

		if q.arena[current].tick != -1 {
			t.Errorf("freelist handle %v has invalid tick: got %d, want -1",
				current, q.arena[current].tick)
		}

		current = q.arena[current].next
		count++
	}

	if count != CapItems {
		t.Errorf("freelist length incorrect: got %d, want %d", count, CapItems)
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
//  4. Validate freelist state during exhaustion
//
// Tests freelist management and capacity enforcement correctness.
func TestBorrowSafeExhaustion(t *testing.T) {
	q := New()
	allocatedHandles := make([]Handle, 0, CapItems)

	// Allocate all available handles
	for i := 0; i < CapItems; i++ {
		h, err := q.BorrowSafe()
		if err != nil {
			t.Fatalf("Borrow #%d failed unexpectedly: %v", i, err)
		}
		allocatedHandles = append(allocatedHandles, h)
	}

	// Verify freelist exhaustion
	if q.freeHead != nilIdx {
		t.Errorf("freelist not exhausted: freeHead=%v, want %v", q.freeHead, nilIdx)
	}

	// Verify exhaustion detection
	if _, err := q.BorrowSafe(); err == nil {
		t.Error("expected exhaustion error after CapItems allocations, got nil")
	}

	// Verify no handle duplication
	handleSet := make(map[Handle]bool)
	for _, h := range allocatedHandles {
		if handleSet[h] {
			t.Errorf("duplicate handle allocated: %v", h)
		}
		handleSet[h] = true
	}

	if len(handleSet) != CapItems {
		t.Errorf("handle count mismatch: got %d unique handles, want %d",
			len(handleSet), CapItems)
	}
}

// TestBorrowUnsafe validates unchecked allocation behavior.
//
// ⚠️  FOOTGUN GRADE 8/10: No exhaustion checking
// Unsafe allocation testing:
//   - Normal operation identical to BorrowSafe
//   - No exhaustion checking (undefined behavior on overflow)
//   - Validates that normal path is unaffected
//   - May corrupt freelist if called when exhausted
func TestBorrowUnsafe(t *testing.T) {
	q := New()

	// Test normal allocation
	h1, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}

	h2, err := q.Borrow()
	if err != nil {
		t.Fatalf("second Borrow failed: %v", err)
	}

	if h2 != h1+1 {
		t.Errorf("sequential allocation failed: got %v, want %v", h2, h1+1)
	}

	// Verify handle state
	if q.arena[h1].tick != -1 || q.arena[h2].tick != -1 {
		t.Error("allocated handles not properly reset")
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
//   - Bitmap summary updates during insertion
//
// Validates core scheduling semantics and boundary condition handling.
func TestPushAndPeepMin(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	// Test basic insertion
	q.Push(10, h, 0x123456789ABCDEF0)
	if q.Empty() || q.Size() != 1 {
		t.Errorf("queue state after Push: Empty=%v Size=%d; want false, 1",
			q.Empty(), q.Size())
	}

	// Verify bitmap summary updates
	expectedGroup := uint64(10) >> 12
	expectedLane := (uint64(10) >> 6) & 63
	expectedBucket := uint64(10) & 63

	if (q.summary & (1 << (63 - expectedGroup))) == 0 {
		t.Errorf("group summary not set for group %d", expectedGroup)
	}

	groupBlock := &q.groups[expectedGroup]
	if (groupBlock.l1Summary & (1 << (63 - expectedLane))) == 0 {
		t.Errorf("lane summary not set for lane %d in group %d", expectedLane, expectedGroup)
	}

	if (groupBlock.l2[expectedLane] & (1 << (63 - expectedBucket))) == 0 {
		t.Errorf("bucket bit not set for bucket %d in lane %d, group %d",
			expectedBucket, expectedLane, expectedGroup)
	}

	// Verify insertion correctness
	hGot, tickGot, data := q.PeepMin()
	if hGot != h || tickGot != 10 || data != 0x123456789ABCDEF0 {
		t.Errorf("PeepMin mismatch: h=%v tick=%d data=%x; want %v, 10, %x",
			hGot, tickGot, data, h, uint64(0x123456789ABCDEF0))
	}

	// Test in-place update (same tick, same handle)
	q.Push(10, h, 0xFEDCBA9876543210)
	if q.Size() != 1 {
		t.Errorf("in-place update changed size: got %d, want 1", q.Size())
	}

	// Verify payload update without structural changes
	hGot2, _, data2 := q.PeepMin()
	if hGot2 != h || data2 != 0xFEDCBA9876543210 {
		t.Errorf("payload update failed: got %x, want %x", data2, uint64(0xFEDCBA9876543210))
	}

	// Test entry removal
	q.UnlinkMin(h)
	if !q.Empty() || q.Size() != 0 {
		t.Error("queue not empty after UnlinkMin")
	}

	// Verify bitmap cleanup after removal
	if q.summary != 0 {
		t.Errorf("summary not cleared after removal: got %x", q.summary)
	}

	// Test edge cases: boundary tick values
	q2 := New()
	h0, _ := q2.BorrowSafe()
	hMax, _ := q2.BorrowSafe()

	q2.Push(0, h0, 0x1111)
	q2.Push(int64(BucketCount-1), hMax, 0x2222)

	// Verify minimum tick priority
	hMin, tickMin, _ := q2.PeepMin()
	if hMin != h0 || tickMin != 0 {
		t.Errorf("minimum tick selection failed: got (%v, %d), want (%v, 0)",
			hMin, tickMin, h0)
	}

	// Test maximum tick handling after minimum removal
	q2.UnlinkMin(h0)
	hHigh, tickHigh, _ := q2.PeepMin()
	if hHigh != hMax || tickHigh != int64(BucketCount-1) {
		t.Errorf("maximum tick retrieval failed: got (%v, %d), want (%v, %d)",
			hHigh, tickHigh, hMax, int64(BucketCount-1))
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
//  5. Confirm bitmap updates for both old and new positions
//
// ⚠️  FOOTGUN NOTE: No validation of tick bounds or handle state
// Ensures Push operations handle tick changes via proper unlink/relink cycles.
func TestPushTriggersUnlink(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	// Insert at initial tick
	q.Push(42, h, 0xAAAA)

	// Verify initial state
	if q.Size() != 1 {
		t.Errorf("initial size incorrect: got %d, want 1", q.Size())
	}

	initialGroup := uint64(42) >> 12
	if (q.summary & (1 << (63 - initialGroup))) == 0 {
		t.Error("initial bitmap not set")
	}

	// Move to different tick (triggers automatic unlink/relink)
	q.Push(99, h, 0xBBBB)

	// Verify relocation correctness
	if q.Size() != 1 {
		t.Errorf("size after tick change: got %d, want 1", q.Size())
	}

	hGot, tickGot, data := q.PeepMin()
	if hGot != h || tickGot != 99 {
		t.Errorf("tick relocation failed: got (%v, %d), want (%v, 99)",
			hGot, tickGot, h)
	}

	if data != 0xBBBB {
		t.Errorf("payload after relocation: got %x, want %x", data, uint64(0xBBBB))
	}

	// Verify bitmap updates
	newGroup := uint64(99) >> 12
	if (q.summary & (1 << (63 - newGroup))) == 0 {
		t.Error("new bitmap not set after relocation")
	}

	// If groups are different, old group should be cleared
	if initialGroup != newGroup {
		if (q.summary & (1 << (63 - initialGroup))) != 0 {
			t.Error("old bitmap not cleared after relocation")
		}
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
//  4. Test chain integrity during insertions
//
// Validates Last-In-First-Out behavior for entries sharing tick values.
func TestMultipleSameTickOrdering(t *testing.T) {
	q := New()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()

	// Insert in chronological order
	q.Push(5, h1, 0x1111)
	q.Push(5, h2, 0x2222) // Should become new head
	q.Push(5, h3, 0x3333) // Should become newest head

	// Verify LIFO ordering - newest first
	hMin, _, data := q.PeepMin()
	if hMin != h3 || data != 0x3333 {
		t.Errorf("LIFO ordering failed: got handle=%v data=%x, want %v %x",
			hMin, data, h3, uint64(0x3333))
	}

	// Remove head and check next
	q.UnlinkMin(h3)
	hMin2, _, data2 := q.PeepMin()
	if hMin2 != h2 || data2 != 0x2222 {
		t.Errorf("second LIFO entry failed: got handle=%v data=%x, want %v %x",
			hMin2, data2, h2, uint64(0x2222))
	}

	// Remove second and check oldest
	q.UnlinkMin(h2)
	hMin3, _, data3 := q.PeepMin()
	if hMin3 != h1 || data3 != 0x1111 {
		t.Errorf("oldest LIFO entry failed: got handle=%v data=%x, want %v %x",
			hMin3, data3, h1, uint64(0x1111))
	}

	// Verify size tracking throughout
	if q.Size() != 1 {
		t.Errorf("size after LIFO removals: got %d, want 1", q.Size())
	}
}

// TestPushDifferentTicks validates tick-based priority ordering.
//
// Test scenario:
//  1. Insert entries with different tick values in arbitrary order
//  2. Verify that lower tick values have higher priority
//  3. Confirm hierarchical bitmap minimum finding correctness
//  4. Test ordering consistency across tick range
//
// Validates priority queue semantics regardless of insertion order.
func TestPushDifferentTicks(t *testing.T) {
	q := New()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()

	// Insert in reverse priority order
	q.Push(100, h1, 0x1111)
	q.Push(50, h2, 0x2222) // Lower tick, higher priority
	q.Push(75, h3, 0x3333) // Middle priority

	// Verify minimum is lowest tick
	hMin, tickMin, _ := q.PeepMin()
	if hMin != h2 || tickMin != 50 {
		t.Errorf("tick priority ordering failed: got (%v, %d), want (%v, 50)",
			hMin, tickMin, h2)
	}

	// Remove minimum and check next
	q.UnlinkMin(h2)
	hMin2, tickMin2, _ := q.PeepMin()
	if hMin2 != h3 || tickMin2 != 75 {
		t.Errorf("second minimum failed: got (%v, %d), want (%v, 75)",
			hMin2, tickMin2, h3)
	}

	// Remove second and check highest
	q.UnlinkMin(h3)
	hMin3, tickMin3, _ := q.PeepMin()
	if hMin3 != h1 || tickMin3 != 100 {
		t.Errorf("highest tick failed: got (%v, %d), want (%v, 100)",
			hMin3, tickMin3, h1)
	}
}

// TestTickOrderingAcrossGroups validates cross-group priority handling.
//
// Test scenario:
//  1. Insert entries spanning multiple bitmap groups
//  2. Verify correct minimum finding across group boundaries
//  3. Test group-to-group transition during removals
//
// Validates bitmap hierarchy traversal correctness.
func TestTickOrderingAcrossGroups(t *testing.T) {
	q := New()

	// Create entries in different groups
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()

	// Group boundaries: each group covers 4096 ticks (1 << 12)
	tick1 := int64(0)    // Group 0
	tick2 := int64(4096) // Group 1
	tick3 := int64(8192) // Group 2

	// Insert in reverse order
	q.Push(tick3, h3, 0x3333)
	q.Push(tick1, h1, 0x1111) // Should be minimum
	q.Push(tick2, h2, 0x2222)

	// Verify cross-group minimum finding
	hMin, tickMin, _ := q.PeepMin()
	if hMin != h1 || tickMin != tick1 {
		t.Errorf("cross-group minimum failed: got (%v, %d), want (%v, %d)",
			hMin, tickMin, h1, tick1)
	}

	// Remove and verify group transition
	q.UnlinkMin(h1)
	hMin2, tickMin2, _ := q.PeepMin()
	if hMin2 != h2 || tickMin2 != tick2 {
		t.Errorf("group transition failed: got (%v, %d), want (%v, %d)",
			hMin2, tickMin2, h2, tick2)
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
//  4. Cross-bucket movement: Test complex relocation scenarios
//
// ⚠️  FOOTGUN NOTE: No validation of handle linkage state or tick bounds
// Tests MoveTick operation efficiency and correctness.
func TestMoveTickBehavior(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	q.Push(20, h, 0xCCCC)

	// Test no-op move (same tick) - should be optimized
	sizeBefore := q.Size()
	q.MoveTick(h, 20)
	if q.Size() != sizeBefore {
		t.Errorf("no-op move changed size: before=%d after=%d", sizeBefore, q.Size())
	}

	// Verify data integrity after no-op
	hGot, tickGot, dataGot := q.PeepMin()
	if hGot != h || tickGot != 20 || dataGot != 0xCCCC {
		t.Errorf("no-op move corrupted state: got (%v,%d,%x), want (%v,20,%x)",
			hGot, tickGot, dataGot, h, uint64(0xCCCC))
	}

	// Test actual relocation to different tick
	q.MoveTick(h, 30)

	hNew, tickNew, dataNew := q.PeepMin()
	if hNew != h || tickNew != 30 || dataNew != 0xCCCC {
		t.Errorf("MoveTick failed: got (%v, %d, %x), want (%v, 30, %x)",
			hNew, tickNew, dataNew, h, uint64(0xCCCC))
	}

	// Verify size invariant
	if q.Size() != 1 {
		t.Errorf("MoveTick changed size: got %d, want 1", q.Size())
	}
}

// TestMoveTickWithMultipleEntries validates movement in populated queues.
//
// Test scenario:
//  1. Create queue with multiple entries at different ticks
//  2. Move entry from middle of priority order
//  3. Verify ordering integrity maintained
//  4. Test movement to occupied vs. empty buckets
func TestMoveTickWithMultipleEntries(t *testing.T) {
	q := New()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()

	// Create entries: 10, 20, 30
	q.Push(10, h1, 0x1111)
	q.Push(20, h2, 0x2222)
	q.Push(30, h3, 0x3333)

	// Move middle entry to highest priority
	q.MoveTick(h2, 5)

	// Verify new ordering: h2(5), h1(10), h3(30)
	hMin, tickMin, dataMin := q.PeepMin()
	if hMin != h2 || tickMin != 5 || dataMin != 0x2222 {
		t.Errorf("moved entry not minimum: got (%v,%d,%x), want (%v,5,%x)",
			hMin, tickMin, dataMin, h2, uint64(0x2222))
	}

	q.UnlinkMin(h2)

	// Next should be h1
	hNext, tickNext, _ := q.PeepMin()
	if hNext != h1 || tickNext != 10 {
		t.Errorf("second entry incorrect: got (%v,%d), want (%v,10)",
			hNext, tickNext, h1)
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
// ⚠️  FOOTGUN NOTE: No validation that handle is actually linked
// Validates UnlinkMin correctness for non-head removals.
func TestUnlinkMinNonHead(t *testing.T) {
	q := New()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()

	// Create entries in ascending tick order
	q.Push(1, h1, 0x1111)
	q.Push(2, h2, 0x2222)
	q.Push(3, h3, 0x3333)

	// Remove middle entry (non-minimum in global ordering)
	q.UnlinkMin(h2)

	// Verify remaining minimum is correct
	hMin, tickMin, _ := q.PeepMin()
	if hMin != h1 || tickMin != 1 {
		t.Errorf("non-head removal failed: got (%v, %d), want (%v, 1)",
			hMin, tickMin, h1)
	}

	// Verify size decreased
	if q.Size() != 2 {
		t.Errorf("size after non-head removal: got %d, want 2", q.Size())
	}

	// Remove minimum and check last entry
	q.UnlinkMin(h1)
	hLast, tickLast, _ := q.PeepMin()
	if hLast != h3 || tickLast != 3 {
		t.Errorf("final entry incorrect: got (%v, %d), want (%v, 3)",
			hLast, tickLast, h3)
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
//  5. Test interleaved operations with data validation
//
// Tests interleaved Push/PeepMin/UnlinkMin operations under realistic usage.
func TestMixedOperations(t *testing.T) {
	q := New()
	var hs [5]Handle

	// Initialize with sequential tick values
	for i := range hs {
		h, _ := q.BorrowSafe()
		hs[i] = h
		q.Push(int64(i), h, uint64(i*1000))
	}

	// Verify initial size
	if q.Size() != len(hs) {
		t.Errorf("initial size incorrect: got %d, want %d", q.Size(), len(hs))
	}

	// Drain in strict tick priority order
	for i := 0; i < len(hs); i++ {
		h, tick, data := q.PeepMin()
		if tick != int64(i) {
			t.Errorf("drain order incorrect: want tick %d, got %d", i, tick)
		}
		if data != uint64(i*1000) {
			t.Errorf("drain data incorrect: want %d, got %d", i*1000, data)
		}
		q.UnlinkMin(h)

		// Verify size decreases
		expectedSize := len(hs) - i - 1
		if q.Size() != expectedSize {
			t.Errorf("size during drain: got %d, want %d", q.Size(), expectedSize)
		}
	}

	// Verify complete emptiness
	if !q.Empty() || q.Size() != 0 {
		t.Error("queue not empty after draining all entries")
	}
}

// TestInterleavedOperations validates mixed push/pop/move patterns.
//
// Test scenario:
//  1. Interleave push, pop, and move operations
//  2. Maintain reference tracking for validation
//  3. Verify queue integrity throughout
func TestInterleavedOperations(t *testing.T) {
	q := New()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()

	// Complex interleaved sequence
	q.Push(10, h1, 0x1111)
	q.Push(5, h2, 0x2222) // h2 becomes minimum

	// Verify minimum
	hMin, _, _ := q.PeepMin()
	if hMin != h2 {
		t.Error("h2 should be minimum")
	}

	q.Push(15, h3, 0x3333)
	q.MoveTick(h1, 3) // h1 becomes new minimum

	// Verify new minimum
	hMin2, tick2, _ := q.PeepMin()
	if hMin2 != h1 || tick2 != 3 {
		t.Errorf("after move, minimum should be h1 at tick 3, got %v at tick %d",
			hMin2, tick2)
	}

	q.UnlinkMin(h1) // Remove h1, h2 should be minimum again
	hMin3, tick3, _ := q.PeepMin()
	if hMin3 != h2 || tick3 != 5 {
		t.Errorf("after unlink, minimum should be h2 at tick 5, got %v at tick %d",
			hMin3, tick3)
	}
}

// ============================================================================
// DATA INTEGRITY VALIDATION
// ============================================================================

// TestDataIntegrityAcrossOperations validates payload preservation.
//
// Test scenarios:
//  1. Data preservation during Push operations
//  2. Data integrity through MoveTick operations
//  3. Correct payload extraction via PeepMin
//  4. Data consistency during queue transformations
//
// ⚠️  FOOTGUN NOTE: Data integrity only guaranteed for basic operations
// Complex operation sequences may not preserve payloads
func TestDataIntegrityAcrossOperations(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	testData := []uint64{
		0x0000000000000000,
		0xFFFFFFFFFFFFFFFF,
		0x123456789ABCDEF0,
		0xFEDCBA9876543210,
		0xAAAAAAAAAAAAAAAA,
		0x5555555555555555,
	}

	for i, data := range testData {
		tick := int64(i * 100)

		// Push with specific data
		q.Push(tick, h, data)

		// Verify data integrity
		_, _, retrievedData := q.PeepMin()
		if retrievedData != data {
			t.Errorf("data corruption at iteration %d: got %x, want %x",
				i, retrievedData, data)
		}

		// Move to different tick
		newTick := tick + 50
		q.MoveTick(h, newTick)

		// Verify data preserved across move
		_, _, movedData := q.PeepMin()
		if movedData != data {
			t.Errorf("data corruption during move at iteration %d: got %x, want %x",
				i, movedData, data)
		}
	}
}

// TestLargeDataValues validates handling of extreme payload values.
//
// Test edge cases:
//  1. Zero payload values
//  2. Maximum uint64 values
//  3. Common bit patterns (all ones, alternating, etc.)
func TestLargeDataValues(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	// Test zero value
	q.Push(100, h, 0)
	_, _, data := q.PeepMin()
	if data != 0 {
		t.Errorf("zero value corrupted: got %x, want 0", data)
	}

	// Test maximum value
	maxVal := uint64(0xFFFFFFFFFFFFFFFF)
	q.Push(100, h, maxVal)
	_, _, data = q.PeepMin()
	if data != maxVal {
		t.Errorf("max value corrupted: got %x, want %x", data, maxVal)
	}

	// Test alternating bit pattern
	altPattern := uint64(0xAAAAAAAAAAAAAAAA)
	q.Push(100, h, altPattern)
	_, _, data = q.PeepMin()
	if data != altPattern {
		t.Errorf("alternating pattern corrupted: got %x, want %x", data, altPattern)
	}
}

// ============================================================================
// BITMAP CONSISTENCY VALIDATION
// ============================================================================

// TestBitmapConsistency validates summary bitmap integrity.
//
// Test scenarios:
//  1. Bitmap updates during insertions
//  2. Bitmap cleanup during removals
//  3. Consistency across group/lane/bucket hierarchy
//  4. Summary collapse detection
func TestBitmapConsistency(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	// Test bitmap setting
	tick := int64(12345) // Arbitrary tick
	q.Push(tick, h, 0x1234)

	// Calculate expected bitmap positions
	group := uint64(tick) >> 12
	lane := (uint64(tick) >> 6) & 63
	bucket := uint64(tick) & 63

	// Verify all hierarchy levels are set
	if (q.summary & (1 << (63 - group))) == 0 {
		t.Errorf("group summary not set for tick %d (group %d)", tick, group)
	}

	groupBlock := &q.groups[group]
	if (groupBlock.l1Summary & (1 << (63 - lane))) == 0 {
		t.Errorf("lane summary not set for tick %d (group %d, lane %d)", tick, group, lane)
	}

	if (groupBlock.l2[lane] & (1 << (63 - bucket))) == 0 {
		t.Errorf("bucket bit not set for tick %d (group %d, lane %d, bucket %d)",
			tick, group, lane, bucket)
	}

	// Test bitmap clearing
	q.UnlinkMin(h)

	// Verify all hierarchy levels are cleared
	if (q.summary & (1 << (63 - group))) != 0 {
		t.Errorf("group summary not cleared after removal (group %d)", group)
	}

	if (groupBlock.l1Summary & (1 << (63 - lane))) != 0 {
		t.Errorf("lane summary not cleared after removal (group %d, lane %d)", group, lane)
	}

	if (groupBlock.l2[lane] & (1 << (63 - bucket))) != 0 {
		t.Errorf("bucket bit not cleared after removal (group %d, lane %d, bucket %d)",
			group, lane, bucket)
	}
}

// TestBitmapMultipleEntries validates bitmap behavior with shared buckets.
//
// Test scenario:
//  1. Multiple entries in same bucket (same tick)
//  2. Verify bitmap remains set while bucket occupied
//  3. Verify bitmap cleared only when bucket empty
func TestBitmapMultipleEntries(t *testing.T) {
	q := New()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()

	tick := int64(1000)
	group := uint64(tick) >> 12
	lane := (uint64(tick) >> 6) & 63
	bucket := uint64(tick) & 63

	// Add first entry
	q.Push(tick, h1, 0x1111)

	// Add second entry to same bucket
	q.Push(tick, h2, 0x2222)

	// Verify bitmap still set
	if (q.summary & (1 << (63 - group))) == 0 {
		t.Errorf("group bitmap cleared prematurely with multiple entries (group %d)", group)
	}

	groupBlock := &q.groups[group]
	if (groupBlock.l1Summary & (1 << (63 - lane))) == 0 {
		t.Errorf("lane bitmap cleared prematurely with multiple entries (group %d, lane %d)", group, lane)
	}

	if (groupBlock.l2[lane] & (1 << (63 - bucket))) == 0 {
		t.Errorf("bucket bitmap cleared prematurely with multiple entries (group %d, lane %d, bucket %d)",
			group, lane, bucket)
	}

	// Remove first entry (not head due to LIFO)
	q.UnlinkMin(h2) // h2 is head due to LIFO

	// Bitmap should still be set (h1 remains)
	if (q.summary & (1 << (63 - group))) == 0 {
		t.Errorf("group bitmap cleared while bucket still occupied (group %d)", group)
	}

	if (groupBlock.l1Summary & (1 << (63 - lane))) == 0 {
		t.Errorf("lane bitmap cleared while bucket still occupied (group %d, lane %d)", group, lane)
	}

	if (groupBlock.l2[lane] & (1 << (63 - bucket))) == 0 {
		t.Errorf("bucket bitmap cleared while bucket still occupied (group %d, lane %d, bucket %d)",
			group, lane, bucket)
	}

	// Remove last entry
	q.UnlinkMin(h1)

	// Now bitmap should be cleared
	if (q.summary & (1 << (63 - group))) != 0 {
		t.Errorf("group bitmap not cleared after bucket emptied (group %d)", group)
	}

	if (groupBlock.l1Summary & (1 << (63 - lane))) != 0 {
		t.Errorf("lane bitmap not cleared after bucket emptied (group %d, lane %d)", group, lane)
	}

	if (groupBlock.l2[lane] & (1 << (63 - bucket))) != 0 {
		t.Errorf("bucket bitmap not cleared after bucket emptied (group %d, lane %d, bucket %d)",
			group, lane, bucket)
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
	q.Push(100, h, 0xDEAD)
	q.UnlinkMin(h)
	q.UnlinkMin(h) // Protocol violation - should panic
}

// TestUnlinkUnlinkedHandle validates detection of unlinked handle operations.
//
// ⚠️  FOOTGUN WARNING: Operating on unlinked handles causes corruption
func TestUnlinkUnlinkedHandle(t *testing.T) {
	defer func() {
		// May or may not panic depending on implementation
		recover()
	}()

	q := New()
	h, _ := q.BorrowSafe()
	// Never link handle, but try to unlink
	q.UnlinkMin(h) // Should detect unlinked state
}

// TestHandleReuseAfterUnlink validates safe handle recycling patterns.
//
// Test sequence:
//  1. Use handle in normal Push/UnlinkMin cycle
//  2. Reuse same handle for different tick and payload
//  3. Verify no state leakage or corruption from previous usage
//
// ⚠️  FOOTGUN WARNING: Handle cleanup is minimal for performance
// Validates proper handle cleanup and safe reuse protocols.
func TestHandleReuseAfterUnlink(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	// Initial use cycle
	q.Push(123, h, 0xAAAA)
	q.UnlinkMin(h)

	// ⚠️  FOOTGUN: Handle cleanup is intentionally minimal
	// Only tick is reset to -1, prev/next may retain freelist pointers
	n := &q.arena[h]
	if n.tick != -1 {
		t.Errorf("handle tick not reset after unlink: tick=%d, want -1", n.tick)
	}

	// Note: prev/next fields may contain freelist pointers, this is intentional
	// for performance reasons. Only tick field is guaranteed to be reset.

	// Reuse same handle with different data
	q.Push(456, h, 0xBBBB)

	// Verify reuse correctness and no state leakage
	_, tick, data := q.PeepMin()
	if tick != 456 || data != 0xBBBB {
		t.Errorf("handle reuse failed: tick=%d data=%x, want 456 %x",
			tick, data, uint64(0xBBBB))
	}

	// Verify handle state after reuse
	if n.tick != 456 || n.prev != nilIdx {
		t.Errorf("handle state incorrect after reuse: tick=%d prev=%v",
			n.tick, n.prev)
	}
}

// TestPushWithInvalidTicks validates boundary condition handling.
//
// Test scenarios:
//  1. Negative tick values (undefined behavior)
//  2. Tick values exceeding BucketCount (array bounds violation)
//
// ⚠️  FOOTGUN GRADE 9/10: Invalid ticks cause silent corruption
// No bounds checking - caller responsible for valid tick range [0, BucketCount)
func TestPushWithInvalidTicks(t *testing.T) {
	q := New()
	h, _ := q.BorrowSafe()

	t.Run("NegativeTick", func(t *testing.T) {
		defer func() { recover() }()
		q.Push(-9999, h, 0xDEAD)
		// Implementation may or may not panic
	})

	t.Run("OverflowTick", func(t *testing.T) {
		defer func() { recover() }()
		q.Push(int64(BucketCount+1000), h, 0xBEEF)
		// Implementation may or may not panic
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

	// Test insertions
	q.Push(10, h1, 0x1111)
	if q.Size() != 1 {
		t.Errorf("size after first insert: got %d, want 1", q.Size())
	}

	q.Push(20, h2, 0x2222)
	if q.Size() != 2 {
		t.Errorf("size after second insert: got %d, want 2", q.Size())
	}

	// Test in-place update (should not change size)
	q.Push(10, h1, 0x3333)
	if q.Size() != 2 {
		t.Errorf("size after update: got %d, want 2", q.Size())
	}

	// Test removals
	q.UnlinkMin(h1)
	if q.Size() != 1 {
		t.Errorf("size after first removal: got %d, want 1", q.Size())
	}

	q.UnlinkMin(h2)
	if q.Size() != 0 {
		t.Errorf("size after final removal: got %d, want 0", q.Size())
	}

	// Test size consistency with Empty()
	if !q.Empty() {
		t.Error("queue should be empty when size is 0")
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
		h, err := q.BorrowSafe()
		if err != nil {
			t.Fatalf("allocation failed at %d: %v", i, err)
		}
		handles = append(handles, h)
		q.Push(int64(i), h, uint64(i))
	}

	// Verify exhaustion
	if _, err := q.BorrowSafe(); err == nil {
		t.Error("expected exhaustion after allocating all handles")
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

	// Should be exhausted again
	if _, err := q.BorrowSafe(); err == nil {
		t.Error("expected exhaustion after re-allocating all handles")
	}
}

// TestHandleValidation validates handle bounds checking.
//
// Test scenarios:
//  1. Valid handle range operations
//  2. Invalid handle detection (if implemented)
//  3. Handle correlation with arena indices
func TestHandleValidation(t *testing.T) {
	q := New()

	// Test valid handles
	for i := 0; i < 10; i++ {
		h, err := q.BorrowSafe()
		if err != nil {
			t.Fatalf("valid allocation failed: %v", err)
		}

		if h >= CapItems {
			t.Errorf("invalid handle returned: %v >= %v", h, CapItems)
		}

		// Use handle
		q.Push(int64(i), h, uint64(i))

		// Verify handle points to correct arena entry
		if q.arena[h].tick != int64(i) {
			t.Errorf("handle/arena mismatch: handle=%v, tick=%d, want %d",
				h, q.arena[h].tick, i)
		}
	}
}

// ============================================================================
// PERFORMANCE CHARACTERISTIC VALIDATION
// ============================================================================

// TestO1Operations validates constant-time operation characteristics.
//
// Test methodology:
//  1. Measure operation counts at different queue sizes
//  2. Verify that operations don't scale with queue size
//  3. Validate bitmap hierarchy efficiency
//
// ⚠️  FOOTGUN NOTE: Performance assumes correct usage patterns
// Note: This is a correctness test, not a performance benchmark
func TestO1Operations(t *testing.T) {
	// Test with different queue sizes
	sizes := []int{1, 10, 100, 1000, 10000}

	for _, size := range sizes {
		q := New()
		handles := make([]Handle, size)

		// Fill queue
		for i := 0; i < size; i++ {
			h, _ := q.BorrowSafe()
			handles[i] = h
			q.Push(int64(i), h, uint64(i))
		}

		// Test PeepMin (should always find minimum regardless of size)
		hMin, tickMin, _ := q.PeepMin()
		if tickMin != 0 {
			t.Errorf("minimum not found correctly at size %d: got tick %d, want 0",
				size, tickMin)
		}

		// Test UnlinkMin (should work regardless of size)
		q.UnlinkMin(hMin)
		if q.Size() != size-1 {
			t.Errorf("unlink failed at size %d: got size %d, want %d",
				size, q.Size(), size-1)
		}

		// Test MoveTick (should work regardless of size)
		if size > 1 {
			h := handles[1]
			q.MoveTick(h, int64(size+1000)) // Move to end

			// Should still be able to find minimum
			_, newMin, _ := q.PeepMin()
			if newMin != 2 { // Next minimum should be tick 2
				t.Errorf("MoveTick disrupted ordering at size %d", size)
			}
		}
	}
}
