// ============================================================================
// COMPACTQUEUE128 CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive test coverage for minimal CompactQueue128 operations with
// emphasis on shared memory pool architecture and external handle management.

package compactqueue128

import (
	"testing"
	"unsafe"
)

// InitializePool properly initializes a pool to unlinked state.
func InitializePool(pool []Entry) {
	for i := range pool {
		pool[i].Tick = -1     // Mark as unlinked
		pool[i].Prev = nilIdx // Clear prev pointer
		pool[i].Next = nilIdx // Clear next pointer
		pool[i].Data = 0      // Clear data
	}
}

const testPoolSize = 10000 // Pool size for testing

// ============================================================================
// BASIC CONSTRUCTION AND INITIALIZATION
// ============================================================================

// TestNewQueueBehavior validates proper queue initialization with external pools.
func TestNewQueueBehavior(t *testing.T) {
	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Verify initial empty state
	if !q.Empty() || q.Size() != 0 {
		t.Errorf("new queue state invalid: Empty=%v Size=%d; want true, 0",
			q.Empty(), q.Size())
	}

	// Verify arena pointer is set
	if q.arena == 0 {
		t.Error("arena pointer not set")
	}

	// Verify bitmap summaries are clear for minimal structure
	if q.summary != 0 {
		t.Errorf("summary bitmap not cleared: got %x, want 0", q.summary)
	}

	// Verify group structure is clear
	gb := &q.groups[0] // Only one group in minimal structure
	if gb.l1Summary != 0 {
		t.Errorf("group 0 l1Summary not cleared: got %x, want 0", gb.l1Summary)
	}
	for j, lane := range gb.l2 {
		if lane != 0 {
			t.Errorf("group 0 lane %d not cleared: got %x, want 0", j, lane)
		}
	}

	// Verify all buckets are empty
	for i, bucket := range q.buckets {
		if bucket != nilIdx {
			t.Errorf("bucket %d not empty: got %v, want %v", i, bucket, nilIdx)
		}
	}
}

// TestPoolAccess validates external pool entry access.
func TestPoolAccess(t *testing.T) {
	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Test entry access for various handles
	for h := Handle(0); h < 10; h++ {
		entry := q.entry(h)

		// Verify entry is within pool bounds
		entryAddr := uintptr(unsafe.Pointer(entry))
		poolStart := uintptr(unsafe.Pointer(&pool[0]))
		poolEnd := poolStart + uintptr(len(pool))*unsafe.Sizeof(Entry{})

		if entryAddr < poolStart || entryAddr >= poolEnd {
			t.Errorf("entry %d outside pool bounds: addr=%x, start=%x, end=%x",
				h, entryAddr, poolStart, poolEnd)
		}

		// Verify handle-to-entry mapping
		expectedEntry := &pool[h]
		if entry != expectedEntry {
			t.Errorf("handle %d maps to wrong entry: got %p, want %p",
				h, entry, expectedEntry)
		}
	}
}

// ============================================================================
// CORE API OPERATIONS
// ============================================================================

// TestPushAndPeepMin validates fundamental queue operations and edge cases.
func TestPushAndPeepMin(t *testing.T) {
	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)

	// Test basic insertion
	q.Push(10, h, 0x123456789ABCDEF0)
	if q.Empty() || q.Size() != 1 {
		t.Errorf("queue state after Push: Empty=%v Size=%d; want false, 1",
			q.Empty(), q.Size())
	}

	// Verify bitmap summary updates for minimal structure with original bit logic
	// Use ORIGINAL bit decomposition
	g := uint64(10) >> 12       // Group index (always 0 for 0-127)
	l := (uint64(10) >> 6) & 63 // Lane index (0 for ticks 0-63)
	bb := uint64(10) & 63       // Bucket index (10)

	if (q.summary & (1 << (63 - g))) == 0 {
		t.Errorf("group summary not set for group %d", g)
	}

	gb := &q.groups[g] // Always groups[0]
	if (gb.l1Summary & (1 << (63 - l))) == 0 {
		t.Errorf("lane summary not set for lane %d in group %d", l, g)
	}

	if (gb.l2[l] & (1 << (63 - bb))) == 0 {
		t.Errorf("bucket bit not set for bucket %d in lane %d, group %d",
			bb, l, g)
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
	pool2 := make([]Entry, testPoolSize)
	InitializePool(pool2)
	q2 := New(unsafe.Pointer(&pool2[0]))
	h0 := Handle(0)
	hMax := Handle(1)

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
func TestPushTriggersUnlink(t *testing.T) {
	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)

	// Insert at initial tick
	q.Push(42, h, 0xAAAA)

	// Verify initial state
	if q.Size() != 1 {
		t.Errorf("initial size incorrect: got %d, want 1", q.Size())
	}

	initialGroup := uint64(42) >> 6
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
	newGroup := uint64(99) >> 6
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
func TestMultipleSameTickOrdering(t *testing.T) {
	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h1 := Handle(0)
	h2 := Handle(1)
	h3 := Handle(2)

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
func TestPushDifferentTicks(t *testing.T) {
	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h1 := Handle(0)
	h2 := Handle(1)
	h3 := Handle(2)

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

// ============================================================================
// ADVANCED OPERATIONS
// ============================================================================

// TestMoveTickBehavior validates tick relocation functionality.
func TestMoveTickBehavior(t *testing.T) {
	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)

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

// ============================================================================
// FOOTGUN BEHAVIOR VALIDATION
// ============================================================================

// TestPeepMinWhenEmpty validates panic behavior on empty queue access.
func TestPeepMinWhenEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when calling PeepMin on empty queue")
		}
	}()
	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	q.PeepMin()
}

// TestDoubleUnlink validates panic behavior on protocol violations.
func TestDoubleUnlink(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on double unlink operation")
		}
	}()

	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	q.Push(100, h, 0xDEAD)
	q.UnlinkMin(h)
	q.UnlinkMin(h) // Protocol violation - should panic
}

// ============================================================================
// SHARED POOL VALIDATION
// ============================================================================

// TestSharedPoolUsage validates multiple queues sharing a single memory pool.
func TestSharedPoolUsage(t *testing.T) {
	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q1 := New(unsafe.Pointer(&pool[0]))
	q2 := New(unsafe.Pointer(&pool[0]))
	q3 := New(unsafe.Pointer(&pool[0]))

	// Use different handle ranges for each queue
	h1 := Handle(100)
	h2 := Handle(200)
	h3 := Handle(300)

	// Independent operations on shared pool
	q1.Push(10, h1, 0x1111)
	q2.Push(20, h2, 0x2222)
	q3.Push(5, h3, 0x3333)

	// Verify independent queue states
	if q1.Size() != 1 || q2.Size() != 1 || q3.Size() != 1 {
		t.Errorf("independent queue sizes incorrect: q1=%d q2=%d q3=%d",
			q1.Size(), q2.Size(), q3.Size())
	}

	// Verify independent minimums AND use all return values
	h1Min, tick1, data1 := q1.PeepMin()
	h2Min, tick2, data2 := q2.PeepMin()
	h3Min, tick3, data3 := q3.PeepMin()

	if h1Min != h1 || tick1 != 10 || data1 != 0x1111 {
		t.Errorf("q1 PeepMin failed: got (%v,%d,%x), want (%v,10,%x)",
			h1Min, tick1, data1, h1, uint64(0x1111))
	}

	if h2Min != h2 || tick2 != 20 || data2 != 0x2222 {
		t.Errorf("q2 PeepMin failed: got (%v,%d,%x), want (%v,20,%x)",
			h2Min, tick2, data2, h2, uint64(0x2222))
	}

	if h3Min != h3 || tick3 != 5 || data3 != 0x3333 {
		t.Errorf("q3 PeepMin failed: got (%v,%d,%x), want (%v,5,%x)",
			h3Min, tick3, data3, h3, uint64(0x3333))
	}

	// Verify shared pool entries AND cross-validate queue operations
	entry1 := &pool[h1]
	entry2 := &pool[h2]
	entry3 := &pool[h3]

	if entry1.Tick != 10 || entry1.Data != 0x1111 {
		t.Errorf("shared pool entry1 incorrect: tick=%d data=%x",
			entry1.Tick, entry1.Data)
	}

	if entry2.Tick != 20 || entry2.Data != 0x2222 {
		t.Errorf("shared pool entry2 incorrect: tick=%d data=%x",
			entry2.Tick, entry2.Data)
	}

	if entry3.Tick != 5 || entry3.Data != 0x3333 {
		t.Errorf("shared pool entry3 incorrect: tick=%d data=%x",
			entry3.Tick, entry3.Data)
	}

	// Verify queue independence by manipulating one queue
	// and ensuring others are unaffected
	q1.UnlinkMin(h1)
	if q1.Size() != 0 {
		t.Error("q1 should be empty after unlink")
	}
	if q2.Size() != 1 || q3.Size() != 1 {
		t.Error("q2 and q3 should be unaffected by q1 operations")
	}

	// Verify the other queues still work correctly
	h2Min2, tick2_2, data2_2 := q2.PeepMin()
	h3Min2, tick3_2, data3_2 := q3.PeepMin()

	if h2Min2 != h2 || tick2_2 != 20 || data2_2 != 0x2222 {
		t.Errorf("q2 affected by q1 operation: got (%v,%d,%x), want (%v,20,%x)",
			h2Min2, tick2_2, data2_2, h2, uint64(0x2222))
	}

	if h3Min2 != h3 || tick3_2 != 5 || data3_2 != 0x3333 {
		t.Errorf("q3 affected by q1 operation: got (%v,%d,%x), want (%v,5,%x)",
			h3Min2, tick3_2, data3_2, h3, uint64(0x3333))
	}
}

// TestPoolBoundaryAccess validates handle bounds within pool capacity.
func TestPoolBoundaryAccess(t *testing.T) {
	pool := make([]Entry, testPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Test various handle positions within pool
	testHandles := []Handle{0, 1, 100, 1000, Handle(testPoolSize - 1)}

	for i, h := range testHandles {
		// PROPER FIX: Use valid tick values for 128-priority queue
		tick := int64(i * 20) // Use ticks 0, 20, 40, 60, 80 - all valid

		// Use handle
		q.Push(tick, h, uint64(h)*1000)

		// Verify entry access AND operation correctness
		entry := q.entry(h)
		expectedEntry := &pool[h]

		if entry != expectedEntry {
			t.Errorf("handle %d entry mismatch: got %p, want %p",
				h, entry, expectedEntry)
		}

		// Verify data consistency through queue operations
		if entry.Tick != tick || entry.Data != uint64(h)*1000 {
			t.Errorf("handle %d data incorrect: tick=%d data=%d",
				h, entry.Tick, entry.Data)
		}

		// Verify queue operations work correctly with this handle
		retrievedH, retrievedTick, retrievedData := q.PeepMin()
		if retrievedH != h || retrievedTick != tick || retrievedData != uint64(h)*1000 {
			t.Errorf("handle %d queue operation failed: got (%v,%d,%d), want (%v,%d,%d)",
				h, retrievedH, retrievedTick, retrievedData, h, tick, uint64(h)*1000)
		}

		// Clean up
		q.UnlinkMin(h)

		// Verify cleanup was successful
		if !q.Empty() {
			t.Errorf("queue not empty after removing handle %d", h)
		}
	}
}
