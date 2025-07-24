// ============================================================================
// COMPACTQUEUE128 DEBUG TEST SUITE
// ============================================================================
//
// Comprehensive debugging tests to isolate LIFO ordering issues and compare
// behavior with reference implementations.

package compactqueue128

import (
	"container/heap"
	"testing"
	"unsafe"
)

// ============================================================================
// BASIC LIFO ORDERING TEST
// ============================================================================

// TestBasicLIFO verifies that our LIFO behavior works correctly
func TestBasicLIFO(t *testing.T) {
	pool := make([]Entry, 1000)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Insert same tick with different handles in sequence
	handles := []Handle{100, 200, 300}
	tick := int64(86) // Same tick as stress test failure

	t.Logf("=== INSERTION PHASE ===")
	for i, h := range handles {
		t.Logf("Inserting handle %v at tick %d (order %d)", h, tick, i)
		q.Push(tick, h, uint64(h))

		// Show current head after each insertion
		if !q.Empty() {
			hHead, _, _ := q.PeepMin()
			t.Logf("  Current head: %v", hHead)
		}
	}

	t.Logf("\n=== EXTRACTION PHASE ===")
	// Check what our queue returns (should be LIFO: 300, 200, 100)
	expectedOrder := []Handle{300, 200, 100} // LIFO order

	for i, expectedH := range expectedOrder {
		if q.Empty() {
			t.Fatalf("Queue empty prematurely at position %d", i)
		}

		hGot, tickGot, dataGot := q.PeepMin()

		t.Logf("PeepMin %d: got h=%v tick=%d data=%x, expected h=%v",
			i, hGot, tickGot, dataGot, expectedH)

		if hGot != expectedH {
			t.Errorf("LIFO order wrong at position %d: got h=%v, want h=%v", i, hGot, expectedH)
		}

		if tickGot != tick {
			t.Errorf("Wrong tick at position %d: got %d, want %d", i, tickGot, tick)
		}

		q.UnlinkMin(hGot)
		t.Logf("  Unlinked handle %v", hGot)
	}

	if !q.Empty() {
		t.Error("Queue should be empty after removing all entries")
	}

	t.Logf("=== TEST COMPLETE ===")
}

// ============================================================================
// COMPARISON WITH REFERENCE HEAP
// ============================================================================

// TestCompareWithReference compares our queue behavior with reference heap
func TestCompareWithReference(t *testing.T) {
	pool := make([]Entry, 1000)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Initialize reference heap
	ref := &stressHeap{}
	heap.Init(ref)

	// Test data
	operations := []struct {
		op     string
		tick   int64
		handle Handle
		seq    int
	}{
		{"push", 86, 100, 1},
		{"push", 86, 200, 2},
		{"push", 86, 300, 3},
		{"push", 50, 400, 4}, // Different tick
		{"push", 86, 500, 5}, // Back to same tick
	}

	t.Logf("=== OPERATION SEQUENCE ===")
	for _, op := range operations {
		t.Logf("Operation: %s tick=%d handle=%v seq=%d", op.op, op.tick, op.handle, op.seq)

		// Add to our queue
		q.Push(op.tick, op.handle, uint64(op.handle))

		// Add to reference heap
		heap.Push(ref, &stressItem{h: op.handle, tick: op.tick, seq: op.seq})

		// Show current minimums
		if !q.Empty() {
			qH, qTick, _ := q.PeepMin()
			t.Logf("  Queue min: h=%v tick=%d", qH, qTick)
		}

		if ref.Len() > 0 {
			refTop := (*ref)[0]
			t.Logf("  Ref min: h=%v tick=%d seq=%d", refTop.h, refTop.tick, refTop.seq)
		}
	}

	t.Logf("\n=== EXTRACTION COMPARISON ===")
	// Compare extraction order
	for i := 0; !q.Empty() && ref.Len() > 0; i++ {
		// Get from our queue
		qH, qTick, _ := q.PeepMin()
		q.UnlinkMin(qH)

		// Get from reference
		refItem := heap.Pop(ref).(*stressItem)

		t.Logf("Extract %d: Queue=(h=%v,tick=%d) Ref=(h=%v,tick=%d,seq=%d)",
			i, qH, qTick, refItem.h, refItem.tick, refItem.seq)

		if qH != refItem.h || qTick != refItem.tick {
			t.Errorf("Mismatch at extraction %d: got (h=%v,tick=%d); want (h=%v,tick=%d)",
				i, qH, qTick, refItem.h, refItem.tick)
		}
	}

	// Verify both are empty
	if !q.Empty() {
		t.Error("Queue not empty after comparison")
	}
	if ref.Len() != 0 {
		t.Error("Reference heap not empty after comparison")
	}
}

// ============================================================================
// MOVETICK BEHAVIOR TEST
// ============================================================================

// TestMoveTick verifies MoveTick behavior matches expectations
func TestMoveTick(t *testing.T) {
	pool := make([]Entry, 1000)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Setup: Put some entries in different ticks
	q.Push(50, Handle(100), 0x1111)
	q.Push(60, Handle(200), 0x2222)
	q.Push(60, Handle(300), 0x3333) // Same tick as 200

	t.Logf("=== INITIAL STATE ===")
	for i := 0; i < q.Size(); i++ {
		h, tick, data := q.PeepMin()
		t.Logf("Entry %d: h=%v tick=%d data=%x", i, h, tick, data)
		q.UnlinkMin(h)
		// Re-add for next iteration (this is just for viewing)
		q.Push(tick, h, data)
	}

	// Now move handle 200 from tick 60 to tick 86
	t.Logf("\n=== MOVING HANDLE 200 FROM TICK 60 TO TICK 86 ===")
	q.MoveTick(Handle(200), 86)

	// Add another entry at tick 86 to test LIFO
	t.Logf("Adding handle 400 at tick 86")
	q.Push(86, Handle(400), 0x4444)

	t.Logf("\n=== FINAL STATE ===")
	extractionOrder := []struct {
		handle Handle
		tick   int64
	}{}

	for !q.Empty() {
		h, tick, data := q.PeepMin()
		t.Logf("Extract: h=%v tick=%d data=%x", h, tick, data)
		extractionOrder = append(extractionOrder, struct {
			handle Handle
			tick   int64
		}{h, tick})
		q.UnlinkMin(h)
	}

	// Expected order:
	// tick 50: handle 100
	// tick 60: handle 300 (only one left at tick 60)
	// tick 86: handle 400, then handle 200 (LIFO within tick 86)

	expected := []struct {
		handle Handle
		tick   int64
	}{
		{Handle(100), 50}, // Lowest tick first
		{Handle(300), 60}, // Next tick
		{Handle(400), 86}, // tick 86, newest entry first (LIFO)
		{Handle(200), 86}, // tick 86, moved entry second (LIFO)
	}

	if len(extractionOrder) != len(expected) {
		t.Fatalf("Wrong number of extractions: got %d, want %d",
			len(extractionOrder), len(expected))
	}

	for i, exp := range expected {
		got := extractionOrder[i]
		if got.handle != exp.handle || got.tick != exp.tick {
			t.Errorf("Wrong extraction order at position %d: got (h=%v,tick=%d), want (h=%v,tick=%d)",
				i, got.handle, got.tick, exp.handle, exp.tick)
		}
	}
}

// ============================================================================
// STRESS TEST SIMULATION
// ============================================================================

// TestStressSimulation simulates the exact failure scenario from stress test
func TestStressSimulation(t *testing.T) {
	pool := make([]Entry, 1000)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Initialize reference heap
	ref := &stressHeap{}
	heap.Init(ref)

	// Simulate the exact sequence that causes stress test failure
	// This is a simplified version of the stress test operations

	operations := []struct {
		op     string
		tick   int64
		handle Handle
		seq    int
	}{
		// These simulate the operations leading to the failure
		{"push", 86, Handle(49968), 1000},
		{"push", 86, Handle(49969), 1001},
		{"push", 50, Handle(49970), 1002}, // Different tick
		{"move", 86, Handle(49970), 1003}, // Move to same tick as others
	}

	t.Logf("=== SIMULATING STRESS TEST FAILURE SCENARIO ===")

	for _, op := range operations {
		t.Logf("Op: %s tick=%d handle=%v seq=%d", op.op, op.tick, op.handle, op.seq)

		if op.op == "push" {
			// Add to our queue
			q.Push(op.tick, op.handle, uint64(op.handle))

			// Add to reference heap
			heap.Push(ref, &stressItem{h: op.handle, tick: op.tick, seq: op.seq})

		} else if op.op == "move" {
			// Move in our queue
			q.MoveTick(op.handle, op.tick)

			// Simulate move in reference: remove old, add new with new seq
			// Find and remove the old entry
			for i := len(*ref) - 1; i >= 0; i-- {
				if (*ref)[i].h == op.handle {
					heap.Remove(ref, i)
					break
				}
			}
			// Add back with new sequence number
			heap.Push(ref, &stressItem{h: op.handle, tick: op.tick, seq: op.seq})
		}

		// Show current state
		if !q.Empty() {
			qH, qTick, _ := q.PeepMin()
			t.Logf("  Queue min: h=%v tick=%d", qH, qTick)
		}

		if ref.Len() > 0 {
			refTop := (*ref)[0]
			t.Logf("  Ref min: h=%v tick=%d seq=%d", refTop.h, refTop.tick, refTop.seq)
		}
	}

	t.Logf("\n=== CHECKING FOR MISMATCH ===")

	// This should reproduce the stress test failure
	if !q.Empty() && ref.Len() > 0 {
		qH, qTick, _ := q.PeepMin()
		refItem := heap.Pop(ref).(*stressItem)

		t.Logf("Final comparison: Queue=(h=%v,tick=%d) Ref=(h=%v,tick=%d,seq=%d)",
			qH, qTick, refItem.h, refItem.tick, refItem.seq)

		if qH != refItem.h || qTick != refItem.tick {
			t.Logf("*** MISMATCH REPRODUCED! ***")
			t.Logf("This explains the stress test failure.")
			t.Logf("The issue is that MoveTick gives the moved entry a newer sequence")
			t.Logf("number in the reference, making it higher priority than entries")
			t.Logf("that were already in that bucket.")
		} else {
			t.Logf("No mismatch found - need to investigate further")
		}
	}
}

// ============================================================================
// BITMAP STATE INSPECTION
// ============================================================================

// TestBitmapState inspects internal bitmap state for debugging
func TestBitmapState(t *testing.T) {
	pool := make([]Entry, 1000)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	printBitmapState := func(label string) {
		t.Logf("=== %s ===", label)
		t.Logf("Summary: %064b", q.summary)

		gb := &q.groups[0]
		t.Logf("Group 0 l1Summary: %064b", gb.l1Summary)
		t.Logf("Group 0 l2[0]: %064b", gb.l2[0])
		t.Logf("Group 0 l2[1]: %064b", gb.l2[1])

		t.Logf("Size: %d", q.Size())
	}

	printBitmapState("INITIAL STATE")

	// Add some entries
	q.Push(10, Handle(100), 0x1111) // tick 10: g=0, l=0, b=10
	printBitmapState("AFTER PUSH tick=10")

	q.Push(80, Handle(200), 0x2222) // tick 80: g=0, l=1, b=16
	printBitmapState("AFTER PUSH tick=80")

	q.Push(10, Handle(300), 0x3333) // Same tick as first
	printBitmapState("AFTER PUSH tick=10 (same as first)")

	// Remove entries
	h, tick, _ := q.PeepMin()
	t.Logf("PeepMin returned: h=%v tick=%d", h, tick)
	q.UnlinkMin(h)
	printBitmapState("AFTER UNLINK FIRST")

	h, tick, _ = q.PeepMin()
	t.Logf("PeepMin returned: h=%v tick=%d", h, tick)
	q.UnlinkMin(h)
	printBitmapState("AFTER UNLINK SECOND")

	h, tick, _ = q.PeepMin()
	t.Logf("PeepMin returned: h=%v tick=%d", h, tick)
	q.UnlinkMin(h)
	printBitmapState("AFTER UNLINK THIRD (SHOULD BE EMPTY)")
}
