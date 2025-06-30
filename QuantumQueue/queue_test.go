// Package quantumqueue contains correctness tests for the QuantumQueue
// implementation. It validates arena reuse, pointer rewiring, bitmap updates,
// and lifecycle safety under footgun-mode conditions.
package quantumqueue

import "testing"

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

// arr48 is a helper to convert a byte slice into a fixed-size [48]byte pointer.
// Used to construct inline payloads without heap allocations.
func arr48(b []byte) *[48]byte {
	var a [48]byte
	copy(a[:], b)
	return &a
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

// TestNewQueueBehavior verifies a new queue is empty, Size=0, and Borrow resets nodes.
func TestNewQueueBehavior(t *testing.T) {
	q := NewQuantumQueue()
	if !q.Empty() {
		t.Error("new queue should be empty")
	}
	if got := q.Size(); got != 0 {
		t.Errorf("Size of new queue = %d; want 0", got)
	}

	h1, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow returned unexpected error: %v", err)
	}
	n1 := &q.arena[h1]
	if n1.tick != -1 {
		t.Errorf("Borrow did not reset tick; got %d; want -1", n1.tick)
	}
	if n1.prev != nilIdx || n1.next != nilIdx {
		t.Errorf("Borrow did not reset pointers; prev=%v next=%v; want both nilIdx", n1.prev, n1.next)
	}

	h2, err2 := q.Borrow()
	if err2 != nil {
		t.Fatalf("second Borrow returned unexpected error: %v", err2)
	}
	if h2 != h1+1 {
		t.Errorf("Borrow order wrong; second handle = %v; want %v", h2, h1+1)
	}
}

// TestBorrowSafeExhaustion checks BorrowSafe fails after exhausting all handles.
func TestBorrowSafeExhaustion(t *testing.T) {
	q := NewQuantumQueue()
	for i := 0; i < CapItems; i++ {
		if _, err := q.BorrowSafe(); err != nil {
			t.Fatalf("unexpected error at borrow #%d: %v", i, err)
		}
	}
	if _, err := q.BorrowSafe(); err == nil {
		t.Error("expected error after exhausting handles, got nil")
	}
}

// TestPushAndPeepMin tests single and multiple Push/PeepMin cycles and edge ticks.
func TestPushAndPeepMin(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(10, h, arr48([]byte("foo")))
	if q.Empty() {
		t.Error("queue should not be empty after push")
	}
	if size := q.Size(); size != 1 {
		t.Errorf("Size = %d; want 1", size)
	}
	hGot, tickGot, data := q.PeepMin()
	if hGot != h || tickGot != 10 {
		t.Errorf("PeepMin = (%v, %d); want (%v, 10)", hGot, tickGot, h)
	}
	if string(data[:3]) != "foo" {
		t.Errorf("PeepMin data = %q; want \"foo\"", data[:3])
	}

	// update path: same tick should not change size
	q.Push(10, h, arr48([]byte("bar")))
	if size := q.Size(); size != 1 {
		t.Errorf("Size after update = %d; want still 1", size)
	}
	hGot2, _, data2 := q.PeepMin()
	if hGot2 != h || string(data2[:3]) != "bar" {
		t.Errorf("update path failed: got data %q; want \"bar\"", data2[:3])
	}

	// unlink and ensure empty
	q.UnlinkMin(h, 10)
	if !q.Empty() || q.Size() != 0 {
		t.Errorf("queue not empty after UnlinkMin: Empty=%v Size=%d", q.Empty(), q.Size())
	}

	// edge ticks: 0 and max
	q2 := NewQuantumQueue()
	h0, _ := q2.BorrowSafe()
	hMax, _ := q2.BorrowSafe()
	q2.Push(0, h0, arr48([]byte("low")))
	q2.Push(int64(CapItems-1), hMax, arr48([]byte("hi")))
	hMin, tickMin, _ := q2.PeepMin()
	if hMin != h0 || tickMin != 0 {
		t.Errorf("edge low tick: got (%v, %d); want (%v, 0)", hMin, tickMin, h0)
	}
	q2.UnlinkMin(h0, 0)
	hHigh, tickHigh, _ := q2.PeepMin()
	if hHigh != hMax || tickHigh != int64(CapItems-1) {
		t.Errorf("edge high tick: got (%v, %d); want (%v, %d)", hHigh, tickHigh, hMax, CapItems-1)
	}
	q2.UnlinkMin(hMax, tickHigh)
	if !q2.Empty() {
		t.Error("queue should be empty after removing both edges")
	}
}

// TestMultipleSameTickOrdering ensures newest item at same tick is returned first.
func TestMultipleSameTickOrdering(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	q.Push(5, h1, arr48([]byte("a1")))
	q.Push(5, h2, arr48([]byte("a2")))
	hMin, _, data := q.PeepMin()
	if hMin != h2 {
		t.Errorf("same-tick order: got %v; want %v", hMin, h2)
	}
	if string(data[:2]) != "a2" {
		t.Errorf("data ordering: got %q; want \"a2\"", data[:2])
	}
}

// TestPushDifferentTicks verifies ordering when ticks differ.
func TestPushDifferentTicks(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	q.Push(100, h1, arr48([]byte("one")))
	q.Push(50, h2, arr48([]byte("two")))
	hMin, tickMin, _ := q.PeepMin()
	if hMin != h2 || tickMin != 50 {
		t.Errorf("different ticks order: got (%v, %d); want (%v, 50)", hMin, tickMin, h2)
	}
}

// TestMoveTickBehavior covers no-op and relocation for MoveTick.
func TestMoveTickBehavior(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(20, h, arr48([]byte("x")))
	q.MoveTick(h, 20) // no-op
	if size := q.Size(); size != 1 {
		t.Errorf("size after no-op MoveTick = %d; want 1", size)
	}
	q.MoveTick(h, 30)
	hNew, tickNew, _ := q.PeepMin()
	if hNew != h || tickNew != 30 {
		t.Errorf("MoveTick failed: got (%v, %d); want (%v, 30)", hNew, tickNew, h)
	}
}

// TestUnlinkMinNonHead ensures UnlinkMin can remove any handle by handle/tick.
func TestUnlinkMinNonHead(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()
	q.Push(1, h1, arr48([]byte("h1")))
	q.Push(2, h2, arr48([]byte("h2")))
	q.Push(3, h3, arr48([]byte("h3")))
	q.UnlinkMin(h2, 2)
	hMin, tickMin, _ := q.PeepMin()
	if hMin != h1 || tickMin != 1 {
		t.Errorf("remove non-head: got (%v, %d); want (%v, 1)", hMin, tickMin, h1)
	}
}

// TestMixedOperations runs combined Push, PeepMin, UnlinkMin cycles end-to-end.
func TestMixedOperations(t *testing.T) {
	q := NewQuantumQueue()
	handles := make([]Handle, 3)
	for i := 0; i < 3; i++ {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, arr48([]byte{byte(i)}))
	}
	for i := 0; i < 3; i++ {
		hGot, tick, _ := q.PeepMin()
		if tick != int64(i) {
			t.Errorf("mixed pop order: expected tick %d, got %d", i, tick)
		}
		q.UnlinkMin(hGot, tick)
	}
	if !q.Empty() {
		t.Error("queue should be empty after mixed pop")
	}
}

// TestUnlinkMiddleNode triggers unlink and relink branches when relocating a middle node.
func TestUnlinkMiddleNode(t *testing.T) {
	q := NewQuantumQueue()
	// Borrow three handles
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()
	// Push all to tick 7: list becomes h3->h2->h1
	q.Push(7, h1, arr48([]byte("one")))
	q.Push(7, h2, arr48([]byte("two")))
	q.Push(7, h3, arr48([]byte("thr")))
	// Relocate h2 to tick 8 to trigger unlink(h2) and linkAtHead(h2,8)
	q.Push(8, h2, arr48([]byte("TWO")))
	// head (min tick) should still be h3 at tick 7
	hGot, tickGot, data := q.PeepMin()
	if hGot != h3 || tickGot != 7 || string(data[:3]) != "thr" {
		t.Errorf("After relocate, head = (%v, %d, %q); want (%v, 7, \"thr\")", hGot, tickGot, data[:3], h3)
	}
	// Unlink head at tick7 (h3), next head should be h1
	q.UnlinkMin(h3, 7)
	hNext, tickNext, data1 := q.PeepMin()
	if hNext != h1 || tickNext != 7 || string(data1[:3]) != "one" {
		t.Errorf("After removing h3, head = (%v, %d, %q); want (%v, 7, \"one\")", hNext, tickNext, data1[:3], h1)
	}
	// Unlink remaining tick7 node h1; now head should be relocated h2 at tick8
	q.UnlinkMin(h1, 7)
	hLast, tickLast, data2 := q.PeepMin()
	if hLast != h2 || tickLast != 8 || string(data2[:3]) != "TWO" {
		t.Errorf("After removing h1, head = (%v, %d, %q); want (%v, 8, \"TWO\")", hLast, tickLast, data2[:3], h2)
	}
}
