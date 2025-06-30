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
// Basic Sanity Tests
// -----------------------------------------------------------------------------

// TestNewQueueEmptyAndSize verifies that a new queue starts empty and with size 0.
func TestNewQueueEmptyAndSize(t *testing.T) {
	q := NewQuantumQueue()
	if !q.Empty() {
		t.Error("New queue should be empty")
	}
	if got := q.Size(); got != 0 {
		t.Errorf("Size of new queue = %d; want 0", got)
	}
}

// TestBorrowSafeExhaustion checks that BorrowSafe fails cleanly after arena exhaustion.
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

// TestBorrowResetsNode ensures Borrow resets internal tick and pointer state.
func TestBorrowResetsNode(t *testing.T) {
	q := NewQuantumQueue()
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
	h2, _ := q.Borrow()
	if h2 != h1+1 {
		t.Errorf("Borrow order wrong; second handle = %v; want %v", h2, h1+1)
	}
}

// -----------------------------------------------------------------------------
// Functional Lifecycle Tests
// -----------------------------------------------------------------------------

// TestBasicPushPeepMinMoveTickUnlink exercises Push, PeepMin, MoveTick, and UnlinkMin together.
// Validates correct ordering, updates, and removal logic.
func TestBasicPushPeepMinMoveTickUnlink(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()

	q.Push(10, h1, arr48([]byte("foo")))
	if q.Empty() {
		t.Error("queue should not be empty after first push")
	}
	if got := q.Size(); got != 1 {
		t.Errorf("Size after one push = %d; want 1", got)
	}
	h, tick, data := q.PeepMin()
	if h != h1 || tick != 10 {
		t.Errorf("PeepMin = (%v, %d); want (%v, 10)", h, tick, h1)
	}
	if string(data[:3]) != "foo" {
		t.Errorf("PeepMin data = %q; want 'foo'", data[:3])
	}

	q.Push(5, h2, arr48([]byte("bar")))
	if got := q.Size(); got != 2 {
		t.Errorf("Size after two pushes = %d; want 2", got)
	}
	h, tick, data = q.PeepMin()
	if h != h2 || tick != 5 {
		t.Errorf("PeepMin = (%v, %d); want (%v, 5)", h, tick, h2)
	}
	if string(data[:3]) != "bar" {
		t.Errorf("PeepMin data = %q; want 'bar'", data[:3])
	}

	q.MoveTick(h2, 20)
	if got := q.Size(); got != 2 {
		t.Errorf("Size after MoveTick = %d; want 2", got)
	}
	h, tick, _ = q.PeepMin()
	if h != h1 || tick != 10 {
		t.Errorf("PeepMin after MoveTick = (%v, %d); want (%v, 10)", h, tick, h1)
	}

	q.UnlinkMin(h1, 10)
	if got := q.Size(); got != 1 {
		t.Errorf("Size after UnlinkMin = %d; want 1", got)
	}
	h, tick, _ = q.PeepMin()
	if h != h2 || tick != 20 {
		t.Errorf("PeepMin after UnlinkMin = (%v, %d); want (%v, 20)", h, tick, h2)
	}

	q.UnlinkMin(h2, 20)
	if !q.Empty() || q.Size() != 0 {
		t.Errorf("Queue not empty after removing all entries: Empty=%v, Size=%d", q.Empty(), q.Size())
	}
}
