// Package quantumqueue contains unit tests for core QuantumQueue operations,
// ensuring basic behaviors like emptiness, sizing, and handle borrowing operate correctly.
package quantumqueue

import (
	"testing"
)

// TestNewQueueEmptyAndSize verifies that a newly initialized queue
// reports empty and has size zero. Any deviation indicates an incorrect initialization.
func TestNewQueueEmptyAndSize(t *testing.T) {
	t.Parallel() // allow parallel execution with other tests for faster CI runs
	q := NewQuantumQueue()

	// Expect the queue to be empty immediately after creation
	if !q.Empty() {
		t.Error("Expected new queue to be empty, but Empty() returned false")
	}

	// Expect the size to be exactly zero
	if got := q.Size(); got != 0 {
		t.Errorf("Size() = %d; want 0", got)
	}
}

// TestBorrowSafeExhaustion confirms that BorrowSafe allocates handles until capacity,
// and then returns an error when no handles remain.
func TestBorrowSafeExhaustion(t *testing.T) {
	t.Parallel()
	q := NewQuantumQueue()

	// Continuously borrow until reaching CapItems, expecting no error
	for i := 0; i < CapItems; i++ {
		h, err := q.BorrowSafe()
		if err != nil {
			t.Fatalf("Unexpected error on BorrowSafe() at iteration %d: %v", i, err)
		}
		// BorrowSafe should never return nilIdx until exhausted
		if h == nilIdx {
			t.Fatalf("BorrowSafe returned nilIdx at iteration %d; capacity should not be exhausted yet", i)
		}
	}

	// One more borrow should fail due to exhaustion of handles
	if _, err := q.BorrowSafe(); err == nil {
		t.Error("Expected error after exhausting all handles, but got nil error")
	}
}

// TestBorrowResetsNode ensures that Borrow resets the internal node state:
// tick to -1, prev and next to nilIdx; and that handles are issued in sequence.
func TestBorrowResetsNode(t *testing.T) {
	t.Parallel()
	q := NewQuantumQueue()

	// Borrow first handle and inspect its node
	h1, err := q.Borrow()
	if err != nil {
		t.Fatalf("Unexpected error on Borrow(): %v", err)
	}

	n1 := &q.arena[h1] // direct node reference
	// After Borrow, tick must be reset to -1
	if n1.tick != -1 {
		t.Errorf("Node.tick = %d; want -1 after Borrow", n1.tick)
	}
	// prev and next pointers must both be nilIdx sentinel
	if n1.prev != nilIdx || n1.next != nilIdx {
		t.Errorf("Node.prev/next = %v/%v; want both nilIdx", n1.prev, n1.next)
	}

	// Borrowing again should yield the next handle in sequence
	h2, _ := q.Borrow()
	if h2 != h1+1 {
		t.Errorf("Second borrow handle = %v; want %v", h2, h1+1)
	}
}
