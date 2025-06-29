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

// TestPushSameTickCopiesData verifies that pushing on the same tick
// updates the payload in place without changing queue membership or size.
func TestPushSameTickCopiesData(t *testing.T) {
	q := NewQuantumQueue()
	h, err := q.BorrowSafe()
	if err != nil {
		t.Fatalf("BorrowSafe error: %v", err)
	}

	// Initial push
	val1 := []byte("firstValue")
	q.Push(123, h, val1)
	// Confirm data set
	if got := string(q.arena[h].data[:len(val1)]); got != "firstValue" {
		t.Errorf("Initial data = %q; want %q", got, "firstValue")
	}
	sizeBefore := q.Size()

	// Push again on same tick with new data
	val2 := []byte("secondValue")
	q.Push(123, h, val2)

	// Size must not change
	if got := q.Size(); got != sizeBefore {
		t.Errorf("Size after same-tick Push = %d; want %d", got, sizeBefore)
	}

	// Data must have been updated in place
	if got := string(q.arena[h].data[:len(val2)]); got != "secondValue" {
		t.Errorf("Data after same-tick Push = %q; want %q", got, "secondValue")
	}
}

// TestPushExistingHandleUnlink ensures that pushing an existing handle to a different tick
// removes it from its previous bucket and links it at the new tick.
func TestPushExistingHandleUnlink(t *testing.T) {
	q := NewQuantumQueue()
	h, err := q.BorrowSafe()
	if err != nil {
		t.Fatalf("BorrowSafe error: %v", err)
	}

	// First push to tickA
	tickA := int64(100)
	q.Push(tickA, h, nil)
	bucketA := idx32(uint64(tickA))
	if q.buckets[bucketA] != h {
		t.Fatalf("Bucket head for tickA = %v; want %v", q.buckets[bucketA], h)
	}

	// Push again to a different tickB
	tickB := int64(200)
	q.Push(tickB, h, nil)
	bucketB := idx32(uint64(tickB))

	// Old bucket must be emptied
	if q.buckets[bucketA] != nilIdx {
		t.Errorf("Old bucket not emptied; got %v; want nilIdx", q.buckets[bucketA])
	}

	// New bucket must head with h
	if q.buckets[bucketB] != h {
		t.Errorf("New bucket head = %v; want %v", q.buckets[bucketB], h)
	}

	// Queue size should remain 1
	if got := q.Size(); got != 1 {
		t.Errorf("Size after reassign Push = %d; want 1", got)
	}
}
