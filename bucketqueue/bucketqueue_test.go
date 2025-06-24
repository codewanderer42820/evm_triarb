// Package bucketqueue implements a zero-allocation, low-latency time-bucket priority queue.
// Items are distributed across a fixed-size sliding window of time-indexed buckets.
// A two-level bitmap structure allows O(1) retrieval of the earliest item.
//
// This implementation uses a fixed arena allocator, intrusive linked lists,
// and compact handle management for high-throughput applications such as
// schedulers, simulation engines, or event queues.
package bucketqueue

import (
	"testing"
	"unsafe"
)

// TestNewEmpty verifies that a freshly-initialised queue is empty, size==0,
// and PopMin/PeepMin immediately return the invalid handle.
func TestNewEmpty(t *testing.T) {
	q := New()
	if q == nil {
		t.Fatal("New() returned nil")
	}
	if !q.Empty() {
		t.Error("expected new queue to be empty")
	}
	if got := q.Size(); got != 0 {
		t.Errorf("Size() = %d; want 0", got)
	}
	h, tick, data := q.PopMin()
	if h != Handle(nilIdx) || tick != 0 || data != nil {
		t.Errorf("PopMin on empty = (%v, %d, %v); want (invalid,0,nil)", h, tick, data)
	}
}

// TestBorrowExhaustion allocates every available handle and ensures Borrow()
// fails with ErrFull afterwards and returns an invalid handle value.
func TestBorrowExhaustion(t *testing.T) {
	q := New()
	for i := 0; i < capItems; i++ {
		if _, err := q.Borrow(); err != nil {
			t.Fatalf("Borrow #%d returned error %v", i, err)
		}
	}
	h, err := q.Borrow()
	if err != ErrFull {
		t.Errorf("after exhausting Borrow, error = %v; want ErrFull", err)
	}
	if h != Handle(nilIdx) {
		t.Errorf("after exhausting Borrow, handle = %v; want invalid", h)
	}
}

// TestPushErrors covers the three error branches of Push(): invalid handle,
// tick too far in the past, and tick too far in the future.
func TestPushErrors(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	invalid := Handle(nilIdx)

	if err := q.Push(0, invalid, nil); err != ErrItemNotFound {
		t.Errorf("Push with invalid handle: error = %v; want ErrItemNotFound", err)
	}
	if err := q.Push(-1, h, nil); err != ErrPastWindow {
		t.Errorf("Push with tick too far in past: error = %v; want ErrPastWindow", err)
	}
	if err := q.Push(int64(numBuckets), h, nil); err != ErrBeyondWindow {
		t.Errorf("Push with tick too far in future: error = %v; want ErrBeyondWindow", err)
	}
}

// TestUpdateErrors exercises Update() error paths: invalid handle and updating
// a handle that has not yet been pushed into the queue.
func TestUpdateErrors(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	invalid := Handle(nilIdx)

	if err := q.Update(0, invalid, nil); err != ErrItemNotFound {
		t.Errorf("Update with invalid handle: %v; want ErrItemNotFound", err)
	}
	if err := q.Update(0, h, nil); err != ErrItemNotFound {
		t.Errorf("Update on non-existent item: %v; want ErrItemNotFound", err)
	}
}

// TestPushPopBasic pushes a single item then verifies PopMin removes it and
// leaves the queue empty again.
func TestPushPopBasic(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	x := 42
	ptr := unsafe.Pointer(&x)

	if err := q.Push(5, h, ptr); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	if q.Empty() {
		t.Error("queue should not be empty after Push")
	}
	if got := q.Size(); got != 1 {
		t.Errorf("Size after Push = %d; want 1", got)
	}

	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 5 {
		t.Errorf("PopMin = (%v,%d); want (%v,5)", h2, tick2, h)
	}
	if data2 != nil {
		t.Errorf("PopMin data = %v; want nil on single-pop", data2)
	}
	if !q.Empty() || q.Size() != 0 {
		t.Errorf("after PopMin, Empty=%v Size=%d; want true,0", q.Empty(), q.Size())
	}
}

// TestPopMinCountMoreThanOne ensures that when the same handle is pushed twice
// into the same bucket (count>1), the first PopMin returns the latest data
// pointer and decrements the count, while the second PopMin cleans up.
func TestPopMinCountMoreThanOne(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	a, b := 1, 2
	pA := unsafe.Pointer(&a)
	pB := unsafe.Pointer(&b)

	if err := q.Push(3, h, pA); err != nil {
		t.Fatalf("first Push: %v", err)
	}
	if err := q.Push(3, h, pB); err != nil {
		t.Fatalf("second Push: %v", err)
	}
	if got := q.Size(); got != 2 {
		t.Errorf("Size after two pushes = %d; want 2", got)
	}

	// First PopMin should return the latest data pointer
	h1, tick1, data1 := q.PopMin()
	if h1 != h || tick1 != 3 || data1 != pB {
		t.Errorf("first PopMin = (%v,%d,%v); want (%v,3,%v)", h1, tick1, data1, h, pB)
	}
	if got := q.Size(); got != 1 {
		t.Errorf("Size after first PopMin = %d; want 1", got)
	}

	// Second PopMin cleans up, returns nil data
	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 3 || data2 != nil {
		t.Errorf("second PopMin = (%v,%d,%v); want (%v,3,nil)", h2, tick2, data2, h)
	}
	if !q.Empty() {
		t.Error("queue should be empty after second PopMin")
	}
}

// TestPushRemovalPrevNil hits the Push() removal branch where the node being
// re-pushed was previously the head of its bucket list (n.prev == nilIdx).
func TestPushRemovalPrevNil(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	y := 7
	ptr := unsafe.Pointer(&y)

	// initial push at tick=0
	if err := q.Push(0, h, nil); err != nil {
		t.Fatalf("initial Push: %v", err)
	}
	// push again at tick=1 removes head and re-pushes with ptr
	if err := q.Push(1, h, ptr); err != nil {
		t.Fatalf("second Push: %v", err)
	}

	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 1 {
		t.Errorf("PopMin after head-removal = (%v,%d); want (%v,1)", h2, tick2, h)
	}
	if data2 != nil {
		t.Errorf("PopMin data = %v; want nil on single-pop", data2)
	}
}

// TestPushRemovalPrevNonNil re‑pushes a node that sits in the *middle* of a
// bucket list, exercising both prev/next link fix‑ups.
func TestPushRemovalPrevNonNil(t *testing.T) {
	q := New()
	h1, _ := q.Borrow()
	h2, _ := q.Borrow()
	ptr := unsafe.Pointer(new(int))

	// push two different handles into the same bucket (tick=0)
	if err := q.Push(0, h1, nil); err != nil {
		t.Fatalf("Push h1: %v", err)
	}
	if err := q.Push(0, h2, nil); err != nil {
		t.Fatalf("Push h2: %v", err)
	}

	// re-push h1 at tick=1 should remove it from mid-list
	if err := q.Push(1, h1, ptr); err != nil {
		t.Fatalf("re-push h1: %v", err)
	}

	// first PopMin → h2@0
	hA, tickA, dataA := q.PopMin()
	if hA != h2 || tickA != 0 || dataA != nil {
		t.Errorf("first PopMin = (%v,%d,%v); want (%v,0,nil)", hA, tickA, dataA, h2)
	}
	// second PopMin → h1@1
	hB, tickB, dataB := q.PopMin()
	if hB != h1 || tickB != 1 {
		t.Errorf("second PopMin = (%v,%d); want (%v,1)", hB, tickB, h1)
	}
	if dataB != nil {
		t.Errorf("second PopMin data = %v; want nil on single-pop", dataB)
	}
}

// TestUpdateValid updates a pushed handle to a new tick and verifies the move.
func TestUpdateValid(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	a, b := 100, 200
	p1 := unsafe.Pointer(&a)
	p2 := unsafe.Pointer(&b)

	if err := q.Push(2, h, p1); err != nil {
		t.Fatalf("Push before Update: %v", err)
	}
	if err := q.Update(8, h, p2); err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 8 {
		t.Errorf("PopMin after Update = (%v,%d); want (%v,8)", h2, tick2, h)
	}
	if data2 != nil {
		t.Errorf("PopMin data = %v; want nil on single-pop", data2)
	}
	if !q.Empty() {
		t.Error("queue should be empty after final PopMin")
	}
}
