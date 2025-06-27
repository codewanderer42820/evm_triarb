// bucketqueue_test.go validates correctness of the Queue implementation
// under various functional scenarios: creation, borrow exhaustion, push/pop
// semantics, multi-count, list integrity, and update logic.
//
// Each test is isolated and uses minimal allocations to validate
// arena handle lifecycle, mutation correctness, and tick-based ordering.
package bucketqueue

import (
	"testing"
	"unsafe"
)

// TestNewEmpty ensures a freshly initialized queue behaves correctly.
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

// TestBorrowExhaustion fills the arena and expects the next Borrow to fail.
func TestBorrowExhaustion(t *testing.T) {
	q := New()
	for i := 0; i < capItems-1; i++ {
		if _, err := q.Borrow(); err != nil {
			t.Fatalf("Borrow #%d returned error %v", i, err)
		}
	}
	h, err := q.Borrow()
	if err != ErrFull {
		t.Errorf("expected ErrFull, got %v", err)
	}
	if h != Handle(nilIdx) {
		t.Errorf("expected nilIdx handle, got %v", h)
	}
}

// TestPushErrors validates boundary conditions for Push().
func TestPushErrors(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	invalid := Handle(nilIdx)

	if err := q.Push(0, invalid, nil); err != ErrItemNotFound {
		t.Errorf("invalid handle: got %v, want ErrItemNotFound", err)
	}
	if err := q.Push(-1, h, nil); err != ErrPastWindow {
		t.Errorf("past tick: got %v, want ErrPastWindow", err)
	}
	if err := q.Push(int64(numBuckets), h, nil); err != ErrBeyondWindow {
		t.Errorf("future tick: got %v, want ErrBeyondWindow", err)
	}
}

// TestUpdateErrors ensures Update() handles invalid cases correctly.
func TestUpdateErrors(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	invalid := Handle(nilIdx)

	if err := q.Update(0, invalid, nil); err != ErrItemNotFound {
		t.Errorf("invalid handle update: %v", err)
	}
	if err := q.Update(0, h, nil); err != ErrItemNotFound {
		t.Errorf("update on non-existent item: %v", err)
	}
}

// TestPushPopBasic confirms the happy path of pushing and popping an item.
func TestPushPopBasic(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	x := 42
	ptr := unsafe.Pointer(&x)

	if err := q.Push(5, h, ptr); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	if q.Empty() || q.Size() != 1 {
		t.Errorf("unexpected queue state after push")
	}

	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 5 || data2 != ptr {
		t.Errorf("PopMin = (%v,%d,%v); want (%v,5,%v)", h2, tick2, data2, h, ptr)
	}
}

// TestPopMinCountMoreThanOne ensures count is decremented and not fully removed.
func TestPopMinCountMoreThanOne(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	a, b := 1, 2
	pA := unsafe.Pointer(&a)
	pB := unsafe.Pointer(&b)

	_ = q.Push(3, h, pA)
	_ = q.Push(3, h, pB) // overwrite, increment count

	if q.Size() != 2 {
		t.Errorf("unexpected size after double push")
	}

	h1, tick1, data1 := q.PopMin()
	if h1 != h || tick1 != 3 || data1 != pB {
		t.Errorf("first PopMin: got (%v,%d,%v), want (%v,3,%v)", h1, tick1, data1, h, pB)
	}
	if q.Size() != 1 {
		t.Errorf("expected size 1 after first PopMin")
	}

	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 3 || data2 != pB {
		t.Errorf("second PopMin: got (%v,%d,%v), want (%v,3,%v)", h2, tick2, data2, h, pB)
	}
	if !q.Empty() {
		t.Error("expected queue to be empty after two pops")
	}
}

// TestPushRemovalPrevNil verifies bucket head removal updates prev/next.
func TestPushRemovalPrevNil(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	y := 7
	ptr := unsafe.Pointer(&y)
	_ = q.Push(0, h, nil)
	_ = q.Push(1, h, ptr) // move to new tick

	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 1 || data2 != ptr {
		t.Errorf("unexpected PopMin after tick change")
	}
}

// TestPushRemovalPrevNonNil validates interior node unlinking.
func TestPushRemovalPrevNonNil(t *testing.T) {
	q := New()
	h1, _ := q.Borrow()
	h2, _ := q.Borrow()
	ptr := unsafe.Pointer(new(int))

	_ = q.Push(0, h1, nil)
	_ = q.Push(0, h2, nil)
	_ = q.Push(1, h1, ptr)

	hA, tickA, _ := q.PopMin()
	if hA != h2 || tickA != 0 {
		t.Errorf("expected h2@0, got %v@%d", hA, tickA)
	}
	hB, tickB, dataB := q.PopMin()
	if hB != h1 || tickB != 1 || dataB != ptr {
		t.Errorf("expected h1@1 %v, got %v@%d %v", ptr, hB, tickB, dataB)
	}
}

// TestUpdateValid confirms update requeues to new tick.
func TestUpdateValid(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	a, b := 100, 200
	p1 := unsafe.Pointer(&a)
	p2 := unsafe.Pointer(&b)

	_ = q.Push(2, h, p1)
	_ = q.Update(8, h, p2)

	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 8 || data2 != p2 {
		t.Errorf("Update failed: got (%v,%d,%v), want (%v,8,%v)", h2, tick2, data2, h, p2)
	}
}

// TestPushMiddleNodeCorrectsNextPrev checks prev/next patching after middle node removed.
func TestPushMiddleNodeCorrectsNextPrev(t *testing.T) {
	q := New()
	hA, _ := q.Borrow()
	hB, _ := q.Borrow()
	hC, _ := q.Borrow()

	_ = q.Push(0, hA, nil)
	_ = q.Push(0, hB, nil)
	_ = q.Push(0, hC, nil) // hC is head, hB middle
	_ = q.Push(1, hB, nil)

	nC := &q.arena[hC]
	nA := &q.arena[hA]

	if nC.next != idx32(hA) {
		t.Errorf("hC.next expected %d, got %d", hA, nC.next)
	}
	if nA.prev != idx32(hC) {
		t.Errorf("hA.prev expected %d, got %d", hC, nA.prev)
	}
}
