package bucketqueue

import (
	"testing"
	"unsafe"
)

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

func TestBorrowExhaustion(t *testing.T) {
	q := New()
	for i := 0; i < capItems-1; i++ { // Handle(0) is skipped
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
	if data2 != ptr {
		t.Errorf("PopMin data = %v; want %v", data2, ptr)
	}
	if !q.Empty() || q.Size() != 0 {
		t.Errorf("after PopMin, Empty=%v Size=%d; want true,0", q.Empty(), q.Size())
	}
}

func TestPopMinCountMoreThanOne(t *testing.T) {
	q := New()
	h, _ := q.Borrow()

	a, b := 1, 2
	pA := unsafe.Pointer(&a)
	pB := unsafe.Pointer(&b)

	// First push at tick=3 with data = pA
	if err := q.Push(3, h, pA); err != nil {
		t.Fatalf("first Push: %v", err)
	}

	// Second push at same tick=3 with data = pB (should overwrite)
	if err := q.Push(3, h, pB); err != nil {
		t.Fatalf("second Push: %v", err)
	}

	if got := q.Size(); got != 2 {
		t.Errorf("Size after two pushes = %d; want 2", got)
	}

	// First PopMin should return pB (latest pushed data)
	h1, tick1, data1 := q.PopMin()
	if h1 != h || tick1 != 3 || data1 != pB {
		t.Errorf("first PopMin = (%v,%d,%v); want (%v,3,%v)", h1, tick1, data1, h, pB)
	}

	if got := q.Size(); got != 1 {
		t.Errorf("Size after first PopMin = %d; want 1", got)
	}

	// Second PopMin should still return pB (data field is not updated per-count)
	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 3 || data2 != pB {
		t.Errorf("second PopMin = (%v,%d,%v); want (%v,3,%v)", h2, tick2, data2, h, pB)
	}

	if !q.Empty() {
		t.Error("queue should be empty after second PopMin")
	}
}

func TestPushRemovalPrevNil(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	y := 7
	ptr := unsafe.Pointer(&y)

	if err := q.Push(0, h, nil); err != nil {
		t.Fatalf("initial Push: %v", err)
	}
	if err := q.Push(1, h, ptr); err != nil {
		t.Fatalf("second Push: %v", err)
	}

	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 1 {
		t.Errorf("PopMin after head-removal = (%v,%d); want (%v,1)", h2, tick2, h)
	}
	if data2 != ptr {
		t.Errorf("PopMin data = %v; want %v", data2, ptr)
	}
}

func TestPushRemovalPrevNonNil(t *testing.T) {
	q := New()
	h1, _ := q.Borrow()
	h2, _ := q.Borrow()
	ptr := unsafe.Pointer(new(int))

	if err := q.Push(0, h1, nil); err != nil {
		t.Fatalf("Push h1: %v", err)
	}
	if err := q.Push(0, h2, nil); err != nil {
		t.Fatalf("Push h2: %v", err)
	}
	if err := q.Push(1, h1, ptr); err != nil {
		t.Fatalf("re-push h1: %v", err)
	}

	hA, tickA, dataA := q.PopMin()
	if hA != h2 || tickA != 0 || dataA != nil {
		t.Errorf("first PopMin = (%v,%d,%v); want (%v,0,nil)", hA, tickA, dataA, h2)
	}
	hB, tickB, dataB := q.PopMin()
	if hB != h1 || tickB != 1 {
		t.Errorf("second PopMin = (%v,%d); want (%v,1)", hB, tickB, h1)
	}
	if dataB != ptr {
		t.Errorf("second PopMin data = %v; want %v", dataB, ptr)
	}
}

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
	if data2 != p2 {
		t.Errorf("PopMin data = %v; want %v", data2, p2)
	}
	if !q.Empty() {
		t.Error("queue should be empty after final PopMin")
	}
}

func TestPushMiddleNodeCorrectsNextPrev(t *testing.T) {
	q := New()

	// Allocate 3 handles: hA, hB, hC
	hA, _ := q.Borrow()
	hB, _ := q.Borrow()
	hC, _ := q.Borrow()

	// Push all 3 to the same tick=0 to form a chain: C -> B -> A (insertion order is reverse)
	q.Push(0, hA, nil)
	q.Push(0, hB, nil)
	q.Push(0, hC, nil)

	// Internally, the list should be: hC (head) -> hB -> hA

	// Re-push hB to tick=1, which should remove it from the chain
	q.Push(1, hB, nil)

	// Now the list at tick=0 should be: hC -> hA
	// Validate that hC.next == hA and hA.prev == hC

	nC := &q.arena[hC]
	nA := &q.arena[hA]

	if nC.next != idx32(hA) {
		t.Errorf("expected hC.next = %d; got %d", hA, nC.next)
	}
	if nA.prev != idx32(hC) {
		t.Errorf("expected hA.prev = %d; got %d", hC, nA.prev)
	}
}
