// bucketqueue_test.go
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
	if data2 != nil {
		t.Errorf("PopMin data = %v; want nil on single-pop", data2)
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

// TestPushRemovalNextPrevBoth exercises the Push() removal path
// when the removed node sits in the *middle* of a 3‐node list,
// so both n.prev != nilIdx and n.next != nilIdx branches fire.
func TestPushRemovalNextPrevBoth(t *testing.T) {
	q := New()
	h1, _ := q.Borrow()
	h2, _ := q.Borrow()
	h3, _ := q.Borrow()

	// Value for the re–push of the middle node:
	x := 123
	p := unsafe.Pointer(&x)

	// Build a 3-node list at tick=0: head→h3→h2→h1
	if err := q.Push(0, h1, nil); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(0, h2, nil); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(0, h3, nil); err != nil {
		t.Fatal(err)
	}

	// Re-push h2 at tick=1: this must remove h2 from the middle,
	// hitting both q.arena[n.prev].next = n.next and
	// q.arena[n.next].prev = n.prev
	if err := q.Push(1, h2, p); err != nil {
		t.Fatalf("Push removal-next-prev-both: %v", err)
	}

	// After that, size==3 and the queue contains:
	//  • two items at tick=0 (h3, h1) in that order,
	//  • one item at tick=1 (h2).
	// Pop them and verify:
	h, tick, data := q.PopMin()
	if h != h3 || tick != 0 || data != nil {
		t.Errorf("1st PopMin = (%v,%d,%v); want (%v,0,nil)", h, tick, data, h3)
	}
	h, tick, data = q.PopMin()
	if h != h1 || tick != 0 || data != nil {
		t.Errorf("2nd PopMin = (%v,%d,%v); want (%v,0,nil)", h, tick, data, h1)
	}
	h, tick, data = q.PopMin()
	if h != h2 || tick != 1 || data != nil {
		t.Errorf("3rd PopMin = (%v,%d,%v); want (%v,1,nil)", h, tick, data, h2)
	}
}

// TestUpdateRemovalNextPrevBoth exercises the same “remove from middle” logic
// in Update(), covering both q.arena[n.prev].next = n.next and
// q.arena[n.next].prev = n.prev.
func TestUpdateRemovalNextPrevBoth(t *testing.T) {
	q := New()
	h1, _ := q.Borrow()
	h2, _ := q.Borrow()
	h3, _ := q.Borrow()

	// Value for Update()
	x := 456
	p := unsafe.Pointer(&x)

	// Build a 3-node list at tick=2: head→h3→h2→h1
	if err := q.Push(2, h1, nil); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(2, h2, nil); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(2, h3, nil); err != nil {
		t.Fatal(err)
	}

	// Now Update h2 → tick=5. Removal of h2 from the middle
	// must hit both prev and next fixups.
	if err := q.Update(5, h2, p); err != nil {
		t.Fatalf("Update removal-next-prev-both: %v", err)
	}

	// The queue now has two at tick=2 (h3, h1) and one at tick=5 (h2).
	// Pop in order:
	h, tick, data := q.PopMin()
	if h != h3 || tick != 2 || data != nil {
		t.Errorf("1st PopMin = (%v,%d,%v); want (%v,2,nil)", h, tick, data, h3)
	}
	h, tick, data = q.PopMin()
	if h != h1 || tick != 2 || data != nil {
		t.Errorf("2nd PopMin = (%v,%d,%v); want (%v,2,nil)", h, tick, data, h1)
	}
	h, tick, data = q.PopMin()
	if h != h2 || tick != 5 || data != nil {
		t.Errorf("3rd PopMin = (%v,%d,%v); want (%v,5,nil)", h, tick, data, h2)
	}
}

// TestPopMinRemovalHeadPrev covers the PopMin() head-removal branch
// where n.prev == nilIdx but n.next != nilIdx, causing
// q.arena[n.next].prev = nilIdx to run.
func TestPopMinRemovalHeadPrev(t *testing.T) {
	q := New()
	h1, _ := q.Borrow()
	h2, _ := q.Borrow()

	// Push two distinct handles at the same tick so
	// the first PopMin must remove a head with a non-nil next.
	if err := q.Push(10, h1, nil); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(10, h2, nil); err != nil {
		t.Fatal(err)
	}

	// First PopMin should return h2 (the head),
	// and internally clear h1.prev = nilIdx.
	h, tick, data := q.PopMin()
	if h != h2 || tick != 10 || data != nil {
		t.Errorf("1st PopMin = (%v,%d,%v); want (%v,10,nil)", h, tick, data, h2)
	}

	// Now the next head must be h1, with no panics or linkage bugs.
	h, tick, data = q.PopMin()
	if h != h1 || tick != 10 || data != nil {
		t.Errorf("2nd PopMin = (%v,%d,%v); want (%v,10,nil)", h, tick, data, h1)
	}
}
