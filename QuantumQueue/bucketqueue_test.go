package quantumqueue

import (
	"testing"
	"unsafe"
)

func TestNewQuantumQueueEmpty(t *testing.T) {
	q := NewQuantumQueue()
	if !q.Empty() {
		t.Error("expected new queue to be empty")
	}
	if got := q.Size(); got != 0 {
		t.Errorf("Size() = %d; want 0", got)
	}
	h, tick, data := q.PopMin()
	if h != Handle(nilIdx) || tick != 0 || data != nil {
		t.Errorf("PopMin empty = (%v,%d,%v); want (invalid,0,nil)", h, tick, data)
	}
}

func TestBorrowExhaustion(t *testing.T) {
	q := NewQuantumQueue()
	for i := 0; i < CapItems-1; i++ {
		if _, err := q.Borrow(); err != nil {
			t.Fatalf("Borrow #%d error: %v", i, err)
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

func TestInvalidHandlePush(t *testing.T) {
	q := NewQuantumQueue()
	if err := q.Push(0, Handle(nilIdx), nil); err != ErrNotFound {
		t.Errorf("Push invalid handle: got %v; want ErrNotFound", err)
	}
}

func TestPushErrors(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.Borrow()
	if err := q.Push(-1, h, nil); err != ErrPastWindow {
		t.Errorf("push past: got %v; want ErrPastWindow", err)
	}
	if err := q.Push(int64(BucketCount), h, nil); err != ErrBeyondWindow {
		t.Errorf("push beyond: got %v; want ErrBeyondWindow", err)
	}
}

func TestPushPopBasic(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.Borrow()
	x := 123
	ptr := unsafe.Pointer(&x)
	if err := q.Push(5, h, ptr); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	if q.Empty() || q.Size() != 1 {
		t.Error("unexpected state after push")
	}
	h2, tick2, data2 := q.PopMin()
	if h2 != h || tick2 != 5 || data2 != ptr {
		t.Errorf("PopMin = (%v,%d,%v); want (%v,5,%v)", h2, tick2, data2, h, ptr)
	}
}

func TestPeepMin(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.Borrow()
	h2, _ := q.Borrow()
	_ = q.Push(10, h1, nil)
	_ = q.Push(3, h2, nil)
	ph, pt, _ := q.PeepMin()
	if ph != h2 || pt != 3 {
		t.Errorf("PeepMin = (%v,%d); want (%v,3)", ph, pt, h2)
	}
	if q.Size() != 2 {
		t.Errorf("size mutated after PeepMin; got %d; want 2", q.Size())
	}
}

func TestDuplicateCount(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.Borrow()
	p1 := unsafe.Pointer(new(int))
	p2 := unsafe.Pointer(new(int))
	_ = q.Push(7, h, p1)
	_ = q.Push(7, h, p2)
	if q.Size() != 2 {
		t.Errorf("expected size=2 after two pushes; got %d", q.Size())
	}
	h1, t1, d1 := q.PopMin()
	if h1 != h || t1 != 7 || d1 != p2 {
		t.Errorf("first PopMin = (%v,%d,%v); want (%v,7,%v)", h1, t1, d1, h, p2)
	}
	h2, t2, d2 := q.PopMin()
	if h2 != h || t2 != 7 || d2 != p2 {
		t.Errorf("second PopMin = (%v,%d,%v); want (%v,7,%v)", h2, t2, d2, h, p2)
	}
	if !q.Empty() {
		t.Error("expected empty after duplicate pops")
	}
}
