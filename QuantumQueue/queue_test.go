package quantumqueue

import (
	"testing"
	"unsafe"
)

/*──────── helpers ───────*/

func payload(i int) unsafe.Pointer { return unsafe.Pointer(&[]int{i}[0]) }

func mustBorrow(t *testing.T, q *QuantumQueue) Handle {
	h, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	return h
}

/*──────── construction / borrow ───────*/

func TestBorrowAndErrFull(t *testing.T) {
	q := NewQuantumQueue()

	var last Handle
	for i := 1; i < CapItems; i++ { // handle 0 is reserved
		h, err := q.Borrow()
		if err != nil {
			t.Fatalf("Borrow #%d errored: %v", i, err)
		}
		last = h
	}
	if h, err := q.Borrow(); err != ErrFull || h != Handle(nilIdx) {
		t.Fatalf("want ErrFull, got (%v,%v)", h, err)
	}

	if err := q.Push(42, last, payload(0)); err != nil {
		t.Fatal(err)
	}
	_, _, _ = q.PopMin()
	if _, err := q.Borrow(); err != nil {
		t.Fatalf("Borrow after recycle errored: %v", err)
	}
}

/*──────── push / pop basics ───────*/

func TestPushPopBasic(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)

	const tick = 10
	wantPtr := payload(123)

	if err := q.Push(tick, h, wantPtr); err != nil {
		t.Fatal(err)
	}
	if q.Empty() || q.Size() != 1 {
		t.Fatalf("queue state wrong after push")
	}

	ph, pt, pv := q.PeepMin()
	if ph != h || pt != tick || pv != wantPtr {
		t.Fatalf("PeepMin got (%v,%d,%p)", ph, pt, pv)
	}
	popH, popT, popV := q.PopMin()
	if popH != h || popT != tick || popV != wantPtr {
		t.Fatalf("PopMin got (%v,%d,%p)", popH, popT, popV)
	}
	if !q.Empty() || q.Size() != 0 {
		t.Fatalf("queue not empty after pop")
	}
}

/*──────── duplicate-tick path ───────*/

func TestDuplicateTickHandling(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)

	if err := q.Push(5, h, payload(1)); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(5, h, payload(2)); err != nil {
		t.Fatal(err)
	} // dup
	if q.Size() != 2 {
		t.Fatalf("size want 2 got %d", q.Size())
	}

	_, _, _ = q.PopMin()
	if q.Size() != 1 {
		t.Fatalf("size after dup pop want 1 got %d", q.Size())
	}

	_, _, _ = q.PopMin()
	if !q.Empty() {
		t.Fatalf("queue should be empty")
	}
}

/*──────── update paths ───────*/

func TestUpdatePaths(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)

	/* same-tick payload patch */
	ptr99 := payload(99)
	if err := q.Push(20, h, payload(0)); err != nil {
		t.Fatal(err)
	}
	if err := q.Update(20, h, ptr99); err != nil {
		t.Fatalf("Update same tick: %v", err)
	}

	if _, tick, ptr := q.PeepMin(); tick != 20 || ptr != ptr99 {
		t.Fatalf("Update same tick failed")
	}

	/* move to later tick */
	ptr42 := payload(42)
	if err := q.Update(25, h, ptr42); err != nil {
		t.Fatalf("Update move: %v", err)
	}
	ph, pt, pv := q.PeepMin()
	if ph != h || pt != 25 || pv != ptr42 {
		t.Fatalf("Update move got (%v,%d,%p)", ph, pt, pv)
	}
}

/*──────── sliding-window errors ───────*/

func TestWindowErrors(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)

	if err := q.Push(-1, h, payload(0)); err != ErrPastWindow {
		t.Fatalf("expect ErrPastWindow, got %v", err)
	}
	if err := q.Push(int64(BucketCount)+1, h, payload(0)); err != ErrBeyondWindow {
		t.Fatalf("expect ErrBeyondWindow, got %v", err)
	}
}

/*──────── invalid handles ───────*/

func TestInvalidHandle(t *testing.T) {
	q := NewQuantumQueue()

	if err := q.Push(1, Handle(nilIdx), payload(0)); err != ErrNotFound {
		t.Fatalf("want ErrNotFound for nilIdx, got %v", err)
	}
	h := Handle(idx32(CapItems + 10))
	if err := q.Push(1, h, payload(0)); err != ErrNotFound {
		t.Fatalf("want ErrNotFound for out-of-range, got %v", err)
	}
}

/*──────── pop empty ───────*/

func TestPopEmptyQueue(t *testing.T) {
	q := NewQuantumQueue()
	h, tick, ptr := q.PopMin()
	if h != Handle(nilIdx) || tick != 0 || ptr != nil {
		t.Fatalf("PopMin empty got (%v,%d,%p)", h, tick, ptr)
	}
}
