package quantumqueue

import (
	"testing"
	"unsafe"
)

/*──────────────────── helpers ───────────────────*/

// payload returns a stable, non-nil pointer
func payload(i int) unsafe.Pointer { return unsafe.Pointer(&[]int{i}[0]) }

// mustBorrow wraps Borrow with fatal handling
func mustBorrow(t *testing.T, q *QuantumQueue) Handle {
	h, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	return h
}

/*──────────────────── Borrow exhaustion ─────────*/

func TestBorrowExhaustion(t *testing.T) {
	q := NewQuantumQueue()
	var last Handle
	for i := 1; i < CapItems; i++ {
		h, err := q.Borrow()
		if err != nil {
			t.Fatalf("Borrow #%d errored: %v", i, err)
		}
		last = h
	}
	if h, err := q.Borrow(); err != ErrFull || h != Handle(nilIdx) {
		t.Fatalf("want ErrFull, got (%v,%v)", h, err)
	}
	// recycle one handle
	if err := q.Push(0, last, payload(0)); err != nil {
		t.Fatal(err)
	}
	_, _, _ = q.PopMin()
	if _, err := q.Borrow(); err != nil {
		t.Fatalf("Borrow after recycle: %v", err)
	}
}

/*──────────────────── Push / Pop basics ─────────*/

func TestPushPopBasic(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	wantPtr := payload(123)
	if err := q.Push(10, h, wantPtr); err != nil {
		t.Fatal(err)
	}

	ph, pt, pv := q.PeepMin()
	if ph != h || pt != 10 || pv != wantPtr {
		t.Fatalf("PeepMin wrong value")
	}
	popH, popT, popV := q.PopMin()
	if popH != h || popT != 10 || popV != wantPtr {
		t.Fatalf("PopMin wrong value")
	}
	if !q.Empty() {
		t.Fatalf("queue should be empty")
	}
}

/*──────────────────── Duplicate fast path ───────*/

func TestDuplicateTick(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	_ = q.Push(5, h, payload(1))
	_ = q.Push(5, h, payload(2)) // duplicate
	if q.Size() != 2 {
		t.Fatalf("size want 2")
	}
	_, _, _ = q.PopMin()
	if q.Size() != 1 {
		t.Fatalf("dup pop size want 1")
	}
	_, _, _ = q.PopMin()
	if !q.Empty() {
		t.Fatalf("should be empty")
	}
}

/*──────────────────── Update paths ──────────────*/

func TestUpdatePaths(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)

	// same-tick patch
	ptr99 := payload(99)
	_ = q.Push(20, h, payload(0))
	_ = q.Update(20, h, ptr99)
	if _, tk, ptr := q.PeepMin(); tk != 20 || ptr != ptr99 {
		t.Fatalf("same-tick Update failed")
	}

	// move tick
	ptr42 := payload(42)
	_ = q.Update(25, h, ptr42)
	ph, pt, pv := q.PeepMin()
	if ph != h || pt != 25 || pv != ptr42 {
		t.Fatalf("move Update wrong")
	}
}

/*──────────────────── Window errors ─────────────*/

func TestWindowErrors(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	if err := q.Push(-1, h, payload(0)); err != ErrPastWindow {
		t.Fatalf("want ErrPastWindow")
	}
	if err := q.Push(int64(BucketCount)+1, h, payload(0)); err != ErrBeyondWindow {
		t.Fatalf("want ErrBeyondWindow")
	}
}

/*──────────────────── Invalid handles ───────────*/

func TestInvalidHandles(t *testing.T) {
	q := NewQuantumQueue()
	if err := q.Push(0, Handle(nilIdx), payload(0)); err != ErrNotFound {
		t.Fatalf("nilIdx")
	}
	if err := q.Push(0, Handle(idx32(CapItems)), payload(0)); err != ErrNotFound {
		t.Fatalf("big handle")
	}
}

/*──────────────────── unlink prev != nilIdx ─────*/

func TestUnlinkPrevBranch(t *testing.T) {
	q := NewQuantumQueue()
	h1, h2 := mustBorrow(t, q), mustBorrow(t, q)
	_ = q.Push(10, h1, payload(1))
	_ = q.Push(10, h2, payload(2))   // h2 is new head
	_ = q.Update(12, h1, payload(3)) // unlink middle node

	if _, tk, _ := q.PeepMin(); tk != 10 {
		t.Fatalf("min tick lost")
	}
	if next := q.arena[idx32(h2)].next; next != nilIdx {
		t.Fatalf("h2.next not cleared")
	}
	_, t1, _ := q.PopMin()
	_, t2, _ := q.PopMin()
	if t1 != 10 || t2 != 12 {
		t.Fatalf("pop order wrong")
	}
}

/*──────────────────── Push relocate branch ──────*/

func TestPushRelocate(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	_ = q.Push(5, h, payload(0))
	_ = q.Push(8, h, payload(1)) // relocate
	if _, tk, _ := q.PeepMin(); tk != 8 {
		t.Fatalf("tick after relocate wrong")
	}
	if q.Size() != 1 {
		t.Fatalf("size after relocate wrong")
	}
}

/*──────────────────── Update early exits ────────*/

func TestUpdateEarlyExits(t *testing.T) {
	q := NewQuantumQueue()
	v := mustBorrow(t, q)

	if err := q.Update(0, Handle(nilIdx), payload(0)); err != ErrNotFound {
		t.Fatalf("nilIdx")
	}
	if err := q.Update(0, Handle(idx32(CapItems)), payload(0)); err != ErrNotFound {
		t.Fatalf("big handle")
	}
	if err := q.Update(0, v, payload(0)); err != ErrNotFound {
		t.Fatalf("inactive handle")
	}

	_ = q.Push(0, v, payload(0))
	if err := q.Update(-1, v, payload(0)); err != ErrPastWindow {
		t.Fatalf("past window")
	}
	if err := q.Update(int64(BucketCount)+1, v, payload(0)); err != ErrBeyondWindow {
		t.Fatalf("beyond window")
	}
}

/*──────────────────── Lane & group clear ─────────
   Push a single item in a unique group/lane/bit, pop it,
   and ensure summary bitmaps are fully cleared.
──────────────────────────────────────────────────*/

func TestGroupClear(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	// choose delta that maps to group=3 lane=4 bit=5
	delta := uint64((3 << 12) | (4 << 6) | 5)
	tick := int64(delta) // baseTick is 0
	_ = q.Push(tick, h, payload(0))
	if q.summary == 0 {
		t.Fatalf("summary not set")
	}
	_, _, _ = q.PopMin() // removes the only item → should clear all bits
	if q.summary != 0 {
		t.Fatalf("summary bit not cleared")
	}
}
