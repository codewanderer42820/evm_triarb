package quantumqueue

import "testing"

/*── unlink path where prev != nilIdx ─*/

func TestUnlinkPrevBranch(t *testing.T) {
	q := NewQuantumQueue()
	h1 := mustBorrow(t, q)
	h2 := mustBorrow(t, q)

	if err := q.Push(10, h1, payload(1)); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(10, h2, payload(2)); err != nil {
		t.Fatal(err)
	} // head

	if err := q.Update(12, h1, payload(3)); err != nil {
		t.Fatal(err)
	} // unlink middle

	if _, tick, _ := q.PeepMin(); tick != 10 {
		t.Fatalf("min tick want 10 got %d", tick)
	}
	if next := q.arena[idx32(h2)].next; next != nilIdx {
		t.Fatalf("unlink failed: h2.next = %d (want nilIdx)", next)
	}
	_, t1, _ := q.PopMin()
	_, t2, _ := q.PopMin()
	if t1 != 10 || t2 != 12 {
		t.Fatalf("pop order wrong")
	}
	if !q.Empty() {
		t.Fatalf("queue should be empty")
	}
}

/*── Push relocation path ─*/

func TestPushMoveExisting(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)

	if err := q.Push(5, h, payload(0)); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(8, h, payload(1)); err != nil {
		t.Fatal(err)
	}

	_, tick, _ := q.PeepMin()
	if tick != 8 {
		t.Fatalf("tick after move want 8 got %d", tick)
	}
	if q.Size() != 1 {
		t.Fatalf("size want 1 got %d", q.Size())
	}
}

/*── Update early-exit branches ─*/

func TestUpdateEarlyExits(t *testing.T) {
	q := NewQuantumQueue()
	valid := mustBorrow(t, q)

	if err := q.Update(0, Handle(nilIdx), payload(0)); err != ErrNotFound {
		t.Fatalf("nilIdx handle: %v", err)
	}
	if err := q.Update(0, Handle(idx32(CapItems)), payload(0)); err != ErrNotFound {
		t.Fatalf("big handle: %v", err)
	}
	if err := q.Update(0, valid, payload(0)); err != ErrNotFound {
		t.Fatalf("inactive handle: %v", err)
	}

	if err := q.Push(0, valid, payload(0)); err != nil {
		t.Fatal(err)
	}
	if err := q.Update(-1, valid, payload(0)); err != ErrPastWindow {
		t.Fatalf("past window: %v", err)
	}
	if err := q.Update(int64(BucketCount)+1, valid, payload(0)); err != ErrBeyondWindow {
		t.Fatalf("beyond window: %v", err)
	}
}
