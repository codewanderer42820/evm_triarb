package quantumqueue

import (
	"bytes"
	"math"
	"testing"
)

/*──────── helpers ───────────────────────────────────────────────────*/

func payload(id int) []byte { return []byte{byte(id)} }

func mustBorrow(t *testing.T, q *QuantumQueue) Handle {
	h, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	return h
}

func inWindow(q *QuantumQueue, off int64) int64 {
	base := int64(q.baseTick)
	switch {
	case off > 0 && base > math.MaxInt64-off:
		return base - off
	case off < 0 && base < math.MinInt64-off:
		return base - off
	default:
		return base + off
	}
}

// equalPrefix reports whether pv starts with want’s bytes.
func equalPrefix(pv, want []byte) bool {
	return len(pv) >= len(want) && bytes.Equal(pv[:len(want)], want)
}

/*──────── Empty-queue guards ───────────────────────────────────────*/

func TestEmptyQueueBranches(t *testing.T) {
	q := NewQuantumQueue()
	if h, tk, ptr := q.PeepMin(); h != Handle(nilIdx) || tk != 0 || ptr != nil {
		t.Fatalf("PeepMin empty = (%v,%d,%v)", h, tk, ptr)
	}
	if h, tk, ptr := q.PopMin(); h != Handle(nilIdx) || tk != 0 || ptr != nil {
		t.Fatalf("PopMin empty = (%v,%d,%v)", h, tk, ptr)
	}
}

/*──────── Push / Pop basics ─────────────────────────────────────────*/

func TestPushPopBasic(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	want := payload(123)
	tk := inWindow(q, 10)

	if err := q.Push(tk, h, want); err != nil {
		t.Fatal(err)
	}
	ph, pt, pv := q.PeepMin()
	if ph != h || pt != tk || !equalPrefix(pv, want) {
		t.Fatalf("PeepMin wrong value")
	}
	popH, popT, popV := q.PopMin()
	if popH != h || popT != tk || !equalPrefix(popV, want) {
		t.Fatalf("PopMin wrong value")
	}
	if !q.Empty() {
		t.Fatalf("queue should be empty")
	}
}

/*──────── Duplicate-tick path ───────────────────────────────────────*/

func TestDuplicateTick(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	tk := inWindow(q, 5)

	_ = q.Push(tk, h, payload(1))
	_ = q.Push(tk, h, payload(2)) // duplicate, count=2

	if q.Size() != 1 {
		t.Fatalf("size want 1 after dup push")
	}
	_, _, _ = q.PopMin() // pops first duplicate
	if q.Size() != 0 {   // handle removed because count now 1→0
		t.Fatalf("size after first dup pop want 0")
	}
}

/*──────── Update same-tick / move ───────────────────────────────────*/

func TestUpdatePaths(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)

	sameTick := inWindow(q, 20)
	want99 := payload(99)
	_ = q.Push(sameTick, h, payload(0))
	_ = q.Update(sameTick, h, want99)
	if _, tk, pv := q.PeepMin(); tk != sameTick || !equalPrefix(pv, want99) {
		t.Fatalf("same-tick Update failed")
	}

	moveTick := inWindow(q, 25)
	want42 := payload(42)
	_ = q.Update(moveTick, h, want42)
	ph, pt, pv := q.PeepMin()
	if ph != h || pt != moveTick || !equalPrefix(pv, want42) {
		t.Fatalf("move Update wrong")
	}
}

/*──────── Window error guards ───────────────────────────────────────*/

func TestWindowErrors(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	if err := q.Push(inWindow(q, -BucketCount-1), h, payload(0)); err != ErrPastWindow {
		t.Fatalf("want ErrPastWindow, got %v", err)
	}
	if err := q.Push(inWindow(q, BucketCount+1), h, payload(0)); err != ErrBeyondWindow {
		t.Fatalf("want ErrBeyondWindow, got %v", err)
	}
}

/*──────── Link-patch branches ───────────────────────────────────────*/

func TestLinkFixups(t *testing.T) {
	q := NewQuantumQueue()
	hA, hB, hC := mustBorrow(t, q), mustBorrow(t, q), mustBorrow(t, q)

	_ = q.Push(inWindow(q, 5), hA, payload(0))
	_ = q.Push(inWindow(q, 8), hB, payload(1)) // head
	_ = q.Push(inWindow(q, 8), hC, payload(2)) // new head

	if err := q.Update(inWindow(q, 8), hA, payload(3)); err != nil {
		t.Fatalf("Update move failed: %v", err)
	}
	if q.arena[idx32(hA)].next == nilIdx {
		t.Fatalf("next not patched")
	}
	if q.arena[q.arena[idx32(hA)].next].prev != idx32(hA) {
		t.Fatalf("prev link not patched")
	}

	// also exercise unlink->patch of head.prev
	h1, h2, h3 := mustBorrow(t, q), mustBorrow(t, q), mustBorrow(t, q)
	_ = q.Push(inWindow(q, 10), h1, payload(1))
	_ = q.Push(inWindow(q, 10), h2, payload(2))
	_ = q.Push(inWindow(q, 10), h3, payload(3)) // head
	if err := q.Update(inWindow(q, 12), h2, payload(9)); err != nil {
		t.Fatalf("Update move failed: %v", err)
	}
	if q.arena[idx32(h3)].prev != nilIdx {
		t.Fatalf("unlink failed: h3.prev != nilIdx")
	}
}

/*──────── Lane & group clear ───────────────────────────────────────*/

func TestGroupClear(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)

	delta := uint64((3 << 12) | (4 << 6) | 5)
	tick := int64(q.baseTick) + int64(delta)
	_ = q.Push(tick, h, payload(0))
	if q.summary == 0 {
		t.Fatalf("summary not set")
	}
	_, _, _ = q.PopMin()
	if q.summary != 0 {
		t.Fatalf("summary bit not cleared")
	}
}
