// -------------------------
// File: queue_test.go
// -------------------------
package quantumqueue

import (
	"bytes"
	"testing"
)

func payload(id byte) []byte { return []byte{id} }

func TestEmptySafePeep(t *testing.T) {
	q := NewQuantumQueue()
	if h, tk, data := q.PeepMinSafe(); h != nilIdx || tk != 0 || data != nil {
		t.Fatalf("PeepMinSafe empty = (%v, %d, %v)", h, tk, data)
	}
}

func TestPushPeepUnlink(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.Borrow()
	tick := int64(10)
	want := payload(7)
	q.Push(tick, h, want)
	ph, pt, pdata := q.PeepMin()
	if ph != h || pt != tick || !bytes.Equal(pdata[:1], want) {
		t.Fatalf("PeepMin = (%v,%v,%v), want (%v,%v,%v)", ph, pt, pdata[:1], h, tick, want)
	}
	q.UnlinkMin(ph, pt)
	if !q.Empty() {
		t.Fatalf("queue not empty after UnlinkMin")
	}
}

func TestDuplicatePush(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.Borrow()
	tick := int64(5)
	p1 := payload(1)
	p2 := payload(2)
	q.Push(tick, h, p1)
	if q.Size() != 1 {
		t.Fatalf("Size after first push = %d, want 1", q.Size())
	}
	q.Push(tick, h, p2) // updates existing
	if q.Size() != 1 {
		t.Fatalf("Size after duplicate push = %d, want 1", q.Size())
	}
	_, _, pdata := q.PeepMin()
	if !bytes.Equal(pdata[:1], p2) {
		t.Fatalf("payload after duplicate push = %v, want %v", pdata[:1], p2)
	}
	ph, pt, _ := q.PeepMin()
	q.UnlinkMin(ph, pt)
}

func TestPushIfFree(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.Borrow()
	ok := q.PushIfFree(1, h, payload(1))
	if !ok || q.Size() != 1 {
		t.Fatalf("PushIfFree first = (%v, size=%d), want ok and size=1", ok, q.Size())
	}
	ok2 := q.PushIfFree(2, h, payload(2))
	if ok2 || q.Size() != 1 {
		t.Fatalf("PushIfFree second = (%v, size=%d), want false and size=1", ok2, q.Size())
	}
	ph, pt, _ := q.PeepMin()
	q.UnlinkMin(ph, pt)
}

func TestMoveTick(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.Borrow()
	q.Push(1, h, payload(3))
	q.MoveTick(h, 5)
	ph, pt, pdata := q.PeepMin()
	if ph != h || pt != 5 || !bytes.Equal(pdata[:1], payload(3)) {
		t.Fatalf("after MoveTick = (%v,%v,%v), want (%v,5,%v)", ph, pt, pdata[:1], h, payload(3))
	}
	q.UnlinkMin(ph, pt)
}

// Test that moving two handles to the same new tick works correctly.
func TestMoveTickDuplicate(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.Borrow()
	h2, _ := q.Borrow()
	origTick := int64(5)
	newTick := int64(10)
	q.Push(origTick, h1, payload(1))
	q.Push(origTick, h2, payload(2))
	// move both to newTick
	q.MoveTick(h1, newTick)
	q.MoveTick(h2, newTick)
	// pop first
	ph1, pt1, data1 := q.PeepMin()
	if pt1 != newTick {
		t.Fatalf("first pop tick = %d, want %d", pt1, newTick)
	}
	if ph1 != h2 && ph1 != h1 {
		t.Fatalf("first pop handle = %v, want one of {%v,%v}", ph1, h1, h2)
	}
	if data1 == nil {
		t.Fatal("first pop data is nil")
	}
	q.UnlinkMin(ph1, pt1)
	// pop second
	ph2, pt2, data2 := q.PeepMin()
	if pt2 != newTick {
		t.Fatalf("second pop tick = %d, want %d", pt2, newTick)
	}
	if (ph2 != h1 && ph2 != h2) || ph2 == ph1 {
		t.Fatalf("second pop handle = %v, want the other handle", ph2)
	}
	if data2 == nil {
		t.Fatal("second pop data is nil")
	}
	q.UnlinkMin(ph2, pt2)
	if !q.Empty() {
		t.Fatal("queue not empty after popping both entries")
	}
}

func TestSizeEmpty(t *testing.T) {
	q := NewQuantumQueue()
	if !q.Empty() || q.Size() != 0 {
		t.Fatalf("new queue: Empty=%v, Size=%d; want true,0", q.Empty(), q.Size())
	}
	h, _ := q.Borrow()
	q.Push(0, h, payload(0))
	if q.Empty() || q.Size() != 1 {
		t.Fatalf("after push: Empty=%v, Size=%d; want false,1", q.Empty(), q.Size())
	}
	ph, pt, _ := q.PeepMin()
	q.UnlinkMin(ph, pt)
	if !q.Empty() || q.Size() != 0 {
		t.Fatalf("after unlink: Empty=%v, Size=%d; want true,0", q.Empty(), q.Size())
	}
}

// TestPush_DuplicateReplacesOldEntry ensures that pushing the same handle twice
// will unlink the old bucket and keep the queue size at 1.
func TestPush_DuplicateReplacesOldEntry(t *testing.T) {
	q := NewQuantumQueue()

	// Borrow a handle
	h, err := q.Borrow()
	if err != nil {
		t.Fatalf("unexpected error on Borrow: %v", err)
	}

	data := []byte{0xAA}
	firstTick := int64(10)

	// First push should increase size to 1
	q.Push(firstTick, h, data)
	if got := q.Size(); got != 1 {
		t.Fatalf("after first Push, Size() = %d; want 1", got)
	}

	// Push again with a later tick: this should unlink the old entry
	// (size goes back to 0) then insert the new one (size == 1).
	secondTick := firstTick + 5
	q.Push(secondTick, h, data)
	if got := q.Size(); got != 1 {
		t.Errorf("after duplicate Push, Size() = %d; want 1", got)
	}

	// And PeepMin should reflect the new tick
	ph, pt, pd := q.PeepMin()
	if ph != h || pt != secondTick || !bytes.Equal(pd, data) {
		t.Errorf("PeepMin = (%v,%d,%v); want (%v,%d,%v)", ph, pt, pd, h, secondTick, data)
	}
}

// TestPeepMinSafe_EquivalentToPeepMin verifies that PeepMinSafe()
// is a drop-in for PeepMin() on both empty and non-empty queues.
func TestPeepMinSafe_EquivalentToPeepMin(t *testing.T) {
	q := NewQuantumQueue()

	// On an empty queue, both should return the identical zero-value result.
	h1, t1, d1 := q.PeepMin()
	h2, t2, d2 := q.PeepMinSafe()
	if h1 != h2 || t1 != t2 || !bytes.Equal(d1, d2) {
		t.Errorf("empty queue: PeepMinSafe = (%v,%d,%v); want same as PeepMin = (%v,%d,%v)",
			h2, t2, d2, h1, t1, d1)
	}

	// Now push one item
	h, err := q.Borrow()
	if err != nil {
		t.Fatalf("unexpected error on Borrow: %v", err)
	}
	payload := []byte{0x01, 0x02}
	tick := int64(42)
	q.Push(tick, h, payload)

	// On non-empty, PeepMinSafe must match PeepMin
	ph1, pt1, pd1 := q.PeepMin()
	ph2, pt2, pd2 := q.PeepMinSafe()
	if ph1 != ph2 || pt1 != pt2 || !bytes.Equal(pd1, pd2) {
		t.Errorf("after one Push: PeepMinSafe = (%v,%d,%v); want same as PeepMin = (%v,%d,%v)",
			ph2, pt2, pd2, ph1, pt1, pd1)
	}
}
