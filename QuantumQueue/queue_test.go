// Refactored tests for the footgun edition of QuantumQueue
// This file contains both unit and benchmark tests updated to match the current API.

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
	// clean up
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
	// clean up
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
