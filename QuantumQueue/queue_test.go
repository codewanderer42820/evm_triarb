// SPDX-Free: Public-Domain
// Ultra-low-latency QuantumQueue — footgun edition test suite
// PeepMinSafe/PopMinSafe allow empty-safe testing

package quantumqueue

import (
	"bytes"
	"math"
	"testing"
)

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

func equalPrefix(pv, want []byte) bool {
	return len(pv) >= len(want) && bytes.Equal(pv[:len(want)], want)
}

func TestEmptyQueueBranches(t *testing.T) {
	q := NewQuantumQueue()
	if h, tk, ptr := q.PeepMinSafe(); h != Handle(nilIdx) || tk != 0 || ptr != nil {
		t.Fatalf("PeepMinSafe empty = (%v,%d,%v)", h, tk, ptr)
	}
	if h, tk, ptr := q.PopMinSafe(); h != Handle(nilIdx) || tk != 0 || ptr != nil {
		t.Fatalf("PopMinSafe empty = (%v,%d,%v)", h, tk, ptr)
	}
}

func TestPushPopBasic(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	want := payload(123)
	tk := inWindow(q, 10)

	q.Push(tk, h, want)
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

func TestDuplicateTick(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	tk := inWindow(q, 5)

	q.Push(tk, h, payload(1))
	q.Push(tk, h, payload(2))

	if q.Size() != 1 {
		t.Fatalf("size want 1 after dup push")
	}
	_, _, _ = q.PopMin()
	if q.Size() != 0 {
		t.Fatalf("size after first dup pop want 0")
	}
}

func TestUpdatePaths(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)

	sameTick := inWindow(q, 20)
	want99 := payload(99)
	q.Push(sameTick, h, payload(0))
	q.Update(sameTick, h, want99)
	if _, tk, pv := q.PeepMin(); tk != sameTick || !equalPrefix(pv, want99) {
		t.Fatalf("same-tick Update failed")
	}

	moveTick := inWindow(q, 25)
	want42 := payload(42)
	q.Update(moveTick, h, want42)
	ph, pt, pv := q.PeepMin()
	if ph != h || pt != moveTick || !equalPrefix(pv, want42) {
		t.Fatalf("move Update wrong")
	}
}

func TestLinkFixups(t *testing.T) {
	q := NewQuantumQueue()
	hA, hB, hC := mustBorrow(t, q), mustBorrow(t, q), mustBorrow(t, q)

	q.Push(inWindow(q, 5), hA, payload(0))
	q.Push(inWindow(q, 8), hB, payload(1))
	q.Push(inWindow(q, 8), hC, payload(2))
	q.Update(inWindow(q, 8), hA, payload(3))

	if q.arena[idx32(hA)].next == nilIdx {
		t.Fatalf("next not patched")
	}
	if q.arena[q.arena[idx32(hA)].next].prev != idx32(hA) {
		t.Fatalf("prev link not patched")
	}

	h1, h2, h3 := mustBorrow(t, q), mustBorrow(t, q), mustBorrow(t, q)
	q.Push(inWindow(q, 10), h1, payload(1))
	q.Push(inWindow(q, 10), h2, payload(2))
	q.Push(inWindow(q, 10), h3, payload(3))
	q.Update(inWindow(q, 12), h2, payload(9))

	if q.arena[idx32(h3)].prev != nilIdx {
		t.Fatalf("unlink failed: h3.prev != nilIdx")
	}
}

func TestGroupClear(t *testing.T) {
	q := NewQuantumQueue()
	h := mustBorrow(t, q)
	delta := uint64((3 << 12) | (4 << 6) | 5)
	tick := int64(q.baseTick) + int64(delta)
	q.Push(tick, h, payload(0))
	if q.summary == 0 {
		t.Fatalf("summary not set")
	}
	_, _, _ = q.PopMin()
	if q.summary != 0 {
		t.Fatalf("summary bit not cleared")
	}
}
