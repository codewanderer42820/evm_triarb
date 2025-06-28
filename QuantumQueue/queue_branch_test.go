package quantumqueue

import "testing"

/*──────────────── PeepMin empty branch ───────────*/

func TestPeepMinEmptyQueue(t *testing.T) {
	q := NewQuantumQueue()
	h, tk, ptr := q.PeepMin()
	if h != Handle(nilIdx) || tk != 0 || ptr != nil {
		t.Fatalf("PeepMin on empty queue returned (%v,%d,%p)", h, tk, ptr)
	}
}

/*──────────────── PopMin empty branch ────────────*/

func TestPopMinEmptyQueue(t *testing.T) {
	q := NewQuantumQueue()
	h, tk, ptr := q.PopMin()
	if h != Handle(nilIdx) || tk != 0 || ptr != nil {
		t.Fatalf("PopMin on empty queue returned (%v,%d,%p)", h, tk, ptr)
	}
}

/*──────────────── Update: n.next != nilIdx path ──*/

func TestUpdatePrevLinkFixup(t *testing.T) {
	q := NewQuantumQueue()
	// Three handles, two of them share the same target bucket (tick 8).
	hA := mustBorrow(t, q)
	hB := mustBorrow(t, q)
	hC := mustBorrow(t, q)

	_ = q.Push(5, hA, payload(0)) // initial bucket
	_ = q.Push(8, hB, payload(1)) // will be existing head for tick 8
	_ = q.Push(8, hC, payload(2)) // becomes new head for tick 8

	// Move hA into tick 8 → insert into non-empty bucket
	if err := q.Update(8, hA, payload(3)); err != nil {
		t.Fatalf("Update move failed: %v", err)
	}

	// hA should now be head, and its next's prev must point back to hA
	if q.arena[idx32(hA)].next == nilIdx {
		t.Fatalf("Update did not link next")
	}
	if q.arena[q.arena[idx32(hA)].next].prev != idx32(hA) {
		t.Fatalf("Update failed to patch next.prev link")
	}
}

/*──────────────── unlinkByIndex: n.next != nilIdx ─*/

func TestUnlinkNextPrevPatch(t *testing.T) {
	q := NewQuantumQueue()
	h1 := mustBorrow(t, q)
	h2 := mustBorrow(t, q)
	h3 := mustBorrow(t, q)

	// Bucket tick 10: head h3 → h2 → h1
	_ = q.Push(10, h1, payload(1))
	_ = q.Push(10, h2, payload(2))
	_ = q.Push(10, h3, payload(3))

	// Update h2 to tick 12, causing unlink with n.next != nilIdx
	if err := q.Update(12, h2, payload(9)); err != nil {
		t.Fatalf("Update move failed: %v", err)
	}

	// After unlink, h3.prev must still be nilIdx (head) and not point to h2
	if q.arena[idx32(h3)].prev != nilIdx {
		t.Fatalf("unlink failed: h3.prev = %d (want nilIdx)", q.arena[idx32(h3)].prev)
	}
}
