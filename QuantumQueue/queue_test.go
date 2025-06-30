package quantumqueue

import (
	"testing"
)

func arr48(b []byte) *[48]byte {
	var a [48]byte
	copy(a[:], b)
	return &a
}

// TestNewQueueEmptyAndSize verifies that a new queue reports empty and size zero.
func TestNewQueueEmptyAndSize(t *testing.T) {
	q := NewQuantumQueue()
	if !q.Empty() {
		t.Error("New queue should be empty")
	}
	if got := q.Size(); got != 0 {
		t.Errorf("Size of new queue = %d; want 0", got)
	}
}

// TestBorrowSafeExhaustion verifies that BorrowSafe returns an error once the arena is exhausted.
func TestBorrowSafeExhaustion(t *testing.T) {
	q := NewQuantumQueue()
	for i := 0; i < CapItems; i++ {
		if _, err := q.BorrowSafe(); err != nil {
			t.Fatalf("unexpected error at borrow #%d: %v", i, err)
		}
	}
	if _, err := q.BorrowSafe(); err == nil {
		t.Error("expected error after exhausting handles, got nil")
	}
}

// TestBorrowResetsNode verifies that Borrow resets node fields.
func TestBorrowResetsNode(t *testing.T) {
	q := NewQuantumQueue()
	h1, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow returned unexpected error: %v", err)
	}
	n1 := &q.arena[h1]
	if n1.tick != -1 {
		t.Errorf("Borrow did not reset tick; got %d; want -1", n1.tick)
	}
	if n1.prev != nilIdx || n1.next != nilIdx {
		t.Errorf("Borrow did not reset pointers; prev=%v next=%v; want both nilIdx", n1.prev, n1.next)
	}
	h2, _ := q.Borrow()
	if h2 != h1+1 {
		t.Errorf("Borrow order wrong; second handle = %v; want %v", h2, h1+1)
	}
}

// TestBasicPushPeepMinMoveTickUnlink exercises Push, PeepMin, MoveTick, and UnlinkMin in sequence.
func TestBasicPushPeepMinMoveTickUnlink(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()

	q.Push(10, h1, arr48([]byte("foo")))
	if q.Empty() {
		t.Error("queue should not be empty after first push")
	}
	if got := q.Size(); got != 1 {
		t.Errorf("Size after one push = %d; want 1", got)
	}
	h, tick, data := q.PeepMin()
	if h != h1 || tick != 10 {
		t.Errorf("PeepMin = (%v, %d); want (%v, 10)", h, tick, h1)
	}
	if string(data[:3]) != "foo" {
		t.Errorf("PeepMin data = %q; want 'foo'", data[:3])
	}

	q.Push(5, h2, arr48([]byte("bar")))
	if got := q.Size(); got != 2 {
		t.Errorf("Size after two pushes = %d; want 2", got)
	}
	h, tick, data = q.PeepMin()
	if h != h2 || tick != 5 {
		t.Errorf("PeepMin = (%v, %d); want (%v, 5)", h, tick, h2)
	}
	if string(data[:3]) != "bar" {
		t.Errorf("PeepMin data = %q; want 'bar'", data[:3])
	}

	q.MoveTick(h2, 20)
	if got := q.Size(); got != 2 {
		t.Errorf("Size after MoveTick = %d; want 2", got)
	}
	h, tick, _ = q.PeepMin()
	if h != h1 || tick != 10 {
		t.Errorf("PeepMin after MoveTick = (%v, %d); want (%v, 10)", h, tick, h1)
	}

	q.UnlinkMin(h1, 10)
	if got := q.Size(); got != 1 {
		t.Errorf("Size after UnlinkMin = %d; want 1", got)
	}
	h, tick, _ = q.PeepMin()
	if h != h2 || tick != 20 {
		t.Errorf("PeepMin after UnlinkMin = (%v, %d); want (%v, 20)", h, tick, h2)
	}

	q.UnlinkMin(h2, 20)
	if !q.Empty() || q.Size() != 0 {
		t.Errorf("Queue not empty after removing all entries: Empty=%v, Size=%d", q.Empty(), q.Size())
	}
}

// TestUpdateSameTick ensures that a Push on an existing handle with the same tick
// updates the payload in place without changing its bucket.
func TestUpdateSameTick(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(3, h, arr48([]byte("abc")))
	sz := q.Size()
	q.Push(3, h, arr48([]byte("xyz")))
	if got := q.Size(); got != sz {
		t.Errorf("Size after update Push = %d; want %d", got, sz)
	}
	ph, _, data := q.PeepMin()
	if ph != h {
		t.Errorf("PeepMin handle = %v; want %v", ph, h)
	}
	if string(data[:3]) != "xyz" {
		t.Errorf("Updated data = %q; want 'xyz'", data[:3])
	}
}

// TestPushReassignExistingHandle verifies that reassigning an existing handle moves it to the new bucket.
func TestPushReassignExistingHandle(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(30, h, arr48([]byte("old")))
	sz := q.Size()
	q.Push(40, h, arr48([]byte("new")))
	if q.Size() != sz {
		t.Errorf("Size after reassign Push = %d; want %d", q.Size(), sz)
	}
	oldB := idx32(30)
	newB := idx32(40)
	if q.buckets[oldB] != nilIdx {
		t.Errorf("Old bucket not emptied; got %v; want nilIdx", q.buckets[oldB])
	}
	if q.buckets[newB] != h {
		t.Errorf("New bucket head = %v; want %v", q.buckets[newB], h)
	}
	ph, tick, data := q.PeepMin()
	if ph != h || tick != 40 {
		t.Errorf("PeepMin after reassign = (%v, %d); want (%v, 40)", ph, tick, h)
	}
	if string(data[:3]) != "new" {
		t.Errorf("Reassigned data = %q; want 'new'", data[:3])
	}
	n := &q.arena[h]
	if n.prev != nilIdx || n.next != nilIdx {
		t.Errorf("Node pointers not reset on reassign; prev=%v next=%v; want nilIdx", n.prev, n.next)
	}
}

// TestDuplicateTicks_UnlinkAndMoveTick ensures correct behavior with duplicate ticks and removals.
func TestDuplicateTicks_UnlinkAndMoveTick(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	q.Push(7, h1, arr48([]byte("A")))
	q.Push(7, h2, arr48([]byte("B")))
	if got := q.Size(); got != 2 {
		t.Fatalf("Size with duplicates = %d; want 2", got)
	}
	h, tick, data := q.PeepMin()
	if h != h2 || tick != 7 || string(data[:1]) != "B" {
		t.Errorf("PeepMin duplicate head = (%v, %d, %q); want (%v, 7, 'B')", h, tick, data[:1], h2)
	}

	q.UnlinkMin(h1, 7)
	if got := q.Size(); got != 1 {
		t.Errorf("Size after UnlinkMin non-head = %d; want 1", got)
	}
	h, tick, _ = q.PeepMin()
	if h != h2 || tick != 7 {
		t.Errorf("Remaining after UnlinkMin = (%v, %d); want (%v, 7)", h, tick, h2)
	}

	h3, _ := q.BorrowSafe()
	if h3 != h1 {
		t.Fatalf("Expected reuse of freed handle %v; got %v", h1, h3)
	}
	q.Push(7, h3, arr48([]byte("A2")))

	q.MoveTick(h2, 8)
	if got := q.Size(); got != 2 {
		t.Errorf("Size after MoveTick non-head = %d; want 2", got)
	}
	h, tick, _ = q.PeepMin()
	if h != h3 || tick != 7 {
		t.Errorf("PeepMin after MoveTick non-head = (%v, %d); want (%v, 7)", h, tick, h3)
	}
}

// TestUnlinkDoublyLinkedPrevAfterRemove ensures proper doubly-linked stitching when removing a middle node.
func TestUnlinkDoublyLinkedPrevAfterRemove(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()
	// Build chain h1->h2->h3 at same tick
	q.Push(100, h1, arr48([]byte{1}))
	q.Push(100, h2, arr48([]byte{2}))
	q.Push(100, h3, arr48([]byte{3}))
	if q.arena[h2].prev != h3 || q.arena[h2].next != h1 {
		t.Fatalf("Initial chain incorrect: prev=%v next=%v; want prev=%v next=%v", q.arena[h2].prev, q.arena[h2].next, h3, h1)
	}
	q.UnlinkMin(h2, 100)
	b := idx32(100)
	if q.buckets[b] != h3 {
		t.Errorf("Bucket head after removal = %v; want %v", q.buckets[b], h3)
	}
	if q.arena[h3].next != h1 {
		t.Errorf("After unlink, h3.next = %v; want %v", q.arena[h3].next, h1)
	}
	if q.arena[h1].prev != h3 {
		t.Errorf("After unlink, h1.prev = %v; want %v", q.arena[h1].prev, h3)
	}
	h4, _ := q.BorrowSafe()
	if h4 != h2 {
		t.Errorf("Expected freed handle %v reused; got %v", h2, h4)
	}
}

// TestMoveTickNoOp ensures that MoveTick to the same tick does nothing.
func TestMoveTickNoOp(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(5, h, arr48([]byte("Z")))
	sz := q.Size()
	q.MoveTick(h, 5)
	if got := q.Size(); got != sz {
		t.Errorf("Size after MoveTick no-op = %d; want %d", got, sz)
	}
	h2, tick2, data := q.PeepMin()
	if h2 != h || tick2 != 5 || string(data[:1]) != "Z" {
		t.Errorf("PeepMin after MoveTick no-op = (%v, %d, %q); want (%v, 5, 'Z')", h2, tick2, data[:1], h)
	}
}

// TestPushSameTickUpdatesData ensures that a Push on an existing handle with
// the same tick updates the payload in place without moving it.
func TestPushSameTickUpdatesData(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(42, h, arr48([]byte("orig")))
	sz := q.Size()
	q.Push(42, h, arr48([]byte("edit")))
	if got := q.Size(); got != sz {
		t.Errorf("Size after same-tick Push = %d; want %d", got, sz)
	}
	// verify data updated
	ph, tick, data := q.PeepMin()
	if ph != h || tick != 42 {
		t.Errorf("PeepMin after same-tick update = (%v, %d); want (%v, 42)", ph, tick, h)
	}
	if string(data[:4]) != "edit" {
		t.Errorf("Data after same-tick update = %q; want 'edit'", data[:4])
	}
	// bucket head unchanged
	b := idx32(42)
	if q.buckets[b] != h {
		t.Errorf("Bucket head moved on same-tick update; got %v; want %v", q.buckets[b], h)
	}
}

// TestPushExistingHandleUnlink ensures that a Push on an existing handle with
// a different tick unlinks it from its old bucket before linking at the new tick.
func TestPushExistingHandleUnlink(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(10, h, arr48([]byte("first")))
	sz := q.Size()
	q.Push(20, h, arr48([]byte("second")))
	if got := q.Size(); got != sz {
		t.Errorf("Size after reassign Push = %d; want %d", got, sz)
	}
	oldB := idx32(10)
	newB := idx32(20)
	if q.buckets[oldB] != nilIdx {
		t.Errorf("Old bucket not emptied; got %v; want nilIdx", q.buckets[oldB])
	}
	if q.buckets[newB] != h {
		t.Errorf("New bucket head = %v; want %v", q.buckets[newB], h)
	}
	ph, tick, _ := q.PeepMin()
	if ph != h || tick != 20 {
		t.Errorf("PeepMin after reassign = (%v, %d); want (%v, 20)", ph, tick, h)
	}
}
