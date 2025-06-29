package quantumqueue

import (
	"testing"
)

func TestNewQuantumQueueEmpty(t *testing.T) {
	q := NewQuantumQueue()
	if !q.Empty() {
		t.Errorf("New queue should be empty")
	}
	if got := q.Size(); got != 0 {
		t.Errorf("Size = %d; want 0", got)
	}

	// PeepMinSafe on empty
	h, tick, data := q.PeepMinSafe()
	if h != nilIdx {
		t.Errorf("PeepMinSafe handle = %v; want nilIdx", h)
	}
	if tick != 0 {
		t.Errorf("PeepMinSafe tick = %d; want 0", tick)
	}
	if *data != zeroPayload {
		t.Errorf("PeepMinSafe payload = %v; want all zeros", *data)
	}
}

func TestBorrowAndBorrowSafe(t *testing.T) {
	q := NewQuantumQueue()

	h1, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow error: %v", err)
	}
	if h1 == nilIdx {
		t.Errorf("Borrow returned nilIdx")
	}

	// BorrowSafe same on fresh queue
	h2, err := q.BorrowSafe()
	if err != nil {
		t.Fatalf("BorrowSafe error: %v", err)
	}
	if h2 == nilIdx {
		t.Errorf("BorrowSafe returned nilIdx on non-exhausted arena")
	}
}

func TestBorrowSafeExhaustion(t *testing.T) {
	q := NewQuantumQueue()
	// exhaust the arena
	for i := Handle(0); i < CapItems; i++ {
		_, err := q.BorrowSafe()
		if err != nil {
			t.Fatalf("BorrowSafe failed at iteration %d: %v", i, err)
		}
	}
	// one more should error
	if _, err := q.BorrowSafe(); err == nil {
		t.Errorf("BorrowSafe expected error on exhaustion, got nil")
	}
}

func TestPushAndPeepMin(t *testing.T) {
	q := NewQuantumQueue()
	h, err := q.BorrowSafe()
	if err != nil {
		t.Fatalf("BorrowSafe: %v", err)
	}

	val := []byte{1, 2, 3}
	q.Push(10, h, val)

	if q.Empty() {
		t.Errorf("Queue should not be empty after Push")
	}
	if got := q.Size(); got != 1 {
		t.Errorf("Size = %d; want 1", got)
	}

	// direct PeepMin
	h2, tick2, data2 := q.PeepMin()
	if h2 != h {
		t.Errorf("PeepMin handle = %v; want %v", h2, h)
	}
	if tick2 != 10 {
		t.Errorf("PeepMin tick = %d; want 10", tick2)
	}
	arr := *data2
	for i, b := range val {
		if arr[i] != b {
			t.Errorf("data[%d] = %d; want %d", i, arr[i], b)
		}
	}
	// rest should be zero
	for i := len(val); i < len(arr); i++ {
		if arr[i] != 0 {
			t.Errorf("data[%d] = %d; want 0", i, arr[i])
		}
	}
}

func TestPushOverwrite(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	first := []byte{9, 9}
	second := []byte{4, 5, 6}

	q.Push(42, h, first)
	if q.Size() != 1 {
		t.Fatalf("Size after first push = %d; want 1", q.Size())
	}
	q.Push(42, h, second) // same tick: overwrite
	if q.Size() != 1 {
		t.Errorf("Size after overwrite = %d; want still 1", q.Size())
	}

	h2, tick2, data2 := q.PeepMin()
	if h2 != h || tick2 != 42 {
		t.Errorf("PeepMin after overwrite = (%v,%d); want (%v,42)", h2, tick2, h)
	}
	arr := *data2
	for i, b := range second {
		if arr[i] != b {
			t.Errorf("overwritten data[%d] = %d; want %d", i, arr[i], b)
		}
	}
}

func TestMultiplePushAndUnlink(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()

	q.Push(20, h2, []byte{2})
	q.Push(10, h1, []byte{1})
	if got := q.Size(); got != 2 {
		t.Fatalf("Size after two pushes = %d; want 2", got)
	}

	// first min should be h1
	hm, tm, _ := q.PeepMin()
	if hm != h1 || tm != 10 {
		t.Errorf("First PeepMin = (%v,%d); want (%v,10)", hm, tm, h1)
	}

	// remove it
	q.UnlinkMin(hm, tm)
	if got := q.Size(); got != 1 {
		t.Errorf("Size after UnlinkMin = %d; want 1", got)
	}

	// next min
	hm2, tm2, _ := q.PeepMin()
	if hm2 != h2 || tm2 != 20 {
		t.Errorf("Second PeepMin = (%v,%d); want (%v,20)", hm2, tm2, h2)
	}
}

func TestUnlinkMinNoop(t *testing.T) {
	q := NewQuantumQueue()
	origHead := q.freeHead
	// tick<0 should do nothing
	q.UnlinkMin(0, -1)
	if q.freeHead != origHead {
		t.Errorf("UnlinkMin with tick<0 changed freeHead")
	}
}

func TestUnlinkMin(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(5, h, []byte{7})
	q.UnlinkMin(h, 5)
	if !q.Empty() {
		t.Errorf("Queue should be empty after UnlinkMin")
	}
	if q.Size() != 0 {
		t.Errorf("Size after UnlinkMin = %d; want 0", q.Size())
	}
}

func TestMoveTick(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	// initial insert via Push
	q.Push(15, h, []byte{8})

	// move to a new tick
	q.MoveTick(h, 25)
	if q.Size() != 1 {
		t.Errorf("Size after MoveTick = %d; want 1", q.Size())
	}
	hm, tm, data := q.PeepMin()
	if hm != h || tm != 25 {
		t.Errorf("PeepMin after MoveTick = (%v,%d); want (%v,25)", hm, tm, h)
	}
	if (*data)[0] != 8 {
		t.Errorf("payload after MoveTick = %v; want first byte 8", (*data)[0])
	}

	// moving again to same tick should do nothing
	q.MoveTick(h, 25)
	if q.Size() != 1 {
		t.Errorf("Size after redundant MoveTick = %d; want still 1", q.Size())
	}
}

func TestPeepMinSafeOnNonEmpty(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(33, h, []byte{3})
	h2, t2, d2 := q.PeepMinSafe()
	h3, t3, d3 := q.PeepMin()
	if h2 != h3 || t2 != t3 || d2 != d3 {
		t.Errorf("PeepMinSafe != PeepMin on non-empty")
	}
}
