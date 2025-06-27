// bucketqueue_peep_test.go verifies that PeepMin behaves identically to PopMin
// in terms of ordering but does not mutate the internal queue state.
//
// These tests act as read-only validation of the earliest item retrieval
// logic and its response to reordering and updates.
package bucketqueue

import "testing"

// TestPeepMinEmpty ensures PeepMin returns a safe sentinel when empty.
func TestPeepMinEmpty(t *testing.T) {
	q := New()
	h, tick, val := q.PeepMin()
	if h != Handle(nilIdx) || tick != 0 || val != nil {
		t.Fatalf("expected empty peep → (nilIdx,0,nil); got h=%v tick=%d val=%v", h, tick, val)
	}
}

// TestPeepMinBasic confirms PeepMin sees the earliest tick item without removal.
func TestPeepMinBasic(t *testing.T) {
	q := New()

	h1, _ := q.Borrow()
	h2, _ := q.Borrow()
	h3, _ := q.Borrow()

	_ = q.Push(50, h1, nil)
	_ = q.Push(10, h2, nil) // earliest
	_ = q.Push(30, h3, nil)

	peekH, peekTick, _ := q.PeepMin()
	if peekH != h2 || peekTick != 10 {
		t.Fatalf("PeepMin mismatch: got h=%v tick=%d want h=%v tick=10", peekH, peekTick, h2)
	}

	if q.Size() != 3 {
		t.Fatalf("queue mutated after PeepMin: size=%d want 3", q.Size())
	}

	popH, popTick, _ := q.PopMin()
	if popH != h2 || popTick != 10 {
		t.Fatalf("PopMin mismatch after PeepMin: got h=%v tick=%d want h=%v tick=10", popH, popTick, h2)
	}
}

// TestPeepMinUpdates checks if PeepMin respects tick updates.
func TestPeepMinUpdates(t *testing.T) {
	q := New()

	h1, _ := q.Borrow()
	_ = q.Push(90, h1, nil)

	_, tick, _ := q.PeepMin()
	if tick != 90 {
		t.Fatalf("expected tick 90, got %d", tick)
	}

	// Move handle to earlier tick and confirm PeepMin tracks it
	_ = q.Update(5, h1, nil)
	_, tick2, _ := q.PeepMin()
	if tick2 != 5 {
		t.Fatalf("PeepMin failed to reflect updated tick: got %d want 5", tick2)
	}
}
