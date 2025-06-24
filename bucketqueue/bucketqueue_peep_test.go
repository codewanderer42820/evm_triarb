// Package bucketqueue implements a zero-allocation, low-latency time-bucket priority queue.
// Items are distributed across a fixed-size sliding window of time-indexed buckets.
// A two-level bitmap structure allows O(1) retrieval of the earliest item.
//
// This implementation uses a fixed arena allocator, intrusive linked lists,
// and compact handle management for high-throughput applications such as
// schedulers, simulation engines, or event queues.
//
// Peep‑oriented test‑suite — verifies that PeepMin() behaves exactly like PopMin()
// in terms of ordering *without* mutating queue state.

package bucketqueue

import "testing"

// TestPeepMinEmpty confirms that PeepMin on an empty queue returns the sentinel
// nilIdx handle and does not panic.
func TestPeepMinEmpty(t *testing.T) {
	q := New()
	if h, tick, val := q.PeepMin(); h != Handle(nilIdx) || tick != 0 || val != nil {
		t.Fatalf("expected empty peep → (nilIdx,0,nil); got h=%v tick=%d val=%v", h, tick, val)
	}
}

// TestPeepMinBasic pushes three items with ascending tick values and ensures
// PeepMin sees the earliest (lowest tick) *without* removing it, whereas the
// subsequent PopMin must return the same item.
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

// TestPeepMinUpdates checks that PeepMin always tracks the current minimum when
// items are updated to an earlier tick.
func TestPeepMinUpdates(t *testing.T) {
	q := New()

	h1, _ := q.Borrow()
	_ = q.Push(90, h1, nil)

	if _, tick, _ := q.PeepMin(); tick != 90 {
		t.Fatalf("expected tick 90, got %d", tick)
	}

	// Move handle earlier and confirm PeepMin reflects change
	_ = q.Update(5, h1, nil)
	if _, tick, _ := q.PeepMin(); tick != 5 {
		t.Fatalf("PeepMin failed to reflect updated tick: got %d want 5", tick)
	}
}
