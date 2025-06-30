// Package quantumqueue contains a long-running stress test to validate
// correctness under extensive randomized operations against a reference heap.
package quantumqueue

import (
	"container/heap"
	"math/rand"
	"testing"
)

// -----------------------------------------------------------------------------
// Reference model (heap-based)
// -----------------------------------------------------------------------------

// stressItem represents an entry in the heap-based reference model.
// It mirrors the QuantumQueue node state: handle, tick, and sequence number.
// The `seq` ensures deterministic tiebreaking on duplicate ticks.
type stressItem struct {
	h    Handle // Handle into the QuantumQueue arena
	tick int64  // Tick value (queue priority key)
	seq  int    // Sequence number to enforce LIFO for identical ticks
}

// stressHeap implements heap.Interface on []*stressItem.
// It is used to track expected ordering outside of QuantumQueue.
type stressHeap []*stressItem

// Len returns the number of items in the reference heap.
func (h stressHeap) Len() int { return len(h) }

// Less implements heap ordering:
// - Lower tick comes first
// - For equal ticks, newer (higher seq) items come first (LIFO)
func (h stressHeap) Less(i, j int) bool {
	if h[i].tick != h[j].tick {
		return h[i].tick < h[j].tick
	}
	return h[i].seq > h[j].seq // favor newer entries
}

// Swap swaps two elements in the reference heap.
func (h stressHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push appends a new item to the reference heap.
func (h *stressHeap) Push(x interface{}) {
	*h = append(*h, x.(*stressItem))
}

// Pop removes the minimum item from the reference heap.
func (h *stressHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	it := old[n]
	*h = old[:n]
	return it
}

// -----------------------------------------------------------------------------
// Stress test: randomized push, move, and pop operations
// -----------------------------------------------------------------------------

// TestQueueStressRandomOperations performs millions of randomized operations
// across both QuantumQueue and the reference heap to ensure behavioral parity.
// This is a slow, high-confidence regression test for correctness.
func TestQueueStressRandomOperations(t *testing.T) {
	const iterations = 10_000_000

	// Use deterministic PRNG for reproducibility
	rng := rand.New(rand.NewSource(69))

	q := NewQuantumQueue() // queue under test
	ref := &stressHeap{}   // reference Go heap
	heap.Init(ref)

	// Track available handles. QuantumQueue has exactly CapItems.
	free := make([]Handle, CapItems)
	for i := range free {
		free[i] = Handle(i)
	}

	// live tracks which handles are actively in the queue
	live := make(map[Handle]bool)

	// Sequence number increments per insert to break tie ordering
	seq := 0

	// makeVal produces a unique [48]byte payload for a given seed.
	// Ensures deterministic contents per tick or sequence number.
	makeVal := func(seed int64) *[48]byte {
		var b [48]byte
		for i := range b {
			b[i] = byte((seed + int64(i)) & 0xFF)
		}
		return &b
	}

	// Main test loop: randomly apply push, move, or pop operations
	for i := 0; i < iterations; i++ {
		op := rng.Intn(3)                    // random op: 0=Push, 1=MoveTick, 2=Pop
		tick := int64(rng.Intn(BucketCount)) // pick random tick

		switch op {
		case 0:
			// ---------------------
			// PUSH new handle
			// ---------------------
			if len(free) == 0 {
				continue // skip if out of handles
			}
			h := free[len(free)-1]
			free = free[:len(free)-1]
			val := makeVal(int64(seq))
			q.Push(tick, h, val)
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			live[h] = true
			seq++

		case 1:
			// ---------------------
			// MOVE existing handle
			// ---------------------
			if len(live) == 0 {
				continue // nothing to move
			}
			var h Handle
			for hh := range live {
				h = hh
				break
			}
			q.MoveTick(h, tick)

			// Remove all old versions of this handle from the heap...
			for j := len(*ref) - 1; j >= 0; j-- {
				if (*ref)[j].h == h {
					heap.Remove(ref, j)
				}
			}
			// ...then re-insert with new tick + seq
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			seq++

		case 2:
			// ---------------------
			// PEEK + UNLINK
			// ---------------------
			if q.Empty() {
				continue
			}
			h, poppedTick, _ := q.PeepMin()
			exp := heap.Pop(ref).(*stressItem)
			if exp.h != h || exp.tick != poppedTick {
				t.Fatalf("Mismatch at iter %d: got (h=%v,t=%d); want (h=%v,t=%d)",
					i, h, poppedTick, exp.h, exp.tick)
			}
			q.UnlinkMin(h, poppedTick)
			delete(live, h)
			free = append(free, h)
		}
	}

	// Final sweep: drain remaining entries and confirm match to heap
	for !q.Empty() {
		h, poppedTick, _ := q.PeepMin()
		exp := heap.Pop(ref).(*stressItem)
		if exp.h != h || exp.tick != poppedTick {
			t.Fatalf("Drain mismatch: got (h=%v,t=%d); want (%v,%d)",
				h, poppedTick, exp.h, exp.tick)
		}
		q.UnlinkMin(h, poppedTick)
		delete(live, h)
		free = append(free, h)
	}

	// Final safety check: reference heap must now also be empty
	if ref.Len() != 0 {
		t.Fatalf("Reference heap not empty after drain: %d items left", ref.Len())
	}
}
