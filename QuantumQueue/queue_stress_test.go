// Package quantumqueue contains a long-running stress test to validate
// correctness under extensive randomized operations against a reference heap.
package quantumqueue

import (
	"container/heap"
	"math/rand"
	"testing"
)

// stressItem represents an entry in the heap-based reference model.
// It stores a queue handle, its tick value, and a sequence number to break ties
// ensuring LIFO order for equal ticks (newer items win).
type stressItem struct {
	h    Handle // handle into the queue arena
	tick int64  // tick key used for ordering
	seq  int    // sequence number for tie-breaking (higher = newer)
}

// stressHeap implements heap.Interface for stressItem pointers.
type stressHeap []*stressItem

// Len returns the number of items in the heap.
func (h stressHeap) Len() int { return len(h) }

// Less orders by lowest tick first; when ticks equal, larger seq (newer) comes first.
func (h stressHeap) Less(i, j int) bool {
	if h[i].tick != h[j].tick {
		return h[i].tick < h[j].tick
	}
	// For identical ticks, place newer item (higher seq) earlier
	return h[i].seq > h[j].seq
}

// Swap swaps two elements in the heap slice.
func (h stressHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds a new stressItem to the heap.
func (h *stressHeap) Push(x interface{}) {
	*h = append(*h, x.(*stressItem))
}

// Pop removes and returns the smallest stressItem (by Less ordering).
func (h *stressHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	it := old[n]
	*h = old[:n]
	return it
}

// TestQueueStressRandomOperations runs a billion randomized operations on the
// QuantumQueue and a Go heap reference, verifying that peek/unlink produce
// identical sequences under push, move, and pop operations.
func TestQueueStressRandomOperations(t *testing.T) {
	const iterations = 10_000_000

	// deterministic RNG for repeatable test
	rng := rand.New(rand.NewSource(69))
	q := NewQuantumQueue() // the queue under test
	ref := &stressHeap{}   // reference heap
	heap.Init(ref)         // initialize heap structure

	// Prepopulate free handles [0 .. CapItems)
	free := make([]Handle, CapItems)
	for i := range free {
		free[i] = Handle(i)
	}
	// Track which handles are live in the queue
	live := make(map[Handle]bool)
	seq := 0 // global sequence counter for tie-breaking

	// makeVal deterministically fills a 48-byte payload from a seed
	makeVal := func(seed int64) *[48]byte {
		var b [48]byte
		for i := range b {
			b[i] = byte((seed + int64(i)) & 0xFF)
		}
		return &b
	}

	// Main loop: randomly push, move, or pop
	for i := 0; i < iterations; i++ {
		op := rng.Intn(3)
		tick := int64(rng.Intn(BucketCount))

		switch op {
		case 0:
			// Push: borrow from free, push into queue and reference heap
			if len(free) == 0 {
				continue
			}
			h := free[len(free)-1]
			free = free[:len(free)-1]
			val := makeVal(int64(seq))
			q.Push(tick, h, val)
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			live[h] = true
			seq++

		case 1:
			// MoveTick: pick any live handle, update its tick in both structures
			if len(live) == 0 {
				continue
			}
			var h Handle
			for hh := range live {
				h = hh
				break
			}
			q.MoveTick(h, tick)
			// Remove old entry in ref heap then re-push with new seq
			for j := len(*ref) - 1; j >= 0; j-- {
				if (*ref)[j].h == h {
					heap.Remove(ref, j)
				}
			}
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			seq++

		case 2:
			// Pop/Unlink: compare PeepMin and UnlinkMin against reference heap
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

	// Drain any remaining items, verifying final ordering
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

	// Final check: reference heap must also be empty
	if ref.Len() != 0 {
		t.Fatalf("Reference heap not empty after drain: %d items left", ref.Len())
	}
}
