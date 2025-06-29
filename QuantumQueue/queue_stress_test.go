// Package quantumqueue contains a long-running stress test to validate
// correctness under extensive randomized operations against a reference heap.
package quantumqueue

import (
	"container/heap"
	"math/rand"
	"testing"
)

// stressItem represents an entry in the heap-based reference model.
// Stores handle, tick, and a sequence number to disambiguate tie-breaks.
type stressItem struct {
	h    Handle // queue handle
	tick int64  // associated tick value
	seq  int    // sequence number to break ties (newer = higher)
}

// stressHeap is a heap of stressItems used as a correctness reference.
type stressHeap []*stressItem

// Len returns the number of elements in the heap.
func (h stressHeap) Len() int { return len(h) }

// Less implements the ordering: lowest tick first, then newest seq (LIFO for equal ticks).
func (h stressHeap) Less(i, j int) bool {
	if h[i].tick != h[j].tick {
		return h[i].tick < h[j].tick
	}
	return h[i].seq > h[j].seq // newer seq wins for insert-at-head match
}

// Swap swaps elements i and j in the heap.
func (h stressHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push appends a new element to the heap.
func (h *stressHeap) Push(x interface{}) { *h = append(*h, x.(*stressItem)) }

// Pop removes and returns the minimum element from the heap.
func (h *stressHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	it := old[n]
	*h = old[:n]
	return it
}

// TestQueueStressRandomOperations runs millions of random push, move, and pop operations,
// comparing the queue against a Go heap to catch any ordering or linking bugs.
func TestQueueStressRandomOperations(t *testing.T) {
	const iterations = 50_000_000

	// Use a fixed seed for deterministic test runs
	rng := rand.New(rand.NewSource(42))
	q := NewQuantumQueue()
	ref := &stressHeap{}
	heap.Init(ref)

	// Prepare pool of free handles and a set of live handles
	free := make([]Handle, CapItems)
	for i := range free {
		free[i] = Handle(i)
	}
	live := make(map[Handle]bool) // tracks handles currently in queue
	seq := 0                      // monotonically increasing sequence number

	for i := 0; i < iterations; i++ {
		op := rng.Intn(3)                    // randomly choose operation: 0=push,1=move,2=pop
		tick := int64(rng.Intn(BucketCount)) // random tick within valid range

		switch op {
		case 0:
			// Push a new handle if available
			if len(free) == 0 {
				continue
			}
			h := free[len(free)-1]
			free = free[:len(free)-1]
			q.Push(tick, h, nil)
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			live[h] = true
			seq++

		case 1:
			// Move tick of an existing live handle
			if len(live) == 0 {
				continue
			}
			// Select any handle from live set
			var h Handle
			for hh := range live {
				h = hh
				break
			}
			q.MoveTick(h, tick)
			// Remove all stale versions of h from reference heap (reverse order)
			for j := len(*ref) - 1; j >= 0; j-- {
				if (*ref)[j].h == h {
					heap.Remove(ref, j)
				}
			}
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			seq++

		case 2:
			// Pop the minimum if queue isn't empty
			if q.Empty() {
				continue
			}
			h, poppedTick, _ := q.PeepMin()
			exp := heap.Pop(ref).(*stressItem)

			// Verify queue behavior matches reference
			if exp.h != h || exp.tick != poppedTick {
				t.Fatalf("Mismatch at iter %d: got (h=%v,t=%d); want (h=%v,t=%d)",
					i, h, poppedTick, exp.h, exp.tick)
			}
			// Remove from queue and return handle to free pool
			q.UnlinkMin(h, poppedTick)
			delete(live, h)
			free = append(free, h)
		}
	}

	// Drain any remaining elements and verify heap/queue stay consistent
	for !q.Empty() {
		h, poppedTick, _ := q.PeepMin()
		exp := heap.Pop(ref).(*stressItem)
		if exp.h != h || exp.tick != poppedTick {
			t.Fatalf("Drain mismatch: got (h=%v,t=%d); want (h=%v,t=%d)",
				h, poppedTick, exp.h, exp.tick)
		}
		q.UnlinkMin(h, poppedTick)
		delete(live, h)
		free = append(free, h)
	}

	// Ensure reference heap is empty
	if ref.Len() != 0 {
		t.Fatalf("Reference heap not empty after drain: %d items left", ref.Len())
	}
}
