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

	rng := rand.New(rand.NewSource(42))
	q := NewQuantumQueue()
	ref := &stressHeap{}
	heap.Init(ref)

	free := make([]Handle, CapItems)
	for i := range free {
		free[i] = Handle(i)
	}
	live := make(map[Handle]bool)
	seq := 0

	// Helper to make deterministic payload
	makeVal := func(seed int64) *[48]byte {
		var b [48]byte
		for i := range b {
			b[i] = byte((seed + int64(i)) & 0xFF)
		}
		return &b
	}

	for i := 0; i < iterations; i++ {
		op := rng.Intn(3)
		tick := int64(rng.Intn(BucketCount))

		switch op {
		case 0:
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
			if len(live) == 0 {
				continue
			}
			var h Handle
			for hh := range live {
				h = hh
				break
			}
			q.MoveTick(h, tick)
			for j := len(*ref) - 1; j >= 0; j-- {
				if (*ref)[j].h == h {
					heap.Remove(ref, j)
				}
			}
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			seq++

		case 2:
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

	if ref.Len() != 0 {
		t.Fatalf("Reference heap not empty after drain: %d items left", ref.Len())
	}
}
