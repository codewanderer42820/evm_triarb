package quantumqueue

import (
	"container/heap"
	"math/rand"
	"testing"
)

// stressItem records the expected queue entries in a simple heap.
type stressItem struct {
	h    Handle
	tick int64
	seq  int
}

type stressHeap []*stressItem

func (h stressHeap) Len() int { return len(h) }
func (h stressHeap) Less(i, j int) bool {
	if h[i].tick != h[j].tick {
		return h[i].tick < h[j].tick
	}
	// tie-break by seq: later wins
	return h[i].seq > h[j].seq
}
func (h stressHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *stressHeap) Push(x interface{}) { *h = append(*h, x.(*stressItem)) }
func (h *stressHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	it := old[n]
	*h = old[:n]
	return it
}

// TestStressBasicRandom uses an explicit free list to avoid duplicate borrows,
// and mirrors operations in a reference heap.
func TestStressBasicRandom(t *testing.T) {
	const iterations = 80_000_000

	// initialize free list with all possible handles
	free := make([]Handle, CapItems)
	for i := range free {
		free[i] = Handle(i)
	}

	rng := rand.New(rand.NewSource(42))
	q := NewQuantumQueue()
	ref := &stressHeap{}
	heap.Init(ref)

	// live tracks which handles are currently in queue
	live := make(map[Handle]bool)
	seq := 0

	for i := 0; i < iterations; i++ {
		op := rng.Intn(3)
		tick := int64(rng.Intn(BucketCount))

		switch op {
		case 0:
			// Borrow from free
			if len(free) == 0 {
				continue
			}
			h := free[len(free)-1]
			free = free[:len(free)-1]
			if live[h] {
				t.Fatalf("double-borrow handle %d at iter %d", h, i)
			}
			// Push
			q.Push(tick, h, nil)
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			live[h] = true
			seq++

		case 1:
			// Update existing via MoveTick
			if len(live) == 0 {
				continue
			}
			// pick any live handle
			var h Handle
			for hh := range live {
				h = hh
				break
			}
			q.MoveTick(h, tick)

			// sync reference heap
			for j, it := range *ref {
				if it.h == h {
					heap.Remove(ref, j)
					break
				}
			}
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			seq++

		case 2:
			// Pop
			if q.Empty() {
				continue
			}
			h, poppedTick, _ := q.PeepMin()
			exp := heap.Pop(ref).(*stressItem)
			if exp.h != h || exp.tick != poppedTick {
				t.Fatalf("pop mismatch at iter %d: got (h=%v,t=%d) want (h=%v,t=%d)",
					i, h, poppedTick, exp.h, exp.tick)
			}
			q.UnlinkMin(h, poppedTick)
			// mark free
			delete(live, h)
			free = append(free, h)
		}
	}

	// Drain leftover
	for !q.Empty() {
		h, poppedTick, _ := q.PeepMin()
		exp := heap.Pop(ref).(*stressItem)
		if exp.h != h || exp.tick != poppedTick {
			t.Fatalf("drain mismatch: got (h=%v,t=%d) want (h=%v,t=%d)",
				h, poppedTick, exp.h, exp.tick)
		}
		q.UnlinkMin(h, poppedTick)
		delete(live, h)
		free = append(free, h)
	}

	if ref.Len() != 0 {
		t.Fatalf("reference heap not empty: %d left", ref.Len())
	}
}
