// bucketqueue_stress_test.go exercises bucketqueue against a high-volume randomized
// sequence of operations to verify correctness under stress.
//
// It uses a reference heap model to ensure pop order, updates, and insertions match
// the priority queue behavior expected under adversarial conditions.
package bucketqueue

import (
	"container/heap"
	"math/rand"
	"testing"
)

// stressItem represents a single tracked entry used by the reference heap.
type stressItem struct {
	h    Handle
	tick int64
	seq  int // to retain insertion order for stability testing
}

// stressHeap is a min-heap implementation used for validating queue order.
type stressHeap []*stressItem

func (sh stressHeap) Len() int { return len(sh) }
func (sh stressHeap) Less(i, j int) bool {
	if sh[i].tick != sh[j].tick {
		return sh[i].tick < sh[j].tick
	}
	return sh[i].seq > sh[j].seq // later seq wins on tie → mimics push overwrites
}
func (sh stressHeap) Swap(i, j int)       { sh[i], sh[j] = sh[j], sh[i] }
func (sh *stressHeap) Push(x interface{}) { *sh = append(*sh, x.(*stressItem)) }
func (sh *stressHeap) Pop() interface{} {
	old := *sh
	n := len(old)
	item := old[n-1]
	*sh = old[:n-1]
	return item
}

// TestStressBasicRandom validates that push/update/pop sequences remain coherent
// with a reference heap under high load and pseudo-random conditions.
func TestStressBasicRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(1234))
	q := New()
	ref := &stressHeap{}
	heap.Init(ref)
	handles := make([]Handle, 0, 1<<12)
	seq := 0

	for i := 0; i < 10_000_000; i++ {
		switch rng.Intn(3) {
		case 0: // Borrow + Push
			h, err := q.Borrow()
			if err == nil {
				tick := int64(rng.Intn(numBuckets))
				_ = q.Push(tick, h, nil)
				handles = append(handles, h)
				heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
				seq++
			}
		case 1: // Update existing
			if len(handles) > 0 {
				idx := rng.Intn(len(handles))
				h := handles[idx]
				tick := int64(rng.Intn(numBuckets))
				_ = q.Update(tick, h, nil)

				for j, it := range *ref {
					if it.h == h {
						heap.Remove(ref, j)
						heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
						seq++
						break
					}
				}
			}
		case 2: // Pop
			if !q.Empty() {
				h, tick1, _ := q.PopMin()
				ri := heap.Pop(ref).(*stressItem)
				if ri.h != h || ri.tick != tick1 {
					t.Fatalf("pop mismatch: got h=%v tick=%d want h=%v tick=%d", h, tick1, ri.h, ri.tick)
				}
			}
		}
	}

	// Final drain validation.
	for !q.Empty() && ref.Len() > 0 {
		h, tick1, _ := q.PopMin()
		ri := heap.Pop(ref).(*stressItem)
		if ri.h != h || ri.tick != tick1 {
			t.Fatalf("drain mismatch: got h=%v tick=%d want h=%v tick=%d", h, tick1, ri.h, ri.tick)
		}
	}

	if ref.Len() != 0 {
		t.Fatalf("reference heap not empty: %d left", ref.Len())
	}
}
