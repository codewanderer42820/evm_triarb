// bucketqueue_stress_test.go — randomized stress tests for bucketqueue robustness
// ========================================================================
// This suite executes large volumes of randomized operations to uncover
// edge-case bugs, validate invariants, and cross-check against a reference
// min-heap implementation.

package bucketqueue

import (
	"container/heap"
	"fmt"
	"math/rand"
	"testing"
)

type stressItem struct {
	h    Handle
	tick int64
	seq  int
}

type stressHeap []*stressItem

func (sh stressHeap) Len() int { return len(sh) }
func (sh stressHeap) Less(i, j int) bool {
	if sh[i].tick != sh[j].tick {
		return sh[i].tick < sh[j].tick
	}
	return sh[i].seq > sh[j].seq // LIFO order for same-tick items
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

func TestStressBasicRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(1234))
	q := New()
	handles := make([]Handle, 0, 1<<12)
	ref := &stressHeap{}
	heap.Init(ref)
	seq := 0

	for i := 0; i < 1_000_000; i++ {
		switch rng.Intn(3) {
		case 0: // Push
			fmt.Printf("[%06d] → push\n", i)
			h, err := q.Borrow()
			if err != nil {
				t.Fatalf("[%06d] Borrow error: %v", i, err)
			}
			tick := int64(q.baseTick)
			fmt.Printf("[%06d] pushing tick=%d handle=%v\n", i, tick, h)
			if err := q.Push(tick, h, nil); err != nil {
				t.Fatalf("[%06d] Push error: %v", i, err)
			}
			handles = append(handles, h)
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			seq++
		case 1: // PopMin
			if q.Empty() || ref.Len() == 0 {
				continue
			}
			fmt.Printf("[%06d] ← pop", i)
			h, tick1, _ := q.PopMin()
			fmt.Printf("[%06d] popped handle=%v tick=%d", i, h, tick1)
			if h == Handle(nilIdx) {
				t.Fatalf("[%06d] PopMin returned nilIdx unexpectedly (tick=%d, summary=%064b)", i, tick1, q.summary)
			}
			ri := heap.Pop(ref).(*stressItem)
			if ri.h != h || ri.tick != tick1 {
				t.Fatalf("[%06d] mismatch: got h=%v tick=%d want h=%v tick=%d", i, h, tick1, ri.h, ri.tick)
			}
		case 2: // Update
			if len(handles) == 0 {
				continue
			}
			idx := rng.Intn(len(handles))
			h := handles[idx]
			tick := int64(q.baseTick)
			fmt.Printf("[%06d] ↻ update handle=%v tick=%d", i, h, tick)
			if err := q.Update(tick, h, nil); err != nil {
				fmt.Printf("[%06d] (warn) update skipped: %v", i, err)
				continue
			}
			// Remove any existing entry with the same handle from ref heap
			for j := 0; j < ref.Len(); j++ {
				if (*ref)[j].h == h {
					heap.Remove(ref, j)
					break
				}
			}
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			seq++
		}
	}

	// Drain
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
