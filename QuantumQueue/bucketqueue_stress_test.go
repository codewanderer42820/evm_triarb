package quantumqueue

import (
	"container/heap"
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
	// on ties, later seq is considered “newer”
	return sh[i].seq > sh[j].seq
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
	rng := rand.New(rand.NewSource(42))
	q := NewQuantumQueue()
	ref := &stressHeap{}
	heap.Init(ref)
	handles := make([]Handle, 0, 1024)
	seq := 0

	for i := 0; i < 100_000; i++ {
		switch rng.Intn(3) {
		case 0:
			h, err := q.Borrow()
			if err == nil {
				tick := int64(rng.Intn(BucketCount))
				_ = q.Push(tick, h, nil)
				handles = append(handles, h)
				heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
				seq++
			}
		case 1:
			if !q.Empty() {
				h, tick, _ := q.PopMin()
				ri := heap.Pop(ref).(*stressItem)
				if ri.h != h || ri.tick != tick {
					t.Fatalf("pop mismatch: got (%v,%d), want (%v,%d)", h, tick, ri.h, ri.tick)
				}
			}
		default:
			// no-op
		}
	}

	// drain
	for !q.Empty() {
		h, tick, _ := q.PopMin()
		ri := heap.Pop(ref).(*stressItem)
		if ri.h != h || ri.tick != tick {
			t.Fatalf("drain mismatch: got (%v,%d), want (%v,%d)", h, tick, ri.h, ri.tick)
		}
	}
	if ref.Len() != 0 {
		t.Fatalf("reference heap not empty: %d left", ref.Len())
	}
}
