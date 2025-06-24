// bucketqueue_stress_test.go — randomized stress tests for bucketqueue robustness
package bucketqueue

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
	rng := rand.New(rand.NewSource(1234))
	q := New()
	handles := make([]Handle, 0, 1<<12)
	ref := &stressHeap{}
	heap.Init(ref)
	seq := 0

	for i := 0; i < 100_000_000; i++ {
		switch rng.Intn(3) {
		case 0:
			h, err := q.Borrow()
			if err != nil {
				t.Fatalf("[%06d] Borrow error: %v", i, err)
			}
			tick := int64(q.baseTick)
			if err := q.Push(tick, h, nil); err != nil {
				t.Fatalf("[%06d] Push error: %v", i, err)
			}
			handles = append(handles, h)
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			seq++
		case 1:
			if q.Empty() || ref.Len() == 0 {
				continue
			}
			h, tick1, _ := q.PopMin()
			if h == Handle(nilIdx) {
				t.Fatalf("[%06d] PopMin returned nilIdx (tick=%d, summary=%064b)", i, tick1, q.summary)
			}
			ri := heap.Pop(ref).(*stressItem)
			if ri.h != h || ri.tick != tick1 {
				t.Fatalf("[%06d] mismatch: got h=%v tick=%d want h=%v tick=%d", i, h, tick1, ri.h, ri.tick)
			}
		case 2:
			if len(handles) == 0 {
				continue
			}
			h := handles[rng.Intn(len(handles))]
			tick := int64(q.baseTick)
			if err := q.Update(tick, h, nil); err != nil {
				continue
			}
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
