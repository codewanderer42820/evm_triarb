// Package bucketqueue implements a zero-allocation, low-latency time-bucket priority queue.
// Items are distributed across a fixed-size sliding window of time-indexed buckets.
// A two-level bitmap structure allows O(1) retrieval of the earliest item.
//
// This implementation uses a fixed arena allocator, intrusive linked lists,
// and compact handle management for high-throughput applications such as
// schedulers, simulation engines, or event queues.

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
	return sh[i].seq > sh[j].seq // later seq wins on equal tick to mimic queue’s stable ordering
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

// TestStressBasicRandom exercises randomised borrow‑push‑update‑pop cycles at massive scale to ensure
// bucketqueue behaviour matches a reference heap under adversarial conditions.
func TestStressBasicRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(1234))
	q := New()
	handles := make([]Handle, 0, 1<<12)
	ref := &stressHeap{}
	heap.Init(ref)
	seq := 0

	for i := 0; i < 10_000_000; i++ {
		switch rng.Intn(3) {
		case 0: // borrow + push
			h, err := q.Borrow()
			if err == nil {
				tick := int64(rng.Intn(numBuckets))
				_ = q.Push(tick, h, nil)
				heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
				handles = append(handles, h)
				seq++
			}
		case 1: // update 50% chance
			if len(handles) > 0 {
				idx := rng.Intn(len(handles))
				h := handles[idx]
				tick := int64(rng.Intn(numBuckets))
				_ = q.Update(tick, h, nil)
				// reference heap update: remove and re‑add
				for j, it := range *ref {
					if it.h == h {
						heap.Remove(ref, j)
						heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
						seq++
						break
					}
				}
			}
		case 2: // pop 25% chance
			if !q.Empty() {
				h, tick1, _ := q.PopMin()
				ri := heap.Pop(ref).(*stressItem)
				if ri.h != h || ri.tick != tick1 {
					t.Fatalf("pop mismatch: got h=%v tick=%d want h=%v tick=%d", h, tick1, ri.h, ri.tick)
				}
			}
		}
	}

	// Drain leftover elements and ensure order matches.
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
