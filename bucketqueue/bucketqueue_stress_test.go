// bucketqueue_stress_test.go — randomized stress tests for bucketqueue robustness
// ========================================================================
// This suite executes large volumes of randomized operations to uncover
// edge-case bugs, validate invariants, and cross-check against a reference
// min-heap implementation.

package bucketqueue

import (
	"container/heap"
	"flag"
	"fmt"
	"math/rand"
	"testing"
)

// Configuration flags for stress test scale and verbosity
var (
	stressOps   = flag.Int64("stress.ops", 1_000_000, "number of random operations to execute")
	stressDebug = flag.Bool("stress.debug", false, "enable verbose debug logging")
)

// ───── Reference priority-queue used for cross-checking ────────────────
// refItem holds the queue handle, its tick, and a sequence counter to
// resolve same-tick ordering.
type refItem struct {
	h    Handle
	tick uint64
	seq  int
}

type refPQ []*refItem

func (pq refPQ) Len() int { return len(pq) }
func (pq refPQ) Less(i, j int) bool {
	if pq[i].tick != pq[j].tick {
		return pq[i].tick < pq[j].tick
	}
	return pq[i].seq < pq[j].seq
}
func (pq refPQ) Swap(i, j int)       { pq[i], pq[j] = pq[j], pq[i] }
func (pq *refPQ) Push(x interface{}) { *pq = append(*pq, x.(*refItem)) }
func (pq *refPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	it := old[n-1]
	*pq = old[:n-1]
	return it
}

// TestStressRandomPatterns performs stressOps random operations:
//   - push:    borrow a handle and enqueue at current baseTick
//   - pop:     dequeue min and verify against refPQ
//   - update:  duplicate-push existing handle (ref-counter semantics)
//   - remove:  alias to pop for random deletion
//   - jump:    force window to slide by pushing a far-ahead tick
//
// At the end, drains remaining items and asserts refPQ is empty.
func TestStressRandomPatterns(t *testing.T) {
	fmt.Printf("⚙️  stressOps=%d, debug=%v\n", *stressOps, *stressDebug)

	q := New()
	rng := rand.New(rand.NewSource(42))

	live := make([]Handle, 0, 1<<16)
	refHeap := &refPQ{}
	heap.Init(refHeap)
	seq := 0

	// push closure
	push := func() {
		h, err := q.Borrow()
		if err != nil {
			t.Fatalf("Borrow error: %v", err)
		}
		tick := uint64(q.baseTick)
		if err := q.Push(int64(tick), h); err != nil {
			t.Fatalf("Push error: %v", err)
		}
		live = append(live, h)
		heap.Push(refHeap, &refItem{h: h, tick: tick, seq: seq})
		seq++
		if *stressDebug {
			fmt.Printf("→ push (queue=%d, ref=%d)\n", q.Size(), refHeap.Len())
		}
	}

	// pop closure
	pop := func() {
		h, act := q.PopMin()
		if h < 0 {
			t.Fatalf("PopMin returned invalid handle despite non-empty queue")
		}
		refIt := heap.Pop(refHeap).(*refItem)
		exp := refIt.tick
		if uint64(act) != exp {
			t.Fatalf("Tick mismatch: got %d, want %d", act, exp)
		}
		// update live list
		for i, v := range live {
			if v == h {
				live[i] = live[len(live)-1]
				live = live[:len(live)-1]
				break
			}
		}
		if *stressDebug {
			fmt.Printf("← pop (queue=%d, ref=%d)\n", q.Size(), refHeap.Len())
		}
	}

	// update closure
	update := func() {
		h := live[rng.Intn(len(live))]
		tick := uint64(q.baseTick)
		if err := q.Push(int64(tick), h); err != nil {
			t.Fatalf("Update error: %v", err)
		}
		heap.Push(refHeap, &refItem{h: h, tick: tick, seq: seq})
		live = append(live, h)
		seq++
		if *stressDebug {
			fmt.Printf("→ update (queue=%d, ref=%d)\n", q.Size(), refHeap.Len())
		}
	}

	// remove alias
	remove := pop

	// jump closure
	jump := func() {
		h, err := q.Borrow()
		if err != nil {
			t.Fatalf("Borrow error (jump): %v", err)
		}
		farTick := int64(q.baseTick) + numBuckets + 1000
		if err := q.Push(farTick, h); err != nil {
			t.Fatalf("Push error (jump): %v", err)
		}

		// reset state
		*refHeap = refPQ{}
		live = live[:0]
		heap.Push(refHeap, &refItem{h: h, tick: uint64(farTick), seq: seq})
		live = append(live, h)
		seq++
		if *stressDebug {
			fmt.Printf("→ jump (tick=%d, queue=%d)\n", farTick, q.Size())
		}
	}

	// main loop
	for i := int64(0); i < *stressOps; i++ {
		if len(live) == 0 {
			push()
			continue
		}
		switch rng.Intn(5) {
		case 0:
			push()
		case 1:
			pop()
		case 2:
			update()
		case 3:
			remove()
		case 4:
			jump()
		}
	}

	// drain
	for len(live) > 0 {
		pop()
	}
	if refHeap.Len() != 0 {
		t.Fatalf("reference heap not empty: len=%d", refHeap.Len())
	}
	fmt.Printf("✔️  finished %d ops (queue=%d, ref=%d)\n", *stressOps, q.Size(), refHeap.Len())
}

// TestPushNegativeTickStress verifies that pushing at a tick less than baseTick
// returns ErrPastTick, preventing regression into stale buckets.
func TestPushNegativeTickStress(t *testing.T) {
	q := New()
	h, _ := q.Borrow()
	for i := 0; i < numBuckets; i++ {
		_ = q.Push(int64(q.baseTick), h)
		q.PopMin()
	}
	if err := q.Push(int64(q.baseTick)-1, h); err != ErrPastTick {
		t.Fatalf("Expected ErrPastTick for tick before window; got %v", err)
	}
}

// TestPopEmptyStress ensures PopMin on an empty stress build returns zero values
func TestPopEmptyStress(t *testing.T) {
	q := New()
	h, tick := q.PopMin()
	if h != nilIdx || tick != 0 {
		t.Fatalf("Expected nilIdx,0 on empty PopMin; got %v,%d", h, tick)
	}
}

// TestSizeAndEmptyStress ...
func TestSizeAndEmptyStress(t *testing.T) {
	q := New()
	rng := rand.New(rand.NewSource(99))
	count := 0
	for i := 0; i < 10000; i++ {
		if rng.Intn(2) == 0 {
			h, _ := q.Borrow()
			_ = q.Push(int64(q.baseTick), h)
			count++
		} else if !q.Empty() {
			_, _ = q.PopMin()
			count--
		}
		if q.Size() != count || q.Empty() != (count == 0) {
			t.Fatalf("Size/Empty mismatch: got size=%d, empty=%v; want %d/%v", q.Size(), q.Empty(), count, count == 0)
		}
	}
}

// TestRingWrapStress ...
func TestRingWrapStress(t *testing.T) {
	q := New()
	h1, _ := q.Borrow()
	h2, _ := q.Borrow()
	pushOrPanic(t, q, int64(q.baseTick)+1, h1)
	pushOrPanic(t, q, int64(q.baseTick)+2, h2)
	far := int64(q.baseTick) + numBuckets + 5
	pushOrPanic(t, q, far, h1)
	p, _ := q.PopMin()
	if p != h1 {
		t.Fatalf("Expected h1 at wrapped tick; got %v", p)
	}
}

// TestSameTickOrderingStress ...
func TestSameTickOrderingStress(t *testing.T) {
	q := New()
	hA, _ := q.Borrow()
	hB, _ := q.Borrow()
	pushOrPanic(t, q, 0, hA)
	pushOrPanic(t, q, 0, hB)
	p1, _ := q.PopMin()
	p2, _ := q.PopMin()
	if p1 != hB || p2 != hA {
		t.Fatalf("LIFO ordering failed: got %v then %v", p1, p2)
	}
}
