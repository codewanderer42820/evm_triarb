// bucketqueue_bench_test.go — micro‑benchmarks for the arena‑backed bucketqueue
// ==================================================================
// This file defines benchmarks isolating the cost of each core operation
// in the bucketqueue implementation. All benchmarks are allocation‑free
// and focus on a single hot path under controlled preconditions.

package bucketqueue

import (
	"math/rand"
	"testing"
)

// seededQueue returns a Queue with one borrowed handle already pushed at tick=0.
// The handle `h` has its internal count==1, ready for duplicate‑push tests.
func seededQueue() (*Queue, Handle) {
	q := New()         // create a fresh queue
	h, _ := q.Borrow() // borrow one handle from the arena
	_ = q.Push(0, h)   // push it at tick=0; count becomes 1
	return q, h        // return queue and handle for reuse
}

// BenchmarkPush measures the cost of repeatedly pushing the same handle
// into the same tick bucket. We exercise the duplicate‑push fast path,
// which increments the ref‑counter and does minimal work. No allocations
// occur, and no arena pressure.
func BenchmarkPush(b *testing.B) {
	q, h := seededQueue() // prepare queue and handle (count==1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(0, h) // duplicate‑push path: just bump count
	}
	b.StopTimer()
}

// BenchmarkPopMin measures the cost of popping the minimum element when
// the handle has count==b.N. We preload the arena through legitimate
// duplicate-pushes so PopMin can run in a tight loop.
func BenchmarkPopMin(b *testing.B) {
	q, h := seededQueue()
	// Push (b.N-1) additional duplicates into tick 0
	for i := 1; i < b.N; i++ {
		_ = q.Push(0, h) // fast-path: just bumps ref-counter
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = q.PopMin()
	}
}

// BenchmarkPushPopCycle alternates Push and PopMin on a small working set
// of borrowed handles. This mimics a steady‑state load without arena
// exhaustion, measuring combined cost of each cycle.
func BenchmarkPushPopCycle(b *testing.B) {
	q := New()
	// pre‑borrow a set of handles to avoid arena operations in the loop
	handles := make([]Handle, 0, 1024)
	for i := 0; i < 1024; i++ {
		h, _ := q.Borrow()
		handles = append(handles, h)
	}
	idx := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[idx]
		idx++
		if idx == len(handles) {
			idx = 0
		}
		_ = q.Push(0, h)  // push at tick=0
		_, _ = q.PopMin() // pop the smallest tick
	}
}

// BenchmarkUpdate measures the cost of calling Update(), which is
// equivalent to Remove+Push under the hot path when the node is
// already in the queue. We exercise the duplicate‑push removal path.
func BenchmarkUpdate(b *testing.B) {
	q, h := seededQueue()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Update(1, h) // move handle from tick=0 to tick=1
	}
}

// BenchmarkMixedHeavy simulates a random mix: 50% Push, 40% Pop, 10% Update.
// We use a fixed‑seed RNG for reproducibility. Operations on an empty queue
// skip pops. This measures realistic workloads with branching.
func BenchmarkMixedHeavy(b *testing.B) {
	q := New()
	handles := make([]Handle, 0, 1024)
	for i := 0; i < 1024; i++ {
		h, _ := q.Borrow()
		handles = append(handles, h)
	}
	rng := rand.New(rand.NewSource(1))
	idx := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := rng.Intn(10)
		switch {
		case n < 5: // 50% pushes: reuse handles in circular buffer
			h := handles[idx]
			idx = (idx + 1) % len(handles)
			_ = q.Push(0, h)

		case n < 9: // 40% pops: only when non‑empty
			if !q.Empty() {
				_, _ = q.PopMin()
			}

		default: // 10% updates: bump existing handle
			h := handles[idx]
			_ = q.Update(0, h)
		}
	}
}
