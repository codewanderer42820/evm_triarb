// bucketqueue_bench_test.go — micro‑benchmarks for the arena‑backed bucketqueue
// ==================================================================
// Isolates the cost of each core queue operation in tight loops.
// All benchmarks are designed to remain allocation-free under stable conditions.

package bucketqueue

import (
	"math/rand"
	"testing"
)

// seededQueue returns a Queue with one handle pushed at tick=0.
// The handle starts with count==1, ready for repeated fast-path reuse.
func seededQueue() (*Queue, Handle) {
	q := New()
	h, _ := q.Borrow()
	_ = q.Push(0, h)
	return q, h
}

// BenchmarkPush exercises the duplicate-push fast path for count bumping.
func BenchmarkPush(b *testing.B) {
	q, h := seededQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(0, h)
	}
}

// BenchmarkPopMin tests minimal-cost popping of the same handle repeatedly.
func BenchmarkPopMin(b *testing.B) {
	q, h := seededQueue()
	// Preload with (N-1) more refs to match expected pop count.
	for i := 1; i < b.N; i++ {
		_ = q.Push(0, h)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = q.PopMin()
	}
}

// BenchmarkPeepMin measures non-mutating read of the current minimum.
func BenchmarkPeepMin(b *testing.B) {
	q, h := seededQueue()
	// Push a few extra to simulate small bucket load.
	for i := 0; i < 7; i++ {
		_ = q.Push(0, h)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = q.PeepMin()
	}
}

// BenchmarkPushPopCycle tests end-to-end cost of a full push+pop.
func BenchmarkPushPopCycle(b *testing.B) {
	q := New()
	handles := make([]Handle, 1024)
	for i := range handles {
		h, _ := q.Borrow()
		handles[i] = h
	}
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[idx]
		idx = (idx + 1) % len(handles)
		_ = q.Push(0, h)
		_, _ = q.PopMin()
	}
}

// BenchmarkUpdate tests cost of Update when the node is in queue.
func BenchmarkUpdate(b *testing.B) {
	q, h := seededQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Update(1, h)
	}
}

// BenchmarkMixedHeavy tests a random workload: 50% Push, 40% PopMin, 10% Update.
func BenchmarkMixedHeavy(b *testing.B) {
	q := New()
	handles := make([]Handle, 1024)
	for i := range handles {
		h, _ := q.Borrow()
		handles[i] = h
	}
	rng := rand.New(rand.NewSource(1))
	idx := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := rng.Intn(10)
		switch {
		case n < 5:
			h := handles[idx]
			idx = (idx + 1) % len(handles)
			_ = q.Push(0, h)
		case n < 9:
			if !q.Empty() {
				_, _ = q.PopMin()
			}
		default:
			h := handles[idx]
			_ = q.Update(0, h)
		}
	}
}
