// bucketqueue_bench_test.go — micro-benchmarks for the arena-backed bucketqueue
package bucketqueue

import (
	"math/rand"
	"testing"
)

func seededQueue() (*Queue, Handle) {
	q := New()
	h, _ := q.Borrow()
	_ = q.Push(0, h, nil)
	return q, h
}

// ─── Push (duplicate-count fast path) ──────────────────────────────────────────
func BenchmarkPush(b *testing.B) {
	q, h := seededQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(0, h, nil)
	}
}

// ─── PopMin minimal cost ──────────────────────────────────────────────────────
func BenchmarkPopMin(b *testing.B) {
	q, h := seededQueue()
	for i := 1; i < b.N; i++ {
		_ = q.Push(0, h, nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = q.PopMin()
	}
}

// ─── PeepMin (read-only) ──────────────────────────────────────────────────────
func BenchmarkPeepMin(b *testing.B) {
	q, h := seededQueue()
	for i := 0; i < 7; i++ {
		_ = q.Push(0, h, nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = q.PeepMin()
	}
}

// ─── Push+Pop cycle ───────────────────────────────────────────────────────────
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
		_ = q.Push(0, h, nil)
		_, _, _ = q.PopMin()
	}
}

// ─── Update while active ──────────────────────────────────────────────────────
func BenchmarkUpdate(b *testing.B) {
	q, h := seededQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Update(1, h, nil)
	}
}

// ─── Mixed workload (50 % Push, 40 % Pop, 10 % Update) ────────────────────────
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
		switch n := rng.Intn(10); {
		case n < 5:
			h := handles[idx]
			idx = (idx + 1) % len(handles)
			_ = q.Push(0, h, nil)
		case n < 9:
			if !q.Empty() {
				_, _, _ = q.PopMin()
			}
		default:
			h := handles[idx]
			_ = q.Update(0, h, nil)
		}
	}
}
