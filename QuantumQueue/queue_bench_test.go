// -------------------------
// File: queue_bench_test.go
// -------------------------
package quantumqueue

import (
	"math/rand"
	"testing"
	"time"
)

func mustBorrow(_ *testing.B, q *QuantumQueue) Handle {
	h, _ := q.Borrow()
	return h
}

func borrowMany(b *testing.B, q *QuantumQueue, n int) []Handle {
	hs := make([]Handle, n)
	for i := range hs {
		hs[i] = mustBorrow(b, q)
	}
	return hs
}

// popAllSafe removes up to count items using PeepMinSafe + UnlinkMin
func popAllSafe(q *QuantumQueue, count int) {
	for i := 0; i < count; i++ {
		h, tk, data := q.PeepMinSafe()
		if data == nil {
			return
		}
		q.UnlinkMin(h, tk)
	}
}

func BenchmarkPushPopSequential(b *testing.B) {
	const N = 1 << 13
	q := NewQuantumQueue()
	hs := borrowMany(b, q, N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Push sequential ticks
		for j, h := range hs {
			q.Push(int64(j), h, nil)
		}
		// Pop all safely
		popAllSafe(q, N)
	}
	totalOps := int64(2 * N)
	perOp := float64(b.Elapsed().Nanoseconds()) / float64(int64(b.N)*totalOps)
	b.ReportMetric(perOp, "queue_ns/op")
}

func BenchmarkPushPopRandom(b *testing.B) {
	const N = 1 << 13
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	q := NewQuantumQueue()
	hs := borrowMany(b, q, N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Push random ticks
		for _, h := range hs {
			q.Push(int64(rng.Intn(1<<13)), h, nil)
		}
		// Pop all safely
		popAllSafe(q, N)
	}
	totalOps := int64(2 * N)
	perOp := float64(b.Elapsed().Nanoseconds()) / float64(int64(b.N)*totalOps)
	b.ReportMetric(perOp, "queue_ns/op")
}

func BenchmarkDuplicateBurst(b *testing.B) {
	const Dups = 8
	q := NewQuantumQueue()
	h := mustBorrow(b, q)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Push duplicates
		for j := 0; j < Dups; j++ {
			q.Push(42, h, nil)
		}
		// Pop Dups items safely
		popAllSafe(q, Dups)
	}
	totalOps := int64(2 * Dups)
	perOp := float64(b.Elapsed().Nanoseconds()) / float64(int64(b.N)*totalOps)
	b.ReportMetric(perOp, "queue_ns/op")
}

func BenchmarkUpdateRelocate(b *testing.B) {
	const N = 1 << 12
	q := NewQuantumQueue()
	hs := borrowMany(b, q, N)
	// Pre-populate queue
	for i, h := range hs {
		q.Push(int64(i), h, nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Relocate each element twice
		for j, h := range hs {
			q.Push(int64(j+8), h, nil)
			q.Push(int64(j), h, nil)
		}
		// Pop all safely
		popAllSafe(q, N)
	}
	totalOps := int64(2 * N)
	perOp := float64(b.Elapsed().Nanoseconds()) / float64(int64(b.N)*totalOps)
	b.ReportMetric(perOp, "queue_ns/op")
}

func BenchmarkMixedWorkload(b *testing.B) {
	const N = 128
	rng := rand.New(rand.NewSource(1))
	q := NewQuantumQueue()
	hs := borrowMany(b, q, N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		totalPush := 0
		// Mixed pushes
		for j, h := range hs {
			tk := rng.Int63n(256)
			q.Push(tk, h, nil)
			totalPush++
			if j&3 == 0 {
				q.Push(tk, h, nil)
				totalPush++
			}
		}
		for j, h := range hs {
			if j&1 == 0 {
				q.Push(int64(j+64), h, nil)
				totalPush++
			}
		}
		// Pop all safely
		popAllSafe(q, totalPush)
	}
	totalOps := int64(N * 3)
	perOp := float64(b.Elapsed().Nanoseconds()) / float64(int64(b.N)*totalOps)
	b.ReportMetric(perOp, "queue_ns/op")
}
