package quantumqueue

import (
	"math/rand"
	"testing"
	"time"
)

func mustBorrowB(b *testing.B, q *QuantumQueue) Handle {
	h, err := q.Borrow()
	if err != nil {
		b.Fatalf("Borrow failed: %v", err)
	}
	return h
}

func borrowManyB(b *testing.B, q *QuantumQueue, n int) []Handle {
	hs := make([]Handle, n)
	for i := 0; i < n; i++ {
		hs[i] = mustBorrowB(b, q)
	}
	return hs
}

func reportPerOp(b *testing.B, opsPerIter int64) {
	totalOps := int64(b.N) * opsPerIter
	perOp := float64(b.Elapsed().Nanoseconds()) / float64(totalOps)
	b.ReportMetric(perOp, "queue_ns/op")
}

func BenchmarkPushPopSequential(b *testing.B) {
	const N = 1 << 13
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, h := range hs {
			q.Push(int64(j), h, nil)
		}
		for range hs {
			_, _, _ = q.PopMinSafe()
		}
	}
	reportPerOp(b, int64(2*N))
}

func BenchmarkPushPopRandom(b *testing.B) {
	const N = 1 << 13
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, h := range hs {
			q.Push(int64(rng.Intn(1<<13)), h, nil)
		}
		for range hs {
			_, _, _ = q.PopMinSafe()
		}
	}
	reportPerOp(b, int64(2*N))
}

func BenchmarkDuplicateBurst(b *testing.B) {
	const Dups = 8
	q := NewQuantumQueue()
	h := mustBorrowB(b, q)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < Dups; j++ {
			q.Push(42, h, nil)
		}
		for j := 0; j < Dups; j++ {
			_, _, _ = q.PopMinSafe()
		}
	}
	reportPerOp(b, int64(2*Dups))
}

func BenchmarkUpdateRelocate(b *testing.B) {
	const N = 1 << 12
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)
	for i, h := range hs {
		q.Push(int64(i), h, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, h := range hs {
			q.Update(int64(j+8), h, nil)
			q.Update(int64(j), h, nil)
		}
	}
	reportPerOp(b, int64(2*N))
}

func BenchmarkMixedWorkload(b *testing.B) {
	const N = 128

	rng := rand.New(rand.NewSource(1))
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, h := range hs {
			tk := rng.Int63n(256)
			q.Push(tk, h, nil)
			if j&3 == 0 {
				q.Push(tk, h, nil)
			}
		}
		for j, h := range hs {
			if j&1 == 0 {
				q.Update(int64(j+64), h, nil)
			}
		}
		for j := 0; j < N; j++ {
			_, _, _ = q.PopMinSafe()
		}
	}
	reportPerOp(b, int64(N*3))
}
