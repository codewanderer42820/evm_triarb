package quantumqueue

import (
	"math/rand"
	"testing"
	"time"
)

/*──────── helpers ───────*/

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

// reportPerOp prints an extra metric already divided by queue-op count.
func reportPerOp(b *testing.B, opsPerIter int64) {
	totalOps := int64(b.N) * opsPerIter
	perOp := float64(b.Elapsed().Nanoseconds()) / float64(totalOps)
	b.ReportMetric(perOp, "queue_ns/op")
}

/*──────── 1. sequential push → pop ───────*/

func BenchmarkPushPopSequential(b *testing.B) {
	const N = 1 << 13
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, h := range hs {
			_ = q.Push(int64(j), h, nil)
		}
		for range hs {
			_, _, _ = q.PopMin()
		}
	}
	reportPerOp(b, int64(2*N))
}

/*──────── 2. random push → pop ───────────*/

func BenchmarkPushPopRandom(b *testing.B) {
	const N = 1 << 13
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, h := range hs {
			_ = q.Push(int64(rng.Intn(1<<13)), h, nil)
		}
		for range hs {
			_, _, _ = q.PopMin()
		}
	}
	reportPerOp(b, int64(2*N))
}

/*──────── 3. duplicate-tick burst ────────*/

func BenchmarkDuplicateBurst(b *testing.B) {
	const Dups = 8
	q := NewQuantumQueue()
	h := mustBorrowB(b, q)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < Dups; j++ {
			_ = q.Push(42, h, nil)
		}
		for j := 0; j < Dups; j++ {
			_, _, _ = q.PopMin()
		}
	}
	reportPerOp(b, int64(2*Dups))
}

/*──────── 4. heavy relocate ─────────────*/

func BenchmarkUpdateRelocate(b *testing.B) {
	const N = 1 << 12
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)
	for i, h := range hs {
		_ = q.Push(int64(i), h, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, h := range hs {
			_ = q.Update(int64(j+8), h, nil)
			_ = q.Update(int64(j), h, nil)
		}
	}
	reportPerOp(b, int64(2*N)) // two Updates per handle
}

/*──────── 5. mixed workload ─────────────*/

func BenchmarkMixedWorkload(b *testing.B) {
	const N = 1
	rng := rand.New(rand.NewSource(1))
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, h := range hs {
			tk := rng.Int63n(256)
			_ = q.Push(tk, h, nil)
			if rng.Intn(4) == 0 {
				_ = q.Push(tk, h, nil)
			}
		}
		for j, h := range hs {
			if j&1 == 0 {
				_ = q.Update(int64(j+128), h, nil)
			}
		}
		for !q.Empty() {
			_, _, _ = q.PopMin()
		}
	}
	opsApprox := int64(N*2 + N/2 + N/4) // push+pop+update estimate
	reportPerOp(b, opsApprox)
}
