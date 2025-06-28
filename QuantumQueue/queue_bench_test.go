package quantumqueue

import (
	"math/rand"
	"testing"
	"time"
)

/*───────────────── small helpers ─────────────────*/

// mustBorrowB is the benchmark-safe variant of mustBorrow.
func mustBorrowB(b *testing.B, q *QuantumQueue) Handle {
	h, err := q.Borrow()
	if err != nil {
		b.Fatalf("Borrow failed: %v", err)
	}
	return h
}

// borrowManyB pre-allocates N handles for a benchmark iteration.
func borrowManyB(b *testing.B, q *QuantumQueue, n int) []Handle {
	hs := make([]Handle, n)
	for i := 0; i < n; i++ {
		hs[i] = mustBorrowB(b, q)
	}
	return hs
}

/*───────────────── Benchmarks ────────────────────*/

// 1. Sequential tick push → pop
func BenchmarkPushPopSequential(b *testing.B) {
	const N = 1 << 15
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
}

// 2. Random tick push → pop
func BenchmarkPushPopRandom(b *testing.B) {
	const N = 1 << 15
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, h := range hs {
			_ = q.Push(int64(rng.Intn(1<<15)), h, nil)
		}
		for range hs {
			_, _, _ = q.PopMin()
		}
	}
}

// 3. Duplicate-tick burst (same handle, same tick)
func BenchmarkDuplicateBurst(b *testing.B) {
	const Dups = 16
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
}

// 4. Heavy relocate (Update) traffic
func BenchmarkUpdateRelocate(b *testing.B) {
	const N = 1 << 14
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)

	// initial insert
	for i, h := range hs {
		_ = q.Push(int64(i), h, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, h := range hs {
			_ = q.Update(int64(j+8), h, nil) // forward
			_ = q.Update(int64(j), h, nil)   // back
		}
	}
}

// 5. Mixed workload (push, update, duplicate, pop)
func BenchmarkMixedWorkload(b *testing.B) {
	const N = 1 << 14
	rng := rand.New(rand.NewSource(1))
	q := NewQuantumQueue()
	hs := borrowManyB(b, q, N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// random pushes with 25 % duplicates
		for _, h := range hs {
			tk := rng.Int63n(256)
			_ = q.Push(tk, h, nil)
			if rng.Intn(4) == 0 {
				_ = q.Push(tk, h, nil)
			}
		}
		// relocate half the handles
		for j, h := range hs {
			if j&1 == 0 {
				_ = q.Update(int64(j+128), h, nil)
			}
		}
		// drain queue
		for !q.Empty() {
			_, _, _ = q.PopMin()
		}
	}
}
