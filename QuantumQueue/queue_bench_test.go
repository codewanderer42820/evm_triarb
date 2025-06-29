// Package quantumqueue benchmarks core operations to catch regressions in performance.
package quantumqueue

import (
	"math/rand"
	"testing"
	"time"
)

const benchSize = CapItems // maximum number of handles in the arena

// BenchmarkEmpty measures cost of the Empty() check only.
func BenchmarkEmpty(b *testing.B) {
	b.ReportAllocs() // track memory allocations
	q := NewQuantumQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Empty()
	}
}

// BenchmarkSize measures cost of the Size() call.
func BenchmarkSize(b *testing.B) {
	b.ReportAllocs()
	q := NewQuantumQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Size()
	}
}

// BenchmarkPushUnique continuously pushes to unique handles in round-robin,
// exercising core push path without trigger for re-linking existing nodes.
func BenchmarkPushUnique(b *testing.B) {
	b.ReportAllocs()
	q := NewQuantumQueue()

	// Pre-borrow all handles to avoid allocation cost during benchmark
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}

	val := make([]byte, 48) // sample payload
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, val)
	}
}

// BenchmarkPushRandom pushes to handles with random ticks,
// simulating non-uniform workloads and exercising update/unlink as needed.
func BenchmarkPushRandom(b *testing.B) {
	b.ReportAllocs()

	// deterministic random generator for reproducibility
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	q := NewQuantumQueue()

	// borrow handles ahead of time
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}

	// prepare random ticks sequence
	ticks := make([]int64, benchSize)
	for i := range ticks {
		ticks[i] = rng.Int63n(benchSize)
	}

	val := make([]byte, 48)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(ticks[i%benchSize], h, val)
	}
}
