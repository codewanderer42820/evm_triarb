package quantumqueue

import (
	"math/rand"
	"testing"
	"time"
)

const benchSize = CapItems // one handle per tick slot

// BenchmarkEmpty measures the cost of checking emptiness.
func BenchmarkEmpty(b *testing.B) {
	q := NewQuantumQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Empty()
	}
}

// BenchmarkSize measures the cost of querying size.
func BenchmarkSize(b *testing.B) {
	q := NewQuantumQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Size()
	}
}

// BenchmarkPushUnique benchmarks Push on unique ticks (cold insert).
func BenchmarkPushUnique(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}
	val := make([]byte, 48)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, val)
	}
}

// BenchmarkPushUpdate benchmarks Push for in-place update (same tick).
func BenchmarkPushUpdate(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, make([]byte, 48))
	}
	val := make([]byte, 48)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, val)
	}
}

// BenchmarkPushSameTickZero benchmarks repeated Push on tick 0.
func BenchmarkPushSameTickZero(b *testing.B) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	val := make([]byte, 48)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(0, h, val)
	}
}

// BenchmarkPushSameTickMax benchmarks repeated Push on max tick (CapItems-1).
func BenchmarkPushSameTickMax(b *testing.B) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	val := make([]byte, 48)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(int64(benchSize-1), h, val)
	}
}

// BenchmarkPeepMin benchmarks retrieving the minimum element.
func BenchmarkPeepMin(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, make([]byte, 48))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PeepMin()
	}
}

// BenchmarkUnlinkMin benchmarks removing the minimum element.
func BenchmarkUnlinkMin(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, make([]byte, 48))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, tick, _ := q.PeepMin()
		q.UnlinkMin(h, tick)
		q.Push((tick+benchSize)%CapItems, h, make([]byte, 48))
	}
}

// BenchmarkMoveTick benchmarks moving existing handles to new ticks.
func BenchmarkMoveTick(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, make([]byte, 48))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.MoveTick(h, int64((i+1)%benchSize))
	}
}

// BenchmarkPushRandom benchmarks Push on random ticks.
func BenchmarkPushRandom(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}
	rand.Seed(time.Now().UnixNano())
	ticks := make([]int64, benchSize)
	for i := range ticks {
		ticks[i] = rand.Int63n(benchSize)
	}
	val := make([]byte, 48)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(ticks[i%benchSize], h, val)
	}
}
