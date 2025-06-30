package quantumqueue

import (
	"math/rand"
	"testing"
	"time"
)

// Use the full arena capacity (1 entry per tick) for maximal stress
const benchSize = CapItems

// BenchmarkEmpty measures the overhead of calling Empty().
// This is a constant-time operation reading a field.
func BenchmarkEmpty(b *testing.B) {
	q := NewQuantumQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Empty()
	}
}

// BenchmarkSize measures the cost of calling Size().
// This is also a constant-time operation reading an int field.
func BenchmarkSize(b *testing.B) {
	q := NewQuantumQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Size()
	}
}

// BenchmarkPushUnique benchmarks Push on unique tick values.
// Each tick is used exactly once, simulating cold inserts.
func BenchmarkPushUnique(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}
	var valArr [48]byte
	val := &valArr
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, val)
	}
}

// BenchmarkPushUpdate benchmarks Push on the same tick repeatedly.
// This hits the fast-path: no unlinking, just payload update.
func BenchmarkPushUpdate(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, new([48]byte))
	}
	val := new([48]byte)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, val)
	}
}

// BenchmarkPushSameTickZero benchmarks repeated updates on tick 0.
// This simulates multiple updates on the same tick (constant key).
func BenchmarkPushSameTickZero(b *testing.B) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	val := new([48]byte)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(0, h, val)
	}
}

// BenchmarkPushSameTickMax benchmarks Push on the maximum tick value.
// This tests the edge of the tick range (CapItems - 1).
func BenchmarkPushSameTickMax(b *testing.B) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	val := new([48]byte)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(int64(benchSize-1), h, val)
	}
}

// BenchmarkPeepMin benchmarks the cost of retrieving the minimum item.
// Assumes queue is fully populated with unique, increasing tick values.
func BenchmarkPeepMin(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, new([48]byte))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PeepMin()
	}
}

// BenchmarkUnlinkMin benchmarks repeated PeepMin + UnlinkMin + Push.
// Simulates continuous consumption of the min and recycling.
func BenchmarkUnlinkMin(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, new([48]byte))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, tick, _ := q.PeepMin()
		q.UnlinkMin(h, tick)
		q.Push((tick+benchSize)%CapItems, h, new([48]byte))
	}
}

// BenchmarkMoveTick benchmarks relocating existing handles to new ticks.
// Tests unlinking from one bucket and relinking into another.
func BenchmarkMoveTick(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, new([48]byte))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.MoveTick(h, int64((i+1)%benchSize))
	}
}

// BenchmarkPushRandom benchmarks Push on randomly selected tick values.
// This simulates unordered, bursty traffic with no locality.
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
	var valArr [48]byte
	val := &valArr
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(ticks[i%benchSize], h, val)
	}
}
