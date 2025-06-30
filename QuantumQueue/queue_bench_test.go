// Package quantumqueue contains microbenchmarks for evaluating performance
// characteristics of the QuantumQueue under hot-path and cold-path conditions.
// These cover tick reuse, bitmap collapse, payload updates, and randomized churn.
package quantumqueue

import (
	"math/rand"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

// Use the full arena capacity (1 entry per tick) for maximal stress
const benchSize = CapItems

// -----------------------------------------------------------------------------
// Metadata Access Benchmarks
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Push Benchmarks
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Read and Pop Benchmarks
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// UnlinkMin Benchmarks (hot vs cold path variants)
// -----------------------------------------------------------------------------

// BenchmarkUnlinkMin_StableBucket benchmarks UnlinkMin on a stable,
// single-entry bucket that never clears summary bits.
func BenchmarkUnlinkMin_StableBucket(b *testing.B) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	val := new([48]byte)
	q.Push(2048, h, val)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.UnlinkMin(h, 2048)
		q.Push(2048, h, val)
	}
}

// BenchmarkUnlinkMin_DenseBucket benchmarks UnlinkMin on a 3-handle bucket
// that remains populated; no summary bit resets triggered.
func BenchmarkUnlinkMin_DenseBucket(b *testing.B) {
	q := NewQuantumQueue()
	var hs [3]Handle
	val := new([48]byte)
	for i := 0; i < 3; i++ {
		hs[i], _ = q.BorrowSafe()
		q.Push(1234, hs[i], val)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := hs[i%3]
		q.UnlinkMin(h, 1234)
		q.Push(1234, h, val)
	}
}

// BenchmarkUnlinkMin_BitmapCollapse benchmarks UnlinkMin with full collapse.
// Each unlink operation empties the bucket, triggering summary clear.
func BenchmarkUnlinkMin_BitmapCollapse(b *testing.B) {
	q := NewQuantumQueue()
	val := new([48]byte)
	h, _ := q.BorrowSafe()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick := int64(i % BucketCount)
		q.Push(tick, h, val)
		q.UnlinkMin(h, tick)
	}
}

// BenchmarkUnlinkMin_ScatterCollapse benchmarks UnlinkMin with randomized tick churn.
// Models dynamic workloads with sparse locality and frequent summary updates.
func BenchmarkUnlinkMin_ScatterCollapse(b *testing.B) {
	q := NewQuantumQueue()
	val := new([48]byte)
	h, _ := q.BorrowSafe()
	ticks := make([]int64, b.N)
	for i := range ticks {
		ticks[i] = int64(rand.Intn(BucketCount))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick := ticks[i]
		q.Push(tick, h, val)
		q.UnlinkMin(h, tick)
	}
}

// BenchmarkUnlinkMin_ReinsertAfterCollapse benchmarks UnlinkMin where the same tick
// is reused but collapsed each time, producing steady collapse + insert pattern.
func BenchmarkUnlinkMin_ReinsertAfterCollapse(b *testing.B) {
	q := NewQuantumQueue()
	val := new([48]byte)
	h, _ := q.BorrowSafe()
	const tick = 4095
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(tick, h, val)
		q.UnlinkMin(h, tick)
	}
}

// -----------------------------------------------------------------------------
// Tick Relocation
// -----------------------------------------------------------------------------

// BenchmarkMoveTick benchmarks relocating existing handles to new ticks.
// Triggers unlinking and relinking for each op.
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
