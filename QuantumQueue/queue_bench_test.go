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

// BenchmarkUnlinkMin_StableBucket benchmarks UnlinkMin when operating on a stable,
// single-entry bucket. The tick is reused continuously and the bitmap summary
// never changes. This reflects the common-case hot-path in core-pinned routers.
func BenchmarkUnlinkMin_StableBucket(b *testing.B) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	val := new([48]byte)
	q.Push(2048, h, val) // chosen mid-range stable tick

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.UnlinkMin(h, 2048)
		q.Push(2048, h, val)
	}
}

// BenchmarkUnlinkMin_DenseBucket benchmarks UnlinkMin when operating within
// a dense bucket (3 handles pushed to same tick). This ensures the list head
// rotates but the bucket never empties, so bitmap summaries remain untouched.
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

// BenchmarkUnlinkMin_BitmapCollapse benchmarks worst-case unlink behavior,
// where each UnlinkMin empties the bucket and triggers a full bitmap clear
// across group, lane, and bucket summary levels. This represents the cold-path
// edge case and exercises all 3 layers of the bitmap hierarchy.
func BenchmarkUnlinkMin_BitmapCollapse(b *testing.B) {
	q := NewQuantumQueue()
	val := new([48]byte)
	h, _ := q.BorrowSafe()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick := int64(i % BucketCount) // rotate across all tick buckets
		q.Push(tick, h, val)
		q.UnlinkMin(h, tick)
	}
}

// BenchmarkUnlinkMin_ScatterCollapse benchmarks UnlinkMin with randomized ticks.
// This simulates wide churn where handles are constantly inserted into random
// tick positions, forcing worst-case cache churn and frequent bitmap collapses.
// It models dynamic traffic with poor temporal locality.
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

// BenchmarkUnlinkMin_ReinsertAfterCollapse benchmarks a pattern where the
// same tick is reused, but the bucket is fully collapsed on every UnlinkMin.
// This represents a midpoint between the worst-case collapse and steady-state,
// and simulates real tick reuse without handle batching.
func BenchmarkUnlinkMin_ReinsertAfterCollapse(b *testing.B) {
	q := NewQuantumQueue()
	val := new([48]byte)
	h, _ := q.BorrowSafe()
	const tick = 4095 // max tick, stable index

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(tick, h, val)
		q.UnlinkMin(h, tick)
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
