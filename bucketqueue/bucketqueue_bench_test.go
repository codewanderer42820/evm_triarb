package bucketqueue

import (
	"math/rand"
	"testing"
)

// Borrow + Return (allocator loop)
func BenchmarkBorrowReturn(b *testing.B) {
	q := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		_ = q.Return(h)
	}
}

// Push same tick repeatedly (duplicate path)
func BenchmarkPushDuplicate(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	_ = q.Push(42, h, nil) // prime duplicate
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(42, h, nil)
	}
}

// Push increasing tick, immediate PopMin (insert+remove tight loop)
func BenchmarkPushPopCycle(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(int64(i), h, nil)
		q.PopMin()
	}
}

// Push to unique buckets, then PopMin (pure PopMin stress)
func BenchmarkPopMinSingle(b *testing.B) {
	q := New()
	for i := 0; i < 1024; i++ {
		h, _ := q.Borrow()
		_ = q.Push(int64(i), h, nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PopMin()
	}
}

// Push large count to single bucket, then PopMin (count-decrement path)
func BenchmarkPopMinDuplicate(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	_ = q.Push(99, h, nil)
	for i := 0; i < 1023; i++ {
		_ = q.Push(99, h, nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PopMin()
	}
}

// Peek at min repeatedly without popping
func BenchmarkPeepMin(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	_ = q.Push(999, h, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = q.PeepMin()
	}
}

// Update tick in-place (detach + re-insert)
func BenchmarkUpdate(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	_ = q.Push(0, h, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		_ = q.Update(int64(i), h, nil)
	}
}

// Random mix of Push, Update, PopMin
func BenchmarkMixedHeavy(b *testing.B) {
	q := New()
	const W = 8192
	handles := make([]Handle, W)
	for i := 0; i < W; i++ {
		h, _ := q.Borrow()
		_ = q.Push(int64(i), h, nil)
		handles[i] = h
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch r := rand.Intn(100); {
		case r < 70:
			h, _ := q.Borrow()
			_ = q.Push(int64(i+W), h, nil)
		case r < 90:
			h := handles[rand.Intn(W)]
			_ = q.Update(int64(i+W+1), h, nil)
		default:
			q.PopMin()
		}
	}
}
