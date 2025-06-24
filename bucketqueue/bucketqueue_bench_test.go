// bucketqueue_bench_test.go — lean and complete benchmark suite for bucketqueue
package bucketqueue

import (
	"math/rand"
	"testing"
)

func BenchmarkBorrowReturn(b *testing.B) {
	q := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		q.freeHead = idx32(h) // avoid Return logic for max speed
	}
}

func BenchmarkPushPopSameTick(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	_ = q.Push(0, h, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(0, h, nil)
		q.PopMin()
	}
}

func BenchmarkPushPopIncrementTick(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(int64(i), h, nil)
		q.PopMin()
	}
}

func BenchmarkPushMultipleBuckets(b *testing.B) {
	q := New()
	handles := make([]Handle, 1024)
	for i := 0; i < 1024; i++ {
		h, _ := q.Borrow()
		_ = q.Push(int64(i), h, nil)
		handles[i] = h
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PopMin()
	}
}

func BenchmarkPushCountedBucket(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	for i := 0; i < 1024; i++ {
		_ = q.Push(99, h, nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PopMin()
	}
}

func BenchmarkInPlaceUpdate(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	_ = q.Push(0, h, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Update(int64(i), h, nil)
	}
}

func BenchmarkUpdateAfterPop(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	_ = q.Push(0, h, nil)
	q.PopMin()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Update(int64(i), h, nil)
	}
}

func BenchmarkRandomMix(b *testing.B) {
	q := New()
	const W = 2048
	handles := make([]Handle, W)
	for i := range handles {
		h, _ := q.Borrow()
		_ = q.Push(int64(i), h, nil)
		handles[i] = h
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch r := rand.Intn(100); {
		case r < 60:
			h, _ := q.Borrow()
			_ = q.Push(int64(i+W), h, nil)
		case r < 85:
			h := handles[rand.Intn(W)]
			_ = q.Update(int64(i+W+1), h, nil)
		default:
			q.PopMin()
		}
	}
}
