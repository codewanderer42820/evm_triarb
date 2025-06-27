// bucketqueue_bench_test.go benchmarks various hot paths of the Queue
// including allocation, push-pop pairs, intra-bucket updates, and randomized mixes.
//
// Each benchmark isolates a specific access pattern to inform throughput and
// latency under constrained and realistic load profiles.
package bucketqueue

import (
	"math/rand"
	"testing"
)

// BenchmarkBorrowReturn measures Borrow() cost under sustained throughput.
func BenchmarkBorrowReturn(b *testing.B) {
	q := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		q.freeHead = idx32(h) // direct freelist patch avoids full Return()
	}
}

// BenchmarkPushPopSameTick tests push/pop of one item within the same tick bucket.
func BenchmarkPushPopSameTick(b *testing.B) {
	q := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		_ = q.Push(0, h, nil)
		q.PopMin()
	}
}

// BenchmarkPushPopIncrementTick pushes items at increasing ticks and pops immediately.
func BenchmarkPushPopIncrementTick(b *testing.B) {
	q := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		_ = q.Push(int64(i), h, nil)
		q.PopMin()
	}
}

// BenchmarkPushMultipleBuckets spreads pushes across many tick buckets.
func BenchmarkPushMultipleBuckets(b *testing.B) {
	const window = 4096
	q := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		_ = q.Push(int64(i%window), h, nil)
	}
}

// BenchmarkPushCountedBucket triggers the count++ fast-path inside Push().
func BenchmarkPushCountedBucket(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(0, h, nil)
	}
}

// BenchmarkInPlaceUpdate tests Update() where tick stays unchanged.
func BenchmarkInPlaceUpdate(b *testing.B) {
	q := New()
	h, _ := q.Borrow()
	_ = q.Push(0, h, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Update(0, h, nil)
	}
}

// BenchmarkUpdateAfterPop simulates updating an item after popping it once.
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

// BenchmarkRandomMix exercises a probabilistic mixture of Borrow/Push/Update/Pop.
func BenchmarkRandomMix(b *testing.B) {
	const W = 1024
	q := New()
	handles := make([]Handle, 0, W)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch r := rand.Intn(100); {
		case r < 60:
			h, _ := q.Borrow()
			handles = append(handles, h)
			_ = q.Push(int64(i), h, nil)
		case r < 85 && len(handles) > 0:
			h := handles[rand.Intn(len(handles))]
			_ = q.Update(int64(i+1), h, nil)
		default:
			if !q.Empty() {
				q.PopMin()
			}
		}
	}
}
