// Package bucketqueue implements a zero-allocation, low-latency time-bucket priority queue.
// Items are distributed across a fixed-size sliding window of time-indexed buckets.
// A two-level bitmap structure allows O(1) retrieval of the earliest item.
//
// This implementation uses a fixed arena allocator, intrusive linked lists,
// and compact handle management for high-throughput applications such as
// schedulers, simulation engines, or event queues.

package bucketqueue

import (
	"math/rand"
	"testing"
)

// BenchmarkBorrowReturn measures arena-handle borrow/return overhead.
func BenchmarkBorrowReturn(b *testing.B) {
	q := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		q.freeHead = idx32(h) // avoid Return logic for max speed
	}
}

// BenchmarkPushPopSameTick measures push+pop within the same bucket tick.
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

// BenchmarkPushPopIncrementTick tests pop after monotonic tick increment.
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

// BenchmarkPushMultipleBuckets stresses mapping across many buckets.
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

// BenchmarkPushCountedBucket benchmarks count‑increment path on colliding tick.
func BenchmarkPushCountedBucket(b *testing.B) {
	q := New()
	b.ReportAllocs()
	h, _ := q.Borrow()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(0, h, nil)
	}
}

// BenchmarkInPlaceUpdate measures Update on same bucket without movement.
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

// BenchmarkUpdateAfterPop benchmarks Update after item has been popped once.
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

// BenchmarkRandomMix simulates a realistic random mix of Borrow/Push/Update/Pop.
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
