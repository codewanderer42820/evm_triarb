package quantumqueue

import (
	"testing"
)

func BenchmarkBorrowReturn(b *testing.B) {
	q := NewQuantumQueue()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		// return to freelist with minimal overhead
		idx := idx32(h)
		q.arena[idx].next = q.freeHead
		q.freeHead = idx
	}
}

func BenchmarkPushPopSameTick(b *testing.B) {
	q := NewQuantumQueue()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		_ = q.Push(0, h, nil)
		q.PopMin()
	}
}

func BenchmarkPushPopIncrementTick(b *testing.B) {
	q := NewQuantumQueue()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		_ = q.Push(int64(i), h, nil)
		q.PopMin()
	}
}

func BenchmarkPushMultipleBuckets(b *testing.B) {
	const window = 4096
	q := NewQuantumQueue()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := q.Borrow()
		_ = q.Push(int64(i%window), h, nil)
	}
}

func BenchmarkPushCountedBucket(b *testing.B) {
	q := NewQuantumQueue()
	h, _ := q.Borrow()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(0, h, nil)
	}
}
