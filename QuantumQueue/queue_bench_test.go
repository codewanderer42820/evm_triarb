package quantumqueue

import (
	"testing"
)

var benchPayload = [52]byte{}

// BenchmarkBorrowSafe measures safe handle allocation with exhaustion check.
// Once exhaustion occurs, BorrowSafe returns an error, which we ignore, so this
// loop never panics and measures the common-case path.
func BenchmarkBorrowSafe(b *testing.B) {
	q := NewQuantumQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = q.BorrowSafe()
	}
}

// BenchmarkPush tests pushing into an otherwise-empty queue.
func BenchmarkPush(b *testing.B) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	data := benchPayload[:]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(int64(i%BucketCount), h, data)
	}
}

// BenchmarkPushUnique tests pushing into distinct slots.
func BenchmarkPushUnique(b *testing.B) {
	q := NewQuantumQueue()
	handles := make([]Handle, BucketCount)
	for i := range handles {
		handles[i], _ = q.BorrowSafe()
	}
	data := benchPayload[:]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % BucketCount
		tick := int64(idx)
		q.Push(tick, handles[idx], data)
	}
}

// BenchmarkMoveTick moves M items around to new ticks.
func BenchmarkMoveTick(b *testing.B) {
	const M = 1000
	q := NewQuantumQueue()
	handles := make([]Handle, M)
	for i := 0; i < M; i++ {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, benchPayload[:])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%M]
		newTick := int64((i + 1) % M)
		q.MoveTick(h, newTick)
	}
}

// BenchmarkPeepMin repeatedly peeks the minimum in an N-item queue.
func BenchmarkPeepMin(b *testing.B) {
	const N = 10000
	q := NewQuantumQueue()
	for i := 0; i < N; i++ {
		h, _ := q.BorrowSafe()
		q.Push(int64(i), h, benchPayload[:])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = q.PeepMin()
	}
}

// BenchmarkPeepAndUnlink simulates continuous pop/push at tick 0 using a fixed bucket range.
func BenchmarkPeepAndUnlink(b *testing.B) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(0, h, benchPayload[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Pop current min
		h0, t0, _ := q.PeepMin()
		q.UnlinkMin(h0, t0)
		// Push back into a valid bucket range to avoid out-of-bounds
		newTick := int64(i % BucketCount)
		q.Push(newTick, h0, benchPayload[:])
	}
}
