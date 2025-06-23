package pairidx

import (
	"fmt"
	"testing"
)

// BenchmarkGetHit measures the performance of successful lookups.
// The map contains one pre-inserted key ("hit"), which is accessed repeatedly.
func BenchmarkGetHit(b *testing.B) {
	m := New()
	m.Put("hit", 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Get("hit")
	}
}

// BenchmarkGetMiss measures performance of failed lookups.
// It attempts to retrieve a key that was never inserted ("miss").
func BenchmarkGetMiss(b *testing.B) {
	m := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Get("miss")
	}
}

// BenchmarkPutOverwrite measures the cost of overwriting a single key repeatedly.
// This hits the hot path for `Put` when the key already exists.
func BenchmarkPutOverwrite(b *testing.B) {
	m := New()
	m.Put("dup", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put("dup", uint32(i))
	}
}

// BenchmarkPutInsert benchmarks cost of inserting new, unique keys.
// It resets the map every `batch` insertions to avoid cluster overflows.
func BenchmarkPutInsert(b *testing.B) {
	const batch = bucketCnt * clustersPerBkt * clusterSlots / 10
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("k_%08d", i)
	}
	b.ResetTimer()

	m := New()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%batch == 0 {
			m = New()
		}
		m.Put(keys[i], uint32(i))
	}
}

// BenchmarkGetSameSlotHit measures `Get` time for a key that always hits
// the first probed slot (best-case locality).
func BenchmarkGetSameSlotHit(b *testing.B) {
	m := New()
	m.Put("slot0", 123) // Intentionally crafted to hit slot 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Get("slot0")
	}
}

// BenchmarkGetSlotMiss benchmarks `Get` on a stream of guaranteed misses.
// Each key is new and never inserted into the map.
func BenchmarkGetSlotMiss(b *testing.B) {
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("miss_%08d", i)
	}
	b.ResetTimer()

	m := New()
	for i := 0; i < b.N; i++ {
		_, _ = m.Get(keys[i])
	}
}

// BenchmarkPutOverwriteHit measures cost of updating an existing key with same value.
// Intended to hit the `atomic.LoadUint32 == val32` fast-path.
func BenchmarkPutOverwriteHit(b *testing.B) {
	m := New()
	m.Put("hit", 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put("hit", 2)
	}
}

// BenchmarkSizeScan measures the time it takes to read the size counter.
// The map is prefilled with half its theoretical capacity.
func BenchmarkSizeScan(b *testing.B) {
	m := New()
	healf := bucketCnt * clustersPerBkt * clusterSlots / 2
	for i := 0; i < healf; i++ {
		m.Put(fmt.Sprintf("size_%d", i), uint32(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Size()
	}
}
