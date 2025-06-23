package pairidx

import (
	"fmt"
	"testing"
)

// BenchmarkGetHit measures successful look-ups.
func BenchmarkGetHit(b *testing.B) {
	m := New()
	m.Put("hit", 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get("hit")
	}
}

// BenchmarkGetMiss measures failed look-ups.
func BenchmarkGetMiss(b *testing.B) {
	m := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get("miss")
	}
}

// BenchmarkPutOverwrite hits the “key already exists” fast-path.
func BenchmarkPutOverwrite(b *testing.B) {
	m := New()
	m.Put("dup", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put("dup", uint32(i))
	}
}

// BenchmarkPutInsert inserts fresh keys; map is reset every `batch`.
func BenchmarkPutInsert(b *testing.B) {
	const batch = bucketCnt * clustersPerBkt * clusterSlots / 4
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("k_%08d", i)
	}

	m := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%batch == 0 {
			m = New()
		}
		m.Put(keys[i], uint32(i))
	}
}

// Best-case slot locality.
func BenchmarkGetSameSlotHit(b *testing.B) {
	m := New()
	m.Put("slot0", 123)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get("slot0")
	}
}

// Stream of guaranteed misses (each key unique).
func BenchmarkGetSlotMiss(b *testing.B) {
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("miss_%08d", i)
	}

	m := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get(keys[i])
	}
}

// Overwrite with identical value (checks update short-circuit).
func BenchmarkPutOverwriteHit(b *testing.B) {
	m := New()
	m.Put("hit", 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put("hit", 1)
	}
}

// Size() hot-path.
func BenchmarkSizeScan(b *testing.B) {
	m := New()
	half := bucketCnt * clustersPerBkt * clusterSlots / 2
	for i := 0; i < half; i++ {
		m.Put(fmt.Sprintf("size_%d", i), uint32(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Size()
	}
}
