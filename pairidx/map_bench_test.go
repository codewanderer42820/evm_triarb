package pairidx

import (
	"fmt"
	"sync"
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
		_ = m.size
	}
}

// BenchmarkMixedTraffic25 keeps the table ≤25 % full and mixes operations
// to mimic real traffic: ~50 % inserts, 40 % gets, 10 % overwrites.
func BenchmarkMixedTraffic25(b *testing.B) {
	const (
		capacity    = bucketCnt * clustersPerBkt * clusterSlots
		maxLoad     = capacity / 4 // 25 % utilisation
		getEvery    = 5            // 40 % reads
		updateEvery = 10           // 10 % overwrites
	)

	// Pre-generate keys so we time only map ops, not fmt.Sprintf.
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("k_%08d", i)
	}

	m := New()
	size := 0 // unique entries currently in the map

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		/* ------------- 40 % Get hits ------------- */
		if i%getEvery == 0 && size > 0 {
			_ = m.Get(keys[i%size])
			continue
		}

		/* ------------- Put / overwrite ----------- */
		keyIdx := i
		if i%updateEvery == 0 && size > 0 { // 10 % overwrite
			keyIdx = i % size
		}
		m.Put(keys[keyIdx], uint32(i))
		if keyIdx == i { // counted only on new insert
			size++
		}

		/* ------------- Reset when hitting 25 % load ------------- */
		if size >= maxLoad {
			b.StopTimer()
			m = New()
			size = 0
			b.StartTimer()
		}
	}
}

/* ---------- Reader-contention micro-benchmark ------------------------ */

func BenchmarkReaders8Contention(b *testing.B) {
	m := New()
	for i := 0; i < 1_000; i++ {
		m.Put(fmt.Sprintf("k%04d", i), uint32(i))
	}

	var wg sync.WaitGroup
	readers := 8
	b.ResetTimer()
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < b.N/readers; j++ {
				_ = m.Get(fmt.Sprintf("k%04d", j%1000))
			}
		}(i)
	}
	wg.Wait()
}
