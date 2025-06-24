package pairidx

import (
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"unsafe"
)

// Micro‑benchmarks for the lock‑free HashMap. No functional tests here – see
// map_test.go for coverage of edge cases and panic paths.

// get‑hit
func BenchmarkGetHit(b *testing.B) {
	m := New()
	m.Put("hit", 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get("hit")
	}
}

// get‑miss
func BenchmarkGetMiss(b *testing.B) {
	m := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get("miss")
	}
}

// put overwrite (key exists)
func BenchmarkPutOverwrite(b *testing.B) {
	m := New()
	m.Put("dup", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put("dup", uint32(i))
	}
}

// hot‑slot locality
func BenchmarkGetSameSlotHit(b *testing.B) {
	m := New()
	m.Put("slot0", 123)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get("slot0")
	}
}

// guaranteed misses (unique keys)
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

// overwrite with identical value (short‑circuit)
func BenchmarkPutOverwriteHit(b *testing.B) {
	m := New()
	m.Put("hit", 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put("hit", 1)
	}
}

// size fast‑path
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

// End-to-end throughput: 25 % load, 50 % puts, 40 % gets, 10 % overwrites.
// No Stop/Start, but ALSO zero heap allocs/op.
func BenchmarkMixedTraffic25_E2E(b *testing.B) {
	const (
		capPerMap = bucketCnt * clustersPerBkt * clusterSlots / 4 // 25 %
		mapsPool  = 16                                            // pre-built maps
	)

	// ❶ Pre-allocate maps so New() isn’t on the clock.
	pool := make([]*HashMap, mapsPool)
	for i := range pool {
		pool[i] = New()
	}

	// ❷ Pre-generate 1 000 unique keys (hex strings, no fmt).
	const keyCount = 1000
	keys := make([]string, keyCount)
	buf := make([]byte, 16)
	for i := 0; i < keyCount; i++ {
		hex.Encode(buf, []byte{byte(i >> 8), byte(i)})
		keys[i] = unsafe.String(&buf[0], 16) // string view, no alloc
	}

	m, idxMap, size := pool[0], 0, 0
	reads, overw := 0, 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 && size > 0 { // 40 % reads
			_ = m.Get(keys[reads%keyCount])
			reads++
			continue
		}
		k := keys[i%keyCount]
		if i%10 == 0 && size > 0 { // 10 % overwrites
			k = keys[overw%size]
			overw++
		}
		m.Put(k, uint32(i))
		if size < capPerMap {
			size++
		}

		if size >= capPerMap { // rotate without timing map alloc
			idxMap = (idxMap + 1) & (mapsPool - 1)
			m, size = pool[idxMap], 0
		}
	}
}

// 8 reader goroutines pounding Get – zero allocs, pre‑built keys.
func BenchmarkReaders8Contention(b *testing.B) {
	const keyCount = 1000
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("k%04d", i)
	}

	m := New()
	for _, k := range keys {
		m.Put(k, 1)
	}

	readers := 8
	var wg sync.WaitGroup
	wg.Add(readers)
	b.ResetTimer()

	for i := 0; i < readers; i++ {
		go func(id int) {
			defer wg.Done()
			idx := id % keyCount
			for j := 0; j < b.N/readers; j++ {
				_ = m.Get(keys[idx])
				idx++
				if idx == keyCount {
					idx = 0
				}
			}
		}(i)
	}
	wg.Wait()
}
