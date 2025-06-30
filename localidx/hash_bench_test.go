// Package localidx provides microbenchmarks for the fixed-capacity,
// robin-hood footgun hashmap defined in hash.go. These tests emphasize
// raw throughput, probe dynamics, and cache-behavior sensitivity.
// Designed for single-threaded latency testing under stress loads.
package localidx

import (
	"math/rand"
	"testing"
)

const (
	insertSize = 1 << 17        // 131072 → ~512 KiB table, fits in L2/L3
	lookupSize = insertSize / 2 // 65536 → 50% hit/miss balance
)

var rnd = rand.New(rand.NewSource(1337)) // deterministic RNG for reproducibility

// Pre-allocated keys to avoid slice alloc noise during benchmarking
var (
	keys     = make([]uint32, insertSize)
	missKeys = make([]uint32, lookupSize)
)

func init() {
	for i := 0; i < insertSize; i++ {
		keys[i] = uint32(i + 1)
	}
	rnd.Shuffle(insertSize, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for i := 0; i < lookupSize; i++ {
		missKeys[i] = uint32(i + insertSize + 100)
	}
}

// -----------------------------------------------------------------------------
// ░░ Benchmark: Pure Inserts ░░
// -----------------------------------------------------------------------------

// BenchmarkHashPutUnique measures raw insert throughput (cold writes).
func BenchmarkHashPutUnique(b *testing.B) {
	for n := 0; n < b.N; n++ {
		h := New(insertSize)
		for i := 0; i < insertSize; i++ {
			h.Put(keys[i], keys[i]*2)
		}
	}
}

// BenchmarkHashPutOverwrite hits overwrite path for hot table.
// Robin-Hood shouldn't trigger; path is minimal.
func BenchmarkHashPutOverwrite(b *testing.B) {
	h := New(insertSize)
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for i := 0; i < insertSize; i++ {
			h.Put(keys[i], keys[i]*3)
		}
	}
}

// -----------------------------------------------------------------------------
// ░░ Benchmark: Lookup Paths ░░
// -----------------------------------------------------------------------------

func BenchmarkHashGetHit(b *testing.B) {
	h := New(insertSize)
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		k := keys[n%insertSize]
		_, _ = h.Get(k)
	}
}

func BenchmarkHashGetMiss(b *testing.B) {
	h := New(insertSize)
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		k := missKeys[n%lookupSize]
		_, _ = h.Get(k)
	}
}

func BenchmarkHashGetMixed(b *testing.B) {
	h := New(insertSize)
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var k uint32
		if n&1 == 0 {
			k = keys[n%insertSize]
		} else {
			k = missKeys[n%lookupSize]
		}
		_, _ = h.Get(k)
	}
}

func BenchmarkHashGetTightLoop(b *testing.B) {
	h := New(insertSize)
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}
	k := keys[12345]
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _ = h.Get(k)
	}
}

func BenchmarkHashGetTinyMap(b *testing.B) {
	const size = 256
	h := New(size)
	for i := 0; i < size; i++ {
		h.Put(uint32(i+1), uint32(i+100))
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		k := uint32((n & (size - 1)) + 1)
		_, _ = h.Get(k)
	}
}

func BenchmarkHashPutLinearProbeWorst(b *testing.B) {
	h := New(insertSize)
	base := uint32(0xDEADBEEF)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		h.Put(base+uint32(n&255), uint32(n))
	}
}
