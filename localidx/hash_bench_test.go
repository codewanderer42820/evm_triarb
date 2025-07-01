// ─────────────────────────────────────────────────────────────────────────────
// hash_bench_test.go — Microbenchmarks for ISR Hash (localidx)
//
// Purpose:
//   - Measures ISR hashmap latency and throughput in cold/hot scenarios
//   - Benchmarks probe behavior: cold insert, overwrite, hit, miss, mix
//   - Validates L1/L2/L3 cache stress profiles, probe tightness, and hazard depth
//
// Benchmark Categories:
//   - Cold Put (unique insert)
//   - Hot Put (overwrite path)
//   - Hit/Miss Get performance and mixed access patterns
//   - Tight-loop key read and pathological probe churn
//
// Notes:
//   - Designed for pinned single-core benchmarking
//   - Uses pre-shuffled keys to eliminate alloc and noise
// ─────────────────────────────────────────────────────────────────────────────

package localidx

import (
	"testing"
)

const (
	insertSize = 1 << 17        // 131072 → ~512 KiB table, fits in L2/L3
	lookupSize = insertSize / 2 // 65536 → 50% hit/miss balance
)

// Pre-allocated and shuffled keys for noise-free benchmarking
var (
	keys     = make([]uint32, insertSize)
	missKeys = make([]uint32, lookupSize)
)

func init() {
	for i := 0; i < insertSize; i++ {
		keys[i] = uint32(i + 1)
	}
	rnd.Shuffle(insertSize, func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for i := 0; i < lookupSize; i++ {
		missKeys[i] = uint32(i + insertSize + 100)
	}
}

// -----------------------------------------------------------------------------
// ░░ Benchmark: Pure Inserts ░░
// -----------------------------------------------------------------------------

// BenchmarkHashPutUnique measures cold insert throughput into a clean table.
func BenchmarkHashPutUnique(b *testing.B) {
	for n := 0; n < b.N; n++ {
		h := New(insertSize)
		for i := 0; i < insertSize; i++ {
			h.Put(keys[i], keys[i]*2)
		}
	}
}

// BenchmarkHashPutOverwrite hits overwrite fast-path — no Robin-Hood triggers.
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

// BenchmarkHashGetHit validates fast hit path with 100% present keys.
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

// BenchmarkHashGetMiss triggers early exit on sentinel boundary (key=0 path).
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

// BenchmarkHashGetMixed performs 50/50 alternating hit/miss probes.
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

// BenchmarkHashGetTightLoop stresses repeated reads of the same key.
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

// BenchmarkHashGetTinyMap measures hit latency in a small map (L1-optimal).
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

// BenchmarkHashPutLinearProbeWorst tests degenerate collisions with same bucket.
func BenchmarkHashPutLinearProbeWorst(b *testing.B) {
	h := New(insertSize)
	base := uint32(0xDEADBEEF)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		h.Put(base+uint32(n&255), uint32(n))
	}
}
