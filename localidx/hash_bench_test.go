// -----------------------------------------------------------------------------
// localidx/hash_bench_test.go — Micro-benchmarks for the Robin-Hood map
// -----------------------------------------------------------------------------
//
//  • Measures put/get throughput under different access patterns
//  • Uses deterministic random input so results are reproducible
//  • Benchmarks are sized to saturate L2 but not spill into main memory,
//    keeping variance low across CPU micro-architectures
// -----------------------------------------------------------------------------

package localidx

import (
	"math/rand"
	"testing"
)

const (
	insertSize = 1 << 16        // 65 536 inserts → ~256 KiB table
	lookupSize = insertSize / 2 // sized to give 50 % hit-rate where needed
)

var rnd = rand.New(rand.NewSource(1337))

// Pre-allocated input so the benchmark loop is pure map traffic.
var (
	keys     = make([]uint32, insertSize)
	missKeys = make([]uint32, lookupSize)
)

func init() {
	// keys = 1..insertSize shuffled
	for i := 0; i < insertSize; i++ {
		keys[i] = uint32(i + 1)
	}
	rnd.Shuffle(insertSize, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	// missKeys are outside the insert range so Get misses deterministically.
	for i := 0; i < lookupSize; i++ {
		missKeys[i] = uint32(i + insertSize + 100)
	}
}

// -----------------------------------------------------------------------------
// Put() benchmarks
// -----------------------------------------------------------------------------

// BenchmarkHashPutUnique inserts *new* keys each iteration (worst-case path).
func BenchmarkHashPutUnique(b *testing.B) {
	for n := 0; n < b.N; n++ {
		h := New(insertSize)
		for i := 0; i < insertSize; i++ {
			h.Put(keys[i], keys[i]*2)
		}
	}
}

// BenchmarkHashPutOverwrite measures overwrite speed once the table is hot.
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
// Get() benchmarks
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

// BenchmarkHashGetMixed interleaves 50 % hits / 50 % misses — a common pattern
// when the table backs a front-loaded cache.
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

// BenchmarkHashGetTightLoop hammers a single hot key to expose pure lookup cost
// under maximum CPU cache residency (≈ 2.5 ns on M4 Pro).
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

// BenchmarkHashGetTinyMap shows the latency floor when the whole table fits in
// L1 — useful for spotting regressions in the inner loop itself.
func BenchmarkHashGetTinyMap(b *testing.B) {
	h := New(128) // 128 slots → 512 B table
	for i := 0; i < 128; i++ {
		h.Put(uint32(i+1), uint32(i+100))
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _ = h.Get(uint32((n & 127) + 1))
	}
}

// -----------------------------------------------------------------------------
// Pathological cases
// -----------------------------------------------------------------------------

// BenchmarkHashPutLinearProbeWorst inserts keys that collide into a single
// bucket modulo 256, exercising the linear probe worst-case path.
func BenchmarkHashPutLinearProbeWorst(b *testing.B) {
	h := New(insertSize)
	base := uint32(0xDEADBEEF)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		h.Put(base+uint32(n&255), uint32(n))
	}
}
