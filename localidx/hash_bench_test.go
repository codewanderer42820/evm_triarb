package localidx

import (
	"math/rand"
	"testing"
)

const (
	insertSize = 1 << 16        // 65,536 → ~256 KiB table, fits in L2
	lookupSize = insertSize / 2 // 32,768 → ensures 50% hit-rate in mixed tests
)

var rnd = rand.New(rand.NewSource(1337)) // deterministic RNG for reproducibility

// Pre-allocated input keys to avoid measuring slice ops during benchmarking
var (
	keys     = make([]uint32, insertSize) // inserted keys
	missKeys = make([]uint32, lookupSize) // guaranteed misses
)

func init() {
	// Populate keys with 1..insertSize, then shuffle
	for i := 0; i < insertSize; i++ {
		keys[i] = uint32(i + 1)
	}
	rnd.Shuffle(insertSize, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	// missKeys start well beyond inserted key range to avoid accidental hits
	for i := 0; i < lookupSize; i++ {
		missKeys[i] = uint32(i + insertSize + 100)
	}
}

// -----------------------------------------------------------------------------
// ░░ Benchmark: Put() with fresh keys (worst case) ░░
// -----------------------------------------------------------------------------

// BenchmarkHashPutUnique simulates worst-case insert throughput:
// new table each time, all unique keys.
func BenchmarkHashPutUnique(b *testing.B) {
	for n := 0; n < b.N; n++ {
		h := New(insertSize)
		for i := 0; i < insertSize; i++ {
			h.Put(keys[i], keys[i]*2)
		}
	}
}

// -----------------------------------------------------------------------------
// ░░ Benchmark: Put() overwrite into hot table ░░
// -----------------------------------------------------------------------------

// BenchmarkHashPutOverwrite measures overwrite performance into a prefilled table.
// Should exercise fast match-path, not Robin-Hood insert path.
func BenchmarkHashPutOverwrite(b *testing.B) {
	h := New(insertSize)
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for i := 0; i < insertSize; i++ {
			h.Put(keys[i], keys[i]*3) // logically overwrites, but value isn't stored
		}
	}
}

// -----------------------------------------------------------------------------
// ░░ Benchmark: Get() hit-path ░░
// -----------------------------------------------------------------------------

// BenchmarkHashGetHit measures lookup throughput on a table with all keys present.
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

// -----------------------------------------------------------------------------
// ░░ Benchmark: Get() miss-path ░░
// -----------------------------------------------------------------------------

// BenchmarkHashGetMiss stresses full-probe-path for keys not present in the table.
// Each miss guarantees full traversal until bound-check fails.
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

// -----------------------------------------------------------------------------
// ░░ Benchmark: Mixed hit/miss workload (50/50) ░░
// -----------------------------------------------------------------------------

// BenchmarkHashGetMixed simulates a front-loaded LRU cache scenario,
// where some lookups hit and others predictably miss.
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

// -----------------------------------------------------------------------------
// ░░ Benchmark: Repeated hit on hot key ░░
// -----------------------------------------------------------------------------

// BenchmarkHashGetTightLoop isolates per-access latency under L1 cache residency.
// Measures latency ceiling when probing cost is minimized.
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

// -----------------------------------------------------------------------------
// ░░ Benchmark: Table small enough to fully fit in L1 ░░
// -----------------------------------------------------------------------------

// BenchmarkHashGetTinyMap measures ultra-low latency in tables where
// the entire working set fits in L1 (128 entries = 512 bytes).
func BenchmarkHashGetTinyMap(b *testing.B) {
	h := New(128)
	for i := 0; i < 128; i++ {
		h.Put(uint32(i+1), uint32(i+100))
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, _ = h.Get(uint32((n & 127) + 1))
	}
}

// -----------------------------------------------------------------------------
// ░░ Benchmark: Insert pattern with maximal clustering ░░
// -----------------------------------------------------------------------------

// BenchmarkHashPutLinearProbeWorst stresses the longest probe chains possible
// by inserting keys that all map to the same bucket (mod 256).
func BenchmarkHashPutLinearProbeWorst(b *testing.B) {
	h := New(insertSize)
	base := uint32(0xDEADBEEF)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h.Put(base+uint32(n&255), uint32(n))
	}
}
