package localidx

import (
	"math/rand"
	"testing"
)

const (
	insertSize = 1 << 16        // 65536 entries
	lookupSize = insertSize / 2 // for miss rate control
)

var rnd = rand.New(rand.NewSource(1337))

// preallocate random keys for fair comparison
var keys = make([]uint32, insertSize)
var missKeys = make([]uint32, lookupSize)

func init() {
	// Populate keys with [1, insertSize]
	for i := 0; i < insertSize; i++ {
		keys[i] = uint32(i + 1)
	}
	rnd.Shuffle(insertSize, func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	// Miss keys: offset away from insert set
	for i := 0; i < lookupSize; i++ {
		missKeys[i] = uint32(i + insertSize + 100)
	}
}

// Benchmark full Put for unique keys
func BenchmarkHashPutUnique(b *testing.B) {
	for n := 0; n < b.N; n++ {
		h := New(insertSize)
		for i := 0; i < insertSize; i++ {
			h.Put(keys[i], keys[i]*2)
		}
	}
}

// Benchmark repeated Put with existing keys (overwrite)
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

// Benchmark Get hits (key exists)
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

// Benchmark Get misses (key does not exist)
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

// Benchmark interleaved Get hit/miss (50/50)
func BenchmarkHashGetMixed(b *testing.B) {
	h := New(insertSize)
	for i := 0; i < insertSize; i++ {
		h.Put(keys[i], keys[i]*2)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var k uint32
		if n%2 == 0 {
			k = keys[n%insertSize]
		} else {
			k = missKeys[n%lookupSize]
		}
		_, _ = h.Get(k)
	}
}

// Simulate high-load pressure (tight loop Get)
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

// Benchmark very small map (L1-resident, minimal masking)
func BenchmarkHashGetTinyMap(b *testing.B) {
	h := New(128)
	for i := 0; i < 128; i++ {
		h.Put(uint32(i+1), uint32(i+100))
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _ = h.Get(uint32((n % 128) + 1))
	}
}

// Simulate worst-case long probe (collisions)
func BenchmarkHashPutLinearProbeWorst(b *testing.B) {
	h := New(insertSize)
	base := uint32(0xDEADBEEF)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		h.Put(base+uint32(n%256), uint32(n))
	}
}
