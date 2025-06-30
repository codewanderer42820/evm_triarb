// ring_bench_test.go — Micro-benchmarks for 56-byte fixed-capacity SPSC ring.
//
// Focus:
//   - Hot-path performance for Push, Pop, and PushPop
//   - Cross-core contention simulation
//   - Elision-resistant test design (via runtime.KeepAlive + global sink)
//
// Assumptions:
//   - Ring is correctly sized (power-of-two).
//   - Producer and consumer maintain SPSC contract.

package ring56

import (
	"runtime"
	"sync"
	"testing"
)

const benchCap = 1024 // small enough to remain hot in L1/L2 cache

var dummy56 = &[56]byte{1, 2, 3} // constant payload
var sink any                     // escape sink to prevent elision

// BenchmarkRing_Push measures producer-only throughput under full ring pressure.
// Caller spins on Pop if full, emulating best-effort batching.
func BenchmarkRing_Push(b *testing.B) {
	r := New(benchCap)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !r.Push(dummy56) {
			_ = r.Pop()
			_ = r.Push(dummy56)
		}
	}
}

// BenchmarkRing_Pop measures raw consumer throughput assuming ring is preloaded.
func BenchmarkRing_Pop(b *testing.B) {
	r := New(benchCap)
	for i := 0; i < benchCap-1; i++ {
		r.Push(dummy56)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := r.Pop()
		if p == nil {
			r.Push(dummy56)
			p = r.Pop()
		}
		sink = p
		_ = r.Push(dummy56)
	}
	runtime.KeepAlive(sink)
}

// BenchmarkRing_PushPop measures a tight in-goroutine loop of Pop→Push.
// This approximates microservice-style relaying.
func BenchmarkRing_PushPop(b *testing.B) {
	r := New(benchCap)
	for i := 0; i < benchCap/2; i++ {
		r.Push(dummy56)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := r.Pop()
		sink = p
		_ = r.Push(dummy56)
	}
	runtime.KeepAlive(sink)
}

// BenchmarkRing_CrossCore simulates a full producer-consumer split across OS threads.
//
// GOMAXPROCS(2) gives us real core separation on most OSes.
// This captures true inter-core communication cost through L2 or L3.
func BenchmarkRing_CrossCore(b *testing.B) {
	runtime.GOMAXPROCS(2)
	r := New(benchCap)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for i := 0; i < b.N; {
			if r.Pop() != nil {
				i++
			}
		}
		wg.Done()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for !r.Push(dummy56) {
			// spin until slot frees
		}
	}
	wg.Wait()
}
