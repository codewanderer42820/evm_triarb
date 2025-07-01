// ring_bench_test.go — Microbenchmarks for ISR-grade ring buffer
//
// Benchmarks measure SPSC performance in real-world ISR scenarios:
//   - Producer-only (Push loop under contention)
//   - Consumer-only (Pop throughput)
//   - Combined relay (tight Push→Pop pipeline)
//   - Cross-core transfer (SPSC with CPU handoff)
//
// Focus is on latency, throughput, cacheline contention, and memory locality.
//
// Tools like `go test -bench` can be used to isolate specific pipeline stages.

package ring24

import (
	"runtime"
	"sync"
	"testing"
)

const benchCap = 1024 // Fits entirely in L1/L2 to reflect tight ISR loop

var dummy24 = &[24]byte{1, 2, 3} // Dummy payload (cache-friendly)
var sink any                     // Escape variable to prevent elision

/*──────────────────── Benchmark: Push Only ───────────────────*/

// BenchmarkRing_Push measures write-side throughput under full ring pressure.
// Producer spins to make room simulating retry behavior in ISR.
func BenchmarkRing_Push(b *testing.B) {
	r := New(benchCap)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !r.Push(dummy24) {
			_ = r.Pop()         // clear one
			_ = r.Push(dummy24) // retry
		}
	}
}

/*──────────────────── Benchmark: Pop Only ───────────────────*/

// BenchmarkRing_Pop isolates consumer read latency.
// The ring is pre-filled to simulate ISR-feed-style readiness.
func BenchmarkRing_Pop(b *testing.B) {
	r := New(benchCap)
	for i := 0; i < benchCap-1; i++ {
		r.Push(dummy24)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := r.Pop()
		if p == nil {
			r.Push(dummy24)
			p = r.Pop()
		}
		sink = p
		_ = r.Push(dummy24)
	}
	runtime.KeepAlive(sink)
}

/*──────────────────── Benchmark: Push + Pop (Relay) ───────────────────*/

// BenchmarkRing_PushPop measures interleaved sequential Push/Pop.
// Simulates single-threaded relays or microservice local rings.
func BenchmarkRing_PushPop(b *testing.B) {
	r := New(benchCap)
	for i := 0; i < benchCap/2; i++ {
		r.Push(dummy24)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := r.Pop()
		sink = p
		_ = r.Push(dummy24)
	}
	runtime.KeepAlive(sink)
}

/*──────────────────── Benchmark: Cross-Core SPSC ───────────────────*/

// BenchmarkRing_CrossCore simulates cross-thread full SPSC fanout.
// One thread produces, another consumes, mimicking ISR to CPU pipeline.
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
		for !r.Push(dummy24) {
			// Spin until consumer frees slot
		}
	}
	wg.Wait()
}
