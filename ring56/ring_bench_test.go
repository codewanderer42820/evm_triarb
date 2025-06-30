// -----------------------------------------------------------------------------
// ░░ Ring Buffer Performance Benchmarks ░░
// -----------------------------------------------------------------------------
//
// These microbenchmarks target critical paths for latency and throughput:
//   - Producer-only (Push)
//   - Consumer-only (Pop)
//   - Relay (Push+Pop)
//   - Cross-core SPSC behavior

package ring56

import (
	"runtime"
	"sync"
	"testing"
)

const benchCap = 1024 // Small enough to fit in L1/L2 on modern cores

var dummy56 = &[56]byte{1, 2, 3} // Constant payload for benchmark sanity
var sink any                     // Global escape sink (prevents compiler elision)

// -----------------------------------------------------------------------------
// ░░ Benchmark: Push only (write path) ░░
// -----------------------------------------------------------------------------

// BenchmarkRing_Push measures producer-only throughput under full ring pressure.
// Producer spins if full, simulating backpressure + retry.
func BenchmarkRing_Push(b *testing.B) {
	r := New(benchCap)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !r.Push(dummy56) {
			_ = r.Pop()         // free one
			_ = r.Push(dummy56) // should now succeed
		}
	}
}

// -----------------------------------------------------------------------------
// ░░ Benchmark: Pop only (read path) ░░
// -----------------------------------------------------------------------------

// BenchmarkRing_Pop measures pure consumer latency.
// The ring is prefilled and partially consumed to keep it hot.
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

// -----------------------------------------------------------------------------
// ░░ Benchmark: Push + Pop (single goroutine relay) ░░
// -----------------------------------------------------------------------------

// BenchmarkRing_PushPop measures tightly-coupled sequential push/pop throughput.
// This simulates relay systems or memory-local stream processing.
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

// -----------------------------------------------------------------------------
// ░░ Benchmark: Cross-Core (full SPSC with pinned consumer) ░░
// -----------------------------------------------------------------------------

// BenchmarkRing_CrossCore simulates true SPSC behavior with a pinned producer
// and a consumer running on a separate OS thread.
// This captures L2/L3 core-to-core handoff performance.
func BenchmarkRing_CrossCore(b *testing.B) {
	runtime.GOMAXPROCS(2) // ensure scheduler isn't bottlenecked
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
			// spin until consumer frees a slot
		}
	}
	wg.Wait()
}
