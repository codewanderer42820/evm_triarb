// ring_bench_test.go — Benchmarks for fixed-size [32]byte ring
//
// Includes:
//   • BenchmarkRing_Push      – producer-only path
//   • BenchmarkRing_Pop       – consumer-only path
//   • BenchmarkRing_PushPop   – push+pop in one goroutine
//   • BenchmarkRing_CrossCore – simultaneous producer/consumer on two cores
//
// Each benchmark keeps the ring 50-100 % full so steady-state costs are measured
// without allocation noise or re-zeroing of slots.

package ring

import (
	"runtime"
	"sync"
	"testing"
)

const benchCap = 1024 // fits comfortably in L1/L2 on modern CPUs

var dummy32 = &[32]byte{1, 2, 3}
var sink any // global escape sink ∴ compiler cannot optimise access away

// -----------------------------------------------------------------------------
// Push only: producer-only path
// -----------------------------------------------------------------------------

func BenchmarkRing_Push(b *testing.B) {
	r := New(benchCap)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !r.Push(dummy32) {
			_ = r.Pop()         // free one slot
			_ = r.Push(dummy32) // guaranteed to succeed
		}
	}
}

// -----------------------------------------------------------------------------
// Pop only: consumer-only path
// -----------------------------------------------------------------------------

func BenchmarkRing_Pop(b *testing.B) {
	r := New(benchCap)
	for i := 0; i < benchCap-1; i++ { // pre-fill so ring is mostly full
		r.Push(dummy32)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := r.Pop()
		if p == nil { // ring became empty – refill one
			r.Push(dummy32)
			p = r.Pop()
		}
		sink = p
		_ = r.Push(dummy32)
	}
	runtime.KeepAlive(sink)
}

// -----------------------------------------------------------------------------
// Push + Pop inside single goroutine
// -----------------------------------------------------------------------------

func BenchmarkRing_PushPop(b *testing.B) {
	r := New(benchCap)
	for i := 0; i < benchCap/2; i++ {
		r.Push(dummy32)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := r.Pop()
		sink = p
		_ = r.Push(dummy32)
	}
	runtime.KeepAlive(sink)
}

// -----------------------------------------------------------------------------
// Cross-core: producer and consumer on separate OS threads
// -----------------------------------------------------------------------------

func BenchmarkRing_CrossCore(b *testing.B) {
	runtime.GOMAXPROCS(2) // ensure the runtime can run both goroutines at once
	r := New(benchCap)

	var wg sync.WaitGroup
	wg.Add(1)

	// Consumer goroutine — continuously pops until N items processed.
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

	// Producer loop — pushes N items, waiting when ring is full.
	for i := 0; i < b.N; i++ {
		for !r.Push(dummy32) { /* spin until a slot frees */
		}
	}

	wg.Wait()
}
