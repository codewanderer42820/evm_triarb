// ring_bench_test.go — Micro-benchmarks for 32-byte fixed-capacity ring
package ring32

import (
	"runtime"
	"sync"
	"testing"
)

const benchCap = 1024 // small enough to remain hot in L1/L2 cache

var dummy32 = &[32]byte{1, 2, 3} // constant payload
var sink any                     // prevent compiler elision

// BenchmarkRing_Push measures producer-only throughput with backpressure handling.
func BenchmarkRing_Push(b *testing.B) {
	r := New(benchCap)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !r.Push(dummy32) {
			_ = r.Pop()         // clear one slot
			_ = r.Push(dummy32) // must succeed now
		}
	}
}

// BenchmarkRing_Pop measures standalone pop cost assuming ring is mostly full.
func BenchmarkRing_Pop(b *testing.B) {
	r := New(benchCap)
	for i := 0; i < benchCap-1; i++ {
		r.Push(dummy32)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := r.Pop()
		if p == nil {
			r.Push(dummy32)
			p = r.Pop()
		}
		sink = p
		_ = r.Push(dummy32)
	}
	runtime.KeepAlive(sink)
}

// BenchmarkRing_PushPop runs producer+consumer in the same goroutine.
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

// BenchmarkRing_CrossCore runs producer and consumer on separate goroutines.
// Ensures high-throughput across cores using dedicated threads.
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
		for !r.Push(dummy32) {
			// spin until slot frees
		}
	}
	wg.Wait()
}
