// ring_bench_test.go — Benchmarks for fixed-size [32]byte ring
//
// Includes tests for push, pop, push+pop, and cross-core usage.

package ring

import (
	"runtime"
	"testing"
)

const benchCap = 1024 // fits in L1/L2 cache

var dummy32 = &[32]byte{1, 2, 3}
var sink any

// -----------------------------------------------------------------------------
// Push only: producer-only path
// -----------------------------------------------------------------------------

func BenchmarkRing_Push(b *testing.B) {
	r := New(benchCap)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !r.Push(dummy32) {
			_ = r.Pop()
			_ = r.Push(dummy32)
		}
	}
}

// -----------------------------------------------------------------------------
// Pop only: consumer-only path
// -----------------------------------------------------------------------------

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
