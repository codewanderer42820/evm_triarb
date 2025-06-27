package ring

import (
	"runtime"
	"testing"
)

const benchCap = 1024 // chosen so the whole ring fits inside L2 on most CPUs

var dummy32 = &[32]byte{1, 2, 3} // reused buffer to avoid allocation noise
var sink any                     // global escape sink (prevent compiler cheats)

// BenchmarkRing_Push measures producer throughput under zero contention.
func BenchmarkRing_Push(b *testing.B) {
	r := New(benchCap)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !r.Push(dummy32) {
			_ = r.Pop() // make room
			_ = r.Push(dummy32)
		}
	}
}

// BenchmarkRing_Pop measures consumer throughput when the ring is mostly full.
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

// BenchmarkRing_PushPop benchmarks the balanced case where the same goroutine
// alternates push and pop, simulating a stage that both produces and consumes.
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
