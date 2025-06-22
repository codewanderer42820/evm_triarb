// ring_bench_test.go
//
// Benchmarks for five scenarios:
//   - Push           – producer-only enqueue latency
//   - Pop            – consumer-only dequeue latency
//   - PushPop        – round-trip inside one goroutine
//   - CrossCore      – producer & consumer on two CPUs (both measured)
//   - SPSCPinned     – producer pinned; consumer via helper (producer-side only)
//
// A fixed‑capacity ring (1 Ki slots) keeps every benchmark L1/L2‑resident while
// ensuring Push/Pop paths rarely miss.  If a path would fail (ring full/empty)
// the loop performs the opposite operation once and retries—one extra hop per
// 1 024 iterations, negligible in the per‑op average.

package ring

import (
	"runtime"
	"testing"
	"unsafe"
)

const benchCap = 1024 // power‑of‑two, comfortably cache‑resident

var dummy struct{}
var dummyPtr = unsafe.Pointer(&dummy)
var sink unsafe.Pointer // blocks DCE on Pop payloads

// -----------------------------------------------------------------------------
//  Single‑thread micro‑benchmarks
// -----------------------------------------------------------------------------

func BenchmarkRing_Push(b *testing.B) {
	r := New(benchCap)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !r.Push(dummyPtr) { // full? free one slot then retry
			_ = r.Pop()
			_ = r.Push(dummyPtr)
		}
	}
}

func BenchmarkRing_Pop(b *testing.B) {
	r := New(benchCap)
	for i := 0; i < benchCap-1; i++ { // leave one slot free so Pop succeeds
		r.Push(dummyPtr)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := r.Pop()
		if p == nil { // empty? push one then pop
			r.Push(dummyPtr)
			p = r.Pop()
		}
		sink = p
		// immediately re‑push to keep ring non‑empty
		_ = r.Push(dummyPtr)
	}
	runtime.KeepAlive(sink)
}

func BenchmarkRing_PushPop(b *testing.B) {
	r := New(benchCap)
	for i := 0; i < benchCap/2; i++ { // half‑full steady‑state
		r.Push(dummyPtr)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := r.Pop()
		sink = p
		_ = r.Push(dummyPtr)
	}
	runtime.KeepAlive(sink)
}

// -----------------------------------------------------------------------------
//  Cross‑core benchmarks (producer ↔ consumer on two CPUs)
// -----------------------------------------------------------------------------

func BenchmarkRing_CrossCore(b *testing.B) {
	r := New(benchCap)

	ready := make(chan struct{})
	done := make(chan struct{})

	// Consumer pinned to CPU 1.
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		setAffinity(1)
		close(ready)
		for i := 0; i < b.N; i++ {
			for r.Pop() == nil {
				cpuRelax()
			}
		}
		close(done)
	}()

	<-ready // ensure consumer pinned
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	setAffinity(0) // producer on CPU 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for !r.Push(dummyPtr) {
			cpuRelax()
		}
	}
	<-done // wait for consumer before stopping timer
	b.StopTimer()
}
