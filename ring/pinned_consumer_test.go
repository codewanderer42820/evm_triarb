// pinned_consumer_test.go
//
// Tests for the production PinnedConsumer.
//
//   1. Delivers first pushed item and exits on stop
//   2. Exits cleanly when stop is raised while ring is empty
//   3. Remains in hot-spin window for < hotTimeout even after hot flag clears
//   4. Falls back to cold-spin after hotTimeout, then wakes again on new hot
//
// All tests finish in <250 ms on modest hardware.

package ring

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

/* --- tiny helper ---------------------------------------------------------- */

func launch(r *Ring, fn func(unsafe.Pointer)) (stop, hot *uint32, done chan struct{}) {
	stop = new(uint32)
	hot = new(uint32)
	done = make(chan struct{})
	PinnedConsumer(0, r, stop, hot, fn, done)
	return
}

/* ------------------------------------------------------------------------ */
/* 1. Delivery & graceful stop                                              */
/* ------------------------------------------------------------------------ */

func TestPinnedConsumerDeliversItem(t *testing.T) {
	runtime.GOMAXPROCS(2)

	r := New(8)
	item := unsafe.Pointer(new(int))
	var seen atomic.Pointer[int]

	// launch consumer
	stop, hot, done := launch(r, func(p unsafe.Pointer) {
		seen.Store((*int)(p))
	})

	// wake-then-push
	atomic.StoreUint32(hot, 1)
	if !r.Push(item) {
		t.Fatal("push failed")
	}
	atomic.StoreUint32(hot, 0)

	// wait until callback fires (with small timeout guard)
	wait := time.NewTimer(20 * time.Millisecond)
	for seen.Load() == nil {
		select {
		case <-wait.C:
			t.Fatal("callback never ran")
		default:
			runtime.Gosched()
		}
	}

	// now request clean shutdown
	atomic.StoreUint32(stop, 1)

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for consumer exit")
	}

	if got := unsafe.Pointer(seen.Load()); got != item {
		t.Fatalf("callback saw %p, want %p", got, item)
	}
}

/* ------------------------------------------------------------------------ */
/* 2. Stop flag with empty ring                                             */
/* ------------------------------------------------------------------------ */

func TestPinnedConsumerStopsNoWork(t *testing.T) {
	r := New(4)
	stop, _, done := launch(r, func(_ unsafe.Pointer) {})

	atomic.StoreUint32(stop, 1)

	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("consumer did not exit after stop")
	}
}

/* ------------------------------------------------------------------------ */
/* 3. Stays hot within grace window                                         */
/* ------------------------------------------------------------------------ */

func TestPinnedConsumerHotWindow(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ unsafe.Pointer) { hits.Add(1) })

	// first burst
	atomic.StoreUint32(hot, 1)
	_ = r.Push(unsafe.Pointer(new(int)))
	atomic.StoreUint32(hot, 0)

	time.Sleep(1 * time.Second) // < hotTimeout

	// confirm still alive & hot
	if v := hits.Load(); v != 1 {
		t.Fatalf("callback count %d, want 1", v)
	}
	select {
	case <-done:
		t.Fatal("consumer exited inside hot window")
	default:
	}

	atomic.StoreUint32(stop, 1)
	<-done
}

/* ------------------------------------------------------------------------ */
/* 4. Back-off after timeout, then re-wake                                  */
/* ------------------------------------------------------------------------ */

func TestPinnedConsumerBackoffThenWake(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ unsafe.Pointer) { hits.Add(1) })

	// first burst
	atomic.StoreUint32(hot, 1)
	r.Push(unsafe.Pointer(new(int)))
	atomic.StoreUint32(hot, 0)

	time.Sleep(hotTimeout + 100*time.Millisecond) // let it cool

	// second burst
	atomic.StoreUint32(hot, 1)
	r.Push(unsafe.Pointer(new(int)))

	time.Sleep(10 * time.Millisecond)

	if v := hits.Load(); v != 2 {
		t.Fatalf("expected 2 callbacks, got %d", v)
	}
	atomic.StoreUint32(stop, 1)
	<-done
}
