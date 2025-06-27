// pinned_consumer_test.go — Unit tests for PinnedConsumer behavior and lifecycle
package ring32

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// launch spins up a pinned consumer on a fresh thread.
// Returns: stop and hot flags (to control lifecycle), and done (signal channel).
// Used by all tests to avoid duplicated setup code.
func launch(r *Ring, fn func(*[32]byte)) (stop, hot *uint32, done chan struct{}) {
	stop = new(uint32)
	hot = new(uint32)
	done = make(chan struct{})
	PinnedConsumer(0, r, stop, hot, fn, done)
	return
}

// TestPinnedConsumerDeliversItem validates that a pushed element triggers
// the handler exactly once, and that the goroutine exits cleanly when asked.
func TestPinnedConsumerDeliversItem(t *testing.T) {
	runtime.GOMAXPROCS(2) // Ensure scheduler has enough threads
	r := New(8)
	want := [32]byte{1, 2, 3, 4}
	var got [32]byte
	var zero [32]byte

	stop, hot, done := launch(r, func(p *[32]byte) { got = *p })

	atomic.StoreUint32(hot, 1)
	if !r.Push(&want) {
		t.Fatal("Push failed unexpectedly")
	}
	atomic.StoreUint32(hot, 0)

	wait := time.NewTimer(20 * time.Millisecond)
	for got == zero {
		select {
		case <-wait.C:
			t.Fatal("callback never ran")
		default:
			runtime.Gosched()
		}
	}

	atomic.StoreUint32(stop, 1)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("consumer exit timeout")
	}

	if got != want {
		t.Fatalf("handler saw %v, want %v", got, want)
	}
}

// TestPinnedConsumerStopsNoWork ensures a consumer exits even when idle.
func TestPinnedConsumerStopsNoWork(t *testing.T) {
	r := New(4)
	stop, _, done := launch(r, func(_ *[32]byte) {})
	atomic.StoreUint32(stop, 1)

	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("consumer did not terminate")
	}
}

// TestPinnedConsumerHotWindow checks that the consumer stays alive within
// hotWindow duration even if producer becomes idle immediately.
func TestPinnedConsumerHotWindow(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ *[32]byte) { hits.Add(1) })

	atomic.StoreUint32(hot, 1)
	_ = r.Push(&[32]byte{9})
	atomic.StoreUint32(hot, 0)

	time.Sleep(1 * time.Second)
	if v := hits.Load(); v != 1 {
		t.Fatalf("callback count = %d, want 1", v)
	}

	select {
	case <-done:
		t.Fatal("consumer exited inside hot window")
	default:
	}

	atomic.StoreUint32(stop, 1)
	<-done
}

// TestPinnedConsumerBackoffThenWake simulates a cold wake-up after spin budget + hot window.
func TestPinnedConsumerBackoffThenWake(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ *[32]byte) { hits.Add(1) })

	atomic.StoreUint32(hot, 1)
	r.Push(&[32]byte{7})
	atomic.StoreUint32(hot, 0)

	time.Sleep(15*time.Second + 100*time.Millisecond)

	atomic.StoreUint32(hot, 1)
	r.Push(&[32]byte{8})
	time.Sleep(10 * time.Millisecond)

	if v := hits.Load(); v != 2 {
		t.Fatalf("expected 2 callbacks, got %d", v)
	}

	atomic.StoreUint32(stop, 1)
	<-done
}
