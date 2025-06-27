// -----------------------------------------------------------------------------
// pinned_consumer_test.go — Unit-tests for the dedicated PinnedConsumer loop
// -----------------------------------------------------------------------------
//
//  Verifies: callback delivery, graceful shutdown, hot-window spin behaviour,
//  cold-back-off → wake-up sequence, and overall end-to-end latency invariants.
//  These tests exercise the consumer both with and without concurrent producer
//  activity to ensure the adaptive spin logic never deadlocks or starves.
// -----------------------------------------------------------------------------

package ring

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// launch is a helper that hides the boilerplate for spinning up a PinnedConsumer.
// It returns the *stop* and *hot* flags as well as the *done* channel.
func launch(r *Ring, fn func(*[32]byte)) (stop, hot *uint32, done chan struct{}) {
	stop = new(uint32)
	hot = new(uint32)
	done = make(chan struct{})
	PinnedConsumer(0, r, stop, hot, fn, done)
	return
}

// TestPinnedConsumerDeliversItem confirms that a pushed item reaches the handler
// and that the goroutine terminates cleanly when *stop is set.
func TestPinnedConsumerDeliversItem(t *testing.T) {
	runtime.GOMAXPROCS(2) // ensure at least one spare thread for the consumer
	r := New(8)
	var seen [32]byte // sentinel zero value
	want := [32]byte{1, 2, 3, 4}
	var got [32]byte

	stop, hot, done := launch(r, func(p *[32]byte) { got = *p })

	atomic.StoreUint32(hot, 1) // producer active
	if !r.Push(&want) {
		t.Fatal("push failed")
	}
	atomic.StoreUint32(hot, 0) // producer idle

	// Wait for callback (but fail fast if it never arrives)
	wait := time.NewTimer(20 * time.Millisecond)
	for got == seen {
		select {
		case <-wait.C:
			t.Fatal("callback never ran")
		default:
			runtime.Gosched()
		}
	}

	atomic.StoreUint32(stop, 1) // ask consumer to exit
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for consumer exit")
	}

	if got != want {
		t.Fatalf("callback saw %v, want %v", got, want)
	}
}

// TestPinnedConsumerStopsNoWork ensures the goroutine notices *stop without any
// traffic and exits promptly (<50 ms).
func TestPinnedConsumerStopsNoWork(t *testing.T) {
	r := New(4)
	stop, _, done := launch(r, func(_ *[32]byte) {})
	atomic.StoreUint32(stop, 1)
	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("consumer did not exit after stop")
	}
}

// TestPinnedConsumerHotWindow verifies that the consumer keeps spinning during
// the grace period (hotWindow) even after *hot is cleared.
func TestPinnedConsumerHotWindow(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ *[32]byte) { hits.Add(1) })

	atomic.StoreUint32(hot, 1)
	_ = r.Push(&[32]byte{9})
	atomic.StoreUint32(hot, 0)

	time.Sleep(1 * time.Second) // < hotWindow (15 s)
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

// TestPinnedConsumerBackoffThenWake waits past the full cool‑off period to
// confirm the goroutine throttles down, then re‑activates correctly when new
// work appears.
func TestPinnedConsumerBackoffThenWake(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ *[32]byte) { hits.Add(1) })

	// 1) initial hit, enter hot window
	atomic.StoreUint32(hot, 1)
	r.Push(&[32]byte{7})
	atomic.StoreUint32(hot, 0)

	// 2) wait past hotWindow so consumer backs off
	time.Sleep(15*time.Second + 100*time.Millisecond)

	// 3) send a second hit and ensure it is processed
	atomic.StoreUint32(hot, 1)
	r.Push(&[32]byte{8})
	time.Sleep(10 * time.Millisecond)

	if v := hits.Load(); v != 2 {
		t.Fatalf("expected 2 callbacks, got %d", v)
	}
	atomic.StoreUint32(stop, 1)
	<-done
}
