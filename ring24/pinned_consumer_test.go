// -----------------------------------------------------------------------------
// ░░ Pinned Consumer Lifecycle Tests ░░
// -----------------------------------------------------------------------------
//
// These tests verify the correct lifecycle behavior of pinned consumers:
//   - Push/poll semantics
//   - Shutdown signaling
//   - Hot window retention and cold resume

package ring24

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// ░░ Pinned Consumer Lifecycle Tests ░░
// -----------------------------------------------------------------------------

// launch spins up a pinned consumer on its own thread.
// Returns: stop, hot (control flags), and done (channel closed on exit).
func launch(r *Ring, fn func(*[24]byte)) (stop, hot *uint32, done chan struct{}) {
	stop = new(uint32)
	hot = new(uint32)
	done = make(chan struct{})
	PinnedConsumer(0, r, stop, hot, fn, done)
	return
}

// TestPinnedConsumerDeliversItem verifies handler fires once on push
// and consumer shuts down cleanly on signal.
func TestPinnedConsumerDeliversItem(t *testing.T) {
	runtime.GOMAXPROCS(2)
	r := New(8)

	want := [24]byte{1, 2, 3, 4}
	var got [24]byte
	var zero [24]byte

	stop, hot, done := launch(r, func(p *[24]byte) { got = *p })

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

// TestPinnedConsumerStopsNoWork confirms idle consumers exit cleanly.
func TestPinnedConsumerStopsNoWork(t *testing.T) {
	r := New(4)
	stop, _, done := launch(r, func(_ *[24]byte) {})
	atomic.StoreUint32(stop, 1)

	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("consumer did not terminate")
	}
}

// TestPinnedConsumerHotWindow ensures consumer persists within hotWindow
// even after producer stops.
func TestPinnedConsumerHotWindow(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ *[24]byte) { hits.Add(1) })

	atomic.StoreUint32(hot, 1)
	_ = r.Push(&[24]byte{9})
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

// TestPinnedConsumerBackoffThenWake tests cold-resume behavior after timeout.
func TestPinnedConsumerBackoffThenWake(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ *[24]byte) { hits.Add(1) })

	atomic.StoreUint32(hot, 1)
	r.Push(&[24]byte{7})
	atomic.StoreUint32(hot, 0)

	time.Sleep(15*time.Second + 100*time.Millisecond)

	atomic.StoreUint32(hot, 1)
	r.Push(&[24]byte{8})
	time.Sleep(10 * time.Millisecond)

	if v := hits.Load(); v != 2 {
		t.Fatalf("expected 2 callbacks, got %d", v)
	}

	atomic.StoreUint32(stop, 1)
	<-done
}

// -----------------------------------------------------------------------------
// ░░ Late Consumer Start Test ░░
// -----------------------------------------------------------------------------

// TestDelayedConsumerStart validates a Push before consumer is active.
func TestDelayedConsumerStart(t *testing.T) {
	r := New(4)
	var seen atomic.Bool
	r.Push(&[24]byte{11})
	stop := new(uint32)
	hot := new(uint32)
	done := make(chan struct{})
	go PinnedConsumer(0, r, stop, hot, func(*[24]byte) {
		seen.Store(true)
	}, done)
	time.Sleep(10 * time.Millisecond)
	atomic.StoreUint32(stop, 1)
	<-done
	if !seen.Load() {
		t.Fatal("consumer did not see pre-pushed value")
	}
}
