// pinned_consumer_test.go — Validates pinned goroutine consumer
//
// Ensures that handler triggers, hot/cold transitions, and exit
// logic all work with [32]byte buffers.

package ring

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

/* Helper: launch pinned consumer with test handler */

func launch(r *Ring, fn func(*[32]byte)) (stop, hot *uint32, done chan struct{}) {
	stop = new(uint32)
	hot = new(uint32)
	done = make(chan struct{})
	PinnedConsumer(0, r, stop, hot, fn, done)
	return
}

/* 1. Verify handler is called and clean exit */

func TestPinnedConsumerDeliversItem(t *testing.T) {
	runtime.GOMAXPROCS(2)
	r := New(8)

	var seen [32]byte
	want := [32]byte{1, 2, 3, 4}
	var got [32]byte
	stop, hot, done := launch(r, func(p *[32]byte) {
		got = *p
	})

	atomic.StoreUint32(hot, 1)
	if !r.Push(&want) {
		t.Fatal("push failed")
	}
	atomic.StoreUint32(hot, 0)

	wait := time.NewTimer(20 * time.Millisecond)
	for got == seen {
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
		t.Fatal("timeout waiting for consumer exit")
	}
	if got != want {
		t.Fatalf("callback saw %v, want %v", got, want)
	}
}

/* 2. Exit cleanly on stop without work */

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

/* 3. Hot window timeout behavior */

func TestPinnedConsumerHotWindow(t *testing.T) {
	r := New(4)
	var hits atomic.Uint32
	stop, hot, done := launch(r, func(_ *[32]byte) { hits.Add(1) })

	atomic.StoreUint32(hot, 1)
	_ = r.Push(&[32]byte{9})
	atomic.StoreUint32(hot, 0)

	time.Sleep(1 * time.Second)
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

/* 4. Full cool-off followed by reactivation */

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
