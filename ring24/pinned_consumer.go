// pinned_consumer.go — Dedicated consumer goroutine pinned to one CPU core.
//
// ⚠️ Footgun Mode: Designed for high-frequency ISR pipelines with zero margin for error.
// Used in arbitrage/tick feed systems where latency budgets are sub-µs.
//
// Purpose:
//   - Binds the goroutine to a physical CPU using LockOSThread + sched_setaffinity
//   - Polls a local ring buffer and calls handler on each Pop
//   - Spins while hot, relaxes politely on cooldown
//
// Assumptions:
//   - Only one consumer per core — never duplicate
//   - Caller owns lifecycle and ensures proper shutdown
//   - Ring must be correctly constructed SPSC with [24]byte payloads
//   - No bounds checks, no panic recovery, no fallback behavior
//
// Directives:
//   - nosplit: guarantees stack won’t grow mid-loop
//   - inline: encourages inlining of control logic
//   - registerparams: ABI optimization (Go 1.21+)

package ring24

import (
	"runtime"
	"time"
)

const (
	hotWindow  = 5 * time.Second // duration to stay hot after last hit
	spinBudget = 224             // # of misses before calling cpuRelax
)

// PinnedConsumer spins up a core-bound goroutine consuming from the ring.
//
// It repeatedly polls the given SPSC ring. When data is available,
// it invokes `handler(*[24]byte)` with the item. It stays hot while
// *hot == 1 or until hotWindow timeout expires.
//
// Exit occurs when *stop is set to 1. Cleanup is signaled via `done <- struct{}{}`.
//
//go:nosplit
//go:inline
//go:registerparams
func PinnedConsumer(
	core int,                  // target logical CPU core (0-indexed)
	ring *Ring,                // SPSC ring buffer
	stop *uint32,              // stop flag: 1 = exit loop
	hot *uint32,               // hot flag: 1 = producer active
	handler func(*[24]byte),   // invoked on every Pop
	done chan<- struct{},      // closed when consumer exits
) {
	go func() {
		runtime.LockOSThread() // bind to single OS thread
		setAffinity(core)      // pin thread to desired CPU
		defer func() {
			runtime.UnlockOSThread()
			close(done) // signal termination
		}()

		var miss int
		lastHit := time.Now()

		for {
			if *stop != 0 {
				return
			}

			if p := ring.Pop(); p != nil {
				handler(p)
				miss = 0
				lastHit = time.Now()
				continue
			}

			if *hot == 1 || time.Since(lastHit) <= hotWindow {
				continue
			}

			if miss++; miss >= spinBudget {
				miss = 0
				cpuRelax()
			}
		}
	}()
}
