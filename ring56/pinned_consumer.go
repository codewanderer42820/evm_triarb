// pinned_consumer.go — Dedicated consumer goroutine pinned to one CPU core.
//
// ⚠️ Footgun Mode: Designed for high-frequency tick pipelines with no safety net.
//
// Purpose:
//   - Runs tight pop→callback loop locked to specific OS thread & CPU.
//   - Spins during hot windows, backs off gently using cpuRelax() after inactivity.
//
// Assumptions:
//   - Ring buffer must be SPSC and size-valid.
//   - Payloads are [24]byte (cacheline-fit).
//   - Caller controls lifecycle via atomic stop/hot flags.
//   - Only call once per CPU — this is pinned.
//
// No panics. No retries. No forgiveness.

package ring24

import (
	"runtime"
	"time"
)

const (
	hotWindow  = 5 * time.Second // how long to stay warm after traffic stops
	spinBudget = 224             // # empty polls before we relax()
)

// PinnedConsumer runs a dedicated, CPU-bound goroutine locked to one core.
//
// It repeatedly polls the ring for entries, invoking `handler(*[24]byte)`
// when data is present. Exit is signaled by setting *stop = 1. External
// activity hints can set *hot = 1 to keep it spinning longer.
//
// When the consumer exits, the `done` channel is closed for cleanup.
//
// Compiler directives:
//   - nosplit: avoid stack checks, safe due to fixed size and no runtime calls inside loop
//   - inline: encourage aggressive inlining of caller
//   - registerparams: pass arguments in registers (Go 1.21+ ABI)
//
//go:nosplit
//go:inline
//go:registerparams
func PinnedConsumer(
	core int, // Logical CPU core to bind this thread to
	ring *Ring, // Ring to consume from (SPSC)
	stop *uint32, // Pointer to atomic flag (set to 1 to stop)
	hot *uint32, // Pointer to atomic flag (1 = producer active)
	handler func(*[24]byte), // Callback invoked on each Pop
	done chan<- struct{}, // Closed on graceful exit
) {
	go func() {
		runtime.LockOSThread() // Pin this goroutine to a real OS thread
		setAffinity(core)      // Linux-specific: syscall-based core pinning
		defer func() {
			runtime.UnlockOSThread()
			close(done) // Notify caller that we’ve exited
		}()

		var miss int          // Empty poll streak counter
		lastHit := time.Now() // Last time we successfully popped

		for {
			if *stop != 0 {
				return // Exit cleanly on signal
			}

			if p := ring.Pop(); p != nil {
				handler(p) // Call user logic
				miss = 0   // Reset streak
				lastHit = time.Now()
				continue
			}

			if *hot == 1 || time.Since(lastHit) <= hotWindow {
				continue // Stay hot
			}

			if miss++; miss >= spinBudget {
				miss = 0
				cpuRelax() // Yield politely (PAUSE or Gosched)
			}
		}
	}()
}
