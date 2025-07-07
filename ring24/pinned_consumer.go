// ring24 - ISR-grade pinned consumer system for dedicated core processing
package ring24

import (
	"main/control"
	"runtime"
	"time"
)

const (
	hotWindow  = 5 * time.Second // Duration to maintain hot polling
	spinBudget = 224             // Misses before CPU relaxation
)

// PinnedConsumer launches core-bound goroutine for ring consumption.
// Implements adaptive polling: hot spinning during activity, relaxed when idle.
//
// Parameters:
//
//	core: Target CPU core (0-indexed)
//	ring: SPSC ring buffer
//	stop: Shutdown flag pointer
//	hot: Producer activity flag
//	handler: Per-item callback
//	done: Completion channel
//
// ⚠️  SAFETY: Single consumer per core only
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func PinnedConsumer(
	core int,
	ring *Ring,
	stop *uint32,
	hot *uint32,
	handler func(*[24]byte),
	done chan<- struct{},
) {
	go func() {
		// Pin to OS thread for affinity
		runtime.LockOSThread()
		setAffinity(core)

		defer func() {
			runtime.UnlockOSThread()
			close(done)
		}()

		var miss int
		lastHit := time.Now()

		for {
			// Check shutdown
			if *stop != 0 {
				return
			}

			// Attempt pop
			if p := ring.Pop(); p != nil {
				handler(p)
				miss = 0
				lastHit = time.Now()
				continue
			}

			// Hot polling decision
			if *hot == 1 || time.Since(lastHit) <= hotWindow {
				continue // Keep spinning
			}

			// CPU relaxation after budget
			if miss++; miss >= spinBudget {
				miss = 0
				cpuRelax()
			}
		}
	}()
}

// PinnedConsumerWithCooldown - Special variant for core 1 with global cooldown
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func PinnedConsumerWithCooldown(
	core int,
	ring *Ring,
	stop *uint32,
	hot *uint32,
	handler func(*[24]byte),
	done chan<- struct{},
) {
	go func() {
		// Pin to OS thread
		runtime.LockOSThread()
		setAffinity(core)

		defer func() {
			runtime.UnlockOSThread()
			close(done)
		}()

		var miss int
		lastHit := time.Now()

		for {
			// Check shutdown
			if *stop != 0 {
				return
			}

			// Attempt pop
			if p := ring.Pop(); p != nil {
				handler(p)
				miss = 0
				lastHit = time.Now()
				continue
			}

			// Core 1 manages global cooldown
			control.PollCooldown()

			// Hot polling decision
			if *hot == 1 || time.Since(lastHit) <= hotWindow {
				continue
			}

			// CPU relaxation
			if miss++; miss >= spinBudget {
				miss = 0
				cpuRelax()
			}
		}
	}()
}
