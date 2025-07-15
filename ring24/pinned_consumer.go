// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ CORE-PINNED CONSUMER SYSTEM
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Trading System
// Component: Dedicated Core Message Processing
//
// Description:
//   CPU core-bound consumer implementation for lock-free ring buffers. Provides adaptive
//   polling strategies with hot/cold detection and automatic CPU relaxation to balance
//   latency and power consumption in multi-core processing systems.
//
// Adaptive Behavior:
//   - Hot mode: Continuous polling during active message flow
//   - Cool mode: CPU relaxation after idle threshold
//   - Automatic transition based on message arrival patterns
//   - Special core 1 variant with global cooldown management
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package ring24

import (
	"main/control"
	"runtime"
	"time"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONFIGURATION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// hotWindow defines the duration to maintain aggressive polling after activity.
	// During this window, the consumer assumes more messages are likely to arrive.
	hotWindow = 5 * time.Second

	// spinBudget sets the number of failed polls before CPU relaxation.
	// Balances responsiveness with power efficiency.
	spinBudget = 224
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// STANDARD PINNED CONSUMER
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// PinnedConsumer launches a goroutine bound to a specific CPU core for ring consumption.
// The consumer adaptively adjusts its polling strategy based on message traffic patterns.
//
// PARAMETERS:
//   - core: Target CPU core index (0-based)
//   - ring: SPSC ring buffer to consume from
//   - stop: Pointer to shutdown flag (non-zero triggers shutdown)
//   - hot: Pointer to producer activity flag (1 = active producer)
//   - handler: Callback function for processing each message
//   - done: Channel closed when consumer terminates
//
// THREADING MODEL:
//
//	The goroutine locks to an OS thread and sets CPU affinity to ensure
//	consistent NUMA locality and predictable cache behavior.
//
// ADAPTIVE POLLING:
//   - Continuous polling when messages arrive or producer is active
//   - Graduated relaxation after idle periods to save power
//   - Immediate response to shutdown signals
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
		// Lock goroutine to OS thread for CPU affinity
		runtime.LockOSThread()
		setAffinity(core) // Platform-specific CPU binding

		// Ensure cleanup on exit
		defer func() {
			runtime.UnlockOSThread()
			close(done)
		}()

		// Polling state management
		var miss int          // Consecutive failed polls
		lastHit := time.Now() // Last successful message receipt

		// Main consumption loop
		for {
			// Priority 1: Check for shutdown signal
			if *stop != 0 {
				return
			}

			// Priority 2: Attempt message consumption
			if p := ring.Pop(); p != nil {
				handler(p)           // Process the message
				miss = 0             // Reset miss counter
				lastHit = time.Now() // Update activity timestamp
				continue
			}

			// Priority 3: Determine polling strategy
			// Stay in hot mode if producer is active or recent activity
			if *hot == 1 || time.Since(lastHit) <= hotWindow {
				continue // Keep spinning for low latency
			}

			// Priority 4: Apply CPU relaxation after threshold
			if miss++; miss >= spinBudget {
				miss = 0
				cpuRelax() // Reduce power consumption
			}
		}
	}()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SPECIAL CORE 1 CONSUMER WITH COOLDOWN
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// PinnedConsumerWithCooldown provides a special variant for core 1 with global cooldown polling.
// This variant includes additional responsibility for managing system-wide cooldown state.
//
// SPECIAL BEHAVIOR:
//
//	In addition to standard message consumption, this variant calls
//	control.PollCooldown() to manage global system state transitions.
//	Typically used for the primary message processing core.
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
		// Thread affinity setup
		runtime.LockOSThread()
		setAffinity(core)

		// Cleanup on termination
		defer func() {
			runtime.UnlockOSThread()
			close(done)
		}()

		// Polling state
		var miss int
		lastHit := time.Now()

		// Main loop with cooldown polling
		for {
			// Check shutdown
			if *stop != 0 {
				return
			}

			// Attempt consumption
			if p := ring.Pop(); p != nil {
				handler(p)
				miss = 0
				lastHit = time.Now()
				continue
			}

			// Core 1 special: Poll global cooldown state
			// This manages system-wide hot/cold transitions
			control.PollCooldown()

			// Adaptive polling decision
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
