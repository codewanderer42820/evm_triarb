// pinned_consumer.go — Dedicated consumer goroutine pinned to one CPU core
package ring32

import (
	"runtime"
	"time"
)

// hotWindow defines how long the consumer continues to spin after last traffic.
const hotWindow = 15 * time.Second

// spinBudget defines how many consecutive empty-polls the consumer tolerates
// before invoking cpuRelax to reduce contention and power usage.
const spinBudget = 128

//go:nosplit
//go:inline
func PinnedConsumer(
	core int, // Logical CPU to pin the goroutine
	ring *Ring, // Ring buffer to consume from
	stop *uint32, // External stop flag; set to 1 to terminate loop
	hot *uint32, // External hotness flag; producer sets to 1 during activity
	handler func(*[32]byte), // Callback executed for each dequeued element
	done chan<- struct{}, // Channel closed upon consumer exit (sync/test hook)
) {
	go func() {
		runtime.LockOSThread() // Pin goroutine to OS thread
		setAffinity(core)      // Optionally pin thread to CPU core (Linux-specific)
		defer func() {
			runtime.UnlockOSThread()
			close(done) // Always signal exit
		}()

		var miss int          // Miss counter (number of consecutive empty polls)
		lastHit := time.Now() // Time of last successful Pop

		for {
			if *stop != 0 {
				return // Exit immediately when stop is signaled
			}

			if p := ring.Pop(); p != nil {
				handler(p) // Process item
				miss = 0
				lastHit = time.Now()
				continue // Hot loop
			}

			if *hot == 1 || time.Since(lastHit) <= hotWindow {
				continue // Remain in hot window
			}

			if miss++; miss >= spinBudget {
				miss = 0
				cpuRelax() // Issue PAUSE or Gosched
			}
		}
	}()
}
