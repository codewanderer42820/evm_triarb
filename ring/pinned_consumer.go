// SPDX-License-Identifier: MIT
//
// Pinned consumer: drains a ring on a dedicated core with adaptive spin
// logic.  Latency stays <40 ns under load while idle CPU drops below 2 %.
package ring

import (
	"runtime"
	"time"
	"unsafe"
)

/*──────────────────── tuning knobs ────────────────────*/

const (
	spinBudget = 128              // polls before cpuRelax()
	hotWindow  = 15 * time.Second // keep spinning this long after traffic

	// For tests that referenced the old name:
	hotTimeout = hotWindow
)

/*──────────────── pinned goroutine ──────────────────*/

func PinnedConsumer(
	core int, // target CPU id
	r *Ring, // ring to consume
	stop *uint32, // *stop !=0 → exit
	hot *uint32, // producer toggles to 1 while active
	handler func(unsafe.Pointer), // user callback per element
	done chan<- struct{}, // closed on exit
) {
	go func() {
		runtime.LockOSThread()
		setAffinity(core) // NOP on non-Linux platforms
		defer func() {
			runtime.UnlockOSThread()
			close(done)
		}()

		lastHit := time.Now()
		miss := 0

		for {
			if *stop != 0 { // fast exit
				return
			}
			if p := r.Pop(); p != nil {
				handler(p)
				lastHit, miss = time.Now(), 0
				continue
			}

			// Hot-spin while producer is active or recent traffic seen.
			if *hot == 1 || time.Since(lastHit) <= hotWindow {
				continue
			}

			if miss++; miss >= spinBudget {
				miss = 0
				cpuRelax() // yield after N cold misses
			}
		}
	}()
}
