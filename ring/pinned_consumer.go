// pinned_consumer.go
//
// Low-latency SPSC consumer.
//
//   • Dedicated OS thread pinned to `core`.
//   • Stays in **hot-spin** (tight loop, no cpuRelax) while
//       – new work has arrived within hotTimeout, OR
//       – producer keeps globalHot/hot flag == 1.
//   • After the grace window *and* once hot == 0 it drops to
//     the **cold-spin** path: cpuRelax every iteration and an optional
//     deep-sleep after spinBudget misses (hwWait).
//   • Exits only when *stop == 1 and closes `done` exactly once.
//
// Rationale: keep nanosecond latency during bursts (< 15 s) yet avoid
// burning ~2 W/core when the feed is quiet.
//
// All cross-goroutine variables are accessed atomically; no other
// synchronisation primitives appear in the hot path.
//
// hot flag contract:
//     Producer             Consumer
//     --------             ------------------------------
//     Store 1  ─────────▶  read (wake / stay hot-spin)
//     ...push items…
//     (optionally) Store 0  ◀─ consumer never writes
//
// hwWait(): UMWAIT (x86) / WFE (arm64) / futex2 fallback.

package ring

import (
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	spinBudget = 256              // polls before cold back-off
	hotTimeout = 15 * time.Second // hot-spin grace
)

// PinnedConsumer drains r until *stop is set.
func PinnedConsumer(
	core int,
	r *Ring,
	stop, hot *uint32,
	fn func(unsafe.Pointer),
	done chan<- struct{},
) {
	go func() {
		// ── thread & affinity ─────────────────────────────
		runtime.LockOSThread()
		setAffinity(core) // stub on non-Linux
		defer func() {
			runtime.UnlockOSThread()
			close(done)
		}()

		last := time.Now() // last time Pop delivered
		miss := 0

		// ── main loop ─────────────────────────────────────
		for {
			// fast path: Pop succeeded → process & mark activity
			if p := r.Pop(); p != nil {
				fn(p)
				last, miss = time.Now(), 0
				continue
			}

			// stop request?
			if atomic.LoadUint32(stop) != 0 {
				return
			}

			// ---------- choose spin mode ------------------
			hotSpin := atomic.LoadUint32(hot) != 0 ||
				time.Since(last) <= hotTimeout

			if hotSpin {
				// tight loop: no cpuRelax
				continue
			}

			// cold-spin path: power-friendlier
			if miss++; miss >= spinBudget {
				miss = 0
				// hwWait(hot)     // ← enable if you have UMWAIT/WFE
			}
			cpuRelax()
		}
	}()
}
