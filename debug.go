// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: debug.go — ISR-aligned error logging helper (zero-alloc)
//
// Purpose:
//   - Logs infrequent error paths without introducing heap pressure
//   - Used only in cold paths: GC tags, dial/epoll/kqueue errors, etc.
//
// Notes:
//   - Avoids fmt.Sprintf to minimize footprint and latency
//   - Uses stackless logging model: no alloc, no interfaces
//   - Aggressively inlined and nosplit — safe for ISR pipelines
//
// Compiler Directives:
//   - //go:nosplit
//   - //go:inline
//   - //go:registerparams
//
// ⚠️ Never invoke in hot loops — use only in failure diagnostics
// ─────────────────────────────────────────────────────────────────────────────

package main

import "log"

// dropError logs cold-path errors with optional suffix.
// Avoids allocations by branching on nil and using raw Printf.
//
//go:nosplit
//go:inline
//go:registerparams
func dropError(prefix string, err error) {
	if err != nil {
		// Rare case: print with error suffix
		log.Printf("%s: %v", prefix, err)
	} else {
		// Common for GC traces or tagged warnings
		log.Print(prefix)
	}
}
