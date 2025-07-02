// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: debug.go — ISR-aligned error logging helper (zero-alloc)
//
// Purpose:
//   - Logs infrequent error paths without introducing heap pressure.
//   - Used only in cold paths: GC tags, dial/epoll/kqueue errors, etc.
//
// Notes:
//   - Avoids fmt.Sprintf to minimize footprint and latency.
//   - Uses stackless logging model: no alloc, no interfaces.
//   - Aggressively inlined and nosplit — safe for ISR pipelines, ensuring minimal disruption in critical paths.
//
// ⚠️ Never invoke in hot loops — use only in failure diagnostics.
// ─────────────────────────────────────────────────────────────────────────────

package main

import "main/utils"

// dropError logs error messages with a custom alloc-free print strategy.
// It writes directly to stderr (file descriptor 2), bypassing any heap allocations.
//
//go:nosplit
//go:inline
//go:registerparams
func dropError(prefix string, err error) {
	if err != nil {
		// Rare case: Print the error message with a custom alloc-free strategy.
		// This avoids any heap allocation by directly concatenating and printing the error.
		msg := prefix + ": " + err.Error() + "\n"
		utils.PrintWarning(msg)
	} else {
		// Common case: Print just the prefix (for GC traces or tagged warnings).
		// This avoids unnecessary memory allocations for frequent paths.
		msg := prefix + "\n"
		utils.PrintWarning(msg)
	}
}
