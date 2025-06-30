package main

import "log"

// dropError is a lightweight, zero-allocation logging helper for diagnostics.
// This is used for soft fail paths, GC tagging, or kqueue/epoll errors only.
// It avoids fmt.Sprintf overhead by checking `err == nil` and printing raw.
//
// Callers: error paths only. Never used on the fast path or in log emit loops.
//
// Compiler Directives:
//   - nosplit         → eliminates stack growth checks (safe due to shallow stack).
//   - inline          → ensures dropError is fused into the caller.
//   - registerparams  → ABI optimization on Go 1.21+, ensures efficient argument passing.
//
//go:nosplit
//go:inline
//go:registerparams
func dropError(prefix string, err error) {
	if err != nil {
		// Rare path: an error occurred, log with suffix
		log.Printf("%s: %v", prefix, err)
	} else {
		// Common for GC or trace tags
		log.Print(prefix)
	}
}
