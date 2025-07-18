// ════════════════════════════════════════════════════════════════════════════════════════════════
// CPU Affinity - Fallback Implementation
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Cross-Platform Compatibility Layer
//
// Description:
//   Fallback implementation for platforms without CPU affinity support.
//   Maintains API compatibility while gracefully degrading on unsupported systems.
//
// Compilation Targets:
//   - Non-Linux operating systems (Windows, macOS, BSD)
//   - TinyGo runtime (embedded systems)
//   - Platforms without thread affinity APIs
//
// Supported Platforms (with dedicated implementations):
//   - Linux: Uses sched_setaffinity syscall (setaffinity_linux.go)
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

//go:build !linux || tinygo

package ring24

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// AFFINITY CONTROL
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// setAffinity provides a no-op implementation for platform compatibility.
// On systems without CPU affinity support, threads are scheduled by the OS
// scheduler without specific core binding.
//
// Behavior:
//   - Thread remains on whichever core the OS scheduler assigns
//   - runtime.LockOSThread() still prevents goroutine migration
//   - Cache locality depends on OS scheduler heuristics
//
// Platform Notes:
//   - Windows: Could use SetThreadAffinityMask via syscall
//   - macOS: Could use thread_policy_set (requires CGO)
//   - FreeBSD: Could use cpuset_setaffinity
//   - NetBSD: Could use pthread_setaffinity_np
//   - OpenBSD: Limited support, relies on scheduler
//   - Plan9: No direct support
//   - WASM: Single-threaded, no affinity needed
//
// Performance Impact:
//   - Without CPU affinity, may experience more cache misses
//   - Thread migration between cores can impact performance
//   - Still functional but less predictable latency
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func setAffinity(cpu int) {
	// No-op implementation
	// OS scheduler handles thread placement
}
