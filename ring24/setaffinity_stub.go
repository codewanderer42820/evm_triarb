// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ CPU AFFINITY - FALLBACK IMPLEMENTATION
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Trading System
// Component: Cross-Platform Compatibility Layer
//
// Description:
//   Fallback implementation for platforms without CPU affinity support.
//   Maintains API compatibility while gracefully degrading on unsupported systems.
//
// Supported Platforms:
//   This stub is used for:
//   - Non-Linux operating systems (Windows, macOS, BSD)
//   - TinyGo runtime (embedded systems)
//   - Platforms without thread affinity APIs
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

//go:build !linux || tinygo

package ring24

// setAffinity provides a no-op implementation for platform compatibility.
// On systems without CPU affinity support, threads are scheduled by the OS
// scheduler without specific core binding.
//
// BEHAVIOR:
//
//	While CPU affinity is not set, the OS scheduler may still provide
//	reasonable cache locality through its own heuristics. Performance
//	may be less predictable but remains functional.
//
// FUTURE ENHANCEMENTS:
//
//	Platform-specific implementations could be added:
//	- Windows: SetThreadAffinityMask
//	- macOS: thread_policy_set with THREAD_AFFINITY_POLICY
//	- FreeBSD: cpuset_setaffinity
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
