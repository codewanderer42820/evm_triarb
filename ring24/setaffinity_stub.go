// ============================================================================
// CROSS-PLATFORM COMPATIBILITY STUB SYSTEM
// ============================================================================

// setaffinity_stub.go - CPU Affinity No-Op Implementation
// ============================================================================
//
// Cross-platform compatibility stub for CPU affinity operations on systems
// where sched_setaffinity(2) is unavailable or unsupported.
//
// Supported platforms:
//   - Non-Linux systems: macOS, Windows, BSD variants
//   - Restricted toolchains: TinyGo, embedded targets
//   - Limited runtime environments: WebAssembly, containers
//
// Compatibility strategy:
//   - Maintains identical API surface for conditional compilation elimination
//   - Zero overhead: Function completely eliminated by compiler inlining
//   - Safe fallback: No-op behavior prevents compilation failures
//   - Transparent operation: Higher-level code requires no modifications
//
// Use cases:
//   - Cross-platform deployment without conditional compilation
//   - Development environments lacking affinity support
//   - Embedded systems with simplified threading models
//   - Container deployments with restricted syscall access

//go:build !linux || tinygo

package ring24

// setAffinity provides no-op CPU affinity for unsupported platforms.
// Maintains API compatibility while providing safe fallback behavior.
//
// Implementation characteristics:
//   - Zero overhead: Completely eliminated by compiler optimization
//   - Safe operation: No side effects or error conditions
//   - API preservation: Identical signature to Linux implementation
//   - Silent operation: No indication of unsupported functionality
//
// Parameters:
//
//	cpu: Target CPU index (ignored on unsupported platforms)
//
//go:nosplit
//go:inline
func setAffinity(cpu int) {
	// No-op implementation for platform compatibility
}
