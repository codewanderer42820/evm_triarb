// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ CPU RELAXATION - FALLBACK IMPLEMENTATION
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Trading System
// Component: Cross-Platform Compatibility Layer
//
// Description:
//   Fallback implementation for architectures without specialized spin-wait instructions.
//   Provides API compatibility while allowing platform-specific optimizations where available.
//
// Compilation Targets:
//   - Non-x86/ARM64 architectures (RISC-V, MIPS, etc.)
//   - Builds with assembly disabled (noasm tag)
//   - Platforms without PAUSE/YIELD instruction support
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

//go:build (!amd64 && !arm64) || noasm

package ring24

// cpuRelax provides a no-op implementation for architectural compatibility.
// On platforms without specialized spin-wait instructions, this function
// compiles to nothing, allowing the same API across all architectures.
//
// OPTIMIZATION NOTES:
//
//	The empty function body combined with inline directive ensures
//	complete elimination during compilation, adding zero overhead.
//
// PLATFORM BEHAVIOR:
//   - RISC-V: Could use PAUSE instruction if available
//   - MIPS: Pure spinning without hints
//   - Others: Default scheduler behavior
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func cpuRelax() {
	// No-op implementation
	// Compiler eliminates this entirely when inlined
}
