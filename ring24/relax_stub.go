// ════════════════════════════════════════════════════════════════════════════════════════════════
// CPU Relaxation - Fallback Implementation
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Cross-Platform Compatibility Layer
//
// Description:
//   Fallback implementation for architectures without specialized spin-wait instructions.
//   Provides API compatibility while allowing platform-specific optimizations where available.
//
// Compilation Targets:
//   - RISC-V, MIPS, PowerPC, s390x, and other architectures
//   - Builds with assembly disabled (noasm tag)
//   - Builds with CGO disabled (nocgo tag)
//   - Platforms without PAUSE/YIELD instruction support
//
// Supported Architectures (with dedicated implementations):
//   - amd64: Uses PAUSE instruction (relax_amd64.go)
//   - arm64: Uses YIELD instruction (relax_arm64.go)
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

//go:build (!amd64 && !arm64) || noasm || nocgo

package ring24

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CPU RELAXATION FUNCTION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// cpuRelax provides a no-op implementation for architectural compatibility.
// On platforms without specialized spin-wait instructions, this function
// compiles to nothing, allowing the same API across all architectures.
//
// Optimization Notes:
//   - The empty function body combined with inline directive ensures
//     complete elimination during compilation, adding zero overhead
//   - Processor will continue spinning at full speed without hints
//
// Platform Behavior:
//   - RISC-V: Could use PAUSE instruction if available (Zihintpause extension)
//   - PowerPC: Could use "or 31,31,31" (low priority yield)
//   - MIPS: Pure spinning without hints
//   - s390x: Could use CPU relax facilities
//   - WASM: No threading, so no-op is appropriate
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
