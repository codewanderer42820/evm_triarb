// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ CPU RELAXATION - ARM64 ARCHITECTURE (CGO)
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Trading System
// Component: ARM64 Spin-Wait Optimization
//
// Description:
//   Platform-specific implementation for ARM64 processors using the YIELD instruction.
//   Uses CGO to avoid conflicts with other CGO usage in the package.
//
// Hardware Benefits:
//   - Reduced power consumption during spin loops
//   - Better resource sharing on multi-core systems
//   - Improved performance on Apple Silicon and other ARM64 processors
//   - Lower thermal output in high-frequency polling
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

//go:build arm64 && !noasm && !nocgo

package ring24

/*
static inline void cpu_yield() {
    __asm__ __volatile__("yield" ::: "memory");
}
*/
import "C"

// cpuRelax emits ARM64 YIELD instruction for efficient spin-wait loops.
// This function provides a hint to the processor that the calling thread
// is in a busy-wait loop, allowing for power and performance optimizations.
//
// IMPLEMENTATION:
//
//	Uses inline assembly through CGO to emit the YIELD instruction.
//	This avoids conflicts between CGO and Go assembly in the same package.
//
// USE CASES:
//   - Spin-wait loops in lock-free algorithms
//   - Polling loops waiting for data availability
//   - Backoff strategies in contended scenarios
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func cpuRelax() {
	C.cpu_yield()
}
