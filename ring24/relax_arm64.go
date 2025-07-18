// ════════════════════════════════════════════════════════════════════════════════════════════════
// CPU Relaxation - ARM64 Architecture
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: ARM64 Spin-Wait Optimization
//
// Description:
//   Platform-specific implementation for ARM64 processors using the YIELD instruction.
//   Improves power efficiency and performance in multi-core environments during
//   busy-wait loops by providing hints to the CPU pipeline.
//
// Hardware Benefits:
//   - Reduced power consumption during spin loops
//   - Better resource sharing on multi-core systems
//   - Improved performance on Apple Silicon and other ARM64 processors
//   - Lower thermal output in high-frequency polling
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

//go:build arm64 && !noasm

package ring24

/*
#ifdef __aarch64__
static inline void cpu_yield() {
    __asm__ __volatile__("yield" ::: "memory");
}
#else
#error "This file requires ARM64 architecture"
#endif
*/
import "C"

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CPU RELAXATION FUNCTION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// cpuRelax emits ARM64 YIELD instruction for efficient spin-wait loops.
// This function provides a hint to the processor that the calling thread
// is in a busy-wait loop, allowing for power and performance optimizations.
//
// Implementation:
//   - Uses inline assembly through CGO to emit the YIELD instruction
//   - YIELD provides a hint that the current thread is performing a spin-wait
//     loop, allowing the processor to optimize power consumption
//   - Particularly effective on Apple Silicon and modern ARM cores
//
// Use Cases:
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
