// ════════════════════════════════════════════════════════════════════════════════════════════════
// CPU Relaxation - AMD64 Architecture
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: x86-64 Spin-Wait Optimization
//
// Description:
//   Platform-specific implementation for x86-64 processors using the PAUSE instruction.
//   Improves power efficiency and performance in hyperthreaded environments during
//   busy-wait loops by providing hints to the CPU pipeline.
//
// Hardware Benefits:
//   - Reduced power consumption during spin loops
//   - Better resource sharing on SMT/hyperthreaded cores
//   - Minimized memory ordering speculation
//   - Lower thermal output in high-frequency polling
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

//go:build amd64 && !noasm && !nocgo

package ring24

/*
#ifdef __x86_64__
static inline void cpu_pause() {
    __asm__ __volatile__("pause" ::: "memory");
}
#else
#error "This file requires x86-64 architecture"
#endif
*/
import "C"

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CPU RELAXATION FUNCTION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// cpuRelax emits x86-64 PAUSE instruction for efficient spin-wait loops.
// This function provides a hint to the processor that the calling thread
// is in a busy-wait loop, allowing for power and performance optimizations.
//
// Implementation:
//   - Uses inline assembly through CGO to emit the PAUSE instruction
//   - PAUSE delays the next instruction's execution while allowing other
//     hyperthreads to make progress
//   - Typical delay: 10-140 cycles depending on processor generation
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
	C.cpu_pause()
}
