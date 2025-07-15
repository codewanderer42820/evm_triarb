// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ CPU RELAXATION - AMD64 ARCHITECTURE
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Trading System
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

//go:build amd64 && !noasm

package ring24

// cpuRelax emits x86-64 PAUSE instruction for efficient spin-wait loops.
// This function provides a hint to the processor that the calling thread
// is in a busy-wait loop, allowing for power and performance optimizations.
//
// IMPLEMENTATION:
//
//	Assembly implementation in relax_amd64.s uses the PAUSE instruction
//	which delays the next instruction's execution while allowing other
//	hyperthreads to make progress.
//
// USE CASES:
//   - Spin-wait loops in lock-free algorithms
//   - Polling loops waiting for data availability
//   - Backoff strategies in contended scenarios
//
//go:norace
//go:nocheckptr
//go:noescape
//go:nosplit
//go:inline
//go:registerparams
func cpuRelax()
