// ============================================================================

// relax_amd64.go - x86-64 CPU Relaxation Declaration
// ============================================================================
//
// Go declaration for hardware-accelerated CPU relaxation on x86-64 platforms.
// Implementation provided by corresponding assembly file (relax_amd64.s).
//
// Hardware optimization:
//   - PAUSE instruction: Reduces power consumption in spin loops
//   - Hyperthread efficiency: Improves SMT performance under contention
//   - Cache optimization: Minimizes unnecessary cache line invalidations
//   - Branch prediction: Assists with speculative execution efficiency
//
// Integration points:
//   - Spin loop yield: Cooperative waiting during active polling
//   - Power management: Reduced heat generation during busy wait
//   - Performance scaling: Better behavior under high concurrency

//go:build amd64 && !noasm

package ring24

// cpuRelax emits x86-64 PAUSE instruction for efficient spin-wait loops.
// Implementation provided by relax_amd64.s assembly file.
//
// Hardware benefits:
//   - Power efficiency: Reduces CPU power consumption during wait
//   - SMT optimization: Improves hyperthread resource sharing
//   - Memory efficiency: Reduces unnecessary memory bus traffic
//   - Thermal management: Lower heat generation during active polling
//
// Compiler directives:
//   - //go:noescape: Guarantees no heap escape analysis
//   - //go:nosplit: Safe for use in stack-sensitive contexts
//   - //go:inline: Aggressive inlining for call elimination
//
//go:norace
//go:nocheckptr
//go:noescape
//go:nosplit
//go:inline
//go:registerparams
func cpuRelax()
