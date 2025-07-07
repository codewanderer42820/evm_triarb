// relax_amd64.go - x86-64 CPU relaxation for spin loops
//
// Emits PAUSE instruction to improve power efficiency and SMT performance
// during active polling. Implementation in relax_amd64.s assembly file.

//go:build amd64 && !noasm

package ring24

// cpuRelax emits x86-64 PAUSE instruction for efficient spin-wait.
// Benefits:
//   - Reduces CPU power consumption
//   - Improves hyperthread resource sharing
//   - Minimizes memory bus traffic
//   - Lowers heat generation
//
//go:norace
//go:nocheckptr
//go:noescape
//go:nosplit
//go:inline
//go:registerparams
func cpuRelax()
