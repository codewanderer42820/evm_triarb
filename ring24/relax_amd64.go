// relax_amd64.go — Go declaration for cpuRelax on x86-64 systems
//
// This version is used when real x86-64 assembly is available (PAUSE).
// Declaration only; implementation is in relax_amd64.s.
//
//go:build amd64 && !noasm

package ring24

// cpuRelax emits the x86-64 PAUSE instruction via relax_amd64.s
//
// Use this in spin loops to reduce contention during active polling.
//
// Compiler directives:
//   - noescape: assures no heap interactions
//   - nosplit: safe for spin paths
//   - inline: should be aggressively inlined
//
//go:noescape
//go:nosplit
//go:inline
func cpuRelax()