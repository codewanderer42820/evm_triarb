// relax_stub.go — Fallback no-op for cpuRelax on non-x86 systems
//
// This stub ensures builds on ARM, RISC-V, WASM, or TinyGo do not fail.
// It provides a safe no-op drop-in for platforms lacking a real PAUSE instruction.
//
// Use-case:
//   - Safe to embed in spin loops
//   - Does nothing by design on unsupported hardware
//
// Compiler directives:
//   - nosplit: ensures compatibility with ISR spin paths
//   - inline: allows this no-op to be fully eliminated
//
//go:build !amd64 || noasm

package ring24

//go:nosplit
//go:inline
func cpuRelax() {}
