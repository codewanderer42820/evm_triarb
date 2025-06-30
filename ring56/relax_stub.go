// relax_stub.go — portable fallback for cpuRelax on non-x86 systems
//go:build !amd64 || noasm

package ring56

// cpuRelax is a no-op on platforms without PAUSE instruction support.
// This stub is used for:
//
//   - non-amd64 platforms (e.g., ARM, RISC-V, etc.)
//   - builds where assembly is disabled via `noasm` tag
//
// The function is safe to call unconditionally in spin loops — on unsupported
// targets it simply does nothing.
//
// Compiler directives:
//   - nosplit: safe to embed in spin loops
//   - inline: encourages compiler to drop into instruction stream
//
//go:nosplit
//go:inline
func cpuRelax() {}
