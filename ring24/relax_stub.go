// ============================================================================

// relax_stub.go - CPU Relaxation No-Op Implementation
// ============================================================================
//
// Cross-platform compatibility stub for CPU relaxation hints on systems
// lacking dedicated pause/yield instructions.
//
// Supported platforms:
//   - Non-x86 architectures: ARM, RISC-V, MIPS, PowerPC
//   - WebAssembly targets: Browser and server-side WASM
//   - Embedded systems: TinyGo and resource-constrained environments
//   - Assembly-disabled builds: Configurations with noasm tag
//
// Compatibility strategy:
//   - Maintains identical API for spin loop integration
//   - Zero overhead: Complete compiler elimination via inlining
//   - Safe integration: No-op behavior in existing spin loops
//   - Architecture agnostic: Works on any target platform

//go:build !amd64 || noasm

package ring24

// cpuRelax provides no-op CPU yield hint for unsupported architectures.
// Maintains API compatibility for spin loops across all target platforms.
//
// Implementation characteristics:
//   - Zero overhead: Completely eliminated by aggressive inlining
//   - Loop safety: Safe to call in tight spin loops
//   - Platform agnostic: Functions identically across architectures
//   - Power neutral: No power management impact
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func cpuRelax() {
	// No-op implementation for architecture compatibility
}
