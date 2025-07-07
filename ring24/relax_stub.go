// relax_stub.go - CPU relaxation no-op for non-x86 platforms
//
// Provides API compatibility for architectures without PAUSE instruction.
// Completely eliminated by compiler inlining.

//go:build !amd64 || noasm

package ring24

// cpuRelax no-op for architecture compatibility
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func cpuRelax() {
	// No-op implementation
}
