// setaffinity_stub.go - CPU affinity no-op for non-Linux platforms

//go:build !linux || tinygo

package ring24

// setAffinity no-op for platform compatibility
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func setAffinity(cpu int) {
	// No-op implementation
}
