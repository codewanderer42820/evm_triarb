//go:build !amd64 || noasm

// relax_stub.go
//
// Portable fall-back for non-amd64 builds or when assembly stubs are
// disabled.  Declares cpuRelax as an empty function so source compiles
// unchanged on every architecture.

package ring32

// cpuRelax is a no-op on unsupported targets.
func cpuRelax() {}
