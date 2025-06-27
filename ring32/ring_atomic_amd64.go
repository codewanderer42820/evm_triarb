//go:build amd64 && !noasm

// ring_atomic_amd64.go
//
// Function stubs whose bodies live in ring_atomic_amd64.s.  They wrap a
// plain MOVQ to provide acquire / release semantics and act as compiler
// barriers without the cost of mfence on TSO x86-64.

package ring32

// loadAcquireUint64 returns *p with acquire ordering.
//
//go:nosplit
func loadAcquireUint64(p *uint64) (v uint64)

// storeReleaseUint64 performs *p = v with release ordering.
//
//go:nosplit
func storeReleaseUint64(p *uint64, v uint64)
