// ring_atomic_fallback.go — Portable atomic acquire/release for non-x86 or noasm builds
//go:build !amd64 || noasm

package ring32

import "sync/atomic"

// loadAcquireUint64 performs an acquire load from *p.
//
// Ensures that any loads or stores following this call will not be reordered
// before it. Backed by sync/atomic.LoadUint64 which emits memory fences
// where needed per platform.
//
// Used on architectures without TSO (e.g., ARM, RISC-V) or when x86-64
// assembly stubs are disabled (via 'noasm').
//
//go:nosplit
//go:inline
func loadAcquireUint64(p *uint64) uint64 {
	return atomic.LoadUint64(p)
}

// storeReleaseUint64 performs a release store to *p.
//
// Guarantees that all memory operations prior to the call are visible before
// the new value at *p becomes observable. Matches the semantics of
// atomic.StoreUint64 with release ordering.
//
// Used in fallback paths where custom MOVQ is unavailable.
//
//go:nosplit
//go:inline
func storeReleaseUint64(p *uint64, v uint64) {
	atomic.StoreUint64(p, v)
}
