// ring_atomic_amd64.go — Acquire/Release atomic helpers for x86-64
//go:build amd64 && !noasm

package ring32

// loadAcquireUint64 performs a load with acquire semantics from *p.
// This guarantees that any subsequent loads/stores will observe memory
// operations that happened-before the release-store to *p by another core.
//
// It is implemented as a single MOVQ instruction without LOCK prefix.
// On TSO architectures like x86-64, this satisfies acquire ordering and
// acts as a compiler barrier without requiring mfence.
//
//go:nosplit
//go:inline
func loadAcquireUint64(p *uint64) (v uint64)

// storeReleaseUint64 stores the value v into *p with release semantics.
// This ensures that all memory writes before the store are visible to
// another core before it observes the new value at *p.
//
// It is implemented using MOVQ without LOCK or MFENCE on x86-64, as
// TSO memory ordering already preserves the required visibility guarantees.
//
//go:nosplit
//go:inline
func storeReleaseUint64(p *uint64, v uint64)
