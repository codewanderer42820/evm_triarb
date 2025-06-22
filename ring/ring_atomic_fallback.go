//go:build !amd64 || noasm

// ring_atomic_fallback.go
//
// Portable implementations for the acquire/release helpers using
// sync/atomic.  Seq-cst is a conservative superset of the required order.

package ring

import "sync/atomic"

// loadAcquireUint64 is an acquire load of *p.
func loadAcquireUint64(p *uint64) uint64 {
	return atomic.LoadUint64(p)
}

// storeReleaseUint64 is a release store to *p.
func storeReleaseUint64(p *uint64, v uint64) {
	atomic.StoreUint64(p, v)
}
