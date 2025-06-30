// ring_atomic_amd64.go — Acquire/Release atomic helpers for x86-64
//go:build amd64 && !noasm

package ring56

// loadAcquireUint64 performs a load with acquire semantics from *p.
// Replaced at link time by .s version (MOVQ-based).
//
// Compiler directives:
//   - noescape: p does not escape
//   - nosplit: used in tight loops
//   - inline: allow inlining in ring paths
//
//go:noescape
//go:nosplit
//go:inline
func loadAcquireUint64(p *uint64) uint64

// storeReleaseUint64 performs a release store to *p.
// Replaced at link time by .s version (MOVQ-based).
//
// Compiler directives:
//   - noescape: args do not escape
//   - nosplit: avoid stack check overhead
//   - inline: embed store directly into ring ops
//
//go:noescape
//go:nosplit
//go:inline
func storeReleaseUint64(p *uint64, v uint64)
