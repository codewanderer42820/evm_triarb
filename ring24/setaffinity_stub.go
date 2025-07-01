// setaffinity_stub.go — No-op fallback for setAffinity on unsupported targets
//
// This stub is used when core pinning via sched_setaffinity is unavailable:
//   - Non-Linux platforms (e.g. macOS, Windows, BSD)
//   - TinyGo or restricted toolchains
//
// It ensures the API surface remains valid and eliminates the need for
// conditional compilation in higher-level code.
//
// Behavior:
//   - Function is a no-op
//   - Safe to call unconditionally
//   - Fully inlined and stackless
//
// Compiler directives:
//   - nosplit: safe for ISR paths and spin loops
//   - inline: ensures total compiler elimination
//
//go:build !linux || tinygo

package ring24

//go:nosplit
//go:inline
func setAffinity(cpu int) {}
