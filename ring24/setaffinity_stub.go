// setaffinity_stub.go — No-op fallback for non-Linux or TinyGo builds
//go:build !linux || tinygo

package ring24

// setAffinity is a no-op stub used when the Linux syscall version
// is unavailable (non-Linux OS or restricted build toolchains).
//
// This allows the same API to be used across all targets without
// conditional code in the caller — simply call unconditionally.
//
// Compiler directives:
//   - nosplit: avoids stack growth for safe substitution in tight loops
//   - inline: ensures complete elimination by inlining away as no-op
//
//go:nosplit
//go:inline
func setAffinity(cpu int) {}
