//go:build !linux || tinygo

// setaffinity_stub.go
//
// No‑op replacement for `setAffinity` on every target where the fast Linux
// `sched_setaffinity` syscall either doesn’t exist **or** TinyGo is in use.
// Concretely, this stub builds on:
//
//   - any non‑Linux OS  (darwin, windows, freebsd, plan9, wasm, …)
//   - TinyGo tool‑chains, even when `GOOS=linux`, because the TinyGo runtime
//     lacks the necessary raw‑syscall wrappers.
//
// The build tags are the logical inverse of `linux && !tinygo` used by
// setaffinity_linux.go:
//
//	linux && !tinygo  → real syscall version
//	!linux || tinygo  → this stub
//
// Keeping the symbol in the package means callers can invoke `setAffinity`
// unconditionally; when this stub is selected it silently does nothing and the
// goroutine remains scheduled wherever the Go runtime decides.

package ring32

// setAffinity is a no‑op on unsupported OSes or when building with TinyGo.
func setAffinity(cpu int) {}
