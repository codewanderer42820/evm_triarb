// relax_amd64.go — Go-side declaration for cpuRelax used in spin loops
//go:build amd64 && !noasm

package ring24

// cpuRelax emits the x86-64 PAUSE instruction, acting as a polite hint to the CPU
// that this thread is busy-spinning and should back off slightly.
//
// Used in spin loops to avoid excess power draw or wasting hyperthreaded sibling resources.
// The actual implementation is provided in relax_amd64.s.
//
// Compiler directives:
//   - noescape: tells compiler this pointer-returning fn leaks no memory to heap
//   - nosplit: suppresses stack split logic (safe for tight spin loops)
//   - inline: strongly encourages inlining at call sites
//
//go:noescape
//go:nosplit
//go:inline
func cpuRelax()
