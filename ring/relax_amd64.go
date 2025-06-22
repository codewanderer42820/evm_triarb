//go:build amd64 && !noasm

// relax_amd64.go
//
// Go declaration for cpuRelax on amd64.  The implementation lives in
// relax_amd64.s and emits a single PAUSE instruction so busy-wait loops
// back-off politely while remaining in userspace.

package ring

// cpuRelax executes the x86_64 PAUSE instruction.
//
//go:noescape
func cpuRelax()
