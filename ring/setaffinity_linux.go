//go:build linux && !tinygo

// setaffinity_linux.go
//
// Linux-only binding for `sched_setaffinity(2)` that pins **this** OS thread
// to a single logical CPU.  The helper is intentionally ultra-lightweight:
// no heap allocations, no per-call masks built on the stack.
//
// Design notes
// ------------
//   • A compile-time array `cpuMasks` pre-defines one `uintptr` bitmask for
//     every logical CPU 0–63.  Each mask lives in read-only data so the
//     Go compiler can embed it directly; the kernel sees a contiguous
//     8-byte buffer, exactly what `sched_setaffinity` expects on 64-bit.
//   • We ignore CPUs ≥ 64—on such systems users can still benchmark with the
//     first 64 cores, and the fast path stays allocation-free.
//   • Errors are deliberately swallowed: on a containerised or cgroup-heavy
//     system the call might be EPERM/EINVAL; the fallback is simply “no pin”.
//
// This file is built only when `GOOS=linux` and **not** under TinyGo.

package ring

import (
	"syscall"
	"unsafe"
)

// Pre-computed one-word affinity masks for logical CPUs 0-63.
var cpuMasks = [...][1]uintptr{
	{1 << 0}, {1 << 1}, {1 << 2}, {1 << 3}, {1 << 4}, {1 << 5}, {1 << 6}, {1 << 7},
	{1 << 8}, {1 << 9}, {1 << 10}, {1 << 11}, {1 << 12}, {1 << 13}, {1 << 14}, {1 << 15},
	{1 << 16}, {1 << 17}, {1 << 18}, {1 << 19}, {1 << 20}, {1 << 21}, {1 << 22}, {1 << 23},
	{1 << 24}, {1 << 25}, {1 << 26}, {1 << 27}, {1 << 28}, {1 << 29}, {1 << 30}, {1 << 31},
	{1 << 32}, {1 << 33}, {1 << 34}, {1 << 35}, {1 << 36}, {1 << 37}, {1 << 38}, {1 << 39},
	{1 << 40}, {1 << 41}, {1 << 42}, {1 << 43}, {1 << 44}, {1 << 45}, {1 << 46}, {1 << 47},
	{1 << 48}, {1 << 49}, {1 << 50}, {1 << 51}, {1 << 52}, {1 << 53}, {1 << 54}, {1 << 55},
	{1 << 56}, {1 << 57}, {1 << 58}, {1 << 59}, {1 << 60}, {1 << 61}, {1 << 62}, {1 << 63},
}

// setAffinity pins the *current thread* to `cpu` (0-based).  Out-of-range
// indices are ignored for portability.
func setAffinity(cpu int) {
	if cpu < 0 || cpu >= len(cpuMasks) {
		return
	}
	mask := &cpuMasks[cpu]
	_, _, _ = syscall.RawSyscall(
		syscall.SYS_SCHED_SETAFFINITY,
		0,                               // pid 0 → current thread
		uintptr(unsafe.Sizeof(mask[0])), // mask length (8 bytes)
		uintptr(unsafe.Pointer(mask)),
	)
}
