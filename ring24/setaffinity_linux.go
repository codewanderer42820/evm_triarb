// setaffinity_linux.go - Linux CPU affinity via sched_setaffinity(2)

//go:build linux && !tinygo

package ring24

import (
	"syscall"
	"unsafe"
)

// Pre-computed CPU masks for cores 0-63
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

// setAffinity pins current thread to specified CPU core
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func setAffinity(cpu int) {
	// Validate CPU index
	if cpu < 0 || cpu >= len(cpuMasks) {
		return
	}

	// Get pre-computed mask
	mask := &cpuMasks[cpu]

	// Direct syscall for minimum overhead
	_, _, _ = syscall.RawSyscall(
		syscall.SYS_SCHED_SETAFFINITY,
		0,                               // Current thread
		uintptr(unsafe.Sizeof(mask[0])), // Mask size
		uintptr(unsafe.Pointer(mask)),   // Mask pointer
	)
}
