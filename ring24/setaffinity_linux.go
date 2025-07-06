// ============================================================================
// LINUX CPU AFFINITY MANAGEMENT SYSTEM
// ============================================================================
//
// Linux-specific implementation for CPU core pinning via sched_setaffinity(2)
// system call with zero-allocation, high-performance operation.
//
// Core capabilities:
//   - Direct syscall interface for maximum performance
//   - Pre-computed bitmasks for CPUs 0-63
//   - Zero heap allocation during affinity operations
//   - Stack-resident data structures for minimal overhead
//
// Performance characteristics:
//   - Sub-microsecond affinity setting via direct syscall
//   - Pre-computed masks eliminate runtime calculation overhead
//   - Cache-friendly static data layout
//   - Zero GC pressure from allocation-free operation
//
// Architecture support:
//   - Linux kernel sched_setaffinity(2) interface
//   - 64-CPU maximum capacity (standard server configurations)
//   - Thread-local affinity (current OS thread only)
//   - Silent failure for invalid CPU specifications
//
// Use cases:
//   - ISR-grade consumer goroutine pinning
//   - Cache locality optimization for real-time systems
//   - NUMA-aware thread placement
//   - Elimination of scheduler migration overhead
//
// Compiler optimizations:
//   - //go:nosplit for syscall safety
//   - //go:inline for call overhead elimination
//   - //go:registerparams for register-based parameter passing

//go:build linux && !tinygo

package ring24

import (
	"syscall"
	"unsafe"
)

// ============================================================================
// PRE-COMPUTED CPU AFFINITY MASKS
// ============================================================================

// cpuMasks contains pre-computed CPU affinity bitmasks for logical CPUs 0-63.
// Each mask is a single-word (8-byte) value with exactly one bit set,
// corresponding to the target CPU core.
//
// Memory layout optimization:
//   - Static allocation: No runtime memory management
//   - Stack-resident: Accessed via pointer arithmetic
//   - Cache-friendly: Sequential layout for predictable access
//   - Size-optimized: Single uintptr per CPU (8 bytes on 64-bit)
//
// Bitmask format:
//   - CPU 0: mask[0] = {1 << 0} = 0x0000000000000001
//   - CPU 1: mask[1] = {1 << 1} = 0x0000000000000002
//   - CPU N: mask[N] = {1 << N} = 0x0000000000000001 << N
//
// Capacity: Supports CPUs 0-63 (standard server configurations)
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

// ============================================================================
// CPU AFFINITY IMPLEMENTATION
// ============================================================================

// setAffinity pins the current OS thread to the specified logical CPU core.
// Uses direct Linux syscall interface for maximum performance and minimal overhead.
//
// Algorithm:
//  1. Validate CPU index against available mask array
//  2. Retrieve pre-computed bitmask for target CPU
//  3. Execute sched_setaffinity(2) syscall with bitmask
//  4. Silent failure for invalid inputs (safety mechanism)
//
// Syscall parameters:
//   - pid: 0 (current thread)
//   - cpusetsize: sizeof(uintptr) = 8 bytes on 64-bit systems
//   - mask: Pointer to CPU bitmask
//
// Error handling:
//   - Invalid CPU: Silent return (no error propagation)
//   - Syscall failure: Ignored (non-critical for operation)
//   - Range check: Prevents array bounds violations
//
// Performance characteristics:
//   - Sub-microsecond execution via direct syscall
//   - Zero allocation: Uses stack-based mask access
//   - Cache-efficient: Pre-computed masks eliminate calculations
//   - System call overhead: ~100-200ns on modern Linux
//
// Use cases:
//   - Pinned consumer goroutine initialization
//   - Real-time thread cache locality optimization
//   - NUMA-aware processing thread placement
//   - Scheduler migration elimination
//
// ⚠️  PLATFORM REQUIREMENTS:
//   - Linux kernel with sched_setaffinity(2) support
//   - Valid CPU index within system topology
//   - Sufficient privileges for affinity modification
//
// Parameters:
//
//	cpu: Target logical CPU index (0-63, -1 for invalid)
//
// Returns:
//
//	No return value (silent failure on error)
//
//go:nosplit
//go:inline
//go:registerparams
func setAffinity(cpu int) {
	// Validate CPU index against available masks
	if cpu < 0 || cpu >= len(cpuMasks) {
		return // Invalid CPU specification
	}

	// Retrieve pre-computed mask for target CPU
	mask := &cpuMasks[cpu]

	// Execute sched_setaffinity syscall
	// Parameters: pid=0 (current), size=8 bytes, mask=pointer
	_, _, _ = syscall.RawSyscall(
		syscall.SYS_SCHED_SETAFFINITY,
		0,                               // Current thread (pid=0)
		uintptr(unsafe.Sizeof(mask[0])), // Bitmask size (8 bytes)
		uintptr(unsafe.Pointer(mask)),   // Pointer to CPU bitmask
	)
	// Note: Return values intentionally ignored for performance
}
