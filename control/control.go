// ════════════════════════════════════════════════════════════════════════════════════════════════
// Lock-Free Coordination Engine
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Syscall-Free Control Coordination
//
// Description:
//   Lock-free coordination system using virtual timing and atomic flags for multi-core
//   synchronization. Eliminates syscall overhead from critical paths via poll-based timing.
//
// Features:
//   - Virtual timing without syscall overhead
//   - Lock-free activity coordination
//   - Branchless cooldown algorithms
//   - Wait-free shutdown signaling
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package control

import (
	"main/constants"
	"sync"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COORDINATION STATE VARIABLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 64
var (
	// CACHE LINE 1: Hottest fields (accessed together in every PollCooldown() call)
	pollCounter       uint64   // 8B - Incremented every PollCooldown() call
	lastActivityCount uint64   // 8B - Read every PollCooldown() for elapsed calculation
	activityFlag      uint32   // 4B - Read/written every PollCooldown()
	shutdownFlag      uint32   // 4B - Checked in worker loops but less frequently
	_                 [40]byte // 40B - Padding to fill cache line

	// COLD: Global shutdown synchronization (accessed only during startup/shutdown)
	ShutdownWG sync.WaitGroup // 12B - WaitGroup for coordinated shutdown
	_          [52]byte       // 52B - Padding to complete cache line alignment
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ACTIVITY SIGNALING OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// SignalActivity marks the system as active and records the current poll time.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SignalActivity() {
	activityFlag = 1
	lastActivityCount = pollCounter
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// VIRTUAL TIMING COORDINATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// PollCooldown advances virtual time and clears activity flag after cooldown period expires.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func PollCooldown() {
	pollCounter++
	elapsed := pollCounter - lastActivityCount
	stillActive := uint32(((constants.CooldownPolls - elapsed) >> 63) ^ 1)
	activityFlag &= stillActive
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM CONTROL OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Shutdown signals all workers to terminate gracefully.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Shutdown() {
	shutdownFlag = 1
}

// Flags returns pointers to shutdown and activity flags for direct polling by workers.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Flags() (*uint32, *uint32) {
	return &shutdownFlag, &activityFlag
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MONITORING AND DIAGNOSTICS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// GetPollCount returns the total number of polls executed since system start.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func GetPollCount() uint64 {
	return pollCounter
}

// GetActivityAge returns the number of polls elapsed since the last activity signal.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func GetActivityAge() uint64 {
	return (pollCounter - lastActivityCount) & 0x7FFFFFFFFFFFFFFF
}

// IsActive returns true if the system has been active within the cooldown period.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func IsActive() bool {
	return activityFlag == 1
}

// IsShuttingDown returns true if shutdown has been signaled.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func IsShuttingDown() bool {
	return shutdownFlag == 1
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADVANCED MONITORING OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// GetCooldownProgress returns the cooldown completion percentage (0-100).
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func GetCooldownProgress() uint8 {
	inactiveBonus := uint64((activityFlag ^ 1) * 100)
	elapsed := (pollCounter - lastActivityCount) & 0x7FFFFFFFFFFFFFFF
	rawProgress := (elapsed * 100) / constants.CooldownPolls
	overflow := rawProgress - 100
	clampMask := uint64(int64(overflow) >> 63)
	clampedProgress := rawProgress - (overflow &^ clampMask)
	return uint8(inactiveBonus | (uint64(activityFlag) * clampedProgress))
}

// GetCooldownRemaining returns the number of polls remaining in the cooldown period.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func GetCooldownRemaining() uint64 {
	elapsed := (pollCounter - lastActivityCount) & 0x7FFFFFFFFFFFFFFF
	remaining := constants.CooldownPolls - elapsed
	return remaining &^ uint64(int64(remaining)>>63)
}

// IsWithinCooldown returns true if the system is active and within the cooldown window.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func IsWithinCooldown() bool {
	elapsed := (pollCounter - lastActivityCount) & 0x7FFFFFFFFFFFFFFF
	withinWindow := uint32(((constants.CooldownPolls - elapsed) >> 63) ^ 1)
	return (activityFlag & withinWindow) == 1
}

// GetSystemState returns a packed state word with activity, shutdown, and cooldown flags.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func GetSystemState() uint32 {
	elapsed := (pollCounter - lastActivityCount) & 0x7FFFFFFFFFFFFFFF
	withinCooldown := uint32(((constants.CooldownPolls - elapsed) >> 63) ^ 1)
	return activityFlag | (shutdownFlag << 1) | (withinCooldown << 2)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TESTING AND DEBUGGING UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ResetPollCounter resets both poll counter and activity timestamp to zero for testing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ResetPollCounter() {
	pollCounter = 0
	lastActivityCount = 0
}

// ForceActive sets the activity flag to active state for testing purposes.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ForceActive() {
	activityFlag = 1
}

// ForceInactive clears the activity flag to inactive state for testing purposes.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ForceInactive() {
	activityFlag = 0
}
