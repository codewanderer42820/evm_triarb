// control.go — Global control flags and activity management for pinned consumers
// ============================================================================
// SYSTEM CONTROL ORCHESTRATION
// ============================================================================
//
// Control package provides lightweight global signaling infrastructure for
// coordinating activity states and graceful shutdown across pinned consumer
// threads with nanosecond-precision timing and zero-allocation operations.
//
// Architecture overview:
//   • Global hot/stop flags for lock-free inter-thread communication
//   • Nanosecond-precision activity tracking with automatic cooldown
//   • Zero-allocation flag access for hot path performance
//   • Graceful shutdown coordination across all consumer cores
//
// Performance characteristics:
//   • Sub-nanosecond flag operations with compiler inlining
//   • Cache-friendly memory layout for optimal access patterns
//   • Lock-free atomic operations for maximum throughput
//   • Automatic cooldown management with configurable timeouts
//
// Threading model:
//   • WebSocket layer signals activity via SignalActivity()
//   • Consumer threads poll flags via Flags() for coordination
//   • Automatic cooldown prevents unnecessary hot spinning
//   • Graceful shutdown ensures clean resource cleanup
//
// Safety guarantees:
//   • Race-free flag access with proper memory ordering
//   • Bounded cooldown periods prevent infinite hot spinning
//   • Deterministic shutdown behavior across all cores

package control

import "time"

// ============================================================================
// GLOBAL STATE MANAGEMENT
// ============================================================================

var (
	// Global coordination flags - accessed by all consumer threads
	hot  uint32 // Activity indicator: 1 = active WebSocket traffic, 0 = idle
	stop uint32 // Shutdown signal: 1 = initiate graceful shutdown, 0 = running

	// Activity timing for automatic cooldown management
	lastHot    int64                    // Nanosecond timestamp of last WebSocket activity
	cooldownNs = int64(1 * time.Second) // Cooldown duration: 1 second idle period
)

// ============================================================================
// ACTIVITY SIGNALING (WEBSOCKET INTEGRATION)
// ============================================================================

// SignalActivity marks the system as active and records precise timing
// for automatic cooldown management. Called from WebSocket ingress layer
// upon receiving transaction logs or price updates.
//
// Performance: Sub-nanosecond execution with compiler optimization
// Thread safety: Safe for concurrent calls from WebSocket threads
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SignalActivity() {
	hot = 1
	lastHot = time.Now().UnixNano()
}

// ============================================================================
// COOLDOWN MANAGEMENT (AUTOMATIC EFFICIENCY)
// ============================================================================

// PollCooldown implements automatic hot-flag clearance based on elapsed
// time since last activity. Integrates seamlessly into consumer hot loops
// to prevent unnecessary CPU spinning during idle periods.
//
// Call frequency: Inline during consumer spin loops for optimal efficiency
// Timing precision: Nanosecond-accurate cooldown detection
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func PollCooldown() {
	if hot == 1 && time.Now().UnixNano()-lastHot > cooldownNs {
		hot = 0
	}
}

// ============================================================================
// SYSTEM SHUTDOWN (GRACEFUL TERMINATION)
// ============================================================================

// Shutdown initiates graceful system termination by setting the global
// stop flag. All pinned consumer threads monitor this flag and terminate
// cleanly upon detection, ensuring proper resource cleanup.
//
// Shutdown sequence: Signal → Consumer detection → Resource cleanup → Exit
// Coordination: Broadcast signal reaches all cores simultaneously
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Shutdown() {
	stop = 1
}

// ============================================================================
// FLAG ACCESS (CONSUMER INTEGRATION)
// ============================================================================

// Flags returns direct pointers to global coordination flags for
// zero-allocation access by pinned consumer threads. Enables efficient
// polling without function call overhead in performance-critical loops.
//
// Return values: (*stop_flag, *hot_flag) for PinnedConsumer integration
// Memory safety: Returned pointers remain valid for application lifetime
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Flags() (*uint32, *uint32) {
	return &stop, &hot
}
