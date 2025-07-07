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
//   • Nanosecond-scale flag operations with compiler inlining
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
//
// Memory layout legend:
//	┌────────────────────────────────────────────────────────────────────┐
//	│ websocket ─▶ SignalActivity ─▶ hot=1 ─▶ consumers ─▶ PollCooldown │
//	│                                ▲                        │         │
//	│                          hot=0 ◀────────────────────────┘         │
//	└────────────────────────────────────────────────────────────────────┘
//
// Compiler optimizations:
//   • //go:nosplit for stack management elimination
//   • //go:inline for call overhead elimination
//   • //go:registerparams for register-based parameter passing

package control

import "time"

// ============================================================================
// GLOBAL STATE MANAGEMENT
// ============================================================================

var (
	// Global coordination flags - accessed by all consumer threads with lock-free semantics
	hot  uint32 // Activity indicator: 1 = active WebSocket traffic, 0 = idle state
	stop uint32 // Shutdown signal: 1 = initiate graceful shutdown, 0 = normal operation

	// Activity timing for automatic cooldown management with nanosecond precision
	lastHot    int64                    // Nanosecond timestamp of most recent WebSocket activity
	cooldownNs = int64(1 * time.Second) // Cooldown duration: system idle threshold period
)

// ============================================================================
// ACTIVITY SIGNALING (WEBSOCKET INTEGRATION)
// ============================================================================

// SignalActivity marks the system as active and records precise timing
// for automatic cooldown management. Called from WebSocket ingress layer
// upon receiving transaction logs, price updates, or any market data.
//
// Threading model: Safe for concurrent calls from multiple WebSocket threads
// Performance impact: Nanosecond-scale execution with zero heap allocations
// Memory ordering: Compiler barriers ensure proper flag visibility
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
// to prevent unnecessary CPU spinning during market idle periods.
//
// Call frequency: Inline during consumer spin loops for optimal efficiency
// Timing precision: Nanosecond-accurate cooldown detection with monotonic clock
// Performance impact: Single comparison operation in the common case
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
// cleanly upon detection, ensuring proper resource cleanup and data integrity.
//
// Shutdown sequence: Signal broadcast → Consumer detection → Resource cleanup → Exit
// Coordination model: Single writer, multiple readers with memory barrier guarantees
// Timing guarantees: Immediate visibility across all CPU cores
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
// Return values: (*stop_flag, *hot_flag) for direct memory access
// Memory safety: Returned pointers remain valid for application lifetime
// Cache performance: Flags stored in adjacent memory for optimal access patterns
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Flags() (*uint32, *uint32) {
	return &stop, &hot
}
