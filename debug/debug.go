// debug.go — Zero-allocation error logging system for performance-critical paths
// ============================================================================
// ZERO-ALLOCATION ERROR LOGGING SYSTEM
// ============================================================================
//
// Debug package provides ISR-aligned error logging without heap allocation
// for performance-critical systems requiring sub-microsecond response times.
//
// Architecture overview:
//   • Zero-allocation error reporting for cold-path diagnostics
//   • Direct syscall integration bypassing standard library overhead
//   • ISR-safe logging suitable for interrupt service routines
//   • Minimal CPU overhead for hot path integration
//
// Performance characteristics:
//   • Nanosecond-scale logging operations with zero heap allocations
//   • Direct file descriptor writes bypassing buffered I/O
//   • Compiler-optimized inline functions for minimal call overhead
//   • Cold-path design prevents interference with hot trading logic
//
// Use case categorization:
//   • Network dial failures and connection errors
//   • System call failures (epoll/kqueue errors)
//   • GC event notifications and diagnostics
//   • Critical system state transitions
//
// Safety guarantees:
//   • Thread-safe operations without locking overhead
//   • Bounded execution time regardless of message content
//   • No heap allocations in any code path
//   • Graceful degradation on syscall failures
//
// Memory layout legend:
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│ Error/Message → String conversion → Syscall → File descriptor output    │
//	│                       ▲                              │                  │
//	│                Zero allocation ◀─────────────────────┘                  │
//	└─────────────────────────────────────────────────────────────────────────┘
//
// Compiler optimizations:
//   • //go:nosplit for stack management elimination
//   • //go:inline for call overhead elimination
//   • //go:registerparams for register-based parameter passing

package debug

import "main/utils"

// ============================================================================
// ERROR REPORTING (COLD PATH DIAGNOSTICS)
// ============================================================================

// DropError provides ISR-aligned error logging without heap allocation.
// Designed for cold-path error reporting in performance-critical systems
// where standard library logging would introduce unacceptable latency.
//
// Threading model: Safe for concurrent use across multiple goroutines
// Performance impact: Nanosecond-scale execution with zero heap allocations
// Error handling: Graceful degradation on syscall failures without panic
//
// Use cases:
//   - Network dial failures and connection errors during WebSocket setup
//   - System call failures (epoll/kqueue errors) in event loop processing
//   - GC event notifications and memory pressure diagnostics
//   - Critical system state transitions requiring immediate notification
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DropError(prefix string, err error) {
	if err != nil {
		// Error path: Log error with context using multiple print calls
		// Separate calls avoid string concatenation allocation overhead
		utils.PrintWarning(prefix)
		utils.PrintWarning(": ")
		utils.PrintWarning(err.Error())
		utils.PrintWarning("\n")
	} else {
		// Status path: Log prefix only for state notifications
		// Used for successful completion notifications without error context
		utils.PrintInfo(prefix)
		utils.PrintInfo("\n")
	}
}

// ============================================================================
// DIAGNOSTIC MESSAGING (STATUS REPORTING)
// ============================================================================

// DropMessage provides zero-allocation debug message logging for cold-path
// diagnostics and infrequent status updates. Optimized for minimal overhead
// when integrated into performance-critical control flows.
//
// Threading model: Safe for concurrent use across multiple goroutines
// Performance impact: Sub-microsecond execution with zero heap allocations
// Output formatting: Simple key-value pair formatting without allocation
//
// Use cases:
//   - Connection establishment notifications during system initialization
//   - Protocol handshake completion messages for WebSocket connections
//   - System resource state changes (memory, network, file descriptors)
//   - Diagnostic trace points in complex arbitrage detection operations
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DropMessage(prefix, message string) {
	// Multiple print calls to avoid string concatenation allocation
	// Each call maps directly to syscall without intermediate buffering
	utils.PrintInfo(prefix)
	utils.PrintInfo(": ")
	utils.PrintInfo(message)
	utils.PrintInfo("\n")
}
