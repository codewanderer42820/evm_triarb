// ════════════════════════════════════════════════════════════════════════════════════════════════
// 📊 ZERO-ALLOCATION DIAGNOSTICS
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Performance-Safe Logging
//
// Description:
//   Diagnostic logging designed for hot paths where heap allocations are forbidden. Direct
//   syscall writing with guaranteed zero allocations for production debugging.
//
// Performance Characteristics:
//   - Allocations: Zero per log call
//   - Overhead: Direct syscall only
//   - Memory: Stack-only operation
//   - Safety: Hot-path compatible
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package debug

import (
	"main/utils"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ERROR LOGGING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// DropError provides zero-allocation error logging for diagnostic purposes.
// This function is designed for use in cold paths where errors need to be
// logged without impacting the performance of the main processing pipeline.
//
// The implementation avoids string concatenation and heap allocations by
// making multiple write calls. While this increases syscall count, it
// eliminates memory pressure and GC overhead, which is critical for
// maintaining consistent latency in high-frequency trading systems.
//
// Parameters:
//   - prefix: Context identifier for the error (e.g., "ARBITRAGE_ENGINE")
//   - err: The error to log, or nil for status notifications
//
// Usage patterns:
//   - Error case: Outputs "prefix: error message\n" to stderr
//   - Status case: Outputs "prefix\n" to stdout when err is nil
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DropError(prefix string, err error) {
	if err != nil {
		// Error path: Write to stderr with full context
		// Multiple calls avoid string concatenation and heap allocation
		utils.PrintWarning(prefix)
		utils.PrintWarning(": ")
		utils.PrintWarning(err.Error())
		utils.PrintWarning("\n")
	} else {
		// Status path: Simple notification to stdout
		// Used for operational status updates without error context
		utils.PrintInfo(prefix)
		utils.PrintInfo("\n")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MESSAGE LOGGING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// DropMessage provides zero-allocation message logging for diagnostic traces.
// This function enables detailed system monitoring and debugging without
// impacting performance in production environments.
//
// Designed for high-frequency use cases where traditional logging would
// create unacceptable overhead. The zero-allocation guarantee ensures
// that diagnostic instrumentation doesn't degrade system performance.
//
// Parameters:
//   - prefix: Category or source identifier (e.g., "[EVENT]", "[ARBITRAGE]")
//   - message: The diagnostic message to log
//
// Common usage:
//   - Event processing traces
//   - Arbitrage opportunity detection
//   - System state transitions
//   - Performance checkpoints
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DropMessage(prefix, message string) {
	// Direct output to stdout with minimal overhead
	// Format: "prefix: message\n"
	utils.PrintInfo(prefix)
	utils.PrintInfo(": ")
	utils.PrintInfo(message)
	utils.PrintInfo("\n")
}
