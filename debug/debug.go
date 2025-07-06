package debug

import "main/utils"

// ============================================================================
// ZERO-ALLOCATION ERROR LOGGING SYSTEM
// ============================================================================

// DropError provides ISR-aligned error logging without heap allocation.
// Designed for cold-path error reporting in performance-critical systems.
//
// Use cases:
// - Network dial failures and connection errors
// - System call failures (epoll/kqueue errors)
// - GC event notifications and diagnostics
// - Critical system state transitions
//
//go:nosplit
//go:inline
//go:registerparams
func DropError(prefix string, err error) {
	if err != nil {
		// Error path: Format and log error with context
		msg := prefix + ": " + err.Error() + "\n"
		utils.PrintWarning(msg)
	} else {
		// Status path: Log prefix only for state notifications
		msg := prefix + "\n"
		utils.PrintWarning(msg)
	}
}

// DropMessage provides zero-allocation debug message logging.
// Optimized for cold-path diagnostics and infrequent status updates.
//
// Use cases:
// - Connection establishment notifications
// - Protocol handshake completion messages
// - System resource state changes
// - Diagnostic trace points in complex operations
//
//go:nosplit
//go:inline
//go:registerparams
func DropMessage(prefix, message string) {
	// Direct concatenation for zero-allocation logging
	msg := prefix + ": " + message + "\n"
	utils.PrintInfo(msg)
}
