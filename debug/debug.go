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
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DropError(prefix string, err error) {
	if err != nil {
		// Error path: Log error with context using multiple print calls
		utils.PrintWarning(prefix)
		utils.PrintWarning(": ")
		utils.PrintWarning(err.Error())
		utils.PrintWarning("\n")
	} else {
		// Status path: Log prefix only for state notifications
		utils.PrintInfo(prefix)
		utils.PrintInfo("\n")
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
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DropMessage(prefix, message string) {
	// Multiple print calls to avoid string concatenation allocation
	utils.PrintInfo(prefix)
	utils.PrintInfo(": ")
	utils.PrintInfo(message)
	utils.PrintInfo("\n")
}
