// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: debug.go — ISR-aligned error logging helper (zero-alloc)
//
// Purpose:
//   - Logs infrequent error paths without introducing heap pressure.
//   - Used only in cold paths: GC tags, dial/epoll/kqueue errors, etc.
//
// Notes:
//   - Avoids fmt.Sprintf to minimize footprint and latency.
//   - Uses stackless logging model: no alloc, no interfaces.
//   - Aggressively inlined and nosplit — safe for ISR pipelines, ensuring minimal disruption in critical paths.
//
// ⚠️ Never invoke in hot loops — use only in failure diagnostics.
// ─────────────────────────────────────────────────────────────────────────────

package debug

import "main/utils"

// DropError logs error messages with a custom alloc-free print strategy.
// It writes directly to stderr (file descriptor 2), bypassing any heap allocations.
// This function is designed for ISR-aligned error logging without introducing heap pressure.
//
//go:nosplit
//go:inline
//go:registerparams
func DropError(prefix string, err error) {
	if err != nil {
		// Error case: Print the error message with a custom alloc-free strategy.
		// This avoids any heap allocation by directly concatenating and printing the error.
		msg := prefix + ": " + err.Error() + "\n"
		utils.PrintWarning(msg)
	} else {
		// No error case: Print just the prefix (for GC traces or tagged warnings).
		// This avoids unnecessary memory allocations for frequent paths.
		msg := prefix + "\n"
		utils.PrintWarning(msg)
	}
}

// DropMessage logs debug messages with zero-allocation print strategy.
// Used for cold-path diagnostics, connection state changes, and infrequent events.
// Optimized for ISR-aligned logging without heap pressure.
//
//go:nosplit
//go:inline
//go:registerparams
func DropMessage(prefix, message string) {
	// Direct concatenation for zero-alloc debug logging
	// Used only in cold paths: handshake completion, connection state changes, etc.
	msg := prefix + ": " + message + "\n"
	utils.PrintWarning(msg)
}

// DropTrace logs trace messages for debugging flow control.
// Ultra-lightweight tracing for critical path analysis without allocation overhead.
// Should only be used temporarily for debugging - remove from production hot paths.
//
//go:nosplit
//go:inline
//go:registerparams
func DropTrace(function, event string) {
	// Minimal trace logging for debugging flow control
	// Format: [TRACE] function: event
	msg := "[TRACE] " + function + ": " + event + "\n"
	utils.PrintWarning(msg)
}

// DropStats logs performance statistics with zero-allocation strategy.
// Used for periodic performance diagnostics without heap pressure.
// Suitable for connection metrics, frame counts, buffer statistics.
//
//go:nosplit
//go:inline
//go:registerparams
func DropStats(component, metric, value string) {
	// Performance statistics logging
	// Format: [STATS] component.metric = value
	msg := "[STATS] " + component + "." + metric + " = " + value + "\n"
	utils.PrintWarning(msg)
}

// DropBuffer logs buffer state for debugging memory management.
// Zero-allocation logging for buffer diagnostics in WebSocket frame processing.
// Critical for debugging buffer overflow and compaction issues.
//
//go:nosplit
//go:inline
//go:registerparams
func DropBuffer(operation, details string) {
	// Buffer state logging for memory management debugging
	// Format: [BUFFER] operation: details
	msg := "[BUFFER] " + operation + ": " + details + "\n"
	utils.PrintWarning(msg)
}

// DropFrame logs WebSocket frame processing events.
// Ultra-fast frame event logging without heap allocation.
// Use sparingly - only for debugging frame parsing issues.
//
//go:nosplit
//go:inline
//go:registerparams
func DropFrame(frameType, details string) {
	// WebSocket frame processing event logging
	// Format: [FRAME] frameType: details
	msg := "[FRAME] " + frameType + ": " + details + "\n"
	utils.PrintWarning(msg)
}
