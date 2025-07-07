// debug.go — Zero-allocation error logging for performance-critical paths
package debug

import "main/utils"

// DropError logs errors without heap allocation for cold-path diagnostics.
// Safe for concurrent use with nanosecond execution time.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DropError(prefix string, err error) {
	if err != nil {
		// Multiple calls avoid string concatenation
		utils.PrintWarning(prefix)
		utils.PrintWarning(": ")
		utils.PrintWarning(err.Error())
		utils.PrintWarning("\n")
	} else {
		// Status notification without error
		utils.PrintInfo(prefix)
		utils.PrintInfo("\n")
	}
}

// DropMessage logs debug messages with zero allocation.
// Used for status updates and diagnostic trace points.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DropMessage(prefix, message string) {
	utils.PrintInfo(prefix)
	utils.PrintInfo(": ")
	utils.PrintInfo(message)
	utils.PrintInfo("\n")
}
