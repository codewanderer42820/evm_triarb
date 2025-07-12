package control

import "time"

var (
	// Global coordination flags - accessed by all consumer threads
	hot  uint32 // 1 = active WebSocket traffic, 0 = idle
	stop uint32 // 1 = graceful shutdown, 0 = normal operation

	// Activity timing for automatic cooldown
	lastHot    int64                    // Nanosecond timestamp of last activity
	cooldownNs = int64(1 * time.Second) // System idle threshold
)

// SignalActivity marks the system as active upon receiving market data.
// Called from WebSocket layer with zero heap allocations.
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

// PollCooldown clears hot flag after idle period to prevent CPU spinning.
// Called inline during consumer loops for efficiency.
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

// Shutdown initiates graceful termination across all consumer threads.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Shutdown() {
	stop = 1
}

// Flags returns pointers to control flags for zero-allocation polling.
// Returns: (*stop_flag, *hot_flag)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Flags() (*uint32, *uint32) {
	return &stop, &hot
}
