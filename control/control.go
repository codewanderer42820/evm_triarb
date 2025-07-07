// Package control provides global flags for pinned consumers.
package control

import "time"

var (
	hot        uint32                   // Global hot flag - set on WebSocket activity
	stop       uint32                   // Global stop flag - set to shutdown all consumers
	lastHot    int64                    // Nanosecond timestamp of last activity
	cooldownNs = int64(1 * time.Second) // 1 second cooldown period
)

// SignalActivity sets the global hot flag and updates timestamp.
// Call from WebSocket layer.
//
//go:nosplit
//go:inline
func SignalActivity() {
	hot = 1
	lastHot = time.Now().UnixNano()
}

// PollCooldown checks if hot flag should be cleared based on time.
// Call this inline during hot spinning loops for automatic cooldown.
//
//go:nosplit
//go:inline
func PollCooldown() {
	if hot == 1 && time.Now().UnixNano()-lastHot > cooldownNs {
		hot = 0
	}
}

// Shutdown sets the global stop flag.
//
//go:nosplit
//go:inline
func Shutdown() {
	stop = 1
}

// Flags returns pointers to the global flags.
// Use with PinnedConsumer.
//
//go:nosplit
//go:inline
func Flags() (*uint32, *uint32) {
	return &stop, &hot
}
