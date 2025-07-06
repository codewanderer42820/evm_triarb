// Package control provides global flags for pinned consumers.
package control

var (
	hot  uint32 // Global hot flag - set on WebSocket activity
	stop uint32 // Global stop flag - set to shutdown all consumers
)

// SignalActivity sets the global hot flag.
// Call from WebSocket layer.
func SignalActivity() {
	hot = 1
}

// ClearHot clears the global hot flag.
// Call periodically to cool down consumers.
func ClearHot() {
	hot = 0
}

// Shutdown sets the global stop flag.
func Shutdown() {
	stop = 1
}

// Flags returns pointers to the global flags.
// Use with PinnedConsumer.
func Flags() (*uint32, *uint32) {
	return &stop, &hot
}
