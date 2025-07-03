//go:build darwin
// +build darwin

// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: main_darwin.go — macOS ISR event loop (kqueue-powered)
//
// Purpose:
//   - Establishes TLS→WebSocket→kqueue event pipeline on macOS/BSD
//   - Performs hot-path log parsing in a pinned, GC-suppressed goroutine
//
// Notes:
//   - All socket reads handled via syscall-triggered readiness (kqueue)
//   - Buffer rotation and GC are manually controlled for latency determinism
//   - Restart-on-error model ensures resilience without leak propagation
//
// ⚠️ Single-threaded loop — ISR model assumes core pinning and isolation
// ─────────────────────────────────────────────────────────────────────────────

package main

import (
	"crypto/tls"
	"net"
	"runtime"
	"runtime/debug"
	"syscall"
)

var (
	events   = [1]syscall.Kevent_t{} // Reusable single event slot for kqueue
	memstats runtime.MemStats        // Tracked heap stats for GC pressure monitoring
)

//go:inline
//go:registerparams
func main() {
	debug.SetGCPercent(-1) // Disable GC entirely (manual GC control only)
	runtime.LockOSThread() // Pin this goroutine to a specific OS thread to avoid preemption

	for {
		// Step 1: Run the WebSocket publisher pipeline
		if err := runPublisher(); err != nil {
			dropError("main loop error", err) // Log and continue if an error occurs
		}

		// Step 2: Monitor memory stats and manage heap usage
		runtime.ReadMemStats(&memstats)
		if memstats.HeapAlloc > heapSoftLimit {
			// Trigger garbage collection if heap exceeds soft limit
			debug.SetGCPercent(100)
			runtime.GC() // Force a GC cycle
			debug.SetGCPercent(-1)
			dropError("[GC] heap trimmed", nil) // Log GC action
		}
		// Step 3: Panic if heap usage exceeds hard limit (indicating a memory leak)
		if memstats.HeapAlloc > heapHardLimit {
			panic("heap usage exceeded hard cap — leak likely")
		}
	}
}

// runPublisher establishes the full WebSocket + kqueue + parser pipeline.
//
//go:inline
//go:registerparams
func runPublisher() error {
	// ───── Step 1: Dial raw TCP connection and wrap it in TLS ─────
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}
	tlsConfig := &tls.Config{
		ServerName:             wsHost, // Ensure the ServerName is set for proper SNI handling
		SessionTicketsDisabled: false,  // Enable session resumption
	}
	conn := tls.Client(raw, tlsConfig)
	defer func() { _ = conn.Close(); _ = raw.Close() }() // Ensure connections are closed after use

	// ───── Step 2: Perform WebSocket Upgrade ─────
	// Send WebSocket upgrade request and wait for a response
	if _, err := conn.Write(upgradeRequest); err != nil {
		dropError("ws upgrade write", err)
		return err
	}
	if _, err := readHandshake(conn); err != nil {
		dropError("ws handshake", err)
		return err
	}
	if _, err := conn.Write(subscribePacket); err != nil {
		dropError("subscribe write", err)
		return err
	}

	// ───── Step 3: Extract raw FD for kqueue ─────
	// Retrieve file descriptor from the TCP connection for kqueue registration
	tcp := raw.(*net.TCPConn)
	rs, _ := tcp.SyscallConn()
	var fd int
	rs.Control(func(f uintptr) { fd = int(f) })
	_ = syscall.SetNonblock(fd, true) // Set the socket to non-blocking mode

	// ───── Step 4: Register socket with kqueue ─────
	// Register the socket with the kqueue event system for read readiness
	kq, _ := syscall.Kqueue()
	change := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ, // Monitor the socket for readable events
		Flags:  syscall.EV_ADD,
	}
	syscall.Kevent(kq, []syscall.Kevent_t{change}, nil, nil)

	// ───── Step 5: kqueue wait loop ─────
	// Wait for events on kqueue and process frames when they are ready
	for {
		_, err := syscall.Kevent(kq, nil, events[:], nil)
		if err == syscall.EINTR {
			continue // Ignore interrupted system calls and retry
		}
		if err != nil {
			dropError("kqueue wait", err)
			return err
		}

		// Process the frame read from the WebSocket connection
		f, err := readFrame(conn)
		if err != nil {
			dropError("read frame", err)
			return err
		}
		handleFrame(f.Payload) // Handle the parsed frame payload

		// Update WebSocket read state after consuming the frame
		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
	}
}
