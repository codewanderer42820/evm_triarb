//go:build linux
// +build linux

// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: main_linux.go — Linux ISR event loop (epoll-powered)
//
// Purpose:
//   - Performs zero-GC, core-pinned WebSocket ingestion loop using epoll
//   - Mirrors Darwin variant with platform-specific event source
//
// Notes:
//   - Epoll is edge-triggered, syscall.EPOLLIN-driven read dispatch
//   - Buffer logic and parser flow are identical to macOS (see main_darwin.go)
//   - GC is disabled and manually re-enabled when limits are breached
//
// ⚠️ Assumes dedicated core. Thread is locked to CPU.
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
	events   = [1]syscall.EpollEvent{} // Single reusable epoll event structure
	memstats runtime.MemStats          // Used to track heap memory pressure
)

//go:inline
//go:registerparams
func main() {
	debug.SetGCPercent(-1) // Disable GC to manually control memory management
	runtime.LockOSThread() // Lock the current goroutine to a specific OS thread (CPU pinning)

	for {
		// Run the WebSocket ingestion and parsing pipeline
		if err := runPublisher(); err != nil {
			dropError("main loop error", err) // Log any errors encountered in the main loop
		}

		// Track heap memory usage and trigger GC when limits are exceeded
		runtime.ReadMemStats(&memstats)
		if memstats.HeapAlloc > heapSoftLimit {
			// Trigger garbage collection if memory exceeds the soft limit
			debug.SetGCPercent(100)
			runtime.GC()                        // Perform GC cycle
			debug.SetGCPercent(-1)              // Disable GC again
			dropError("[GC] heap trimmed", nil) // Log heap trimming action
		}
		if memstats.HeapAlloc > heapHardLimit {
			panic("heap usage exceeded hard cap — leak likely") // Panic if heap usage exceeds hard limit
		}
	}
}

// runPublisher builds the TLS→WebSocket→epoll read pipeline
//
//go:inline
//go:registerparams
func runPublisher() error {
	// ───── Step 1: Dial TCP and wrap the connection in TLS ─────
	// Establish a TCP connection and wrap it in TLS for secure communication
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}
	conn := tls.Client(raw, &tls.Config{ServerName: wsHost})
	defer func() { _ = conn.Close(); _ = raw.Close() }() // Ensure proper closure of connections

	// ───── Step 2: Perform WebSocket Upgrade ─────
	// Upgrade the connection to WebSocket protocol and send subscription request
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

	// ───── Step 3: Setup epoll monitoring ─────
	// Retrieve the file descriptor and set it to non-blocking mode for epoll monitoring
	tcp := raw.(*net.TCPConn)
	rs, _ := tcp.SyscallConn()
	var fd int
	rs.Control(func(f uintptr) { fd = int(f) })
	_ = syscall.SetNonblock(fd, true) // Set socket to non-blocking mode

	// Create an epoll instance and add the socket file descriptor for monitoring
	efd, _ := syscall.EpollCreate1(0)
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd)}
	syscall.EpollCtl(efd, syscall.EPOLL_CTL_ADD, fd, &ev)

	// ───── Step 4: Epoll dispatch loop ─────
	// Wait for events on the epoll instance and process frames when available
	for {
		_, err := syscall.EpollWait(efd, events[:], -1) // Wait for I/O events
		if err == syscall.EINTR {
			continue // Ignore interrupted system calls and retry
		}
		if err != nil {
			dropError("epoll wait", err) // Log any epoll wait errors
			return err
		}

		// Read a frame from the WebSocket connection and handle it
		f, err := readFrame(conn)
		if err != nil {
			dropError("read frame", err) // Log any frame reading errors
			return err
		}
		handleFrame(f.Payload) // Process the frame payload

		// Update the WebSocket state after processing the frame
		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
	}
}
