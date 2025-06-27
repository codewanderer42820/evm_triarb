//go:build linux
// +build linux

// main_linux.go — Linux-specific event loop using epoll.
// Establishes a TLS WebSocket connection, subscribes to logs,
// and processes frames with minimal GC interference and zero heap churn.

package main

import (
	"crypto/tls"
	"net"
	"runtime"
	"runtime/debug"
	"syscall"
)

var (
	events   = [1]syscall.EpollEvent{} // single-slot reusable epoll event list
	memstats runtime.MemStats          // used for GC diagnostics
)

//go:nosplit
func main() {
	debug.SetGCPercent(-1) // disable background GC for deterministic latency
	runtime.LockOSThread() // hard pin thread to avoid preemption/migration

	for {
		if err := runPublisher(); err != nil {
			dropError("main loop error", err)
		}

		// ───── Soft GC trigger & hard limit ─────
		runtime.ReadMemStats(&memstats)

		if memstats.HeapAlloc > heapSoftLimit {
			debug.SetGCPercent(100)
			runtime.GC()
			debug.SetGCPercent(-1)
			dropError("[GC] heap trimmed", nil)
		}

		if memstats.HeapAlloc > heapHardLimit {
			panic("heap usage exceeded hard cap — leak likely")
		}
	}
}

// runPublisher connects to the WebSocket, sets up epoll, and enters read loop.
//
//go:nosplit
func runPublisher() error {
	// Establish raw TCP connection and TLS wrap
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}
	conn := tls.Client(raw, &tls.Config{ServerName: wsHost})
	defer func() { _ = conn.Close(); _ = raw.Close() }()

	// Perform WebSocket upgrade handshake
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

	// Get TCP socket file descriptor
	tcp := raw.(*net.TCPConn)
	rs, _ := tcp.SyscallConn()

	var fd int
	rs.Control(func(f uintptr) { fd = int(f) })

	// Enable non-blocking mode
	_ = syscall.SetNonblock(fd, true)

	// Create epoll instance and register for readable events
	efd, _ := syscall.EpollCreate1(0)
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd)}
	syscall.EpollCtl(efd, syscall.EPOLL_CTL_ADD, fd, &ev)

	// Epoll-based read loop
	for {
		_, err := syscall.EpollWait(efd, events[:], -1)
		if err == syscall.EINTR {
			continue // retry on signal
		}
		if err != nil {
			dropError("epoll wait", err)
			return err
		}

		// Read WebSocket frame and handle log event
		f, err := readFrame(conn)
		if err != nil {
			dropError("read frame", err)
			return err
		}
		handleFrame(f.Payload)

		// Shift wsBuf to reclaim space
		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
	}
}
