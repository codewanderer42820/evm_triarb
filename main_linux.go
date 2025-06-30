//go:build linux
// +build linux

// main_linux.go — Linux-specific event loop using `epoll` for non-blocking I/O.
// Establishes persistent WebSocket stream, subscribes to logs,
// and processes frames with minimal GC interference and zero allocations.

package main

import (
	"crypto/tls"
	"net"
	"runtime"
	"runtime/debug"
	"syscall"
)

var (
	events   = [1]syscall.EpollEvent{} // reused single-slot buffer for epoll events (no alloc)
	memstats runtime.MemStats          // GC metrics for runtime guardrails
)

func main() {
	// Disable background GC — full manual control to limit jitter
	debug.SetGCPercent(-1)

	// Prevent OS thread migration to reduce latency spikes
	runtime.LockOSThread()

	for {
		// Outer loop retries on network failure or memory violations
		if err := runPublisher(); err != nil {
			dropError("main loop error", err)
		}

		// ───── Manual GC Check ─────
		runtime.ReadMemStats(&memstats)

		if memstats.HeapAlloc > heapSoftLimit {
			// Trigger GC pass, then revert back to disabled state
			debug.SetGCPercent(100)
			runtime.GC()
			debug.SetGCPercent(-1)
			dropError("[GC] heap trimmed", nil)
		}

		if memstats.HeapAlloc > heapHardLimit {
			// Hard failure: crash with visible message
			panic("heap usage exceeded hard cap — leak likely")
		}
	}
}

// runPublisher establishes the full WebSocket stack:
// TCP dial → TLS handshake → WS upgrade → epoll read loop.
//
// Compiler Directives:
//   - nosplit         → avoids call stack metadata overhead
//   - registerparams  → ABI-optimal for performance
//
//go:nosplit
//go:registerparams
func runPublisher() error {
	// ───── Step 1: TCP Connect and TLS Wrap ─────
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}
	conn := tls.Client(raw, &tls.Config{ServerName: wsHost})
	defer func() { _ = conn.Close(); _ = raw.Close() }()

	// ───── Step 2: WebSocket Protocol Upgrade ─────
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

	// ───── Step 3: Get TCP FD and Configure for Epoll ─────
	tcp := raw.(*net.TCPConn)
	rs, _ := tcp.SyscallConn()

	var fd int
	rs.Control(func(f uintptr) { fd = int(f) })

	_ = syscall.SetNonblock(fd, true)

	// ───── Step 4: Create Epoll FD and Register Socket ─────
	efd, _ := syscall.EpollCreate1(0)
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd)}
	syscall.EpollCtl(efd, syscall.EPOLL_CTL_ADD, fd, &ev)

	// ───── Step 5: Epoll Read Loop ─────
	for {
		_, err := syscall.EpollWait(efd, events[:], -1)
		if err == syscall.EINTR {
			continue // retry on signal (e.g. Ctrl+Z)
		}
		if err != nil {
			dropError("epoll wait", err)
			return err
		}

		// ───── Step 6: Parse WebSocket Frame ─────
		f, err := readFrame(conn)
		if err != nil {
			dropError("read frame", err)
			return err
		}
		handleFrame(f.Payload)

		// ───── Step 7: Trim Read Buffer ─────
		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
	}
}
