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
//   - GC is disabled and manually re-enabled when limits breached
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
	events   = [1]syscall.EpollEvent{} // single reusable epoll event
	memstats runtime.MemStats          // used for heap pressure tracking
)

//go:inline
//go:registerparams
func main() {
	debug.SetGCPercent(-1)
	runtime.LockOSThread()

	for {
		if err := runPublisher(); err != nil {
			dropError("main loop error", err)
		}

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

// runPublisher builds the TLS→WS→epoll read pipeline
//
//go:inline
//go:registerparams
func runPublisher() error {
	// ───── Step 1: Dial TCP and wrap in TLS ─────
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}
	conn := tls.Client(raw, &tls.Config{ServerName: wsHost})
	defer func() { _ = conn.Close(); _ = raw.Close() }()

	// ───── Step 2: WebSocket Upgrade ─────
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
	tcp := raw.(*net.TCPConn)
	rs, _ := tcp.SyscallConn()
	var fd int
	rs.Control(func(f uintptr) { fd = int(f) })
	_ = syscall.SetNonblock(fd, true)

	efd, _ := syscall.EpollCreate1(0)
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd)}
	syscall.EpollCtl(efd, syscall.EPOLL_CTL_ADD, fd, &ev)

	// ───── Step 4: Epoll dispatch loop ─────
	for {
		_, err := syscall.EpollWait(efd, events[:], -1)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			dropError("epoll wait", err)
			return err
		}

		f, err := readFrame(conn)
		if err != nil {
			dropError("read frame", err)
			return err
		}
		handleFrame(f.Payload)

		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
	}
}
