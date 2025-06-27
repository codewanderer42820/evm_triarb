//go:build darwin
// +build darwin

// main_darwin.go — Darwin-specific event loop using kqueue.
// Establishes a TLS WebSocket connection, subscribes to logs,
// and processes incoming frames with zero allocations.

package main

import (
	"crypto/tls"
	"net"
	"runtime"
	"runtime/debug"
	"syscall"
)

var (
	events   = [1]syscall.Kevent_t{} // single-element reused slice for kqueue → no alloc
	memstats runtime.MemStats        // used for GC soft/hard guardrails
)

//go:nosplit
func main() {
	debug.SetGCPercent(-1) // disable background GC (manual only)
	runtime.LockOSThread() // pin thread to prevent latency spikes from migration

	for {
		if err := runPublisher(); err != nil {
			dropError("main loop error", err)
		}

		// ───── Soft heap guard ─────
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

// runPublisher establishes the TCP/TLS connection, upgrades to WebSocket,
// registers a kqueue event, and enters the read loop.
//
//go:nosplit
func runPublisher() error {
	// Dial TCP → upgrade to TLS
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}
	conn := tls.Client(raw, &tls.Config{ServerName: wsHost})
	defer func() { _ = conn.Close(); _ = raw.Close() }()

	// Perform WebSocket upgrade
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

	// Register TCP fd in kqueue
	tcp := raw.(*net.TCPConn)
	rs, _ := tcp.SyscallConn()

	var fd int
	rs.Control(func(f uintptr) { fd = int(f) }) // extract file descriptor

	_ = syscall.SetNonblock(fd, true) // enable non-blocking mode

	kq, _ := syscall.Kqueue()
	change := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}
	syscall.Kevent(kq, []syscall.Kevent_t{change}, nil, nil)

	// Event-driven read loop
	for {
		_, err := syscall.Kevent(kq, nil, events[:], nil)
		if err == syscall.EINTR {
			continue // retry on signal
		}
		if err != nil {
			dropError("kqueue wait", err)
			return err
		}

		// Read and parse WebSocket frame
		f, err := readFrame(conn)
		if err != nil {
			dropError("read frame", err)
			return err
		}
		handleFrame(f.Payload)

		// Reclaim used buffer space
		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
	}
}
