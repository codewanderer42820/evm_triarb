//go:build darwin
// +build darwin

// main_darwin.go — platform glue: TLS dial → kqueue wait → frame pump.
package main

import (
	"crypto/tls"
	"net"
	"runtime"
	"runtime/debug"
	"syscall"
)

var (
	events   = [1]syscall.Kevent_t{} // reused slice → no allocs
	memstats runtime.MemStats
)

func main() {
	debug.SetGCPercent(-1) // totally manual GC
	runtime.LockOSThread() // low-latency, avoid migrations

	for {
		if err := runPublisher(); err != nil {
			dropError("main loop error", err)
		}

		// soft GC guard-rail
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

func runPublisher() error {
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}
	conn := tls.Client(raw, &tls.Config{ServerName: wsHost})
	defer func() { _ = conn.Close(); _ = raw.Close() }()

	// 1. upgrade → WebSocket → subscribe
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

	// 2. put the TCP fd into kqueue
	tcp := raw.(*net.TCPConn)
	rs, _ := tcp.SyscallConn()
	var fd int
	rs.Control(func(f uintptr) { fd = int(f) })
	_ = syscall.SetNonblock(fd, true)

	kq, _ := syscall.Kqueue()
	change := syscall.Kevent_t{Ident: uint64(fd), Filter: syscall.EVFILT_READ, Flags: syscall.EV_ADD}
	syscall.Kevent(kq, []syscall.Kevent_t{change}, nil, nil)

	// 3. event-driven read loop
	for {
		_, err := syscall.Kevent(kq, nil, events[:], nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			dropError("kqueue wait", err)
			return err
		}

		f, err := readFrame(conn)
		if err != nil {
			dropError("read frame", err)
			return err
		}
		handleFrame(f.Payload)

		// reclaim space in wsBuf
		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
	}
}
