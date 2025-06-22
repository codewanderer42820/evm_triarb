//go:build linux
// +build linux

// main_linux.go — platform glue: TLS dial → epoll wait → frame pump.
package main

import (
	"crypto/tls"
	"net"
	"runtime"
	"runtime/debug"
	"syscall"
)

var (
	events   = [1]syscall.EpollEvent{} // reused slice → no allocs
	memstats runtime.MemStats
)

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

func runPublisher() error {
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}
	conn := tls.Client(raw, &tls.Config{ServerName: wsHost})
	defer func() { _ = conn.Close(); _ = raw.Close() }()

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

	// Epoll plumbing
	tcp := raw.(*net.TCPConn)
	rs, _ := tcp.SyscallConn()
	var fd int
	rs.Control(func(f uintptr) { fd = int(f) })
	_ = syscall.SetNonblock(fd, true)

	efd, _ := syscall.EpollCreate1(0)
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd)}
	syscall.EpollCtl(efd, syscall.EPOLL_CTL_ADD, fd, &ev)

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
