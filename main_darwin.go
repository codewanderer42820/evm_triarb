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
// Compiler Directives:
//   - //go:nosplit
//   - //go:registerparams
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
	events   = [1]syscall.Kevent_t{} // reusable single event slot
	memstats runtime.MemStats        // tracked heap stats for GC pressure
)

func main() {
	debug.SetGCPercent(-1) // disable GC entirely (manual only)
	runtime.LockOSThread() // pin this goroutine to avoid preemption

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

// runPublisher establishes the full WS + kqueue + parser pipeline.
//
//go:nosplit
//go:registerparams
func runPublisher() error {
	// ───── Step 1: Dial raw TCP and wrap in TLS ─────
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}
	conn := tls.Client(raw, &tls.Config{ServerName: wsHost})
	defer func() { _ = conn.Close(); _ = raw.Close() }()

	// ───── Step 2: Perform WebSocket Upgrade ─────
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
	tcp := raw.(*net.TCPConn)
	rs, _ := tcp.SyscallConn()
	var fd int
	rs.Control(func(f uintptr) { fd = int(f) })
	_ = syscall.SetNonblock(fd, true)

	// ───── Step 4: Register socket with kqueue ─────
	kq, _ := syscall.Kqueue()
	change := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}
	syscall.Kevent(kq, []syscall.Kevent_t{change}, nil, nil)

	// ───── Step 5: kqueue wait loop ─────
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

		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
	}
}
