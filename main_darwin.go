//go:build darwin
// +build darwin

// main_darwin.go — Darwin-specific event loop using `kqueue`.
// This variant is tailored for low-latency I/O on macOS/BSD platforms.
// Establishes a persistent WebSocket connection, subscribes to logs,
// and processes incoming frames via zero-copy, event-driven logic.

package main

import (
	"crypto/tls"
	"net"
	"runtime"
	"runtime/debug"
	"syscall"
)

var (
	events   = [1]syscall.Kevent_t{} // single-element reused event slice for `kqueue` (no alloc)
	memstats runtime.MemStats        // reused struct for GC guardrail metrics
)

func main() {
	// Disable automatic GC to prevent pause noise
	debug.SetGCPercent(-1)

	// Pin current goroutine to avoid preemption or OS migration
	runtime.LockOSThread()

	for {
		// Restart on error — outer loop acts as fail-safe
		if err := runPublisher(); err != nil {
			dropError("main loop error", err)
		}

		// ───── Soft GC Trigger ─────
		runtime.ReadMemStats(&memstats)

		if memstats.HeapAlloc > heapSoftLimit {
			// Soft limit exceeded — re-enable GC briefly
			debug.SetGCPercent(100)
			runtime.GC()
			debug.SetGCPercent(-1)
			dropError("[GC] heap trimmed", nil)
		}

		if memstats.HeapAlloc > heapHardLimit {
			// Hard failure — program must exit
			panic("heap usage exceeded hard cap — leak likely")
		}
	}
}

// runPublisher sets up the WebSocket transport, registers for `kqueue` events,
// and processes log frames in-place via readFrame → handleFrame.
//
// Compiler Directives:
//   - nosplit         → removes stack frame check overhead
//
//go:nosplit
func runPublisher() error {
	// ───── Step 1: Dial TCP + wrap with TLS ─────
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

	// ───── Step 3: Extract TCP FD and configure non-blocking ─────
	tcp := raw.(*net.TCPConn)
	rs, _ := tcp.SyscallConn()

	var fd int
	rs.Control(func(f uintptr) { fd = int(f) })

	_ = syscall.SetNonblock(fd, true)

	// ───── Step 4: Create + Register kqueue ─────
	kq, _ := syscall.Kqueue()
	change := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}
	syscall.Kevent(kq, []syscall.Kevent_t{change}, nil, nil)

	// ───── Step 5: Event Loop ─────
	for {
		_, err := syscall.Kevent(kq, nil, events[:], nil)
		if err == syscall.EINTR {
			continue // retry on signal interruption
		}
		if err != nil {
			dropError("kqueue wait", err)
			return err
		}

		// ───── Step 6: Read + Parse Frame ─────
		f, err := readFrame(conn)
		if err != nil {
			dropError("read frame", err)
			return err
		}
		handleFrame(f.Payload)

		// ───── Step 7: Advance wsBuf Window ─────
		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
	}
}
