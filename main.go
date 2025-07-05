// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: main.go — Debug Version with handleFrame Implementation
// ─────────────────────────────────────────────────────────────────────────────

package main

import (
	"crypto/tls"
	"main/constants"
	"main/debug"
	"main/ws"
	"net"
	"runtime"
	rtdebug "runtime/debug"
	"syscall"
)

// memstats holds memory statistics for tracking heap usage and managing garbage collection.
var (
	memstats runtime.MemStats // Holds memory statistics to monitor heap usage
)

// main is the entry point for the program. It initializes garbage collection control and runs the main event loop.
//
//go:inline
//go:registerparams
func main() {
	debug.DropMessage("STARTUP", "initializing DEX arbitrage system")

	// Disable the automatic garbage collector and manage it manually for better performance.
	rtdebug.SetGCPercent(-1)

	// Lock the goroutine to a specific operating system thread.
	runtime.LockOSThread()

	// Start the main loop where WebSocket publisher is executed continuously.
	for {
		debug.DropMessage("LOOP", "starting WebSocket connection attempt")

		// Run the WebSocket publisher. If there's an error, log it and continue to the next iteration.
		if err := runPublisher(); err != nil {
			debug.DropError("main loop error", err) // Log the error and continue on failure.
		}

		// Read memory stats to monitor heap allocation and trigger garbage collection if needed.
		runtime.ReadMemStats(&memstats)
		if memstats.HeapAlloc > constants.HeapSoftLimit {
			rtdebug.SetGCPercent(100)
			runtime.GC()
			rtdebug.SetGCPercent(-1)
			debug.DropError("[GC] heap trimmed", nil)
		}

		// If memory allocation exceeds the hard limit, panic to indicate a potential memory leak.
		if memstats.HeapAlloc > constants.HeapHardLimit {
			panic("heap usage exceeded hard cap — leak likely detected")
		}
	}
}

// runPublisher establishes a WebSocket connection, performs the WebSocket handshake, and continuously processes frames.
//
//go:inline
//go:registerparams
func runPublisher() error {
	debug.DropMessage("TCP", "connecting to "+constants.WsDialAddr)

	// Step 1: Establish a raw TCP connection to the WebSocket server.
	raw, err := net.Dial("tcp", constants.WsDialAddr)
	if err != nil {
		debug.DropError("tcp dial", err)
		return err
	}

	debug.DropMessage("TCP", "connection established")

	// Cast the raw connection to a TCP connection to apply TCP-specific settings.
	tcpConn := raw.(*net.TCPConn)

	// Step 2: Configure TCP settings before obtaining the file descriptor.
	tcpConn.SetNoDelay(true)
	tcpConn.SetReadBuffer(constants.MaxFrameSize)
	tcpConn.SetWriteBuffer(constants.MaxFrameSize)

	// Apply platform-specific optimizations to the socket for better performance.
	if rawFile, err := tcpConn.File(); err == nil {
		fd := int(rawFile.Fd())
		defer rawFile.Close()
		applySocketOptimizations(fd)
		debug.DropMessage("TCP", "socket optimizations applied")
	}

	// Step 3: Wrap the raw TCP connection with TLS for secure WebSocket communication.
	tlsConfig := &tls.Config{
		ServerName:             constants.WsHost,
		SessionTicketsDisabled: false,
	}
	conn := tls.Client(raw, tlsConfig)
	defer func() { _ = conn.Close() }()

	debug.DropMessage("TLS", "encrypted connection established")

	// Step 4: Perform WebSocket upgrade handshake
	debug.DropMessage("WS", "sending upgrade request")
	if _, err := conn.Write(ws.GetUpgradeRequest()); err != nil {
		debug.DropError("ws upgrade write", err)
		return err
	}

	debug.DropMessage("WS", "processing handshake response")
	if err := ws.ProcessHandshake(conn); err != nil {
		debug.DropError("ws handshake", err)
		return err
	}

	debug.DropMessage("WS", "sending subscribe packet")
	if _, err := conn.Write(ws.GetSubscribePacket()); err != nil {
		debug.DropError("subscribe write", err)
		return err
	}

	debug.DropMessage("WS", "entering frame processing loop")

	// Step 5: Enter ultra-high performance frame processing loop
	frameCount := 0
	for {
		// Ingest frame with zero-allocation, zero-copy processing
		frame, err := ws.IngestFrame(conn)
		if err != nil {
			debug.DropError("frame ingestion", err)
			return err
		}

		frameCount++
		if frameCount%1000 == 0 {
			debug.DropMessage("PERF", "processed 1000 frames")
		}

		// Process frame payload with direct memory access
		handleFrame(frame.ExtractPayload())
	}
}

// applySocketOptimizations applies platform-specific socket optimizations for MAXIMUM performance
//
//go:inline
//go:registerparams
func applySocketOptimizations(fd int) {
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)

	switch runtime.GOOS {
	case "linux":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)     // SO_BUSY_POLL
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 18, 1000) // TCP_USER_TIMEOUT
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 16, 1)    // TCP_THIN_LINEAR_TIMEOUTS
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 17, 1)    // TCP_THIN_DUPACK
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // TCP_CONGESTION
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 1)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 3)

	case "darwin":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x10, 1)  // TCP_KEEPIDLE equivalent
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x101, 1) // TCP_KEEPINTVL equivalent
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x102, 3) // TCP_KEEPCNT equivalent
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 4, 0) // TCP_NOPUSH

	case "windows":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x0004, 1) // SO_EXCLUSIVEADDRUSE
	}
}
