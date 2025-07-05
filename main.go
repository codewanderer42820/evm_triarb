// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: main.go — ISR-grade Cross-Platform Low Latency WebSocket (blocking reads)
//
// Purpose:
//   - Implements an ISR-grade, low-latency WebSocket ingestion pipeline using blocking reads.
//   - Cross-platform with optimized TCP settings for maximum throughput.
//
// Key Features:
//   - Utilizes a simple blocking conn.Read() for optimal throughput.
//   - Maintains control over garbage collection and thread pinning for stability.
//   - Eliminates event system overhead to minimize latency.
//   - Ultra-high performance frame processing with <100ns target latency.
//
// ⚠️ Single-threaded blocking loop — assumes execution on a dedicated core.
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
	// Disable the automatic garbage collector and manage it manually for better performance.
	// By setting the GC percentage to -1, we effectively disable the default GC behavior.
	rtdebug.SetGCPercent(-1)

	// Lock the goroutine to a specific operating system thread. This ensures that the goroutine
	// is always executed on the same OS thread, which helps to avoid latency introduced by thread migrations.
	runtime.LockOSThread()

	// Start the main loop where WebSocket publisher is executed continuously.
	for {
		// Run the WebSocket publisher. If there's an error, log it and continue to the next iteration.
		if err := runPublisher(); err != nil {
			debug.DropError("main loop error", err) // Log the error and continue on failure.
		}

		// Read memory stats to monitor heap allocation and trigger garbage collection if needed.
		runtime.ReadMemStats(&memstats)
		if memstats.HeapAlloc > constants.HeapSoftLimit {
			// Trigger garbage collection if heap allocation exceeds the soft memory limit.
			rtdebug.SetGCPercent(100)                 // Set GC to run at 100% for manual garbage collection.
			runtime.GC()                              // Force garbage collection.
			rtdebug.SetGCPercent(-1)                  // Disable GC after manual collection.
			debug.DropError("[GC] heap trimmed", nil) // Log the GC activity.
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
	// Step 1: Establish a raw TCP connection to the WebSocket server.
	// This is the first step in setting up a WebSocket connection over TCP.
	raw, err := net.Dial("tcp", constants.WsDialAddr)
	if err != nil {
		debug.DropError("tcp dial", err)
		return err
	}

	// Cast the raw connection to a TCP connection to apply TCP-specific settings.
	tcpConn := raw.(*net.TCPConn)

	// Step 2: Configure TCP settings before obtaining the file descriptor.
	tcpConn.SetNoDelay(true)                       // Disable Nagle's algorithm for low-latency communication.
	tcpConn.SetReadBuffer(constants.MaxFrameSize)  // Set read buffer size for the connection.
	tcpConn.SetWriteBuffer(constants.MaxFrameSize) // Set write buffer size for the connection.

	// Apply platform-specific optimizations to the socket for better performance.
	if rawFile, err := tcpConn.File(); err == nil {
		fd := int(rawFile.Fd())
		defer rawFile.Close() // Ensure the file descriptor is closed after use.

		// Apply platform-specific optimizations for the socket.
		applySocketOptimizations(fd)
	}

	// Step 3: Wrap the raw TCP connection with TLS for secure WebSocket communication.
	// TLS is used to encrypt the WebSocket traffic to ensure secure communication.
	tlsConfig := &tls.Config{
		ServerName:             constants.WsHost, // Set ServerName for correct SNI (Server Name Indication) handling.
		SessionTicketsDisabled: false,            // Enable session resumption for faster reconnections.
	}
	conn := tls.Client(raw, tlsConfig)  // Create a TLS connection.
	defer func() { _ = conn.Close() }() // Ensure the TLS connection is closed when done.

	// Step 4: Perform WebSocket upgrade handshake with high-performance API
	// Send upgrade request to initiate WebSocket connection
	if _, err := conn.Write(ws.GetUpgradeRequest()); err != nil {
		debug.DropError("ws upgrade write", err)
		return err
	}

	// Process handshake response with zero-allocation parser
	if err := ws.ProcessHandshake(conn); err != nil {
		debug.DropError("ws handshake", err)
		return err
	}

	// Send subscription packet to start receiving data
	if _, err := conn.Write(ws.GetSubscribePacket()); err != nil {
		debug.DropError("subscribe write", err)
		return err
	}

	// Step 5: Enter ultra-high performance frame processing loop
	// This loop achieves <100ns frame processing latency through:
	//   - Zero-allocation frame ingestion
	//   - Cache-aligned data structures
	//   - Branch-prediction optimized parsing
	//   - Direct memory operations with unsafe pointers
	for {
		// Ingest frame with zero-allocation, zero-copy processing
		frame, err := ws.IngestFrame(conn)
		if err != nil {
			debug.DropError("frame ingestion", err)
			return err
		}

		// Process frame payload with direct memory access
		// ExtractPayload() returns a slice pointing directly to the stability buffer
		// Data is valid until the next IngestFrame() call
		handleFrame(frame.ExtractPayload())
	}
}

// applySocketOptimizations applies platform-specific socket optimizations for MAXIMUM performance
//
//go:inline
//go:registerparams
func applySocketOptimizations(fd int) {
	// TCP_NODELAY is already set via SetNoDelay(), but ensuring it's set at syscall level
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)

	// Platform-specific optimizations
	switch runtime.GOOS {
	case "linux":
		// TCP_QUICKACK for immediate ACK (Linux-specific) - disable delayed ACK
		//syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_QUICKACK, 1)

		// SO_BUSY_POLL for reduced latency - set to 1 microsecond (minimum)
		// This requires kernel support and appropriate privileges
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1) // SO_BUSY_POLL = 46

		// TCP_USER_TIMEOUT for faster connection failure detection (1 second)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 18, 1000) // TCP_USER_TIMEOUT = 18

		// SO_PRIORITY for higher socket priority (requires CAP_NET_ADMIN)
		//syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_PRIORITY, 7)

		// TCP_THIN_LINEAR_TIMEOUTS for faster retransmission
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 16, 1) // TCP_THIN_LINEAR_TIMEOUTS = 16

		// TCP_THIN_DUPACK for faster duplicate ACK handling
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 17, 1) // TCP_THIN_DUPACK = 17

		// SO_RCVBUF and SO_SNDBUF - set to exact frame size for minimal buffering
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)

		// TCP_CONGESTION - use BBR if available, otherwise CUBIC
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // TCP_CONGESTION = 13

		// SO_REUSEADDR for faster port reuse
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)

		// SO_KEEPALIVE with aggressive settings
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)
		//syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, 1)  // Start after 1 second
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 1) // Interval 1 second
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 3)   // 3 probes max

	case "darwin":
		// TCP_NODELAY is already set
		// macOS doesn't have TCP_QUICKACK, but we can optimize other settings

		// SO_PRIORITY for higher socket priority
		//syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_PRIORITY, 7)

		// TCP_KEEPALIVE with aggressive settings
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x10, 1)  // TCP_KEEPIDLE equivalent
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x101, 1) // TCP_KEEPINTVL equivalent
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x102, 3) // TCP_KEEPCNT equivalent

		// SO_RCVBUF and SO_SNDBUF
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)

		// SO_REUSEADDR for faster port reuse
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)

		// TCP_NOPUSH disable for immediate sending (opposite of TCP_CORK)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 4, 0) // TCP_NOPUSH = 4

	case "windows":
		// TCP_NODELAY is already set via SetNoDelay()
		// Windows has different socket option constants

		// SO_PRIORITY equivalent (Windows uses QoS)
		// This would require WinSock2 QoS API calls

		// Aggressive keep-alive settings
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)

		// Buffer sizes
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)

		// SO_REUSEADDR
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)

		// Windows-specific: SO_EXCLUSIVEADDRUSE for performance
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x0004, 1) // SO_EXCLUSIVEADDRUSE
	}
}
