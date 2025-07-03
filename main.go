// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: main.go — Cross-Platform Low Latency WebSocket (blocking reads)
//
// Purpose:
//   - Implements a low-latency WebSocket ingestion pipeline using blocking reads.
//   - Cross-platform with optimized TCP settings for maximum throughput.
//
// Key Features:
//   - Utilizes a simple blocking conn.Read() for optimal throughput.
//   - Maintains control over garbage collection and thread pinning for stability.
//   - Eliminates event system overhead to minimize latency.
//
// ⚠️ Single-threaded blocking loop — assumes execution on a dedicated core.
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
	memstats runtime.MemStats // Holds memory statistics to monitor heap usage
)

// Frame represents a WebSocket frame structure.
//
//go:notinheap
//go:align 64
type Frame struct {
	Payload []byte // Raw frame payload
	End     int    // End index of the payload
}

//go:inline
//go:registerparams
func main() {
	// Disable GC and manage it manually for precise control over memory.
	debug.SetGCPercent(-1)

	// Lock this goroutine to a specific OS thread for performance consistency.
	runtime.LockOSThread()

	for {
		// Run the WebSocket publisher pipeline.
		if err := runPublisher(); err != nil {
			dropError("main loop error", err) // Log and continue on error
		}

		// Monitor and manage memory usage.
		runtime.ReadMemStats(&memstats)
		if memstats.HeapAlloc > heapSoftLimit {
			// Trigger garbage collection if heap exceeds soft limit.
			debug.SetGCPercent(100)
			runtime.GC() // Force garbage collection cycle.
			debug.SetGCPercent(-1)
			dropError("[GC] heap trimmed", nil) // Log GC activity.
		}

		// Panic if heap exceeds hard memory limit (potential memory leak).
		if memstats.HeapAlloc > heapHardLimit {
			panic("heap usage exceeded hard cap — leak likely detected")
		}
	}
}

// runPublisher establishes a WebSocket connection and handles the blocking read loop.
//
//go:inline
//go:registerparams
func runPublisher() error {
	// Step 1: Establish raw TCP connection to WebSocket server.
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}

	tcpConn := raw.(*net.TCPConn)

	// Step 2: Configure TCP settings before obtaining the file descriptor.
	tcpConn.SetNoDelay(true) // Disable Nagle's algorithm for low-latency communication.
	tcpConn.SetReadBuffer(maxFrameSize)
	tcpConn.SetWriteBuffer(maxFrameSize)

	// Apply platform-specific socket optimizations for better performance.
	if rawFile, err := tcpConn.File(); err == nil {
		fd := int(rawFile.Fd())
		defer rawFile.Close() // Ensure the file descriptor is closed after use.

		// Apply platform-specific optimizations for the socket.
		applySocketOptimizations(fd)
	}

	// Step 3: Wrap the raw connection with TLS for secure WebSocket communication.
	tlsConfig := &tls.Config{
		ServerName:             wsHost, // Set ServerName for correct SNI handling.
		SessionTicketsDisabled: false,  // Enable session resumption for faster connections.
	}
	conn := tls.Client(raw, tlsConfig)
	defer func() { _ = conn.Close() }() // Ensure the TLS connection is closed when done.

	// Step 4: Perform WebSocket upgrade handshake.
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

	// Step 5: Block on reading WebSocket frames with minimal overhead.
	for {
		// Read the frame from the WebSocket connection.
		f, err := readFrame(conn)
		if err != nil {
			dropError("read frame", err)
			return err
		}

		// Process the frame immediately to minimize delay.
		handleFrame(f.Payload)

		// Update the WebSocket read state after processing the frame.
		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
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
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, maxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, maxFrameSize)

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
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, maxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, maxFrameSize)

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
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, maxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, maxFrameSize)

		// SO_REUSEADDR
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)

		// Windows-specific: SO_EXCLUSIVEADDRUSE for performance
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x0004, 1) // SO_EXCLUSIVEADDRUSE
	}
}
