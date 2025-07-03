// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: main.go — Cross-Platform Low Latency WebSocket (blocking reads)
//
// Purpose:
//   - Minimal latency WebSocket ingestion using blocking reads
//   - Cross-platform implementation with optimized TCP settings
//
// Notes:
//   - Uses simple blocking conn.Read() for maximum throughput
//   - Maintains GC control and thread pinning for consistency
//   - Eliminates all event system overhead for lowest latency
//
// ⚠️ Single-threaded blocking loop — assumes dedicated core
// ─────────────────────────────────────────────────────────────────────────────

package main

import (
	"crypto/tls"
	"net"
	"runtime"
	"runtime/debug"
)

var (
	memstats runtime.MemStats // Tracked heap stats for GC pressure monitoring
)

//go:inline
//go:registerparams
func main() {
	debug.SetGCPercent(-1) // Disable GC entirely (manual GC control only)
	runtime.LockOSThread() // Pin this goroutine to a specific OS thread

	for {
		// Step 1: Run the WebSocket publisher pipeline
		if err := runPublisher(); err != nil {
			dropError("main loop error", err) // Log and continue if an error occurs
		}

		// Step 2: Monitor memory stats and manage heap usage
		runtime.ReadMemStats(&memstats)
		if memstats.HeapAlloc > heapSoftLimit {
			// Trigger garbage collection if heap exceeds soft limit
			debug.SetGCPercent(100)
			runtime.GC() // Force a GC cycle
			debug.SetGCPercent(-1)
			dropError("[GC] heap trimmed", nil) // Log GC action
		}
		// Step 3: Panic if heap usage exceeds hard limit
		if memstats.HeapAlloc > heapHardLimit {
			panic("heap usage exceeded hard cap — leak likely")
		}
	}
}

// runPublisher establishes WebSocket connection and runs blocking read loop
//
//go:inline
//go:registerparams
func runPublisher() error {
	// ───── Step 1: Dial raw TCP connection and wrap it in TLS ─────
	raw, err := net.Dial("tcp", wsDialAddr)
	if err != nil {
		dropError("tcp dial", err)
		return err
	}

	// Optimize TCP connection for low latency across all platforms
	if tcpConn, ok := raw.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true) // Disable Nagle's algorithm
		tcpConn.SetReadBuffer(maxFrameSize)
		tcpConn.SetWriteBuffer(maxFrameSize)
	}

	tlsConfig := &tls.Config{
		ServerName:             wsHost, // Ensure the ServerName is set for proper SNI handling
		SessionTicketsDisabled: false,  // Enable session resumption
	}
	conn := tls.Client(raw, tlsConfig)
	defer func() { _ = conn.Close(); _ = raw.Close() }() // Ensure connections are closed

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

	// ───── Step 3: MINIMAL BLOCKING READ LOOP ─────
	// This is the absolute minimum latency path - no event systems
	for {
		// Direct blocking read using existing frame parser
		f, err := readFrame(conn)
		if err != nil {
			dropError("read frame", err)
			return err
		}

		// Process frame immediately using existing handler
		handleFrame(f.Payload)

		// Update WebSocket read state after consuming the frame
		consumed := f.End - wsStart
		wsStart = f.End
		wsLen -= consumed
	}
}
