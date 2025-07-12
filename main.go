package main

import (
	"crypto/tls"
	"main/constants"
	"main/debug"
	"main/parser"
	"main/ws"
	"net"
	"runtime"
	"syscall"
)

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func main() {
	debug.DropMessage("STARTUP", "")
	runtime.LockOSThread() // Pin to OS thread

	// Main processing loop with auto-recovery
	for {
		if err := runStream(); err != nil {
			debug.DropError("stream error", err)
		}
	}
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func runStream() error {
	// Establish optimized connection
	conn, err := establishConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	// WebSocket handshake
	if err := ws.Handshake(conn); err != nil {
		return err
	}

	// Subscribe to logs
	if err := ws.SendSubscription(conn); err != nil {
		return err
	}

	debug.DropMessage("READY", "")

	// Hot processing loop
	for {
		payload, err := ws.SpinUntilCompleteMessage(conn)
		if err != nil {
			return err
		}

		// Zero-copy processing
		parser.HandleFrame(payload)
	}
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func establishConnection() (*tls.Conn, error) {
	// TCP dial
	raw, err := net.Dial("tcp", constants.WsDialAddr)
	if err != nil {
		return nil, err
	}

	// TCP optimizations
	tcpConn := raw.(*net.TCPConn)
	tcpConn.SetNoDelay(true)
	tcpConn.SetReadBuffer(constants.MaxFrameSize)
	tcpConn.SetWriteBuffer(constants.MaxFrameSize)

	// Socket-level optimizations
	if rawFile, err := tcpConn.File(); err == nil {
		optimizeSocket(int(rawFile.Fd()))
		rawFile.Close()
	}

	// TLS upgrade
	tlsConn := tls.Client(raw, &tls.Config{
		ServerName: constants.WsHost,
	})

	return tlsConn, nil
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func optimizeSocket(fd int) {
	// Universal optimizations
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)

	switch runtime.GOOS {
	case "linux":
		// Linux-specific
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)         // SO_BUSY_POLL
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 18, 1000)     // TCP_USER_TIMEOUT
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 16, 1)        // TCP_THIN_LINEAR_TIMEOUTS
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 17, 1)        // TCP_THIN_DUPACK
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // TCP_CONGESTION

	case "darwin":
		// macOS-specific
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x10, 1)  // TCP_KEEPIDLE
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x101, 1) // TCP_KEEPINTVL
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x102, 3) // TCP_KEEPCNT
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1) // SO_RECV_ANYIF

	case "windows":
		// Windows-specific
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x0004, 1) // SO_EXCLUSIVEADDRUSE
	}
}
