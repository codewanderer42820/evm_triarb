// main.go - Arbitrage detection engine with WebSocket event processing
package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"syscall"

	"main/constants"
	"main/debug"
	"main/parser"
	"main/router"
	"main/ws"

	_ "github.com/mattn/go-sqlite3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// init performs automatic system initialization with database integration.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func init() {
	debug.DropMessage("SYSTEM_INIT", "Starting arbitrage detection engine")

	// Try database initialization first
	err := initializeFromDatabase("uniswap_pairs.db", "cycles_3_3.txt")
	if err != nil {
		// Fall back to file-only mode
		debug.DropMessage("DATABASE_FALLBACK", fmt.Sprintf("Database unavailable (%v), using file-only mode", err))

		cycles, err := loadArbitrageCyclesFromFile("cycles_3_3.txt")
		if err != nil {
			panic(fmt.Sprintf("Critical failure loading arbitrage cycles: %v", err))
		}

		router.InitializeArbitrageSystem(cycles)
		debug.DropMessage("INIT_COMPLETE", "File-only mode")
	} else {
		debug.DropMessage("INIT_COMPLETE", "Database integration active")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATABASE INTEGRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// loadArbitrageCyclesFromFile loads triangular arbitrage cycles from configuration files.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadArbitrageCyclesFromFile(filename string) ([]router.ArbitrageTriplet, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	// Estimate cycles count for pre-allocation
	estimatedCycles := len(data) / 50
	if estimatedCycles < 100 {
		estimatedCycles = 100
	}
	cycles := make([]router.ArbitrageTriplet, 0, estimatedCycles)

	// Parse file byte by byte
	i := 0
	dataLen := len(data)

	for i < dataLen {
		var pairIDs [3]uint64
		pairCount := 0

		// Extract 3 pairs from current line
		for pairCount < 3 && i < dataLen && data[i] != '\n' {
			// Find opening parenthesis
			for i < dataLen && data[i] != '(' && data[i] != '\n' {
				i++
			}
			if i >= dataLen || data[i] == '\n' {
				break
			}
			i++ // Skip opening '('

			// Extract digits
			pairID := uint64(0)
			digitFound := false
			for i < dataLen && data[i] >= '0' && data[i] <= '9' {
				pairID = pairID*10 + uint64(data[i]-'0')
				digitFound = true
				i++
			}

			// Find closing parenthesis
			for i < dataLen && data[i] != ')' && data[i] != '\n' {
				i++
			}

			// Store valid pair ID
			if digitFound && pairID > 0 {
				pairIDs[pairCount] = pairID
				pairCount++
			}

			// Skip closing parenthesis
			if i < dataLen && data[i] == ')' {
				i++
			}
		}

		// Skip to next line
		for i < dataLen && data[i] != '\n' {
			i++
		}
		if i < dataLen {
			i++ // Skip newline
		}

		// Create arbitrage triplet if we found 3 pairs
		if pairCount == 3 {
			cycles = append(cycles, router.ArbitrageTriplet{
				router.PairID(pairIDs[0]),
				router.PairID(pairIDs[1]),
				router.PairID(pairIDs[2]),
			})
		}
	}

	if len(cycles) == 0 {
		return nil, fmt.Errorf("no valid arbitrage cycles found in %s", filename)
	}

	return cycles, nil
}

// loadPoolsFromDatabase reads trading pairs from SQLite database.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadPoolsFromDatabase(dbPath string) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("database connection failed: %v", err)
	}
	defer db.Close()

	// Query all pools with token information
	const query = `
		SELECT 
			p.id,
			p.pool_address,
			t0.address as token0_address,
			t1.address as token1_address,
			p.fee_bps,
			e.name as exchange_name
		FROM pools p
		JOIN tokens t0 ON p.token0_id = t0.id  
		JOIN tokens t1 ON p.token1_id = t1.id
		JOIN exchanges e ON p.exchange_id = e.id
		ORDER BY p.id`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("pool query failed: %v", err)
	}
	defer rows.Close()

	// Register each pool address
	count := 0
	for rows.Next() {
		var (
			pairID        int64
			poolAddress   string
			token0Address string
			token1Address string
			feeBps        sql.NullInt64
			exchangeName  string
		)

		err := rows.Scan(&pairID, &poolAddress, &token0Address,
			&token1Address, &feeBps, &exchangeName)
		if err != nil {
			return fmt.Errorf("pool row scan failed: %v", err)
		}

		// Register pool address for event processing
		poolAddressBytes := []byte(strings.TrimPrefix(poolAddress, "0x"))
		router.RegisterPairAddress(poolAddressBytes, router.PairID(pairID))
		count++
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("pool iteration error: %v", err)
	}

	debug.DropMessage("POOLS_LOADED", fmt.Sprintf("%d trading pairs registered", count))
	return nil
}

// initializeFromDatabase performs complete system initialization with database integration.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func initializeFromDatabase(dbPath string, cyclesFile string) error {
	// Load trading pairs from database
	if err := loadPoolsFromDatabase(dbPath); err != nil {
		return fmt.Errorf("trading pair loading failed: %v", err)
	}

	// Load arbitrage cycles from file
	cycles, err := loadArbitrageCyclesFromFile(cyclesFile)
	if err != nil {
		return fmt.Errorf("arbitrage cycle loading failed: %v", err)
	}

	// Initialize arbitrage detection system
	router.InitializeArbitrageSystem(cycles)

	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN EXECUTION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// main orchestrates the arbitrage detection system with fault tolerance.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func main() {
	debug.DropMessage("STARTUP", "Arbitrage detection engine starting")
	runtime.LockOSThread() // Pin to OS thread

	// Main processing loop with auto-recovery
	for {
		if err := runStream(); err != nil {
			debug.DropError("STREAM_ERROR", err)
		}
	}
}

// runStream establishes WebSocket connection and processes events.
//
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

	// Subscribe to events
	if err := ws.SendSubscription(conn); err != nil {
		return err
	}

	debug.DropMessage("READY", "Event processing active")

	// Process incoming messages
	for {
		payload, err := ws.SpinUntilCompleteMessage(conn)
		if err != nil {
			return err
		}

		// Process events
		parser.HandleFrame(payload)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// NETWORK OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// establishConnection creates optimized TCP/TLS connection.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func establishConnection() (*tls.Conn, error) {
	// TCP connection
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

// optimizeSocket applies platform-specific socket optimizations.
//
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

	// Platform-specific optimizations
	switch runtime.GOOS {
	case "linux":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)         // SO_BUSY_POLL
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 18, 1000)     // TCP_USER_TIMEOUT
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 16, 1)        // TCP_THIN_LINEAR_TIMEOUTS
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 17, 1)        // TCP_THIN_DUPACK
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // TCP_CONGESTION

	case "darwin":
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x10, 1)  // TCP_KEEPIDLE
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x101, 1) // TCP_KEEPINTVL
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x102, 3) // TCP_KEEPCNT
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1) // SO_RECV_ANYIF

	case "windows":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x0004, 1) // SO_EXCLUSIVEADDRUSE
	}
}
