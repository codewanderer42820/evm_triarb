// main.go — High-Performance Arbitrage Detection System Entry Point
//
// This application orchestrates a complete arbitrage detection pipeline for Ethereum DEX trading,
// processing WebSocket event streams with nanosecond-scale latency. The system loads arbitrage
// cycles and trading pair mappings at startup, then maintains persistent connections to Ethereum
// nodes for real-time event processing.
//
// Architecture: Database-backed initialization, WebSocket event streaming, automatic recovery
// Performance: Zero-allocation hot paths, disabled GC during operation, TCP/TLS optimizations
// Fault tolerance: Automatic reconnection, graceful error handling, continuous operation

package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"net"
	"os"
	"runtime"
	rtdebug "runtime/debug"
	"strings"
	"syscall"

	"main/constants"
	"main/debug"
	"main/parser"
	"main/router"
	"main/utils"
	"main/ws"

	_ "github.com/mattn/go-sqlite3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// init performs critical system initialization before main() execution.
// This function loads all arbitrage cycles and trading pair mappings into memory,
// then aggressively cleans up initialization data to minimize memory footprint.
// The garbage collector is disabled after initialization for maximum runtime performance.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func init() {
	debug.DropMessage("SYSTEM_INIT", "Starting arbitrage detection engine")

	// Load arbitrage cycle definitions from configuration file
	// These define the three-pair trading loops we'll monitor for opportunities
	cycles, err := loadArbitrageCyclesFromFile("cycles_3_3.txt")
	if err != nil {
		panic(fmt.Sprintf("Critical failure loading arbitrage cycles: %v", err))
	}
	printCyclesInfo(cycles)

	// Attempt to load trading pair addresses from database
	// This maps Ethereum contract addresses to our internal pair IDs
	if err := loadPoolsFromDatabase("uniswap_pairs.db"); err != nil {
		debug.DropMessage("DATABASE_FALLBACK", fmt.Sprintf("Database unavailable (%v), using cycles-only mode", err))
	}

	// Initialize the arbitrage detection system with loaded configuration
	// This distributes cycles across CPU cores and sets up message routing
	router.InitializeArbitrageSystem(cycles)

	// Aggressive memory cleanup phase
	// We clear all initialization data since it's no longer needed
	debug.DropMessage("MEMORY_CLEANUP", "Clearing initialization data")

	// Zero out and release the cycles array
	for i := range cycles {
		cycles[i] = router.ArbitrageTriangle{} // Clear individual elements
	}
	cycles = nil // Release the slice itself

	// Force immediate garbage collection to reclaim initialization memory
	// Double GC ensures both young and old generation cleanup
	runtime.GC()
	runtime.GC()
	debug.DropMessage("GC_COMPLETE", "Forced garbage collection completed")

	// Disable garbage collection for production operation
	// This eliminates GC pauses during latency-critical event processing
	rtdebug.SetGCPercent(-1)
	debug.DropMessage("GC_DISABLED", "Garbage collector disabled for production operation")

	debug.DropMessage("INIT_COMPLETE", "System ready")
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATA LOADING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// loadArbitrageCyclesFromFile parses arbitrage cycle definitions from a text file.
// Each line defines a three-pair arbitrage opportunity in the format: (ID1) (ID2) (ID3)
// The parser is optimized for speed with minimal allocations and aggressive cleanup.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadArbitrageCyclesFromFile(filename string) ([]router.ArbitrageTriangle, error) {
	debug.DropMessage("FILE_LOADING", "Reading cycles from: "+filename)

	// Read entire file into memory for efficient parsing
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	fileSizeStr := utils.Itoa(len(data))
	debug.DropMessage("FILE_SIZE", "File size: "+fileSizeStr+" bytes")

	// Pre-allocate cycle storage with capacity estimation
	// Typical line is ~50 bytes, so this provides reasonable initial capacity
	estimatedCycles := len(data) / 50
	if estimatedCycles < 100 {
		estimatedCycles = 100
	}
	cycles := make([]router.ArbitrageTriangle, 0, estimatedCycles)

	// Parse file using single-pass algorithm with no temporary allocations
	i, lineCount := 0, 0
	dataLen := len(data)

	for i < dataLen {
		var pairIDs [3]uint64
		pairCount := 0
		lineCount++

		// Extract exactly 3 pair IDs from the current line
		for pairCount < 3 && i < dataLen && data[i] != '\n' {
			// Skip to opening parenthesis
			for i < dataLen && data[i] != '(' && data[i] != '\n' {
				i++
			}
			if i >= dataLen || data[i] == '\n' {
				break
			}
			i++ // Move past '('

			// Parse numeric pair ID
			pairID := uint64(0)
			digitFound := false
			for i < dataLen && data[i] >= '0' && data[i] <= '9' {
				pairID = pairID*10 + uint64(data[i]-'0')
				digitFound = true
				i++
			}

			// Skip to closing parenthesis
			for i < dataLen && data[i] != ')' && data[i] != '\n' {
				i++
			}

			// Store valid pair ID
			if digitFound && pairID > 0 {
				pairIDs[pairCount] = pairID
				pairCount++
			}

			// Move past closing parenthesis
			if i < dataLen && data[i] == ')' {
				i++
			}
		}

		// Advance to next line
		for i < dataLen && data[i] != '\n' {
			i++
		}
		if i < dataLen {
			i++
		}

		// Create arbitrage triangle if we found exactly 3 pairs
		if pairCount == 3 {
			cycles = append(cycles, router.ArbitrageTriangle{
				router.TradingPairID(pairIDs[0]),
				router.TradingPairID(pairIDs[1]),
				router.TradingPairID(pairIDs[2]),
			})
		} else if pairCount > 0 {
			// Log incomplete lines for debugging
			lineStr := utils.Itoa(lineCount)
			pairCountStr := utils.Itoa(pairCount)
			debug.DropMessage("INCOMPLETE_LINE", "Line "+lineStr+" only had "+pairCountStr+" pairs")
		}
	}

	linesStr := utils.Itoa(lineCount)
	debug.DropMessage("LINES_PROCESSED", "Total lines: "+linesStr)

	if len(cycles) == 0 {
		return nil, fmt.Errorf("no valid arbitrage cycles found in %s", filename)
	}

	// Release file data immediately to minimize memory usage
	data = nil
	runtime.GC()
	runtime.GC()

	return cycles, nil
}

// loadPoolsFromDatabase reads trading pair configurations from SQLite database.
// This maps Ethereum contract addresses to internal pair IDs for fast lookup
// during event processing. The function uses minimal allocations and cleans up aggressively.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func loadPoolsFromDatabase(dbPath string) error {
	debug.DropMessage("DATABASE_CONNECT", "Connecting to: "+dbPath)

	// Open database connection with SQLite driver
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("database connection failed: %v", err)
	}
	defer db.Close()

	// Query for all trading pools with their addresses and metadata
	// The ORDER BY ensures consistent initialization across runs
	const query = `
		SELECT 
			p.id, p.pool_address, t0.address as token0_address,
			t1.address as token1_address, p.fee_bps, e.name as exchange_name
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

	count := 0
	// Pre-allocate buffer for address conversion to avoid repeated allocations
	poolAddressBytes := make([]byte, 0, 40) // Ethereum addresses are 40 hex chars

	// Process each trading pool from the database
	for rows.Next() {
		var (
			pairID        int64
			poolAddress   string
			token0Address string
			token1Address string
			feeBps        sql.NullInt64
			exchangeName  string
		)

		if err := rows.Scan(&pairID, &poolAddress, &token0Address, &token1Address, &feeBps, &exchangeName); err != nil {
			return fmt.Errorf("pool row scan failed: %v", err)
		}

		// Convert address to bytes, stripping 0x prefix
		// Reuse the buffer to avoid allocations
		poolAddressBytes = poolAddressBytes[:0]
		trimmed := strings.TrimPrefix(poolAddress, "0x")
		poolAddressBytes = append(poolAddressBytes, trimmed...)

		// Register the address-to-pair mapping in the router
		router.RegisterTradingPairAddress(poolAddressBytes, router.TradingPairID(pairID))
		count++

		// Periodic progress updates for large databases
		if count%10000 == 0 {
			progressStr := utils.Itoa(count)
			debug.DropMessage("POOL_PROGRESS", progressStr+" pools loaded")
		}
	}

	// Check for iteration errors
	if err = rows.Err(); err != nil {
		return fmt.Errorf("pool iteration error: %v", err)
	}

	countStr := utils.Itoa(count)
	debug.DropMessage("POOLS_LOADED", countStr+" trading pairs registered")

	// Release temporary buffer
	poolAddressBytes = nil
	runtime.GC()
	runtime.GC()

	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// VERIFICATION UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// printCyclesInfo outputs diagnostic information about loaded arbitrage cycles.
// This helps verify correct configuration loading during system startup.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func printCyclesInfo(cycles []router.ArbitrageTriangle) {
	cycleCountStr := utils.Itoa(len(cycles))
	debug.DropMessage("CYCLES_LOADED", "Total cycles: "+cycleCountStr)

	// Display first 10 cycles as examples
	maxToPrint := len(cycles)
	if maxToPrint > 10 {
		maxToPrint = 10
	}

	for i := 0; i < maxToPrint; i++ {
		cycle := cycles[i]
		// Format cycle as: Cycle N: (ID1) -> (ID2) -> (ID3)
		indexStr := utils.Itoa(i + 1)
		pair0Str := utils.Itoa(int(cycle[0]))
		pair1Str := utils.Itoa(int(cycle[1]))
		pair2Str := utils.Itoa(int(cycle[2]))

		cycleInfo := "Cycle " + indexStr + ": (" + pair0Str + ") -> (" + pair1Str + ") -> (" + pair2Str + ")"
		debug.DropMessage("CYCLE_DETAIL", cycleInfo)
	}

	// Indicate if additional cycles exist beyond the sample
	if len(cycles) > 10 {
		remainingStr := utils.Itoa(len(cycles) - 10)
		debug.DropMessage("CYCLES_TRUNCATED", "... and "+remainingStr+" more cycles")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN EXECUTION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// main implements the primary event processing loop with automatic recovery.
// The system maintains continuous operation despite network failures or other errors.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func main() {
	debug.DropMessage("STARTUP", "Arbitrage detection engine starting")

	// Lock to OS thread for consistent CPU affinity and syscall handling
	runtime.LockOSThread()

	// Infinite loop with automatic error recovery
	// This ensures the system remains operational 24/7
	for {
		if err := runStream(); err != nil {
			debug.DropError("STREAM_ERROR", err)
			// Loop continues, establishing new connection
		}
	}
}

// runStream manages a single WebSocket connection lifecycle.
// This function establishes the connection, performs handshake, and processes
// incoming events until an error occurs.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func runStream() error {
	// Create optimized network connection
	conn, err := establishConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	// Perform WebSocket protocol upgrade
	if err := ws.Handshake(conn); err != nil {
		return err
	}

	// Subscribe to Ethereum log events
	if err := ws.SendSubscription(conn); err != nil {
		return err
	}

	debug.DropMessage("READY", "Event processing active")

	// Main event processing loop
	for {
		// Receive complete WebSocket message
		payload, err := ws.SpinUntilCompleteMessage(conn)
		if err != nil {
			return err
		}

		// Parse and route the event for arbitrage detection
		parser.HandleFrame(payload)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// NETWORK OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// establishConnection creates a TLS-secured TCP connection with aggressive optimizations.
// The connection is configured for minimum latency and maximum throughput.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func establishConnection() (*tls.Conn, error) {
	// Establish raw TCP connection
	raw, err := net.Dial("tcp", constants.WsDialAddr)
	if err != nil {
		return nil, err
	}

	// Type assert to access TCP-specific methods
	tcpConn := raw.(*net.TCPConn)

	// Disable Nagle's algorithm for minimum latency
	tcpConn.SetNoDelay(true)

	// Set buffer sizes to match maximum WebSocket frame size
	tcpConn.SetReadBuffer(constants.MaxFrameSize)
	tcpConn.SetWriteBuffer(constants.MaxFrameSize)

	// Apply platform-specific socket optimizations
	if rawFile, err := tcpConn.File(); err == nil {
		optimizeSocket(int(rawFile.Fd()))
		rawFile.Close() // Close the file descriptor duplicate
	}

	// Upgrade to TLS with minimal configuration
	tlsConn := tls.Client(raw, &tls.Config{
		ServerName: constants.WsHost, // Required for SNI
	})

	return tlsConn, nil
}

// optimizeSocket applies platform-specific TCP/IP stack optimizations.
// These settings minimize latency and maximize throughput for real-time event processing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func optimizeSocket(fd int) {
	// Universal TCP optimizations applicable to all platforms
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)                   // Disable Nagle's algorithm
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize) // Receive buffer size
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize) // Send buffer size
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)                   // Allow address reuse
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)                   // Enable keepalive

	// Platform-specific optimizations for maximum performance
	switch runtime.GOOS {
	case "linux":
		// Linux-specific optimizations for low-latency networking
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)         // SO_BUSY_POLL - spin on socket
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 18, 1000)     // TCP_USER_TIMEOUT - 1 second
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 16, 1)        // TCP_THIN_LINEAR_TIMEOUTS
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 17, 1)        // TCP_THIN_DUPACK
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // TCP_CONGESTION - BBR algorithm

	case "darwin":
		// macOS-specific TCP keepalive and routing optimizations
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x10, 1)  // TCP_KEEPIDLE - 1 second
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x101, 1) // TCP_KEEPINTVL - 1 second interval
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x102, 3) // TCP_KEEPCNT - 3 probes
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1) // SO_RECV_ANYIF - receive on any interface

	case "windows":
		// Windows-specific socket exclusivity
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x0004, 1) // SO_EXCLUSIVEADDRUSE
	}
}
