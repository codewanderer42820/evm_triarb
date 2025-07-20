// ════════════════════════════════════════════════════════════════════════════════════════════════
// Arbitrage Detection System - Main Entry Point
// ────────────────────────────────────────────────────────────────────────────────────────────────
// 🇯🇵 MADE IN JAPAN. INSPIRED BY JAPANESE ENGINEERING. FROM NIIKAPPU HIDAKA HOKKAIDO 🇯🇵
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Main Entry Point & System Orchestration
//
// Description:
//   System orchestration with phased initialization and clean separation of concerns.
//   Bootstrap → Memory Optimization → Production Event Processing
//
// Architecture:
//   - Phase 0: System initialization and data loading
//   - Phase 1: Blockchain synchronization and arbitrage system setup
//   - Phase 2: Memory optimization with production mode activation
//   - Phase 3: Real-time event processing loop
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package main

import (
	"crypto/tls"
	"database/sql"
	"net"
	"os"
	"os/signal"
	"runtime"
	rtdebug "runtime/debug"
	"syscall"

	"main/constants"
	"main/control"
	"main/debug"
	"main/parser"
	"main/router"
	"main/syncharvester"
	"main/utils"
	"main/ws"

	_ "github.com/mattn/go-sqlite3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE DATA STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Pool represents a trading pair with its database identifier and contract address.
// Optimized for cache efficiency with 32-byte alignment for improved memory access patterns.
//
//go:notinheap
//go:align 32
type Pool struct {
	ID      int64   // Database identifier for the trading pair
	Address string  // Ethereum contract address (hex format)
	_       [8]byte // Padding to achieve 32-byte boundary alignment for cache optimization
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM LIFECYCLE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// setupSignalHandling configures graceful shutdown coordination for SIGINT and SIGTERM signals.
// Utilizes the control package's ShutdownWG for proper subsystem shutdown coordination.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func setupSignalHandling() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Background goroutine for coordinated shutdown handling
	go func() {
		<-sigChan
		debug.DropMessage("SIG", "Shutdown initiated")

		// Signal shutdown to all registered subsystems
		control.Shutdown()

		// Wait for all subsystems to complete their graceful shutdown procedures
		control.ShutdownWG.Wait()

		debug.DropMessage("SIG", "Shutdown complete")
		os.Exit(0)
	}()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATABASE OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// openDatabase establishes a SQLite database connection for pool data initialization.
// The connection is designed to be closed immediately after loading, as syncharvester manages its own connection pool.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func openDatabase(dbPath string) *sql.DB {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		panic("Failed to establish database connection to " + dbPath + ": " + err.Error())
	}
	return db
}

// loadPoolsFromDatabase retrieves all trading pairs from the database with precise memory allocation.
// Uses a COUNT query to determine exact capacity requirements before data loading for optimal memory usage.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadPoolsFromDatabase(db *sql.DB) []Pool {
	// Determine exact number of pools for precise memory allocation
	var poolCount int
	err := db.QueryRow("SELECT COUNT(*) FROM pools").Scan(&poolCount)
	if err != nil {
		panic("Failed to retrieve pool count from database: " + err.Error())
	}

	if poolCount == 0 {
		panic("Database contains no trading pairs - system cannot operate")
	}

	// Pre-allocate slice with exact capacity to prevent reallocations
	pools := make([]Pool, poolCount)
	poolIndex := 0

	// Load all pools with deterministic ordering by ID
	rows, err := db.Query(`
		SELECT p.id, p.pool_address 
		FROM pools p
		ORDER BY p.id`)
	if err != nil {
		panic("Failed to execute pool query: " + err.Error())
	}
	defer rows.Close()

	// Populate pools slice with exact indexing
	for rows.Next() && poolIndex < poolCount {
		var pairID int64
		var poolAddress string
		if err := rows.Scan(&pairID, &poolAddress); err != nil {
			panic("Failed to scan pool row data: " + err.Error())
		}

		pools[poolIndex] = Pool{
			ID:      pairID,
			Address: poolAddress,
		}
		poolIndex++
	}

	if err := rows.Err(); err != nil {
		panic("Database iteration error encountered: " + err.Error())
	}

	return pools
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// FILE PARSING OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// loadArbitrageCyclesFromFile parses triangular arbitrage cycles from a structured text file.
// Processes lines in format "(12345) → (67890) → (11111)" with exact memory allocation and zero-copy parsing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadArbitrageCyclesFromFile(filename string) []router.ArbitrageTriangle {
	data, err := os.ReadFile(filename)
	if err != nil {
		panic("Failed to read arbitrage cycles file: " + err.Error())
	}

	// Count total lines for exact slice allocation
	lineCount := 0
	for _, b := range data {
		if b == '\n' {
			lineCount++
		}
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		lineCount++ // Account for final line without trailing newline
	}

	// Pre-allocate result slice with exact capacity
	cycles := make([]router.ArbitrageTriangle, lineCount)
	cycleIndex := 0
	i, dataLen := 0, len(data)
	var pairIDs [3]uint64

	// Byte-by-byte parsing without string allocations or intermediate buffers for maximum efficiency
	for i < dataLen && cycleIndex < lineCount {
		pairCount := 0

		// Parse up to 3 pair IDs from the current line
		for pairCount < 3 && i < dataLen && data[i] != '\n' {
			// Locate opening parenthesis
			for i < dataLen && data[i] != '(' && data[i] != '\n' {
				i++
			}
			if i >= dataLen || data[i] == '\n' {
				break
			}
			i++ // Skip opening parenthesis

			// Parse numeric value with overflow protection
			pairID := uint64(0)
			for i < dataLen && data[i] >= '0' && data[i] <= '9' {
				if pairID > (^uint64(0)-10)/10 {
					break // Prevent integer overflow
				}
				pairID = pairID*10 + uint64(data[i]-'0')
				i++
			}

			if pairID > 0 {
				pairIDs[pairCount] = pairID
				pairCount++
			}

			// Advance to closing parenthesis
			for i < dataLen && data[i] != ')' && data[i] != '\n' {
				i++
			}
			if i < dataLen && data[i] == ')' {
				i++ // Skip closing parenthesis
			}
		}

		// Advance to next line
		for i < dataLen && data[i] != '\n' {
			i++
		}
		if i < dataLen {
			i++ // Skip newline character
		}

		// Store arbitrage triangle at exact index position
		cycles[cycleIndex] = router.ArbitrageTriangle{
			router.TradingPairID(pairIDs[0]),
			router.TradingPairID(pairIDs[1]),
			router.TradingPairID(pairIDs[2]),
		}
		cycleIndex++
	}

	// Validate that cycles were successfully loaded
	if cycleIndex == 0 {
		panic("No valid arbitrage cycles found in configuration file")
	}
	return cycles
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// NETWORK OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processEventStream establishes an optimized WebSocket connection and processes events until connection failure.
// Implements comprehensive connection-level optimizations and handles low-level network protocol details.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processEventStream() error {
	// Establish raw TCP connection with target endpoint
	raw, _ := net.Dial("tcp", constants.WsDialAddr)
	tcpConn := raw.(*net.TCPConn)

	// Configure TCP-level optimizations for low-latency operation
	tcpConn.SetNoDelay(true)                       // Disable Nagle's algorithm for immediate packet transmission
	tcpConn.SetReadBuffer(constants.MaxFrameSize)  // Optimize kernel read buffer size
	tcpConn.SetWriteBuffer(constants.MaxFrameSize) // Optimize kernel write buffer size

	// Apply advanced socket optimizations using direct syscalls
	rawFile, _ := tcpConn.File()
	fd := int(rawFile.Fd())

	// Standard TCP socket optimizations
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)

	// Platform-specific network stack optimizations
	switch runtime.GOOS {
	case "linux":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)         // SO_REUSEPORT for load balancing
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // BBR congestion control algorithm
	case "darwin":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1) // SO_REUSEPORT for macOS
	}
	rawFile.Close() // Close file descriptor wrapper to prevent resource leak

	// Establish TLS connection over the optimized TCP socket
	conn := tls.Client(raw, &tls.Config{ServerName: constants.WsHost})

	// Perform WebSocket handshake and establish event subscription
	ws.Handshake(conn)
	ws.SendSubscription(conn)

	// Main event processing loop - runs until connection failure
	for {
		// Wait for complete WebSocket message frame with proper framing
		payload, err := ws.SpinUntilCompleteMessage(conn)
		if err != nil {
			conn.Close()
			return err // Return error to trigger connection retry logic
		}

		// Dispatch message payload to parser subsystem for event processing
		parser.HandleFrame(payload)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN ORCHESTRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// main orchestrates the complete system lifecycle through distinct operational phases.
// Each phase has specific responsibilities and memory/performance optimization characteristics.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func main() {
	// PHASE 0: System initialization and foundational data loading
	debug.DropMessage("INIT", "System startup")

	// Load trading pair configuration from SQLite database
	db := openDatabase("uniswap_pairs.db")
	pools := loadPoolsFromDatabase(db)
	db.Close()

	// Register pool addresses in router's optimized hash table for O(1) lookups
	for _, pool := range pools {
		router.RegisterTradingPairAddress([]byte(pool.Address[2:]), router.TradingPairID(pool.ID))
	}

	// Load triangular arbitrage cycle configurations from file
	cycles := loadArbitrageCyclesFromFile("cycles_3_3.txt")

	debug.DropMessage("LOAD", utils.Itoa(len(pools))+" pools, "+utils.Itoa(len(cycles))+" cycles")

	setupSignalHandling()

	// PHASE 1: Blockchain synchronization and arbitrage system setup
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("SYNC", "Requirement check failed: "+err.Error())
			continue
		}
		if !syncNeeded {
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", utils.Itoa(int(blocksBehind))+" blocks behind")

		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("SYNC", "Block harvesting failed: "+err.Error())
			continue
		}
	}

	// Initialize arbitrage detection engine with loaded cycle configurations
	router.InitializeArbitrageSystem(cycles)

	// PHASE 2: Memory optimization - First pass garbage collection
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()

	// Verify blockchain synchronization state after garbage collection
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("SYNC", "Post-GC requirement check failed: "+err.Error())
			continue
		}
		if !syncNeeded {
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Post-GC: "+utils.Itoa(int(blocksBehind))+" blocks behind")

		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("SYNC", "Post-GC harvesting failed: "+err.Error())
			continue
		}
	}

	// Load current reserve data into router from harvested blockchain state
	if err := syncharvester.FlushHarvestedReservesToRouter(); err != nil {
		panic("Critical system failure: Reserve data flush to router failed - " + err.Error())
	}

	// PHASE 2.5: Final memory optimization - Second pass for production readiness
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()

	// Production mode configuration for maximum performance
	debug.DropMessage("PROD", "Production mode active")
	runtime.LockOSThread()
	rtdebug.SetGCPercent(-1)
	control.ForceActive()

	// PHASE 3: Final incremental synchronization using temporary storage
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("SYNC", "Temp requirement check failed: "+err.Error())
			continue
		}
		if !syncNeeded {
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Temp sync: "+utils.Itoa(int(blocksBehind))+" blocks behind")

		err = syncharvester.ExecuteHarvestingToTemp(constants.DefaultConnections)
		if err != nil {
			debug.DropMessage("SYNC", "Temp harvesting failed: "+err.Error())
			continue
		}
	}

	// Load any newly harvested reserve data from temporary storage into router
	if err := syncharvester.FlushHarvestedReservesToRouterFromTemp(); err != nil {
		panic("Critical system failure: Temporary reserve data flush failed - " + err.Error())
	}

	// PHASE 4: Production mode - Continuous real-time event processing
	for {
		processEventStream()
	}
}
