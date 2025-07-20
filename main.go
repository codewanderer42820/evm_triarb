// ════════════════════════════════════════════════════════════════════════════════════════════════
// Arbitrage Detection System - Main Entry Point
// ────────────────────────────────────────────────────────────────────────────────────────────────
// 🇯🇵 MADE IN JAPAN. INSPIRED BY JAPANESE ENGINEERING. FROM NIIKAPPU HIDAKA HOKKAIDO. 🇯🇵
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Main Entry Point & System Orchestration
//
// Description:
//   System orchestration with phased initialization and structured event processing.
//   Manages initialization, synchronization, and real-time event handling.
//
// Architecture:
//   - Phase 0: System initialization and data loading
//   - Phase 1: Blockchain synchronization and system setup
//   - Phase 2: Memory optimization and production mode activation
//   - Phase 3: Real-time event processing
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
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
// Structure is aligned for memory efficiency.
//
//go:notinheap
//go:align 32
type Pool struct {
	ID      int64   // Database identifier for the trading pair
	Address string  // Ethereum contract address (hex format)
	_       [8]byte // Padding for memory alignment
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM LIFECYCLE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// setupSignalHandling configures graceful shutdown for SIGINT and SIGTERM signals.
// Coordinates shutdown across all subsystems using the control package.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func setupSignalHandling() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Background handler for graceful shutdown
	go func() {
		<-sigChan
		debug.DropMessage("SIG", "Shutdown initiated")

		// Signal shutdown to all subsystems
		control.Shutdown()

		// Wait for graceful completion
		control.ShutdownWG.Wait()

		// Clean up temporary files
		if err := os.Remove(constants.HarvesterTempPath); err != nil {
			// File may not exist - this is expected
			if !os.IsNotExist(err) {
				debug.DropMessage("SIG", "Failed to remove temp file: "+err.Error())
			}
		}

		debug.DropMessage("SIG", "Shutdown complete")
		os.Exit(0)
	}()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATABASE OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// openDatabase establishes a SQLite database connection.
// Connection should be closed after initial data loading.
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

// loadPoolsFromDatabase retrieves all trading pairs from the database.
// Pre-allocates memory based on exact count for efficiency.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadPoolsFromDatabase(db *sql.DB) []Pool {
	// Get total count for memory allocation
	var poolCount int
	err := db.QueryRow("SELECT COUNT(*) FROM pools").Scan(&poolCount)
	if err != nil {
		panic("Failed to retrieve pool count from database: " + err.Error())
	}

	if poolCount == 0 {
		panic("Database contains no trading pairs")
	}

	// Allocate exact capacity
	pools := make([]Pool, poolCount)
	poolIndex := 0

	// Load pools ordered by ID
	rows, err := db.Query(`
		SELECT p.id, p.pool_address 
		FROM pools p
		ORDER BY p.id`)
	if err != nil {
		panic("Failed to execute pool query: " + err.Error())
	}
	defer rows.Close()

	// Populate pools array
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

// loadArbitrageCyclesFromFile parses triangular arbitrage cycles from a text file.
// Expected format: "(12345) → (67890) → (11111)" per line.
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

	// Count lines for allocation
	lineCount := 0
	for _, b := range data {
		if b == '\n' {
			lineCount++
		}
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		lineCount++ // Account for last line without newline
	}

	// Pre-allocate result array
	cycles := make([]router.ArbitrageTriangle, lineCount)
	cycleIndex := 0
	i, dataLen := 0, len(data)
	var pairIDs [3]uint64

	// Parse byte-by-byte for efficiency
	for i < dataLen && cycleIndex < lineCount {
		pairCount := 0

		// Parse up to 3 pair IDs per line
		for pairCount < 3 && i < dataLen && data[i] != '\n' {
			// Find opening parenthesis
			for i < dataLen && data[i] != '(' && data[i] != '\n' {
				i++
			}
			if i >= dataLen || data[i] == '\n' {
				break
			}
			i++ // Skip '('

			// Parse numeric ID
			pairID := uint64(0)
			for i < dataLen && data[i] >= '0' && data[i] <= '9' {
				if pairID > (^uint64(0)-10)/10 {
					break // Prevent overflow
				}
				pairID = pairID*10 + uint64(data[i]-'0')
				i++
			}

			if pairID > 0 {
				pairIDs[pairCount] = pairID
				pairCount++
			}

			// Find closing parenthesis
			for i < dataLen && data[i] != ')' && data[i] != '\n' {
				i++
			}
			if i < dataLen && data[i] == ')' {
				i++ // Skip ')'
			}
		}

		// Advance to next line
		for i < dataLen && data[i] != '\n' {
			i++
		}
		if i < dataLen {
			i++ // Skip newline
		}

		// Store parsed triangle
		cycles[cycleIndex] = router.ArbitrageTriangle{
			router.TradingPairID(pairIDs[0]),
			router.TradingPairID(pairIDs[1]),
			router.TradingPairID(pairIDs[2]),
		}
		cycleIndex++
	}

	// Verify cycles were loaded
	if cycleIndex == 0 {
		panic("No valid arbitrage cycles found in configuration file")
	}
	return cycles
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// init performs complete system initialization in three phases.
// Phase 0: Load configuration and initialize core structures
// Phase 1: Synchronize with blockchain state
// Phase 2: Optimize memory and activate production mode
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func init() {
	// Clean up any leftover temporary files
	os.Remove(constants.HarvesterTempPath) // Ignore errors - file may not exist

	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 0: FOUNDATION INITIALIZATION
	//═══════════════════════════════════════════════════════════════════════════════════════
	// Load configuration data and initialize core system components.

	debug.DropMessage("INIT", "System startup")

	// Load trading pairs from database
	db := openDatabase("uniswap_pairs.db")
	pools := loadPoolsFromDatabase(db)
	db.Close()

	// Register addresses for fast lookup
	for _, pool := range pools {
		router.RegisterTradingPairAddress([]byte(pool.Address[2:]), router.TradingPairID(pool.ID))
	}

	// Load arbitrage cycle definitions
	cycles := loadArbitrageCyclesFromFile("cycles_3_3.txt")

	debug.DropMessage("LOAD", utils.Itoa(len(pools))+" pools, "+utils.Itoa(len(cycles))+" cycles")

	// Configure shutdown handling
	setupSignalHandling()

	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 1: BLOCKCHAIN SYNCHRONIZATION
	//═══════════════════════════════════════════════════════════════════════════════════════
	// Ensure system is synchronized with current blockchain state.

	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("SYNC", "Requirement check failed: "+err.Error())
			continue // Retry on error
		}
		if !syncNeeded {
			break // Already synchronized
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", utils.Itoa(int(blocksBehind))+" blocks behind")

		// Harvest historical data
		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("SYNC", "Block harvesting failed: "+err.Error())
			continue // Retry until successful
		}
	}

	// Initialize arbitrage detection system
	router.InitializeArbitrageSystem(cycles)

	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 2: MEMORY OPTIMIZATION
	//═══════════════════════════════════════════════════════════════════════════════════════
	// Optimize memory layout and prepare for production operation.

	// Initial garbage collection
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()

	// Verify synchronization after GC
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

		// Re-synchronize if needed
		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("SYNC", "Post-GC harvesting failed: "+err.Error())
			continue
		}
	}

	// Load reserve data into arbitrage engine
	if err := syncharvester.FlushHarvestedReservesToRouter(); err != nil {
		panic("Critical: Reserve data flush failed - " + err.Error())
	}

	// Final memory optimization
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()

	// Activate production mode
	debug.DropMessage("PROD", "Production mode active")
	runtime.LockOSThread()
	rtdebug.SetGCPercent(-1) // Disable automatic GC
	control.ForceActive()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN ORCHESTRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// main executes the real-time arbitrage detection engine.
// Phase 3: Continuous event processing with automatic reconnection.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func main() {
	// Panic recovery for system resilience
	defer func() {
		if r := recover(); r != nil {
			debug.DropMessage("PANIC", "System panic recovered: "+fmt.Sprintf("%v", r))
			debug.DropMessage("STACK", string(rtdebug.Stack()))
			// Continue execution after recovery
		}
	}()

	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 3: REAL-TIME EVENT PROCESSING
	//═══════════════════════════════════════════════════════════════════════════════════════
	// Main event processing loop with automatic reconnection on failure.

	// Track synchronization progress across reconnections
	latestTempSyncedBlock := syncharvester.LoadMetadata()

	// Main processing loop
	for {
		// Establish TCP connection with optimizations
		raw, _ := net.Dial("tcp", constants.WsDialAddr)
		tcpConn := raw.(*net.TCPConn)

		// Configure TCP socket options
		tcpConn.SetNoDelay(true)                       // Disable Nagle's algorithm
		tcpConn.SetReadBuffer(constants.MaxFrameSize)  // Set read buffer size
		tcpConn.SetWriteBuffer(constants.MaxFrameSize) // Set write buffer size

		// Apply socket-level optimizations
		rawFile, _ := tcpConn.File()
		fd := int(rawFile.Fd())

		// Standard TCP optimizations
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)

		// Platform-specific optimizations
		switch runtime.GOOS {
		case "linux":
			syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)         // SO_REUSEPORT
			syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // BBR congestion control
		case "darwin":
			syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1) // SO_REUSEPORT on macOS
		}
		rawFile.Close() // Release file descriptor

		// Synchronize any missing blocks before WebSocket connection
		for {
			syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirementFromBlock(latestTempSyncedBlock)
			if err != nil {
				debug.DropMessage("SYNC", "Temp requirement check failed: "+err.Error())
				continue
			}
			if !syncNeeded {
				break
			}

			blocksBehind := targetBlock - lastBlock
			debug.DropMessage("SYNC", "Temp sync: "+utils.Itoa(int(blocksBehind))+" blocks behind")

			// Harvest to temporary storage
			newLastProcessed, err := syncharvester.ExecuteHarvestingToTemp(constants.DefaultConnections)
			if err != nil {
				debug.DropMessage("SYNC", "Temp harvesting failed: "+err.Error())
				continue
			}

			// Update progress tracker
			latestTempSyncedBlock = newLastProcessed
		}

		// Load reserve data from temporary storage
		for {
			err := syncharvester.FlushHarvestedReservesToRouterFromTemp()
			if err == nil {
				break
			}
			debug.DropMessage("SYNC", "Temporary reserve data flush failed: "+err.Error())
			// Retry until successful
		}

		// Establish secure WebSocket connection
		conn := tls.Client(raw, &tls.Config{ServerName: constants.WsHost})

		// Initialize WebSocket and subscribe to events
		ws.Handshake(conn)
		ws.SendSubscription(conn)

		// Event processing loop
		for {
			// Receive WebSocket message
			payload, err := ws.SpinUntilCompleteMessage(conn)
			if err != nil {
				conn.Close()
				break // Reconnect on error
			}

			// Process arbitrage opportunities
			parser.HandleFrame(payload)
		}
		// Connection lost - loop will reconnect with synchronization
	}
}
