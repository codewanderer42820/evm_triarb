// ════════════════════════════════════════════════════════════════════════════════════════════════
// Arbitrage Detection System - Main Entry Point
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Main Entry Point & System Orchestration
//
// Description:
//   System orchestration with phased initialization and clean separation of concerns.
//   Bootstrap → Memory Optimization → Production Event Processing
//
// Architecture:
//   - Phase 1: Bootstrap synchronization with blockchain state
//   - Phase 2: Memory cleanup and optimization for production
//   - Phase 3: Real-time event processing with GC disabled
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
// Optimized for cache efficiency with 32-byte alignment.
//
//go:notinheap
//go:align 32
type Pool struct {
	ID      int64   // 8B - Database identifier for the trading pair
	Address string  // 16B - Ethereum contract address
	_       [8]byte // 8B - Padding to 32-byte boundary
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN ORCHESTRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// main orchestrates the complete system lifecycle in distinct phases.
// Each phase has specific responsibilities and optimization characteristics.
func main() {
	// PHASE 0: System initialization and data loading
	debug.DropMessage("INIT", "Loading system data")

	// Initialize database connection and load core data structures
	db := openDatabase("uniswap_pairs.db")
	pools := loadPoolsFromDatabase(db)
	cycles := loadArbitrageCyclesFromFile("cycles_3_3.txt")

	debug.DropMessage("LOADED", utils.Itoa(len(pools))+" pools, "+utils.Itoa(len(cycles))+" cycles")

	// Display sample data for verification during development
	for i := 0; i < 3 && i < len(pools); i++ {
		p := pools[i]
		debug.DropMessage("POOL", utils.Itoa(i+1)+": ID "+utils.Itoa(int(p.ID))+" → "+p.Address)
	}

	for i := 0; i < 3 && i < len(cycles); i++ {
		c := cycles[i]
		debug.DropMessage("CYCLE", utils.Itoa(i+1)+": ("+utils.Itoa(int(c[0]))+")→("+utils.Itoa(int(c[1]))+")→("+utils.Itoa(int(c[2]))+")")
	}

	// Register pool addresses in the router hash table before system initialization
	// This ensures the lookup infrastructure is populated when the router initializes
	for _, pool := range pools {
		router.RegisterTradingPairAddress([]byte(pool.Address[2:]), router.TradingPairID(pool.ID))
	}

	// Initialize the multi-core arbitrage detection system
	router.InitializeArbitrageSystem(cycles)

	// Close database after loading (syncharvester doesn't need it)
	db.Close()

	debug.DropMessage("READY", "System initialized")

	setupSignalHandling()

	// PHASE 1: Bootstrap synchronization with blockchain state
	// Ensures local database reflects current blockchain state before real-time processing
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("SYNC_ERROR", err.Error())
			break
		}
		if !syncNeeded {
			debug.DropMessage("SYNC", "Fully synchronized with blockchain")
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Syncing "+utils.Itoa(int(blocksBehind))+" blocks")

		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("HARVEST_ERROR", err.Error())
			break
		}
	}

	// PHASE 2: Memory optimization for deterministic runtime behavior
	// Performs garbage collection and memory consolidation before production mode
	runtime.GC()
	runtime.GC() // Double GC to ensure thorough cleanup
	rtdebug.FreeOSMemory()

	// Re-verify synchronization status after memory cleanup
	// Blockchain continues advancing during GC, so re-check is necessary
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("SYNC_ERROR", err.Error())
			break
		}
		if !syncNeeded {
			debug.DropMessage("SYNC", "Confirmed synchronized post-GC")
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Post-GC sync: "+utils.Itoa(int(blocksBehind))+" blocks")

		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("HARVEST_ERROR", err.Error())
			break
		}
	}

	// Load synchronized reserve data into the router for arbitrage calculations
	if err := syncharvester.FlushHarvestedReservesToRouter(); err != nil {
		debug.DropMessage("FLUSH_ERROR", err.Error())
	}

	// PHASE 3: Production mode with optimized runtime characteristics
	// Disables garbage collection and locks to current thread for consistent performance
	rtdebug.SetGCPercent(-1) // Disable garbage collection
	runtime.LockOSThread()   // Lock to current OS thread
	control.ForceHot()       // Signal control system to enter active mode

	// Infinite reconnection loop for continuous event processing
	// Handles network disconnections and protocol errors gracefully
	for {
		processEventStream()
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATA LOADING FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// loadArbitrageCyclesFromFile parses triangular arbitrage cycles from text file.
// Processes "(12345) → (67890) → (11111)" format with exact memory allocation.
func loadArbitrageCyclesFromFile(filename string) []router.ArbitrageTriangle {
	data, err := os.ReadFile(filename)
	if err != nil {
		panic("Failed to load cycles: " + err.Error())
	}

	// Count total lines for exact slice allocation
	lineCount := 0
	for _, b := range data {
		if b == '\n' {
			lineCount++
		}
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		lineCount++ // Account for final line without newline
	}

	// Pre-allocate result slice to exact capacity
	cycles := make([]router.ArbitrageTriangle, 0, lineCount)
	i, dataLen := 0, len(data)
	var pairIDs [3]uint64

	// Byte-by-byte parsing without string allocations or intermediate buffers
	for i < dataLen {
		pairCount := 0

		// Parse up to 3 pair IDs from current line
		for pairCount < 3 && i < dataLen && data[i] != '\n' {
			// Find opening parenthesis
			for i < dataLen && data[i] != '(' && data[i] != '\n' {
				i++
			}
			if i >= dataLen || data[i] == '\n' {
				break
			}
			i++ // Skip '('

			// Parse numeric value with overflow protection
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

			// Skip to closing parenthesis
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
			i++ // Skip '\n'
		}

		// Create arbitrage triangle from parsed pair IDs
		cycles = append(cycles, router.ArbitrageTriangle{
			router.TradingPairID(pairIDs[0]),
			router.TradingPairID(pairIDs[1]),
			router.TradingPairID(pairIDs[2]),
		})
	}

	if len(cycles) == 0 {
		panic("No cycles found in file")
	}
	return cycles
}

// openDatabase establishes database connection for initialization only.
// Connection is closed after loading data since syncharvester manages its own.
func openDatabase(dbPath string) *sql.DB {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		panic("Failed to open database " + dbPath + ": " + err.Error())
	}
	return db
}

// loadPoolsFromDatabase retrieves all trading pairs with exact memory allocation.
// Uses COUNT query to determine exact capacity requirements before loading data.
func loadPoolsFromDatabase(db *sql.DB) []Pool {
	// Determine exact number of pools for precise allocation
	var poolCount int
	err := db.QueryRow("SELECT COUNT(*) FROM pools").Scan(&poolCount)
	if err != nil {
		panic("Failed to count pools: " + err.Error())
	}

	if poolCount == 0 {
		panic("No pools found in database")
	}

	// Pre-allocate slice to exact capacity
	pools := make([]Pool, 0, poolCount)

	// Load all pools with deterministic ordering
	rows, err := db.Query(`
		SELECT p.id, p.pool_address 
		FROM pools p
		ORDER BY p.id`)
	if err != nil {
		panic("Failed to query pools: " + err.Error())
	}
	defer rows.Close()

	// Populate pools slice
	for rows.Next() {
		var pairID int64
		var poolAddress string
		if err := rows.Scan(&pairID, &poolAddress); err != nil {
			panic("Failed to scan pool row: " + err.Error())
		}

		pools = append(pools, Pool{
			ID:      pairID,
			Address: poolAddress,
		})
	}

	if err := rows.Err(); err != nil {
		panic("Database iteration error: " + err.Error())
	}

	return pools
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PRODUCTION EVENT PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processEventStream establishes WebSocket connection and processes events until failure.
// Implements connection-level optimizations and handles network-level protocol details.
func processEventStream() error {
	// Establish raw TCP connection with optimal parameters
	raw, _ := net.Dial("tcp", constants.WsDialAddr)
	tcpConn := raw.(*net.TCPConn)

	// Configure TCP-level optimizations
	tcpConn.SetNoDelay(true)                       // Disable Nagle's algorithm
	tcpConn.SetReadBuffer(constants.MaxFrameSize)  // Optimize read buffer size
	tcpConn.SetWriteBuffer(constants.MaxFrameSize) // Optimize write buffer size

	// Apply low-level socket optimizations using syscalls
	rawFile, _ := tcpConn.File()
	fd := int(rawFile.Fd())

	// Standard TCP optimizations
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)

	// Platform-specific optimizations for improved performance
	switch runtime.GOOS {
	case "linux":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)         // SO_REUSEPORT
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // TCP_CONGESTION=bbr
	case "darwin":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1) // SO_REUSEPORT
	}
	rawFile.Close() // Close file descriptor wrapper

	// Establish TLS connection over the optimized TCP connection
	conn := tls.Client(raw, &tls.Config{ServerName: constants.WsHost})

	// Perform WebSocket handshake and establish subscription
	ws.Handshake(conn)
	ws.SendSubscription(conn)

	// Main event processing loop
	for {
		// Wait for complete WebSocket message frame
		payload, err := ws.SpinUntilCompleteMessage(conn)
		if err != nil {
			conn.Close()
			return err // Return error to trigger reconnection
		}

		// Dispatch message payload to parser for processing
		parser.HandleFrame(payload)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM LIFECYCLE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// setupSignalHandling configures graceful shutdown coordination.
// Uses control package's ShutdownWG for proper subsystem coordination.
func setupSignalHandling() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Background signal handler for coordinated shutdown
	go func() {
		<-sigChan
		debug.DropMessage("SIGNAL", "Received interrupt, shutting down...")

		// Signal shutdown to all subsystems
		control.Shutdown()

		// Wait for all subsystems to complete graceful shutdown
		control.ShutdownWG.Wait()

		debug.DropMessage("SIGNAL", "All subsystems shutdown complete")
		os.Exit(0)
	}()
}
