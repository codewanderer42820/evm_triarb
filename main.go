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
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func main() {
	// PHASE 0: System initialization and data loading
	debug.DropMessage("INIT", "System startup")

	// Load trading pairs from database
	db := openDatabase("uniswap_pairs.db")
	pools := loadPoolsFromDatabase(db)
	db.Close()

	// Load arbitrage cycles from configuration
	cycles := loadArbitrageCyclesFromFile("cycles_3_3.txt")

	debug.DropMessage("LOADED", utils.Itoa(len(pools))+" pools, "+utils.Itoa(len(cycles))+" cycles")

	// Register pool addresses in router hash table
	for _, pool := range pools {
		router.RegisterTradingPairAddress([]byte(pool.Address[2:]), router.TradingPairID(pool.ID))
	}

	setupSignalHandling()
	debug.DropMessage("INIT", "Setup complete")

	// PHASE 1: Blockchain synchronization
	debug.DropMessage("SYNC", "Checking blockchain state")
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("ERROR", "Sync check failed: "+err.Error())
			break
		}
		if !syncNeeded {
			debug.DropMessage("SYNC", "Blockchain synchronized")
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Processing "+utils.Itoa(int(blocksBehind))+" blocks")

		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("ERROR", "Harvest failed: "+err.Error())
			break
		}
	}

	// Initialize arbitrage detection system
	router.InitializeArbitrageSystem(cycles)

	// PHASE 2: Memory optimization (First Pass)
	debug.DropMessage("GC", "Memory optimization")
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()

	// Verify synchronization after GC
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("ERROR", "Post-GC sync check failed: "+err.Error())
			break
		}
		if !syncNeeded {
			debug.DropMessage("SYNC", "Post-GC verification complete")
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Post-GC processing "+utils.Itoa(int(blocksBehind))+" blocks")

		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("ERROR", "Post-GC harvest failed: "+err.Error())
			break
		}
	}

	// PHASE 2.5: Final memory optimization (Second Pass)
	debug.DropMessage("GC", "Final memory optimization")
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()

	runtime.LockOSThread()
	rtdebug.SetGCPercent(-1)
	control.ForceActive()

	// Final synchronization verification
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("ERROR", "Final sync check failed: "+err.Error())
			break
		}
		if !syncNeeded {
			debug.DropMessage("SYNC", "Final verification complete")
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Final processing "+utils.Itoa(int(blocksBehind))+" blocks")

		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("ERROR", "Final harvest failed: "+err.Error())
			break
		}
	}

	// Load reserve data into router from main CSV file
	if err := syncharvester.FlushHarvestedReservesToRouter(); err != nil {
		debug.DropMessage("ERROR", "Reserve flush failed: "+err.Error())
	}

	// PHASE 4: Final quick sync to temporary file
	debug.DropMessage("SYNC", "Final quick sync to temp file")
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("ERROR", "Temp sync check failed: "+err.Error())
			break
		}
		if !syncNeeded {
			debug.DropMessage("SYNC", "Temp sync verification complete")
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Temp sync processing "+utils.Itoa(int(blocksBehind))+" blocks")

		err = syncharvester.ExecuteHarvestingToTemp(constants.DefaultConnections)
		if err != nil {
			debug.DropMessage("ERROR", "Temp harvest failed: "+err.Error())
			break
		}
	}

	// Load any new reserve data from temp file into router
	if err := syncharvester.FlushHarvestedReservesToRouterFromTemp(); err != nil {
		debug.DropMessage("ERROR", "Temp reserve flush failed: "+err.Error())
	}

	// PHASE 5: Production mode
	debug.DropMessage("PROD", "Entering production mode")

	// Main event processing loop
	for {
		processEventStream()
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATA LOADING FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// loadArbitrageCyclesFromFile parses triangular arbitrage cycles from text file.
// Processes "(12345) → (67890) → (11111)" format with exact memory allocation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
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
	cycles := make([]router.ArbitrageTriangle, lineCount)
	cycleIndex := 0
	i, dataLen := 0, len(data)
	var pairIDs [3]uint64

	// Byte-by-byte parsing without string allocations or intermediate buffers
	for i < dataLen && cycleIndex < lineCount {
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

		// Set arbitrage triangle at exact index
		cycles[cycleIndex] = router.ArbitrageTriangle{
			router.TradingPairID(pairIDs[0]),
			router.TradingPairID(pairIDs[1]),
			router.TradingPairID(pairIDs[2]),
		}
		cycleIndex++
	}

	// Return slice with exact valid entries
	if cycleIndex == 0 {
		panic("No cycles found in file")
	}
	return cycles
}

// openDatabase establishes database connection for initialization only.
// Connection is closed after loading data since syncharvester manages its own.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func openDatabase(dbPath string) *sql.DB {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		panic("Failed to open database " + dbPath + ": " + err.Error())
	}
	return db
}

// loadPoolsFromDatabase retrieves all trading pairs with exact memory allocation.
// Uses COUNT query to determine exact capacity requirements before loading data.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
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
	pools := make([]Pool, poolCount)
	poolIndex := 0

	// Load all pools with deterministic ordering
	rows, err := db.Query(`
		SELECT p.id, p.pool_address 
		FROM pools p
		ORDER BY p.id`)
	if err != nil {
		panic("Failed to query pools: " + err.Error())
	}
	defer rows.Close()

	// Populate pools slice at exact indices
	for rows.Next() && poolIndex < poolCount {
		var pairID int64
		var poolAddress string
		if err := rows.Scan(&pairID, &poolAddress); err != nil {
			panic("Failed to scan pool row: " + err.Error())
		}

		pools[poolIndex] = Pool{
			ID:      pairID,
			Address: poolAddress,
		}
		poolIndex++
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
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
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
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
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
