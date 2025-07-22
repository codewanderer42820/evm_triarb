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

// Pool represents a trading pair with database ID and contract address.
// Structure aligned for memory efficiency.
//
//go:notinheap
//go:align 32
type Pool struct {
	ID      int64   // Database identifier for trading pair
	Address string  // Ethereum contract address (hex format)
	_       [8]byte // Padding for memory alignment
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL RESOURCE TRACKING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Global resource tracking variables for emergency cleanup and connection management.
// Cache-aligned for optimal memory access patterns during high-frequency operations.
// These variables are accessed from both normal error handling and panic recovery paths.
//
//go:notinheap
//go:align 64
var (
	// CACHE LINE 1: Connection management (accessed together during cleanup)
	rawConn               net.Conn // Raw TCP connection to WebSocket server for immediate cleanup
	tlsConn               net.Conn // TLS-wrapped connection for secure WebSocket communication
	latestTempSyncedBlock uint64   // Last processed block for temporary synchronization tracking
	_                     [40]byte // Padding to complete cache line alignment
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM LIFECYCLE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// setupSignalHandling configures graceful shutdown for SIGINT and SIGTERM.
// Coordinates shutdown across all subsystems.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func setupSignalHandling() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		debug.DropMessage("SIG", "Shutdown initiated")
		control.Shutdown()
		control.ShutdownWG.Wait()
		debug.DropMessage("SIG", "Complete")
		os.Exit(0)
	}()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATABASE OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// openDatabase establishes SQLite database connection.
// Connection closed after initial data loading.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func openDatabase(dbPath string) *sql.DB {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		panic("Failed database connection to " + dbPath + ": " + err.Error())
	}
	return db
}

// loadPoolsFromDatabase retrieves all trading pairs from database.
// Pre-allocates memory based on exact count for efficiency.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadPoolsFromDatabase(db *sql.DB) []Pool {
	var poolCount int
	err := db.QueryRow("SELECT COUNT(*) FROM pools").Scan(&poolCount)
	if err != nil {
		panic("Failed pool count query: " + err.Error())
	}
	if poolCount == 0 {
		panic("No trading pairs in database")
	}

	pools := make([]Pool, poolCount)
	poolIndex := 0

	rows, err := db.Query(`SELECT p.id, p.pool_address FROM pools p ORDER BY p.id`)
	if err != nil {
		panic("Failed pool query: " + err.Error())
	}
	defer rows.Close()

	for rows.Next() && poolIndex < poolCount {
		var pairID int64
		var poolAddress string
		if err := rows.Scan(&pairID, &poolAddress); err != nil {
			panic("Failed pool scan: " + err.Error())
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
// FILE PARSING OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// loadArbitrageCyclesFromFile parses triangular arbitrage cycles from text file.
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
		panic("Failed reading cycles file: " + err.Error())
	}

	// Count lines for allocation
	lineCount := 0
	for _, b := range data {
		if b == '\n' {
			lineCount++
		}
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		lineCount++
	}

	cycles := make([]router.ArbitrageTriangle, lineCount)
	cycleIndex := 0
	i, dataLen := 0, len(data)
	var pairIDs [3]uint64

	// Parse byte-by-byte for efficiency
	for i < dataLen && cycleIndex < lineCount {
		pairCount := 0

		for pairCount < 3 && i < dataLen && data[i] != '\n' {
			// Find opening parenthesis
			for i < dataLen && data[i] != '(' && data[i] != '\n' {
				i++
			}
			if i >= dataLen || data[i] == '\n' {
				break
			}
			i++

			// Parse numeric ID
			pairID := uint64(0)
			for i < dataLen && data[i] >= '0' && data[i] <= '9' {
				if pairID > (^uint64(0)-10)/10 {
					break
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

		cycles[cycleIndex] = router.ArbitrageTriangle{
			router.TradingPairID(pairIDs[0]),
			router.TradingPairID(pairIDs[1]),
			router.TradingPairID(pairIDs[2]),
		}
		cycleIndex++
	}

	if cycleIndex == 0 {
		panic("No valid cycles found in configuration")
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
	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 0: FOUNDATION INITIALIZATION
	//═══════════════════════════════════════════════════════════════════════════════════════

	debug.DropMessage("INIT", "System startup")

	// Load trading pairs from database
	db := openDatabase("uniswap_pairs.db")
	pools := loadPoolsFromDatabase(db)
	db.Close()

	// Register addresses for fast lookup
	for _, pool := range pools {
		router.RegisterTradingPairAddress([]byte(pool.Address[2:]), router.TradingPairID(pool.ID))
	}
	debug.DropMessage("ADDR", "Registered "+utils.Itoa(len(pools))+" addresses")

	// Load arbitrage cycle definitions
	cycles := loadArbitrageCyclesFromFile("cycles_3_3.txt")
	debug.DropMessage("CYCLE", "Loaded "+utils.Itoa(len(cycles))+" cycles")

	debug.DropMessage("LOAD", utils.Itoa(len(pools))+"p "+utils.Itoa(len(cycles))+"c")

	setupSignalHandling()
	debug.DropMessage("SIG", "Handler ready")

	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 1: BLOCKCHAIN SYNCHRONIZATION
	//═══════════════════════════════════════════════════════════════════════════════════════

	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("SYNC", "Check failed: "+err.Error())
			continue
		}
		if !syncNeeded {
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", utils.Itoa(int(blocksBehind))+" behind")

		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("SYNC", "Harvest failed: "+err.Error())
			continue
		}
	}

	// Initialize arbitrage detection system
	router.InitializeArbitrageSystem(cycles)
	debug.DropMessage("ARB", "System initialized")

	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 2: MEMORY OPTIMIZATION
	//═══════════════════════════════════════════════════════════════════════════════════════

	debug.DropMessage("GC", "Starting optimization")
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()
	debug.DropMessage("GC", "Initial complete")

	// Verify synchronization after GC
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("SYNC", "Post-GC check failed: "+err.Error())
			continue
		}
		if !syncNeeded {
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Post-GC "+utils.Itoa(int(blocksBehind))+" behind")

		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("SYNC", "Post-GC harvest failed: "+err.Error())
			continue
		}
	}

	// Track synchronization progress across reconnections
	latestTempSyncedBlock = syncharvester.LoadMetadata()
	debug.DropMessage("META", "Sync block "+utils.Itoa(int(latestTempSyncedBlock)))

	// Load reserve data into arbitrage engine
	if err := syncharvester.FlushHarvestedReservesToRouter(); err != nil {
		panic("Reserve data flush failed: " + err.Error())
	}
	debug.DropMessage("RSRV", "Data loaded")

	debug.DropMessage("GC", "Final optimization")
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()
	debug.DropMessage("GC", "Final complete")

	// Disable automatic GC permanently for hot spinning
	rtdebug.SetGCPercent(-1)
	debug.DropMessage("GC", "Disabled")

	// Signal workers: GC disabled, hot spin mode safe
	router.SignalGCComplete()
	debug.DropMessage("HOT", "Spin mode enabled")

	debug.DropMessage("PROD", "Active")
	runtime.LockOSThread()
	control.ForceActive()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN ORCHESTRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// main executes real-time arbitrage detection engine.
// Phase 3: Continuous event processing with automatic reconnection.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func main() {
	// Panic recovery with resource cleanup
	defer func() {
		if r := recover(); r != nil {
			debug.DropMessage("PANIC", "Recovered: "+fmt.Sprintf("%v", r))
			debug.DropMessage("STACK", string(rtdebug.Stack()))

			// Close TLS connection
			if tlsConn != nil {
				tlsConn.Close()
				tlsConn = nil
			}

			// Close raw TCP connection
			if rawConn != nil {
				rawConn.Close()
				rawConn = nil
			}

			// Clean up temporary files
			os.Remove(constants.HarvesterTempPath)

			debug.DropMessage("CLEAN", "Resources cleaned")
		}
	}()

	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 3: REAL-TIME EVENT PROCESSING
	//═══════════════════════════════════════════════════════════════════════════════════════

	for {
		// Establish TCP connection with optimizations
		debug.DropMessage("CONN", "Establishing")
		rawConn, _ = net.Dial("tcp", constants.WsDialAddr)
		tcpConn := rawConn.(*net.TCPConn)

		// Configure TCP socket options
		tcpConn.SetNoDelay(true)
		tcpConn.SetReadBuffer(constants.MaxFrameSize)
		tcpConn.SetWriteBuffer(constants.MaxFrameSize)

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
		rawFile.Close()
		debug.DropMessage("OPT", "Socket optimized")

		// Synchronize missing blocks before WebSocket connection
		for {
			syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirementFromBlock(latestTempSyncedBlock)
			if err != nil {
				debug.DropMessage("SYNC", "Temp check failed: "+err.Error())
				continue
			}
			if !syncNeeded {
				break
			}

			blocksBehind := targetBlock - lastBlock
			debug.DropMessage("SYNC", "Temp "+utils.Itoa(int(blocksBehind))+" behind")

			newLastProcessed, err := syncharvester.ExecuteHarvestingToTemp(constants.DefaultConnections)
			if err != nil {
				debug.DropMessage("SYNC", "Temp harvest failed: "+err.Error())
				continue
			}

			latestTempSyncedBlock = newLastProcessed
		}

		// Load reserve data from temporary storage
		for {
			err := syncharvester.FlushHarvestedReservesToRouterFromTemp()
			if err == nil {
				os.Remove(constants.HarvesterTempPath)
				break
			}
			debug.DropMessage("SYNC", "Temp flush failed: "+err.Error())
		}
		debug.DropMessage("SYNC", "Temp data loaded")

		// Establish secure WebSocket connection
		tlsConn = tls.Client(rawConn, &tls.Config{ServerName: constants.WsHost})

		// Initialize WebSocket and subscribe to events
		ws.Handshake(tlsConn)
		ws.SendSubscription(tlsConn)
		debug.DropMessage("WS", "Connected")

		// Event processing loop
		debug.DropMessage("PROC", "Starting")
		for {
			payload, err := ws.SpinUntilCompleteMessage(tlsConn)
			if err != nil {
				debug.DropMessage("CONN", "Lost, reconnecting")
				tlsConn.Close()
				break
			}

			parser.HandleFrame(payload)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
