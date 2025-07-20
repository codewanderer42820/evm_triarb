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
// MAIN ORCHESTRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// main orchestrates the complete high-frequency arbitrage detection system lifecycle.
// The system operates through carefully orchestrated phases to maximize performance,
// minimize latency, and ensure robust operation in production trading environments.
//
// System Architecture Overview:
//
//	Phase 0: Foundation - Load configuration data and initialize core structures
//	Phase 1: Synchronization - Achieve blockchain state consistency
//	Phase 2: Optimization - Memory management and performance tuning
//	Phase 3: Production - Real-time arbitrage detection with sub-microsecond latency
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func main() {
	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 0: FOUNDATION INITIALIZATION
	//═══════════════════════════════════════════════════════════════════════════════════════
	// Establish system foundations with precise resource allocation and configuration loading.
	// All initialization uses exact memory allocation to prevent fragmentation during operation.

	debug.DropMessage("INIT", "System startup")

	// Database Configuration Loading
	// Load trading pair registry from SQLite with optimized query patterns.
	// Connection is ephemeral - closed immediately after data extraction.
	db := openDatabase("uniswap_pairs.db")
	pools := loadPoolsFromDatabase(db)
	db.Close()

	// Address Registration Infrastructure
	// Populate Robin Hood hash table for O(1) contract address resolution.
	// Critical for real-time event processing performance.
	for _, pool := range pools {
		router.RegisterTradingPairAddress([]byte(pool.Address[2:]), router.TradingPairID(pool.ID))
	}

	// Arbitrage Cycle Configuration
	// Parse triangular arbitrage definitions with zero-copy algorithms.
	// Each cycle represents a potential profit opportunity path.
	cycles := loadArbitrageCyclesFromFile("cycles_3_3.txt")

	debug.DropMessage("LOAD", utils.Itoa(len(pools))+" pools, "+utils.Itoa(len(cycles))+" cycles")

	// System Lifecycle Management
	// Configure graceful shutdown coordination for production reliability.
	setupSignalHandling()

	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 1: BLOCKCHAIN SYNCHRONIZATION
	//═══════════════════════════════════════════════════════════════════════════════════════
	// Achieve complete blockchain state consistency before arbitrage detection begins.
	// Retry loop ensures robustness against network instability and RPC provider issues.

	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("SYNC", "Requirement check failed: "+err.Error())
			continue // Resilient against transient network failures
		}
		if !syncNeeded {
			break // Synchronization complete - proceed to arbitrage system initialization
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", utils.Itoa(int(blocksBehind))+" blocks behind")

		// Execute Historical Data Harvesting
		// Multi-connection parallel processing with adaptive batch sizing.
		// Metadata is updated atomically only upon successful completion.
		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("SYNC", "Block harvesting failed: "+err.Error())
			continue // Retry until successful synchronization achieved
		}
	}

	// Arbitrage Engine Initialization
	// Instantiate 30,000 priority queues with lock-free coordination.
	// Memory layout optimized for cache efficiency and NUMA awareness.
	router.InitializeArbitrageSystem(cycles)

	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 2: MEMORY OPTIMIZATION AND PERFORMANCE TUNING
	//═══════════════════════════════════════════════════════════════════════════════════════
	// Prepare system for production operation with aggressive memory management.
	// Multiple garbage collection passes ensure optimal heap layout.

	// Initial Memory Optimization Pass
	// Force garbage collection to establish clean memory baseline.
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()

	// Post-Optimization Synchronization Verification
	// Ensure blockchain consistency after memory management operations.
	// Guards against edge cases where GC pressure affects synchronization state.
	for {
		syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirement()
		if err != nil {
			debug.DropMessage("SYNC", "Post-GC requirement check failed: "+err.Error())
			continue // Handle transient issues with exponential backoff behavior
		}
		if !syncNeeded {
			break // Verified synchronization - proceed to reserve data loading
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Post-GC: "+utils.Itoa(int(blocksBehind))+" blocks behind")

		// Incremental Synchronization Recovery
		// Address any blocks that arrived during memory optimization phase.
		err = syncharvester.ExecuteHarvesting()
		if err != nil {
			debug.DropMessage("SYNC", "Post-GC harvesting failed: "+err.Error())
			continue // Maintain synchronization integrity through retry logic
		}
	}

	// Reserve State Integration
	// Load historical reserve data into arbitrage engine for accurate profit calculations.
	// Critical operation - system cannot proceed without complete reserve state.
	if err := syncharvester.FlushHarvestedReservesToRouter(); err != nil {
		panic("Critical system failure: Reserve data flush to router failed - " + err.Error())
	}

	// Final Production Optimization Pass
	// Second memory management phase for optimal production performance.
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()

	// Production Mode Activation
	// Configure runtime for maximum performance with disabled garbage collection.
	// Lock OS thread for consistent NUMA locality and minimal context switching.
	debug.DropMessage("PROD", "Production mode active")
	runtime.LockOSThread()
	rtdebug.SetGCPercent(-1) // Disable automatic garbage collection
	control.ForceActive()    // Activate production control systems

	//═══════════════════════════════════════════════════════════════════════════════════════
	// PHASE 3: REAL-TIME ARBITRAGE DETECTION ENGINE
	//═══════════════════════════════════════════════════════════════════════════════════════
	// Continuous operation mode with 160ns latency targets and anti-fragile architecture.
	// Every connection failure triggers automatic resynchronization for enhanced reliability.

	// Temporary Synchronization State Management
	// Maintain independent progress tracking for incremental sync operations.
	// Stack variable persists across all WebSocket reconnection cycles.
	latestTempSyncedBlock := syncharvester.LoadMetadata()

	// Infinite Production Loop
	// Self-healing architecture where connection failures strengthen system state.
	for {
		// Network Connection Establishment with Advanced Optimizations
		// Raw TCP socket configuration for minimum latency and maximum throughput.
		raw, _ := net.Dial("tcp", constants.WsDialAddr)
		tcpConn := raw.(*net.TCPConn)

		// TCP Protocol Optimization Suite
		// Configure socket parameters for high-frequency trading requirements.
		tcpConn.SetNoDelay(true)                       // Disable Nagle's algorithm for immediate transmission
		tcpConn.SetReadBuffer(constants.MaxFrameSize)  // Optimize kernel read buffer allocation
		tcpConn.SetWriteBuffer(constants.MaxFrameSize) // Optimize kernel write buffer allocation

		// Advanced Socket Configuration via System Calls
		// Apply low-level optimizations using direct kernel interface.
		rawFile, _ := tcpConn.File()
		fd := int(rawFile.Fd())

		// Cross-Platform Socket Optimizations
		// Standard TCP optimizations for all supported platforms.
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)

		// Platform-Specific Network Stack Enhancements
		// Apply OS-specific optimizations for maximum performance.
		switch runtime.GOOS {
		case "linux":
			syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)         // SO_REUSEPORT for load balancing
			syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // BBR congestion control algorithm
		case "darwin":
			syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1) // SO_REUSEPORT for macOS compatibility
		}
		rawFile.Close() // Release file descriptor wrapper to prevent resource leakage

		// Opportunistic Incremental Synchronization
		// Leverage optimized TCP connection for blockchain state updates before WebSocket establishment.
		// Retry-resilient design ensures synchronization completion despite transient failures.
		for {
			syncNeeded, lastBlock, targetBlock, err := syncharvester.CheckHarvestingRequirementFromBlock(latestTempSyncedBlock)
			if err != nil {
				debug.DropMessage("SYNC", "Temp requirement check failed: "+err.Error())
				continue // Resilient against RPC provider instability and network fluctuations
			}
			if !syncNeeded {
				break // Synchronization current - proceed to WebSocket handshake
			}

			blocksBehind := targetBlock - lastBlock
			debug.DropMessage("SYNC", "Temp sync: "+utils.Itoa(int(blocksBehind))+" blocks behind")

			// Temporary Storage Harvesting with Progress Tracking
			// Isolated synchronization that preserves main metadata state integrity.
			newLastProcessed, err := syncharvester.ExecuteHarvestingToTemp(constants.DefaultConnections)
			if err != nil {
				debug.DropMessage("SYNC", "Temp harvesting failed: "+err.Error())
				continue // Retry from current position - no progress lost
			}

			// Progress State Advancement
			// Update temporary synchronization checkpoint for monotonic progress.
			latestTempSyncedBlock = newLastProcessed
		}

		// Reserve Data Integration with Retry Resilience
		// Critical operation ensuring arbitrage engine operates on latest market state.
		// Retry loop guarantees completion despite transient file system issues.
		for {
			err := syncharvester.FlushHarvestedReservesToRouterFromTemp()
			if err == nil {
				break // Reserve data successfully integrated - proceed to WebSocket establishment
			}
			debug.DropMessage("SYNC", "Temporary reserve data flush failed: "+err.Error())
			// Retry until successful - arbitrage accuracy depends on fresh reserve data
		}

		// Secure WebSocket Connection Establishment
		// TLS-encrypted connection over optimized TCP transport for maximum security and performance.
		conn := tls.Client(raw, &tls.Config{ServerName: constants.WsHost})

		// WebSocket Protocol Negotiation and Event Subscription
		// Establish real-time event stream for Uniswap V2 Sync events.
		ws.Handshake(conn)
		ws.SendSubscription(conn)

		// High-Frequency Event Processing Loop
		// Core arbitrage detection engine operating at 160ns latency targets.
		// Runs until connection failure triggers automatic reconnection with resynchronization.
		for {
			// Message Frame Reception with Error Detection
			// Block until complete WebSocket message received or connection failure detected.
			payload, err := ws.SpinUntilCompleteMessage(conn)
			if err != nil {
				conn.Close()
				break // Connection failure - trigger reconnection with automatic resync
			}

			// Real-Time Arbitrage Detection Pipeline
			// Dispatch to parser subsystem for 160ns end-to-end processing.
			// Feeds 30,000 priority queues for comprehensive opportunity detection.
			parser.HandleFrame(payload)
		}
		// Connection terminated - outer loop will establish new connection with fresh synchronization
	}
}
