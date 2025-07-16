// ════════════════════════════════════════════════════════════════════════════════════════════════
// 🚀 HIGH-PERFORMANCE ARBITRAGE DETECTION SYSTEM
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Main Entry Point & Orchestration with Bootstrap Integration
//
// 🇯🇵 MADE IN JAPAN. INSPIRED BY JAPANESE ENGINEERING. FROM NIIKAPPU HIDAKA HOKKAIDO 🇯🇵
//
// Description:
//   Orchestrates a complete arbitrage detection pipeline with integrated bootstrap synchronization.
//   The system first initializes all core components, then ensures historical data completeness
//   through bootstrap synchronization, performs cleanup, and finally enters production processing.
//
// Architecture:
//   1. System initialization - loads cycles, pools, and initializes router
//   2. Bootstrap synchronization - ensures complete historical data coverage
//   3. Resource cleanup - prepares system for production processing
//   4. Production processing - continuous real-time event stream processing
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

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
	"time"

	"main/constants"
	"main/debug"
	"main/parser"
	"main/router"
	"main/syncharvest"
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
// GC is NOT disabled here - that happens after bootstrap completion.
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

	debug.DropMessage("INIT_COMPLETE", "System ready for bootstrap")
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN EXECUTION ORCHESTRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// main implements the primary system orchestration with integrated bootstrap.
// The system operates in distinct phases to ensure complete data coverage
// while maintaining production processing reliability.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func main() {
	debug.DropMessage("MAIN_START", "Starting system orchestration")

	// PHASE 1: BOOTSTRAP SYNCHRONIZATION
	// Ensures complete historical data coverage before production processing begins.
	// The bootstrap system either catches up to latest block height and exits,
	// or handles CTRL+C gracefully by saving current state and exiting.
	bootstrapStart := time.Now()

	if requiresBootstrapSync() {
		debug.DropMessage("BOOTSTRAP_EXECUTE", "Running bootstrap synchronization")

		// Execute detached bootstrap synchronization
		// This process runs to completion and terminates, ensuring no interference
		// with production processing. Handles CTRL+C gracefully with state persistence.
		if err := executeBootstrapSync(); err != nil {
			debug.DropMessage("BOOTSTRAP_ERROR", fmt.Sprintf("Bootstrap synchronization failed: %v", err))
			debug.DropMessage("BOOTSTRAP_CONTINUE", "Continuing with available data")
		} else {
			debug.DropMessage("BOOTSTRAP_SUCCESS", "Bootstrap synchronization completed")
		}
	} else {
		debug.DropMessage("BOOTSTRAP_SKIP", "Bootstrap synchronization not required")
	}

	bootstrapElapsed := time.Since(bootstrapStart)
	debug.DropMessage("BOOTSTRAP_DURATION", fmt.Sprintf("Bootstrap phase completed in %v", bootstrapElapsed))

	// PHASE 2: RESOURCE CLEANUP
	// Performs aggressive cleanup of bootstrap artifacts to ensure clean production startup.
	// This phase eliminates any residual memory or resources from bootstrap operations.
	cleanupStart := time.Now()
	performPostBootstrapCleanup()
	cleanupElapsed := time.Since(cleanupStart)
	debug.DropMessage("CLEANUP_DURATION", fmt.Sprintf("Resource cleanup completed in %v", cleanupElapsed))

	// PHASE 3: FINAL PRODUCTION PREPARATION
	// After bootstrap is complete and cleanup is done, NOW we disable GC for production
	debug.DropMessage("FINAL_GC", "Final garbage collection before production")

	// ONLY place in the system with explicit GC calls - final cleanup before production
	runtime.GC()
	runtime.GC()

	// NOW disable garbage collection for production operation
	// This eliminates GC pauses during latency-critical event processing
	rtdebug.SetGCPercent(-1)
	debug.DropMessage("GC_DISABLED", "Garbage collector disabled for production operation")

	// PHASE 4: PRODUCTION PROCESSING
	// Enters continuous production event processing with the primary event loop.
	// This phase runs indefinitely, processing real-time WebSocket events with automatic recovery.
	totalBootstrapTime := time.Since(bootstrapStart)
	debug.DropMessage("TOTAL_BOOTSTRAP_TIME", fmt.Sprintf("Total bootstrap overhead: %v", totalBootstrapTime))
	debug.DropMessage("PRODUCTION_START", "Starting production processing")

	// Lock to OS thread for consistent CPU affinity and syscall handling
	runtime.LockOSThread()

	// Enter the main production processing loop
	// This infinite loop maintains continuous operation despite network failures or other errors
	runProductionEventLoop()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// BOOTSTRAP SYNCHRONIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// requiresBootstrapSync determines whether bootstrap synchronization is necessary.
// This function performs a quick check without creating persistent objects or connections.
// Returns true if the system is behind the target synchronization block.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func requiresBootstrapSync() bool {
	// Perform lightweight synchronization status check
	// This function opens minimal database connections and queries sync metadata
	syncNeeded, lastBlock, targetBlock, err := syncharvest.CheckIfSyncNeeded()
	if err != nil {
		debug.DropMessage("BOOTSTRAP_CHECK_ERROR", fmt.Sprintf("Synchronization check failed: %v", err))
		return false // Conservative approach - don't bootstrap if check fails
	}

	// Log synchronization status for monitoring
	if syncNeeded {
		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("BOOTSTRAP_NEEDED", fmt.Sprintf("Synchronization required: %d blocks behind", blocksBehind))
	} else {
		debug.DropMessage("BOOTSTRAP_CURRENT", fmt.Sprintf("System current at block %d", lastBlock))
	}

	return syncNeeded
}

// executeBootstrapSync runs the detached bootstrap synchronization process.
// This function creates a completely isolated bootstrap environment that:
// 1. Syncs from last processed block to current blockchain head
// 2. Handles CTRL+C gracefully with state persistence
// 3. Uses ring buffer with pinned consumer for database writes
// 4. Terminates completely when sync is complete or interrupted
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func executeBootstrapSync() error {
	// Execute the detached synchronization process
	// This function encapsulates the entire bootstrap lifecycle:
	// - Creates harvester with signal handling
	// - Processes blocks in parallel with ring buffer
	// - Saves per-pair sync state continuously
	// - Handles interruption gracefully
	// - Terminates completely when done
	return syncharvest.ExecuteDetachedSync()
}

// performPostBootstrapCleanup performs aggressive resource cleanup after bootstrap completion.
// This function ensures that no bootstrap artifacts remain in memory, providing a clean
// environment for production processing startup.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func performPostBootstrapCleanup() {
	debug.DropMessage("CLEANUP_START", "Starting post-bootstrap resource cleanup")

	// Force return of unused memory to the operating system
	// This ensures minimal memory footprint for production processing
	rtdebug.FreeOSMemory()

	debug.DropMessage("CLEANUP_COMPLETE", "Post-bootstrap cleanup completed")
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PRODUCTION PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// runProductionEventLoop implements the main production processing loop with automatic recovery.
// This function maintains continuous operation despite network failures or other errors.
// The loop establishes WebSocket connections, processes events, and handles reconnection seamlessly.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func runProductionEventLoop() {
	debug.DropMessage("EVENT_LOOP_START", "Starting production event processing loop")

	// Infinite loop with automatic error recovery
	// This ensures the system remains operational despite transient failures
	for {
		// Attempt to process event stream
		// Each iteration represents a complete WebSocket connection lifecycle
		if err := processEventStream(); err != nil {
			debug.DropError("STREAM_ERROR", err)
			// Loop continues, establishing new connection automatically
		}
	}
}

// processEventStream manages a single WebSocket connection lifecycle.
// This function establishes the connection, performs handshake, subscribes to events,
// and processes incoming messages until an error occurs.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func processEventStream() error {
	// Create optimized network connection
	// This includes TCP optimizations and TLS configuration
	conn, err := establishConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	// Perform WebSocket protocol upgrade
	// This negotiates the WebSocket protocol with the remote server
	if err := ws.Handshake(conn); err != nil {
		return err
	}

	// Subscribe to Ethereum log events
	// This sends the subscription request for contract event notifications
	if err := ws.SendSubscription(conn); err != nil {
		return err
	}

	debug.DropMessage("STREAM_READY", "Event stream processing active")

	// Main event processing loop
	// This loop processes incoming WebSocket messages until an error occurs
	for {
		// Receive complete WebSocket message
		// This function handles WebSocket framing and message assembly
		payload, err := ws.SpinUntilCompleteMessage(conn)
		if err != nil {
			return err
		}

		// Parse and route the event for arbitrage detection
		// This processes the JSON-RPC event through the existing pipeline
		parser.HandleFrame(payload)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATA LOADING - ARBITRAGE CYCLES
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

		// Clear pairIDs array after each line
		for j := range pairIDs {
			pairIDs[j] = 0
		}
	}

	linesStr := utils.Itoa(lineCount)
	debug.DropMessage("LINES_PROCESSED", "Total lines: "+linesStr)

	if len(cycles) == 0 {
		// Clear data before returning error
		data = nil
		return nil, fmt.Errorf("no valid arbitrage cycles found in %s", filename)
	}

	// Release file data immediately to minimize memory usage
	data = nil

	return cycles, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATA LOADING - TRADING POOLS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

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

		// Clear temporary variables immediately after use
		poolAddress = ""
		token0Address = ""
		token1Address = ""
		exchangeName = ""

		// Periodic progress updates for large databases
		if count%100000 == 0 {
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

		// Clear temporary strings immediately after use
		indexStr = ""
		pair0Str = ""
		pair1Str = ""
		pair2Str = ""
		cycleInfo = ""
	}

	// Indicate if additional cycles exist beyond the sample
	if len(cycles) > 10 {
		remainingStr := utils.Itoa(len(cycles) - 10)
		debug.DropMessage("CYCLES_TRUNCATED", "... and "+remainingStr+" more cycles")

		// Clear temporary string
		remainingStr = ""
	}

	// Clear cycle count string
	cycleCountStr = ""
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// NETWORK OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// establishConnection creates a TLS-secured TCP connection with optimizations.
// The connection is configured for reliable operation with the WebSocket endpoint.
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

	// Disable Nagle's algorithm for reduced latency
	tcpConn.SetNoDelay(true)

	// Set buffer sizes to match WebSocket frame requirements
	tcpConn.SetReadBuffer(constants.MaxFrameSize)
	tcpConn.SetWriteBuffer(constants.MaxFrameSize)

	// Apply platform-specific socket optimizations
	if rawFile, err := tcpConn.File(); err == nil {
		optimizeSocket(int(rawFile.Fd()))
		rawFile.Close() // Close the file descriptor duplicate
	}

	// Upgrade to TLS with server name indication
	tlsConn := tls.Client(raw, &tls.Config{
		ServerName: constants.WsHost, // Required for SNI
	})

	return tlsConn, nil
}

// optimizeSocket applies platform-specific TCP/IP stack optimizations.
// These settings are tuned for reliable WebSocket communication.
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

	// Platform-specific optimizations for reliable operation
	switch runtime.GOOS {
	case "linux":
		// Linux-specific optimizations for network reliability
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)         // SO_BUSY_POLL
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

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// BOOTSTRAP UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// RunBootstrapOnly executes bootstrap synchronization in isolation.
// This function is useful for manual synchronization without starting production processing.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func RunBootstrapOnly() error {
	debug.DropMessage("BOOTSTRAP_STANDALONE", "Running standalone bootstrap synchronization")

	// Check if bootstrap is required
	if requiresBootstrapSync() {
		// Execute bootstrap synchronization
		if err := executeBootstrapSync(); err != nil {
			return fmt.Errorf("bootstrap synchronization failed: %w", err)
		}
	} else {
		debug.DropMessage("BOOTSTRAP_UNNECESSARY", "Bootstrap synchronization not required")
	}

	// Perform cleanup
	performPostBootstrapCleanup()

	debug.DropMessage("BOOTSTRAP_STANDALONE_COMPLETE", "Standalone bootstrap synchronization complete")
	return nil
}

// CheckSystemStatus returns current synchronization status without side effects.
// This function provides synchronization state information for monitoring purposes.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func CheckSystemStatus() (syncNeeded bool, lastBlock, targetBlock uint64, err error) {
	return syncharvest.CheckIfSyncNeeded()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM DIAGNOSTICS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// analyzeSystemState provides diagnostic information about current system state.
// This function is useful for monitoring and debugging system resource usage.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func analyzeSystemState() {
	debug.DropMessage("SYSTEM_ANALYSIS", "Analyzing system state")

	// Collect memory statistics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Report memory usage
	allocatedStr := utils.Itoa(int(m.Alloc / 1024))
	systemStr := utils.Itoa(int(m.Sys / 1024))
	gcStr := utils.Itoa(int(m.NumGC))

	debug.DropMessage("MEMORY_USAGE", fmt.Sprintf(
		"Allocated: %s KB, System: %s KB, GC cycles: %s",
		allocatedStr, systemStr, gcStr,
	))

	// Report goroutine count
	goroutineStr := utils.Itoa(runtime.NumGoroutine())
	debug.DropMessage("GOROUTINE_COUNT", fmt.Sprintf(
		"Active goroutines: %s", goroutineStr,
	))

	// Clear temporary strings immediately after use
	allocatedStr = ""
	systemStr = ""
	gcStr = ""
	goroutineStr = ""

	debug.DropMessage("SYSTEM_ANALYSIS_COMPLETE", "System state analysis complete")
}
