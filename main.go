// ════════════════════════════════════════════════════════════════════════════════════════════════
// 🚀 HIGH-PERFORMANCE ARBITRAGE DETECTION SYSTEM
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Main Entry Point & Clean Orchestration
//
// 🇯🇵 MADE IN JAPAN. INSPIRED BY JAPANESE ENGINEERING. FROM NIIKAPPU HIDAKA HOKKAIDO 🇯🇵
//
// Description:
//   Clean orchestration with proper phase separation and GC management.
//   Bootstrap → Cleanup → GC Disable → Production WebSocket Processing
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package main

import (
	"crypto/tls"
	"database/sql"
	"net"
	"os"
	"runtime"
	rtdebug "runtime/debug"
	"strings"
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
// GLOBAL DATABASE CONNECTION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// pairsDB holds the shared database connection for trading pairs
// Reused by sync harvester to avoid duplicate connections
var pairsDB *sql.DB

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// init performs mandatory system bootstrap and initializes all core components.
// Both arbitrage cycles and trading pair database must load successfully.
// Panics immediately on any failure to prevent invalid system state.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func init() {
	debug.DropMessage("INIT", "Loading system data")

	// Both data sources are mandatory for operation
	// Cycles define trading paths, database provides address resolution
	cycles := loadArbitrageCyclesFromFile("cycles_3_3.txt")
	poolCount := loadPoolsFromDatabase("uniswap_pairs.db")

	debug.DropMessage("LOADED", utils.Itoa(len(cycles))+" cycles, "+utils.Itoa(poolCount)+" pools")

	// Display sample cycles for verification during system startup
	// Shows actual cycle data to confirm parsing accuracy
	for i := 0; i < 3 && i < len(cycles); i++ {
		c := cycles[i]
		debug.DropMessage("CYCLE", utils.Itoa(i+1)+": ("+utils.Itoa(int(c[0]))+")→("+utils.Itoa(int(c[1]))+")→("+utils.Itoa(int(c[2]))+")")
	}

	// Initialize multi-core arbitrage detection system
	// Distributes cycles across CPU cores and establishes message routing
	router.InitializeArbitrageSystem(cycles)
	debug.DropMessage("READY", "System initialized")
}

// loadArbitrageCyclesFromFile parses triangular arbitrage cycles from configuration file.
// Expected format: lines containing three parenthesized integers representing pair IDs.
// Example line: "(12345) → (67890) → (11111)" defines one complete arbitrage triangle.
// Returns slice of valid triangles or panics on file errors or empty results.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadArbitrageCyclesFromFile(filename string) []router.ArbitrageTriangle {
	// Read entire file into memory for efficient parsing
	// Small config files allow full in-memory processing
	data, err := os.ReadFile(filename)
	if err != nil {
		panic("Failed to load cycles: " + err.Error())
	}

	// Pre-allocate slice capacity based on estimated cycles per file size
	// Heuristic: ~50 characters per cycle line reduces memory reallocations
	// Format: "(12345) → (67890) → (11111)\n" ≈ 50 characters
	cycles := make([]router.ArbitrageTriangle, 0, len(data)/50)
	i, dataLen := 0, len(data)

	// Parse file content byte-by-byte for maximum parsing efficiency
	// Avoids string allocations and regex overhead
	for i < dataLen {
		var pairIDs [3]uint64
		pairCount := 0

		// Extract exactly 3 pair IDs from current line to form valid triangle
		// Each triangle requires 3 pairs to create arbitrage opportunity
		for pairCount < 3 && i < dataLen && data[i] != '\n' {
			// Locate opening parenthesis that precedes each pair ID
			// Skip whitespace and formatting characters
			for i < dataLen && data[i] != '(' && data[i] != '\n' {
				i++
			}
			if i >= dataLen || data[i] == '\n' {
				break
			}
			i++

			// Parse numeric pair ID with overflow protection
			// Pair IDs are positive integers representing trading pair contracts
			pairID := uint64(0)
			for i < dataLen && data[i] >= '0' && data[i] <= '9' {
				// Prevent arithmetic overflow during decimal conversion
				// Check if multiplication would exceed uint64 maximum
				if pairID > (^uint64(0)-10)/10 {
					break
				}
				pairID = pairID*10 + uint64(data[i]-'0')
				i++
			}

			// Store valid non-zero pair ID for triangle construction
			// Zero IDs are invalid and indicate parsing errors
			if pairID > 0 {
				pairIDs[pairCount] = pairID
				pairCount++
			}

			// Skip to closing parenthesis and any trailing formatting
			// Handles various spacing and arrow formatting styles
			for i < dataLen && data[i] != ')' && data[i] != '\n' {
				i++
			}
			if i < dataLen && data[i] == ')' {
				i++
			}
		}

		// Advance to next line for continued parsing
		// Handles both Unix (\n) and Windows (\r\n) line endings
		for i < dataLen && data[i] != '\n' {
			i++
		}
		if i < dataLen {
			i++
		}

		// Create arbitrage triangle only if exactly 3 valid pair IDs found
		// Incomplete triangles cannot form arbitrage opportunities
		if pairCount == 3 {
			cycles = append(cycles, router.ArbitrageTriangle{
				router.TradingPairID(pairIDs[0]),
				router.TradingPairID(pairIDs[1]),
				router.TradingPairID(pairIDs[2]),
			})
		}
	}

	// Ensure at least one valid cycle was parsed from file
	// Empty cycle set renders the arbitrage system non-functional
	if len(cycles) == 0 {
		panic("No cycles found")
	}
	return cycles
}

// loadPoolsFromDatabase loads trading pair addresses from SQLite database.
// Populates global address-to-pair-ID lookup table for blockchain event routing.
// Stores database connection globally for reuse by sync harvester.
// Returns count of loaded pools or panics on any database operation failure.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadPoolsFromDatabase(dbPath string) int {
	// Open SQLite database connection with error checking
	// Database contains trading pair metadata and contract addresses
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		panic("Failed to open database " + dbPath + ": " + err.Error())
	}

	// Store database connection globally for sync harvester reuse
	// This avoids opening duplicate connections and improves efficiency
	pairsDB = db

	// Query trading pairs ordered by ID for consistent processing
	// Only need ID and address for blockchain event routing
	rows, err := db.Query(`
		SELECT p.id, p.pool_address 
		FROM pools p
		ORDER BY p.id`)
	if err != nil {
		panic("Failed to query pools: " + err.Error())
	}
	defer rows.Close()

	count := 0
	// Reuse byte buffer to minimize allocations during address processing
	// Ethereum addresses are 40 hex characters requiring byte conversion
	poolAddressBytes := make([]byte, 0, 40)

	// Process each trading pair and register its contract address
	// Address registration enables routing of blockchain events to pair IDs
	for rows.Next() {
		var pairID int64
		var poolAddress string
		if err := rows.Scan(&pairID, &poolAddress); err != nil {
			panic("Failed to scan pool row: " + err.Error())
		}

		// Convert hex address string to byte slice for hash table registration
		// Remove "0x" prefix and store raw hex characters
		poolAddressBytes = poolAddressBytes[:0]
		poolAddressBytes = append(poolAddressBytes, strings.TrimPrefix(poolAddress, "0x")...)

		// Register address mapping in router's lookup table
		// Enables O(1) address-to-pair-ID resolution during event processing
		router.RegisterTradingPairAddress(poolAddressBytes, router.TradingPairID(pairID))
		count++
	}

	// Check for database iteration errors after processing all rows
	// SQL errors might not surface until iteration completes
	if err := rows.Err(); err != nil {
		panic("Database iteration error: " + err.Error())
	}

	// Ensure at least one pool was loaded from database
	// Empty database renders address resolution non-functional
	if count == 0 {
		panic("No pools loaded from database")
	}

	return count
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN ORCHESTRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// main orchestrates complete system lifecycle through distinct operational phases.
// Phase 1: Bootstrap synchronization ensures current blockchain state
// Phase 2: Memory optimization prepares deterministic runtime environment
// Phase 3: Production processing handles continuous event stream
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func main() {
	// PHASE 1: Bootstrap synchronization with blockchain state
	// Loop until completely caught up with blockchain
	for {
		syncNeeded, lastBlock, targetBlock, _ := syncharvester.CheckIfPeakSyncNeeded()
		if !syncNeeded {
			// Fully caught up, exit sync loop
			debug.DropMessage("SYNC", "Fully synchronized with blockchain")
			break
		}

		// Calculate synchronization workload for progress indication
		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Syncing "+utils.Itoa(int(blocksBehind))+" blocks")

		// Execute blockchain state synchronization to current block
		// Pass existing database connection to avoid duplicate connections
		syncharvester.ExecutePeakSyncWithDB(pairsDB)

		// Loop continues to check if more sync is needed
		// This handles cases where new blocks arrive during sync
	}

	// PHASE 2: Memory optimization for deterministic production runtime
	// Eliminates garbage collection pauses during event processing

	// Double garbage collection ensures complete memory cleanup
	// First GC marks unreachable objects, second GC finalizes cleanup
	runtime.GC()
	runtime.GC()

	// Return unused memory pages to operating system
	// Reduces memory footprint and improves cache locality
	rtdebug.FreeOSMemory()

	// CRITICAL: Re-check sync after GC cleanup which could have taken time
	// Blockchain doesn't pause during our memory optimization
	for {
		syncNeeded, lastBlock, targetBlock, _ := syncharvester.CheckIfPeakSyncNeeded()
		if !syncNeeded {
			// Still caught up after GC, proceed to production
			debug.DropMessage("SYNC", "Confirmed synchronized post-GC")
			break
		}

		// New blocks arrived during GC cleanup
		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Post-GC sync: "+utils.Itoa(int(blocksBehind))+" blocks")

		// Catch up with blocks that arrived during memory cleanup
		syncharvester.ExecutePeakSyncWithDB(pairsDB)
	}

	// Flush all synced reserves to router before entering production mode
	// This loads the latest reserve data into the router for arbitrage detection
	if err := syncharvester.FlushSyncedReservesToRouter(); err != nil {
		debug.DropMessage("FLUSH_ERROR", err.Error())
		// Continue anyway - router will work with whatever data it has
	}

	// Disable garbage collector for deterministic latency
	// Trading systems require predictable response times
	rtdebug.SetGCPercent(-1)

	// PHASE 3: Production event processing with NUMA optimization
	// Lock thread to CPU core for consistent memory access patterns
	runtime.LockOSThread()

	// Force the system into hot state for immediate processing
	// This ensures arbitrage detection starts without waiting for first event
	control.ForceHot()

	// Infinite processing loop with automatic connection recovery
	// Each connection failure triggers immediate reconnection attempt
	for {
		processEventStream()
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PRODUCTION EVENT PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processEventStream establishes WebSocket connection and processes blockchain events.
// Handles complete connection lifecycle: setup, optimization, handshake, event processing.
// Returns error on connection failure to trigger automatic reconnection by caller.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func processEventStream() error {
	// Establish TCP connection to WebSocket endpoint
	// Raw TCP connection allows fine-grained socket optimization
	raw, _ := net.Dial("tcp", constants.WsDialAddr)
	tcpConn := raw.(*net.TCPConn)

	// Configure TCP socket for minimal latency operation
	// Disable Nagle algorithm for immediate packet transmission
	tcpConn.SetNoDelay(true)

	// Set buffer sizes to match expected frame sizes
	// Prevents partial reads and optimizes memory usage
	tcpConn.SetReadBuffer(constants.MaxFrameSize)
	tcpConn.SetWriteBuffer(constants.MaxFrameSize)

	// Apply operating system level socket optimizations
	// Access raw file descriptor for advanced socket configuration
	rawFile, _ := tcpConn.File()
	fd := int(rawFile.Fd())

	// Core TCP optimizations applicable to all platforms
	// TCP_NODELAY: Disable Nagle algorithm for immediate transmission
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)

	// SO_RCVBUF/SO_SNDBUF: Optimize kernel buffer sizes for throughput
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)

	// Platform-specific network optimizations for enhanced performance
	// Each OS provides different advanced networking features
	switch runtime.GOOS {
	case "linux":
		// SO_REUSEPORT: Enable port reuse for load balancing
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)

		// TCP_CONGESTION: Use BBR congestion control for optimal throughput
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr")
	case "darwin":
		// SO_REUSEPORT: Enable port reuse on macOS
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1)
	}
	rawFile.Close()

	// Upgrade TCP connection to TLS for secure WebSocket communication
	// Modern blockchain APIs require encrypted connections
	conn := tls.Client(raw, &tls.Config{ServerName: constants.WsHost})

	// Complete WebSocket handshake protocol
	// Establishes WebSocket protocol over TLS connection
	ws.Handshake(conn)

	// Send subscription message to receive relevant blockchain events
	// Configures event filtering for arbitrage-relevant transactions
	ws.SendSubscription(conn)

	// Continuous event processing loop until connection failure
	// Each iteration processes one complete WebSocket frame
	for {
		// Read complete WebSocket frame from connection
		// Blocks until full frame is received or connection fails
		payload, err := ws.SpinUntilCompleteMessage(conn)
		if err != nil {
			// Close connection and return error for reconnection attempt
			conn.Close()
			return err
		}

		// Dispatch parsed event to arbitrage detection engine
		// Frame parsing and routing handled by specialized parser
		parser.HandleFrame(payload)
	}
}
