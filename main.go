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
	"os/signal"
	"runtime"
	rtdebug "runtime/debug"
	"sync"
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
// TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 32
type Pool struct {
	ID      int64   // 8B
	Address string  // 16B
	_       [8]byte // 8B padding to 32B
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL VARIABLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 8
var (
	// Shared database connection reused by sync harvester
	pairsDB *sql.DB

	// Global shutdown coordination
	shutdownWG sync.WaitGroup
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func init() {
	debug.DropMessage("INIT", "Loading system data")

	// Load database first to establish connection for sync harvester
	db := openDatabase("uniswap_pairs.db")
	pools := loadPoolsFromDatabase(db)
	cycles := loadArbitrageCyclesFromFile("cycles_3_3.txt")

	debug.DropMessage("LOADED", utils.Itoa(len(pools))+" pools, "+utils.Itoa(len(cycles))+" cycles")

	// Display samples for verification
	for i := 0; i < 3 && i < len(pools); i++ {
		p := pools[i]
		debug.DropMessage("POOL", utils.Itoa(i+1)+": ID "+utils.Itoa(int(p.ID))+" → "+p.Address)
	}

	for i := 0; i < 3 && i < len(cycles); i++ {
		c := cycles[i]
		debug.DropMessage("CYCLE", utils.Itoa(i+1)+": ("+utils.Itoa(int(c[0]))+")→("+utils.Itoa(int(c[1]))+")→("+utils.Itoa(int(c[2]))+")")
	}

	// Register addresses before router init so lookup table is populated
	for _, pool := range pools {
		router.RegisterTradingPairAddress([]byte(pool.Address[2:]), router.TradingPairID(pool.ID))
	}

	router.InitializeArbitrageSystem(cycles)
	debug.DropMessage("READY", "System initialized")
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN ORCHESTRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Three-phase orchestration: Bootstrap → Memory Cleanup → Production
func main() {
	setupSignalHandling()

	// PHASE 1: Bootstrap synchronization with blockchain state
	for {
		syncNeeded, lastBlock, targetBlock, _ := syncharvester.CheckIfPeakSyncNeeded()
		if !syncNeeded {
			debug.DropMessage("SYNC", "Fully synchronized with blockchain")
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Syncing "+utils.Itoa(int(blocksBehind))+" blocks")
		syncharvester.ExecutePeakSyncWithDB(pairsDB)
	}

	// PHASE 2: Memory optimization for deterministic runtime
	runtime.GC()
	runtime.GC()
	rtdebug.FreeOSMemory()

	// Re-check sync after GC (blockchain doesn't pause)
	for {
		syncNeeded, lastBlock, targetBlock, _ := syncharvester.CheckIfPeakSyncNeeded()
		if !syncNeeded {
			debug.DropMessage("SYNC", "Confirmed synchronized post-GC")
			break
		}

		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("SYNC", "Post-GC sync: "+utils.Itoa(int(blocksBehind))+" blocks")
		syncharvester.ExecutePeakSyncWithDB(pairsDB)
	}

	// Load synced reserves into router
	if err := syncharvester.FlushSyncedReservesToRouter(); err != nil {
		debug.DropMessage("FLUSH_ERROR", err.Error())
	}

	// PHASE 3: Production mode with GC disabled and thread affinity
	rtdebug.SetGCPercent(-1)
	runtime.LockOSThread()
	control.ForceHot()

	// Infinite reconnection loop
	for {
		processEventStream()
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATA LOADING FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Parses "(12345) → (67890) → (11111)" format with precise allocation
func loadArbitrageCyclesFromFile(filename string) []router.ArbitrageTriangle {
	data, err := os.ReadFile(filename)
	if err != nil {
		panic("Failed to load cycles: " + err.Error())
	}

	// Count lines for exact allocation
	lineCount := 0
	for _, b := range data {
		if b == '\n' {
			lineCount++
		}
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		lineCount++
	}

	cycles := make([]router.ArbitrageTriangle, 0, lineCount)
	i, dataLen := 0, len(data)
	var pairIDs [3]uint64

	// Byte-by-byte parsing without string allocations
	for i < dataLen {
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

			// Parse number with overflow protection
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

			// Skip to closing parenthesis
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

		// Trust data contains valid triangles
		cycles = append(cycles, router.ArbitrageTriangle{
			router.TradingPairID(pairIDs[0]),
			router.TradingPairID(pairIDs[1]),
			router.TradingPairID(pairIDs[2]),
		})
	}

	if len(cycles) == 0 {
		panic("No cycles found")
	}
	return cycles
}

func openDatabase(dbPath string) *sql.DB {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		panic("Failed to open database " + dbPath + ": " + err.Error())
	}

	pairsDB = db
	return db
}

// Loads with exact allocation based on COUNT query
func loadPoolsFromDatabase(db *sql.DB) []Pool {
	var poolCount int
	err := db.QueryRow("SELECT COUNT(*) FROM pools").Scan(&poolCount)
	if err != nil {
		panic("Failed to count pools: " + err.Error())
	}

	if poolCount == 0 {
		panic("No pools found in database")
	}

	pools := make([]Pool, 0, poolCount)

	rows, err := db.Query(`
		SELECT p.id, p.pool_address 
		FROM pools p
		ORDER BY p.id`)
	if err != nil {
		panic("Failed to query pools: " + err.Error())
	}
	defer rows.Close()

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

// Establishes optimized WebSocket connection and processes events until failure
func processEventStream() error {
	raw, _ := net.Dial("tcp", constants.WsDialAddr)
	tcpConn := raw.(*net.TCPConn)

	tcpConn.SetNoDelay(true)
	tcpConn.SetReadBuffer(constants.MaxFrameSize)
	tcpConn.SetWriteBuffer(constants.MaxFrameSize)

	// Low-level socket optimizations
	rawFile, _ := tcpConn.File()
	fd := int(rawFile.Fd())

	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)

	// Platform-specific optimizations
	switch runtime.GOOS {
	case "linux":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)         // SO_REUSEPORT
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // TCP_CONGESTION=bbr
	case "darwin":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1) // SO_REUSEPORT
	}
	rawFile.Close()

	conn := tls.Client(raw, &tls.Config{ServerName: constants.WsHost})
	ws.Handshake(conn)
	ws.SendSubscription(conn)

	for {
		payload, err := ws.SpinUntilCompleteMessage(conn)
		if err != nil {
			conn.Close()
			return err
		}

		parser.HandleFrame(payload)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SIGNAL HANDLING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Proper shutdown coordination using WaitGroup
func setupSignalHandling() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		debug.DropMessage("SIGNAL", "Received interrupt, shutting down...")

		// Wait for all components to finish cleanly
		shutdownWG.Wait()

		if pairsDB != nil {
			pairsDB.Close()
		}

		os.Exit(0)
	}()
}
