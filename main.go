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

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func init() {
	debug.DropMessage("SYSTEM_INIT", "Starting arbitrage detection engine")

	// Load arbitrage cycles
	cycles, err := loadArbitrageCyclesFromFile("cycles_3_3.txt")
	if err != nil {
		panic(fmt.Sprintf("Critical failure loading arbitrage cycles: %v", err))
	}
	printCyclesInfo(cycles)

	// Load trading pairs if available
	if err := loadPoolsFromDatabase("uniswap_pairs.db"); err != nil {
		debug.DropMessage("DATABASE_FALLBACK", fmt.Sprintf("Database unavailable (%v), using cycles-only mode", err))
	}

	// Initialize arbitrage system
	router.InitializeArbitrageSystem(cycles)

	// Clean up init data immediately
	cycles = nil

	debug.DropMessage("INIT_COMPLETE", "System ready for bootstrap")
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN EXECUTION - CLEAN PHASES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func main() {
	debug.DropMessage("MAIN_START", "Starting system orchestration")

	// PHASE 1: BOOTSTRAP
	bootstrapStart := time.Now()
	executeBootstrapPhase()
	bootstrapDuration := time.Since(bootstrapStart)
	debug.DropMessage("BOOTSTRAP_DURATION", fmt.Sprintf("Bootstrap phase completed in %v", bootstrapDuration))

	// PHASE 2: CLEANUP & PREPARATION
	cleanupStart := time.Now()
	performCleanupAndPrepareProduction()
	cleanupDuration := time.Since(cleanupStart)
	debug.DropMessage("CLEANUP_DURATION", fmt.Sprintf("Resource cleanup completed in %v", cleanupDuration))

	// PHASE 3: FINAL GC & DISABLE
	prepareProductionMemory()

	// PHASE 4: PRODUCTION
	totalBootstrapTime := time.Since(bootstrapStart)
	debug.DropMessage("TOTAL_BOOTSTRAP_TIME", fmt.Sprintf("Total bootstrap overhead: %v", totalBootstrapTime))
	debug.DropMessage("PRODUCTION_START", "Starting production processing")

	runtime.LockOSThread()
	runProductionEventLoop()
}

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func executeBootstrapPhase() {
	if requiresBootstrapSync() {
		debug.DropMessage("BOOTSTRAP_EXECUTE", "Running bootstrap synchronization")

		if err := syncharvest.ExecutePeakSync(); err != nil {
			debug.DropMessage("BOOTSTRAP_ERROR", fmt.Sprintf("Bootstrap failed: %v", err))
			debug.DropMessage("BOOTSTRAP_CONTINUE", "Continuing with available data")
		} else {
			debug.DropMessage("BOOTSTRAP_SUCCESS", "Bootstrap synchronization completed")
		}
	} else {
		debug.DropMessage("BOOTSTRAP_SKIP", "Bootstrap synchronization not required")
	}
}

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func performCleanupAndPrepareProduction() {
	debug.DropMessage("CLEANUP_START", "Starting post-bootstrap resource cleanup")

	// Force return of unused memory to OS
	rtdebug.FreeOSMemory()

	debug.DropMessage("CLEANUP_COMPLETE", "Post-bootstrap cleanup completed")
}

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func prepareProductionMemory() {
	debug.DropMessage("FINAL_GC", "Final garbage collection before production")

	// Final GC to clean everything before disabling
	runtime.GC()
	rtdebug.FreeOSMemory()

	// NOW disable GC for production
	rtdebug.SetGCPercent(-1)
	debug.DropMessage("GC_DISABLED", "Garbage collector disabled for production operation")
}

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func requiresBootstrapSync() bool {
	syncNeeded, lastBlock, targetBlock, err := syncharvest.CheckIfPeakSyncNeeded()
	if err != nil {
		debug.DropMessage("BOOTSTRAP_CHECK_ERROR", fmt.Sprintf("Synchronization check failed: %v", err))
		return false
	}

	if syncNeeded {
		blocksBehind := targetBlock - lastBlock
		debug.DropMessage("BOOTSTRAP_NEEDED", fmt.Sprintf("Synchronization required: %d blocks behind", blocksBehind))
	} else {
		debug.DropMessage("BOOTSTRAP_CURRENT", fmt.Sprintf("System current at block %d", lastBlock))
	}

	return syncNeeded
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PRODUCTION PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func runProductionEventLoop() {
	debug.DropMessage("EVENT_LOOP_START", "Starting production event processing loop")

	for {
		if err := processEventStream(); err != nil {
			debug.DropError("STREAM_ERROR", err)
		}
	}
}

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func processEventStream() error {
	conn, err := establishConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := ws.Handshake(conn); err != nil {
		return err
	}

	if err := ws.SendSubscription(conn); err != nil {
		return err
	}

	debug.DropMessage("STREAM_READY", "Event stream processing active")

	for {
		payload, err := ws.SpinUntilCompleteMessage(conn)
		if err != nil {
			return err
		}
		parser.HandleFrame(payload)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATA LOADING - ARBITRAGE CYCLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadArbitrageCyclesFromFile(filename string) ([]router.ArbitrageTriangle, error) {
	debug.DropMessage("FILE_LOADING", "Reading cycles from: "+filename)

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	defer func() { data = nil }() // Clean up immediately

	fileSizeStr := utils.Itoa(len(data))
	debug.DropMessage("FILE_SIZE", "File size: "+fileSizeStr+" bytes")

	estimatedCycles := len(data) / 50
	if estimatedCycles < 100 {
		estimatedCycles = 100
	}
	cycles := make([]router.ArbitrageTriangle, 0, estimatedCycles)

	i, lineCount := 0, 0
	dataLen := len(data)

	for i < dataLen {
		var pairIDs [3]uint64
		pairCount := 0
		lineCount++

		for pairCount < 3 && i < dataLen && data[i] != '\n' {
			for i < dataLen && data[i] != '(' && data[i] != '\n' {
				i++
			}
			if i >= dataLen || data[i] == '\n' {
				break
			}
			i++

			pairID := uint64(0)
			digitFound := false
			for i < dataLen && data[i] >= '0' && data[i] <= '9' {
				pairID = pairID*10 + uint64(data[i]-'0')
				digitFound = true
				i++
			}

			for i < dataLen && data[i] != ')' && data[i] != '\n' {
				i++
			}

			if digitFound && pairID > 0 {
				pairIDs[pairCount] = pairID
				pairCount++
			}

			if i < dataLen && data[i] == ')' {
				i++
			}
		}

		for i < dataLen && data[i] != '\n' {
			i++
		}
		if i < dataLen {
			i++
		}

		if pairCount == 3 {
			cycles = append(cycles, router.ArbitrageTriangle{
				router.TradingPairID(pairIDs[0]),
				router.TradingPairID(pairIDs[1]),
				router.TradingPairID(pairIDs[2]),
			})
		}
	}

	linesStr := utils.Itoa(lineCount)
	debug.DropMessage("LINES_PROCESSED", "Total lines: "+linesStr)

	if len(cycles) == 0 {
		return nil, fmt.Errorf("no valid arbitrage cycles found in %s", filename)
	}

	return cycles, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATA LOADING - TRADING POOLS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func loadPoolsFromDatabase(dbPath string) error {
	debug.DropMessage("DATABASE_CONNECT", "Connecting to: "+dbPath)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("database connection failed: %v", err)
	}
	defer db.Close()

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
	poolAddressBytes := make([]byte, 0, 40)

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

		poolAddressBytes = poolAddressBytes[:0]
		trimmed := strings.TrimPrefix(poolAddress, "0x")
		poolAddressBytes = append(poolAddressBytes, trimmed...)

		router.RegisterTradingPairAddress(poolAddressBytes, router.TradingPairID(pairID))
		count++

		if count%100000 == 0 {
			progressStr := utils.Itoa(count)
			debug.DropMessage("POOL_PROGRESS", progressStr+" pools loaded")
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("pool iteration error: %v", err)
	}

	countStr := utils.Itoa(count)
	debug.DropMessage("POOLS_LOADED", countStr+" trading pairs registered")

	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// VERIFICATION UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func printCyclesInfo(cycles []router.ArbitrageTriangle) {
	cycleCountStr := utils.Itoa(len(cycles))
	debug.DropMessage("CYCLES_LOADED", "Total cycles: "+cycleCountStr)

	maxToPrint := len(cycles)
	if maxToPrint > 10 {
		maxToPrint = 10
	}

	for i := 0; i < maxToPrint; i++ {
		cycle := cycles[i]
		indexStr := utils.Itoa(i + 1)
		pair0Str := utils.Itoa(int(cycle[0]))
		pair1Str := utils.Itoa(int(cycle[1]))
		pair2Str := utils.Itoa(int(cycle[2]))

		cycleInfo := "Cycle " + indexStr + ": (" + pair0Str + ") -> (" + pair1Str + ") -> (" + pair2Str + ")"
		debug.DropMessage("CYCLE_DETAIL", cycleInfo)
	}

	if len(cycles) > 10 {
		remainingStr := utils.Itoa(len(cycles) - 10)
		debug.DropMessage("CYCLES_TRUNCATED", "... and "+remainingStr+" more cycles")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// NETWORK OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func establishConnection() (*tls.Conn, error) {
	raw, err := net.Dial("tcp", constants.WsDialAddr)
	if err != nil {
		return nil, err
	}

	tcpConn := raw.(*net.TCPConn)
	tcpConn.SetNoDelay(true)
	tcpConn.SetReadBuffer(constants.MaxFrameSize)
	tcpConn.SetWriteBuffer(constants.MaxFrameSize)

	if rawFile, err := tcpConn.File(); err == nil {
		optimizeSocket(int(rawFile.Fd()))
		rawFile.Close()
	}

	tlsConn := tls.Client(raw, &tls.Config{
		ServerName: constants.WsHost,
	})

	return tlsConn, nil
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func optimizeSocket(fd int) {
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)

	switch runtime.GOOS {
	case "linux":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 18, 1000)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 16, 1)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 17, 1)
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr")

	case "darwin":
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x10, 1)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x101, 1)
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x102, 3)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1)

	case "windows":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x0004, 1)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func RunBootstrapOnly() error {
	debug.DropMessage("BOOTSTRAP_STANDALONE", "Running standalone bootstrap synchronization")

	if requiresBootstrapSync() {
		if err := syncharvest.ExecutePeakSync(); err != nil {
			return fmt.Errorf("bootstrap synchronization failed: %w", err)
		}
	} else {
		debug.DropMessage("BOOTSTRAP_UNNECESSARY", "Bootstrap synchronization not required")
	}

	performCleanupAndPrepareProduction()
	debug.DropMessage("BOOTSTRAP_STANDALONE_COMPLETE", "Standalone bootstrap synchronization complete")
	return nil
}

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func CheckSystemStatus() (syncNeeded bool, lastBlock, targetBlock uint64, err error) {
	return syncharvest.CheckIfPeakSyncNeeded()
}
