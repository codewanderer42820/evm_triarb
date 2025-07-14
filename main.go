// main.go - Arbitrage detection engine with WebSocket event processing
package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"syscall"

	"main/constants"
	"main/debug"
	"main/parser"
	"main/router"
	"main/utils"
	"main/ws"

	_ "github.com/mattn/go-sqlite3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// init performs automatic system initialization with database integration.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func init() {
	debug.DropMessage("SYSTEM_INIT", "Starting arbitrage detection engine")

	// Load arbitrage cycles first (always required)
	cycles, err := loadArbitrageCyclesFromFile("cycles_3_3.txt")
	if err != nil {
		panic(fmt.Sprintf("Critical failure loading arbitrage cycles: %v", err))
	}
	printCyclesInfo(cycles)

	// Try to load trading pairs from database
	if err := loadPoolsFromDatabase("uniswap_pairs.db"); err != nil {
		debug.DropMessage("DATABASE_FALLBACK", fmt.Sprintf("Database unavailable (%v), using cycles-only mode", err))
	}

	// Initialize arbitrage system with loaded data
	router.InitializeArbitrageSystem(cycles)
	debug.DropMessage("INIT_COMPLETE", "System ready")
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATA LOADING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// loadArbitrageCyclesFromFile loads triangular arbitrage cycles from configuration files.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadArbitrageCyclesFromFile(filename string) ([]router.ArbitrageTriplet, error) {
	debug.DropMessage("FILE_LOADING", "Reading cycles from: "+filename)

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	fileSizeStr := utils.Itoa(len(data))
	debug.DropMessage("FILE_SIZE", "File size: "+fileSizeStr+" bytes")

	// Pre-allocate with estimate
	estimatedCycles := len(data) / 50
	if estimatedCycles < 100 {
		estimatedCycles = 100
	}
	cycles := make([]router.ArbitrageTriplet, 0, estimatedCycles)

	// Parse file
	i, lineCount := 0, 0
	dataLen := len(data)

	for i < dataLen {
		var pairIDs [3]uint64
		pairCount := 0
		lineCount++

		// Extract 3 pairs from current line
		for pairCount < 3 && i < dataLen && data[i] != '\n' {
			// Find opening parenthesis
			for i < dataLen && data[i] != '(' && data[i] != '\n' {
				i++
			}
			if i >= dataLen || data[i] == '\n' {
				break
			}
			i++ // Skip '('

			// Extract digits
			pairID := uint64(0)
			digitFound := false
			for i < dataLen && data[i] >= '0' && data[i] <= '9' {
				pairID = pairID*10 + uint64(data[i]-'0')
				digitFound = true
				i++
			}

			// Find closing parenthesis
			for i < dataLen && data[i] != ')' && data[i] != '\n' {
				i++
			}

			if digitFound && pairID > 0 {
				pairIDs[pairCount] = pairID
				pairCount++
			}

			// Skip closing parenthesis
			if i < dataLen && data[i] == ')' {
				i++
			}
		}

		// Skip to next line
		for i < dataLen && data[i] != '\n' {
			i++
		}
		if i < dataLen {
			i++
		}

		// Create triplet if valid
		if pairCount == 3 {
			cycles = append(cycles, router.ArbitrageTriplet{
				router.PairID(pairIDs[0]),
				router.PairID(pairIDs[1]),
				router.PairID(pairIDs[2]),
			})
		} else if pairCount > 0 {
			lineStr := utils.Itoa(lineCount)
			pairCountStr := utils.Itoa(pairCount)
			debug.DropMessage("INCOMPLETE_LINE", "Line "+lineStr+" only had "+pairCountStr+" pairs")
		}
	}

	linesStr := utils.Itoa(lineCount)
	debug.DropMessage("LINES_PROCESSED", "Total lines: "+linesStr)

	if len(cycles) == 0 {
		return nil, fmt.Errorf("no valid arbitrage cycles found in %s", filename)
	}

	return cycles, nil
}

// loadPoolsFromDatabase reads trading pairs from SQLite database.
//
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

		// Print first few pools for verification
		if count < 5 {
			printPoolInfo(pairID, poolAddress, token0Address, token1Address, exchangeName, feeBps)
		}

		// Register pool address
		poolAddressBytes := []byte(strings.TrimPrefix(poolAddress, "0x"))
		router.RegisterPairAddress(poolAddressBytes, router.PairID(pairID))
		count++

		// Progress indicator
		if count%100 == 0 {
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

// printCyclesInfo prints detailed information about loaded arbitrage cycles
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func printCyclesInfo(cycles []router.ArbitrageTriplet) {
	cycleCountStr := utils.Itoa(len(cycles))
	debug.DropMessage("CYCLES_LOADED", "Total cycles: "+cycleCountStr)

	// Print first 10 cycles for verification
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

// printPoolInfo prints detailed information about a loaded pool
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func printPoolInfo(pairID int64, poolAddress, token0Address, token1Address, exchangeName string, feeBps sql.NullInt64) {
	pairIDStr := utils.Itoa(int(pairID))

	poolInfo := "Pool " + pairIDStr + ": " + poolAddress
	debug.DropMessage("POOL_ADDRESS", poolInfo)

	tokenInfo := "  Tokens: " + token0Address + " <-> " + token1Address
	debug.DropMessage("POOL_TOKENS", tokenInfo)

	exchangeInfo := "  Exchange: " + exchangeName
	if feeBps.Valid {
		feeStr := utils.Itoa(int(feeBps.Int64))
		exchangeInfo += " (Fee: " + feeStr + " bps)"
	}
	debug.DropMessage("POOL_EXCHANGE", exchangeInfo)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN EXECUTION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// main orchestrates the arbitrage detection system with fault tolerance.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func main() {
	debug.DropMessage("STARTUP", "Arbitrage detection engine starting")
	runtime.LockOSThread()

	// Main processing loop with auto-recovery
	for {
		if err := runStream(); err != nil {
			debug.DropError("STREAM_ERROR", err)
		}
	}
}

// runStream establishes WebSocket connection and processes events.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func runStream() error {
	// Establish connection
	conn, err := establishConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	// WebSocket setup
	if err := ws.Handshake(conn); err != nil {
		return err
	}

	if err := ws.SendSubscription(conn); err != nil {
		return err
	}

	debug.DropMessage("READY", "Event processing active")

	// Process messages
	for {
		payload, err := ws.SpinUntilCompleteMessage(conn)
		if err != nil {
			return err
		}
		parser.HandleFrame(payload)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// NETWORK OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// establishConnection creates optimized TCP/TLS connection.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func establishConnection() (*tls.Conn, error) {
	// TCP connection
	raw, err := net.Dial("tcp", constants.WsDialAddr)
	if err != nil {
		return nil, err
	}

	// TCP optimizations
	tcpConn := raw.(*net.TCPConn)
	tcpConn.SetNoDelay(true)
	tcpConn.SetReadBuffer(constants.MaxFrameSize)
	tcpConn.SetWriteBuffer(constants.MaxFrameSize)

	// Socket optimizations
	if rawFile, err := tcpConn.File(); err == nil {
		optimizeSocket(int(rawFile.Fd()))
		rawFile.Close()
	}

	// TLS upgrade
	tlsConn := tls.Client(raw, &tls.Config{
		ServerName: constants.WsHost,
	})

	return tlsConn, nil
}

// optimizeSocket applies platform-specific socket optimizations.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func optimizeSocket(fd int) {
	// Universal optimizations
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, constants.MaxFrameSize)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, constants.MaxFrameSize)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)

	// Platform-specific optimizations
	switch runtime.GOOS {
	case "linux":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 46, 1)         // SO_BUSY_POLL
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 18, 1000)     // TCP_USER_TIMEOUT
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 16, 1)        // TCP_THIN_LINEAR_TIMEOUTS
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 17, 1)        // TCP_THIN_DUPACK
		syscall.SetsockoptString(fd, syscall.IPPROTO_TCP, 13, "bbr") // TCP_CONGESTION

	case "darwin":
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x10, 1)  // TCP_KEEPIDLE
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x101, 1) // TCP_KEEPINTVL
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x102, 3) // TCP_KEEPCNT
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x1006, 1) // SO_RECV_ANYIF

	case "windows":
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x0004, 1) // SO_EXCLUSIVEADDRUSE
	}
}
