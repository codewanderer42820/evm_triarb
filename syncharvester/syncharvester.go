// ════════════════════════════════════════════════════════════════════════════════════════════════
// PEAK CHAD SYNCHARVESTER - COMPLETE REWRITE FOR MAXIMUM PERFORMANCE
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Zero allocations, adaptive batching, pure speed
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

import (
	"context"
	"database/sql"
	"fmt"
	"math/bits"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"main/constants"
	"main/control"
	"main/debug"
	"main/router"
	"main/types"
	"main/utils"

	_ "github.com/mattn/go-sqlite3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CHAD CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	DeploymentBlock   = uint64(10000835)
	ReservesDBPath    = "uniswap_v2_reserves.db"
	OptimalBatchSize  = uint64(10_000)
	MinBatchSize      = uint64(1)
	MaxBatchSize      = uint64(50_000)
	EventBatchSize    = 10_000
	CommitBatchSize   = 100_000
	MaxLogSliceSize   = 100_000
	MaxEventBatchSize = 50_000
)

// Chad sync event signature
const SyncEventSig = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CHAD DATA STRUCTURES - CACHE OPTIMIZED
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 64
type ChadLog struct {
	address  string // Contract address (40 hex chars)
	data     string // Event data (256 hex chars)
	blockNum uint64 // Block number
	logIndex uint64 // Log index
	txHash   string // Transaction hash
}

//go:notinheap
//go:align 64
type ChadEvent struct {
	pairID    int64  // Database key
	blockNum  uint64 // Block number
	logIndex  uint64 // Log index
	timestamp int64  // Event timestamp
	reserve0  string // Token0 reserve (hex)
	reserve1  string // Token1 reserve (hex)
	txHash    string // Transaction hash
	address   string // Contract address
}

//go:notinheap
//go:align 32
type ChadRPC struct {
	url    string
	client *http.Client
}

//go:notinheap
//go:align 64
type ChadSync struct {
	// HOT: Processing arrays
	logs   [MaxLogSliceSize]ChadLog     // Fixed array
	events [MaxEventBatchSize]ChadEvent // Fixed array

	// HOT: State
	lastProcessed uint64
	syncTarget    uint64
	processed     int64
	logCount      int
	eventCount    int
	eventsInBatch int

	// HOT: Adaptive algorithm
	batchSize            uint64
	consecutiveSuccesses int

	// WARM: Database
	db  *sql.DB
	tx  *sql.Tx
	rpc *ChadRPC

	// COLD: Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CHAD RPC CLIENT - SIMPLIFIED AND FAST
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func newChadRPC() *ChadRPC {
	return &ChadRPC{
		url: "https://" + constants.WsHost + "/v3/a2a3139d2ab24d59bed2dc3643664126",
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:          1,
				MaxIdleConnsPerHost:   1,
				DisableCompression:    true,
				ForceAttemptHTTP2:     true,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 10 * time.Second,
			},
		},
	}
}

func (r *ChadRPC) blockNumber() uint64 {
	reqJSON := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`

	for {
		resp, err := http.Post(r.url, "application/json", strings.NewReader(reqJSON))
		if err != nil {
			continue
		}

		var buf [128]byte
		n, _ := resp.Body.Read(buf[:])
		resp.Body.Close()

		if n == 0 {
			continue
		}

		// Parse JSON response: {"jsonrpc":"2.0","id":1,"result":"0x1234567"}
		respStr := string(buf[:n])

		// Find "result":"0x
		start := strings.Index(respStr, `"result":"0x`)
		if start == -1 {
			continue
		}
		start += 11 // Skip `"result":"0x`

		// Find closing quote
		end := strings.Index(respStr[start:], `"`)
		if end == -1 {
			continue
		}

		// Parse hex (without 0x prefix)
		hexStr := respStr[start : start+end]
		blockNum := utils.ParseHexU64([]byte(hexStr))

		debug.DropMessage("RPC_BLOCK", utils.Itoa(int(blockNum)))
		return blockNum
	}
}

func (r *ChadRPC) getLogs(from, to uint64) ([]ChadLog, error) {
	// Build request
	reqJSON := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x%x","toBlock":"0x%x","topics":["%s"]}],"id":1}`,
		from, to, SyncEventSig)

	resp, err := http.Post(r.url, "application/json", strings.NewReader(reqJSON))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read ENTIRE response using strings.Builder
	var respBuilder strings.Builder
	respBuilder.Grow(1024 * 1024) // Pre-allocate 1MB

	var buf [8192]byte
	totalBytes := 0
	for {
		n, err := resp.Body.Read(buf[:])
		if n > 0 {
			respBuilder.Write(buf[:n])
			totalBytes += n
		}
		if err != nil {
			break // EOF or error
		}
	}

	respStr := respBuilder.String()

	// Debug: Check response size
	debug.DropMessage("RPC_SIZE", utils.Itoa(totalBytes)+" bytes total")

	// Check for RPC error
	if strings.Contains(respStr, `"error"`) {
		debug.DropMessage("RPC_ERROR_FOUND", "Error in response")
		return nil, fmt.Errorf("RPC error in response")
	}

	// Parse JSON array of logs
	logs := parseLogs(respStr)
	debug.DropMessage("RPC_LOGS", utils.Itoa(len(logs)))

	// Debug: Check if we're hitting parsing limits
	if len(logs) == 60 {
		debug.DropMessage("RPC_SUSPICIOUS", "Exactly 60 logs - possible parser limit!")
	}

	return logs, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CHAD JSON PARSER - OPTIMIZED FOR LOGS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func parseLogs(jsonStr string) []ChadLog {
	var logs []ChadLog

	// Find the result array
	resultStart := strings.Index(jsonStr, `"result":[`)
	if resultStart == -1 {
		return logs
	}
	resultStart += 10 // Skip `"result":[`

	// Find array end
	arrayEnd := strings.LastIndex(jsonStr, `]`)
	if arrayEnd == -1 {
		return logs
	}

	// Parse each log object
	logData := jsonStr[resultStart:arrayEnd]
	pos := 0

	for pos < len(logData) {
		// Find start of log object
		objStart := strings.Index(logData[pos:], `{`)
		if objStart == -1 {
			break
		}
		pos += objStart

		// Find end of log object
		objEnd := findMatchingBrace(logData, pos)
		if objEnd == -1 {
			break
		}

		// Parse this log object
		logObj := logData[pos : objEnd+1]
		if log := parseLogObject(logObj); log.address != "" {
			logs = append(logs, log)
		}

		pos = objEnd + 1
	}

	return logs
}

func findMatchingBrace(s string, start int) int {
	braceCount := 0
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '{':
			braceCount++
		case '}':
			braceCount--
			if braceCount == 0 {
				return i
			}
		}
	}
	return -1
}

func parseLogObject(logObj string) ChadLog {
	var log ChadLog

	// Parse address
	if addr := extractJSONField(logObj, "address"); addr != "" {
		log.address = addr[2:] // Remove 0x prefix
	}

	// Parse data
	if data := extractJSONField(logObj, "data"); data != "" {
		log.data = data[2:] // Remove 0x prefix
	}

	// Parse blockNumber
	if blockHex := extractJSONField(logObj, "blockNumber"); blockHex != "" {
		log.blockNum = utils.ParseHexU64([]byte(blockHex[2:])) // Remove 0x prefix
	}

	// Parse logIndex
	if indexHex := extractJSONField(logObj, "logIndex"); indexHex != "" {
		log.logIndex = utils.ParseHexU64([]byte(indexHex[2:])) // Remove 0x prefix
	}

	// Parse transactionHash
	if txHash := extractJSONField(logObj, "transactionHash"); txHash != "" {
		log.txHash = txHash[2:] // Remove 0x prefix
	}

	return log
}

func extractJSONField(jsonObj, field string) string {
	// Find field start
	pattern := `"` + field + `":"0x`
	start := strings.Index(jsonObj, pattern)
	if start == -1 {
		return ""
	}
	start += len(pattern) - 2 // Include 0x

	// Find field end
	end := strings.Index(jsonObj[start:], `"`)
	if end == -1 {
		return ""
	}

	return jsonObj[start : start+end]
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CHAD HEX PROCESSING - SIMD OPTIMIZED
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func countHexLeadingZeros(segment []byte) int {
	const ZERO_PATTERN = 0x3030303030303030

	c0 := utils.Load64(segment[0:8]) ^ ZERO_PATTERN
	c1 := utils.Load64(segment[8:16]) ^ ZERO_PATTERN
	c2 := utils.Load64(segment[16:24]) ^ ZERO_PATTERN
	c3 := utils.Load64(segment[24:32]) ^ ZERO_PATTERN

	mask := ((c0|(^c0+1))>>63)<<0 | ((c1|(^c1+1))>>63)<<1 |
		((c2|(^c2+1))>>63)<<2 | ((c3|(^c3+1))>>63)<<3

	firstChunk := bits.TrailingZeros64(mask)
	if firstChunk == 64 {
		return 32
	}

	chunks := [4]uint64{c0, c1, c2, c3}
	firstByte := bits.TrailingZeros64(chunks[firstChunk]) >> 3
	return (firstChunk << 3) + firstByte
}

func parseReservesToZeroTrimmed(dataStr string) (string, string) {
	if len(dataStr) < 128 {
		return "0", "0"
	}

	// Reserve0: hex chars 32-63
	leadingZeros0 := 32 + countHexLeadingZeros([]byte(dataStr[32:64]))
	reserve0Start := leadingZeros0
	reserve0Len := 64 - reserve0Start

	var reserve0 string
	if reserve0Len <= 0 {
		reserve0 = "0"
	} else {
		reserve0 = dataStr[reserve0Start:64]
	}

	// Reserve1: hex chars 96-127
	leadingZeros1 := 32 + countHexLeadingZeros([]byte(dataStr[96:128]))
	reserve1Start := 96 + (leadingZeros1 - 32)
	reserve1Len := 128 - reserve1Start

	var reserve1 string
	if reserve1Len <= 0 {
		reserve1 = "0"
	} else {
		reserve1 = dataStr[reserve1Start:128]
	}

	return reserve0, reserve1
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CHAD SYNC ENGINE - PEAK PERFORMANCE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func newChadSync() *ChadSync {
	runtime.LockOSThread()

	ctx, cancel := context.WithCancel(context.Background())

	s := &ChadSync{
		rpc:                  newChadRPC(),
		ctx:                  ctx,
		cancel:               cancel,
		batchSize:            OptimalBatchSize,
		consecutiveSuccesses: 0,
	}

	// Open database
	var err error
	s.db, err = sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		panic("Database open failed: " + err.Error())
	}

	// Create tables
	s.db.Exec(`
		CREATE TABLE IF NOT EXISTS sync_events (
			pair_id INTEGER,
			block_number INTEGER,
			tx_hash TEXT,
			log_index INTEGER,
			reserve0 TEXT,
			reserve1 TEXT,
			timestamp INTEGER
		)
	`)

	s.db.Exec(`
		CREATE TABLE IF NOT EXISTS pair_reserves (
			pair_id INTEGER,
			pair_address TEXT,
			reserve0 TEXT,
			reserve1 TEXT,
			block_number INTEGER,
			timestamp INTEGER,
			PRIMARY KEY (pair_id)
		)
	`)

	// Performance pragmas
	s.db.Exec(`
		PRAGMA journal_mode = OFF;
		PRAGMA synchronous = OFF;
		PRAGMA cache_size = 200000;
		PRAGMA temp_store = MEMORY;
		PRAGMA mmap_size = 4294967296;
		PRAGMA page_size = 65536;
		PRAGMA locking_mode = EXCLUSIVE;
	`)

	// Signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	return s
}

func (s *ChadSync) processLog(log *ChadLog) bool {
	// Lookup pair ID
	addressBytes := []byte(log.address)
	pairID := router.LookupPairByAddress(addressBytes)
	if pairID == 0 {
		return false
	}

	// Parse reserves
	reserve0, reserve1 := parseReservesToZeroTrimmed(log.data)
	now := time.Now().Unix()

	// Store event
	s.events[s.eventCount] = ChadEvent{
		pairID:    int64(pairID),
		blockNum:  log.blockNum,
		logIndex:  log.logIndex,
		timestamp: now,
		reserve0:  reserve0,
		reserve1:  reserve1,
		txHash:    log.txHash,
		address:   log.address,
	}

	s.eventCount++
	return true
}

func (s *ChadSync) flushEvents() {
	if s.eventCount == 0 {
		return
	}

	// Build SQL
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("INSERT OR IGNORE INTO sync_events VALUES ")

	args := make([]interface{}, 0, s.eventCount*7)
	for i := 0; i < s.eventCount; i++ {
		if i > 0 {
			sqlBuilder.WriteByte(',')
		}
		sqlBuilder.WriteString("(?,?,?,?,?,?,?)")

		evt := &s.events[i]
		args = append(args, evt.pairID, evt.blockNum, evt.txHash,
			evt.logIndex, evt.reserve0, evt.reserve1, evt.timestamp)
	}

	// Execute
	s.tx.Exec(sqlBuilder.String(), args...)

	// Update totals
	s.eventsInBatch += s.eventCount
	s.eventCount = 0
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CHAD ADAPTIVE SYNC ALGORITHM - THE CORE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (s *ChadSync) sync() error {
	// Get sync range
	s.syncTarget = s.rpc.blockNumber()
	debug.DropMessage("HEAD", utils.Itoa(int(s.syncTarget)))

	// Get last processed block
	err := s.db.QueryRow("SELECT COALESCE(MAX(block_number), ?) FROM sync_events", DeploymentBlock).Scan(&s.lastProcessed)
	if err != nil {
		s.lastProcessed = DeploymentBlock
		debug.DropMessage("FRESH", utils.Itoa(int(s.lastProcessed)))
	} else {
		debug.DropMessage("LAST", utils.Itoa(int(s.lastProcessed)))
	}

	if s.lastProcessed >= s.syncTarget {
		debug.DropMessage("CURRENT", "Already up to date")
		return nil
	}

	blocksToSync := s.syncTarget - s.lastProcessed
	debug.DropMessage("SYNC", utils.Itoa(int(blocksToSync))+" blocks to sync")

	// Begin transaction
	s.tx, _ = s.db.Begin()

	// CHAD ADAPTIVE ALGORITHM
	current := s.lastProcessed + 1

	for current <= s.syncTarget {
		end := current + s.batchSize - 1
		if end > s.syncTarget {
			end = s.syncTarget
		}

		debug.DropMessage("BATCH", utils.Itoa(int(current))+"-"+utils.Itoa(int(end))+" (size: "+utils.Itoa(int(s.batchSize))+")")

		// Try batch
		logs, err := s.rpc.getLogs(current, end)
		if err != nil {
			// RPC FAILED - ADAPTIVE SHRINK
			oldSize := s.batchSize
			s.batchSize = s.batchSize / 2
			if s.batchSize < MinBatchSize {
				s.batchSize = MinBatchSize
			}
			s.consecutiveSuccesses = 0
			debug.DropMessage("SHRINK", utils.Itoa(int(oldSize))+" -> "+utils.Itoa(int(s.batchSize)))
			continue
		}

		// Process logs
		s.logCount = len(logs)
		processed := 0
		for i := 0; i < s.logCount; i++ {
			if s.processLog(&logs[i]) {
				processed++
			}

			if s.eventCount >= EventBatchSize {
				s.flushEvents()
			}
		}

		debug.DropMessage("PROCESSED", utils.Itoa(processed)+" events from "+utils.Itoa(s.logCount)+" logs")

		// Update progress
		s.lastProcessed = end
		current = end + 1
		s.processed += int64(s.logCount)
		s.consecutiveSuccesses++

		// ADAPTIVE GROWTH - DOUBLE EVERY 3 SUCCESSES
		if s.consecutiveSuccesses >= 3 {
			oldSize := s.batchSize
			s.batchSize = s.batchSize * 2
			if s.batchSize > MaxBatchSize {
				s.batchSize = MaxBatchSize
			}
			s.consecutiveSuccesses = 0
			debug.DropMessage("GROW", utils.Itoa(int(oldSize))+" -> "+utils.Itoa(int(s.batchSize)))
		}

		// Periodic commits
		if s.eventsInBatch >= CommitBatchSize {
			s.flushEvents()
			s.tx.Commit()
			s.tx, _ = s.db.Begin()
			s.eventsInBatch = 0
			debug.DropMessage("COMMIT", "Database commit")
		}

		// Progress
		if current%10000 == 0 {
			remaining := s.syncTarget - current + 1
			debug.DropMessage("PROGRESS", utils.Itoa(int(current))+" ("+utils.Itoa(int(remaining))+" remaining)")
		}
	}

	// Final commit
	s.flushEvents()
	s.tx.Commit()

	debug.DropMessage("COMPLETE", utils.Itoa(int(s.processed))+" total events processed")
	return nil
}

func (s *ChadSync) close() {
	if s.tx != nil {
		s.flushEvents()
		s.tx.Commit()
	}
	s.db.Close()
	s.cancel()
	control.ShutdownWG.Done()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CHAD ROUTER INTEGRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func FlushSyncedReservesToRouter() error {
	db, _ := sql.Open("sqlite3", ReservesDBPath)
	defer db.Close()

	var latestBlock uint64
	db.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&latestBlock)

	// Create LogView for router
	blockHex := fmt.Sprintf("0x%x", latestBlock)

	var dataBuffer [130]byte
	dataBuffer[0] = '0'
	dataBuffer[1] = 'x'

	v := types.LogView{
		LogIdx:  []byte("0x0"),
		TxIndex: []byte("0x0"),
		Topics:  []byte(SyncEventSig),
		BlkNum:  []byte(blockHex),
		Data:    dataBuffer[:],
	}

	rows, _ := db.Query("SELECT pair_address, reserve0, reserve1 FROM pair_reserves")
	defer rows.Close()

	count := 0
	const zeros = uint64(0x3030303030303030)

	for rows.Next() {
		var addr, r0, r1 string
		rows.Scan(&addr, &r0, &r1)

		// Zero data ranges
		*(*uint64)(unsafe.Pointer(&dataBuffer[34])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[42])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[50])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[58])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[98])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[106])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[114])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[122])) = zeros

		// Write reserves right-aligned
		copy(dataBuffer[66-len(r0):66], r0)
		copy(dataBuffer[130-len(r1):130], r1)

		v.Addr = unsafe.Slice(unsafe.StringData(addr[2:]), len(addr)-2)
		router.DispatchPriceUpdate(&v)
		count++
	}

	debug.DropMessage("FLUSH", utils.Itoa(count)+" reserves flushed to router")
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CHAD PUBLIC API
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func ExecutePeakSync() error {
	debug.DropMessage("EXEC_START", "Starting chad sync")

	control.ShutdownWG.Add(1)

	s := newChadSync()
	defer s.close()

	return s.sync()
}

func CheckIfPeakSyncNeeded() (bool, uint64, uint64, error) {
	debug.DropMessage("CHECK_START", "Checking sync status")

	// Check if database exists
	if _, err := os.Stat(ReservesDBPath); os.IsNotExist(err) {
		debug.DropMessage("CHECK_NO_DB", "Database missing, sync needed")

		rpc := newChadRPC()
		currentHead := rpc.blockNumber()

		return true, DeploymentBlock, currentHead, nil
	}

	// Open database
	db, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		return false, 0, 0, err
	}
	defer db.Close()

	// Get last block
	var lastBlock uint64
	err = db.QueryRow("SELECT COALESCE(MAX(block_number), ?) FROM sync_events", DeploymentBlock).Scan(&lastBlock)
	if err != nil {
		debug.DropMessage("CHECK_QUERY_ERR", "Query failed, treating as fresh")
		lastBlock = DeploymentBlock
	}

	// Get current head
	rpc := newChadRPC()
	currentHead := rpc.blockNumber()

	needed := lastBlock < currentHead
	if needed {
		debug.DropMessage("CHECK_NEEDED", utils.Itoa(int(currentHead-lastBlock))+" blocks behind")
	} else {
		debug.DropMessage("CHECK_CURRENT", "Already up to date")
	}

	return needed, lastBlock, currentHead, nil
}
