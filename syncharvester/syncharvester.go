// ════════════════════════════════════════════════════════════════════════════════════════════════
// Streamlined Peak Performance Sync
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Minimal complexity with maximum trust in RPC data and optimal pre-allocation
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

import (
	"context"
	"database/sql"
	"encoding/json"
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
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Database configuration
	ReservesDBPath = "uniswap_v2_reserves.db"

	// RPC endpoint configuration
	RPCScheme      = "https"
	RPCHost        = constants.WsHost
	RPCProjectPath = "/v3/a2a3139d2ab24d59bed2dc3643664126"

	// Blockchain constants
	SyncEventSignature = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
	DeploymentBlock    = uint64(10000835)

	// Adaptive batch parameters
	OptimalBatchSize            = uint64(10_000)
	CommitBatchSize             = 100_000
	EventBatchSize              = 10_000
	ConsecutiveSuccessThreshold = 3
	MinBatchSize                = 1

	// Pre-allocation sizes
	MaxLogSliceSize   = 100_000 // Worst case block batch
	MaxEventBatchSize = 50_000  // Worst case events per commit
	MaxStringBuilder  = 16_384  // Worst case SQL statement
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PEAK PERFORMANCE DATA STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Log structure - fields ordered by access frequency in hot path
//
//go:notinheap
//go:align 64
type Log struct {
	// HOT: Most accessed fields first (cache line 1 - 64B)
	Data    string   // 16B - Event data (parsed every log)
	Address string   // 16B - Contract address (lookup key)
	Topics  []string // 24B - Event topics (signature validation)
	_       [8]byte  // 8B - Padding to cache boundary

	// WARM: Moderately accessed (cache line 2 - 64B)
	BlockNumber string   // 16B - Block number (parsed for DB)
	TxHash      string   // 16B - Transaction hash (stored in DB)
	LogIndex    string   // 16B - Log index (parsed for DB)
	_           [16]byte // 16B - Padding to cache boundary
}

// ProcessedEvent - cache-aligned for maximum memory bandwidth
//
//go:notinheap
//go:align 64
type ProcessedEvent struct {
	// Cache line 1 (64B) - HOT: Database insert fields accessed together
	pairID    int64  // 8B - Primary key for database operations
	blockNum  uint64 // 8B - Block number for ordering and indexing
	logIndex  uint64 // 8B - Log position within transaction
	timestamp int64  // 8B - Event timestamp for database storage
	reserve0  string // 16B - Zero-trimmed hex string for token0 reserve
	reserve1  string // 16B - Zero-trimmed hex string for token1 reserve

	// Cache line 2 (64B) - WARM: Secondary data for database operations
	txHash      string   // 16B - Transaction hash for database storage
	pairAddress string   // 16B - Contract address for reserve updates
	_           [32]byte // 32B - Padding to complete cache line
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RPC CLIENT STRUCTURES - OPTIMIZED FOR HOT PATH
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// RPCClient - fields ordered by access frequency in RPC hot path
//
//go:notinheap
//go:align 32
type RPCClient struct {
	// HOT: Most accessed during operations
	url    string       // 16B - RPC endpoint URL (used in every request)
	client *http.Client // 8B - HTTP client (used in every request)
	_      [8]byte      // 8B - Padding to 32B boundary for optimal alignment
}

func NewRPCClient() *RPCClient {
	return &RPCClient{
		url: RPCScheme + "://" + RPCHost + RPCProjectPath,
		client: &http.Client{
			Timeout: 30 * time.Second, // ONLY ADD THIS - prevents infinite hangs
			Transport: &http.Transport{
				MaxIdleConns:          1,
				MaxIdleConnsPerHost:   1,
				DisableCompression:    true,
				ForceAttemptHTTP2:     true,
				IdleConnTimeout:       90 * time.Second, // CHANGE FROM 0
				ResponseHeaderTimeout: 10 * time.Second, // ADD
			},
		},
	}
}

// Minimal RPC call - try indefinitely, trust everything
func (c *RPCClient) call(method string, params interface{}) (json.RawMessage, error) {
	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      1,
	}

	data, _ := json.Marshal(req)

	for {
		resp, err := http.Post(c.url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			continue // Try again immediately
		}

		var result struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Code int `json:"code"`
			} `json:"error"`
		}

		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()

		// Chad mode: trust RPC completely, retry everything immediately
		if result.Error != nil {
			continue
		}

		return result.Result, nil
	}
}

func (c *RPCClient) blockNumber() uint64 {
	result, _ := c.call("eth_blockNumber", []interface{}{})
	var blockHex string
	json.Unmarshal(result, &blockHex)
	return utils.ParseHexU64([]byte(blockHex[2:]))
}

func (c *RPCClient) getLogs(from, to uint64) []Log {
	params := map[string]interface{}{
		"fromBlock": fmt.Sprintf("0x%x", from),
		"toBlock":   fmt.Sprintf("0x%x", to),
		"topics":    []string{SyncEventSignature},
	}

	result, _ := c.call("eth_getLogs", []interface{}{params})
	var logs []Log
	json.Unmarshal(result, &logs)
	return logs
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SIMD HEX PROCESSING - PERFORMANCE CRITICAL
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:nosplit
//go:inline
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

//go:nosplit
//go:inline
func parseReservesToZeroTrimmed(dataStr string) (string, string) {
	// Trust RPC: dataStr is always 0x + 128 hex chars
	// Uniswap V2 reserves: first 32 hex chars are always zeros, just count the second 32

	leadingZeros0 := 32 + countHexLeadingZeros([]byte(dataStr[34:66]))
	leadingZeros1 := 32 + countHexLeadingZeros([]byte(dataStr[98:130]))

	reserve0Start := 2 + leadingZeros0
	reserve1Start := 66 + leadingZeros1

	// Branchless zero detection
	var reserve0, reserve1 string

	if reserve0Start >= 66 {
		reserve0 = "0"
	} else {
		reserve0 = dataStr[reserve0Start:66]
	}

	if reserve1Start >= 130 {
		reserve1 = "0"
	} else {
		reserve1 = dataStr[reserve1Start:130]
	}

	return reserve0, reserve1
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PEAK PERFORMANCE SYNC HARVESTER
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// StreamlinedHarvester - fields ordered by access frequency in processing hot path
//
//go:notinheap
//go:align 64
type StreamlinedHarvester struct {
	// CACHE LINE 1: Hot path batch processing arrays (64B)
	// These are accessed together during log processing and flushing
	eventBatch []ProcessedEvent // 24B - Pre-allocated event batch array
	logSlice   []Log            // 24B - Pre-allocated log slice for RPC results
	sqlBuilder strings.Builder  // 16B - Pre-allocated SQL statement builder

	// CACHE LINE 2: Core processing state (64B)
	// Frequently accessed during sync loop progression
	lastProcessed uint64   // 8B - Last successfully processed block number
	syncTarget    uint64   // 8B - Target block number for synchronization
	processed     int64    // 8B - Total events processed counter
	eventsInBatch int      // 8B - Current batch size counter
	_             [32]byte // 32B - Padding to complete cache line

	// CACHE LINE 3: Adaptive batch sizing (64B)
	// Critical for performance - accessed every batch
	consecutiveSuccesses int       // 8B - Success counter for batch size adaptation
	consecutiveFailures  int       // 8B - Failure counter for backoff
	startTime            time.Time // 24B - Start timestamp for progress reporting
	lastCommit           time.Time // 24B - Last commit timestamp

	// CACHE LINE 4: Database connections and transaction state (64B)
	// Accessed together during database operations
	reservesDB *sql.DB    // 8B - Reserves database connection
	currentTx  *sql.Tx    // 8B - Active transaction for batch operations
	rpcClient  *RPCClient // 8B - RPC client for blockchain communication
	_          [40]byte   // 40B - Padding to complete cache line

	// CACHE LINE 5: Context and control flow (64B)
	// Less frequently accessed, used for lifecycle management
	ctx    context.Context    // 16B - Cancellation context for clean shutdown
	cancel context.CancelFunc // 8B - Cancel function for context termination
	_      [40]byte           // 40B - Padding to complete cache line
}

func NewStreamlinedHarvester() *StreamlinedHarvester {
	runtime.LockOSThread()

	ctx, cancel := context.WithCancel(context.Background())

	h := &StreamlinedHarvester{
		// Pre-allocate everything to maximum size
		logSlice:   make([]Log, 0, MaxLogSliceSize),
		eventBatch: make([]ProcessedEvent, 0, MaxEventBatchSize),
		rpcClient:  NewRPCClient(),
		ctx:        ctx,
		cancel:     cancel,
		startTime:  time.Now(),
		lastCommit: time.Now(),
	}

	// Pre-allocate string builder
	h.sqlBuilder.Grow(MaxStringBuilder)

	// Open database
	h.reservesDB, _ = sql.Open("sqlite3", ReservesDBPath)

	// Apply performance pragmas
	h.reservesDB.Exec(`
		PRAGMA journal_mode = OFF;
		PRAGMA synchronous = OFF;
		PRAGMA cache_size = 200000;
		PRAGMA temp_store = MEMORY;
		PRAGMA mmap_size = 4294967296;
		PRAGMA page_size = 65536;
		PRAGMA locking_mode = EXCLUSIVE;
	`)

	// Signal handling with global shutdown coordination
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	return h
}

// Trust RPC completely - minimal validation
func (h *StreamlinedHarvester) processLog(log *Log) bool {
	// Only check if we care about this pair - skip "0x" prefix
	pairID := router.LookupPairByAddress([]byte(log.Address[2:]))
	if pairID == 0 {
		return false
	}

	// Trust RPC format - direct parsing, skip "0x" prefixes
	blockNum := utils.ParseHexU64([]byte(log.BlockNumber[2:]))
	logIndex := utils.ParseHexU64([]byte(log.LogIndex[2:]))
	reserve0, reserve1 := parseReservesToZeroTrimmed(log.Data)
	now := time.Now().Unix()

	// Single structure for both events and reserves
	h.eventBatch = append(h.eventBatch, ProcessedEvent{
		pairID:      int64(pairID),
		blockNum:    blockNum,
		logIndex:    logIndex,
		timestamp:   now,
		reserve0:    reserve0,
		reserve1:    reserve1,
		txHash:      log.TxHash,
		pairAddress: log.Address,
	})

	return true
}

// Optimized batch flush with pre-allocated builder
func (h *StreamlinedHarvester) flushBatch() {
	const maxBatch = 140 // SQLite variable limit

	for start := 0; start < len(h.eventBatch); start += maxBatch {
		end := start + maxBatch
		if end > len(h.eventBatch) {
			end = len(h.eventBatch)
		}

		chunk := h.eventBatch[start:end]

		// Events insert
		h.sqlBuilder.Reset()
		h.sqlBuilder.WriteString("INSERT OR IGNORE INTO sync_events VALUES ")

		args := make([]interface{}, 0, len(chunk)*7)
		for i, evt := range chunk {
			if i > 0 {
				h.sqlBuilder.WriteByte(',')
			}
			h.sqlBuilder.WriteString("(?,?,?,?,?,?,?)")
			args = append(args, evt.pairID, evt.blockNum, evt.txHash,
				evt.logIndex, evt.reserve0, evt.reserve1, evt.timestamp)
		}

		h.currentTx.Exec(h.sqlBuilder.String(), args...)

		// Reserves insert
		h.sqlBuilder.Reset()
		h.sqlBuilder.WriteString("INSERT OR REPLACE INTO pair_reserves VALUES ")

		args = make([]interface{}, 0, len(chunk)*6)
		for i, evt := range chunk {
			if i > 0 {
				h.sqlBuilder.WriteByte(',')
			}
			h.sqlBuilder.WriteString("(?,?,?,?,?,?)")
			args = append(args, evt.pairID, evt.pairAddress, evt.reserve0,
				evt.reserve1, evt.blockNum, evt.timestamp)
		}

		h.currentTx.Exec(h.sqlBuilder.String(), args...)
	}

	h.eventsInBatch += len(h.eventBatch)
	h.eventBatch = h.eventBatch[:0] // Reset without deallocating
}

func (h *StreamlinedHarvester) SyncToLatest() error {
	// Get sync range
	h.syncTarget = h.rpcClient.blockNumber()
	h.reservesDB.QueryRow("SELECT COALESCE(MAX(block_number), ?) FROM sync_events", DeploymentBlock).Scan(&h.lastProcessed)

	if h.lastProcessed >= h.syncTarget {
		debug.DropMessage("SYNC", "Already current")
		return nil
	}

	debug.DropMessage("SYNC", fmt.Sprintf("Syncing %d blocks", h.syncTarget-h.lastProcessed))

	// Begin transaction
	h.currentTx, _ = h.reservesDB.Begin()

	// Chad sync loop - no adaptive sizing, just hammer through
	current := h.lastProcessed + 1
	batchSize := OptimalBatchSize

	for current <= h.syncTarget {
		end := current + batchSize - 1
		if end > h.syncTarget {
			end = h.syncTarget
		}

		// Process batch - trust everything
		h.logSlice = h.logSlice[:0] // Reset without deallocating
		logs := h.rpcClient.getLogs(current, end)
		h.logSlice = append(h.logSlice, logs...)

		for i := range h.logSlice {
			h.processLog(&h.logSlice[i])

			if len(h.eventBatch) >= EventBatchSize {
				h.flushBatch()
			}
		}

		// Update progress and advance
		h.lastProcessed = end
		current = end + 1
		h.processed += int64(len(h.logSlice))

		// Periodic commits
		if h.eventsInBatch >= CommitBatchSize {
			h.flushBatch()
			h.currentTx.Commit()
			h.currentTx, _ = h.reservesDB.Begin()
			h.eventsInBatch = 0
		}

		// Simple progress
		if current%10000 == 0 {
			debug.DropMessage("PROGRESS", fmt.Sprintf("Block %d", current))
		}
	}

	// Final flush and commit
	h.flushBatch()
	h.currentTx.Commit()

	debug.DropMessage("DONE", fmt.Sprintf("%d events synced", h.processed))
	return nil
}

func (h *StreamlinedHarvester) Close() {
	// Ensure all data is properly flushed before marking as done
	if h.currentTx != nil {
		h.flushBatch()       // Flush any remaining events
		h.currentTx.Commit() // Commit final transaction
	}

	h.reservesDB.Close() // Close database connection
	h.cancel()           // Cancel context

	// Signal completion to shutdown coordinator AFTER everything is clean
	control.ShutdownWG.Done()

	debug.DropMessage("SYNC_CLEAN", "Syncharvester shutdown complete")
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ROUTER INTEGRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func FlushSyncedReservesToRouter() error {
	db, _ := sql.Open("sqlite3", ReservesDBPath)
	defer db.Close()

	var latestBlock uint64
	db.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&latestBlock)

	// Pre-allocate buffers
	var blockHexBuf [18]byte
	blockHexBuf[0] = '0'
	blockHexBuf[1] = 'x'
	blockLen := 2 + writeHex64(blockHexBuf[2:], latestBlock)
	blockHex := blockHexBuf[:blockLen]

	var dataBuffer [130]byte
	dataBuffer[0] = '0'
	dataBuffer[1] = 'x'

	// Static LogView
	v := types.LogView{
		LogIdx:  []byte("0x0"),
		TxIndex: []byte("0x0"),
		Topics:  []byte(SyncEventSignature),
		BlkNum:  blockHex,
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

	debug.DropMessage("FLUSH", fmt.Sprintf("%d reserves", count))
	return nil
}

func writeHex64(dst []byte, v uint64) int {
	const hex = "0123456789abcdef"
	if v == 0 {
		dst[0] = '0'
		return 1
	}

	nibbles := 0
	temp := v
	for temp > 0 {
		nibbles++
		temp >>= 4
	}

	for i := nibbles - 1; i >= 0; i-- {
		dst[i] = hex[v&0xF]
		v >>= 4
	}

	return nibbles
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func ExecutePeakSync() error {
	debug.DropMessage("SYNC_START", "Starting peak sync...")

	// Register with global shutdown coordination
	control.ShutdownWG.Add(1)

	debug.DropMessage("SYNC_START", "Creating harvester...")
	h := NewStreamlinedHarvester()
	defer h.Close() // Close() will call ShutdownWG.Done() after proper cleanup

	debug.DropMessage("SYNC_START", "Harvester created, beginning sync...")
	return h.SyncToLatest()
}

func ExecutePeakSyncWithDB(existingPairsDB *sql.DB) error {
	// No longer need pairs DB - using router's address lookup
	return ExecutePeakSync()
}

func CheckIfPeakSyncNeeded() (bool, uint64, uint64, error) {
	db, _ := sql.Open("sqlite3", ReservesDBPath)
	defer db.Close()

	var lastBlock uint64
	db.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&lastBlock)

	client := NewRPCClient()
	currentHead := client.blockNumber()

	needed := lastBlock < currentHead
	debug.DropMessage("CHECK", fmt.Sprintf("Last: %d, Head: %d, Needed: %v", lastBlock, currentHead, needed))

	return needed, lastBlock, currentHead, nil
}
