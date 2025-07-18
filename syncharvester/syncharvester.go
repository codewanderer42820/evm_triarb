// ════════════════════════════════════════════════════════════════════════════════════════════════
// Single-Core Sync Harvester
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Blockchain Event Synchronization Engine
//
// Description:
//   Single-threaded synchronization system for Ethereum event processing with batch database
//   writes and zero-allocation buffer management.
//
// Features:
//   - Single-core execution with thread affinity
//   - Batch database operations for 5-10x write throughput
//   - Pre-allocated memory structures for zero-allocation operation
//   - Adaptive batch sizing based on network conditions
//   - Reuses existing database connections from main
//   - Router integration for flushing synced reserves
//   - Zero-trimmed hex string storage for optimal database efficiency
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// REQUIRED IMPORTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

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
	"main/debug"
	"main/router"
	"main/types"
	"main/utils"

	_ "github.com/mattn/go-sqlite3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE TYPE DEFINITIONS (from router.go)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// TradingPairID uniquely identifies a Uniswap V2 trading pair contract.
type TradingPairID uint64

// PackedAddress represents an Ethereum address optimized for hash table operations.
//
//go:notinheap
//go:align 8
type PackedAddress struct {
	words [3]uint64 // 24B - 160-bit Ethereum address as three 64-bit values
	_     [8]byte   // 8B - Padding for cache line optimization
}

//go:notinheap
//go:align 64
var (
	// File paths - Made variables for testing
	ReservesDBPath = "uniswap_v2_reserves.db"

	// Address lookup infrastructure (same as router.go)
	addressToPairMap  [constants.AddressTableCapacity]TradingPairID
	packedAddressKeys [constants.AddressTableCapacity]PackedAddress
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RPC ENDPOINT CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// RPC configuration constants for Ethereum node connectivity
const (
	// RPCHost specifies the Infura mainnet endpoint host
	RPCHost = "mainnet.infura.io"

	// RPCProjectPath contains the Infura project API path with credentials
	RPCProjectPath = "/v3/a2a3139d2ab24d59bed2dc3643664126"

	// RPCScheme defines the protocol scheme for RPC connections
	RPCScheme = "https"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONFIGURATION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Synchronization parameters
	OptimalBatchSize = uint64(10_000) // Initial batch size for block processing
	SyncTargetOffset = 0              // No offset - sync to exact head (fast enough)

	// Database batch processing - Optimized for high throughput
	CommitBatchSize = 100_000 // Events per transaction commit
	EventBatchSize  = 10_000  // Events per batch insert

	// Buffer sizes - Optimized for real-world usage
	PreAllocLogSliceSize = 50_000 // Block batches with many events

	// Event signatures
	SyncEventSignature = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

	// Uniswap V2 deployment block
	UniswapV2DeploymentBlock = 10000835

	// Adaptive batch sizing parameters
	ConsecutiveSuccessThreshold = 3 // Double batch size after 3 consecutive successes
	MinBatchSize                = 1 // Minimum batch size
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ETHEREUM LOG STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Log represents an Ethereum event log entry with fields ordered by access frequency.
//
//go:notinheap
//go:align 64
type Log struct {
	// HOT: Most accessed fields first (cache line 1 - 64B)
	Data    string   `json:"data"`    // 16B - Event data (parsed every log)
	Address string   `json:"address"` // 16B - Contract address (lookup key)
	Topics  []string `json:"topics"`  // 24B - Event topics (signature validation)
	_       [8]byte  // 8B - Padding to cache boundary

	// WARM: Moderately accessed (cache line 2 - 64B)
	BlockNumber string   `json:"blockNumber"`     // 16B - Block number (parsed for DB)
	TxHash      string   `json:"transactionHash"` // 16B - Transaction hash (stored in DB)
	LogIndex    string   `json:"logIndex"`        // 16B - Log index (parsed for DB)
	_           [16]byte // 16B - Padding to cache boundary
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// BATCH STRUCTURES FOR OPTIMIZED DATABASE WRITES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// batchEvent holds event data for batch insertion
//
//go:notinheap
//go:align 64
type batchEvent struct {
	// Cache line 1 (64B)
	pairID   int64   // 8B
	blockNum uint64  // 8B
	txHash   string  // 16B
	logIndex uint64  // 8B
	reserve0 string  // 16B - Zero-trimmed hex string
	_        [8]byte // 8B - Padding

	// Cache line 2 (64B)
	reserve1  string   // 16B - Zero-trimmed hex string
	timestamp int64    // 8B
	_         [40]byte // 40B - Padding to complete cache line
}

// batchReserve holds reserve update data for batch insertion
//
//go:notinheap
//go:align 64
type batchReserve struct {
	// Cache line 1 (64B)
	pairID      int64   // 8B
	pairAddress string  // 16B
	reserve0    string  // 16B - Zero-trimmed hex string
	reserve1    string  // 16B - Zero-trimmed hex string
	_           [8]byte // 8B - Padding

	// Cache line 2 (64B)
	blockHeight uint64   // 8B
	timestamp   int64    // 8B
	_           [48]byte // 48B - Padding to complete cache line
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RPC CLIENT STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// RPCRequest represents a JSON-RPC request with fields ordered by usage frequency.
//
//go:notinheap
//go:align 64
type RPCRequest struct {
	// HOT: Most accessed during RPC calls
	Method string        `json:"method"` // 16B - RPC method name (every call)
	Params []interface{} `json:"params"` // 24B - Method parameters (every call)

	// WARM: Standard protocol fields
	JSONRPC string `json:"jsonrpc"` // 16B - Protocol version (constant)
	ID      int    `json:"id"`      // 8B - Request ID (incremental)
	// Total: 64B - Perfect cache line fit
}

// RPCResponse represents a JSON-RPC response with result data prioritized.
//
//go:notinheap
//go:align 64
type RPCResponse struct {
	// HOT: Primary response data
	Result json.RawMessage `json:"result"` // 24B - Response data (parsed every response)

	// WARM: Error handling
	Error *RPCError `json:"error"` // 8B - Error object (checked every response)

	// COLD: Protocol fields
	JSONRPC string  `json:"jsonrpc"` // 16B - Protocol version (rarely used)
	ID      int     `json:"id"`      // 8B - Request ID (rarely used)
	_       [8]byte // 8B - Padding to 64B boundary
}

// RPCError represents an RPC error with code and message.
//
//go:notinheap
//go:align 32
type RPCError struct {
	// HOT: Error information (accessed together)
	Code    int     `json:"code"`    // 8B - Error code
	Message string  `json:"message"` // 16B - Error message
	_       [8]byte // 8B - Padding to 32B boundary
}

// RPCClient manages HTTP connections for blockchain RPC communication.
//
//go:notinheap
//go:align 32
type RPCClient struct {
	// HOT: Most accessed during operations
	url    string       // 16B - RPC endpoint URL (used in every request)
	client *http.Client // 8B - HTTP client (used in every request)
	_      [8]byte      // 8B - Padding to 32B boundary
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYNC HARVESTER STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// PeakHarvester orchestrates blockchain synchronization with database persistence.
// Fields are ordered by access frequency in the processing hot path.
//
//go:notinheap
//go:align 64
type PeakHarvester struct {
	// CACHE LINE 1: Batch processing (64B) - Pre-allocated to exact sizes
	eventBatch    []batchEvent   // 24B - Pre-allocated to EventBatchSize capacity
	reserveBatch  []batchReserve // 24B - Pre-allocated to EventBatchSize capacity
	eventsInBatch int            // 8B - Events in current batch
	processed     int64          // 8B - Total events processed

	// CACHE LINE 2: Core processing state (64B)
	lastProcessed uint64   // 8B - Last processed block
	syncTarget    uint64   // 8B - Target block for sync
	_             [48]byte // 48B - Padding

	// CACHE LINE 3: Database connections and lookups (64B)
	reservesDB     *sql.DB    // 8B - Reserves database (used for all writes)
	pairsDB        *sql.DB    // 8B - Pairs database (provided by main)
	currentTx      *sql.Tx    // 8B - Current transaction
	updateSyncStmt *sql.Stmt  // 8B - Sync metadata statement
	rpcClient      *RPCClient // 8B - RPC client
	logSlice       []Log      // 24B - Pre-allocated log slice

	// CACHE LINE 4: Batch adaptation and timing (64B)
	consecutiveSuccesses int       // 8B - Success counter
	consecutiveFailures  int       // 8B - Failure counter
	startTime            time.Time // 24B - Start timestamp
	lastCommit           time.Time // 24B - Last commit timestamp

	// CACHE LINE 5: Context and control (64B)
	ctx        context.Context    // 16B - Cancellation context
	cancel     context.CancelFunc // 8B - Cancel function
	signalChan chan os.Signal     // 24B - Signal channel
	_          [16]byte           // 16B - Padding
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE PROCESSING FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// countHexLeadingZeros performs efficient leading zero counting for hex-encoded numeric data
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func countHexLeadingZeros(segment []byte) int {
	// Define the 64-bit pattern representing eight consecutive ASCII '0' characters
	const ZERO_PATTERN = 0x3030303030303030

	// Process the 32-byte hex segment in four 8-byte chunks simultaneously
	// XOR with ZERO_PATTERN converts zero bytes to 0x00 and non-zero bytes to non-zero values
	c0 := utils.Load64(segment[0:8]) ^ ZERO_PATTERN   // Process bytes 0-7
	c1 := utils.Load64(segment[8:16]) ^ ZERO_PATTERN  // Process bytes 8-15
	c2 := utils.Load64(segment[16:24]) ^ ZERO_PATTERN // Process bytes 16-23
	c3 := utils.Load64(segment[24:32]) ^ ZERO_PATTERN // Process bytes 24-31

	// Create a bitmask indicating which 8-byte chunks contain only zero characters
	// The expression (x|(^x+1))>>63 produces 1 if any byte in x is non-zero, 0 if all bytes are zero
	mask := ((c0|(^c0+1))>>63)<<0 | ((c1|(^c1+1))>>63)<<1 |
		((c2|(^c2+1))>>63)<<2 | ((c3|(^c3+1))>>63)<<3

	// Find the first chunk that contains a non-zero character
	firstChunk := bits.TrailingZeros64(mask)

	// Handle the special case where all 32 hex characters are zeros
	if firstChunk == 64 {
		return 32 // All characters in the segment are zeros
	}

	// Create an array to access the processed chunks by index
	chunks := [4]uint64{c0, c1, c2, c3}

	// Within the first non-zero chunk, find the first non-zero byte
	// Divide by 8 to convert bit position to byte position within the chunk
	firstByte := bits.TrailingZeros64(chunks[firstChunk]) >> 3

	// Calculate the total number of leading zero characters
	return (firstChunk << 3) + firstByte
}

// parseReservesToZeroTrimmedHex extracts zero-trimmed hex strings using SIMD optimization
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) parseReservesToZeroTrimmedHex(dataStr string) (string, string) {
	// Uniswap V2 sync event: 0x + 128 hex chars
	// Layout: [64 chars reserve0][64 chars reserve1]

	// Use SIMD counting for reserve0 (chars 2-65)
	leadingZeros0 := countHexLeadingZeros([]byte(dataStr[2:34])) // First 32 chars
	if leadingZeros0 >= 32 {
		leadingZeros0 += countHexLeadingZeros([]byte(dataStr[34:66])) // Next 32 chars
	}

	// Use SIMD counting for reserve1 (chars 66-129)
	leadingZeros1 := countHexLeadingZeros([]byte(dataStr[66:98])) // First 32 chars
	if leadingZeros1 >= 32 {
		leadingZeros1 += countHexLeadingZeros([]byte(dataStr[98:130])) // Next 32 chars
	}

	// Extract reserves with calculated offsets
	reserve0Start := 2 + leadingZeros0
	if reserve0Start >= 66 {
		return "0", dataStr[66+leadingZeros1 : 130]
	}

	reserve1Start := 66 + leadingZeros1
	if reserve1Start >= 130 {
		return dataStr[reserve0Start:66], "0"
	}

	return dataStr[reserve0Start:66], dataStr[reserve1Start:130]
}

// collectLogForBatch validates and collects a log entry for batch processing
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (h *PeakHarvester) collectLogForBatch(log *Log) bool {
	// Skip non-sync events
	if len(log.Topics) == 0 || log.Topics[0] != SyncEventSignature {
		return false
	}

	// Parse hex directly (skip "0x" prefix)
	blockNum := utils.ParseHexU64([]byte(log.BlockNumber[2:]))
	logIndex := utils.ParseHexU64([]byte(log.LogIndex[2:]))

	// Fast address lookup - skip "0x" prefix directly
	pairID := h.LookupPairByAddress([]byte(log.Address[2:]))
	if pairID == 0 {
		return false
	}

	// Parse reserves to zero-trimmed hex strings
	reserve0Hex, reserve1Hex := h.parseReservesToZeroTrimmedHex(log.Data)

	now := time.Now().Unix()

	// Add to batch with zero-trimmed hex strings
	h.eventBatch = append(h.eventBatch, batchEvent{
		pairID:    int64(pairID),
		blockNum:  blockNum,
		txHash:    log.TxHash,
		logIndex:  logIndex,
		reserve0:  reserve0Hex,
		reserve1:  reserve1Hex,
		timestamp: now,
	})

	h.reserveBatch = append(h.reserveBatch, batchReserve{
		pairID:      int64(pairID),
		pairAddress: log.Address,
		reserve0:    reserve0Hex,
		reserve1:    reserve1Hex,
		blockHeight: blockNum,
		timestamp:   now,
	})

	return true
}

// flushBatch performs batch insert of collected events and reserves
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) flushBatch() error {
	if len(h.eventBatch) == 0 {
		return nil
	}

	// SQLite has a limit of 999 variables per query
	// Each event insert uses 7 variables, so max batch is 999/7 = 142
	const maxBatchSize = 140

	// Pre-allocate builder to reduce allocations
	var builder strings.Builder
	builder.Grow(8192)

	// Process in chunks if needed
	for start := 0; start < len(h.eventBatch); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(h.eventBatch) {
			end = len(h.eventBatch)
		}

		// Build multi-value INSERT for this chunk
		chunkEvents := h.eventBatch[start:end]
		chunkReserves := h.reserveBatch[start:end]

		// Build event insert statement
		builder.Reset()
		builder.WriteString("INSERT OR IGNORE INTO sync_events VALUES ")

		eventArgs := make([]interface{}, 0, len(chunkEvents)*7)
		for i, evt := range chunkEvents {
			if i > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString("(?,?,?,?,?,?,?)")
			eventArgs = append(eventArgs,
				evt.pairID, evt.blockNum, evt.txHash, evt.logIndex,
				evt.reserve0, evt.reserve1, evt.timestamp)
		}

		_, err := h.currentTx.Exec(builder.String(), eventArgs...)
		if err != nil {
			return fmt.Errorf("batch insert events failed: %w", err)
		}

		// Build reserve insert statement
		builder.Reset()
		builder.WriteString("INSERT OR REPLACE INTO pair_reserves VALUES ")

		reserveArgs := make([]interface{}, 0, len(chunkReserves)*6)
		for i, res := range chunkReserves {
			if i > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString("(?,?,?,?,?,?)")
			reserveArgs = append(reserveArgs,
				res.pairID, res.pairAddress, res.reserve0,
				res.reserve1, res.blockHeight, res.timestamp)
		}

		_, err = h.currentTx.Exec(builder.String(), reserveArgs...)
		if err != nil {
			return fmt.Errorf("batch insert reserves failed: %w", err)
		}
	}

	h.eventsInBatch += len(h.eventBatch)

	// Clear batches by reslicing to zero length
	h.eventBatch = h.eventBatch[:0]
	h.reserveBatch = h.reserveBatch[:0]

	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RPC CLIENT IMPLEMENTATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// NewRPCClient creates a new RPC client for blockchain communication
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func NewRPCClient(url string) *RPCClient {
	return &RPCClient{
		url: url,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        1,
				MaxIdleConnsPerHost: 1,
				MaxConnsPerHost:     1,
				DisableCompression:  true,
				ForceAttemptHTTP2:   true,
				IdleConnTimeout:     0, // Never close idle connection
				DisableKeepAlives:   false,
			},
		},
	}
}

// Call executes an RPC method with unlimited retries and exponential backoff
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (c *RPCClient) Call(ctx context.Context, result interface{}, method string, params ...interface{}) error {
	req := RPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      1,
	}

	data, _ := json.Marshal(req)
	dataStr := string(data)

	backoffMs := 100 // Start with 100ms backoff

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		httpReq, _ := http.NewRequestWithContext(ctx, "POST", c.url, strings.NewReader(dataStr))
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := c.client.Do(httpReq)
		if err != nil {
			time.Sleep(time.Millisecond * time.Duration(backoffMs))
			backoffMs *= 2
			if backoffMs > 30000 { // Cap at 30 seconds
				backoffMs = 30000
			}
			continue
		}
		defer resp.Body.Close()

		var rpcResp RPCResponse
		json.NewDecoder(resp.Body).Decode(&rpcResp)

		// Handle rate limit with exponential backoff
		if rpcResp.Error != nil && rpcResp.Error.Code == 429 {
			time.Sleep(time.Millisecond * time.Duration(backoffMs))
			backoffMs *= 2
			if backoffMs > 30000 { // Cap at 30 seconds
				backoffMs = 30000
			}
			continue
		}

		if rpcResp.Error != nil {
			return fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
		}

		return json.Unmarshal(rpcResp.Result, result)
	}
}

// BlockNumber retrieves the current block number from the blockchain
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (c *RPCClient) BlockNumber(ctx context.Context) (uint64, error) {
	var result string
	if err := c.Call(ctx, &result, "eth_blockNumber"); err != nil {
		return 0, err
	}
	// Skip "0x" prefix directly
	return utils.ParseHexU64([]byte(result[2:])), nil
}

// GetLogs retrieves event logs from the blockchain within a specified block range
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (c *RPCClient) GetLogs(ctx context.Context, fromBlock, toBlock uint64, addresses []string, topics []string) ([]Log, error) {
	params := map[string]interface{}{
		"fromBlock": fmt.Sprintf("0x%x", fromBlock),
		"toBlock":   fmt.Sprintf("0x%x", toBlock),
	}

	if len(addresses) > 0 {
		params["address"] = addresses
	}

	if len(topics) > 0 {
		params["topics"] = []string{topics[0]}
	}

	var logs []Log
	if err := c.Call(ctx, &logs, "eth_getLogs", params); err != nil {
		return nil, err
	}

	return logs, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATABASE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// configureDatabase applies optimization settings for write performance
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func configureDatabase(db *sql.DB) error {
	// Database optimization settings
	optimizations := []string{
		"PRAGMA journal_mode = OFF",
		"PRAGMA synchronous = OFF",
		"PRAGMA cache_size = 200000",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 4294967296",
		"PRAGMA page_size = 65536",
		"PRAGMA auto_vacuum = NONE",
		"PRAGMA locking_mode = EXCLUSIVE",
		"PRAGMA busy_timeout = 30000",
		"PRAGMA wal_autocheckpoint = 0",
	}

	for _, pragma := range optimizations {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("failed to execute %s: %w", pragma, err)
		}
	}

	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// NewPeakHarvester creates and initializes a new sync harvester instance
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func NewPeakHarvester(existingPairsDB *sql.DB) (*PeakHarvester, error) {
	debug.DropMessage("INIT", "Initializing harvester")

	// Lock this goroutine to current OS thread
	runtime.LockOSThread()

	// Create context for clean shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Create RPC client
	rpcURL := RPCScheme + "://" + constants.WsHost + RPCProjectPath
	rpcClient := NewRPCClient(rpcURL)

	// Open reserves database
	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to open reserves database: %w", err)
	}

	// Test the connection
	if err := reservesDB.Ping(); err != nil {
		reservesDB.Close()
		cancel()
		return nil, fmt.Errorf("database connection failed: %w", err)
	}

	// Configure database
	if err := configureDatabase(reservesDB); err != nil {
		reservesDB.Close()
		cancel()
		return nil, fmt.Errorf("failed to configure database: %w", err)
	}

	// Create harvester instance
	h := &PeakHarvester{
		// Core fields - Pre-allocate batches to exact EventBatchSize capacity
		logSlice:     make([]Log, 0, PreAllocLogSliceSize),
		eventBatch:   make([]batchEvent, 0, EventBatchSize),
		reserveBatch: make([]batchReserve, 0, EventBatchSize),

		// Database connections
		rpcClient:  rpcClient,
		pairsDB:    existingPairsDB,
		reservesDB: reservesDB,

		// Context and control
		ctx:        ctx,
		cancel:     cancel,
		signalChan: make(chan os.Signal, 1),
		startTime:  time.Now(),
		lastCommit: time.Now(),
	}

	// Set up signal handling
	signal.Notify(h.signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-h.signalChan
		debug.DropMessage("SIGNAL", "Received interrupt")
		h.cancel()
	}()

	// Initialize database schema
	if err := h.initializeSchema(); err != nil {
		h.reservesDB.Close()
		cancel()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Load pair mappings into Robin Hood hash table
	if err := h.loadPairMappings(); err != nil {
		h.reservesDB.Close()
		cancel()
		return nil, fmt.Errorf("failed to load pair mappings: %w", err)
	}

	// Prepare global statements
	if err := h.prepareGlobalStatements(); err != nil {
		h.reservesDB.Close()
		cancel()
		return nil, fmt.Errorf("failed to prepare statements: %w", err)
	}

	debug.DropMessage("READY", "Pairs loaded into hash table")
	return h, nil
}

// initializeSchema creates database tables if they don't exist
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) initializeSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS pair_reserves (
		pair_id      INTEGER PRIMARY KEY,
		pair_address TEXT NOT NULL,
		reserve0     TEXT NOT NULL,
		reserve1     TEXT NOT NULL,
		block_height INTEGER NOT NULL,
		last_updated INTEGER NOT NULL,
		UNIQUE(pair_address)
	) WITHOUT ROWID;
	
	CREATE TABLE IF NOT EXISTS sync_events (
		pair_id      INTEGER NOT NULL,
		block_number INTEGER NOT NULL,
		tx_hash      TEXT NOT NULL,
		log_index    INTEGER NOT NULL,
		reserve0     TEXT NOT NULL,
		reserve1     TEXT NOT NULL,
		created_at   INTEGER NOT NULL,
		PRIMARY KEY (block_number, tx_hash, log_index)
	) WITHOUT ROWID;
	
	CREATE TABLE IF NOT EXISTS sync_metadata (
		id               INTEGER PRIMARY KEY,
		last_block       INTEGER NOT NULL,
		sync_target      INTEGER NOT NULL,
		sync_status      TEXT NOT NULL,
		updated_at       INTEGER NOT NULL,
		events_processed INTEGER NOT NULL DEFAULT 0
	) WITHOUT ROWID;
	`

	_, err := h.reservesDB.Exec(schema)
	return err
}

// loadPairMappings loads trading pair data into the Robin Hood hash table
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) loadPairMappings() error {
	debug.DropMessage("LOADING", "Uniswap V2 pairs")

	query := `
		SELECT p.id, p.pool_address 
		FROM pools p
		JOIN exchanges e ON p.exchange_id = e.id
		WHERE e.name = 'uniswap_v2' AND e.chain_id = 1
	`

	rows, err := h.pairsDB.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query pairs: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int64
		var addr string
		rows.Scan(&id, &addr)

		// Register in Robin Hood hash table (same as router.go)
		RegisterTradingPairAddress([]byte(addr), TradingPairID(id))
		count++
	}

	debug.DropMessage("LOADED", utils.Itoa(count)+" pairs")
	return rows.Err()
}

// prepareGlobalStatements prepares SQL statements for execution
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) prepareGlobalStatements() error {
	var err error

	h.updateSyncStmt, err = h.reservesDB.Prepare(`
		INSERT OR REPLACE INTO sync_metadata 
		(id, last_block, sync_target, sync_status, updated_at, events_processed) 
		VALUES (1, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare update sync statement: %w", err)
	}

	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYNCHRONIZATION EXECUTION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// SyncToLatestAndTerminate executes the main synchronization loop
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (h *PeakHarvester) SyncToLatestAndTerminate() error {
	debug.DropMessage("SYNC", "Starting")

	// Determine sync target
	currentHead, err := h.rpcClient.BlockNumber(h.ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block: %w", err)
	}

	h.syncTarget = currentHead - SyncTargetOffset
	debug.DropMessage("TARGET", fmt.Sprintf("%d (head: %d)", h.syncTarget, currentHead))

	// Get starting block
	startBlock := h.getLastProcessedBlock()
	if startBlock == 0 {
		startBlock = UniswapV2DeploymentBlock
	}

	// Check if already synced
	if startBlock >= h.syncTarget {
		debug.DropMessage("CURRENT", "Already synchronized")
		return h.terminateCleanly()
	}

	blocksToSync := h.syncTarget - startBlock
	debug.DropMessage("SCOPE", utils.Itoa(int(blocksToSync))+" blocks")

	// Begin initial transaction
	if err := h.beginTransaction(); err != nil {
		return fmt.Errorf("failed to begin initial transaction: %w", err)
	}

	// Execute sync loop
	err = h.executeSyncLoop(startBlock)

	// Always commit pending data
	if h.eventsInBatch > 0 {
		debug.DropMessage("FINAL", utils.Itoa(h.eventsInBatch)+" events")
		h.commitTransaction()
	} else if h.currentTx != nil {
		if err != nil {
			h.rollbackTransaction()
		} else {
			h.commitTransaction()
		}
	}

	if err != nil && err != context.Canceled {
		debug.DropMessage("ERROR", err.Error())
	}

	return h.terminateCleanly()
}

// executeSyncLoop processes blocks in adaptive batches
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (h *PeakHarvester) executeSyncLoop(startBlock uint64) error {
	current := startBlock
	batchSize := OptimalBatchSize

	for current < h.syncTarget {
		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		default:
		}

		// Calculate batch end
		batchEnd := current + batchSize
		if batchEnd > h.syncTarget {
			batchEnd = h.syncTarget
		}

		// Process batch
		success := h.processBatch(current, batchEnd)

		if success {
			// Reset failure counter and increment success counter
			h.consecutiveFailures = 0
			h.consecutiveSuccesses++

			if h.consecutiveSuccesses >= ConsecutiveSuccessThreshold {
				oldBatchSize := batchSize
				batchSize *= 2
				h.consecutiveSuccesses = 0
				debug.DropMessage("BATCH+", utils.Itoa(int(oldBatchSize))+"→"+utils.Itoa(int(batchSize)))
			}

			// Update progress and advance
			h.lastProcessed = batchEnd
			current = batchEnd + 1

		} else {
			// Halve batch size on failure (unlimited retries via RPC client)
			h.consecutiveSuccesses = 0
			batchSize /= 2
			if batchSize < MinBatchSize {
				batchSize = MinBatchSize
			}
		}

		// Commit periodically
		if h.eventsInBatch >= CommitBatchSize {
			h.commitTransaction()
			if err := h.beginTransaction(); err != nil {
				return fmt.Errorf("failed to begin new transaction: %w", err)
			}
		}

		// Progress reporting
		if current%10000 == 0 {
			h.reportProgress()
		}
	}

	debug.DropMessage("COMPLETE", fmt.Sprintf("Block %d", current))
	return nil
}

// processBatch fetches and processes logs for a block range
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (h *PeakHarvester) processBatch(fromBlock, toBlock uint64) bool {
	// Reuse pre-allocated slice
	h.logSlice = h.logSlice[:0]

	// Fetch logs with rate limit handling in RPC client
	logs, err := h.rpcClient.GetLogs(h.ctx, fromBlock, toBlock, []string{}, []string{SyncEventSignature})
	if err != nil {
		debug.DropMessage("FETCH_ERROR", fmt.Sprintf("%d-%d: %v", fromBlock, toBlock, err))
		return false
	}

	h.logSlice = logs

	// Collect all valid logs
	processedCount := 0
	for i := range h.logSlice {
		if h.collectLogForBatch(&h.logSlice[i]) {
			processedCount++
		}

		// Flush if we've collected enough for a batch
		if len(h.eventBatch) >= EventBatchSize {
			if err := h.flushBatch(); err != nil {
				debug.DropMessage("BATCH_ERROR", err.Error())
				return false
			}
		}
	}

	// Flush any remaining events
	if len(h.eventBatch) > 0 {
		if err := h.flushBatch(); err != nil {
			debug.DropMessage("BATCH_ERROR", err.Error())
			return false
		}
	}

	h.processed += int64(processedCount)
	if processedCount > 0 {
		debug.DropMessage("BATCH", fmt.Sprintf("%d-%d: %d events", fromBlock, toBlock, processedCount))
	}

	return true
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TRANSACTION MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// beginTransaction starts a new database transaction
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) beginTransaction() error {
	var err error
	h.currentTx, err = h.reservesDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	h.eventsInBatch = 0
	return nil
}

// commitTransaction commits the current database transaction
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (h *PeakHarvester) commitTransaction() {
	// Flush any remaining batch data
	h.flushBatch()

	err := h.currentTx.Commit()
	h.currentTx = nil

	if err != nil {
		debug.DropMessage("TX_ERROR", err.Error())
	} else {
		debug.DropMessage("TX", utils.Itoa(h.eventsInBatch)+" events")
	}

	h.lastCommit = time.Now()
}

// rollbackTransaction rolls back the current database transaction
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) rollbackTransaction() {
	h.currentTx.Rollback()
	h.currentTx = nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MONITORING AND CLEANUP
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// reportProgress logs synchronization progress information
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) reportProgress() {
	elapsed := time.Since(h.startTime)
	eventsPerSecond := float64(h.processed) / elapsed.Seconds()

	blockStr := utils.Itoa(int(h.lastProcessed))
	eventsStr := utils.Itoa(int(h.processed))
	rateStr := utils.Ftoa(eventsPerSecond)

	debug.DropMessage("PROGRESS", fmt.Sprintf(
		"Block %s, %s events (%s/s)",
		blockStr, eventsStr, rateStr,
	))

	// Update sync metadata
	if h.updateSyncStmt != nil {
		now := time.Now().Unix()
		h.updateSyncStmt.Exec(h.lastProcessed, h.syncTarget, "running", now, h.processed)
	}
}

// getLastProcessedBlock retrieves the last processed block from the database
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) getLastProcessedBlock() uint64 {
	var lastBlock uint64
	h.reservesDB.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&lastBlock)
	return lastBlock
}

// terminateCleanly performs final cleanup and resource deallocation
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (h *PeakHarvester) terminateCleanly() error {
	debug.DropMessage("CLEANUP", "Starting")

	// Commit any pending transaction
	if h.currentTx != nil {
		h.commitTransaction()
	}

	// Final metadata update
	now := time.Now().Unix()
	h.updateSyncStmt.Exec(h.lastProcessed, h.syncTarget, "completed", now, h.processed)
	h.updateSyncStmt.Close()

	// Final optimization
	h.reservesDB.Exec("PRAGMA optimize")
	h.reservesDB.Close()

	elapsed := time.Since(h.startTime)
	debug.DropMessage("DONE", fmt.Sprintf(
		"%s events, %s blocks in %v",
		utils.Itoa(int(h.processed)), utils.Itoa(int(h.lastProcessed)), elapsed.Round(time.Second),
	))

	// Nil everything right before exit
	h.eventBatch = nil
	h.reserveBatch = nil
	h.logSlice = nil

	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ROUTER INTEGRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// FlushSyncedReservesToRouter loads all reserves from database and dispatches to router.
// Reads zero-trimmed hex strings and formats them for router consumption.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func FlushSyncedReservesToRouter() error {
	// Open the reserves database
	db, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		return fmt.Errorf("failed to open reserves database: %w", err)
	}
	defer db.Close()

	// Get the latest block height from sync metadata
	var latestBlock uint64
	err = db.QueryRow("SELECT last_block FROM sync_metadata WHERE id = 1").Scan(&latestBlock)
	if err != nil {
		return fmt.Errorf("no sync data found in database")
	}

	// Pre-allocate block hex buffer with optimal size for uint64
	// Max uint64 in hex is 16 chars plus "0x" = 18 chars
	var blockHexBuf [18]byte
	blockHexBuf[0] = '0'
	blockHexBuf[1] = 'x'

	// Write block number as hex
	blockLen := 2 + WriteHex64(blockHexBuf[2:], latestBlock)
	blockHex := blockHexBuf[:blockLen]

	// Pre-allocate data buffer: 0x + 64 hex chars (reserve0) + 64 hex chars (reserve1) = 130 chars
	var dataBuffer [130]byte
	dataBuffer[0] = '0'
	dataBuffer[1] = 'x'

	// Static values that never change - allocate once outside the loop
	staticLogIdx := []byte("0x0")
	staticTxIndex := []byte("0x0")
	staticTopics := []byte(SyncEventSignature)

	// Create LogView once outside the loop and reuse it
	var v types.LogView

	// Set static fields once
	v.LogIdx = staticLogIdx
	v.TxIndex = staticTxIndex
	v.Topics = staticTopics
	v.BlkNum = blockHex
	v.Data = dataBuffer[:]

	// Query all reserves - no ORDER BY for better performance
	rows, err := db.Query(`SELECT pair_address, reserve0, reserve1 FROM pair_reserves`)
	if err != nil {
		return fmt.Errorf("failed to query reserves: %w", err)
	}
	defer rows.Close()

	flushedCount := 0

	// Pre-declare variables outside loop for reuse
	var pairAddress, reserve0Hex, reserve1Hex string
	var writePos, srcLen int
	var srcPtr *byte
	var dstPtr unsafe.Pointer
	const zeros = uint64(0x3030303030303030)

	for rows.Next() {
		rows.Scan(&pairAddress, &reserve0Hex, &reserve1Hex)

		// Zero only the data ranges: 34:66 (reserve0) and 98:130 (reserve1)
		// Each range is 32 bytes = 4 uint64 writes
		*(*uint64)(unsafe.Pointer(&dataBuffer[34])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[42])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[50])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[58])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[98])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[106])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[114])) = zeros
		*(*uint64)(unsafe.Pointer(&dataBuffer[122])) = zeros

		// Write reserve0 right-aligned in positions 2-65 (64 chars)
		srcLen = len(reserve0Hex)
		writePos = 66 - srcLen
		srcPtr = unsafe.StringData(reserve0Hex)
		dstPtr = unsafe.Pointer(&dataBuffer[writePos])

		// Write 8 bytes at a time
		for i := 0; i < srcLen/8; i++ {
			*(*uint64)(unsafe.Add(dstPtr, i*8)) = *(*uint64)(unsafe.Add(unsafe.Pointer(srcPtr), i*8))
		}
		// Write remaining bytes
		for i := srcLen &^ 7; i < srcLen; i++ {
			*(*byte)(unsafe.Add(dstPtr, i)) = *(*byte)(unsafe.Add(unsafe.Pointer(srcPtr), i))
		}

		// Write reserve1 right-aligned in positions 66-129 (64 chars)
		srcLen = len(reserve1Hex)
		writePos = 130 - srcLen
		srcPtr = unsafe.StringData(reserve1Hex)
		dstPtr = unsafe.Pointer(&dataBuffer[writePos])

		// Write 8 bytes at a time
		for i := 0; i < srcLen/8; i++ {
			*(*uint64)(unsafe.Add(dstPtr, i*8)) = *(*uint64)(unsafe.Add(unsafe.Pointer(srcPtr), i*8))
		}
		// Write remaining bytes
		for i := srcLen &^ 7; i < srcLen; i++ {
			*(*byte)(unsafe.Add(dstPtr, i)) = *(*byte)(unsafe.Add(unsafe.Pointer(srcPtr), i))
		}

		// Update address and dispatch
		v.Addr = unsafe.Slice(unsafe.StringData(pairAddress), len(pairAddress))
		router.DispatchPriceUpdate(&v)
		flushedCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating reserves: %w", err)
	}

	debug.DropMessage("FLUSH", utils.Itoa(flushedCount)+" reserves to router")
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS PROCESSING INFRASTRUCTURE (from router.go)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// LookupPairByAddress performs address resolution using Robin Hood hashing
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) LookupPairByAddress(address40HexBytes []byte) TradingPairID {
	// Convert the hex address string to an optimized packed representation for comparison
	key := packEthereumAddress(address40HexBytes)

	// Calculate the initial position in the hash table using the address hash
	i := hashAddressToIndex(address40HexBytes)
	dist := uint64(0) // Track how far we've probed from the ideal position

	// Robin Hood hash table lookup with early termination
	for {
		// Get the current entry at this table position
		currentPairID := addressToPairMap[i]
		currentKey := packedAddressKeys[i]

		// Compare all three 64-bit words of the packed address simultaneously
		// If keyDiff is zero, all words match and we found our target address
		keyDiff := (key.words[0] ^ currentKey.words[0]) |
			(key.words[1] ^ currentKey.words[1]) |
			(key.words[2] ^ currentKey.words[2])

		// Check termination conditions for the search
		if currentPairID == 0 {
			// Empty slot encountered - our address is not in the table
			return 0
		}
		if keyDiff == 0 {
			// Exact address match found - return the associated trading pair ID
			return currentPairID
		}

		// Robin Hood early termination check
		// If the current entry has traveled less distance than us, our key cannot be in the table
		currentKeyHash := hashPackedAddressToIndex(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)
		if currentDist < dist {
			// The current entry is closer to its ideal position than we are to ours
			// This violates the Robin Hood invariant, so our key is not present
			return 0
		}

		// Continue probing to the next slot in the hash table
		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++ // Increment our probe distance
	}
}

// packEthereumAddress converts hex address strings to optimized internal representation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func packEthereumAddress(address40HexChars []byte) PackedAddress {
	// Parse the 40-character hex string into a 20-byte binary address
	parsedAddress := utils.ParseEthereumAddress(address40HexChars)

	// Pack the 20-byte address into three 64-bit words for efficient comparison
	word0 := utils.Load64(parsedAddress[0:8])                       // Load first 8 bytes
	word1 := utils.Load64(parsedAddress[8:16])                      // Load middle 8 bytes
	word2 := uint64(*(*uint32)(unsafe.Pointer(&parsedAddress[16]))) // Load last 4 bytes as uint64

	return PackedAddress{
		words: [3]uint64{word0, word1, word2},
	}
}

// hashAddressToIndex computes hash table indices from raw hex addresses
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func hashAddressToIndex(address40HexChars []byte) uint64 {
	// Parse the last 16 hex characters of the address as a 64-bit hash value
	// This provides good distribution across the hash table
	hash64 := utils.ParseHexU64(address40HexChars[24:40])

	// Mask the hash to fit within the hash table bounds
	return hash64 & uint64(constants.AddressTableMask)
}

// hashPackedAddressToIndex computes hash values from stored PackedAddress structures
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func hashPackedAddressToIndex(key PackedAddress) uint64 {
	// Use the third word as the hash value since it contains the address suffix
	// Mask it to fit within the hash table bounds
	return key.words[2] & uint64(constants.AddressTableMask)
}

// RegisterTradingPairAddress populates the Robin Hood hash table with address-to-pair mappings
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterTradingPairAddress(address42HexBytes []byte, pairID TradingPairID) {
	// Convert the hex address to packed format for efficient storage and comparison
	key := packEthereumAddress(address42HexBytes[2:]) // Skip "0x" prefix

	// Calculate the initial position in the hash table
	i := hashAddressToIndex(address42HexBytes[2:])
	dist := uint64(0) // Track how far we've moved from the ideal position

	// Robin Hood hash table insertion with displacement
	for {
		currentPairID := addressToPairMap[i]

		// If we find an empty slot, insert our entry here
		if currentPairID == 0 {
			packedAddressKeys[i] = key
			addressToPairMap[i] = pairID
			return
		}

		// If we find an existing entry with the same key, update it
		keyDiff := (key.words[0] ^ packedAddressKeys[i].words[0]) |
			(key.words[1] ^ packedAddressKeys[i].words[1]) |
			(key.words[2] ^ packedAddressKeys[i].words[2])
		if keyDiff == 0 {
			addressToPairMap[i] = pairID // Update existing entry
			return
		}

		// Robin Hood displacement: check if we should displace the current entry
		// If the current entry has traveled less distance than us, we displace it
		currentKey := packedAddressKeys[i]
		currentKeyHash := hashPackedAddressToIndex(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)

		if currentDist < dist {
			// Displace the current entry (it's closer to its ideal position than we are)
			// Swap our entry with the current entry and continue inserting the displaced entry
			key, packedAddressKeys[i] = packedAddressKeys[i], key
			pairID, addressToPairMap[i] = addressToPairMap[i], pairID
			dist = currentDist
		}

		// Move to the next slot and increment our distance
		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++
	}
}

// WriteHex64 writes uint64 as hex characters without leading zeros
// Returns the number of hex characters written (1-16)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func WriteHex64(dst []byte, v uint64) int {
	const hex = "0123456789abcdef"

	if v == 0 {
		dst[0] = '0'
		return 1
	}

	// Count nibbles
	nibbles := 0
	temp := v
	for temp > 0 {
		nibbles++
		temp >>= 4
	}

	// Write hex chars from most significant to least
	for i := nibbles - 1; i >= 0; i-- {
		dst[i] = hex[v&0xF]
		v >>= 4
	}

	return nibbles
}

// WriteHex32 writes uint32 as hex characters without leading zeros
// Returns the number of hex characters written (1-8)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func WriteHex32(dst []byte, v uint32) int {
	const hex = "0123456789abcdef"

	if v == 0 {
		dst[0] = '0'
		return 1
	}

	// Count nibbles
	nibbles := 0
	temp := v
	for temp > 0 {
		nibbles++
		temp >>= 4
	}

	// Write hex chars from most significant to least
	for i := nibbles - 1; i >= 0; i-- {
		dst[i] = hex[v&0xF]
		v >>= 4
	}

	return nibbles
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ExecutePeakSync runs the synchronization process (deprecated - use ExecutePeakSyncWithDB)
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func ExecutePeakSync() error {
	debug.DropMessage("EXEC", "Starting sync (opening new DB connection)")

	// Open pairs database
	pairsDB, err := sql.Open("sqlite3", "uniswap_pairs.db")
	if err != nil {
		return fmt.Errorf("failed to open pairs database: %w", err)
	}
	defer pairsDB.Close()

	harvester, err := NewPeakHarvester(pairsDB)
	if err != nil {
		return fmt.Errorf("failed to create harvester: %w", err)
	}

	return harvester.SyncToLatestAndTerminate()
}

// ExecutePeakSyncWithDB runs the synchronization process with provided database connection
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func ExecutePeakSyncWithDB(existingPairsDB *sql.DB) error {
	debug.DropMessage("EXEC", "Starting sync with existing DB")

	harvester, err := NewPeakHarvester(existingPairsDB)
	if err != nil {
		return fmt.Errorf("failed to create harvester: %w", err)
	}

	return harvester.SyncToLatestAndTerminate()
}

// CheckIfPeakSyncNeeded determines if synchronization is required
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func CheckIfPeakSyncNeeded() (bool, uint64, uint64, error) {
	db, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		debug.DropMessage("DB_ERROR", err.Error())
		return true, 0, 0, nil
	}
	defer db.Close()

	var lastBlock uint64
	db.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&lastBlock)

	if lastBlock == 0 {
		return true, 0, 0, nil
	}

	// Get current head
	rpcURL := RPCScheme + "://" + constants.WsHost + RPCProjectPath
	client := NewRPCClient(rpcURL)
	currentHead, err := client.BlockNumber(context.Background())
	if err != nil {
		return true, lastBlock, 0, err
	}

	syncTarget := currentHead - SyncTargetOffset
	syncNeeded := lastBlock < syncTarget

	debug.DropMessage("CHECK", fmt.Sprintf(
		"Last: %s, Target: %s, Current: %s, Needed: %v",
		utils.Itoa(int(lastBlock)), utils.Itoa(int(syncTarget)), utils.Itoa(int(currentHead)), syncNeeded,
	))

	return syncNeeded, lastBlock, syncTarget, nil
}
