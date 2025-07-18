// ════════════════════════════════════════════════════════════════════════════════════════════════
// Single-Core Sync Harvester
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Blockchain Event Synchronization Engine
//
// Description:
//   Single-threaded synchronization system for Ethereum event processing with direct database
//   writes and zero-allocation buffer management.
//
// Features:
//   - Single-core execution with thread affinity
//   - Direct database operations without intermediate buffering
//   - Pre-allocated memory structures for zero-allocation operation
//   - Adaptive batch sizing based on network conditions
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvest

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// REQUIRED IMPORTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"main/constants"
	"main/debug"
	"main/utils"

	_ "github.com/mattn/go-sqlite3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONFIGURATION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONFIGURATION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Synchronization parameters
	OptimalBatchSize = uint64(10_000) // Initial batch size for block processing
	SyncTargetOffset = 50             // Blocks behind head to consider synchronized
	MaxSyncTime      = 4 * time.Hour  // Maximum synchronization duration

	// Database batch processing
	CommitBatchSize = 50_000 // Events per transaction commit

	// File paths
	ReservesDBPath = "uniswap_v2_reserves.db"
	PairsDBPath    = "uniswap_pairs.db?mode=ro"

	// RPC configuration
	RPCPathTemplate = "https://%s/v3/a2a3139d2ab24d59bed2dc3643664126"

	// Event signatures
	SyncEventSignature = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

	// Buffer sizes
	PreAllocLogSliceSize = 10000
	HexDecodeBufferSize  = 64

	// Uniswap V2 deployment block
	UniswapV2DeploymentBlock = 10000835

	// Batch size limits
	MinBatchSize = 100
	MaxRetries   = 3
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
// RPC CLIENT STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// RPCRequest represents a JSON-RPC request with fields ordered by usage frequency.
//
//go:notinheap
//go:align 32
type RPCRequest struct {
	// HOT: Most accessed during RPC calls
	Method string        `json:"method"` // 16B - RPC method name (every call)
	Params []interface{} `json:"params"` // 24B - Method parameters (every call)

	// WARM: Standard protocol fields
	JSONRPC string `json:"jsonrpc"` // 16B - Protocol version (constant)
	ID      int    `json:"id"`      // 8B - Request ID (incremental)
}

// RPCResponse represents a JSON-RPC response with result data prioritized.
//
//go:notinheap
//go:align 32
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
//go:align 16
type RPCError struct {
	// HOT: Error information (accessed together)
	Code    int    `json:"code"`    // 8B - Error code
	Message string `json:"message"` // 16B - Error message
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
	// CACHE LINE 1: Database operations (64B)
	insertEventStmt    *sql.Stmt   // 8B - Insert statement (used every event)
	updateReservesStmt *sql.Stmt   // 8B - Update statement (used every event)
	currentTx          *sql.Tx     // 8B - Current transaction (managed frequently)
	reserveBuffer      [2]*big.Int // 16B - Pre-allocated buffers (every parse)
	eventsInBatch      int         // 8B - Events in current batch (updated every event)
	processed          int64       // 8B - Total events processed (updated every event)
	lastProcessed      uint64      // 8B - Last processed block (updated every batch)

	// CACHE LINE 2: Database and RPC connections (64B)
	reservesDB      *sql.DB    // 8B - Reserves database (used for all writes)
	pairsDB         *sql.DB    // 8B - Pairs database (used during init and queries)
	rpcClient       *RPCClient // 8B - RPC client (used every batch)
	updateSyncStmt  *sql.Stmt  // 8B - Sync metadata statement (periodic updates)
	syncTarget      uint64     // 8B - Target block for sync (checked every batch)
	hexDecodeBuffer []byte     // 24B - Hex decode buffer (reused every parse)

	// CACHE LINE 3: Lookup structures and buffers (64B)
	pairMap       map[string]int64  // 24B - Pair mapping cache (lookup every event)
	addressIntern map[string]string // 24B - String interning (lookup every event)
	logSlice      []Log             // 24B - Pre-allocated log slice (reused every batch)
	_             [16]byte          // 16B - Padding to cache boundary

	// CACHE LINE 4: Batch adaptation and timing (64B)
	consecutiveSuccesses int       // 8B - Success counter (updated every batch)
	consecutiveFailures  int       // 8B - Failure counter (updated on failures)
	startTime            time.Time // 24B - Start timestamp (set once, read for progress)
	lastCommit           time.Time // 24B - Last commit timestamp (updated every commit)

	// CACHE LINE 5: Context and control (64B)
	ctx        context.Context    // 16B - Cancellation context (checked every batch)
	cancel     context.CancelFunc // 8B - Cancel function (called once)
	signalChan chan os.Signal     // 24B - Signal channel (setup once)
	_          [16]byte           // 16B - Padding to cache boundary
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE PROCESSING FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processLogDirect processes a single log entry with minimal allocation overhead.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) processLogDirect(log *Log) bool {
	// Validate sync event signature
	if len(log.Topics) == 0 || log.Topics[0] != SyncEventSignature {
		return false
	}

	// Parse block number using optimized hex parser
	blockNum := fastParseHexUint64(log.BlockNumber)
	if blockNum == 0 && log.BlockNumber != "0x0" {
		return false
	}

	// Parse log index using optimized hex parser
	logIndex := fastParseHexUint64(log.LogIndex)

	// Check if this is a known pair with interned address lookup
	pairAddr := strings.ToLower(log.Address)
	if internedAddr, exists := h.addressIntern[pairAddr]; exists {
		pairAddr = internedAddr
	}

	pairID, exists := h.pairMap[pairAddr]
	if !exists {
		return false
	}

	// Parse reserves using direct parsing
	if !h.parseReservesDirect(log.Data) {
		return false
	}

	// Ensure prepared statements are available
	if h.insertEventStmt == nil || h.updateReservesStmt == nil {
		debug.DropMessage("STMT_ERROR", "Prepared statements are nil")
		return false
	}

	// Write to database
	now := time.Now().Unix()

	// Insert sync event
	_, err := h.insertEventStmt.Exec(
		pairID, blockNum, log.TxHash, logIndex,
		h.reserveBuffer[0].String(), h.reserveBuffer[1].String(), now,
	)
	if err != nil {
		debug.DropMessage("DB_ERROR", "Insert failed: "+err.Error())
		return false
	}

	// Update pair reserves
	_, err = h.updateReservesStmt.Exec(
		pairID, pairAddr,
		h.reserveBuffer[0].String(), h.reserveBuffer[1].String(),
		blockNum, now,
	)
	if err != nil {
		debug.DropMessage("DB_ERROR", "Update failed: "+err.Error())
		return false
	}

	h.eventsInBatch++
	return true
}

// parseReservesDirect parses reserve data from hex string using pre-allocated buffers.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) parseReservesDirect(dataStr string) bool {
	dataStr = strings.TrimPrefix(dataStr, "0x")
	if len(dataStr) != 128 {
		return false
	}

	// Reuse hex decode buffer if it's large enough
	if cap(h.hexDecodeBuffer) < HexDecodeBufferSize {
		h.hexDecodeBuffer = make([]byte, HexDecodeBufferSize)
	} else {
		h.hexDecodeBuffer = h.hexDecodeBuffer[:HexDecodeBufferSize]
	}

	_, err := hex.Decode(h.hexDecodeBuffer, []byte(dataStr))
	if err != nil {
		return false
	}

	// Reuse pre-allocated big.Int buffers
	h.reserveBuffer[0].SetBytes(h.hexDecodeBuffer[:32])
	h.reserveBuffer[1].SetBytes(h.hexDecodeBuffer[32:64])

	return true
}

// fastParseHexUint64 parses hexadecimal strings to uint64 using optimized operations.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func fastParseHexUint64(s string) uint64 {
	if len(s) > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		s = s[2:]
	}

	if len(s) == 0 || len(s) > 16 {
		return 0
	}

	// Use optimized hex parser from utils
	return utils.ParseHexU64([]byte(s))
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RPC CLIENT IMPLEMENTATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// NewRPCClient creates a new RPC client for blockchain communication.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func NewRPCClient(url string) *RPCClient {
	return &RPCClient{
		url:    url,
		client: &http.Client{
			// No timeout for reliability
		},
	}
}

// Call executes an RPC method with given parameters.
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

	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	// Create request without timeout context
	httpReq, err := http.NewRequest("POST", c.url, strings.NewReader(utils.B2s(data)))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var rpcResp RPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return err
	}

	if rpcResp.Error != nil {
		return fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return json.Unmarshal(rpcResp.Result, result)
}

// BlockNumber retrieves the current block number from the blockchain.
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
	return fastParseHexUint64(result), nil
}

// GetLogs retrieves event logs from the blockchain within a specified block range.
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

// openDatabaseWithRetry opens a database connection with error handling.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func openDatabaseWithRetry(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Test the connection immediately
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("database connection failed: %w", err)
	}

	return db, nil
}

// isDatabaseLocked checks if a database is currently locked by another process.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func isDatabaseLocked(dbPath string) bool {
	// Try to open database with immediate timeout
	testDB, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=100")
	if err != nil {
		return true
	}
	defer testDB.Close()

	// Try a simple query
	_, err = testDB.Exec("PRAGMA schema_version")
	return err != nil
}

// configureDatabase applies optimization settings for write performance.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func configureDatabase(db *sql.DB) error {
	// Database optimization settings
	optimizations := []string{
		"PRAGMA journal_mode = OFF",  // Disable journaling
		"PRAGMA synchronous = OFF",   // Disable sync
		"PRAGMA cache_size = 200000", // Large cache
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 4294967296",   // 4GB mmap
		"PRAGMA page_size = 65536",        // Large pages
		"PRAGMA auto_vacuum = NONE",       // Disable auto vacuum
		"PRAGMA locking_mode = EXCLUSIVE", // Exclusive access
		"PRAGMA busy_timeout = 30000",     // 30 second timeout
		"PRAGMA wal_autocheckpoint = 0",   // Disable WAL checkpoints
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

// NewPeakHarvester creates and initializes a new sync harvester instance.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func NewPeakHarvester() (*PeakHarvester, error) {
	debug.DropMessage("INIT", "Initializing harvester")

	// Lock this goroutine to current OS thread
	runtime.LockOSThread()

	// Create context for clean shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Create RPC client
	rpcURL := fmt.Sprintf(RPCPathTemplate, constants.WsHost)
	rpcClient := NewRPCClient(rpcURL)

	// Open databases
	pairsDB, err := openDatabaseWithRetry(PairsDBPath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to open pairs database: %w", err)
	}

	// Check if reserves database is locked
	if isDatabaseLocked(ReservesDBPath) {
		cancel()
		pairsDB.Close()
		return nil, fmt.Errorf("reserves database locked")
	}

	reservesDB, err := openDatabaseWithRetry(ReservesDBPath)
	if err != nil {
		cancel()
		pairsDB.Close()
		return nil, fmt.Errorf("failed to open reserves database: %w", err)
	}

	// Configure database
	if err := configureDatabase(reservesDB); err != nil {
		cancel()
		pairsDB.Close()
		reservesDB.Close()
		return nil, fmt.Errorf("failed to configure database: %w", err)
	}

	// Create harvester instance
	h := &PeakHarvester{
		// Initialize core fields
		reserveBuffer: [2]*big.Int{big.NewInt(0), big.NewInt(0)},
		processed:     0,
		lastProcessed: 0,
		eventsInBatch: 0,

		// Database connections
		rpcClient:  rpcClient,
		pairsDB:    pairsDB,
		reservesDB: reservesDB,

		// Pre-allocated buffers
		hexDecodeBuffer: make([]byte, HexDecodeBufferSize),
		logSlice:        make([]Log, 0, PreAllocLogSliceSize),

		// Lookup structures
		pairMap:       make(map[string]int64),
		addressIntern: make(map[string]string),

		// Batch adaptation
		consecutiveSuccesses: 0,
		consecutiveFailures:  0,

		// Context and control
		ctx:        ctx,
		cancel:     cancel,
		signalChan: make(chan os.Signal, 1),
		startTime:  time.Now(),
		lastCommit: time.Now(),
	}

	// Set up signal handling
	h.setupSignalHandling()

	// Initialize database schema
	if err := h.initializeSchema(); err != nil {
		h.reservesDB.Close()
		h.pairsDB.Close()
		cancel()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Load pair mappings
	if err := h.loadPairMappings(); err != nil {
		h.reservesDB.Close()
		h.pairsDB.Close()
		cancel()
		return nil, fmt.Errorf("failed to load pair mappings: %w", err)
	}

	// Prepare global statements
	if err := h.prepareGlobalStatements(); err != nil {
		h.reservesDB.Close()
		h.pairsDB.Close()
		cancel()
		return nil, fmt.Errorf("failed to prepare statements: %w", err)
	}

	debug.DropMessage("READY", utils.Itoa(len(h.pairMap))+" pairs")
	return h, nil
}

// setupSignalHandling configures graceful shutdown on system signals.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) setupSignalHandling() {
	signal.Notify(h.signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Single goroutine for signal handling
	go func() {
		sig := <-h.signalChan
		debug.DropMessage("SIGNAL", fmt.Sprintf("Received %v", sig))
		h.cancel()
	}()
}

// initializeSchema creates database tables if they don't exist.
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

// loadPairMappings loads trading pair data from the database.
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
		if err := rows.Scan(&id, &addr); err != nil {
			return fmt.Errorf("failed to scan pair: %w", err)
		}

		// Intern the address string
		addr = strings.ToLower(addr)
		if internedAddr, exists := h.addressIntern[addr]; exists {
			addr = internedAddr
		} else {
			h.addressIntern[addr] = addr
		}

		h.pairMap[addr] = id
		count++
	}

	debug.DropMessage("LOADED", utils.Itoa(count)+" pairs")
	return rows.Err()
}

// prepareGlobalStatements prepares SQL statements for execution.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) prepareGlobalStatements() error {
	var err error

	// Prepare sync metadata statement
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

// SyncToLatestAndTerminate executes the main synchronization loop.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) SyncToLatestAndTerminate() error {
	debug.DropMessage("SYNC", "Starting")

	// Determine sync target
	var currentHead uint64
	var err error
	retryCount := 0

	for {
		currentHead, err = h.rpcClient.BlockNumber(h.ctx)
		if err == nil {
			break
		}

		retryCount++
		debug.DropMessage("RETRY", fmt.Sprintf("Target attempt %d failed", retryCount))

		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		default:
			// Immediate retry
		}
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
	} else {
		if err != nil {
			h.rollbackTransaction()
		} else {
			h.commitTransaction()
		}
	}

	if err != nil {
		if err == context.Canceled {
			debug.DropMessage("CANCELLED", "User cancelled")
		} else {
			debug.DropMessage("ERROR", err.Error())
		}
	}

	return h.terminateCleanly()
}

// executeSyncLoop processes blocks in adaptive batches.
//
//go:norace
//go:nocheckptr
//go:nosplit
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

			if h.consecutiveSuccesses >= 3 {
				oldBatchSize := batchSize
				batchSize *= 2
				h.consecutiveSuccesses = 0
				debug.DropMessage("BATCH+", utils.Itoa(int(oldBatchSize))+"→"+utils.Itoa(int(batchSize)))
			}

			// Update progress and advance
			h.lastProcessed = batchEnd
			current = batchEnd + 1

		} else {
			// Halve batch size on failure
			h.consecutiveSuccesses = 0
			oldBatchSize := batchSize
			batchSize /= 2
			if batchSize < MinBatchSize {
				batchSize = MinBatchSize
			}
			debug.DropMessage("BATCH-", utils.Itoa(int(oldBatchSize))+"→"+utils.Itoa(int(batchSize)))

			// Don't advance on failure - retry same range
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

// processBatch fetches and processes logs for a block range.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) processBatch(fromBlock, toBlock uint64) bool {
	// Reuse pre-allocated slice
	h.logSlice = h.logSlice[:0]

	var err error
	retryCount := 0

	// Retry until successful
	for retryCount < MaxRetries {
		h.logSlice, err = h.rpcClient.GetLogs(h.ctx, fromBlock, toBlock, []string{}, []string{SyncEventSignature})
		if err == nil {
			break
		}

		retryCount++
		debug.DropMessage("RPC", fmt.Sprintf("Retry %d", retryCount))

		select {
		case <-h.ctx.Done():
			return false
		default:
			// Immediate retry
		}
	}

	// Return false to trigger batch size reduction
	if err != nil {
		debug.DropMessage("FAILED", fmt.Sprintf("%d-%d", fromBlock, toBlock))
		return false
	}

	// Process logs
	processedCount := 0
	for i := range h.logSlice {
		if h.processLogDirect(&h.logSlice[i]) {
			processedCount++
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

// beginTransaction starts a new database transaction.
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

	// Prepare statements within transaction context
	h.insertEventStmt, err = h.currentTx.Prepare(`
		INSERT OR IGNORE INTO sync_events 
		(pair_id, block_number, tx_hash, log_index, reserve0, reserve1, created_at) 
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		h.currentTx.Rollback()
		h.currentTx = nil
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}

	h.updateReservesStmt, err = h.currentTx.Prepare(`
		INSERT OR REPLACE INTO pair_reserves 
		(pair_id, pair_address, reserve0, reserve1, block_height, last_updated) 
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		h.insertEventStmt.Close()
		h.insertEventStmt = nil
		h.currentTx.Rollback()
		h.currentTx = nil
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}

	h.eventsInBatch = 0
	return nil
}

// commitTransaction commits the current database transaction.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) commitTransaction() {
	// Close and nil statements immediately
	h.insertEventStmt.Close()
	h.insertEventStmt = nil
	h.updateReservesStmt.Close()
	h.updateReservesStmt = nil

	err := h.currentTx.Commit()
	h.currentTx = nil

	if err != nil {
		debug.DropMessage("TX_ERROR", err.Error())
	} else {
		debug.DropMessage("TX", utils.Itoa(h.eventsInBatch)+" events")
	}
	h.lastCommit = time.Now()
}

// rollbackTransaction rolls back the current database transaction.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) rollbackTransaction() {
	// Close and nil statements immediately
	h.insertEventStmt.Close()
	h.insertEventStmt = nil
	h.updateReservesStmt.Close()
	h.updateReservesStmt = nil

	h.currentTx.Rollback()
	h.currentTx = nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MONITORING AND CLEANUP
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// reportProgress logs synchronization progress information.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) reportProgress() {
	elapsed := time.Since(h.startTime)
	eventsPerSecond := float64(h.processed) / elapsed.Seconds()

	debug.DropMessage("PROGRESS", fmt.Sprintf(
		"Block %d, %d events (%.0f/s)",
		h.lastProcessed, h.processed, eventsPerSecond,
	))

	// Update sync metadata
	if h.updateSyncStmt != nil {
		now := time.Now().Unix()
		h.updateSyncStmt.Exec(h.lastProcessed, h.syncTarget, "running", now, h.processed)
	}
}

// getLastProcessedBlock retrieves the last processed block from the database.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) getLastProcessedBlock() uint64 {
	var lastBlock uint64
	err := h.reservesDB.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&lastBlock)
	if err != nil {
		debug.DropMessage("DB_ERROR", err.Error())
		return 0
	}
	return lastBlock
}

// terminateCleanly performs final cleanup and resource deallocation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (h *PeakHarvester) terminateCleanly() error {
	debug.DropMessage("CLEANUP", "Starting")

	// Commit any pending transaction
	h.commitTransaction()

	// Final metadata update and immediate cleanup
	now := time.Now().Unix()
	h.updateSyncStmt.Exec(h.lastProcessed, h.syncTarget, "completed", now, h.processed)
	h.updateSyncStmt.Close()
	h.updateSyncStmt = nil

	// Final optimization and immediate cleanup
	h.reservesDB.Exec("PRAGMA optimize")
	h.reservesDB.Close()
	h.reservesDB = nil

	h.pairsDB.Close()
	h.pairsDB = nil

	// Nil maps immediately
	h.pairMap = nil
	h.addressIntern = nil

	elapsed := time.Since(h.startTime)
	debug.DropMessage("DONE", fmt.Sprintf(
		"%d events, %d blocks in %v",
		h.processed, h.lastProcessed, elapsed.Round(time.Second),
	))

	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ExecutePeakSync runs the synchronization process.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ExecutePeakSync() error {
	debug.DropMessage("EXEC", "Starting sync")

	harvester, err := NewPeakHarvester()
	if err != nil {
		return fmt.Errorf("failed to create harvester: %w", err)
	}

	return harvester.SyncToLatestAndTerminate()
}

// CheckIfPeakSyncNeeded determines if synchronization is required.
//
//go:norace
//go:nocheckptr
//go:nosplit
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
	err = db.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&lastBlock)
	if err != nil {
		return true, 0, 0, nil
	}

	if lastBlock == 0 {
		return true, 0, 0, nil
	}

	// Get current head
	rpcURL := fmt.Sprintf(RPCPathTemplate, constants.WsHost)
	client := NewRPCClient(rpcURL)

	var currentHead uint64
	retryCount := 0

	for {
		currentHead, err = client.BlockNumber(context.Background())

		if err == nil {
			break
		}

		retryCount++
		debug.DropMessage("CHECK_RETRY", fmt.Sprintf("Attempt %d failed", retryCount))
		// Immediate retry
	}

	syncTarget := currentHead - SyncTargetOffset
	syncNeeded := lastBlock < syncTarget

	debug.DropMessage("CHECK", fmt.Sprintf(
		"Last: %d, Target: %d, Current: %d, Needed: %v",
		lastBlock, syncTarget, currentHead, syncNeeded,
	))

	return syncNeeded, lastBlock, syncTarget, nil
}
