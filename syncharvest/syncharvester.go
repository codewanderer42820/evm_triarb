// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ DETACHED SYNC HARVESTER - PEAK SINGLE-CORE PERFORMANCE
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Ultra-Optimized Single-Core Bootstrap System
//
// Description:
//   Peak performance bootstrap system optimized for single-core execution.
//   Eliminates all unnecessary synchronization, atomics, and goroutines.
//   Direct database writes with no ring buffers or inter-goroutine communication.
//
// Performance Characteristics:
//   - Single-threaded: No goroutines, channels, or synchronization overhead
//   - Direct writes: Database operations on main thread for maximum cache locality
//   - Zero allocation: Pre-allocated buffers and structures
//   - Inline processing: No function call overhead in hot paths
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
// CONFIGURATION - PEAK PERFORMANCE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// AGGRESSIVE BOOTSTRAP SETTINGS
	MaxBatchSize     = uint64(15_000) // Larger batches for fewer RPC calls
	MinBatchSize     = uint64(2_000)  // Minimum when network issues occur
	OptimalBatchSize = uint64(8_000)  // Target batch size for optimal performance

	// TERMINATION SETTINGS
	SyncTargetOffset = 50            // Blocks behind head to consider "synced"
	MaxSyncTime      = 4 * time.Hour // Maximum time to spend syncing

	// PERFORMANCE TUNING
	CommitBatchSize = 50_000 // Events per database commit (larger batches)

	// DATABASE SETTINGS
	ReservesDBPath = "uniswap_v2_reserves.db"

	// SYNC EVENT CONFIGURATION
	SyncEventSignature = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PEAK PERFORMANCE HARVESTER - SINGLE THREADED
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type PeakHarvester struct {
	// RPC client
	rpcClient *RPCClient

	// Database connections
	pairsDB    *sql.DB
	reservesDB *sql.DB

	// Pair mapping cache
	pairMap map[string]int64

	// State tracking (no atomics needed - single threaded)
	syncTarget    uint64
	lastProcessed uint64
	processed     int64
	errors        int64

	// Shutdown coordination
	ctx    context.Context
	cancel context.CancelFunc

	// Signal handling
	signalChan chan os.Signal

	// Performance monitoring
	startTime  time.Time
	lastCommit time.Time

	// Pre-allocated buffers for zero allocation
	eventBuffer   []SyncEvent
	reserveBuffer [2]*big.Int

	// Database transaction for batching
	currentTx     *sql.Tx
	eventsInBatch int

	// Prepared statements for peak performance
	insertEventStmt    *sql.Stmt
	updateReservesStmt *sql.Stmt
	updateSyncStmt     *sql.Stmt
}

// SyncEvent represents a parsed sync event
type SyncEvent struct {
	PairID      int64
	PairAddr    string
	BlockNumber uint64
	TxHash      string
	LogIndex    uint64
	Reserve0    string
	Reserve1    string
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RPC CLIENT - OPTIMIZED FOR SINGLE CORE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type RPCClient struct {
	url    string
	client *http.Client
}

type RPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type RPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *RPCError       `json:"error"`
	ID      int             `json:"id"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type Log struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber string   `json:"blockNumber"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"`
}

//go:inline
func NewRPCClient(url string) *RPCClient {
	return &RPCClient{
		url:    url,
		client: &http.Client{
			// NO TIMEOUT - will wait forever until success
		},
	}
}

//go:inline
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

	// NO TIMEOUT - create request without timeout context
	httpReq, err := http.NewRequest("POST", c.url, strings.NewReader(string(data)))
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

//go:inline
func (c *RPCClient) BlockNumber(ctx context.Context) (uint64, error) {
	var result string
	if err := c.Call(ctx, &result, "eth_blockNumber"); err != nil {
		return 0, err
	}
	return parseHexUint64(result)
}

//go:inline
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
// CONSTRUCTOR - PEAK INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func NewPeakHarvester() (*PeakHarvester, error) {
	debug.DropMessage("PEAK_INIT", "Initializing peak performance harvester")

	// Pin to single core for maximum cache locality
	runtime.GOMAXPROCS(1)
	runtime.LockOSThread()

	// Create context for clean shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Use HTTP RPC endpoint
	rpcURL := fmt.Sprintf("https://%s/v3/a2a3139d2ab24d59bed2dc3643664126", constants.WsHost)
	rpcClient := NewRPCClient(rpcURL)

	// Open databases
	pairsDB, err := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to open pairs database: %w", err)
	}

	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		cancel()
		pairsDB.Close()
		return nil, fmt.Errorf("failed to open reserves database: %w", err)
	}

	// Configure database for maximum write performance
	if err := configureDatabase(reservesDB); err != nil {
		cancel()
		pairsDB.Close()
		reservesDB.Close()
		return nil, fmt.Errorf("failed to configure database: %w", err)
	}

	// Create the peak harvester
	h := &PeakHarvester{
		rpcClient:  rpcClient,
		pairsDB:    pairsDB,
		reservesDB: reservesDB,
		pairMap:    make(map[string]int64),
		ctx:        ctx,
		cancel:     cancel,
		signalChan: make(chan os.Signal, 1),
		startTime:  time.Now(),
		lastCommit: time.Now(),

		// Pre-allocate buffers to avoid allocations during processing
		eventBuffer:   make([]SyncEvent, 0, CommitBatchSize),
		reserveBuffer: [2]*big.Int{big.NewInt(0), big.NewInt(0)},
	}

	// Set up signal handling
	h.setupSignalHandling()

	// Initialize database schema
	if err := h.initializeSchema(); err != nil {
		h.cleanup()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Load pair mappings
	if err := h.loadPairMappings(); err != nil {
		h.cleanup()
		return nil, fmt.Errorf("failed to load pair mappings: %w", err)
	}

	// Prepare statements for maximum performance
	if err := h.prepareStatements(); err != nil {
		h.cleanup()
		return nil, fmt.Errorf("failed to prepare statements: %w", err)
	}

	debug.DropMessage("PEAK_READY", fmt.Sprintf("Peak harvester initialized with %d pairs", len(h.pairMap)))
	return h, nil
}

//go:inline
func (h *PeakHarvester) setupSignalHandling() {
	signal.Notify(h.signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Single goroutine for signal handling - minimal overhead
	go func() {
		<-h.signalChan
		debug.DropMessage("SIGNAL_RECEIVED", "Received shutdown signal")
		h.cancel()
	}()
}

//go:inline
func configureDatabase(db *sql.DB) error {
	// Ultra-aggressive database settings for peak write performance
	optimizations := []string{
		"PRAGMA journal_mode = OFF",  // No journaling for maximum speed
		"PRAGMA synchronous = OFF",   // No sync for bootstrap
		"PRAGMA cache_size = 200000", // Very large cache
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 4294967296",   // 4GB mmap
		"PRAGMA page_size = 65536",        // Large pages
		"PRAGMA auto_vacuum = NONE",       // No auto vacuum overhead
		"PRAGMA locking_mode = EXCLUSIVE", // Exclusive access
	}

	for _, pragma := range optimizations {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("failed to execute %s: %w", pragma, err)
		}
	}

	return nil
}

//go:inline
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

//go:inline
func (h *PeakHarvester) loadPairMappings() error {
	debug.DropMessage("PAIR_LOADING", "Loading Uniswap V2 pairs")

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

		addr = strings.ToLower(addr)
		h.pairMap[addr] = id
		count++
	}

	debug.DropMessage("PAIRS_LOADED", fmt.Sprintf("Loaded %d pairs", count))
	return rows.Err()
}

//go:inline
func (h *PeakHarvester) prepareStatements() error {
	var err error

	h.insertEventStmt, err = h.reservesDB.Prepare(`
		INSERT OR IGNORE INTO sync_events 
		(pair_id, block_number, tx_hash, log_index, reserve0, reserve1, created_at) 
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert event statement: %w", err)
	}

	h.updateReservesStmt, err = h.reservesDB.Prepare(`
		INSERT OR REPLACE INTO pair_reserves 
		(pair_id, pair_address, reserve0, reserve1, block_height, last_updated) 
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare update reserves statement: %w", err)
	}

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
// PEAK SYNC EXECUTION - SINGLE THREADED
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:inline
func (h *PeakHarvester) SyncToLatestAndTerminate() error {
	debug.DropMessage("PEAK_SYNC_START", "Starting peak performance synchronization")

	// Determine sync target with infinite retry
	var currentHead uint64
	var err error
	retryCount := 0

	for {
		currentHead, err = h.rpcClient.BlockNumber(h.ctx)
		if err == nil {
			break
		}

		retryCount++
		debug.DropMessage("TARGET_RETRY", fmt.Sprintf("Target retrieval attempt %d failed: %v", retryCount, err))

		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}

	h.syncTarget = currentHead - SyncTargetOffset
	debug.DropMessage("SYNC_TARGET", fmt.Sprintf("Target: %d (head: %d)", h.syncTarget, currentHead))

	// Get starting block
	startBlock := h.getLastProcessedBlock()
	if startBlock == 0 {
		startBlock = 10000835 // Uniswap V2 factory deployment
	}

	// Check if already synced
	if startBlock >= h.syncTarget {
		debug.DropMessage("SYNC_CURRENT", "Already synchronized")
		return h.terminateCleanly()
	}

	// Begin database transaction for the entire sync
	h.beginTransaction()

	blocksToSync := h.syncTarget - startBlock
	debug.DropMessage("SYNC_SCOPE", fmt.Sprintf("Syncing %d blocks from %d to %d", blocksToSync, startBlock, h.syncTarget))

	// Execute sync loop
	err = h.executePeakSyncLoop(startBlock)
	if err != nil {
		debug.DropMessage("SYNC_ERROR", fmt.Sprintf("Sync failed: %v", err))
		h.rollbackTransaction()
		return err
	}

	// Commit final transaction
	h.commitTransaction()

	return h.terminateCleanly()
}

//go:inline
func (h *PeakHarvester) executePeakSyncLoop(startBlock uint64) error {
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

		// Process batch with infinite retry
		h.processPeakBatch(current, batchEnd)

		// Update progress
		h.lastProcessed = batchEnd
		current = batchEnd + 1

		// Commit periodically for memory management
		if h.eventsInBatch >= CommitBatchSize {
			h.commitTransaction()
			h.beginTransaction()
		}

		// Increase batch size for better throughput
		if batchSize < MaxBatchSize {
			batchSize = minUint64(MaxBatchSize, batchSize+500)
		}

		// Progress reporting
		if current%10000 == 0 {
			h.reportProgress()
		}
	}

	debug.DropMessage("SYNC_COMPLETE", fmt.Sprintf("Synced to block %d", current))
	return nil
}

//go:inline
func (h *PeakHarvester) processPeakBatch(fromBlock, toBlock uint64) {
	debug.DropMessage("BATCH_START", fmt.Sprintf("Processing blocks %d-%d", fromBlock, toBlock))

	var logs []Log
	var err error
	retryCount := 0

	// Keep retrying until successful
	for {
		logs, err = h.rpcClient.GetLogs(h.ctx, fromBlock, toBlock, []string{}, []string{SyncEventSignature})
		if err == nil {
			break
		}

		retryCount++
		debug.DropMessage("RPC_RETRY", fmt.Sprintf("RPC attempt %d failed: %v", retryCount, err))

		select {
		case <-h.ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}

	debug.DropMessage("RPC_SUCCESS", fmt.Sprintf("Fetched %d events", len(logs)))

	// Process logs directly - no goroutines or channels
	processedCount := 0
	for i := range logs {
		if h.processLogDirect(&logs[i]) {
			processedCount++
		}
	}

	h.processed += int64(processedCount)
	debug.DropMessage("BATCH_COMPLETE", fmt.Sprintf("Processed %d events", processedCount))
}

//go:inline
func (h *PeakHarvester) processLogDirect(log *Log) bool {
	// Validate sync event
	if len(log.Topics) == 0 || log.Topics[0] != SyncEventSignature {
		return false
	}

	// Parse fields
	blockNum, err := parseHexUint64(log.BlockNumber)
	if err != nil {
		return false
	}

	logIndex, err := parseHexUint64(log.LogIndex)
	if err != nil {
		return false
	}

	// Check if this is a known pair
	pairAddr := strings.ToLower(log.Address)
	pairID, exists := h.pairMap[pairAddr]
	if !exists {
		return false
	}

	// Parse reserves using pre-allocated buffers
	if !h.parseReservesDirect(log.Data) {
		return false
	}

	// Write directly to database - no buffering
	now := time.Now().Unix()

	// Insert sync event
	_, err = h.insertEventStmt.Exec(
		pairID, blockNum, log.TxHash, logIndex,
		h.reserveBuffer[0].String(), h.reserveBuffer[1].String(), now,
	)
	if err != nil {
		debug.DropMessage("DB_ERROR", fmt.Sprintf("Insert failed: %v", err))
		return false
	}

	// Update pair reserves
	_, err = h.updateReservesStmt.Exec(
		pairID, pairAddr,
		h.reserveBuffer[0].String(), h.reserveBuffer[1].String(),
		blockNum, now,
	)
	if err != nil {
		debug.DropMessage("DB_ERROR", fmt.Sprintf("Update failed: %v", err))
		return false
	}

	h.eventsInBatch++
	return true
}

//go:inline
func (h *PeakHarvester) parseReservesDirect(dataStr string) bool {
	dataStr = strings.TrimPrefix(dataStr, "0x")
	if len(dataStr) != 128 {
		return false
	}

	data, err := hex.DecodeString(dataStr)
	if err != nil {
		return false
	}

	// Reuse pre-allocated big.Int buffers
	h.reserveBuffer[0].SetBytes(data[:32])
	h.reserveBuffer[1].SetBytes(data[32:64])

	return true
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TRANSACTION MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:inline
func (h *PeakHarvester) beginTransaction() {
	var err error
	h.currentTx, err = h.reservesDB.Begin()
	if err != nil {
		debug.DropMessage("TX_ERROR", fmt.Sprintf("Failed to begin transaction: %v", err))
		return
	}
	h.eventsInBatch = 0
}

//go:inline
func (h *PeakHarvester) commitTransaction() {
	if h.currentTx != nil {
		err := h.currentTx.Commit()
		if err != nil {
			debug.DropMessage("TX_ERROR", fmt.Sprintf("Failed to commit transaction: %v", err))
		} else {
			debug.DropMessage("TX_COMMIT", fmt.Sprintf("Committed %d events", h.eventsInBatch))
		}
		h.currentTx = nil
	}
	h.lastCommit = time.Now()
}

//go:inline
func (h *PeakHarvester) rollbackTransaction() {
	if h.currentTx != nil {
		h.currentTx.Rollback()
		h.currentTx = nil
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MONITORING AND CLEANUP
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:inline
func (h *PeakHarvester) reportProgress() {
	elapsed := time.Since(h.startTime)
	eventsPerSecond := float64(h.processed) / elapsed.Seconds()
	blocksPerSecond := float64(h.lastProcessed) / elapsed.Seconds()

	debug.DropMessage("PEAK_PROGRESS", fmt.Sprintf(
		"Block: %d, Events: %d (%.1f/s), Blocks: %.1f/s, Elapsed: %v",
		h.lastProcessed, h.processed, eventsPerSecond, blocksPerSecond, elapsed.Round(time.Second),
	))

	// Update sync metadata
	now := time.Now().Unix()
	h.updateSyncStmt.Exec(h.lastProcessed, h.syncTarget, "running", now, h.processed)
}

//go:inline
func (h *PeakHarvester) getLastProcessedBlock() uint64 {
	var lastBlock uint64
	err := h.reservesDB.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&lastBlock)
	if err != nil {
		return 0
	}
	return lastBlock
}

//go:inline
func (h *PeakHarvester) terminateCleanly() error {
	debug.DropMessage("PEAK_TERMINATION", "Beginning clean termination")

	// Commit any pending transaction
	h.commitTransaction()

	// Final metadata update
	now := time.Now().Unix()
	h.updateSyncStmt.Exec(h.lastProcessed, h.syncTarget, "completed", now, h.processed)

	// Close prepared statements
	if h.insertEventStmt != nil {
		h.insertEventStmt.Close()
	}
	if h.updateReservesStmt != nil {
		h.updateReservesStmt.Close()
	}
	if h.updateSyncStmt != nil {
		h.updateSyncStmt.Close()
	}

	// Final optimization
	h.reservesDB.Exec("PRAGMA optimize")

	h.cleanup()

	elapsed := time.Since(h.startTime)
	debug.DropMessage("PEAK_COMPLETE", fmt.Sprintf(
		"Peak sync completed: %d events, %d blocks in %v (%.1f events/sec)",
		h.processed, h.lastProcessed, elapsed.Round(time.Second),
		float64(h.processed)/elapsed.Seconds(),
	))

	return nil
}

//go:inline
func (h *PeakHarvester) cleanup() {
	if h.reservesDB != nil {
		h.reservesDB.Close()
		h.reservesDB = nil
	}
	if h.pairsDB != nil {
		h.pairsDB.Close()
		h.pairsDB = nil
	}

	h.pairMap = nil
	h.eventBuffer = nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:inline
func parseHexUint64(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, fmt.Errorf("empty hex string")
	}
	return utils.ParseHexU64([]byte(s)), nil
}

//go:inline
func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API - PEAK PERFORMANCE EXECUTION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ExecutePeakSync runs peak performance single-core sync
func ExecutePeakSync() error {
	debug.DropMessage("PEAK_EXECUTION", "Starting peak performance sync")

	harvester, err := NewPeakHarvester()
	if err != nil {
		return fmt.Errorf("failed to create peak harvester: %w", err)
	}

	return harvester.SyncToLatestAndTerminate()
}

// CheckIfPeakSyncNeeded determines if sync is required (peak version)
func CheckIfPeakSyncNeeded() (bool, uint64, uint64, error) {
	db, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		debug.DropMessage("PEAK_CHECK_DB_ERROR", fmt.Sprintf("Cannot open DB: %v", err))
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

	// Get current head with infinite retry - NO TIMEOUTS
	rpcURL := fmt.Sprintf("https://%s/v3/a2a3139d2ab24d59bed2dc3643664126", constants.WsHost)
	client := NewRPCClient(rpcURL)

	var currentHead uint64
	retryCount := 0

	for {
		// NO TIMEOUT CONTEXT - will wait forever
		currentHead, err = client.BlockNumber(context.Background())

		if err == nil {
			break
		}

		retryCount++
		debug.DropMessage("PEAK_CHECK_RETRY", fmt.Sprintf("Check attempt %d failed: %v", retryCount, err))
		time.Sleep(5 * time.Second)
	}

	syncTarget := currentHead - SyncTargetOffset
	syncNeeded := lastBlock < syncTarget

	debug.DropMessage("PEAK_CHECK_RESULT", fmt.Sprintf(
		"Last: %d, Target: %d, Current: %d, Sync needed: %v",
		lastBlock, syncTarget, currentHead, syncNeeded,
	))

	return syncNeeded, lastBlock, syncTarget, nil
}
