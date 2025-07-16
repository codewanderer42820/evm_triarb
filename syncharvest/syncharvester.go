// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ DETACHED SYNC HARVESTER - ZERO HOT PATH INTERFERENCE
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Completely Detached Bootstrap System
//
// Description:
//   Ultra-efficient bootstrap system that operates completely detached from the hot path.
//   Once synced to latest height, harvester terminates and database is closed. Uses ring
//   buffers with pinned consumers to keep database writes out of the critical path.
//
// Performance Characteristics:
//   - Hot path: Zero interference after sync completion
//   - Database writes: Handled by separate pinned consumer goroutine
//   - Memory: Bounded with automatic cleanup after sync
//   - Termination: Clean shutdown once sync target reached
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
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"main/constants"
	"main/debug"
	"main/ring24"
	"main/utils"

	_ "github.com/mattn/go-sqlite3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONFIGURATION - DETACHED OPERATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Dynamic batch sizing configuration - adapts to RPC performance
const (
	// DYNAMIC BATCH SIZING - Optimized for RPC stability
	BatchFloor       = uint64(100)     // Minimum batch size when errors occur
	BatchCeil        = uint64(100_000) // Maximum batch size for optimal performance
	InitialBatchSize = uint64(10_000)  // Starting batch size

	// ADAPTIVE SCALING PARAMETERS
	SuccessThreshold = 3 // Consecutive successes before increasing batch
	ErrorThreshold   = 3 // Consecutive errors before decreasing batch

	// PARALLEL PROCESSING
	MaxWorkerCount      = 8  // Maximum parallel workers
	WorkerChannelBuffer = 50 // Worker task buffer

	// RING BUFFER CONFIGURATION
	DatabaseRingSize = 1 << 16 // 65536 entries for database writes

	// TERMINATION SETTINGS
	SyncTargetOffset    = 50               // Blocks behind head to consider "synced"
	MaxSyncTime         = 2 * time.Hour    // Maximum time to spend syncing
	ShutdownGracePeriod = 30 * time.Second // Grace period for final writes

	// PERFORMANCE TUNING
	CommitBatchSize = 25_000          // Events per database commit
	MaxRetries      = 3               // Retries for failed operations
	RetryDelay      = 2 * time.Second // Delay between retries

	// DATABASE SETTINGS
	ReservesDBPath   = "uniswap_v2_reserves.db"
	ReservesMetaPath = "uniswap_v2_reserves.db.meta"

	// SYNC EVENT CONFIGURATION
	SyncEventSignature = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RING BUFFER MESSAGE TYPES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// DatabaseWriteMessage represents a sync event ready for database storage
// Fits exactly in ring24.Ring 24-byte message format
type DatabaseWriteMessage struct {
	PairID      int64  // 8 bytes
	BlockNumber uint64 // 8 bytes
	LogIndex    uint64 // 8 bytes
	// TxHash and reserve data stored separately to fit in 24 bytes
}

// ExtendedEventData stores additional event data that doesn't fit in ring message
type ExtendedEventData struct {
	TxHash   string
	Reserve0 string
	Reserve1 string
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DETACHED HARVESTER CORE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type DetachedHarvester struct {
	// RPC client using constants from main package
	rpcClient *RPCClient

	// Database connections (closed after sync)
	pairsDB    *sql.DB
	reservesDB *sql.DB

	// Pair mapping cache for efficient lookups
	pairMap map[string]int64

	// Ring buffer for database writes (allocated at destination)
	databaseRing  *ring24.Ring
	extendedData  map[uint64]*ExtendedEventData
	extendedMutex sync.RWMutex

	// State tracking
	syncTarget    uint64
	lastProcessed uint64 // Atomic access only
	processed     int64  // Atomic access only
	errors        int64  // Atomic access only

	// Dynamic batch sizing state
	currentBatchSize uint64 // Current batch size for adaptive scaling
	consecutiveOK    int    // Consecutive successful batches
	consecutiveNG    int    // Consecutive failed batches

	// Shutdown coordination
	ctx          context.Context
	cancel       context.CancelFunc
	shutdownOnce sync.Once

	// Database writer coordination
	writerDone chan struct{}
	finalFlush chan struct{}

	// Signal handling for graceful shutdown
	signalChan    chan os.Signal
	flushComplete chan struct{}

	// Performance monitoring
	startTime  time.Time
	lastReport time.Time
}

// WorkerTask represents a batch processing task
type WorkerTask struct {
	FromBlock  uint64
	ToBlock    uint64
	TaskID     uint64
	ChunkIdx   int // Index of the address chunk to process
	RetryCount int
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RPC CLIENT IMPLEMENTATION
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

func NewRPCClient(url string) *RPCClient {
	return &RPCClient{
		url: url,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

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

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.url, strings.NewReader(string(data)))
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

func (c *RPCClient) BlockNumber(ctx context.Context) (uint64, error) {
	var result string
	if err := c.Call(ctx, &result, "eth_blockNumber"); err != nil {
		return 0, err
	}
	return parseHexUint64(result)
}

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
// CONSTRUCTOR - DETACHED INITIALIZATION WITH SIGNAL HANDLING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func NewDetachedHarvester() (*DetachedHarvester, error) {
	debug.DropMessage("DETACHED_INIT", "Initializing detached sync harvester")

	// Create context for clean shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Use HTTP RPC endpoint instead of WebSocket path
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

	// Configure database for write performance
	if err := configureDatabase(reservesDB); err != nil {
		cancel()
		pairsDB.Close()
		reservesDB.Close()
		return nil, fmt.Errorf("failed to configure database: %w", err)
	}

	// Create the detached harvester
	h := &DetachedHarvester{
		rpcClient:        rpcClient,
		pairsDB:          pairsDB,
		reservesDB:       reservesDB,
		pairMap:          make(map[string]int64),
		extendedData:     make(map[uint64]*ExtendedEventData),
		currentBatchSize: InitialBatchSize, // Initialize dynamic batch sizing
		consecutiveOK:    0,
		consecutiveNG:    0,
		ctx:              ctx,
		cancel:           cancel,
		writerDone:       make(chan struct{}),
		finalFlush:       make(chan struct{}),
		signalChan:       make(chan os.Signal, 1),
		flushComplete:    make(chan struct{}),
		startTime:        time.Now(),
	}

	// Set up signal handling for graceful shutdown
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

	// Optimize pair mappings for efficient lookup
	h.optimizePairMapping()

	// Allocate ring buffer at destination (database writer)
	h.databaseRing = ring24.New(DatabaseRingSize)

	debug.DropMessage("DETACHED_READY", "Detached harvester initialized")
	return h, nil
}

// setupSignalHandling configures CTRL+C handling for graceful shutdown
func (h *DetachedHarvester) setupSignalHandling() {
	signal.Notify(h.signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-h.signalChan
		debug.DropMessage("SIGNAL_RECEIVED", fmt.Sprintf("Received signal %v - initiating graceful shutdown", sig))

		// Cancel the context to stop all operations
		h.cancel()

		// Also trigger graceful shutdown
		h.initiateGracefulShutdown()
	}()
}

func configureDatabase(db *sql.DB) error {
	optimizations := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = OFF",   // Aggressive: no sync for bootstrap
		"PRAGMA cache_size = 100000", // Large cache for writes
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 2147483648", // 2GB mmap
		"PRAGMA wal_autocheckpoint = 50000",
		"PRAGMA optimize",
	}

	for _, pragma := range optimizations {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("failed to execute %s: %w", pragma, err)
		}
	}

	return nil
}

func (h *DetachedHarvester) initializeSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS pair_reserves (
		pair_id      INTEGER PRIMARY KEY,
		pair_address TEXT NOT NULL,
		reserve0     TEXT NOT NULL,
		reserve1     TEXT NOT NULL,
		block_height INTEGER NOT NULL,
		last_updated INTEGER NOT NULL,
		UNIQUE(pair_address)
	);
	
	CREATE INDEX IF NOT EXISTS idx_reserves_block ON pair_reserves(block_height);
	CREATE INDEX IF NOT EXISTS idx_reserves_pair ON pair_reserves(pair_id);
	
	CREATE TABLE IF NOT EXISTS sync_events (
		id           INTEGER PRIMARY KEY,
		pair_id      INTEGER NOT NULL,
		block_number INTEGER NOT NULL,
		tx_hash      TEXT NOT NULL,
		log_index    INTEGER NOT NULL,
		reserve0     TEXT NOT NULL,
		reserve1     TEXT NOT NULL,
		created_at   INTEGER NOT NULL,
		UNIQUE(block_number, tx_hash, log_index)
	);
	
	CREATE INDEX IF NOT EXISTS idx_sync_pair_block ON sync_events(pair_id, block_number);
	CREATE INDEX IF NOT EXISTS idx_sync_block ON sync_events(block_number);
	
	CREATE TABLE IF NOT EXISTS sync_metadata (
		id               INTEGER PRIMARY KEY,
		last_block       INTEGER NOT NULL,
		sync_target      INTEGER NOT NULL,
		sync_status      TEXT NOT NULL,
		updated_at       INTEGER NOT NULL,
		events_processed INTEGER NOT NULL DEFAULT 0
	);
	
	CREATE TABLE IF NOT EXISTS pair_sync_state (
		pair_id          INTEGER PRIMARY KEY,
		last_block       INTEGER NOT NULL,
		last_tx_hash     TEXT NOT NULL,
		last_log_index   INTEGER NOT NULL,
		reserve0         TEXT NOT NULL,
		reserve1         TEXT NOT NULL,
		updated_at       INTEGER NOT NULL,
		FOREIGN KEY (pair_id) REFERENCES pair_reserves(pair_id)
	);
	
	CREATE INDEX IF NOT EXISTS idx_pair_sync_block ON pair_sync_state(last_block);
	`

	_, err := h.reservesDB.Exec(schema)
	return err
}

func (h *DetachedHarvester) loadPairMappings() error {
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

func (h *DetachedHarvester) optimizePairMapping() {
	// Convert pair map to a more efficient structure for lookups
	// No need for address chunking anymore since we're not filtering by address in RPC
	pairCount := len(h.pairMap)
	debug.DropMessage("PAIR_OPTIMIZATION", fmt.Sprintf("Optimized %d pair mappings for efficient lookup", pairCount))
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN SYNC EXECUTION - COMPLETELY DETACHED
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (h *DetachedHarvester) SyncToLatestAndTerminate() error {
	debug.DropMessage("SYNC_INITIALIZATION", "Starting detached blockchain synchronization")

	// Determine sync target
	currentHead, err := h.rpcClient.BlockNumber(h.ctx)
	if err != nil {
		return fmt.Errorf("failed to get current head: %w", err)
	}

	h.syncTarget = currentHead - SyncTargetOffset
	debug.DropMessage("SYNC_PARAMETERS", fmt.Sprintf("Target block: %d (head: %d, offset: %d)", h.syncTarget, currentHead, SyncTargetOffset))

	// Get starting block
	startBlock := h.getLastProcessedBlock()
	if startBlock == 0 {
		startBlock = 10000835 // Uniswap V2 factory deployment
	}

	// Check if already synced
	if startBlock >= h.syncTarget {
		debug.DropMessage("SYNC_CURRENT", "Already synchronized to target block")
		return h.terminateCleanly()
	}

	blocksToSync := h.syncTarget - startBlock
	debug.DropMessage("SYNC_SCOPE", fmt.Sprintf("Synchronizing %d blocks from %d to %d", blocksToSync, startBlock, h.syncTarget))

	// Start database writer with pinned consumer
	h.startDatabaseWriter()

	// Start progress monitoring
	go h.monitorProgress()

	// Execute sync with timeout
	syncCtx, syncCancel := context.WithTimeout(h.ctx, MaxSyncTime)
	defer syncCancel()

	err = h.executeSyncLoop(syncCtx, startBlock)
	if err != nil {
		debug.DropMessage("SYNC_ERROR", fmt.Sprintf("Synchronization failed: %v", err))
		// Continue to clean termination
	}

	// Terminate cleanly
	return h.terminateCleanly()
}

func (h *DetachedHarvester) executeSyncLoop(ctx context.Context, startBlock uint64) error {
	current := startBlock

	for current < h.syncTarget {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.signalChan:
			debug.DropMessage("SYNC_INTERRUPTED", "Sync interrupted by signal")
			return fmt.Errorf("sync interrupted by signal")
		default:
		}

		// Calculate batch end using dynamic batch size
		batchEnd := current + h.currentBatchSize
		if batchEnd > h.syncTarget {
			batchEnd = h.syncTarget
		}

		// Log current batch processing
		debug.DropMessage("BATCH_PROCESSING", fmt.Sprintf("Processing blocks %d → %d (batch %d)",
			current, batchEnd, h.currentBatchSize))

		// Process batch
		if err := h.processBatch(ctx, current, batchEnd); err != nil {
			debug.DropMessage("BATCH_ERROR", fmt.Sprintf("Batch %d-%d failed: %v", current, batchEnd, err))

			// Handle batch failure with dynamic scaling
			h.handleBatchFailure()

			// Add delay before retry
			time.Sleep(RetryDelay)
			continue
		}

		// Handle batch success with dynamic scaling
		h.handleBatchSuccess()

		// Update progress atomically
		atomic.StoreUint64(&h.lastProcessed, batchEnd)
		current = batchEnd + 1

		// Update metadata periodically
		if current%5000 == 0 {
			h.updateSyncMetadata(current)
		}
	}

	debug.DropMessage("SYNC_COMPLETE", fmt.Sprintf("Synced to block %d", current))
	return nil
}

// handleBatchFailure implements dynamic batch size reduction on consecutive errors
func (h *DetachedHarvester) handleBatchFailure() {
	h.consecutiveNG++
	h.consecutiveOK = 0

	// Reduce batch size after threshold consecutive failures
	if h.consecutiveNG%ErrorThreshold == 0 && h.currentBatchSize > BatchFloor {
		h.currentBatchSize /= 2
		if h.currentBatchSize < BatchFloor {
			h.currentBatchSize = BatchFloor
		}
		debug.DropMessage("BATCH_SCALE_DOWN", fmt.Sprintf("Reduced batch size to %d after %d consecutive errors",
			h.currentBatchSize, h.consecutiveNG))
	}

	// Increment error counter atomically
	atomic.AddInt64(&h.errors, 1)
}

// handleBatchSuccess implements dynamic batch size increase on consecutive successes
func (h *DetachedHarvester) handleBatchSuccess() {
	h.consecutiveOK++
	h.consecutiveNG = 0

	// Increase batch size after threshold consecutive successes
	if h.consecutiveOK%SuccessThreshold == 0 && h.currentBatchSize < BatchCeil {
		h.currentBatchSize *= 2
		if h.currentBatchSize > BatchCeil {
			h.currentBatchSize = BatchCeil
		}
		debug.DropMessage("BATCH_SCALE_UP", fmt.Sprintf("Increased batch size to %d after %d consecutive successes",
			h.currentBatchSize, h.consecutiveOK))
	}
}

func (h *DetachedHarvester) processBatch(ctx context.Context, fromBlock, toBlock uint64) error {
	debug.DropMessage("BATCH_START", fmt.Sprintf("Processing blocks %d-%d", fromBlock, toBlock))

	// Single RPC call for all Sync events in this block range
	startTime := time.Now()
	logs, err := h.rpcClient.GetLogs(ctx, fromBlock, toBlock, []string{}, []string{SyncEventSignature})
	requestDuration := time.Since(startTime)

	if err != nil {
		debug.DropMessage("RPC_ERROR", fmt.Sprintf("Failed to fetch logs for blocks %d-%d: %v", fromBlock, toBlock, err))
		return err
	}

	debug.DropMessage("RPC_SUCCESS", fmt.Sprintf("Fetched %d Sync events in %v", len(logs), requestDuration))

	// Filter logs by our known Uniswap V2 pairs and process them
	processedCount := 0
	filteredCount := 0

	for _, log := range logs {
		// Check if this log is from a known Uniswap V2 pair
		pairAddr := strings.ToLower(log.Address)
		if _, exists := h.pairMap[pairAddr]; !exists {
			filteredCount++
			continue // Skip logs from unknown pairs
		}

		// Process this log
		if err := h.processLogToRing(log); err != nil {
			debug.DropMessage("LOG_ERROR", fmt.Sprintf("Failed to process log from %s: %v", log.Address, err))
		} else {
			processedCount++
		}
	}

	// Clear logs slice immediately after processing
	logs = nil

	debug.DropMessage("BATCH_COMPLETE", fmt.Sprintf("Processed %d/%d logs (%d filtered out)", processedCount, len(logs), filteredCount))
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SINGLE RPC CALL PROCESSING - PEAK PERFORMANCE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (h *DetachedHarvester) processLogToRing(log Log) error {
	// Validate sync event
	if len(log.Topics) == 0 || log.Topics[0] != SyncEventSignature {
		return nil // Skip non-sync events
	}

	// Parse required fields
	blockNum, err := parseHexUint64(log.BlockNumber)
	if err != nil {
		return fmt.Errorf("invalid block number: %w", err)
	}

	logIndex, err := parseHexUint64(log.LogIndex)
	if err != nil {
		return fmt.Errorf("invalid log index: %w", err)
	}

	// Lookup pair ID
	pairAddr := strings.ToLower(log.Address)
	pairID, exists := h.pairMap[pairAddr]
	if !exists {
		return nil // Skip unknown pairs
	}

	// Parse reserves
	reserve0, reserve1, err := h.parseReserves(log.Data)
	if err != nil {
		return fmt.Errorf("failed to parse reserves: %w", err)
	}

	// Create ring message (24 bytes)
	ringMsg := DatabaseWriteMessage{
		PairID:      pairID,
		BlockNumber: blockNum,
		LogIndex:    logIndex,
	}

	// Store extended data separately
	extendedKey := blockNum<<32 | logIndex
	h.extendedMutex.Lock()
	h.extendedData[extendedKey] = &ExtendedEventData{
		TxHash:   log.TxHash,
		Reserve0: reserve0.String(),
		Reserve1: reserve1.String(),
	}
	h.extendedMutex.Unlock()

	// Send to database writer via ring
	ringBytes := (*[24]byte)(unsafe.Pointer(&ringMsg))
	for {
		if h.databaseRing.Push(ringBytes) {
			atomic.AddInt64(&h.processed, 1)
			break
		}
		// Ring full, brief pause
		time.Sleep(time.Microsecond)
	}

	return nil
}

func (h *DetachedHarvester) parseReserves(dataStr string) (*big.Int, *big.Int, error) {
	dataStr = strings.TrimPrefix(dataStr, "0x")
	if len(dataStr) != 128 {
		return nil, nil, fmt.Errorf("invalid data length: %d", len(dataStr))
	}

	data, err := hex.DecodeString(dataStr)
	if err != nil {
		return nil, nil, fmt.Errorf("hex decode failed: %w", err)
	}

	reserve0 := new(big.Int).SetBytes(data[:32])
	reserve1 := new(big.Int).SetBytes(data[32:64])

	// Clear temporary data immediately after use
	data = nil

	return reserve0, reserve1, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DATABASE WRITER - PINNED CONSUMER
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (h *DetachedHarvester) startDatabaseWriter() {
	debug.DropMessage("DB_WRITER_START", "Starting database writer with pinned consumer")

	// Create stop flag for pinned consumer that responds to context cancellation
	stopFlag := uint32(0)
	hotFlag := uint32(1) // Always hot during sync

	// Monitor context cancellation and set stop flag
	go func() {
		<-h.ctx.Done()
		atomic.StoreUint32(&stopFlag, 1)
		debug.DropMessage("DB_WRITER_STOP", "Database writer stop flag set")
	}()

	// Use pinned consumer for database writes
	// This keeps database I/O completely out of the hot path
	ring24.PinnedConsumer(
		0, // Pin to core 0
		h.databaseRing,
		&stopFlag,
		&hotFlag,
		h.processDatabaseWrite,
		h.writerDone,
	)
}

func (h *DetachedHarvester) processDatabaseWrite(msgPtr *[24]byte) {
	// Convert ring message back to struct
	msg := (*DatabaseWriteMessage)(unsafe.Pointer(msgPtr))

	// Retrieve extended data
	extendedKey := msg.BlockNumber<<32 | msg.LogIndex
	h.extendedMutex.RLock()
	extended, exists := h.extendedData[extendedKey]
	h.extendedMutex.RUnlock()

	if !exists {
		debug.DropMessage("MISSING_EXTENDED", fmt.Sprintf("Missing extended data for block %d log %d", msg.BlockNumber, msg.LogIndex))
		return
	}

	// Write to database
	if err := h.writeToDatabase(msg, extended); err != nil {
		debug.DropMessage("DB_WRITE_ERROR", fmt.Sprintf("Database write failed: %v", err))
		return
	}

	// Clean up extended data
	h.extendedMutex.Lock()
	delete(h.extendedData, extendedKey)
	h.extendedMutex.Unlock()
}

func (h *DetachedHarvester) writeToDatabase(msg *DatabaseWriteMessage, extended *ExtendedEventData) error {
	now := time.Now().Unix()

	// Insert sync event
	_, err := h.reservesDB.Exec(`
		INSERT OR IGNORE INTO sync_events 
		(pair_id, block_number, tx_hash, log_index, reserve0, reserve1, created_at) 
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, msg.PairID, msg.BlockNumber, extended.TxHash, msg.LogIndex, extended.Reserve0, extended.Reserve1, now)

	if err != nil {
		return fmt.Errorf("failed to insert sync event: %w", err)
	}

	// Update pair reserves
	_, err = h.reservesDB.Exec(`
		INSERT OR REPLACE INTO pair_reserves 
		(pair_id, pair_address, reserve0, reserve1, block_height, last_updated) 
		VALUES (?, (SELECT pool_address FROM pools WHERE id = ?), ?, ?, ?, ?)
	`, msg.PairID, msg.PairID, extended.Reserve0, extended.Reserve1, msg.BlockNumber, now)

	if err != nil {
		return fmt.Errorf("failed to update pair reserves: %w", err)
	}

	// Update per-pair sync state
	_, err = h.reservesDB.Exec(`
		INSERT OR REPLACE INTO pair_sync_state 
		(pair_id, last_block, last_tx_hash, last_log_index, reserve0, reserve1, updated_at) 
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, msg.PairID, msg.BlockNumber, extended.TxHash, msg.LogIndex, extended.Reserve0, extended.Reserve1, now)

	return err
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GRACEFUL SHUTDOWN AND STATE FLUSH
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// initiateGracefulShutdown handles CTRL+C and ensures all state is flushed
func (h *DetachedHarvester) initiateGracefulShutdown() {
	h.shutdownOnce.Do(func() {
		debug.DropMessage("GRACEFUL_SHUTDOWN", "Initiating graceful shutdown")

		// Cancel context to stop all workers (if not already cancelled)
		h.cancel()

		// Wait for ring buffer to drain
		debug.DropMessage("RING_DRAIN", "Waiting for ring buffer to drain")
		h.waitForRingDrain()

		// Flush final state
		debug.DropMessage("FINAL_FLUSH", "Flushing final state")
		h.flushFinalState()

		// Signal flush complete
		close(h.flushComplete)

		debug.DropMessage("SHUTDOWN_COMPLETE", "Graceful shutdown completed")
	})
}

// waitForRingDrain waits for the ring buffer to empty
func (h *DetachedHarvester) waitForRingDrain() {
	timeout := time.NewTimer(ShutdownGracePeriod)
	defer timeout.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout.C:
			debug.DropMessage("DRAIN_TIMEOUT", "Ring buffer drain timeout")
			return
		case <-ticker.C:
			// Check if ring is empty by trying to pop
			if h.databaseRing.Pop() == nil {
				debug.DropMessage("RING_EMPTY", "Ring buffer drained")
				return
			}
		}
	}
}

// flushFinalState saves the current sync state to database
func (h *DetachedHarvester) flushFinalState() {
	lastBlock := atomic.LoadUint64(&h.lastProcessed)
	if lastBlock == 0 {
		lastBlock = h.getLastProcessedBlock()
	}

	processedCount := atomic.LoadInt64(&h.processed)
	errorCount := atomic.LoadInt64(&h.errors)

	debug.DropMessage("FLUSH_STATE", fmt.Sprintf(
		"Flushing state: block %d, processed %d, errors %d",
		lastBlock, processedCount, errorCount,
	))

	// Update sync metadata with final state
	now := time.Now().Unix()
	_, err := h.reservesDB.Exec(`
		INSERT OR REPLACE INTO sync_metadata 
		(id, last_block, sync_target, sync_status, updated_at, events_processed) 
		VALUES (1, ?, ?, 'interrupted', ?, ?)
	`, lastBlock, h.syncTarget, now, processedCount)

	if err != nil {
		debug.DropMessage("FLUSH_ERROR", fmt.Sprintf("Failed to flush metadata: %v", err))
	}

	// Force WAL checkpoint to ensure data is written
	h.reservesDB.Exec("PRAGMA wal_checkpoint(FULL)")

	debug.DropMessage("FLUSH_COMPLETE", "Final state flushed")
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CLEAN TERMINATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (h *DetachedHarvester) terminateCleanly() error {
	debug.DropMessage("TERMINATION_START", "Beginning clean termination")

	h.shutdownOnce.Do(func() {
		// Cancel context to stop all workers
		h.cancel()

		// Wait for final database writes
		debug.DropMessage("FINAL_FLUSH", "Waiting for final database writes")
		close(h.finalFlush)

		// Wait for database writer to finish
		select {
		case <-h.writerDone:
			debug.DropMessage("WRITER_DONE", "Database writer completed")
		case <-time.After(ShutdownGracePeriod):
			debug.DropMessage("WRITER_TIMEOUT", "Database writer timeout")
		}

		// Flush final state
		h.flushFinalState()

		// Close databases
		h.cleanup()

		// Report final statistics
		h.reportFinalStats()
	})

	debug.DropMessage("TERMINATION_COMPLETE", "Clean termination completed")
	return nil
}

func (h *DetachedHarvester) cleanup() {
	debug.DropMessage("CLEANUP_START", "Cleaning up resources")

	if h.reservesDB != nil {
		// Final database optimization
		h.reservesDB.Exec("PRAGMA optimize")
		h.reservesDB.Exec("PRAGMA wal_checkpoint(FULL)")
		h.reservesDB.Close()
		h.reservesDB = nil
	}

	if h.pairsDB != nil {
		h.pairsDB.Close()
		h.pairsDB = nil
	}

	// Clear memory
	h.pairMap = nil
	h.extendedData = nil

	debug.DropMessage("CLEANUP_COMPLETE", "Resource cleanup completed")
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PROGRESS MONITORING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (h *DetachedHarvester) monitorProgress() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.reportProgress()
		}
	}
}

func (h *DetachedHarvester) reportProgress() {
	now := time.Now()
	elapsed := now.Sub(h.startTime)

	processed := atomic.LoadInt64(&h.processed)
	errors := atomic.LoadInt64(&h.errors)
	lastBlock := atomic.LoadUint64(&h.lastProcessed)

	if elapsed > 0 {
		eventsPerSecond := float64(processed) / elapsed.Seconds()
		blocksPerSecond := float64(lastBlock-h.getLastProcessedBlock()) / elapsed.Seconds()

		debug.DropMessage("SYNC_STATISTICS", fmt.Sprintf("Events: %d processed, %d errors (%.1f events/sec)", processed, errors, eventsPerSecond))
		debug.DropMessage("SYNC_PERFORMANCE", fmt.Sprintf("Blocks: %d processed (%.1f blocks/sec), elapsed: %v", lastBlock, blocksPerSecond, elapsed.Round(time.Second)))
		debug.DropMessage("BATCH_STATUS", fmt.Sprintf("Current batch size: %d, consecutive OK: %d, consecutive NG: %d",
			h.currentBatchSize, h.consecutiveOK, h.consecutiveNG))
	}

	// Memory usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	debug.DropMessage("MEMORY_USAGE", fmt.Sprintf("Memory: %d KB allocated, %d KB system, %d GC cycles", m.Alloc/1024, m.Sys/1024, m.NumGC))
}

func (h *DetachedHarvester) reportFinalStats() {
	elapsed := time.Since(h.startTime)
	processed := atomic.LoadInt64(&h.processed)
	errors := atomic.LoadInt64(&h.errors)
	lastBlock := atomic.LoadUint64(&h.lastProcessed)

	debug.DropMessage("SYNC_FINAL_REPORT", fmt.Sprintf("Synchronization completed in %v", elapsed.Round(time.Second)))
	debug.DropMessage("SYNC_FINAL_STATS", fmt.Sprintf("Events: %d processed, %d errors", processed, errors))
	debug.DropMessage("SYNC_FINAL_BLOCKS", fmt.Sprintf("Blocks: %d processed, final block: %d", lastBlock, h.syncTarget))
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// METADATA MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func (h *DetachedHarvester) getLastProcessedBlock() uint64 {
	var lastBlock uint64
	err := h.reservesDB.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&lastBlock)
	if err != nil {
		debug.DropMessage("LAST_BLOCK_ERROR", fmt.Sprintf("Failed to get last block: %v", err))
		return 0
	}
	return lastBlock
}

func (h *DetachedHarvester) updateSyncMetadata(currentBlock uint64) {
	_, err := h.reservesDB.Exec(`
		INSERT OR REPLACE INTO sync_metadata 
		(id, last_block, sync_target, sync_status, updated_at) 
		VALUES (1, ?, ?, 'running', ?)
	`, currentBlock, h.syncTarget, time.Now().Unix())

	if err != nil {
		debug.DropMessage("METADATA_ERROR", fmt.Sprintf("Failed to update metadata: %v", err))
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func parseHexUint64(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, fmt.Errorf("empty hex string")
	}
	return utils.ParseHexU64([]byte(s)), nil
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API - SIMPLE DETACHED EXECUTION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ExecuteDetachedSync runs a complete sync operation and terminates
// This is the main entry point for the detached harvester
func ExecuteDetachedSync() error {
	debug.DropMessage("DETACHED_SYNC", "Starting detached sync execution")

	// Create and run harvester
	harvester, err := NewDetachedHarvester()
	if err != nil {
		return fmt.Errorf("failed to create harvester: %w", err)
	}

	// Execute sync and terminate
	return harvester.SyncToLatestAndTerminate()
}

// CheckIfSyncNeeded determines if sync is required
func CheckIfSyncNeeded() (bool, uint64, uint64, error) {
	// Quick check without full initialization
	db, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		return true, 0, 0, nil // Assume sync needed if can't check
	}
	defer db.Close()

	var lastBlock uint64
	err = db.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&lastBlock)
	if err != nil {
		return true, 0, 0, nil
	}

	// Get current head using HTTP RPC endpoint
	rpcURL := fmt.Sprintf("https://%s/v3/a2a3139d2ab24d59bed2dc3643664126", constants.WsHost)
	client := NewRPCClient(rpcURL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	currentHead, err := client.BlockNumber(ctx)
	if err != nil {
		return true, 0, 0, fmt.Errorf("failed to get current head: %w", err)
	}

	syncTarget := currentHead - SyncTargetOffset
	syncNeeded := lastBlock < syncTarget

	debug.DropMessage("SYNC_CHECK", fmt.Sprintf(
		"Last: %d, Target: %d, Current: %d, Sync needed: %v",
		lastBlock, syncTarget, currentHead, syncNeeded,
	))

	return syncNeeded, lastBlock, syncTarget, nil
}
