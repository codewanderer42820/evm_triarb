// sync_harvester.go - Uniswap V2 Sync Event Harvester Module (No External Dependencies)
package syncharvest

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"main/utils"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

const (
	SyncEventBatchFloor  = uint64(100)
	SyncEventBatchCeil   = uint64(10_000)
	SyncEventCommitBatch = 100_000

	// Sync event signature (keccak256("Sync(uint112,uint112)"))
	SyncEventSignature = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

	// Database paths
	ReservesDBPath   = "uniswap_v2_reserves.db"
	ReservesMetaPath = "uniswap_v2_reserves.db.meta"

	// RPC settings
	HeadTTL = 12 * time.Second
)

// -----------------------------------------------------------------------------
// Types
// -----------------------------------------------------------------------------

// RPCClient handles JSON-RPC communication
type RPCClient struct {
	url    string
	client *http.Client
}

// RPCRequest represents a JSON-RPC request
type RPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

// RPCResponse represents a JSON-RPC response
type RPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *RPCError       `json:"error"`
	ID      int             `json:"id"`
}

// RPCError represents a JSON-RPC error
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Log represents an Ethereum log entry
type Log struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber string   `json:"blockNumber"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"`
}

// SyncEvent represents a Uniswap V2 Sync event with reserves
type SyncEvent struct {
	PairID      int64  `json:"pair_id"`
	PairAddress string `json:"pair_address"`
	BlockNumber uint64 `json:"block_number"`
	TxHash      string `json:"tx_hash"`
	LogIndex    uint64 `json:"log_index"`
	Reserve0    string `json:"reserve0"`
	Reserve1    string `json:"reserve1"`
}

// PairReserve tracks current reserves for a pair
type PairReserve struct {
	PairID      int64
	PairAddress string
	Reserve0    string
	Reserve1    string
	BlockHeight uint64
	LastUpdated time.Time
}

// -----------------------------------------------------------------------------
// RPC Client Implementation
// -----------------------------------------------------------------------------

// NewRPCClient creates a new RPC client
func NewRPCClient(url string) *RPCClient {
	return &RPCClient{
		url: url,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Call makes an RPC call
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

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewReader(data))
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

// BlockNumber gets the latest block number
func (c *RPCClient) BlockNumber(ctx context.Context) (uint64, error) {
	var result string
	if err := c.Call(ctx, &result, "eth_blockNumber"); err != nil {
		return 0, err
	}
	return parseHexUint64(result)
}

// GetLogs retrieves logs matching the filter
func (c *RPCClient) GetLogs(ctx context.Context, fromBlock, toBlock uint64, addresses []string, topics []string) ([]Log, error) {
	params := map[string]interface{}{
		"fromBlock": fmt.Sprintf("0x%x", fromBlock),
		"toBlock":   fmt.Sprintf("0x%x", toBlock),
	}

	if len(addresses) > 0 {
		params["address"] = addresses
	}

	if len(topics) > 0 {
		params["topics"] = []string{topics[0]} // Only first topic for sync events
	}

	var logs []Log
	if err := c.Call(ctx, &logs, "eth_getLogs", params); err != nil {
		return nil, err
	}

	return logs, nil
}

// -----------------------------------------------------------------------------
// Harvester
// -----------------------------------------------------------------------------

type Harvester struct {
	dataClient *RPCClient
	headClient *RPCClient
	pairsDB    *sql.DB
	reservesDB *sql.DB

	// Pair address to ID mapping cache
	pairMap map[string]int64

	// Pre-allocated address slice
	addresses []string

	// Head caching
	cachedHead uint64
	headTime   time.Time

	// Reusable buffer for hex decoding
	hexBuffer []byte
}

// NewHarvester creates a new sync event harvester
func NewHarvester(rpcURL string) (*Harvester, error) {
	// Create RPC clients
	dataClient := NewRPCClient(rpcURL)

	// Try public RPC for head, fall back to data RPC
	headClient := NewRPCClient("https://cloudflare-eth.com")

	// Test head client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := headClient.BlockNumber(ctx); err != nil {
		log.Printf("warning: failed to use head RPC: %v — falling back to data RPC", err)
		headClient = dataClient
	}

	// Open pairs database (read-only)
	pairsDB, err := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("open pairs DB: %w", err)
	}

	// Create/open reserves database
	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		return nil, fmt.Errorf("open reserves DB: %w", err)
	}

	// Enable optimizations
	for _, pragma := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = 10000",
		"PRAGMA temp_store = MEMORY",
	} {
		if _, err := reservesDB.Exec(pragma); err != nil {
			return nil, fmt.Errorf("pragma %s: %w", pragma, err)
		}
	}

	h := &Harvester{
		dataClient: dataClient,
		headClient: headClient,
		pairsDB:    pairsDB,
		reservesDB: reservesDB,
		pairMap:    make(map[string]int64),
		hexBuffer:  make([]byte, 64), // Always initialize the buffer
	}

	if err := h.initReservesDB(); err != nil {
		return nil, fmt.Errorf("init reserves DB: %w", err)
	}

	if err := h.loadPairMappings(); err != nil {
		return nil, fmt.Errorf("load pair mappings: %w", err)
	}

	return h, nil
}

// initReservesDB creates the reserves database schema
func (h *Harvester) initReservesDB() error {
	schema := `
	CREATE TABLE IF NOT EXISTS pair_reserves (
		pair_id      INTEGER PRIMARY KEY,
		pair_address TEXT NOT NULL,
		reserve0     TEXT NOT NULL,
		reserve1     TEXT NOT NULL,
		block_height INTEGER NOT NULL,
		last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(pair_address)
	);
	
	CREATE INDEX IF NOT EXISTS idx_block_height ON pair_reserves(block_height);
	CREATE INDEX IF NOT EXISTS idx_pair_address ON pair_reserves(pair_address);
	
	CREATE TABLE IF NOT EXISTS sync_events (
		id           INTEGER PRIMARY KEY AUTOINCREMENT,
		pair_id      INTEGER NOT NULL,
		block_number INTEGER NOT NULL,
		tx_hash      TEXT NOT NULL,
		log_index    INTEGER NOT NULL,
		reserve0     TEXT NOT NULL,
		reserve1     TEXT NOT NULL,
		created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(block_number, tx_hash, log_index)
	);
	
	CREATE INDEX IF NOT EXISTS idx_sync_pair ON sync_events(pair_id);
	CREATE INDEX IF NOT EXISTS idx_sync_block ON sync_events(block_number);
	`

	_, err := h.reservesDB.Exec(schema)
	return err
}

// loadPairMappings loads V2 pair addresses from the pairs database
func (h *Harvester) loadPairMappings() error {
	query := `
		SELECT p.id, p.pool_address 
		FROM pools p
		JOIN exchanges e ON p.exchange_id = e.id
		WHERE e.name = 'uniswap_v2' AND e.chain_id = 1
	`

	rows, err := h.pairsDB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Pre-allocate addresses slice
	h.addresses = make([]string, 0, 3000) // Pre-size for typical Uniswap V2 count

	count := 0
	for rows.Next() {
		var id int64
		var addr string
		if err := rows.Scan(&id, &addr); err != nil {
			return err
		}
		addr = strings.ToLower(addr)
		h.pairMap[addr] = id
		h.addresses = append(h.addresses, addr)
		count++
	}

	log.Printf("Loaded %d Uniswap V2 pair mappings", count)
	return rows.Err()
}

// Start begins harvesting sync events
func (h *Harvester) Start(ctx context.Context, startBlock uint64) error {
	metaF, err := os.OpenFile(ReservesMetaPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer metaF.Close()

	// Read last processed block from meta file
	var lastProcessed uint64
	if stat, _ := metaF.Stat(); stat.Size() >= 8 {
		if err := binary.Read(metaF, binary.BigEndian, &lastProcessed); err == nil && lastProcessed > 0 {
			startBlock = lastProcessed + 1
		}
	}

	if startBlock == 0 {
		startBlock = 10000835 // V2 factory deployment
	}

	log.Printf("Starting sync event harvest from block %d", startBlock)

	// Harvest loop
	batch := uint64(1000)
	consecutiveOK, consecutiveNG := 0, 0
	from := startBlock

	for {
		if ctx.Err() != nil {
			return h.writeMeta(metaF, lastProcessed)
		}

		// Get latest block with caching
		if time.Since(h.headTime) > HeadTTL {
			latest, err := h.headClient.BlockNumber(ctx)
			if err != nil {
				// Try alternate RPC
				if strings.Contains(err.Error(), "429") {
					log.Printf("head RPC 429 — switching to fallback")
					h.headClient = NewRPCClient("https://rpc.ankr.com/eth")
					latest, err = h.headClient.BlockNumber(ctx)
				}

				if err != nil {
					// Final fallback: use data client
					latest, err = h.dataClient.BlockNumber(ctx)
					if err != nil {
						log.Printf("head retrieval err: %v", err)
						time.Sleep(3 * time.Second)
						continue
					}
				}
			}
			h.cachedHead, h.headTime = latest, time.Now()
		}

		latest := h.cachedHead
		if from > latest {
			time.Sleep(10 * time.Second)
			continue
		}

		to := from + batch
		if to > latest {
			to = latest
		}

		log.Printf("Sync events %d → %d (batch %d, %d pairs)", from, to, batch, len(h.addresses))

		// Query logs in chunks to avoid query size limits
		const chunkSize = 1000
		var allLogs []Log

		for i := 0; i < len(h.addresses); i += chunkSize {
			end := i + chunkSize
			if end > len(h.addresses) {
				end = len(h.addresses)
			}

			logs, err := h.dataClient.GetLogs(ctx, from, to, h.addresses[i:end], []string{SyncEventSignature})
			if err != nil {
				consecutiveNG++
				consecutiveOK = 0
				if consecutiveNG%3 == 0 && batch > SyncEventBatchFloor {
					batch /= 2
					if batch < SyncEventBatchFloor {
						batch = SyncEventBatchFloor
					}
					log.Printf("↓ batch %d due to errors", batch)
				}
				log.Printf("Error fetching logs: %v", err)
				time.Sleep(2 * time.Second)
				break // Break inner loop to retry
			}

			allLogs = append(allLogs, logs...)
		}

		if len(allLogs) == 0 && consecutiveNG > 0 {
			continue // Retry if we had errors
		}

		// Process logs
		if err := h.processLogs(allLogs); err != nil {
			return fmt.Errorf("process logs: %w", err)
		}

		// Update meta
		lastProcessed = to
		if err := h.writeMeta(metaF, lastProcessed); err != nil {
			return err
		}

		// Log progress
		if len(allLogs) > 0 {
			log.Printf("Processed %d sync events", len(allLogs))
		}

		// Adjust batch size
		consecutiveOK++
		consecutiveNG = 0
		if consecutiveOK%3 == 0 && batch < SyncEventBatchCeil {
			batch *= 2
			if batch > SyncEventBatchCeil {
				batch = SyncEventBatchCeil
			}
			log.Printf("↑ batch %d due to success", batch)
		}

		from = to + 1
	}
}

// processLogs processes a batch of sync event logs with bulk inserts
func (h *Harvester) processLogs(logs []Log) error {
	if len(logs) == 0 {
		return nil
	}

	tx, err := h.reservesDB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Build bulk insert for sync_events
	const baseQuery = `INSERT OR IGNORE INTO sync_events (pair_id, block_number, tx_hash, log_index, reserve0, reserve1) VALUES `
	values := make([]string, 0, len(logs))
	args := make([]interface{}, 0, len(logs)*6)

	// Track latest reserves for bulk update
	reserveUpdates := make(map[int64]*PairReserve)

	processed := 0
	for _, lg := range logs {
		// Parse hex data using optimized function
		reserve0, reserve1, err := h.parseReservesOptimized(lg.Data)
		if err != nil {
			log.Printf("Failed to parse reserves from %s: %v", lg.Data, err)
			continue
		}

		pairAddr := strings.ToLower(lg.Address)
		pairID, ok := h.pairMap[pairAddr]
		if !ok {
			log.Printf("Unknown pair address: %s", pairAddr)
			continue
		}

		blockNum, err := parseHexUint64(lg.BlockNumber)
		if err != nil {
			log.Printf("Failed to parse block number %s: %v", lg.BlockNumber, err)
			continue
		}

		logIndex, err := parseHexUint64(lg.LogIndex)
		if err != nil {
			log.Printf("Failed to parse log index %s: %v", lg.LogIndex, err)
			continue
		}

		// Add to bulk insert
		values = append(values, "(?, ?, ?, ?, ?, ?)")
		args = append(args, pairID, blockNum, lg.TxHash, logIndex, reserve0.String(), reserve1.String())

		// Track latest reserve for each pair
		if existing, ok := reserveUpdates[pairID]; !ok || blockNum > existing.BlockHeight {
			reserveUpdates[pairID] = &PairReserve{
				PairID:      pairID,
				PairAddress: pairAddr,
				Reserve0:    reserve0.String(),
				Reserve1:    reserve1.String(),
				BlockHeight: blockNum,
			}
		}

		processed++
	}

	if processed == 0 {
		return fmt.Errorf("no logs were processed successfully")
	}

	// Execute bulk insert for events
	query := baseQuery + strings.Join(values, ",")
	if _, err := tx.Exec(query, args...); err != nil {
		return fmt.Errorf("failed to bulk insert sync events: %w", err)
	}

	// Bulk update reserves
	if len(reserveUpdates) > 0 {
		updateQuery := `
			INSERT OR REPLACE INTO pair_reserves (pair_id, pair_address, reserve0, reserve1, block_height, last_updated)
			VALUES `

		updateValues := make([]string, 0, len(reserveUpdates))
		updateArgs := make([]interface{}, 0, len(reserveUpdates)*5)

		for _, reserve := range reserveUpdates {
			updateValues = append(updateValues, "(?, ?, ?, ?, ?, CURRENT_TIMESTAMP)")
			updateArgs = append(updateArgs, reserve.PairID, reserve.PairAddress,
				reserve.Reserve0, reserve.Reserve1, reserve.BlockHeight)
		}

		updateQuery += strings.Join(updateValues, ",")
		if _, err := tx.Exec(updateQuery, updateArgs...); err != nil {
			return fmt.Errorf("failed to bulk update reserves: %w", err)
		}
	}

	return tx.Commit()
}

// parseReservesOptimized efficiently parses reserves from hex data
func (h *Harvester) parseReservesOptimized(dataStr string) (*big.Int, *big.Int, error) {
	// Remove 0x prefix
	dataStr = strings.TrimPrefix(dataStr, "0x")
	if len(dataStr) != 128 { // 64 bytes * 2 hex chars
		return nil, nil, fmt.Errorf("invalid data length: %d", len(dataStr))
	}

	// Ensure buffer is initialized (for tests that create Harvester manually)
	if h.hexBuffer == nil {
		h.hexBuffer = make([]byte, 64)
	}

	// Decode into reusable buffer
	n, err := hex.Decode(h.hexBuffer, []byte(dataStr))
	if err != nil {
		return nil, nil, err
	}
	if n != 64 {
		return nil, nil, fmt.Errorf("decoded length mismatch: %d", n)
	}

	// Create big.Ints from buffer slices
	reserve0 := new(big.Int).SetBytes(h.hexBuffer[:32])
	reserve1 := new(big.Int).SetBytes(h.hexBuffer[32:64])

	return reserve0, reserve1, nil
}

// writeMeta updates the meta file with the last processed block
func (h *Harvester) writeMeta(f *os.File, block uint64) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := f.Truncate(0); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, block); err != nil {
		return err
	}
	return f.Sync()
}

// Close cleanly shuts down the harvester
func (h *Harvester) Close() error {
	if h.pairsDB != nil {
		h.pairsDB.Close()
	}
	if h.reservesDB != nil {
		h.reservesDB.Close()
	}
	return nil
}

// GetLatestReserves returns a map of address -> reserves for initialization
func (h *Harvester) GetLatestReserves() (map[string]*PairReserve, error) {
	rows, err := h.reservesDB.Query(`
		SELECT pair_id, pair_address, reserve0, reserve1, block_height 
		FROM pair_reserves
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	reserves := make(map[string]*PairReserve)
	for rows.Next() {
		var pr PairReserve
		err := rows.Scan(&pr.PairID, &pr.PairAddress, &pr.Reserve0, &pr.Reserve1, &pr.BlockHeight)
		if err != nil {
			return nil, err
		}
		reserves[pr.PairAddress] = &pr
	}

	return reserves, rows.Err()
}

// -----------------------------------------------------------------------------
// Helper Functions
// -----------------------------------------------------------------------------

// parseHexUint64 parses a hex string to uint64
func parseHexUint64(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, fmt.Errorf("empty hex string")
	}

	// Validate hex characters
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return 0, fmt.Errorf("invalid hex string: %s", s)
		}
	}

	// Use the optimized ParseHexU64 from utils
	// It handles up to 16 hex characters which is perfect for uint64
	b := []byte(s)
	if len(b) > 16 {
		b = b[:16]
	}

	return utils.ParseHexU64(b), nil
}
