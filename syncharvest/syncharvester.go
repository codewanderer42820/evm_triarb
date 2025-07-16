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
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
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
		"topics":    []string{topics[0]}, // Only first topic for sync events
	}

	if len(addresses) > 0 {
		params["address"] = addresses
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
	mu      sync.RWMutex

	// Head caching
	cachedHead uint64
	headTime   time.Time
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

	count := 0
	for rows.Next() {
		var id int64
		var addr string
		if err := rows.Scan(&id, &addr); err != nil {
			return err
		}
		h.pairMap[strings.ToLower(addr)] = id
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

	// Get all V2 pair addresses for filtering
	addresses := make([]string, 0, len(h.pairMap))
	for addr := range h.pairMap {
		addresses = append(addresses, addr)
	}

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

		log.Printf("Sync events %d → %d (batch %d, %d pairs)", from, to, batch, len(addresses))

		// Query logs in chunks to avoid query size limits
		const chunkSize = 1000
		var allLogs []Log

		for i := 0; i < len(addresses); i += chunkSize {
			end := i + chunkSize
			if end > len(addresses) {
				end = len(addresses)
			}

			logs, err := h.dataClient.GetLogs(ctx, from, to, addresses[i:end], []string{SyncEventSignature})
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

// processLogs processes a batch of sync event logs
func (h *Harvester) processLogs(logs []Log) error {
	if len(logs) == 0 {
		return nil
	}

	tx, err := h.reservesDB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare statements
	insertEvent, err := tx.Prepare(`
		INSERT OR IGNORE INTO sync_events (pair_id, block_number, tx_hash, log_index, reserve0, reserve1)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer insertEvent.Close()

	updateReserves, err := tx.Prepare(`
		INSERT OR REPLACE INTO pair_reserves (pair_id, pair_address, reserve0, reserve1, block_height, last_updated)
		VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	`)
	if err != nil {
		return err
	}
	defer updateReserves.Close()

	for _, lg := range logs {
		// Parse hex data
		data, err := hex.DecodeString(strings.TrimPrefix(lg.Data, "0x"))
		if err != nil {
			continue
		}

		// Parse reserves from log data
		if len(data) != 64 {
			continue // Invalid sync event
		}

		reserve0 := new(big.Int).SetBytes(data[:32])
		reserve1 := new(big.Int).SetBytes(data[32:])

		pairAddr := strings.ToLower(lg.Address)
		pairID, ok := h.pairMap[pairAddr]
		if !ok {
			continue // Unknown pair
		}

		// Parse block number and log index
		blockNum, err := parseHexUint64(lg.BlockNumber)
		if err != nil {
			continue
		}

		logIndex, err := parseHexUint64(lg.LogIndex)
		if err != nil {
			continue
		}

		// Insert sync event
		_, err = insertEvent.Exec(
			pairID,
			blockNum,
			lg.TxHash,
			logIndex,
			reserve0.String(),
			reserve1.String(),
		)
		if err != nil {
			return err
		}

		// Update current reserves
		_, err = updateReserves.Exec(
			pairID,
			pairAddr,
			reserve0.String(),
			reserve1.String(),
			blockNum,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
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
	h.pairsDB.Close()
	h.reservesDB.Close()
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
	n, err := hex.DecodeString(s)
	if err != nil {
		return 0, err
	}

	// Pad to 8 bytes if necessary
	if len(n) < 8 {
		padded := make([]byte, 8)
		copy(padded[8-len(n):], n)
		n = padded
	}

	return binary.BigEndian.Uint64(n[len(n)-8:]), nil
}
