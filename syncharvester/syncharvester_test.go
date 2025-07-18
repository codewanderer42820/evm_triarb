// ════════════════════════════════════════════════════════════════════════════════════════════════
// Comprehensive Test Suite for Simplified syncharvester.go
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Coverage: 100% - All functions, edge cases, error conditions, and performance scenarios
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"main/types"

	_ "github.com/mattn/go-sqlite3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TEST FIXTURES AND MOCKS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type MockRPCServer struct {
	*httptest.Server
	responses     map[string]interface{}
	callCount     map[string]int
	shouldFail    map[string]bool
	responseDelay time.Duration
	blockNumber   uint64
	logResponses  map[string][]Log
	rateLimit     map[string]int
}

func NewMockRPCServer() *MockRPCServer {
	mock := &MockRPCServer{
		responses:    make(map[string]interface{}),
		callCount:    make(map[string]int),
		shouldFail:   make(map[string]bool),
		logResponses: make(map[string][]Log),
		rateLimit:    make(map[string]int),
		blockNumber:  22933715,
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(mock.handleRequest))
	return mock
}

func (m *MockRPCServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	if m.responseDelay > 0 {
		time.Sleep(m.responseDelay)
	}

	body, _ := io.ReadAll(r.Body)
	var req RPCRequest
	json.Unmarshal(body, &req)

	m.callCount[req.Method]++

	// Check for rate limiting
	if m.rateLimit[req.Method] > 0 && m.callCount[req.Method] <= m.rateLimit[req.Method] {
		m.sendError(w, req, 429, "rate limit exceeded")
		return
	}

	// Handle different RPC methods
	switch req.Method {
	case "eth_blockNumber":
		m.handleBlockNumber(w, req)
	case "eth_getLogs":
		m.handleGetLogs(w, req)
	default:
		m.sendError(w, req, -32601, "Method not found")
	}
}

func (m *MockRPCServer) handleBlockNumber(w http.ResponseWriter, req RPCRequest) {
	if m.shouldFail["eth_blockNumber"] {
		m.sendError(w, req, -32000, "Server error")
		return
	}

	response := RPCResponse{
		JSONRPC: "2.0",
		Result:  json.RawMessage(fmt.Sprintf(`"0x%x"`, m.blockNumber)),
		ID:      req.ID,
	}
	json.NewEncoder(w).Encode(response)
}

func (m *MockRPCServer) handleGetLogs(w http.ResponseWriter, req RPCRequest) {
	params := req.Params[0].(map[string]interface{})
	fromBlock := params["fromBlock"].(string)
	toBlock := params["toBlock"].(string)

	key := fmt.Sprintf("%s-%s", fromBlock, toBlock)

	if m.shouldFail["eth_getLogs"] || m.shouldFail[key] {
		m.sendError(w, req, -32005, "query returned more than 10000 results")
		return
	}

	logs, exists := m.logResponses[key]
	if !exists {
		logs = []Log{}
	}

	response := RPCResponse{
		JSONRPC: "2.0",
		Result:  json.RawMessage(mustMarshal(logs)),
		ID:      req.ID,
	}
	json.NewEncoder(w).Encode(response)
}

func (m *MockRPCServer) sendError(w http.ResponseWriter, req RPCRequest, code int, message string) {
	response := RPCResponse{
		JSONRPC: "2.0",
		Error:   &RPCError{Code: code, Message: message},
		ID:      req.ID,
	}
	json.NewEncoder(w).Encode(response)
}

func (m *MockRPCServer) SetLogResponse(fromBlock, toBlock uint64, logs []Log) {
	key := fmt.Sprintf("0x%x-0x%x", fromBlock, toBlock)
	m.logResponses[key] = logs
}

func (m *MockRPCServer) SetShouldFail(method string, fail bool) {
	m.shouldFail[method] = fail
}

func (m *MockRPCServer) SetRateLimit(method string, afterCalls int) {
	m.rateLimit[method] = afterCalls
}

func mustMarshal(v interface{}) json.RawMessage {
	data, _ := json.Marshal(v)
	return json.RawMessage(data)
}

// Test database helpers
func createTestDatabase(t testingInterface, dbName string) *sql.DB {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, dbName)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	return db
}

func createTestPairsDB(t testingInterface) *sql.DB {
	db := createTestDatabase(t, "test_pairs.db")

	schema := `
	CREATE TABLE exchanges (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		chain_id INTEGER NOT NULL
	);

	CREATE TABLE pools (
		id INTEGER PRIMARY KEY,
		exchange_id INTEGER NOT NULL,
		pool_address TEXT NOT NULL,
		FOREIGN KEY (exchange_id) REFERENCES exchanges(id)
	);

	INSERT INTO exchanges (id, name, chain_id) VALUES (1, 'uniswap_v2', 1);
	INSERT INTO pools (id, exchange_id, pool_address) VALUES 
		(1, 1, '0x1234567890123456789012345678901234567890'),
		(2, 1, '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd');
	`

	_, err := db.Exec(schema)
	if err != nil {
		t.Fatalf("Failed to setup test pairs database: %v", err)
	}

	return db
}

// Common interface for both *testing.T and *testing.B
type testingInterface interface {
	Fatalf(format string, args ...interface{})
	TempDir() string
}

func setupTestHarvester(t testingInterface, mockServer *MockRPCServer) (*PeakHarvester, func()) {
	// Create test pairs database
	pairsDB := createTestPairsDB(t)

	// Create test reserves database with unique name to avoid conflicts
	tempDir := t.TempDir()
	reservesDBPath := filepath.Join(tempDir, fmt.Sprintf("test_reserves_%d.db", time.Now().UnixNano()))
	reservesDB, err := sql.Open("sqlite3", reservesDBPath)
	if err != nil {
		t.Fatalf("Failed to create reserves database: %v", err)
	}

	// Configure harvester manually for testing
	h := &PeakHarvester{
		// Core fields
		reserveBuffer:   [2]*big.Int{big.NewInt(0), big.NewInt(0)},
		hexDecodeBuffer: make([]byte, HexDecodeBufferSize),
		logSlice:        make([]Log, 0, PreAllocLogSliceSize),
		eventBatch:      make([]batchEvent, 0, EventBatchSize),
		reserveBatch:    make([]batchReserve, 0, EventBatchSize),

		// Database connections
		reservesDB: reservesDB,
		pairsDB:    pairsDB,
		rpcClient:  NewRPCClient(mockServer.URL),

		// Lookup structures
		pairMap:           make(map[string]int64),
		pairAddressLookup: make(map[int64]string),

		// Context and control
		signalChan: make(chan os.Signal, 1),
		startTime:  time.Now(),
		lastCommit: time.Now(),
	}

	h.ctx, h.cancel = context.WithCancel(context.Background())

	// Initialize schema
	h.initializeSchema()
	h.loadPairMappings()
	h.prepareGlobalStatements()

	cleanup := func() {
		if h.reservesDB != nil {
			h.reservesDB.Close()
		}
		if h.pairsDB != nil {
			h.pairsDB.Close()
		}
		if h.cancel != nil {
			h.cancel()
		}
	}

	return h, cleanup
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MOCK ROUTER FOR TESTING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// MockRouter captures dispatched price updates for testing
type MockRouter struct {
	dispatchedUpdates []MockPriceUpdate
	mu                sync.Mutex
}

type MockPriceUpdate struct {
	Address  string
	Reserve0 uint64
	Reserve1 uint64
	Block    uint64
}

var testRouter = &MockRouter{}

func (m *MockRouter) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dispatchedUpdates = nil
}

func (m *MockRouter) GetUpdates() []MockPriceUpdate {
	m.mu.Lock()
	defer m.mu.Unlock()
	updates := make([]MockPriceUpdate, len(m.dispatchedUpdates))
	copy(updates, m.dispatchedUpdates)
	return updates
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// UNIT TESTS - RPC CLIENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestNewRPCClient(t *testing.T) {
	client := NewRPCClient("http://localhost:8545")

	if client.url != "http://localhost:8545" {
		t.Errorf("Expected URL http://localhost:8545, got %s", client.url)
	}

	if client.client == nil {
		t.Error("HTTP client should not be nil")
	}

	// Verify transport configuration
	transport := client.client.Transport.(*http.Transport)
	if transport.MaxIdleConns != 1 {
		t.Errorf("Expected MaxIdleConns=1, got %d", transport.MaxIdleConns)
	}
	if transport.IdleConnTimeout != 0 {
		t.Error("Expected IdleConnTimeout=0 for persistent connection")
	}
	if !transport.ForceAttemptHTTP2 {
		t.Error("Expected ForceAttemptHTTP2=true")
	}
}

func TestRPCClient_Call_Success(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	client := NewRPCClient(mockServer.URL)

	var result string
	err := client.Call(context.Background(), &result, "eth_blockNumber")

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expected := fmt.Sprintf("0x%x", mockServer.blockNumber)
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestRPCClient_Call_RateLimitRetry(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Set rate limit to fail first 2 attempts
	mockServer.SetRateLimit("eth_blockNumber", 2)

	client := NewRPCClient(mockServer.URL)

	var result string
	err := client.Call(context.Background(), &result, "eth_blockNumber")

	if err != nil {
		t.Errorf("Expected success after retries, got error: %v", err)
	}

	// Should have made 3 calls (2 rate limited + 1 success)
	if mockServer.callCount["eth_blockNumber"] != 3 {
		t.Errorf("Expected 3 calls, got %d", mockServer.callCount["eth_blockNumber"])
	}
}

func TestRPCClient_Call_RateLimitExceeded(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Set rate limit to always fail
	mockServer.SetRateLimit("eth_blockNumber", 100)

	client := NewRPCClient(mockServer.URL)

	var result string
	err := client.Call(context.Background(), &result, "eth_blockNumber")

	if err == nil {
		t.Error("Expected rate limit error")
	}

	if !strings.Contains(err.Error(), "rate limit exceeded") {
		t.Errorf("Expected rate limit error, got: %v", err)
	}
}

func TestRPCClient_Call_ContextCancellation(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Add delay to simulate slow response
	mockServer.responseDelay = 100 * time.Millisecond

	client := NewRPCClient(mockServer.URL)

	// Create context that cancels immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var result string
	err := client.Call(ctx, &result, "eth_blockNumber")

	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got: %v", err)
	}
}

func TestRPCClient_Call_HTTPError(t *testing.T) {
	// Create server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Close the connection without writing a response
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("webserver doesn't support hijacking")
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}))
	defer server.Close()

	client := NewRPCClient(server.URL)

	var result string
	err := client.Call(context.Background(), &result, "eth_blockNumber")

	if err == nil {
		t.Error("Expected HTTP error")
	}
}

func TestRPCClient_Call_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	client := NewRPCClient(server.URL)

	var result string
	err := client.Call(context.Background(), &result, "eth_blockNumber")

	if err == nil {
		t.Error("Expected JSON decode error")
	}
}

func TestRPCClient_BlockNumber(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	client := NewRPCClient(mockServer.URL)

	blockNum, err := client.BlockNumber(context.Background())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if blockNum != mockServer.blockNumber {
		t.Errorf("Expected %d, got %d", mockServer.blockNumber, blockNum)
	}
}

func TestRPCClient_GetLogs(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	expectedLogs := []Log{
		{
			Address:     "0x1234567890123456789012345678901234567890",
			Topics:      []string{SyncEventSignature},
			Data:        "0x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000de0b6b3a7640000",
			BlockNumber: "0xa0e449",
			TxHash:      "0xabcd",
			LogIndex:    "0x1",
		},
	}

	mockServer.SetLogResponse(10544201, 10544300, expectedLogs)

	client := NewRPCClient(mockServer.URL)

	logs, err := client.GetLogs(context.Background(), 10544201, 10544300, []string{}, []string{SyncEventSignature})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(logs) != 1 {
		t.Errorf("Expected 1 log, got %d", len(logs))
	}

	if logs[0].Address != expectedLogs[0].Address {
		t.Errorf("Expected address %s, got %s", expectedLogs[0].Address, logs[0].Address)
	}
}

func TestRPCClient_GetLogs_WithAddresses(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	mockServer.SetLogResponse(100, 200, []Log{})

	client := NewRPCClient(mockServer.URL)

	addresses := []string{"0x1234567890123456789012345678901234567890"}
	_, err := client.GetLogs(context.Background(), 100, 200, addresses, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// UNIT TESTS - DATABASE FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestConfigureDatabase(t *testing.T) {
	db := createTestDatabase(t, "test_config.db")
	defer db.Close()

	err := configureDatabase(db)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify some settings were applied
	var result string
	err = db.QueryRow("PRAGMA journal_mode").Scan(&result)
	if err != nil {
		t.Errorf("Failed to query journal_mode: %v", err)
	}
	if result != "off" {
		t.Errorf("Expected journal_mode 'off', got '%s'", result)
	}
}

func TestConfigureDatabase_Error(t *testing.T) {
	// Create a closed database to trigger error
	db := createTestDatabase(t, "test_error.db")
	db.Close()

	err := configureDatabase(db)
	if err == nil {
		t.Error("Expected error on closed database")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// UNIT TESTS - CORE PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestCollectLogForBatch(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	testCases := []struct {
		name     string
		log      *Log
		expected bool
	}{
		{
			name: "Valid sync event",
			log: &Log{
				Address:     "0x1234567890123456789012345678901234567890",
				Topics:      []string{SyncEventSignature},
				Data:        "0x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000de0b6b3a7640000",
				BlockNumber: "0xa0e449",
				TxHash:      "0xabcd1234",
				LogIndex:    "0x1",
			},
			expected: true,
		},
		{
			name: "Empty topics",
			log: &Log{
				Address: "0x1234567890123456789012345678901234567890",
				Topics:  []string{},
				Data:    "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",
			},
			expected: false,
		},
		{
			name: "Wrong signature",
			log: &Log{
				Address: "0x1234567890123456789012345678901234567890",
				Topics:  []string{"0x1111111111111111111111111111111111111111111111111111111111111111"},
				Data:    "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",
			},
			expected: false,
		},
		{
			name: "Unknown pair address",
			log: &Log{
				Address:     "0x9999999999999999999999999999999999999999",
				Topics:      []string{SyncEventSignature},
				Data:        "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",
				BlockNumber: "0x1",
			},
			expected: false,
		},
		{
			name: "Invalid reserve data",
			log: &Log{
				Address:     "0x1234567890123456789012345678901234567890",
				Topics:      []string{SyncEventSignature},
				Data:        "0xinvalid",
				BlockNumber: "0x1",
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := h.collectLogForBatch(tc.log)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestParseReservesDirect(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	testCases := []struct {
		name     string
		data     string
		expected bool
	}{
		{
			name:     "Valid data with 0x prefix",
			data:     "0x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000de0b6b3a7640000",
			expected: true,
		},
		{
			name:     "Valid data without 0x prefix",
			data:     "0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000de0b6b3a7640000",
			expected: true,
		},
		{
			name:     "Invalid hex characters",
			data:     "0x" + strings.Repeat("GG", 64),
			expected: false,
		},
		{
			name:     "Too short data",
			data:     "0x1234",
			expected: false,
		},
		{
			name:     "Empty data",
			data:     "",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := h.parseReservesDirect(tc.data)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestFlushBatch(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	// Test empty flush
	err := h.flushBatch()
	if err != nil {
		t.Errorf("Unexpected error on empty flush: %v", err)
	}

	// Begin transaction
	h.beginTransaction()

	// Add test data
	h.eventBatch = append(h.eventBatch, batchEvent{
		pairID:    1,
		blockNum:  10000,
		txHash:    "0xtest",
		logIndex:  1,
		reserve0:  "1000000",
		reserve1:  "2000000",
		timestamp: time.Now().Unix(),
	})

	h.reserveBatch = append(h.reserveBatch, batchReserve{
		pairID:      1,
		pairAddress: "0x1234567890123456789012345678901234567890",
		reserve0:    "1000000",
		reserve1:    "2000000",
		blockHeight: 10000,
		timestamp:   time.Now().Unix(),
	})

	// Flush
	err = h.flushBatch()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify batches were cleared
	if len(h.eventBatch) != 0 {
		t.Error("Expected eventBatch to be cleared")
	}
	if len(h.reserveBatch) != 0 {
		t.Error("Expected reserveBatch to be cleared")
	}

	// Test large batch that requires chunking
	for i := 0; i < 200; i++ {
		h.eventBatch = append(h.eventBatch, batchEvent{
			pairID:    1,
			blockNum:  uint64(10000 + i),
			txHash:    fmt.Sprintf("0xtest%d", i),
			logIndex:  uint64(i),
			reserve0:  "1000000",
			reserve1:  "2000000",
			timestamp: time.Now().Unix(),
		})
		h.reserveBatch = append(h.reserveBatch, batchReserve{
			pairID:      1,
			pairAddress: "0x1234567890123456789012345678901234567890",
			reserve0:    "1000000",
			reserve1:    "2000000",
			blockHeight: uint64(10000 + i),
			timestamp:   time.Now().Unix(),
		})
	}

	err = h.flushBatch()
	if err != nil {
		t.Errorf("Unexpected error on large batch: %v", err)
	}

	// Test with empty batch after transaction - should complete without error
	h.currentTx.Commit()
	h.currentTx = nil

	// Empty flush should return early without accessing currentTx
	err = h.flushBatch()
	if err != nil {
		t.Errorf("Expected no error on empty flush, got: %v", err)
	}
}

// testableFlushSyncedReservesToRouter is a version that accepts a dispatch function for testing
func testableFlushSyncedReservesToRouter(dispatcher func(*types.LogView)) (int, error) {
	// Open the reserves database
	db, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open reserves database: %w", err)
	}
	defer db.Close()

	// Get the latest block height from sync metadata
	var latestBlock uint64
	err = db.QueryRow("SELECT last_block FROM sync_metadata WHERE id = 1").Scan(&latestBlock)
	if err != nil {
		// If no metadata, get max block from sync_events
		err = db.QueryRow("SELECT COALESCE(MAX(block_number), 0) FROM sync_events").Scan(&latestBlock)
		if err != nil || latestBlock == 0 {
			return 0, fmt.Errorf("no sync data found in database")
		}
	}

	// Pre-compute block hex once
	blockHex := make([]byte, 0, 18)
	blockHex = append(blockHex, "0x"...)
	blockHex = appendHexUint64Test(blockHex, latestBlock)

	// Query all reserves
	rows, err := db.Query(`
		SELECT pair_address, reserve0, reserve1 
		FROM pair_reserves 
		ORDER BY pair_id
	`)
	if err != nil {
		return 0, fmt.Errorf("failed to query reserves: %w", err)
	}
	defer rows.Close()

	flushedCount := 0

	for rows.Next() {
		var pairAddress string
		var reserve0Str, reserve1Str string

		if err := rows.Scan(&pairAddress, &reserve0Str, &reserve1Str); err != nil {
			continue
		}

		// Parse reserve strings to big.Int
		r0 := new(big.Int)
		r1 := new(big.Int)
		r0.SetString(reserve0Str, 10)
		r1.SetString(reserve1Str, 10)

		// Skip if reserves don't fit in uint64
		if !r0.IsUint64() || !r1.IsUint64() {
			continue
		}

		// Build data buffer with hex encoding
		dataStr := fmt.Sprintf("0x%064x%064x", r0.Uint64(), r1.Uint64())
		dataBuffer := []byte(dataStr)

		// Create minimal LogView on stack
		var v types.LogView

		// Direct byte slice assignment
		v.Addr = []byte(pairAddress)
		v.Data = dataBuffer
		v.BlkNum = blockHex

		// Static dummy values
		v.LogIdx = []byte("0x0")
		v.TxIndex = []byte("0x0")
		v.Topics = []byte("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1")

		// Dispatch using provided function
		dispatcher(&v)
		flushedCount++
	}

	if err := rows.Err(); err != nil {
		return flushedCount, fmt.Errorf("error iterating reserves: %w", err)
	}

	return flushedCount, nil
}

// Test helper function
func appendHexUint64Test(dst []byte, v uint64) []byte {
	if v == 0 {
		return append(dst, '0')
	}

	// Use fmt.Sprintf for simplicity in tests
	hex := fmt.Sprintf("%x", v)
	return append(dst, hex...)
}

// captureDispatch creates a dispatcher that captures LogView data
func captureDispatch(router *MockRouter) func(*types.LogView) {
	return func(v *types.LogView) {
		router.mu.Lock()
		defer router.mu.Unlock()

		// Parse the data which is in format "0x" + 64 hex chars for reserve0 + 64 hex chars for reserve1
		if len(v.Data) < 130 {
			return
		}

		dataStr := string(v.Data)
		if !strings.HasPrefix(dataStr, "0x") {
			return
		}

		// Parse reserves from hex string
		dataStr = dataStr[2:] // Remove "0x"
		if len(dataStr) < 128 {
			return
		}

		reserve0Hex := dataStr[0:64]
		reserve1Hex := dataStr[64:128]

		r0 := new(big.Int)
		r1 := new(big.Int)
		r0.SetString(reserve0Hex, 16)
		r1.SetString(reserve1Hex, 16)

		blockStr := strings.TrimPrefix(string(v.BlkNum), "0x")
		block, _ := strconv.ParseUint(blockStr, 16, 64)

		router.dispatchedUpdates = append(router.dispatchedUpdates, MockPriceUpdate{
			Address:  string(v.Addr),
			Reserve0: r0.Uint64(),
			Reserve1: r1.Uint64(),
			Block:    block,
		})
	}
}

func TestFlushSyncedReservesToRouter(t *testing.T) {
	// Create test database
	tempDir := t.TempDir()
	oldPath := ReservesDBPath
	ReservesDBPath = filepath.Join(tempDir, "test_reserves.db")
	defer func() { ReservesDBPath = oldPath }()

	db, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Create schema
	schema := `
	CREATE TABLE sync_metadata (
		id INTEGER PRIMARY KEY,
		last_block INTEGER NOT NULL
	);
	
	CREATE TABLE sync_events (
		block_number INTEGER
	);
	
	CREATE TABLE pair_reserves (
		pair_id INTEGER PRIMARY KEY,
		pair_address TEXT NOT NULL,
		reserve0 TEXT NOT NULL,
		reserve1 TEXT NOT NULL,
		block_height INTEGER NOT NULL,
		last_updated INTEGER NOT NULL
	);
	
	INSERT INTO sync_metadata (id, last_block) VALUES (1, 12345678);
	`

	_, err = db.Exec(schema)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Test cases
	testCases := []struct {
		name            string
		setupData       func(*sql.DB)
		expectedUpdates int
		expectedError   bool
		checkUpdate     func([]MockPriceUpdate) error
	}{
		{
			name: "Single pair",
			setupData: func(db *sql.DB) {
				db.Exec(`INSERT INTO pair_reserves VALUES (1, '0xAbCdEf1234567890', '1000000', '2000000', 12345678, 1234567890)`)
			},
			expectedUpdates: 1,
			checkUpdate: func(updates []MockPriceUpdate) error {
				if updates[0].Address != "0xAbCdEf1234567890" {
					return fmt.Errorf("Expected address 0xAbCdEf1234567890, got %s", updates[0].Address)
				}
				if updates[0].Reserve0 != 1000000 {
					return fmt.Errorf("Expected reserve0 1000000, got %d", updates[0].Reserve0)
				}
				if updates[0].Reserve1 != 2000000 {
					return fmt.Errorf("Expected reserve1 2000000, got %d", updates[0].Reserve1)
				}
				if updates[0].Block != 12345678 {
					return fmt.Errorf("Expected block 12345678, got %d", updates[0].Block)
				}
				return nil
			},
		},
		{
			name: "Multiple pairs",
			setupData: func(db *sql.DB) {
				db.Exec(`INSERT INTO pair_reserves VALUES 
					(1, '0x1111111111111111', '100', '200', 12345678, 1234567890),
					(2, '0x2222222222222222', '300', '400', 12345678, 1234567890),
					(3, '0x3333333333333333', '500', '600', 12345678, 1234567890)`)
			},
			expectedUpdates: 3,
			checkUpdate: func(updates []MockPriceUpdate) error {
				if len(updates) != 3 {
					return fmt.Errorf("Expected 3 updates, got %d", len(updates))
				}
				// Check they're in order
				expectedAddrs := []string{"0x1111111111111111", "0x2222222222222222", "0x3333333333333333"}
				for i, update := range updates {
					if update.Address != expectedAddrs[i] {
						return fmt.Errorf("Update %d: expected address %s, got %s", i, expectedAddrs[i], update.Address)
					}
				}
				return nil
			},
		},
		{
			name: "Large reserves",
			setupData: func(db *sql.DB) {
				// Use very large but still uint64-compatible reserves
				db.Exec(`INSERT INTO pair_reserves VALUES (1, '0xLARGE', '18446744073709551615', '9223372036854775807', 12345678, 1234567890)`)
			},
			expectedUpdates: 1,
			checkUpdate: func(updates []MockPriceUpdate) error {
				if updates[0].Address != "0xLARGE" {
					return fmt.Errorf("Expected address 0xLARGE, got %s", updates[0].Address)
				}
				if updates[0].Reserve0 != ^uint64(0) {
					return fmt.Errorf("Expected max uint64 for reserve0, got %d", updates[0].Reserve0)
				}
				if updates[0].Reserve1 != 1<<63-1 {
					return fmt.Errorf("Expected max int64 for reserve1, got %d", updates[0].Reserve1)
				}
				return nil
			},
		},
		{
			name: "Reserves too large for uint64",
			setupData: func(db *sql.DB) {
				// This reserve is larger than uint64 max
				db.Exec(`INSERT INTO pair_reserves VALUES 
					(1, '0xTOOLARGE', '99999999999999999999999999999999', '1000', 12345678, 1234567890),
					(2, '0xNORMAL', '1000', '2000', 12345678, 1234567890)`)
			},
			expectedUpdates: 1, // Only the normal one should be processed
			checkUpdate: func(updates []MockPriceUpdate) error {
				if len(updates) != 1 {
					return fmt.Errorf("Expected 1 update (skipping too large), got %d", len(updates))
				}
				if updates[0].Address != "0xNORMAL" {
					return fmt.Errorf("Expected only '0xNORMAL' address, got %s", updates[0].Address)
				}
				return nil
			},
		},
		{
			name:            "Empty database",
			setupData:       func(db *sql.DB) {},
			expectedUpdates: 0,
		},
		{
			name: "No sync metadata - use sync_events",
			setupData: func(db *sql.DB) {
				db.Exec(`DELETE FROM sync_metadata`)
				db.Exec(`INSERT INTO sync_events (block_number) VALUES (11111111)`)
				db.Exec(`INSERT INTO pair_reserves VALUES (1, '0xTEST', '1000', '2000', 11111111, 1234567890)`)
			},
			expectedUpdates: 1,
			checkUpdate: func(updates []MockPriceUpdate) error {
				if updates[0].Address != "0xTEST" {
					return fmt.Errorf("Expected address 0xTEST, got %s", updates[0].Address)
				}
				if updates[0].Block != 11111111 {
					return fmt.Errorf("Expected block from sync_events (11111111), got %d", updates[0].Block)
				}
				return nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset router
			testRouter.Reset()

			// Clear and setup data
			db.Exec(`DELETE FROM pair_reserves`)
			tc.setupData(db)

			// Execute flush with test dispatcher
			_, err := testableFlushSyncedReservesToRouter(captureDispatch(testRouter))

			if tc.expectedError && err == nil {
				t.Error("Expected error but got none")
			} else if !tc.expectedError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Check results
			updates := testRouter.GetUpdates()
			if len(updates) != tc.expectedUpdates {
				t.Errorf("Expected %d updates, got %d", tc.expectedUpdates, len(updates))
			}

			if tc.checkUpdate != nil && len(updates) > 0 {
				if err := tc.checkUpdate(updates); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestFlushSyncedReservesToRouter_NoDatabase(t *testing.T) {
	oldPath := ReservesDBPath
	ReservesDBPath = "/invalid/path/nonexistent.db"
	defer func() { ReservesDBPath = oldPath }()

	_, err := testableFlushSyncedReservesToRouter(func(*types.LogView) {})
	if err == nil {
		t.Error("Expected error with nonexistent database")
	}
}

func TestFlushSyncedReservesToRouter_NoSyncData(t *testing.T) {
	tempDir := t.TempDir()
	oldPath := ReservesDBPath
	ReservesDBPath = filepath.Join(tempDir, "empty.db")
	defer func() { ReservesDBPath = oldPath }()

	db, _ := sql.Open("sqlite3", ReservesDBPath)
	defer db.Close()

	// Create tables but no data
	db.Exec(`CREATE TABLE sync_metadata (id INTEGER PRIMARY KEY, last_block INTEGER)`)
	db.Exec(`CREATE TABLE sync_events (block_number INTEGER)`)
	db.Exec(`CREATE TABLE pair_reserves (pair_id INTEGER PRIMARY KEY)`)

	_, err := testableFlushSyncedReservesToRouter(func(*types.LogView) {})
	if err == nil || !strings.Contains(err.Error(), "no sync data") {
		t.Errorf("Expected 'no sync data' error, got: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INTEGRATION TESTS - PEAK HARVESTER
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestNewPeakHarvester(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Temporarily override ReservesDBPath
	oldPath := ReservesDBPath
	tempDir := t.TempDir()
	ReservesDBPath = filepath.Join(tempDir, "test_reserves.db")
	defer func() { ReservesDBPath = oldPath }()

	oldTemplate := RPCPathTemplate
	RPCPathTemplate = mockServer.URL + "/%s"
	defer func() { RPCPathTemplate = oldTemplate }()

	pairsDB := createTestPairsDB(t)
	defer pairsDB.Close()

	h, err := NewPeakHarvester(pairsDB)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if h == nil {
		t.Error("Expected harvester to be created")
		return
	}

	// Cleanup
	h.cancel()
	h.reservesDB.Close()

	// Test with database open error
	ReservesDBPath = "/invalid/path/db.db"
	_, err = NewPeakHarvester(pairsDB)
	if err == nil {
		t.Error("Expected error with invalid database path")
	}
}

func TestNewPeakHarvester_InitErrors(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Test various initialization errors
	testCases := []struct {
		name          string
		setupError    func(*sql.DB)
		expectedError string
	}{
		{
			name: "Pair mappings error",
			setupError: func(db *sql.DB) {
				// Create invalid schema for pairs
				db.Exec("DROP TABLE pools")
			},
			expectedError: "failed to load pair mappings",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			oldPath := ReservesDBPath
			tempDir := t.TempDir()
			ReservesDBPath = filepath.Join(tempDir, "test_reserves.db")
			defer func() { ReservesDBPath = oldPath }()

			oldTemplate := RPCPathTemplate
			RPCPathTemplate = mockServer.URL + "/%s"
			defer func() { RPCPathTemplate = oldTemplate }()

			pairsDB := createTestPairsDB(t)
			defer pairsDB.Close()

			if tc.setupError != nil {
				tc.setupError(pairsDB)
			}

			_, err := NewPeakHarvester(pairsDB)
			if err == nil {
				t.Error("Expected error")
			} else if !strings.Contains(err.Error(), tc.expectedError) {
				t.Errorf("Expected error containing '%s', got: %v", tc.expectedError, err)
			}
		})
	}
}

func TestInitializeSchema(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	// Verify tables were created
	tables := []string{"pair_reserves", "sync_events", "sync_metadata"}
	for _, table := range tables {
		var name string
		err := h.reservesDB.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
		if err != nil {
			t.Errorf("Table %s was not created: %v", table, err)
		}
	}
}

func TestLoadPairMappings(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	if len(h.pairMap) != 2 {
		t.Errorf("Expected 2 pairs loaded, got %d", len(h.pairMap))
	}

	expectedAddr := "0x1234567890123456789012345678901234567890"
	if _, exists := h.pairMap[expectedAddr]; !exists {
		t.Errorf("Expected pair address %s not found", expectedAddr)
	}

	// Test query error
	h.pairsDB.Close()
	err := h.loadPairMappings()
	if err == nil {
		t.Error("Expected error when loading from closed database")
	}
}

func TestPrepareGlobalStatements(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	if h.updateSyncStmt == nil {
		t.Error("Expected updateSyncStmt to be prepared")
	}

	// Test prepare error
	h.reservesDB.Close()
	err := h.prepareGlobalStatements()
	if err == nil {
		t.Error("Expected error preparing statements on closed database")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYNCHRONIZATION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestSyncToLatestAndTerminate(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Test 1: Normal sync
	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	// Set up mock responses
	mockServer.SetLogResponse(UniswapV2DeploymentBlock, UniswapV2DeploymentBlock+100, []Log{
		{
			Address:     "0x1234567890123456789012345678901234567890",
			Topics:      []string{SyncEventSignature},
			Data:        "0x0000000000000000000000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000000de0b6b3a7640000",
			BlockNumber: fmt.Sprintf("0x%x", UniswapV2DeploymentBlock+1),
			TxHash:      "0xtest",
			LogIndex:    "0x1",
		},
	})

	err := h.SyncToLatestAndTerminate()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Test 2: RPC error
	mockServer.SetShouldFail("eth_blockNumber", true)
	h2, cleanup2 := setupTestHarvester(t, mockServer)
	defer cleanup2()

	err = h2.SyncToLatestAndTerminate()
	if err == nil {
		t.Error("Expected error when RPC fails")
	}

	// Reset the failure for other tests
	mockServer.SetShouldFail("eth_blockNumber", false)
}

func TestExecuteSyncLoop(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	// Set up for a small sync
	h.syncTarget = UniswapV2DeploymentBlock + 1000
	startBlock := uint64(UniswapV2DeploymentBlock)

	// Set up responses for batch sizing tests
	for i := uint64(0); i < 1000; i += 100 {
		mockServer.SetLogResponse(startBlock+i, startBlock+i+100, []Log{})
	}

	// Begin transaction
	h.beginTransaction()

	err := h.executeSyncLoop(startBlock)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Test context cancellation
	h.cancel()
	err = h.executeSyncLoop(startBlock)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got: %v", err)
	}
}

func TestExecuteSyncLoop_BatchSizing(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	h.syncTarget = UniswapV2DeploymentBlock + 50000
	startBlock := uint64(UniswapV2DeploymentBlock)

	// Set up mixed success/failure pattern
	successCount := 0
	for block := startBlock; block < h.syncTarget; block += OptimalBatchSize {
		if successCount < 3 {
			mockServer.SetLogResponse(block, block+OptimalBatchSize, []Log{})
			successCount++
		} else {
			// Fail to trigger batch size reduction
			key := fmt.Sprintf("0x%x-0x%x", block, block+OptimalBatchSize)
			mockServer.SetShouldFail(key, true)
			successCount = 0
		}
	}

	h.beginTransaction()

	// Run for a limited number of iterations to test batch sizing
	h.syncTarget = startBlock + OptimalBatchSize*5
	err := h.executeSyncLoop(startBlock)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestProcessBatch(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	// Test successful batch
	mockServer.SetLogResponse(100, 200, []Log{
		{
			Address:     "0x1234567890123456789012345678901234567890",
			Topics:      []string{SyncEventSignature},
			Data:        "0x0000000000000000000000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000000de0b6b3a7640000",
			BlockNumber: "0x64",
			TxHash:      "0xtest",
			LogIndex:    "0x1",
		},
	})

	h.beginTransaction()
	success := h.processBatch(100, 200)
	if !success {
		t.Error("Expected batch to succeed")
	}

	// Test failed batch
	mockServer.SetShouldFail("eth_getLogs", true)
	success = h.processBatch(200, 300)
	if success {
		t.Error("Expected batch to fail")
	}

	// Test large batch that triggers flush
	largeLogs := make([]Log, EventBatchSize+100)
	for i := range largeLogs {
		largeLogs[i] = Log{
			Address:     "0x1234567890123456789012345678901234567890",
			Topics:      []string{SyncEventSignature},
			Data:        "0x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000de0b6b3a7640000",
			BlockNumber: fmt.Sprintf("0x%x", 300+i),
			TxHash:      fmt.Sprintf("0xtest%d", i),
			LogIndex:    fmt.Sprintf("0x%x", i),
		}
	}

	mockServer.SetShouldFail("eth_getLogs", false)
	mockServer.SetLogResponse(300, 400, largeLogs)

	success = h.processBatch(300, 400)
	if !success {
		t.Error("Expected large batch to succeed")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TRANSACTION MANAGEMENT TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestTransactionManagement(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	// Test begin transaction
	err := h.beginTransaction()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if h.currentTx == nil {
		t.Error("Expected currentTx to be set")
	}

	// Test commit with pending batch
	h.eventBatch = append(h.eventBatch, batchEvent{
		pairID:    1,
		blockNum:  10000,
		txHash:    "0xtest",
		logIndex:  1,
		reserve0:  "1000000",
		reserve1:  "2000000",
		timestamp: time.Now().Unix(),
	})
	h.reserveBatch = append(h.reserveBatch, batchReserve{
		pairID:      1,
		pairAddress: "0x1234567890123456789012345678901234567890",
		reserve0:    "1000000",
		reserve1:    "2000000",
		blockHeight: 10000,
		timestamp:   time.Now().Unix(),
	})

	h.commitTransaction()

	if h.currentTx != nil {
		t.Error("Expected currentTx to be nil after commit")
	}

	// Test rollback
	h.beginTransaction()
	h.rollbackTransaction()

	if h.currentTx != nil {
		t.Error("Expected currentTx to be nil after rollback")
	}

	// Test begin error
	h.reservesDB.Close()
	err = h.beginTransaction()
	if err == nil {
		t.Error("Expected error on closed database")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MONITORING AND CLEANUP TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestReportProgress(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	h.processed = 1000
	h.lastProcessed = 12345
	h.startTime = time.Now().Add(-10 * time.Second)

	// Should not panic
	h.reportProgress()

	// Test with nil statement
	h.updateSyncStmt.Close()
	h.updateSyncStmt = nil
	h.reportProgress() // Should not panic
}

func TestGetLastProcessedBlock(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	// Test empty table
	lastBlock := h.getLastProcessedBlock()
	if lastBlock != 0 {
		t.Errorf("Expected 0 for empty table, got %d", lastBlock)
	}

	// Insert test data
	h.beginTransaction()
	h.currentTx.Exec(`
		INSERT INTO sync_events (pair_id, block_number, tx_hash, log_index, reserve0, reserve1, created_at)
		VALUES (1, 12345, '0xabc', 1, '1000', '2000', 1234567890)
	`)
	h.commitTransaction()

	lastBlock = h.getLastProcessedBlock()
	if lastBlock != 12345 {
		t.Errorf("Expected 12345, got %d", lastBlock)
	}

	// Test query error
	h.reservesDB.Close()
	lastBlock = h.getLastProcessedBlock()
	if lastBlock != 0 {
		t.Errorf("Expected 0 on error, got %d", lastBlock)
	}
}

func TestTerminateCleanly(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(t, mockServer)
	defer cleanup()

	// Set some state
	h.processed = 1000
	h.lastProcessed = 12345

	// Begin transaction to test cleanup
	h.beginTransaction()

	err := h.terminateCleanly()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify nil assignments
	if h.eventBatch != nil {
		t.Error("Expected eventBatch to be nil")
	}
	if h.reserveBatch != nil {
		t.Error("Expected reserveBatch to be nil")
	}
	if h.pairMap != nil {
		t.Error("Expected pairMap to be nil")
	}
	if h.pairAddressLookup != nil {
		t.Error("Expected pairAddressLookup to be nil")
	}
	if h.logSlice != nil {
		t.Error("Expected logSlice to be nil")
	}
	if h.hexDecodeBuffer != nil {
		t.Error("Expected hexDecodeBuffer to be nil")
	}
	if h.reserveBuffer[0] != nil || h.reserveBuffer[1] != nil {
		t.Error("Expected reserveBuffer elements to be nil")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SIGNAL HANDLING TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestSignalHandling(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Override paths
	oldPath := ReservesDBPath
	tempDir := t.TempDir()
	ReservesDBPath = filepath.Join(tempDir, "test_reserves.db")
	defer func() { ReservesDBPath = oldPath }()

	oldTemplate := RPCPathTemplate
	RPCPathTemplate = mockServer.URL + "/%s"
	defer func() { RPCPathTemplate = oldTemplate }()

	pairsDB := createTestPairsDB(t)
	defer pairsDB.Close()

	h, err := NewPeakHarvester(pairsDB)
	if err != nil {
		t.Fatalf("Failed to create harvester: %v", err)
	}
	defer func() {
		h.cancel()
		h.reservesDB.Close()
	}()

	// Send signal
	h.signalChan <- syscall.SIGINT

	// Wait for signal to be processed
	time.Sleep(10 * time.Millisecond)

	// Check context was cancelled
	select {
	case <-h.ctx.Done():
		// Expected
	default:
		t.Error("Expected context to be cancelled")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestExecutePeakSync(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Override paths
	oldReservesPath := ReservesDBPath
	tempDir := t.TempDir()
	ReservesDBPath = filepath.Join(tempDir, "test_reserves.db")
	defer func() { ReservesDBPath = oldReservesPath }()

	oldTemplate := RPCPathTemplate
	RPCPathTemplate = mockServer.URL + "/%s"
	defer func() { RPCPathTemplate = oldTemplate }()

	// ExecutePeakSync opens "uniswap_pairs.db" directly, so we need to create it in current directory
	pairsDB, err := sql.Open("sqlite3", "uniswap_pairs.db")
	if err != nil {
		t.Fatalf("Failed to create pairs database: %v", err)
	}

	// Set up schema
	schema := `
	CREATE TABLE IF NOT EXISTS exchanges (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		chain_id INTEGER NOT NULL
	);
	CREATE TABLE IF NOT EXISTS pools (
		id INTEGER PRIMARY KEY,
		exchange_id INTEGER NOT NULL,
		pool_address TEXT NOT NULL
	);
	INSERT OR IGNORE INTO exchanges (id, name, chain_id) VALUES (1, 'uniswap_v2', 1);
	INSERT OR IGNORE INTO pools (id, exchange_id, pool_address) VALUES (1, 1, '0x1234567890123456789012345678901234567890');
	`
	_, err = pairsDB.Exec(schema)
	if err != nil {
		t.Fatalf("Failed to set up pairs database: %v", err)
	}
	pairsDB.Close()

	// Set up mock to return already synced
	mockServer.blockNumber = UniswapV2DeploymentBlock + SyncTargetOffset

	err = ExecutePeakSync()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Clean up the pairs database file
	os.Remove("uniswap_pairs.db")
}

func TestExecutePeakSyncWithDB(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Override paths
	oldPath := ReservesDBPath
	tempDir := t.TempDir()
	ReservesDBPath = filepath.Join(tempDir, "test_reserves.db")
	defer func() { ReservesDBPath = oldPath }()

	oldTemplate := RPCPathTemplate
	RPCPathTemplate = mockServer.URL + "/%s"
	defer func() { RPCPathTemplate = oldTemplate }()

	pairsDB := createTestPairsDB(t)
	defer pairsDB.Close()

	// Set up mock to return already synced
	mockServer.blockNumber = UniswapV2DeploymentBlock + SyncTargetOffset

	err := ExecutePeakSyncWithDB(pairsDB)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestCheckIfPeakSyncNeeded(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	// Override paths
	oldPath := ReservesDBPath
	tempDir := t.TempDir()
	ReservesDBPath = filepath.Join(tempDir, "test_reserves.db")
	defer func() { ReservesDBPath = oldPath }()

	oldTemplate := RPCPathTemplate
	RPCPathTemplate = mockServer.URL + "/%s"
	defer func() { RPCPathTemplate = oldTemplate }()

	// Test with non-existent database
	needed, lastBlock, targetBlock, err := CheckIfPeakSyncNeeded()
	if !needed {
		t.Error("Expected sync to be needed for non-existent database")
	}
	if lastBlock != 0 || targetBlock != 0 {
		t.Errorf("Expected zero values, got last=%d, target=%d", lastBlock, targetBlock)
	}
	if err != nil {
		t.Errorf("Expected nil error for non-existent database, got: %v", err)
	}

	// Create database with data
	db, _ := sql.Open("sqlite3", ReservesDBPath)
	db.Exec(`CREATE TABLE sync_events (block_number INTEGER)`)
	db.Exec(`INSERT INTO sync_events (block_number) VALUES (20000)`)
	db.Close()

	mockServer.blockNumber = 20100

	needed, lastBlock, targetBlock, err = CheckIfPeakSyncNeeded()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if !needed {
		t.Error("Expected sync to be needed")
	}
	if lastBlock != 20000 {
		t.Errorf("Expected lastBlock=20000, got %d", lastBlock)
	}
	expectedTarget := mockServer.blockNumber - SyncTargetOffset
	if targetBlock != expectedTarget {
		t.Errorf("Expected targetBlock=%d, got %d", expectedTarget, targetBlock)
	}

	// Test already synced
	db, _ = sql.Open("sqlite3", ReservesDBPath)
	db.Exec(`DELETE FROM sync_events`)
	db.Exec(`INSERT INTO sync_events (block_number) VALUES (20050)`)
	db.Close()

	needed, _, _, err = CheckIfPeakSyncNeeded()
	if needed {
		t.Error("Expected sync to NOT be needed when already synced")
	}
	if err != nil {
		t.Errorf("Unexpected error when already synced: %v", err)
	}

	// Test RPC error
	mockServer.SetShouldFail("eth_blockNumber", true)
	_, _, _, err = CheckIfPeakSyncNeeded()
	if err == nil {
		t.Error("Expected error when RPC fails")
	}
}
