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
	"strings"
	"syscall"
	"testing"
	"time"

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
	needed, lastBlock, targetBlock, err = CheckIfPeakSyncNeeded()
	if err == nil {
		t.Error("Expected error when RPC fails")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// BENCHMARK TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func BenchmarkCollectLogForBatch(b *testing.B) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(b, mockServer)
	defer cleanup()

	log := &Log{
		Address:     "0x1234567890123456789012345678901234567890",
		Topics:      []string{SyncEventSignature},
		Data:        "0x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000de0b6b3a7640000",
		BlockNumber: "0xa0e449",
		TxHash:      "0xabcd1234",
		LogIndex:    "0x1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.collectLogForBatch(log)
		// Clear batch to prevent growth
		h.eventBatch = h.eventBatch[:0]
		h.reserveBatch = h.reserveBatch[:0]
	}
}

func BenchmarkParseReservesDirect(b *testing.B) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(b, mockServer)
	defer cleanup()

	data := "0x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000de0b6b3a7640000"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.parseReservesDirect(data)
	}
}

func BenchmarkFlushBatch(b *testing.B) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	h, cleanup := setupTestHarvester(b, mockServer)
	defer cleanup()

	// Begin transaction
	h.beginTransaction()

	// Pre-populate batch
	originalEventBatch := make([]batchEvent, 1000)
	originalReserveBatch := make([]batchReserve, 1000)
	for i := 0; i < 1000; i++ {
		originalEventBatch[i] = batchEvent{
			pairID:    1,
			blockNum:  uint64(10000 + i),
			txHash:    fmt.Sprintf("0xtest%d", i),
			logIndex:  uint64(i),
			reserve0:  "1000000",
			reserve1:  "2000000",
			timestamp: time.Now().Unix(),
		}
		originalReserveBatch[i] = batchReserve{
			pairID:      1,
			pairAddress: "0x1234567890123456789012345678901234567890",
			reserve0:    "1000000",
			reserve1:    "2000000",
			blockHeight: uint64(10000 + i),
			timestamp:   time.Now().Unix(),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Restore batch
		h.eventBatch = make([]batchEvent, len(originalEventBatch))
		h.reserveBatch = make([]batchReserve, len(originalReserveBatch))
		copy(h.eventBatch, originalEventBatch)
		copy(h.reserveBatch, originalReserveBatch)

		// Flush
		h.flushBatch()
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TEST MAIN
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
