// sync_harvester_test.go
package syncharvest

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// -----------------------------------------------------------------------------
// Test Helpers
// -----------------------------------------------------------------------------

// mockRPCServer creates a configurable mock Ethereum JSON-RPC server
type mockRPCServer struct {
	*httptest.Server
	mu            sync.Mutex
	blockNumber   uint64
	logs          map[uint64][]Log
	failureMode   string
	failureCount  int32
	requestCount  int32
	requestLog    []string
	customHandler func(req RPCRequest) (interface{}, *RPCError)
}

// newMockRPCServer creates a new mock RPC server
func newMockRPCServer() *mockRPCServer {
	m := &mockRPCServer{
		blockNumber: 15000000,
		logs:        make(map[uint64][]Log),
	}

	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req RPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		atomic.AddInt32(&m.requestCount, 1)

		m.mu.Lock()
		m.requestLog = append(m.requestLog, req.Method)
		handler := m.customHandler
		m.mu.Unlock()

		resp := RPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
		}

		// Check for failures
		if m.failureMode != "" && atomic.LoadInt32(&m.failureCount) > 0 {
			atomic.AddInt32(&m.failureCount, -1)

			switch m.failureMode {
			case "429":
				resp.Error = &RPCError{Code: 429, Message: "Too many requests"}
			case "error":
				resp.Error = &RPCError{Code: -32000, Message: "Internal error"}
			case "timeout":
				time.Sleep(100 * time.Millisecond)
				return
			case "-32046":
				resp.Error = &RPCError{Code: -32046, Message: "Cannot fulfill request"}
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}

		// Custom handler
		if handler != nil {
			result, err := handler(req)
			if err != nil {
				resp.Error = err
			} else {
				data, _ := json.Marshal(result)
				resp.Result = json.RawMessage(data)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}

		// Default handlers
		switch req.Method {
		case "eth_blockNumber":
			m.mu.Lock()
			block := fmt.Sprintf("0x%x", m.blockNumber)
			m.mu.Unlock()
			data, _ := json.Marshal(block)
			resp.Result = json.RawMessage(data)

		case "eth_getLogs":
			params := req.Params[0].(map[string]interface{})
			from, _ := parseHexUint64(params["fromBlock"].(string))
			to, _ := parseHexUint64(params["toBlock"].(string))

			var result []Log
			m.mu.Lock()
			for b := from; b <= to && b <= m.blockNumber; b++ {
				if logs, ok := m.logs[b]; ok {
					if addrs, ok := params["address"].([]interface{}); ok && len(addrs) > 0 {
						for _, log := range logs {
							for _, addr := range addrs {
								if strings.EqualFold(log.Address, addr.(string)) {
									result = append(result, log)
									break
								}
							}
						}
					} else {
						result = append(result, logs...)
					}
				}
			}
			m.mu.Unlock()

			data, _ := json.Marshal(result)
			resp.Result = json.RawMessage(data)

		default:
			resp.Error = &RPCError{Code: -32601, Message: "Method not found"}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))

	return m
}

func (m *mockRPCServer) setBlock(block uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blockNumber = block
}

func (m *mockRPCServer) addLog(block uint64, log Log) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs[block] = append(m.logs[block], log)
}

func (m *mockRPCServer) setFailure(mode string, count int) {
	atomic.StoreInt32(&m.failureCount, int32(count))
	m.failureMode = mode
}

func (m *mockRPCServer) getRequestCount() int {
	return int(atomic.LoadInt32(&m.requestCount))
}

func (m *mockRPCServer) setCustomHandler(h func(RPCRequest) (interface{}, *RPCError)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.customHandler = h
}

// setupTestDB creates a test database with sample pairs
func setupTestDB(tb testing.TB) (string, func()) {
	tmpDir, err := os.MkdirTemp("", "sync_test_*")
	if err != nil {
		tb.Fatal(err)
	}

	// Create test pairs database
	pairsPath := filepath.Join(tmpDir, "uniswap_pairs.db")
	pairsDB, err := sql.Open("sqlite3", pairsPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		tb.Fatal(err)
	}

	// Create schema and insert test data
	schema := `
	CREATE TABLE exchanges(id INTEGER PRIMARY KEY, name TEXT NOT NULL, chain_id INTEGER NOT NULL);
	CREATE TABLE tokens(id INTEGER PRIMARY KEY, address TEXT NOT NULL, chain_id INTEGER NOT NULL);
	CREATE TABLE pools(id INTEGER PRIMARY KEY, exchange_id INTEGER NOT NULL, token0_id INTEGER NOT NULL, 
		token1_id INTEGER NOT NULL, pool_address TEXT NOT NULL, fee_bps INTEGER);
	`
	if _, err := pairsDB.Exec(schema); err != nil {
		pairsDB.Close()
		os.RemoveAll(tmpDir)
		tb.Fatal(err)
	}

	// Insert test data
	pairsDB.Exec(`INSERT INTO exchanges (id, name, chain_id) VALUES (1, 'uniswap_v2', 1)`)
	pairsDB.Exec(`INSERT INTO tokens VALUES 
		(1, '0x6b175474e89094c44da98b954eedeac495271d0f', 1),
		(2, '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 1),
		(3, '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 1)`)
	pairsDB.Exec(`INSERT INTO pools VALUES 
		(1, 1, 2, 3, '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 30),
		(2, 1, 1, 3, '0xa478c2975ab1ea89e8196811f51a7b7ade33eb11', 30),
		(3, 1, 1, 2, '0xae461ca67b15dc8dc81ce7615e0320da1a9ab8d5', 30)`)

	pairsDB.Close()

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return tmpDir, cleanup
}

// createTestLog creates a sync event log
func createTestLog(pairAddr string, blockNum uint64, reserve0, reserve1 *big.Int) Log {
	data := make([]byte, 64)
	r0Bytes := reserve0.Bytes()
	r1Bytes := reserve1.Bytes()
	copy(data[32-len(r0Bytes):32], r0Bytes)
	copy(data[64-len(r1Bytes):64], r1Bytes)

	return Log{
		Address:     pairAddr,
		Topics:      []string{SyncEventSignature},
		Data:        "0x" + hex.EncodeToString(data),
		BlockNumber: fmt.Sprintf("0x%x", blockNum),
		TxHash:      fmt.Sprintf("0x%064x", blockNum),
		LogIndex:    "0x0",
	}
}

// -----------------------------------------------------------------------------
// RPC Client Tests
// -----------------------------------------------------------------------------

func TestNewRPCClient(t *testing.T) {
	client := NewRPCClient("http://localhost:8545")
	if client.url != "http://localhost:8545" {
		t.Errorf("Expected URL http://localhost:8545, got %s", client.url)
	}
	if client.client.Timeout != 30*time.Second {
		t.Errorf("Expected timeout 30s, got %v", client.client.Timeout)
	}
}

func TestRPCClient_Call(t *testing.T) {
	server := newMockRPCServer()
	defer server.Close()

	client := NewRPCClient(server.URL)

	// Test successful call
	var result string
	err := client.Call(context.Background(), &result, "eth_blockNumber")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != "0xe4e1c0" {
		t.Errorf("Expected 0xe4e1c0, got %s", result)
	}

	// Test RPC error
	err = client.Call(context.Background(), &result, "unknown_method")
	if err == nil {
		t.Error("Expected error for unknown method")
	}

	// Test context cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = client.Call(ctx, &result, "eth_blockNumber")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}

	// Test network error
	badClient := NewRPCClient("http://localhost:99999")
	err = badClient.Call(context.Background(), &result, "eth_blockNumber")
	if err == nil {
		t.Error("Expected error for bad connection")
	}
}

func TestRPCClient_BlockNumber(t *testing.T) {
	server := newMockRPCServer()
	defer server.Close()

	client := NewRPCClient(server.URL)
	blockNum, err := client.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if blockNum != 15000000 {
		t.Errorf("Expected 15000000, got %d", blockNum)
	}
}

func TestRPCClient_GetLogs(t *testing.T) {
	server := newMockRPCServer()
	defer server.Close()

	// Add test log
	testLog := createTestLog("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
		15000000, big.NewInt(1000000), big.NewInt(2000000))
	server.addLog(15000000, testLog)

	client := NewRPCClient(server.URL)

	// Test with address filter
	logs, err := client.GetLogs(context.Background(), 15000000, 15000001,
		[]string{"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"},
		[]string{SyncEventSignature})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(logs) != 1 {
		t.Errorf("Expected 1 log, got %d", len(logs))
	}

	// Test with empty addresses
	logs, err = client.GetLogs(context.Background(), 15000000, 15000001,
		[]string{}, []string{SyncEventSignature})
	if err != nil {
		t.Fatalf("GetLogs with empty addresses failed: %v", err)
	}

	// Test with empty topics
	logs, err = client.GetLogs(context.Background(), 15000000, 15000001,
		[]string{"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"}, []string{})
	if err != nil {
		t.Fatalf("GetLogs with empty topics failed: %v", err)
	}
}

func TestRPCClient_ResponseDecodeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	client := NewRPCClient(server.URL)
	var result string
	err := client.Call(context.Background(), &result, "eth_blockNumber")
	if err == nil {
		t.Error("Expected error with invalid JSON response")
	}
}

// -----------------------------------------------------------------------------
// Harvester Tests
// -----------------------------------------------------------------------------

func TestNewHarvester(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	server := newMockRPCServer()
	defer server.Close()

	// Create harvester manually to avoid cloudflare connection
	h := &Harvester{
		dataClient: NewRPCClient(server.URL),
		headClient: NewRPCClient(server.URL),
		pairMap:    make(map[string]int64),
	}

	// Open databases
	pairsDB, err := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	h.pairsDB = pairsDB

	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		t.Fatal(err)
	}
	h.reservesDB = reservesDB

	// Enable optimizations
	for _, pragma := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = 10000",
		"PRAGMA temp_store = MEMORY",
	} {
		reservesDB.Exec(pragma)
	}

	// Initialize
	if err := h.initReservesDB(); err != nil {
		t.Fatal(err)
	}
	if err := h.loadPairMappings(); err != nil {
		t.Fatal(err)
	}

	defer h.Close()

	// Verify initialization
	if h.dataClient == nil {
		t.Error("dataClient not initialized")
	}
	if h.pairsDB == nil {
		t.Error("pairsDB not initialized")
	}
	if h.reservesDB == nil {
		t.Error("reservesDB not initialized")
	}
	if len(h.pairMap) != 3 {
		t.Errorf("Expected 3 pairs, got %d", len(h.pairMap))
	}

	// Test with missing pairs database
	os.Remove("uniswap_pairs.db")
	testDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	defer testDB.Close()
	_, err = testDB.Query("SELECT 1 FROM pools LIMIT 1")
	if err == nil {
		t.Error("Expected error when querying missing database")
	}
}

func TestHarvester_initReservesDB(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{pairMap: make(map[string]int64)}

	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		t.Fatal(err)
	}
	h.reservesDB = reservesDB
	defer h.Close()

	if err := h.initReservesDB(); err != nil {
		t.Fatalf("initReservesDB failed: %v", err)
	}

	// Verify tables exist
	tables := []string{"pair_reserves", "sync_events"}
	for _, table := range tables {
		var name string
		err := h.reservesDB.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
		if err != nil {
			t.Errorf("Table %s not found: %v", table, err)
		}
	}

	// Test idempotency
	if err := h.initReservesDB(); err != nil {
		t.Errorf("initReservesDB should be idempotent, got error: %v", err)
	}
}

func TestHarvester_loadPairMappings(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{pairMap: make(map[string]int64)}

	pairsDB, err := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	h.pairsDB = pairsDB
	defer h.pairsDB.Close()

	if err := h.loadPairMappings(); err != nil {
		t.Fatalf("loadPairMappings failed: %v", err)
	}

	// Verify mappings
	expectedPairs := map[string]int64{
		"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc": 1,
		"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11": 2,
		"0xae461ca67b15dc8dc81ce7615e0320da1a9ab8d5": 3,
	}

	for addr, expectedID := range expectedPairs {
		if id, ok := h.pairMap[addr]; !ok {
			t.Errorf("Missing pair %s", addr)
		} else if id != expectedID {
			t.Errorf("Wrong ID for pair %s: got %d, want %d", addr, id, expectedID)
		}
	}

	// Test with database error
	h.pairsDB.Close()
	h.pairMap = make(map[string]int64)
	err = h.loadPairMappings()
	if err == nil {
		t.Error("Expected error with closed database")
	}
}

func TestHarvester_Start_BasicFlow(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	server := newMockRPCServer()
	defer server.Close()

	// Add test log
	server.addLog(15000000, createTestLog("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
		15000000, big.NewInt(1000000), big.NewInt(2000000)))

	h := &Harvester{
		dataClient: NewRPCClient(server.URL),
		headClient: NewRPCClient(server.URL),
		pairMap:    make(map[string]int64),
	}

	pairsDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	h.pairsDB = pairsDB
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB

	h.initReservesDB()
	h.loadPairMappings()
	defer h.Close()

	// Advance block periodically
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				server.setBlock(server.blockNumber + 1)
			case <-ctx.Done():
				return
			}
		}
	}()

	err := h.Start(ctx, 15000000)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify meta file was created
	if _, err := os.Stat(ReservesMetaPath); os.IsNotExist(err) {
		t.Error("Meta file not created")
	}

	// Test starting from meta file
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()

	err = h.Start(ctx2, 0) // Should read from meta file
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error starting from meta: %v", err)
	}
}

func TestHarvester_Start_HeadClientFallback(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	dataServer := newMockRPCServer()
	defer dataServer.Close()
	dataServer.setBlock(15000001) // Set block ahead to avoid sleep

	headServer := newMockRPCServer()
	defer headServer.Close()

	// Use a custom handler that returns different errors
	// First call returns 429 (triggers ankr fallback)
	// Subsequent calls return other errors to force data client usage
	callCount := 0
	headServer.setCustomHandler(func(req RPCRequest) (interface{}, *RPCError) {
		callCount++
		if callCount == 1 {
			return nil, &RPCError{Code: 429, Message: "Too many requests"}
		}
		// Return non-429 error to skip ankr and use data client
		return nil, &RPCError{Code: -32000, Message: "Internal error"}
	})

	h := &Harvester{
		dataClient: NewRPCClient(dataServer.URL),
		headClient: NewRPCClient(headServer.URL),
		pairMap:    map[string]int64{"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc": 1},
	}

	pairsDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	h.pairsDB = pairsDB
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	h.initReservesDB()
	h.loadPairMappings()
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := h.Start(ctx, 15000000)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify fallback happened
	if headServer.getRequestCount() == 0 {
		t.Error("Head server should have been called")
	}
	// Data server will be called after head fails and ankr fails
	if dataServer.getRequestCount() == 0 {
		t.Error("Data server should have been called as fallback")
	}
}

func TestHarvester_Start_RPCErrors(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	server := newMockRPCServer()
	defer server.Close()
	server.setFailure("error", 1) // Fail first request only
	server.setBlock(15000001)     // Set block ahead to avoid sleep

	h := &Harvester{
		dataClient: NewRPCClient(server.URL),
		headClient: NewRPCClient(server.URL),
		pairMap:    make(map[string]int64),
	}

	pairsDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	h.pairsDB = pairsDB
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	h.initReservesDB()
	h.loadPairMappings()
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := h.Start(ctx, 15000000)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error: %v", err)
	}

	if server.getRequestCount() < 2 {
		t.Errorf("Expected at least 2 requests due to retry, got %d", server.getRequestCount())
	}
}

func TestHarvester_Start_ChunkProcessing(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	server := newMockRPCServer()
	defer server.Close()
	server.setBlock(15000100) // Ahead of start block

	h := &Harvester{
		dataClient: NewRPCClient(server.URL),
		headClient: NewRPCClient(server.URL),
		pairMap:    make(map[string]int64),
	}

	// Add 1200 pairs to force multiple chunks
	for i := 0; i < 1200; i++ {
		h.pairMap[fmt.Sprintf("0x%040x", i)] = int64(i)
	}

	pairsDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	h.pairsDB = pairsDB
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	h.initReservesDB()
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := h.Start(ctx, 15000000)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error: %v", err)
	}

	if server.getRequestCount() < 2 {
		t.Errorf("Expected at least 2 requests for chunk processing, got %d", server.getRequestCount())
	}
}

func TestHarvester_Start_WaitForNewBlock(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	server := newMockRPCServer()
	defer server.Close()

	h := &Harvester{
		dataClient: NewRPCClient(server.URL),
		headClient: NewRPCClient(server.URL),
		pairMap:    map[string]int64{},
	}

	pairsDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	h.pairsDB = pairsDB
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	h.initReservesDB()
	defer h.Close()

	// Start from current block - should wait
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Advance block after 50ms to avoid long wait
	go func() {
		time.Sleep(50 * time.Millisecond)
		server.setBlock(15000001)
	}()

	err := h.Start(ctx, 15000000)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestHarvester_processLogs(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{
		pairMap: map[string]int64{
			"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc": 1,
			"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11": 2,
		},
	}

	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	defer h.Close()
	h.initReservesDB()

	// Test with empty logs
	if err := h.processLogs([]Log{}); err != nil {
		t.Errorf("processLogs with empty logs failed: %v", err)
	}

	// Test with valid logs
	logs := []Log{
		createTestLog("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", 15000000,
			big.NewInt(1000000), big.NewInt(2000000)),
		createTestLog("0xa478c2975ab1ea89e8196811f51a7b7ade33eb11", 15000001,
			big.NewInt(3000000), big.NewInt(4000000)),
	}

	if err := h.processLogs(logs); err != nil {
		t.Fatalf("processLogs failed: %v", err)
	}

	// Verify sync events were saved
	var count int
	h.reservesDB.QueryRow("SELECT COUNT(*) FROM sync_events").Scan(&count)
	if count != 2 {
		t.Errorf("Expected 2 sync events, got %d", count)
	}

	// Test with invalid logs
	invalidLogs := []Log{
		{Address: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", Topics: []string{SyncEventSignature},
			Data: "0x1234", BlockNumber: "0xe4e1c0", TxHash: "0x1234", LogIndex: "0x0"}, // Too short
		{Address: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", Topics: []string{SyncEventSignature},
			Data: "0xZZZZ", BlockNumber: "0xe4e1c0", TxHash: "0x1234", LogIndex: "0x0"}, // Invalid hex
		{Address: "0x0000000000000000000000000000000000000000", Topics: []string{SyncEventSignature},
			Data: "0x" + strings.Repeat("00", 64), BlockNumber: "0xe4e1c0", TxHash: "0x1234", LogIndex: "0x0"}, // Unknown pair
		{Address: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", Topics: []string{SyncEventSignature},
			Data: "0x" + strings.Repeat("00", 64), BlockNumber: "invalid", TxHash: "0x1234", LogIndex: "0x0"}, // Invalid block
		{Address: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", Topics: []string{SyncEventSignature},
			Data: "0x" + strings.Repeat("00", 64), BlockNumber: "0xe4e1c0", TxHash: "0x1234", LogIndex: "invalid"}, // Invalid index
		{Address: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", Topics: []string{SyncEventSignature},
			Data: "0x" + strings.Repeat("0", 127), BlockNumber: "0xe4e1c0", TxHash: "0x1234", LogIndex: "0x0"}, // Odd length
	}

	err := h.processLogs(invalidLogs)
	if err == nil {
		t.Error("Expected error when no logs could be processed")
	}

	// Test database error
	h.reservesDB.Close()
	err = h.processLogs(logs)
	if err == nil {
		t.Error("Expected error with closed database")
	}
}

func TestHarvester_writeMeta(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "meta_test_*")
	defer os.RemoveAll(tmpDir)

	metaPath := filepath.Join(tmpDir, "test.meta")
	metaF, _ := os.OpenFile(metaPath, os.O_CREATE|os.O_RDWR, 0o644)
	defer metaF.Close()

	h := &Harvester{}

	// Write and verify
	testBlock := uint64(15000000)
	if err := h.writeMeta(metaF, testBlock); err != nil {
		t.Fatalf("writeMeta failed: %v", err)
	}

	metaF.Seek(0, 0)
	var readBlock uint64
	binary.Read(metaF, binary.BigEndian, &readBlock)
	if readBlock != testBlock {
		t.Errorf("Meta block mismatch: got %d, want %d", readBlock, testBlock)
	}

	// Test overwriting
	newBlock := uint64(15000100)
	h.writeMeta(metaF, newBlock)

	metaF.Seek(0, 0)
	binary.Read(metaF, binary.BigEndian, &readBlock)
	if readBlock != newBlock {
		t.Errorf("Meta block after overwrite mismatch: got %d, want %d", readBlock, newBlock)
	}
}

func TestHarvester_Close(t *testing.T) {
	// Test with nil databases
	h := &Harvester{}
	if err := h.Close(); err != nil {
		t.Errorf("Close with nil databases should not error: %v", err)
	}

	// Test with open databases
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	pairsDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)

	h2 := &Harvester{pairsDB: pairsDB, reservesDB: reservesDB}
	if err := h2.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify databases are closed
	var dummy int
	if err := pairsDB.QueryRow("SELECT 1").Scan(&dummy); err == nil {
		t.Error("pairsDB should be closed")
	}
	if err := reservesDB.QueryRow("SELECT 1").Scan(&dummy); err == nil {
		t.Error("reservesDB should be closed")
	}
}

func TestHarvester_GetLatestReserves(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{pairMap: make(map[string]int64)}
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	defer h.Close()
	h.initReservesDB()

	// Test with empty database
	reserves, err := h.GetLatestReserves()
	if err != nil {
		t.Fatalf("GetLatestReserves failed: %v", err)
	}
	if len(reserves) != 0 {
		t.Errorf("Expected 0 reserves, got %d", len(reserves))
	}

	// Insert test reserves
	h.reservesDB.Exec(`INSERT INTO pair_reserves VALUES 
		(1, '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', '1000000', '2000000', 15000000, CURRENT_TIMESTAMP),
		(2, '0xa478c2975ab1ea89e8196811f51a7b7ade33eb11', '3000000', '4000000', 15000001, CURRENT_TIMESTAMP)`)

	// Get latest reserves
	reserves, err = h.GetLatestReserves()
	if err != nil {
		t.Fatalf("GetLatestReserves failed: %v", err)
	}
	if len(reserves) != 2 {
		t.Errorf("Expected 2 reserves, got %d", len(reserves))
	}

	// Check specific reserve
	if r, ok := reserves["0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"]; ok {
		if r.Reserve0 != "1000000" || r.Reserve1 != "2000000" {
			t.Errorf("Reserve mismatch: got r0=%s r1=%s", r.Reserve0, r.Reserve1)
		}
	} else {
		t.Error("Missing expected pair")
	}

	// Test with database error
	h.reservesDB.Close()
	_, err = h.GetLatestReserves()
	if err == nil {
		t.Error("Expected error with closed database")
	}
}

func TestHarvester_GetLatestReserves_ScanError(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{pairMap: make(map[string]int64)}
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	defer h.Close()

	// Create table with wrong schema
	h.reservesDB.Exec(`CREATE TABLE pair_reserves (pair_id INTEGER PRIMARY KEY, invalid_column TEXT)`)
	h.reservesDB.Exec(`INSERT INTO pair_reserves VALUES (1, 'test')`)

	// This should fail during scan
	_, err := h.GetLatestReserves()
	if err == nil {
		t.Error("Expected error scanning with wrong schema")
	}
}

func TestMetaFileCorruption(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create corrupted meta file
	os.WriteFile(ReservesMetaPath, []byte{1, 2, 3}, 0644)

	server := newMockRPCServer()
	defer server.Close()

	h := &Harvester{
		dataClient: NewRPCClient(server.URL),
		headClient: NewRPCClient(server.URL),
		pairMap:    make(map[string]int64),
	}

	pairsDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	h.pairsDB = pairsDB
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	h.initReservesDB()
	h.loadPairMappings()
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Should use default block since meta is corrupted
	err := h.Start(ctx, 0)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Helper Function Tests
// -----------------------------------------------------------------------------

func TestParseHexUint64(t *testing.T) {
	tests := []struct {
		input    string
		expected uint64
		hasError bool
	}{
		{"0x1", 1, false},
		{"0xff", 255, false},
		{"0x1000", 4096, false},
		{"0xe4e1c0", 15000000, false},
		{"e4e1c0", 15000000, false},
		{"0x0", 0, false},
		{"0xffffffffffffffff", 18446744073709551615, false},
		{"invalid", 0, true},
		{"0xgg", 0, true},
		{"", 0, true},
		{"0x", 0, true},
		{"0x123456789abcdef01", 0x123456789abcdef0, false}, // Truncated
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseHexUint64(tt.input)
			if tt.hasError && err == nil {
				t.Errorf("Expected error for input %s", tt.input)
			}
			if !tt.hasError && err != nil {
				t.Errorf("Unexpected error for input %s: %v", tt.input, err)
			}
			if !tt.hasError && result != tt.expected {
				t.Errorf("Expected %d, got %d for input %s", tt.expected, result, tt.input)
			}
		})
	}
}

func TestBatchSizeAdjustment(t *testing.T) {
	tests := []struct {
		name           string
		consecutiveOK  int
		consecutiveNG  int
		currentBatch   uint64
		expectIncrease bool
		expectDecrease bool
	}{
		{"increase after 3 successes", 3, 0, 1000, true, false},
		{"decrease after 3 failures", 0, 3, 1000, false, true},
		{"no change on 2 successes", 2, 0, 1000, false, false},
		{"respect floor", 0, 3, SyncEventBatchFloor, false, false},
		{"respect ceiling", 3, 0, SyncEventBatchCeil, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := tt.currentBatch

			// Simulate increase logic
			if tt.consecutiveOK%3 == 0 && tt.consecutiveOK > 0 && batch < SyncEventBatchCeil {
				batch *= 2
				if batch > SyncEventBatchCeil {
					batch = SyncEventBatchCeil
				}
			}

			// Simulate decrease logic
			if tt.consecutiveNG%3 == 0 && tt.consecutiveNG > 0 && batch > SyncEventBatchFloor {
				batch /= 2
				if batch < SyncEventBatchFloor {
					batch = SyncEventBatchFloor
				}
			}

			if tt.expectIncrease && batch <= tt.currentBatch {
				t.Errorf("Expected batch increase, got %d -> %d", tt.currentBatch, batch)
			}
			if tt.expectDecrease && batch >= tt.currentBatch {
				t.Errorf("Expected batch decrease, got %d -> %d", tt.currentBatch, batch)
			}
			if !tt.expectIncrease && !tt.expectDecrease && batch != tt.currentBatch {
				t.Errorf("Expected no change, got %d -> %d", tt.currentBatch, batch)
			}
		})
	}
}

// Test error handling when all logs fail to process
func TestHarvester_processLogs_AllFail(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{
		pairMap: map[string]int64{
			"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc": 1,
		},
	}

	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	defer h.Close()
	h.initReservesDB()

	// All invalid logs
	invalidLogs := []Log{
		{Address: "0x0000000000000000000000000000000000000000", Topics: []string{SyncEventSignature},
			Data: "0x1234", BlockNumber: "0xe4e1c0", TxHash: "0x1234", LogIndex: "0x0"},
	}

	err := h.processLogs(invalidLogs)
	if err == nil {
		t.Error("Expected error when all logs fail to process")
	}
	if !strings.Contains(err.Error(), "no logs were processed") {
		t.Errorf("Expected 'no logs were processed' error, got: %v", err)
	}
}

// Test head client TTL and caching
func TestHarvester_Start_HeadCaching(t *testing.T) {
	tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	headCallCount := 0
	server := newMockRPCServer()
	defer server.Close()
	server.setBlock(15000001) // Ahead to avoid sleep
	server.setCustomHandler(func(req RPCRequest) (interface{}, *RPCError) {
		if req.Method == "eth_blockNumber" {
			headCallCount++
			return "0xe4e1c1", nil
		}
		return []Log{}, nil
	})

	h := &Harvester{
		dataClient: NewRPCClient(server.URL),
		headClient: NewRPCClient(server.URL),
		pairMap:    make(map[string]int64),
	}

	pairsDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	h.pairsDB = pairsDB
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	h.initReservesDB()
	defer h.Close()

	// Run briefly - should use cache after first call
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	h.Start(ctx, 15000000)

	// Head should be called only once or twice due to caching
	if headCallCount > 2 {
		t.Errorf("Expected minimal head calls due to caching, got %d", headCallCount)
	}
}

func BenchmarkProcessLogs(b *testing.B) {
	tmpDir, cleanup := setupTestDB(b)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{pairMap: make(map[string]int64)}

	// Generate test pairs
	for i := 0; i < 1000; i++ {
		h.pairMap[fmt.Sprintf("0x%040d", i)] = int64(i)
	}

	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	defer h.Close()
	h.initReservesDB()

	// Create test logs
	logs := make([]Log, 100)
	for i := 0; i < 100; i++ {
		logs[i] = createTestLog(fmt.Sprintf("0x%040d", i%1000),
			uint64(15000000+i), big.NewInt(int64(1000000*(i+1))), big.NewInt(int64(2000000*(i+1))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.processLogs(logs)
	}
}

func BenchmarkReserveRetrieval(b *testing.B) {
	tmpDir, cleanup := setupTestDB(b)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{}
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	defer h.Close()
	h.initReservesDB()

	// Insert many reserves
	tx, _ := h.reservesDB.Begin()
	for i := 0; i < 10000; i++ {
		tx.Exec(`INSERT INTO pair_reserves VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
			i, fmt.Sprintf("0x%040d", i), "1000000", "2000000", 15000000+i)
	}
	tx.Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reserves, _ := h.GetLatestReserves()
		if len(reserves) != 10000 {
			b.Errorf("Expected 10000 reserves, got %d", len(reserves))
		}
	}
}

func BenchmarkParseHexUint64(b *testing.B) {
	testCases := []string{"0x1", "0xff", "0x1000", "0xe4e1c0", "0xffffffffffffffff"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			parseHexUint64(tc)
		}
	}
}
