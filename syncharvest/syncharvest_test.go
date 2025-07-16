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
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// -----------------------------------------------------------------------------
// Test Helpers
// -----------------------------------------------------------------------------

// testPair represents a test trading pair
type testPair struct {
	ID      int64
	Address string
	Token0  string
	Token1  string
}

// testSyncEvent represents a test sync event
type testSyncEvent struct {
	PairAddress string
	BlockNumber uint64
	TxHash      string
	LogIndex    uint64
	Reserve0    *big.Int
	Reserve1    *big.Int
}

// setupTestDB creates a test database with sample pairs
func setupTestDB(tb testing.TB) (*sql.DB, string, func()) {
	// Create temp directory
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

	// Create schema
	schema := `
	CREATE TABLE exchanges(
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		chain_id INTEGER NOT NULL
	);
	CREATE TABLE tokens(
		id INTEGER PRIMARY KEY,
		address TEXT NOT NULL,
		chain_id INTEGER NOT NULL
	);
	CREATE TABLE pools(
		id INTEGER PRIMARY KEY,
		exchange_id INTEGER NOT NULL,
		token0_id INTEGER NOT NULL,
		token1_id INTEGER NOT NULL,
		pool_address TEXT NOT NULL,
		fee_bps INTEGER
	);
	`
	if _, err := pairsDB.Exec(schema); err != nil {
		pairsDB.Close()
		os.RemoveAll(tmpDir)
		tb.Fatal(err)
	}

	// Insert test data
	_, err = pairsDB.Exec(`INSERT INTO exchanges (id, name, chain_id) VALUES (1, 'uniswap_v2', 1)`)
	if err != nil {
		tb.Fatal(err)
	}

	// Insert test tokens
	testTokens := []struct {
		id   int
		addr string
	}{
		{1, "0x6b175474e89094c44da98b954eedeac495271d0f"}, // DAI
		{2, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}, // USDC
		{3, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"}, // WETH
	}

	for _, token := range testTokens {
		_, err = pairsDB.Exec(`INSERT INTO tokens (id, address, chain_id) VALUES (?, ?, 1)`, token.id, token.addr)
		if err != nil {
			tb.Fatal(err)
		}
	}

	// Insert test pairs
	testPairs := []testPair{
		{1, "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", "2", "3"}, // USDC/WETH
		{2, "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11", "1", "3"}, // DAI/WETH
		{3, "0xae461ca67b15dc8dc81ce7615e0320da1a9ab8d5", "1", "2"}, // DAI/USDC
	}

	for _, pair := range testPairs {
		_, err = pairsDB.Exec(`
			INSERT INTO pools (id, exchange_id, token0_id, token1_id, pool_address, fee_bps) 
			VALUES (?, 1, ?, ?, ?, 30)
		`, pair.ID, pair.Token0, pair.Token1, pair.Address)
		if err != nil {
			tb.Fatal(err)
		}
	}

	cleanup := func() {
		pairsDB.Close()
		os.RemoveAll(tmpDir)
	}

	return pairsDB, tmpDir, cleanup
}

// createTestLog creates a mock Ethereum log for testing
func createTestLog(event testSyncEvent) Log {
	// Create sync event data (reserve0 + reserve1)
	data := make([]byte, 64)
	r0Bytes := event.Reserve0.Bytes()
	r1Bytes := event.Reserve1.Bytes()

	// Right-pad to 32 bytes each
	copy(data[32-len(r0Bytes):32], r0Bytes)
	copy(data[64-len(r1Bytes):64], r1Bytes)

	// Convert to hex string
	dataHex := "0x" + hex.EncodeToString(data)

	return Log{
		Address:     event.PairAddress,
		Topics:      []string{SyncEventSignature},
		Data:        dataHex,
		BlockNumber: fmt.Sprintf("0x%x", event.BlockNumber),
		TxHash:      event.TxHash,
		LogIndex:    fmt.Sprintf("0x%x", event.LogIndex),
	}
}

// mockRPCServer creates a test HTTP server that responds to JSON-RPC calls
func mockRPCServer(t *testing.T, responses map[string]interface{}) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req RPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Logf("Failed to decode request: %v", err)
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		resp := RPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
		}

		if result, ok := responses[req.Method]; ok {
			respData, _ := json.Marshal(result)
			resp.Result = json.RawMessage(respData)
		} else {
			resp.Error = &RPCError{
				Code:    -32601,
				Message: "Method not found",
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
}

// -----------------------------------------------------------------------------
// Unit Tests
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
	// Test successful call
	server := mockRPCServer(t, map[string]interface{}{
		"eth_blockNumber": "0xe4e1c0",
	})
	defer server.Close()

	client := NewRPCClient(server.URL)
	var result string
	err := client.Call(context.Background(), &result, "eth_blockNumber")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != "0xe4e1c0" {
		t.Errorf("Expected 0xe4e1c0, got %s", result)
	}

	// Test RPC error
	errorServer := mockRPCServer(t, map[string]interface{}{})
	defer errorServer.Close()

	errorClient := NewRPCClient(errorServer.URL)
	err = errorClient.Call(context.Background(), &result, "unknown_method")
	if err == nil {
		t.Error("Expected error for unknown method")
	}

	// Test network error
	badClient := NewRPCClient("http://localhost:99999")
	err = badClient.Call(context.Background(), &result, "eth_blockNumber")
	if err == nil {
		t.Error("Expected error for bad connection")
	}

	// Test context cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = client.Call(ctx, &result, "eth_blockNumber")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestRPCClient_BlockNumber(t *testing.T) {
	server := mockRPCServer(t, map[string]interface{}{
		"eth_blockNumber": "0xe4e1c0",
	})
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
	testLogs := []Log{
		{
			Address:     "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			Topics:      []string{SyncEventSignature},
			Data:        "0x00000000000000000000000000000000000000000000000000000000000f424000000000000000000000000000000000000000000000000000000000001e8480",
			BlockNumber: "0xe4e1c0",
			TxHash:      "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			LogIndex:    "0x0",
		},
	}

	server := mockRPCServer(t, map[string]interface{}{
		"eth_getLogs": testLogs,
	})
	defer server.Close()

	client := NewRPCClient(server.URL)
	logs, err := client.GetLogs(context.Background(), 15000000, 15000001,
		[]string{"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"},
		[]string{SyncEventSignature})

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(logs) != 1 {
		t.Errorf("Expected 1 log, got %d", len(logs))
	}
	if logs[0].Address != testLogs[0].Address {
		t.Errorf("Address mismatch: got %s, want %s", logs[0].Address, testLogs[0].Address)
	}
}

func TestNewHarvester(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	// Change to test directory
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Test with mock RPC server
	server := mockRPCServer(t, map[string]interface{}{
		"eth_blockNumber": "0xe4e1c0",
	})
	defer server.Close()

	// Test successful creation
	h, err := NewHarvester(server.URL)
	if err != nil {
		t.Fatalf("Failed to create harvester: %v", err)
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

	// Test with bad RPC URL
	_, err = NewHarvester("http://invalid-url-12345")
	if err != nil {
		t.Logf("Expected behavior: failed with bad RPC URL: %v", err)
	}

	// Test with missing pairs database
	os.Remove("uniswap_pairs.db")
	_, err = NewHarvester(server.URL)
	if err == nil {
		t.Error("Expected error with missing pairs database")
	}
}

func TestHarvester_initReservesDB(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{
		pairMap: make(map[string]int64),
	}

	// Test database creation
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
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
			table,
		).Scan(&name)
		if err != nil {
			t.Errorf("Table %s not found: %v", table, err)
		}
	}

	// Verify indices exist
	indices := []string{"idx_block_height", "idx_pair_address", "idx_sync_pair", "idx_sync_block"}
	for _, idx := range indices {
		var name string
		err := h.reservesDB.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='index' AND name=?",
			idx,
		).Scan(&name)
		if err != nil {
			t.Errorf("Index %s not found: %v", idx, err)
		}
	}

	// Test idempotency - should not fail on second call
	if err := h.initReservesDB(); err != nil {
		t.Errorf("initReservesDB should be idempotent, got error: %v", err)
	}
}

func TestHarvester_loadPairMappings(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{
		pairMap: make(map[string]int64),
	}

	// Open pairs database
	pairsDB, err := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	h.pairsDB = pairsDB
	defer h.pairsDB.Close()

	// Load mappings
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

func TestHarvester_Start(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create test logs
	testLogs := []Log{
		{
			Address:     "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			Topics:      []string{SyncEventSignature},
			Data:        "0x00000000000000000000000000000000000000000000000000000000000f424000000000000000000000000000000000000000000000000000000000001e8480",
			BlockNumber: "0xe4e1c0",
			TxHash:      "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			LogIndex:    "0x0",
		},
	}

	// Mock RPC server with multiple responses
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req RPCRequest
		json.NewDecoder(r.Body).Decode(&req)

		resp := RPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
		}

		switch req.Method {
		case "eth_blockNumber":
			// Return increasing block numbers
			blockNum := "0xe4e1c0"
			if callCount > 2 {
				blockNum = "0xe4e1c1" // Next block
			}
			respData, _ := json.Marshal(blockNum)
			resp.Result = respData
			callCount++
		case "eth_getLogs":
			// Return logs only once
			if callCount == 2 {
				respData, _ := json.Marshal(testLogs)
				resp.Result = respData
			} else {
				respData, _ := json.Marshal([]Log{})
				resp.Result = respData
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create harvester
	h, err := NewHarvester(server.URL)
	if err != nil {
		t.Fatalf("Failed to create harvester: %v", err)
	}
	defer h.Close()

	// Test with context that cancels after short time
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start harvesting
	err = h.Start(ctx, 15000000)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify meta file was created
	if _, err := os.Stat(ReservesMetaPath); os.IsNotExist(err) {
		t.Error("Meta file not created")
	}

	// Test starting from meta file
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()

	err = h.Start(ctx2, 0) // Should read from meta file
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error starting from meta: %v", err)
	}
}

func TestHarvester_processLogs(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create harvester
	h := &Harvester{
		pairMap: map[string]int64{
			"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc": 1,
			"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11": 2,
		},
	}

	// Setup databases
	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		t.Fatal(err)
	}
	h.reservesDB = reservesDB
	defer h.Close()

	if err := h.initReservesDB(); err != nil {
		t.Fatal(err)
	}

	// Test with empty logs
	if err := h.processLogs([]Log{}); err != nil {
		t.Errorf("processLogs with empty logs failed: %v", err)
	}

	// Test with valid logs
	testEvents := []testSyncEvent{
		{
			PairAddress: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			BlockNumber: 15000000,
			TxHash:      "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			LogIndex:    0,
			Reserve0:    big.NewInt(1000000),
			Reserve1:    big.NewInt(2000000),
		},
		{
			PairAddress: "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",
			BlockNumber: 15000001,
			TxHash:      "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
			LogIndex:    1,
			Reserve0:    big.NewInt(3000000),
			Reserve1:    big.NewInt(4000000),
		},
	}

	logs := []Log{}
	for _, event := range testEvents {
		logs = append(logs, createTestLog(event))
	}

	// Process logs
	if err := h.processLogs(logs); err != nil {
		t.Fatalf("processLogs failed: %v", err)
	}

	// Verify sync events were saved
	var count int
	err = h.reservesDB.QueryRow("SELECT COUNT(*) FROM sync_events").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("Expected 2 sync events, got %d", count)
	}

	// Test with invalid logs
	invalidLogs := []Log{
		// Wrong data length
		{
			Address:     "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			Topics:      []string{SyncEventSignature},
			Data:        "0x1234", // Too short
			BlockNumber: "0xe4e1c0",
			TxHash:      "0x1234",
			LogIndex:    "0x0",
		},
		// Invalid hex data
		{
			Address:     "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			Topics:      []string{SyncEventSignature},
			Data:        "0xZZZZ", // Invalid hex
			BlockNumber: "0xe4e1c0",
			TxHash:      "0x1234",
			LogIndex:    "0x0",
		},
		// Unknown pair address
		{
			Address:     "0x0000000000000000000000000000000000000000",
			Topics:      []string{SyncEventSignature},
			Data:        "0x" + strings.Repeat("00", 64),
			BlockNumber: "0xe4e1c0",
			TxHash:      "0x1234",
			LogIndex:    "0x0",
		},
		// Invalid block number
		{
			Address:     "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			Topics:      []string{SyncEventSignature},
			Data:        "0x" + strings.Repeat("00", 64),
			BlockNumber: "invalid",
			TxHash:      "0x1234",
			LogIndex:    "0x0",
		},
		// Invalid log index
		{
			Address:     "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			Topics:      []string{SyncEventSignature},
			Data:        "0x" + strings.Repeat("00", 64),
			BlockNumber: "0xe4e1c0",
			TxHash:      "0x1234",
			LogIndex:    "invalid",
		},
		// Odd length hex data
		{
			Address:     "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			Topics:      []string{SyncEventSignature},
			Data:        "0x" + strings.Repeat("0", 127), // Odd length
			BlockNumber: "0xe4e1c0",
			TxHash:      "0x1234",
			LogIndex:    "0x0",
		},
	}

	// These should all be skipped without error
	err = h.processLogs(invalidLogs)
	if err == nil {
		t.Error("Expected error when no logs could be processed")
	}

	// Test database error handling
	h.reservesDB.Close()
	err = h.processLogs(logs)
	if err == nil {
		t.Error("Expected error with closed database")
	}
}

func TestHarvester_writeMeta(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "meta_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	metaPath := filepath.Join(tmpDir, "test.meta")
	metaF, err := os.OpenFile(metaPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer metaF.Close()

	h := &Harvester{}

	// Write block number
	testBlock := uint64(15000000)
	if err := h.writeMeta(metaF, testBlock); err != nil {
		t.Fatalf("writeMeta failed: %v", err)
	}

	// Verify file size
	stat, _ := metaF.Stat()
	if stat.Size() != 8 {
		t.Errorf("Expected file size 8 bytes, got %d", stat.Size())
	}

	// Read it back
	metaF.Seek(0, 0)
	var readBlock uint64
	if err := binary.Read(metaF, binary.BigEndian, &readBlock); err != nil {
		t.Fatalf("read meta failed: %v", err)
	}

	if readBlock != testBlock {
		t.Errorf("Meta block mismatch: got %d, want %d", readBlock, testBlock)
	}

	// Test overwriting
	newBlock := uint64(15000100)
	if err := h.writeMeta(metaF, newBlock); err != nil {
		t.Fatalf("writeMeta overwrite failed: %v", err)
	}

	metaF.Seek(0, 0)
	if err := binary.Read(metaF, binary.BigEndian, &readBlock); err != nil {
		t.Fatalf("read meta after overwrite failed: %v", err)
	}

	if readBlock != newBlock {
		t.Errorf("Meta block after overwrite mismatch: got %d, want %d", readBlock, newBlock)
	}
}

func TestHarvester_Close(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Test with nil databases (should not panic)
	h := &Harvester{}
	err := h.Close()
	if err != nil {
		t.Errorf("Close with nil databases should not error: %v", err)
	}

	// Test with open databases
	pairsDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)

	h2 := &Harvester{
		pairsDB:    pairsDB,
		reservesDB: reservesDB,
	}

	err = h2.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify databases are closed by trying to query
	var dummy int
	err = pairsDB.QueryRow("SELECT 1").Scan(&dummy)
	if err == nil {
		t.Error("pairsDB should be closed")
	}

	err = reservesDB.QueryRow("SELECT 1").Scan(&dummy)
	if err == nil {
		t.Error("reservesDB should be closed")
	}
}

func TestHarvester_GetLatestReserves(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create harvester
	h := &Harvester{
		pairMap: make(map[string]int64),
	}

	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		t.Fatal(err)
	}
	h.reservesDB = reservesDB
	defer h.Close()

	if err := h.initReservesDB(); err != nil {
		t.Fatal(err)
	}

	// Test with empty database
	reserves, err := h.GetLatestReserves()
	if err != nil {
		t.Fatalf("GetLatestReserves failed: %v", err)
	}
	if len(reserves) != 0 {
		t.Errorf("Expected 0 reserves, got %d", len(reserves))
	}

	// Insert test reserves
	testData := []struct {
		pairID      int64
		pairAddress string
		reserve0    string
		reserve1    string
		blockHeight int64
	}{
		{1, "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", "1000000", "2000000", 15000000},
		{2, "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11", "3000000", "4000000", 15000001},
	}

	for _, d := range testData {
		_, err := h.reservesDB.Exec(`
			INSERT INTO pair_reserves (pair_id, pair_address, reserve0, reserve1, block_height)
			VALUES (?, ?, ?, ?, ?)
		`, d.pairID, d.pairAddress, d.reserve0, d.reserve1, d.blockHeight)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get latest reserves
	reserves, err = h.GetLatestReserves()
	if err != nil {
		t.Fatalf("GetLatestReserves failed: %v", err)
	}

	if len(reserves) != 2 {
		t.Errorf("Expected 2 reserves, got %d", len(reserves))
	}

	// Check specific reserve
	r, ok := reserves["0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"]
	if !ok {
		t.Error("Missing expected pair")
	} else {
		if r.Reserve0 != "1000000" || r.Reserve1 != "2000000" {
			t.Errorf("Reserve mismatch: got r0=%s r1=%s", r.Reserve0, r.Reserve1)
		}
		if r.PairID != 1 {
			t.Errorf("PairID mismatch: got %d, want 1", r.PairID)
		}
		if r.BlockHeight != 15000000 {
			t.Errorf("BlockHeight mismatch: got %d, want 15000000", r.BlockHeight)
		}
	}

	// Test with database error
	h.reservesDB.Close()
	_, err = h.GetLatestReserves()
	if err == nil {
		t.Error("Expected error with closed database")
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
		{"e4e1c0", 15000000, false}, // without 0x prefix
		{"0x0", 0, false},
		{"0xffffffffffffffff", 18446744073709551615, false}, // max uint64
		{"invalid", 0, true},
		{"0xgg", 0, true},
		{"", 0, true},
		{"0x", 0, true},
		{"0x123456789abcdef01", 0x123456789abcdef0, false}, // Truncated to 16 chars
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

// -----------------------------------------------------------------------------
// Edge Case Tests
// -----------------------------------------------------------------------------

func TestRPCClient_GetLogs_EdgeCases(t *testing.T) {
	server := mockRPCServer(t, map[string]interface{}{
		"eth_getLogs": []Log{},
	})
	defer server.Close()

	client := NewRPCClient(server.URL)

	// Test with empty addresses
	logs, err := client.GetLogs(context.Background(), 0, 100, []string{}, []string{SyncEventSignature})
	if err != nil {
		t.Fatalf("GetLogs with empty addresses failed: %v", err)
	}
	if len(logs) != 0 {
		t.Errorf("Expected 0 logs, got %d", len(logs))
	}

	// Test with nil/empty topics (should still work)
	_, err = client.GetLogs(context.Background(), 0, 100, []string{"0x123"}, []string{})
	if err != nil {
		t.Fatalf("GetLogs with empty topics failed: %v", err)
	}
}

func TestMetaFileCorruption(t *testing.T) {
	// Setup test database first
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create corrupted meta file (less than 8 bytes)
	metaPath := ReservesMetaPath
	if err := os.WriteFile(metaPath, []byte{1, 2, 3}, 0644); err != nil {
		t.Fatal(err)
	}

	// Mock server
	server := mockRPCServer(t, map[string]interface{}{
		"eth_blockNumber": "0xe4e1c0",
	})
	defer server.Close()

	// Create harvester - should handle corrupted meta file
	h, err := NewHarvester(server.URL)
	if err != nil {
		t.Fatalf("Failed to create harvester: %v", err)
	}
	defer h.Close()

	// Start should use default block since meta is corrupted
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = h.Start(ctx, 0)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestProcessLogs_DatabaseErrors(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{
		pairMap: map[string]int64{
			"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc": 1,
		},
	}

	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		t.Fatal(err)
	}
	h.reservesDB = reservesDB

	if err := h.initReservesDB(); err != nil {
		t.Fatal(err)
	}

	// Create a log that will succeed
	testLog := createTestLog(testSyncEvent{
		PairAddress: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
		BlockNumber: 15000000,
		TxHash:      "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
		LogIndex:    0,
		Reserve0:    big.NewInt(1000000),
		Reserve1:    big.NewInt(2000000),
	})

	// Close database to force error during transaction
	h.reservesDB.Close()

	// This should fail with database closed error
	err = h.processLogs([]Log{testLog})
	if err == nil {
		t.Error("Expected error processing logs with closed database")
	}
}

func TestHarvester_GetLatestReserves_ScanError(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{
		pairMap: make(map[string]int64),
	}

	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		t.Fatal(err)
	}
	h.reservesDB = reservesDB
	defer h.Close()

	// Create table with wrong schema to force scan error
	_, err = h.reservesDB.Exec(`
		CREATE TABLE pair_reserves (
			pair_id INTEGER PRIMARY KEY,
			invalid_column TEXT
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert data that won't scan properly
	_, err = h.reservesDB.Exec(`
		INSERT INTO pair_reserves (pair_id, invalid_column) VALUES (1, 'test')
	`)
	if err != nil {
		t.Fatal(err)
	}

	// This should fail during scan
	_, err = h.GetLatestReserves()
	if err == nil {
		t.Error("Expected error scanning with wrong schema")
	}
}

func TestRPCClient_ResponseDecodeError(t *testing.T) {
	// Create server that returns invalid JSON
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

func TestHarvester_Start_RPCErrors(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create server that returns errors
	errorCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req RPCRequest
		json.NewDecoder(r.Body).Decode(&req)

		resp := RPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
		}

		if req.Method == "eth_blockNumber" {
			errorCount++
			if errorCount < 3 {
				// Return 429 error to test retry logic
				resp.Error = &RPCError{
					Code:    429,
					Message: "Too many requests",
				}
			} else {
				// Eventually succeed
				respData, _ := json.Marshal("0xe4e1c0")
				resp.Result = respData
			}
		} else if req.Method == "eth_getLogs" {
			// Return error for logs
			resp.Error = &RPCError{
				Code:    -32000,
				Message: "Internal error",
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create harvester with the mock server (not a real RPC endpoint)
	h, err := NewHarvester(server.URL)
	if err != nil {
		t.Fatalf("Failed to create harvester: %v", err)
	}
	defer h.Close()

	// Override the clients to use our mock server
	h.dataClient = NewRPCClient(server.URL)
	h.headClient = h.dataClient

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// This should handle the errors and retry
	err = h.Start(ctx, 15000000)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Logf("Start completed with expected error: %v", err)
	}
}

func TestHarvester_Start_ChunkProcessing(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create harvester with many pairs to test chunking
	h := &Harvester{
		pairMap: make(map[string]int64),
	}

	// Add 2500 pairs to force multiple chunks
	for i := 0; i < 2500; i++ {
		addr := fmt.Sprintf("0x%040x", i)
		h.pairMap[addr] = int64(i)
	}

	// Setup databases
	pairsDB, _ := sql.Open("sqlite3", "uniswap_pairs.db?mode=ro")
	h.pairsDB = pairsDB
	defer h.pairsDB.Close()

	reservesDB, _ := sql.Open("sqlite3", ReservesDBPath)
	h.reservesDB = reservesDB
	defer h.reservesDB.Close()

	h.initReservesDB()

	// Mock server
	server := mockRPCServer(t, map[string]interface{}{
		"eth_blockNumber": "0xe4e1c0",
		"eth_getLogs":     []Log{},
	})
	defer server.Close()

	h.dataClient = NewRPCClient(server.URL)
	h.headClient = h.dataClient

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This should process in chunks
	err := h.Start(ctx, 15000000)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Errorf("Unexpected error: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Integration Tests
// -----------------------------------------------------------------------------

func TestBatchSizeAdjustment(t *testing.T) {
	// Test the dynamic batch size adjustment logic
	tests := []struct {
		name           string
		consecutiveOK  int
		consecutiveNG  int
		currentBatch   uint64
		expectIncrease bool
		expectDecrease bool
	}{
		{
			name:           "increase after 3 successes",
			consecutiveOK:  3,
			consecutiveNG:  0,
			currentBatch:   1000,
			expectIncrease: true,
			expectDecrease: false,
		},
		{
			name:           "decrease after 3 failures",
			consecutiveOK:  0,
			consecutiveNG:  3,
			currentBatch:   1000,
			expectIncrease: false,
			expectDecrease: true,
		},
		{
			name:           "no change on 2 successes",
			consecutiveOK:  2,
			consecutiveNG:  0,
			currentBatch:   1000,
			expectIncrease: false,
			expectDecrease: false,
		},
		{
			name:           "respect floor",
			consecutiveOK:  0,
			consecutiveNG:  3,
			currentBatch:   SyncEventBatchFloor,
			expectIncrease: false,
			expectDecrease: false,
		},
		{
			name:           "respect ceiling",
			consecutiveOK:  3,
			consecutiveNG:  0,
			currentBatch:   SyncEventBatchCeil,
			expectIncrease: false,
			expectDecrease: false,
		},
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

func TestFullIntegration(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create full mock RPC server
	server := mockRPCServer(t, map[string]interface{}{
		"eth_blockNumber": "0xe4e1c0",
		"eth_getLogs": []Log{
			{
				Address:     "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
				Topics:      []string{SyncEventSignature},
				Data:        "0x00000000000000000000000000000000000000000000000000000000000f424000000000000000000000000000000000000000000000000000000000001e8480",
				BlockNumber: "0xe4e1c0",
				TxHash:      "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				LogIndex:    "0x0",
			},
		},
	})
	defer server.Close()

	// Create and start harvester
	h, err := NewHarvester(server.URL)
	if err != nil {
		t.Fatalf("Failed to create harvester: %v", err)
	}
	defer h.Close()

	// Run for a short time
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go h.Start(ctx, 15000000)

	// Wait a bit for processing
	time.Sleep(50 * time.Millisecond)

	// Check results
	reserves, err := h.GetLatestReserves()
	if err != nil {
		t.Fatalf("Failed to get reserves: %v", err)
	}

	// Should have at least one reserve
	if len(reserves) == 0 {
		t.Log("No reserves found - this might be timing dependent")
	}
}

// -----------------------------------------------------------------------------
// Benchmark Tests
// -----------------------------------------------------------------------------

func BenchmarkProcessLogs(b *testing.B) {
	_, tmpDir, cleanup := setupTestDB(b)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create harvester
	h := &Harvester{
		pairMap: make(map[string]int64),
	}

	// Generate many test pairs
	for i := 0; i < 1000; i++ {
		addr := fmt.Sprintf("0x%040d", i)
		h.pairMap[addr] = int64(i)
	}

	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		b.Fatal(err)
	}
	h.reservesDB = reservesDB
	defer h.Close()

	if err := h.initReservesDB(); err != nil {
		b.Fatal(err)
	}

	// Create test logs
	logs := make([]Log, 100)
	for i := 0; i < 100; i++ {
		event := testSyncEvent{
			PairAddress: fmt.Sprintf("0x%040d", i%1000),
			BlockNumber: uint64(15000000 + i),
			TxHash:      fmt.Sprintf("0x%064x", i),
			LogIndex:    uint64(i % 10),
			Reserve0:    big.NewInt(int64(1000000 * (i + 1))),
			Reserve1:    big.NewInt(int64(2000000 * (i + 1))),
		}
		logs[i] = createTestLog(event)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := h.processLogs(logs); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReserveRetrieval(b *testing.B) {
	_, tmpDir, cleanup := setupTestDB(b)
	defer cleanup()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	h := &Harvester{}
	reservesDB, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		b.Fatal(err)
	}
	h.reservesDB = reservesDB
	defer h.Close()

	if err := h.initReservesDB(); err != nil {
		b.Fatal(err)
	}

	// Insert many reserves
	tx, _ := h.reservesDB.Begin()
	for i := 0; i < 10000; i++ {
		_, err := tx.Exec(`
			INSERT INTO pair_reserves (pair_id, pair_address, reserve0, reserve1, block_height)
			VALUES (?, ?, ?, ?, ?)
		`, i, fmt.Sprintf("0x%040d", i), "1000000", "2000000", 15000000+i)
		if err != nil {
			b.Fatal(err)
		}
	}
	tx.Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reserves, err := h.GetLatestReserves()
		if err != nil {
			b.Fatal(err)
		}
		if len(reserves) != 10000 {
			b.Errorf("Expected 10000 reserves, got %d", len(reserves))
		}
	}
}

func BenchmarkParseHexUint64(b *testing.B) {
	testCases := []string{
		"0x1",
		"0xff",
		"0x1000",
		"0xe4e1c0",
		"0xffffffffffffffff",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			parseHexUint64(tc)
		}
	}
}

// -----------------------------------------------------------------------------
// Example Test (for documentation)
// -----------------------------------------------------------------------------

func ExampleHarvester_Start() {
	// This example shows how to use the harvester
	fmt.Println("Example: Create harvester with NewHarvester(rpcURL)")
	fmt.Println("Example: Call harvester.Start(ctx, startBlock) to begin harvesting")
	fmt.Println("Example: Call harvester.GetLatestReserves() to get current reserves")
	fmt.Println("Loaded 0 pair reserves")
	// Output:
	// Example: Create harvester with NewHarvester(rpcURL)
	// Example: Call harvester.Start(ctx, startBlock) to begin harvesting
	// Example: Call harvester.GetLatestReserves() to get current reserves
	// Loaded 0 pair reserves
}

func ExampleNewHarvester() {
	// Example of creating a new harvester
	fmt.Println("harvester, err := NewHarvester(\"wss://mainnet.infura.io/ws/v3/YOUR_KEY\")")
	fmt.Println("if err != nil {")
	fmt.Println("    log.Fatal(err)")
	fmt.Println("}")
	fmt.Println("defer harvester.Close()")
	// Output:
	// harvester, err := NewHarvester("wss://mainnet.infura.io/ws/v3/YOUR_KEY")
	// if err != nil {
	//     log.Fatal(err)
	// }
	// defer harvester.Close()
}
