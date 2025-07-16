// sync_harvester_test.go
package syncharvest

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
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
	LogIndex    uint
	Reserve0    *big.Int
	Reserve1    *big.Int
}

// setupTestDB creates a test database with sample pairs
func setupTestDB(t *testing.T) (*sql.DB, string, func()) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "sync_test_*")
	if err != nil {
		t.Fatal(err)
	}

	// Create test pairs database
	pairsPath := filepath.Join(tmpDir, "uniswap_pairs.db")
	pairsDB, err := sql.Open("sqlite3", pairsPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatal(err)
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
		t.Fatal(err)
	}

	// Insert test data
	_, err = pairsDB.Exec(`INSERT INTO exchanges (id, name, chain_id) VALUES (1, 'uniswap_v2', 1)`)
	if err != nil {
		t.Fatal(err)
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
			t.Fatal(err)
		}
	}

	// Insert test pairs
	testPairs := []testPair{
		{1, "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", "1", "3"}, // USDC/WETH
		{2, "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11", "1", "2"}, // DAI/WETH
		{3, "0xae461ca67b15dc8dc81ce7615e0320da1a9ab8d5", "1", "2"}, // DAI/USDC
	}

	for _, pair := range testPairs {
		_, err = pairsDB.Exec(`
			INSERT INTO pools (id, exchange_id, token0_id, token1_id, pool_address, fee_bps) 
			VALUES (?, 1, ?, ?, ?, 30)
		`, pair.ID, pair.Token0, pair.Token1, pair.Address)
		if err != nil {
			t.Fatal(err)
		}
	}

	cleanup := func() {
		pairsDB.Close()
		os.RemoveAll(tmpDir)
	}

	return pairsDB, tmpDir, cleanup
}

// createTestLog creates a mock Ethereum log for testing
func createTestLog(event testSyncEvent) types.Log {
	// Create sync event data (reserve0 + reserve1)
	data := make([]byte, 64)
	r0Bytes := event.Reserve0.Bytes()
	r1Bytes := event.Reserve1.Bytes()

	// Right-pad to 32 bytes each
	copy(data[32-len(r0Bytes):32], r0Bytes)
	copy(data[64-len(r1Bytes):64], r1Bytes)

	return types.Log{
		Address:     common.HexToAddress(event.PairAddress),
		Topics:      []common.Hash{common.HexToHash(SyncEventSignature)},
		Data:        data,
		BlockNumber: event.BlockNumber,
		TxHash:      common.HexToHash(event.TxHash),
		Index:       event.LogIndex,
	}
}

// -----------------------------------------------------------------------------
// Unit Tests
// -----------------------------------------------------------------------------

func TestNewHarvester(t *testing.T) {
	_, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	// Change to test directory
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create harvester (without real RPC)
	h := &Harvester{
		pairMap: make(map[string]int64),
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

	// Initialize
	if err := h.initReservesDB(); err != nil {
		t.Fatalf("initReservesDB failed: %v", err)
	}

	if err := h.loadPairMappings(); err != nil {
		t.Fatalf("loadPairMappings failed: %v", err)
	}

	// Verify mappings loaded
	if len(h.pairMap) != 3 {
		t.Errorf("Expected 3 pairs, got %d", len(h.pairMap))
	}

	// Check specific mapping
	if id, ok := h.pairMap["0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"]; !ok || id != 1 {
		t.Errorf("Pair mapping incorrect: got %d, want 1", id)
	}

	h.Close()
}

func TestProcessLogs(t *testing.T) {
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

	// Create test logs
	testEvents := []testSyncEvent{
		{
			PairAddress: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			BlockNumber: 15000000,
			TxHash:      "0x1234567890abcdef",
			LogIndex:    0,
			Reserve0:    big.NewInt(1000000),
			Reserve1:    big.NewInt(2000000),
		},
		{
			PairAddress: "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",
			BlockNumber: 15000001,
			TxHash:      "0xabcdef1234567890",
			LogIndex:    1,
			Reserve0:    big.NewInt(3000000),
			Reserve1:    big.NewInt(4000000),
		},
	}

	logs := []types.Log{}
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

	// Verify reserves were updated
	var reserve0, reserve1 string
	var blockHeight int64
	err = h.reservesDB.QueryRow(`
		SELECT reserve0, reserve1, block_height 
		FROM pair_reserves 
		WHERE pair_id = 1
	`).Scan(&reserve0, &reserve1, &blockHeight)
	if err != nil {
		t.Fatal(err)
	}

	if reserve0 != "1000000" || reserve1 != "2000000" || blockHeight != 15000000 {
		t.Errorf("Reserves mismatch: got r0=%s r1=%s block=%d", reserve0, reserve1, blockHeight)
	}
}

func TestMetaFile(t *testing.T) {
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

	// Read it back
	metaF.Seek(0, 0)
	var readBlock uint64
	if err := binary.Read(metaF, binary.BigEndian, &readBlock); err != nil {
		t.Fatalf("read meta failed: %v", err)
	}

	if readBlock != testBlock {
		t.Errorf("Meta block mismatch: got %d, want %d", readBlock, testBlock)
	}
}

func TestGetLatestReserves(t *testing.T) {
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
	reserves, err := h.GetLatestReserves()
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
	} else if r.Reserve0 != "1000000" || r.Reserve1 != "2000000" {
		t.Errorf("Reserve mismatch: got r0=%s r1=%s", r.Reserve0, r.Reserve1)
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
	logs := make([]types.Log, 100)
	for i := 0; i < 100; i++ {
		event := testSyncEvent{
			PairAddress: fmt.Sprintf("0x%040d", i%1000),
			BlockNumber: uint64(15000000 + i),
			TxHash:      fmt.Sprintf("0x%064x", i),
			LogIndex:    uint(i % 10),
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

// -----------------------------------------------------------------------------
// Example Test (for documentation)
// -----------------------------------------------------------------------------

func ExampleHarvester_Start() {
	// This example shows how to use the harvester

	// Create harvester
	harvester, err := NewHarvester("wss://mainnet.infura.io/ws/v3/YOUR_KEY")
	if err != nil {
		panic(err)
	}
	defer harvester.Close()

	// Start harvesting from a specific block
	ctx := context.Background()
	startBlock := uint64(15000000)

	if err := harvester.Start(ctx, startBlock); err != nil {
		panic(err)
	}

	// Get latest reserves for bootstrapping
	reserves, err := harvester.GetLatestReserves()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Loaded %d pair reserves\n", len(reserves))
}
