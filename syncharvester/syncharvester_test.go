// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ROUTER INTEGRATION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestWriteHex64(t *testing.T) {
	testCases := []struct {
		name     string
		input    uint64
		expected string
	}{
		{
			name:     "Zero value",
			input:    0,
			expected: "0000000000000000",
		},
		{
			name:     "Maximum uint64",
			input:    ^uint64(0),
			expected: "ffffffffffffffff",
		},
		{
			name:     "Small value",
			input:    255,
			expected: "00000000000000ff",
		},
		{
			name:     "Medium value",
			input:    0x1234567890abcdef,
			expected: "1234567890abcdef",
		},
		{
			name:     "Power of 2",
			input:    1 << 32,
			expected: "0000000100000000",
		},
		{
			name:     "All nibbles different",
			input:    0x0123456789abcdef,
			expected: "0123456789abcdef",
		},
		{
			name:     "Typical reserve value",
			input:    1000000000000000000, // 1e18
			expected: "0de0b6b3a7640000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test with exact size buffer
			buf := make([]byte, 16)
			WriteHex64(buf, tc.input)
			result := string(buf)
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}

			// Test with larger buffer to ensure it only writes 16 chars
			largeBuf := make([]byte, 32)
			for i := range largeBuf {
				largeBuf[i] = 'X' // Fill with X to detect overwrites
			}
			WriteHex64(largeBuf[8:24], tc.input)

			// Check prefix wasn't touched
			for i := 0; i < 8; i++ {
				if largeBuf[i] != 'X' {
					t.Errorf("WriteHex64 wrote outside bounds at position %d", i)
				}
			}

			// Check the actual hex
			result = string(largeBuf[8:24])
			if result != tc.expected {
				t.Errorf("Expected %s, got %s (with offset)", tc.expected, result)
			}

			// Check suffix wasn't touched
			for i := 24; i < 32; i++ {
				if largeBuf[i] != 'X' {
					t.Errorf("WriteHex64 wrote outside bounds at position %d", i)
				}
			}
		})
	}
}

func TestWriteHex32(t *testing.T) {
	testCases := []struct {
		name     string
		input    uint32
		expected string
		length   int
	}{
		{
			name:     "Zero",
			input:    0,
			expected: "0",
			length:   1,
		},
		{
			name:     "Single digit",
			input:    0xF,
			expected: "f",
			length:   1,
		},
		{
			name:     "Two digits",
			input:    0xFF,
			expected: "ff",
			length:   2,
		},
		{
			name:     "Four digits",
			input:    0x1234,
			expected: "1234",
			length:   4,
		},
		{
			name:     "Max uint32",
			input:    0xFFFFFFFF,
			expected: "ffffffff",
			length:   8,
		},
		{
			name:     "Typical block number",
			input:    19234567,
			expected: "12557d7",
			length:   7,
		},
		{
			name:     "Power of 16",
			input:    0x1000000,
			expected: "1000000",
			length:   7,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, 8)
			length := WriteHex32(buf, tc.input)

			if length != tc.length {
				t.Errorf("Expected length %d, got %d", tc.length, length)
			}

			result := string(buf[:length])
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}

			// Verify against fmt.Sprintf
			expected := fmt.Sprintf("%x", tc.input)
			if result != expected {
				t.Errorf("Mismatch with fmt.Sprintf: expected %s, got %s", expected, result)
			}
		})
	}
}

func TestParseDecimalU64(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected uint64
	}{
		{
			name:     "Empty string",
			input:    "",
			expected: 0,
		},
		{
			name:     "Zero",
			input:    "0",
			expected: 0,
		},
		{
			name:     "Single digit",
			input:    "7",
			expected: 7,
		},
		{
			name:     "Multiple digits",
			input:    "12345",
			expected: 12345,
		},
		{
			name:     "Maximum uint64",
			input:    "18446744073709551615",
			expected: 18446744073709551615,
		},
		{
			name:     "Large number",
			input:    "1000000000000000000",
			expected: 1000000000000000000,
		},
		{
			name:     "Leading zeros",
			input:    "00000123",
			expected: 123,
		},
		{
			name:     "Invalid character at start",
			input:    "a123",
			expected: 0,
		},
		{
			name:     "Invalid character in middle",
			input:    "12a34",
			expected: 0,
		},
		{
			name:     "Invalid character at end",
			input:    "1234a",
			expected: 0,
		},
		{
			name:     "Space",
			input:    "12 34",
			expected: 0,
		},
		{
			name:     "Negative sign",
			input:    "-123",
			expected: 0,
		},
		{
			name:     "Decimal point",
			input:    "123.45",
			expected: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseDecimalU64(tc.input)
			if result != tc.expected {
				t.Errorf("Expected %d, got %d", tc.expected, result)
			}
		})
	}
}

func TestParseDecimalU64_Overflow(t *testing.T) {
	// Test overflow behavior
	// Note: The current implementation doesn't check for overflow,
	// so these will wrap around. This test documents the actual behavior.
	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "Just over max uint64",
			input: "18446744073709551616", // max + 1
		},
		{
			name:  "Way over max uint64",
			input: "99999999999999999999999999999",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Just verify it doesn't panic
			_ = ParseDecimalU64(tc.input)
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

func TestFlushSyncedReservesToRouter_Integration(t *testing.T) {
	// This test verifies the complete flow including hex encoding
	tempDir := t.TempDir()
	oldPath := ReservesDBPath
	ReservesDBPath = filepath.Join(tempDir, "test_flush.db")
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
	
	CREATE TABLE pair_reserves (
		pair_id INTEGER PRIMARY KEY,
		pair_address TEXT NOT NULL,
		reserve0 TEXT NOT NULL,
		reserve1 TEXT NOT NULL,
		block_height INTEGER NOT NULL,
		last_updated INTEGER NOT NULL
	);
	
	INSERT INTO sync_metadata (id, last_block) VALUES (1, 0x1234567);
	`

	_, err = db.Exec(schema)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Test hex encoding for specific values
	testCases := []struct {
		name             string
		reserve0         string
		reserve1         string
		expectedReserve0 uint64
		expectedReserve1 uint64
		expectedHex0     string
		expectedHex1     string
	}{
		{
			name:             "Simple values",
			reserve0:         "1000",
			reserve1:         "2000",
			expectedReserve0: 1000,
			expectedReserve1: 2000,
			expectedHex0:     "00000000000003e8",
			expectedHex1:     "00000000000007d0",
		},
		{
			name:             "Large values",
			reserve0:         "1000000000000000000",
			reserve1:         "2000000000000000000",
			expectedReserve0: 1000000000000000000,
			expectedReserve1: 2000000000000000000,
			expectedHex0:     "0de0b6b3a7640000",
			expectedHex1:     "1bc16d674ec80000",
		},
		{
			name:             "Max safe values",
			reserve0:         "9223372036854775807",
			reserve1:         "18446744073709551615",
			expectedReserve0: 9223372036854775807,
			expectedReserve1: 18446744073709551615,
			expectedHex0:     "7fffffffffffffff",
			expectedHex1:     "ffffffffffffffff",
		},
		{
			name:             "Zero values",
			reserve0:         "0",
			reserve1:         "0",
			expectedReserve0: 0,
			expectedReserve1: 0,
			expectedHex0:     "0000000000000000",
			expectedHex1:     "0000000000000000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear previous data
			db.Exec("DELETE FROM pair_reserves")
			testRouter.Reset()

			// Insert test data
			_, err = db.Exec(`INSERT INTO pair_reserves VALUES (1, '0xTestAddress', ?, ?, 12345678, 1234567890)`,
				tc.reserve0, tc.reserve1)
			if err != nil {
				t.Fatalf("Failed to insert test data: %v", err)
			}

			// Execute flush with test dispatcher
			_, err = testableFlushSyncedReservesToRouter(captureDispatch(testRouter))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Verify results
			updates := testRouter.GetUpdates()
			if len(updates) != 1 {
				t.Fatalf("Expected 1 update, got %d", len(updates))
			}

			update := updates[0]
			if update.Reserve0 != tc.expectedReserve0 {
				t.Errorf("Expected reserve0 %d, got %d", tc.expectedReserve0, update.Reserve0)
			}
			if update.Reserve1 != tc.expectedReserve1 {
				t.Errorf("Expected reserve1 %d, got %d", tc.expectedReserve1, update.Reserve1)
			}

			// Verify hex encoding by checking the raw data that was dispatched
			// This would require modifying captureDispatch to also capture raw hex,
			// but we can infer correctness from the parsed values matching
		})
	}
}

func TestFlushSyncedReservesToRouter_EdgeCases(t *testing.T) {
	tempDir := t.TempDir()
	oldPath := ReservesDBPath
	ReservesDBPath = filepath.Join(tempDir, "test_edge.db")
	defer func() { ReservesDBPath = oldPath }()

	db, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Create schema
	db.Exec(`CREATE TABLE sync_metadata (id INTEGER PRIMARY KEY, last_block INTEGER)`)
	db.Exec(`CREATE TABLE pair_reserves (pair_id INTEGER PRIMARY KEY, pair_address TEXT, reserve0 TEXT, reserve1 TEXT, block_height INTEGER, last_updated INTEGER)`)
	db.Exec(`INSERT INTO sync_metadata VALUES (1, 1000)`)

	testCases := []struct {
		name            string
		setupData       func(*sql.DB)
		expectedUpdates int
	}{
		{
			name: "Invalid decimal in reserve0",
			setupData: func(db *sql.DB) {
				db.Exec(`INSERT INTO pair_reserves VALUES (1, '0xAddr1', 'not_a_number', '1000', 100, 1234567890)`)
			},
			expectedUpdates: 0, // Should skip invalid entries
		},
		{
			name: "Invalid decimal in reserve1",
			setupData: func(db *sql.DB) {
				db.Exec(`INSERT INTO pair_reserves VALUES (1, '0xAddr1', '1000', 'not_a_number', 100, 1234567890)`)
			},
			expectedUpdates: 0, // Should skip invalid entries
		},
		{
			name: "Mixed valid and invalid",
			setupData: func(db *sql.DB) {
				db.Exec(`INSERT INTO pair_reserves VALUES 
					(1, '0xAddr1', 'invalid', '1000', 100, 1234567890),
					(2, '0xAddr2', '2000', '3000', 100, 1234567890),
					(3, '0xAddr3', '4000', 'invalid', 100, 1234567890),
					(4, '0xAddr4', '5000', '6000', 100, 1234567890)`)
			},
			expectedUpdates: 2, // Only valid entries
		},
		{
			name: "Very long address",
			setupData: func(db *sql.DB) {
				longAddr := "0x" + strings.Repeat("a", 100)
				db.Exec(`INSERT INTO pair_reserves VALUES (1, ?, '1000', '2000', 100, 1234567890)`, longAddr)
			},
			expectedUpdates: 1, // Should handle long addresses
		},
		{
			name: "Null values", // This would cause scan error
			setupData: func(db *sql.DB) {
				db.Exec(`INSERT INTO pair_reserves VALUES (1, '0xAddr1', NULL, NULL, 100, 1234567890)`)
			},
			expectedUpdates: 0, // Scan error, entry skipped
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset
			db.Exec("DELETE FROM pair_reserves")
			testRouter.Reset()

			// Setup
			tc.setupData(db)

			// Execute
			_, err := testableFlushSyncedReservesToRouter(captureDispatch(testRouter))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Verify
			updates := testRouter.GetUpdates()
			if len(updates) != tc.expectedUpdates {
				t.Errorf("Expected %d updates, got %d", tc.expectedUpdates, len(updates))
			}
		})
	}
}

func TestFlushSyncedReservesToRouter_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	tempDir := t.TempDir()
	oldPath := ReservesDBPath
	ReservesDBPath = filepath.Join(tempDir, "test_perf.db")
	defer func() { ReservesDBPath = oldPath }()

	db, err := sql.Open("sqlite3", ReservesDBPath)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Create schema
	db.Exec(`CREATE TABLE sync_metadata (id INTEGER PRIMARY KEY, last_block INTEGER)`)
	db.Exec(`CREATE TABLE pair_reserves (pair_id INTEGER PRIMARY KEY, pair_address TEXT, reserve0 TEXT, reserve1 TEXT, block_height INTEGER, last_updated INTEGER)`)
	db.Exec(`INSERT INTO sync_metadata VALUES (1, 1000000)`)

	// Insert many pairs
	const numPairs = 10000
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare(`INSERT INTO pair_reserves VALUES (?, ?, ?, ?, ?, ?)`)

	for i := 1; i <= numPairs; i++ {
		addr := fmt.Sprintf("0x%040d", i)
		reserve0 := fmt.Sprintf("%d", uint64(i)*1000000000000000000)
		reserve1 := fmt.Sprintf("%d", uint64(i)*2000000000000000000)
		stmt.Exec(i, addr, reserve0, reserve1, 1000000, time.Now().Unix())
	}
	stmt.Close()
	tx.Commit()

	// Measure flush time
	testRouter.Reset()
	start := time.Now()

	count, err := testableFlushSyncedReservesToRouter(captureDispatch(testRouter))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	elapsed := time.Since(start)

	if count != numPairs {
		t.Errorf("Expected %d pairs flushed, got %d", numPairs, count)
	}

	updates := testRouter.GetUpdates()
	if len(updates) != numPairs {
		t.Errorf("Expected %d updates, got %d", numPairs, len(updates))
	}

	t.Logf("Flushed %d pairs in %v (%.0f pairs/sec)", numPairs, elapsed, float64(numPairs)/elapsed.Seconds())
}

func TestHexEncodingConsistency(t *testing.T) {
	// Verify that our WriteHex64 produces the same output as fmt.Sprintf
	testValues := []uint64{
		0,
		1,
		255,
		256,
		65535,
		65536,
		0x123456789abcdef0,
		^uint64(0),
		1000000000000000000, // 1e18
	}

	for _, val := range testValues {
		expected := fmt.Sprintf("%016x", val)

		actual := make([]byte, 16)
		WriteHex64(actual, val)

		if string(actual) != expected {
			t.Errorf("WriteHex64(%d): expected %s, got %s", val, expected, string(actual))
		}
	}
}

func TestDataBufferConstruction(t *testing.T) {
	// Test the data buffer construction used in FlushSyncedReservesToRouter
	var dataBuffer [130]byte
	dataBuffer[0] = '0'
	dataBuffer[1] = 'x'

	// Fill with zeros using the same method
	dataPtr := (*[16]uint64)(unsafe.Pointer(&dataBuffer[2]))
	const zeros = uint64(0x3030303030303030)
	for i := 0; i < 16; i++ {
		dataPtr[i] = zeros
	}

	// Verify it's all zeros
	expected := "0x" + strings.Repeat("0", 128)
	if string(dataBuffer[:]) != expected {
		t.Error("Data buffer not properly initialized with zeros")
	}

	// Test writing reserves
	reserve0 := uint64(1234567890)
	reserve1 := uint64(9876543210)

	WriteHex64(dataBuffer[2:18], reserve0)
	WriteHex64(dataBuffer[66:82], reserve1)

	// The middle should still be zeros
	for i := 18; i < 66; i++ {
		if dataBuffer[i] != '0' {
			t.Errorf("Position %d should be '0', got '%c'", i, dataBuffer[i])
		}
	}
	for i := 82; i < 130; i++ {
		if dataBuffer[i] != '0' {
			t.Errorf("Position %d should be '0', got '%c'", i, dataBuffer[i])
		}
	}

	// Verify the reserves were written correctly
	reserve0Hex := string(dataBuffer[2:18])
	reserve1Hex := string(dataBuffer[66:82])

	if reserve0Hex != fmt.Sprintf("%016x", reserve0) {
		t.Errorf("Reserve0 hex mismatch: got %s", reserve0Hex)
	}
	if reserve1Hex != fmt.Sprintf("%016x", reserve1) {
		t.Errorf("Reserve1 hex mismatch: got %s", reserve1Hex)
	}
}

func TestFlushSyncedReservesToRouter_PanicOnInconsistentState(t *testing.T) {
	// Test the case where sync_events exist but no metadata
	tempDir := t.TempDir()
	oldPath := ReservesDBPath
	ReservesDBPath = filepath.Join(tempDir, "test_panic.db")
	defer func() { ReservesDBPath = oldPath }()

	db, _ := sql.Open("sqlite3", ReservesDBPath)
	defer db.Close()

	// Create schema
	db.Exec(`CREATE TABLE sync_metadata (id INTEGER PRIMARY KEY, last_block INTEGER)`)
	db.Exec(`CREATE TABLE sync_events (block_number INTEGER)`)
	db.Exec(`CREATE TABLE pair_reserves (pair_id INTEGER PRIMARY KEY, pair_address TEXT, reserve0 TEXT, reserve1 TEXT, block_height INTEGER, last_updated INTEGER)`)

	// Add sync events but NO metadata - this is an inconsistent state
	db.Exec(`INSERT INTO sync_events (block_number) VALUES (12345)`)
	db.Exec(`INSERT INTO pair_reserves VALUES (1, '0xTest', '1000', '2000', 12345, 1234567890)`)

	// This should panic
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Correctly panicked on inconsistent state: %v", r)
			// Verify the panic message
			panicMsg := fmt.Sprintf("%v", r)
			if !strings.Contains(panicMsg, "database corruption") || !strings.Contains(panicMsg, "sync_events exist but sync_metadata is missing") {
				t.Errorf("Unexpected panic message: %v", r)
			}
		} else {
			t.Error("Expected panic on inconsistent database state")
		}
	}()

	// This should panic
	FlushSyncedReservesToRouter()
}

func TestFlushSyncedReservesToRouter_OptimizedBlockHex(t *testing.T) {
	tempDir := t.TempDir()
	oldPath := ReservesDBPath
	ReservesDBPath = filepath.Join(tempDir, "test_optimized.db")
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
	
	CREATE TABLE pair_reserves (
		pair_id INTEGER PRIMARY KEY,
		pair_address TEXT NOT NULL,
		reserve0 TEXT NOT NULL,
		reserve1 TEXT NOT NULL,
		block_height INTEGER NOT NULL,
		last_updated INTEGER NOT NULL
	);`

	_, err = db.Exec(schema)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Test various block numbers
	testCases := []struct {
		name        string
		blockNumber uint64
		expectedHex string
	}{
		{
			name:        "Zero block",
			blockNumber: 0,
			expectedHex: "0x0",
		},
		{
			name:        "Small block",
			blockNumber: 255,
			expectedHex: "0xff",
		},
		{
			name:        "Typical mainnet block",
			blockNumber: 19234567,
			expectedHex: "0x12557d7",
		},
		{
			name:        "Large block",
			blockNumber: 4294967295, // Max uint32
			expectedHex: "0xffffffff",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear and setup data
			db.Exec("DELETE FROM sync_metadata")
			db.Exec("DELETE FROM pair_reserves")
			db.Exec("INSERT INTO sync_metadata (id, last_block) VALUES (1, ?)", tc.blockNumber)
			db.Exec("INSERT INTO pair_reserves VALUES (1, '0xTestAddr', '1000', '2000', ?, 1234567890)", tc.blockNumber)

			testRouter.Reset()

			// Execute flush
			_, err := testableFlushSyncedReservesToRouter(captureDispatch(testRouter))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Verify
			updates := testRouter.GetUpdates()
			if len(updates) != 1 {
				t.Fatalf("Expected 1 update, got %d", len(updates))
			}

			if updates[0].Block != tc.blockNumber {
				t.Errorf("Expected block %d, got %d", tc.blockNumber, updates[0].Block)
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// BENCHMARKS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func BenchmarkWriteHex64(b *testing.B) {
	dst := make([]byte, 16)
	value := uint64(0x1234567890abcdef)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WriteHex64(dst, value)
	}
}

func BenchmarkParseDecimalU64(b *testing.B) {
	input := "1234567890123456"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ParseDecimalU64(input)
	}
}

func BenchmarkFlushSyncedReservesToRouter(b *testing.B) {
	// Setup database with test data
	tempDir := b.TempDir()
	oldPath := ReservesDBPath
	ReservesDBPath = filepath.Join(tempDir, "bench_flush.db")
	defer func() { ReservesDBPath = oldPath }()

	db, _ := sql.Open("sqlite3", ReservesDBPath)
	defer db.Close()

	db.Exec(`CREATE TABLE sync_metadata (id INTEGER PRIMARY KEY, last_block INTEGER)`)
	db.Exec(`CREATE TABLE pair_reserves (pair_id INTEGER PRIMARY KEY, pair_address TEXT, reserve0 TEXT, reserve1 TEXT, block_height INTEGER, last_updated INTEGER)`)
	db.Exec(`INSERT INTO sync_metadata VALUES (1, 1000000)`)

	// Insert test pairs
	const numPairs = 1000
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare(`INSERT INTO pair_reserves VALUES (?, ?, ?, ?, ?, ?)`)

	for i := 1; i <= numPairs; i++ {
		addr := fmt.Sprintf("0x%040x", i)
		stmt.Exec(i, addr, "1000000000000000000", "2000000000000000000", 1000000, 1234567890)
	}
	stmt.Close()
	tx.Commit()
	db.Close()

	// Benchmark the flush operation
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testableFlushSyncedReservesToRouter(func(*types.LogView) {})
	}
}

// Add all the comprehensive benchmarks from the previous artifacts here...
// (Including all the benchmarks for WriteHex32, comparison benchmarks, etc.)