package parser

import (
	"crypto/rand"
	"fmt"
	"main/types"
	"strings"
	"testing"
)

// ============================================================================
// TEST UTILITIES
// ============================================================================

// createValidSyncEvent creates a properly formatted JSON-RPC subscription message
func createValidSyncEvent() []byte {
	rpcWrapper := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`

	logData := `"address":"0x1234567890123456789012345678901234567890",` +
		`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",` +
		`"blockNumber":"0x123456",` +
		`"blockTimestamp":"1234567890",` +
		`"data":"0x000000000000000000000000000000000000000000000000000000000000001234567890abcdef",` +
		`"logIndex":"0x1",` +
		`"removed":false,` +
		`"topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1","0x000000000000000000000000a0b86a33e6f4e3bbe1b85e8e0e9b8d1234567890"],` +
		`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
		`"transactionIndex":"0x5"}}}`

	return []byte(rpcWrapper + logData)
}

// createCustomEventSafe creates an event by modifying the base valid event
// This ensures we don't break the field parsing by accident
func createCustomEventSafe(modifications map[string]string) []byte {
	base := createValidSyncEvent()
	str := string(base)

	// Apply modifications carefully to preserve JSON structure
	for field, value := range modifications {
		switch field {
		case "address":
			str = strings.Replace(str, `"address":"0x1234567890123456789012345678901234567890"`, `"address":"`+value+`"`, 1)
		case "blockNumber":
			str = strings.Replace(str, `"blockNumber":"0x123456"`, `"blockNumber":"`+value+`"`, 1)
		case "data":
			str = strings.Replace(str, `"data":"0x000000000000000000000000000000000000000000000000000000000000001234567890abcdef"`, `"data":"`+value+`"`, 1)
		case "logIndex":
			str = strings.Replace(str, `"logIndex":"0x1"`, `"logIndex":"`+value+`"`, 1)
		case "topics":
			str = strings.Replace(str, `"topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1","0x000000000000000000000000a0b86a33e6f4e3bbe1b85e8e0e9b8d1234567890"]`, `"topics":`+value, 1)
		case "transactionIndex":
			str = strings.Replace(str, `"transactionIndex":"0x5"`, `"transactionIndex":"`+value+`"`, 1)
		}
	}

	return []byte(str)
}

// createEventWithLength creates an event with specified length for boundary testing
func createEventWithLength(length int) []byte {
	if length < 10 {
		return make([]byte, length)
	}

	base := createValidSyncEvent()
	if len(base) >= length {
		return base[:length]
	}

	// Extend with padding
	padding := make([]byte, length-len(base))
	for i := range padding {
		padding[i] = ' '
	}
	return append(base, padding...)
}

// ============================================================================
// CORE FUNCTIONALITY TESTS
// ============================================================================

func TestHandleFrame_ValidSyncEvent(t *testing.T) {
	event := createValidSyncEvent()

	// Should not panic
	HandleFrame(event)
}

func TestHandleFrame_ShortFrame(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{"Empty frame", 0},
		{"Very short", 10},
		{"Exactly 116 bytes", 116},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := createEventWithLength(tt.length)
			HandleFrame(event)
			// Should not panic and should exit early
		})
	}
}

func TestHandleFrame_MissingFields(t *testing.T) {
	tests := []struct {
		name         string
		modification map[string]string
	}{
		{"Missing address", map[string]string{"address": ""}},
		{"Missing blockNumber", map[string]string{"blockNumber": ""}},
		{"Missing logIndex", map[string]string{"logIndex": ""}},
		{"Missing topics", map[string]string{"topics": "[]"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := createCustomEventSafe(tt.modification)
			HandleFrame(event)
			// Should not panic and should exit early
		})
	}
}

func TestHandleFrame_EmptyDataField(t *testing.T) {
	event := createCustomEventSafe(map[string]string{"data": ""})
	HandleFrame(event)
	// Should not panic and should exit early
}

func TestHandleFrame_NonSyncEvent(t *testing.T) {
	// Create event with different topic signature
	wrongTopic := `["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"]`
	event := createCustomEventSafe(map[string]string{"topics": wrongTopic})

	HandleFrame(event)
	// Should not panic and should exit early
}

func TestHandleFrame_LargeDataField(t *testing.T) {
	// Create oversized data field
	largeData := "0x" + strings.Repeat("a", 1000)
	event := createCustomEventSafe(map[string]string{"data": largeData})

	HandleFrame(event)
	// Should not panic and should exit early
}

func TestHandleFrame_LargeTopicsArray(t *testing.T) {
	// Create oversized topics array
	var topics []string
	for i := 0; i < 20; i++ {
		topics = append(topics, fmt.Sprintf(`"0x%064d"`, i))
	}
	largeTopics := "[" + strings.Join(topics, ",") + "]"
	event := createCustomEventSafe(map[string]string{"topics": largeTopics})

	HandleFrame(event)
	// Should not panic and should exit early
}

// ============================================================================
// FIELD PARSING TESTS
// ============================================================================

func TestHandleFrame_FieldParsing(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value string
	}{
		{"Valid address", "address", "0x1234567890123456789012345678901234567890"},
		{"Valid blockNumber", "blockNumber", "0xabcdef"},
		{"Valid logIndex", "logIndex", "0x123"},
		{"Valid transactionIndex", "transactionIndex", "0x456"},
		{"Empty address", "address", ""},
		{"Short topics", "topics", `["0x123"]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := createCustomEventSafe(map[string]string{tt.field: tt.value})
			HandleFrame(event)
			// Should not panic
		})
	}
}

func TestHandleFrame_TransactionHashSkipping(t *testing.T) {
	// Create event with transaction hash (should be skipped)
	event := createValidSyncEvent()
	HandleFrame(event)
	// Should not panic
}

// ============================================================================
// FINGERPRINT GENERATION TESTS
// ============================================================================

func TestGenerateFingerprint_TopicsPreferred(t *testing.T) {
	tests := []struct {
		name   string
		topics []byte
		data   []byte
		addr   []byte
		wantHi bool
		wantLo bool
	}{
		{
			name:   "128-bit from topics",
			topics: []byte("0123456789abcdef0123456789abcdef"),
			data:   []byte("fallback"),
			addr:   []byte("fallback"),
			wantHi: true,
			wantLo: true,
		},
		{
			name:   "64-bit from topics",
			topics: []byte("0123456789abcdef"),
			data:   []byte("fallback"),
			addr:   []byte("fallback"),
			wantHi: false,
			wantLo: true,
		},
		{
			name:   "64-bit from data",
			topics: []byte("short"),
			data:   []byte("0123456789abcdef"),
			addr:   []byte("fallback"),
			wantHi: false,
			wantLo: true,
		},
		{
			name:   "64-bit from address",
			topics: []byte("short"),
			data:   []byte("short"),
			addr:   []byte("0123456789abcdef"),
			wantHi: false,
			wantLo: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &types.LogView{
				Topics: tt.topics,
				Data:   tt.data,
				Addr:   tt.addr,
			}

			generateFingerprint(v)

			if tt.wantHi && v.TagHi == 0 {
				t.Error("Expected TagHi to be set")
			}
			if tt.wantLo && v.TagLo == 0 {
				t.Error("Expected TagLo to be set")
			}
			// Note: Don't test TagHi clearing since the function doesn't clear it
		})
	}
}

func TestGenerateFingerprint_EdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		topics []byte
		data   []byte
		addr   []byte
	}{
		{"Exactly 8 bytes topics", []byte("12345678"), []byte{}, []byte("fallback")},
		{"Exactly 16 bytes topics", []byte("1234567890123456"), []byte{}, []byte("fallback")},
		{"Exactly 8 bytes data", []byte{}, []byte("12345678"), []byte("fallback")},
		{"Only address available", []byte{}, []byte{}, []byte("12345678")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &types.LogView{
				Topics: tt.topics,
				Data:   tt.data,
				Addr:   tt.addr,
			}

			// Should not panic
			generateFingerprint(v)
		})
	}
}

// ============================================================================
// EMIT LOG TESTS
// ============================================================================

func TestEmitLog(t *testing.T) {
	v := &types.LogView{
		Addr:    []byte("0x1234567890123456789012345678901234567890"),
		BlkNum:  []byte("0x123456"),
		Data:    []byte("0xabcdef"),
		LogIdx:  []byte("0x1"),
		Topics:  []byte(`["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"]`),
		TxIndex: []byte("0x5"),
	}

	// Should not panic - testing output formatting
	emitLog(v)
}

// ============================================================================
// BOUNDARY CONDITIONS AND EDGE CASES
// ============================================================================

func TestHandleFrame_BoundaryConditions(t *testing.T) {
	t.Run("Exactly 117 bytes", func(t *testing.T) {
		event := createEventWithLength(117)
		HandleFrame(event)
		// Should not panic
	})

	t.Run("118 bytes", func(t *testing.T) {
		event := createEventWithLength(118)
		HandleFrame(event)
		// Should not panic
	})

	t.Run("Very large valid event", func(t *testing.T) {
		event := createValidSyncEvent()
		// Extend with valid JSON padding
		padding := strings.Repeat(` `, 10000)
		extended := string(event[:len(event)-2]) + padding + `}}`

		HandleFrame([]byte(extended))
		// Should handle large events gracefully
	})
}

func TestHandleFrame_MalformedJSON(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"Short malformed JSON", `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{"address":"0x1234567890123456789012345678901234567890","topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"],"data":"0x123"`},
		{"Invalid bracket", `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{"address":"0x1234567890123456789012345678901234567890","topics":[malformed`},
		{"Missing quotes", `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{"address:broken`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic - malformed JSON should be handled gracefully
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("HandleFrame panicked on malformed JSON: %v", r)
				}
			}()

			HandleFrame([]byte(tt.json))
		})
	}
}

// ============================================================================
// COMPREHENSIVE COVERAGE TESTS
// ============================================================================

func TestHandleFrame_AllCodePaths(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		// Early exit conditions
		{"Empty", []byte{}},
		{"Short frame", make([]byte, 100)},
		{"Exactly 117 bytes", make([]byte, 117)},

		// Field detection paths - create data that matches 8-byte patterns
		{"Address pattern", createFieldTestData("address\":", 200)},
		{"BlockHash pattern", createFieldTestData("blockHas", 200)},
		{"BlockNumber pattern", createFieldTestData("blockNum", 200)},
		{"BlockTimestamp pattern", createFieldTestData("blockTim", 200)},
		{"Data pattern", createFieldTestData("data\":\"0", 200)},
		{"LogIndex pattern", createFieldTestData("logIndex", 200)},
		{"Removed pattern", createFieldTestData("removed\"", 200)},
		{"Topics pattern", createFieldTestData("topics\":", 200)},
		{"Transaction pattern", createFieldTestData("transact", 200)},

		// Validation failure paths
		{"Missing address", createPartialValidEvent("address", "")},
		{"Missing blockNumber", createPartialValidEvent("blockNumber", "")},
		{"Missing logIndex", createPartialValidEvent("logIndex", "")},
		{"Missing topics", createPartialValidEvent("topics", "[]")},
		{"Empty data", createPartialValidEvent("data", "")},

		// Early exit conditions in parsing
		{"Large data field", createLargeFieldEvent("data", 500)},
		{"Large topics array", createLargeFieldEvent("topics", 300)},
		{"Non-sync event", createNonSyncEvent()},

		// Edge cases in field parsing
		{"Short topics for signature check", createShortTopicsEvent()},
		{"Transaction hash vs index", createTransactionTestEvent()},
		{"End boundary condition", createEndBoundaryEvent()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Test %s panicked: %v", tt.name, r)
				}
			}()

			HandleFrame(tt.data)
		})
	}
}

// Helper functions to create specific test data patterns
func createFieldTestData(pattern string, totalLen int) []byte {
	// Create data that will match specific 8-byte field patterns
	prefix := make([]byte, 117) // JSON-RPC wrapper length
	for i := range prefix {
		prefix[i] = byte('a' + (i % 26))
	}

	// Add the pattern
	data := append(prefix, []byte(pattern)...)

	// Pad to desired length
	for len(data) < totalLen {
		data = append(data, ' ')
	}

	return data
}

func createPartialValidEvent(missingField, value string) []byte {
	// Create a JSON structure that will partially parse but fail validation
	jsonStr := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`

	fields := map[string]string{
		"address":     "0x1234567890123456789012345678901234567890",
		"blockNumber": "0x123456",
		"logIndex":    "0x1",
		"topics":      `["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"]`,
		"data":        "0xabcdef",
	}

	// Override the specific field
	fields[missingField] = value

	for field, val := range fields {
		if field == "topics" {
			jsonStr += fmt.Sprintf(`"%s":%s,`, field, val)
		} else {
			jsonStr += fmt.Sprintf(`"%s":"%s",`, field, val)
		}
	}

	jsonStr = jsonStr[:len(jsonStr)-1] + "}}" // Remove last comma and close
	return []byte(jsonStr)
}

func createLargeFieldEvent(fieldName string, size int) []byte {
	jsonStr := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`

	if fieldName == "data" {
		largeData := "0x" + strings.Repeat("a", size)
		jsonStr += fmt.Sprintf(`"data":"%s","address":"0x1234567890123456789012345678901234567890"}}`, largeData)
	} else if fieldName == "topics" {
		var topics []string
		for i := 0; i < size/70; i++ { // Each topic is about 70 chars
			topics = append(topics, fmt.Sprintf(`"0x%064d"`, i))
		}
		largeTopics := "[" + strings.Join(topics, ",") + "]"
		jsonStr += fmt.Sprintf(`"topics":%s,"address":"0x1234567890123456789012345678901234567890"}}`, largeTopics)
	}

	return []byte(jsonStr)
}

func createNonSyncEvent() []byte {
	// Create event with wrong signature to trigger early exit
	jsonStr := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`
	jsonStr += `"address":"0x1234567890123456789012345678901234567890",`
	jsonStr += `"blockNumber":"0x123456",`
	jsonStr += `"logIndex":"0x1",`
	jsonStr += `"data":"0xabcdef",`
	jsonStr += `"topics":["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"]}}` // Wrong signature

	return []byte(jsonStr)
}

func createShortTopicsEvent() []byte {
	// Create event with topics array too short for signature check
	jsonStr := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`
	jsonStr += `"address":"0x1234567890123456789012345678901234567890",`
	jsonStr += `"blockNumber":"0x123456",`
	jsonStr += `"logIndex":"0x1",`
	jsonStr += `"data":"0xabcdef",`
	jsonStr += `"topics":["0x123"]}}` // Too short

	return []byte(jsonStr)
}

func createTransactionTestEvent() []byte {
	// Create event that triggers transaction field handling
	jsonStr := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`
	jsonStr += `"address":"0x1234567890123456789012345678901234567890",`
	jsonStr += `"blockNumber":"0x123456",`
	jsonStr += `"logIndex":"0x1",`
	jsonStr += `"data":"0xabcdef",`
	jsonStr += `"topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"],`
	jsonStr += `"transactionHash":"0x1234567890123456789012345678901234567890123456789012345678901234",`
	jsonStr += `"transactionIndex":"0x5"}}`

	return []byte(jsonStr)
}

func createEndBoundaryEvent() []byte {
	// Create event that tests end boundary conditions
	jsonStr := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`
	jsonStr += `"address":"0x1234567890123456789012345678901234567890"`
	// Deliberately end without proper closing to test boundaries

	return []byte(jsonStr)
}

func TestGenerateFingerprint_AllBranches(t *testing.T) {
	tests := []struct {
		name     string
		topics   []byte
		data     []byte
		addr     []byte
		wantCase string
	}{
		{
			name:     "128-bit from topics",
			topics:   make([]byte, 20),
			data:     make([]byte, 8),
			addr:     make([]byte, 8),
			wantCase: "topics_128",
		},
		{
			name:     "64-bit from topics",
			topics:   make([]byte, 12),
			data:     make([]byte, 8),
			addr:     make([]byte, 8),
			wantCase: "topics_64",
		},
		{
			name:     "64-bit from data",
			topics:   make([]byte, 4),
			data:     make([]byte, 10),
			addr:     make([]byte, 8),
			wantCase: "data_64",
		},
		{
			name:     "64-bit from address",
			topics:   make([]byte, 4),
			data:     make([]byte, 4),
			addr:     make([]byte, 8),
			wantCase: "addr_64",
		},
		{
			name:     "All small",
			topics:   make([]byte, 4),
			data:     make([]byte, 4),
			addr:     make([]byte, 4),
			wantCase: "addr_64", // Falls through to default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fill with deterministic test data
			for i := range tt.topics {
				tt.topics[i] = byte(i%256 + 1) // Avoid zeros
			}
			for i := range tt.data {
				tt.data[i] = byte(i%256 + 1)
			}
			for i := range tt.addr {
				tt.addr[i] = byte(i%256 + 1)
			}

			v := &types.LogView{
				Topics: tt.topics,
				Data:   tt.data,
				Addr:   tt.addr,
			}

			generateFingerprint(v)

			// Verify some fingerprint was generated
			if len(tt.topics) >= 8 || len(tt.data) >= 8 || len(tt.addr) >= 8 {
				if v.TagLo == 0 && v.TagHi == 0 {
					t.Error("Expected some fingerprint to be generated")
				}
			}
		})
	}
}

func TestEmitLog_AllFields(t *testing.T) {
	// Test emitLog with various field combinations
	tests := []struct {
		name string
		view *types.LogView
	}{
		{
			name: "All fields present",
			view: &types.LogView{
				Addr:    []byte("0x1234567890123456789012345678901234567890"),
				BlkNum:  []byte("0x123456"),
				Data:    []byte("0xabcdef"),
				LogIdx:  []byte("0x1"),
				Topics:  []byte(`["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"]`),
				TxIndex: []byte("0x5"),
			},
		},
		{
			name: "Empty fields",
			view: &types.LogView{
				Addr:    []byte(""),
				BlkNum:  []byte(""),
				Data:    []byte(""),
				LogIdx:  []byte(""),
				Topics:  []byte(""),
				TxIndex: []byte(""),
			},
		},
		{
			name: "Long fields",
			view: &types.LogView{
				Addr:    []byte(strings.Repeat("a", 100)),
				BlkNum:  []byte(strings.Repeat("b", 100)),
				Data:    []byte(strings.Repeat("c", 100)),
				LogIdx:  []byte(strings.Repeat("d", 100)),
				Topics:  []byte(strings.Repeat("e", 100)),
				TxIndex: []byte(strings.Repeat("f", 100)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("emitLog panicked: %v", r)
				}
			}()

			emitLog(tt.view)
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ============================================================================
// STRESS TESTS
// ============================================================================

func TestHandleFrame_StressTest(t *testing.T) {
	event := createValidSyncEvent()

	// Process many events to test for memory leaks or panics
	for i := 0; i < 1000; i++ {
		HandleFrame(event)
	}
}

func TestHandleFrame_RandomData(t *testing.T) {
	// Test with random data to ensure no panics
	for i := 0; i < 100; i++ {
		randomData := make([]byte, 200+i*10)
		rand.Read(randomData)

		// Should not panic
		HandleFrame(randomData)
	}
}

// ============================================================================
// PERFORMANCE BENCHMARKS
// ============================================================================

func BenchmarkHandleFrame_ValidEvent(b *testing.B) {
	event := createValidSyncEvent()

	b.ReportAllocs()
	b.SetBytes(int64(len(event)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		HandleFrame(event)
	}
}

// ============================================================================
// PERFORMANCE BENCHMARKS
// ============================================================================

func BenchmarkHandleFrame_ShortFrame(b *testing.B) {
	shortEvent := make([]byte, 50)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		HandleFrame(shortEvent)
	}
}

func BenchmarkHandleFrame_LargeInput(b *testing.B) {
	largeEvent := make([]byte, 5000)
	// Fill with realistic JSON-like data
	copy(largeEvent, `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`)

	b.ReportAllocs()
	b.SetBytes(int64(len(largeEvent)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		HandleFrame(largeEvent)
	}
}

func BenchmarkGenerateFingerprint_AllSizes(b *testing.B) {
	sizes := []int{8, 16, 32, 64, 128}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("topics_%d", size), func(b *testing.B) {
			topics := make([]byte, size)
			for i := range topics {
				topics[i] = byte(i % 256)
			}

			v := &types.LogView{Topics: topics}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				generateFingerprint(v)
			}
		})
	}
}

// ============================================================================
// INTEGRATION VERIFICATION
// ============================================================================

func TestIntegration_NoGlobalStateCorruption(t *testing.T) {
	// Verify that failed parsing doesn't corrupt global state
	originalLatestBlk := latestBlk
	defer func() { latestBlk = originalLatestBlk }()

	// Try to process various invalid inputs
	inputs := [][]byte{
		make([]byte, 50),
		[]byte("invalid json"),
		make([]byte, 200),
		[]byte(`{"partial": "json`),
	}

	for i, input := range inputs {
		t.Run(fmt.Sprintf("input_%d", i), func(t *testing.T) {
			beforeBlk := latestBlk
			HandleFrame(input)
			afterBlk := latestBlk

			// Invalid inputs should not change latestBlk
			if beforeBlk != afterBlk {
				t.Errorf("latestBlk changed from %d to %d for invalid input", beforeBlk, afterBlk)
			}
		})
	}
}

func TestStress_ConcurrentSafety(t *testing.T) {
	// Test that multiple goroutines calling HandleFrame don't cause issues
	const numGoroutines = 10
	const numCalls = 100

	done := make(chan bool, numGoroutines)

	testData := [][]byte{
		make([]byte, 50),
		make([]byte, 200),
		[]byte("test data"),
	}

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Goroutine %d panicked: %v", id, r)
				}
				done <- true
			}()

			for i := 0; i < numCalls; i++ {
				HandleFrame(testData[i%len(testData)])
			}
		}(g)
	}

	// Wait for all goroutines to complete
	for g := 0; g < numGoroutines; g++ {
		<-done
	}
}
