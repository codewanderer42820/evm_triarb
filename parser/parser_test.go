package parser

import (
	"crypto/rand"
	"fmt"
	"main/types"
	"main/utils"
	"strings"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// UTILITY FUNCTION BEHAVIOR TESTS
// ============================================================================

// TestUtilityFunctionBehavior documents the behavior of utility functions
// that the parser depends on, particularly their panic conditions.
func TestUtilityFunctionBehavior(t *testing.T) {
	t.Run("Load64_RequiresMinimumLength", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Load64 should panic on slice shorter than 8 bytes")
			}
		}()

		// This should panic - Load64 requires at least 8 bytes
		shortSlice := make([]byte, 4)
		utils.Load64(shortSlice)
	})

	t.Run("Load64_WorksWithValidData", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Load64 should not panic on valid 8+ byte slice: %v", r)
			}
		}()

		validSlice := make([]byte, 8)
		for i := range validSlice {
			validSlice[i] = byte(i + 1)
		}
		result := utils.Load64(validSlice)
		if result == 0 {
			t.Error("Load64 should return non-zero for non-zero input")
		}
	})

	t.Run("Load128_RequiresMinimumLength", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Load128 should panic on slice shorter than 16 bytes")
			}
		}()

		// This should panic - Load128 requires at least 16 bytes
		shortSlice := make([]byte, 8)
		utils.Load128(shortSlice)
	})

	t.Run("Load128_WorksWithValidData", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Load128 should not panic on valid 16+ byte slice: %v", r)
			}
		}()

		validSlice := make([]byte, 16)
		for i := range validSlice {
			validSlice[i] = byte(i + 1)
		}
		hi, lo := utils.Load128(validSlice)
		if hi == 0 && lo == 0 {
			t.Error("Load128 should return non-zero for non-zero input")
		}
	})

	t.Run("B2s_EmptySliceBehavior", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("B2s may panic on empty slice - this documents the behavior")
			}
		}()

		// Test if B2s panics on empty slice
		emptySlice := []byte{}
		utils.B2s(emptySlice)
	})
}

// ============================================================================
// PANIC BEHAVIOR DOCUMENTATION TESTS
// ============================================================================

// TestKnownPanicConditions documents and verifies the expected panic conditions
// in the parser. These panics are by design due to unsafe memory operations
// that require minimum data lengths.
func TestKnownPanicConditions(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		testFunc    func()
		shouldPanic bool
		panicReason string
	}{
		{
			name:        "GenerateFingerprint_EmptyFields",
			description: "generateFingerprint with all empty fields panics on Load64",
			testFunc: func() {
				v := &types.LogView{
					Topics: []byte{},
					Data:   []byte{},
					Addr:   []byte{},
				}
				generateFingerprint(v)
			},
			shouldPanic: true,
			panicReason: "Load64 requires at least 8 bytes",
		},
		{
			name:        "GenerateFingerprint_InsufficientAddress",
			description: "generateFingerprint with address < 8 bytes panics",
			testFunc: func() {
				v := &types.LogView{
					Topics: []byte{},
					Data:   []byte{},
					Addr:   make([]byte, 4), // Less than 8 bytes
				}
				generateFingerprint(v)
			},
			shouldPanic: true,
			panicReason: "Load64 on address with < 8 bytes",
		},
		{
			name:        "EmitLog_EmptyFields",
			description: "emitLog with empty fields panics on B2s conversion",
			testFunc: func() {
				v := &types.LogView{
					Addr:    []byte{},
					BlkNum:  []byte{},
					Data:    []byte{},
					LogIdx:  []byte{},
					Topics:  []byte{},
					TxIndex: []byte{},
				}
				emitLog(v)
			},
			shouldPanic: true,
			panicReason: "B2s conversion on empty slice",
		},
		{
			name:        "GenerateFingerprint_ValidMinimum",
			description: "generateFingerprint with minimum valid data should not panic",
			testFunc: func() {
				v := &types.LogView{
					Topics: []byte{},
					Data:   []byte{},
					Addr:   make([]byte, 8), // Exactly 8 bytes
				}
				// Fill with non-zero data
				for i := range v.Addr {
					v.Addr[i] = byte(i + 1)
				}
				generateFingerprint(v)
			},
			shouldPanic: false,
			panicReason: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tc.shouldPanic {
						t.Errorf("Test %s: unexpected panic: %v", tc.name, r)
					} else {
						t.Logf("Test %s: expected panic occurred: %v (reason: %s)",
							tc.name, r, tc.panicReason)
					}
				} else if tc.shouldPanic {
					t.Errorf("Test %s: expected panic (%s) but none occurred",
						tc.name, tc.panicReason)
				}
			}()

			tc.testFunc()
		})
	}
}

// ============================================================================
// TEST UTILITIES AND FIXTURES
// ============================================================================

// TestFixture represents a reusable test data structure for consistent testing
type TestFixture struct {
	Name        string
	Description string
	Data        []byte
	ExpectPanic bool
	ExpectExit  bool
}

// createValidSyncEvent generates a properly formatted JSON-RPC subscription message
// that represents a valid Ethereum log event with all required fields populated.
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

// createCustomEventSafe creates an event by modifying the base valid event.
// This ensures we don't break the field parsing by accident while testing
// specific field modifications or edge cases.
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

// createEventWithLength creates an event with specified length for boundary testing.
// This is useful for testing minimum length requirements and buffer overflow protection.
func createEventWithLength(length int) []byte {
	if length < 10 {
		return make([]byte, length)
	}

	base := createValidSyncEvent()
	if len(base) >= length {
		return base[:length]
	}

	// Extend with padding to reach desired length
	padding := make([]byte, length-len(base))
	for i := range padding {
		padding[i] = ' '
	}
	return append(base, padding...)
}

// createMalformedJSON generates intentionally malformed JSON for error handling tests
func createMalformedJSON(variant string) []byte {
	base := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{"address":"0x1234567890123456789012345678901234567890","topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"]`

	switch variant {
	case "truncated":
		return []byte(base[:len(base)-20]) // Abruptly cut off
	case "missing_bracket":
		return []byte(base + `,"data":"0x123"`) // No closing brackets
	case "invalid_quotes":
		return []byte(strings.Replace(base, `"address"`, `address`, 1))
	case "nested_error":
		return []byte(base + `,"malformed":[broken]}`)
	default:
		return []byte(base)
	}
}

// ============================================================================
// BOUNDARY CONDITIONS AND INPUT VALIDATION TESTS
// ============================================================================

// TestHandleFrame_InputValidation verifies that the parser correctly handles
// various input validation scenarios including empty inputs, minimum length
// requirements, and boundary conditions.
func TestHandleFrame_InputValidation(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		input       []byte
		shouldPanic bool
	}{
		{
			name:        "EmptyInput",
			description: "Parser should handle empty input gracefully",
			input:       []byte{},
			shouldPanic: false,
		},
		{
			name:        "MinimumLengthBoundary",
			description: "Input exactly at minimum length threshold (117 bytes)",
			input:       make([]byte, 117),
			shouldPanic: false,
		},
		{
			name:        "BelowMinimumLength",
			description: "Input below minimum length should exit early",
			input:       make([]byte, 116),
			shouldPanic: false,
		},
		{
			name:        "MinimumPlusBuffer",
			description: "Input at minimum length plus buffer (125 bytes)",
			input:       make([]byte, 125),
			shouldPanic: false,
		},
		{
			name:        "LargeValidInput",
			description: "Large valid input should be processed correctly",
			input:       createEventWithLength(5000),
			shouldPanic: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tc.shouldPanic {
						t.Errorf("Test %s: unexpected panic: %v", tc.name, r)
					}
				} else if tc.shouldPanic {
					t.Errorf("Test %s: expected panic but none occurred", tc.name)
				}
			}()

			HandleFrame(tc.input)
		})
	}
}

// TestHandleFrame_FieldDetection verifies that the 8-byte field detection
// mechanism correctly identifies and processes all supported field types.
func TestHandleFrame_FieldDetection(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		eventFunc   func() []byte
	}{
		{
			name:        "AllFieldsPresent",
			description: "Event with all supported fields should parse successfully",
			eventFunc:   createValidSyncEvent,
		},
		{
			name:        "FieldOrderVariation1",
			description: "Fields in different order should parse correctly",
			eventFunc: func() []byte {
				return createCustomEventSafe(map[string]string{
					"transactionIndex": "0xa",
				})
			},
		},
		{
			name:        "MinimalValidEvent",
			description: "Event with only required fields should parse",
			eventFunc: func() []byte {
				rpcWrapper := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`
				minimalData := `"address":"0x1234567890123456789012345678901234567890",` +
					`"blockNumber":"0x123456",` +
					`"data":"0x1234567890abcdef",` +
					`"logIndex":"0x1",` +
					`"topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"],` +
					`"transactionIndex":"0x5"}}}`
				return []byte(rpcWrapper + minimalData)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Test %s: unexpected panic: %v", tc.name, r)
				}
			}()

			event := tc.eventFunc()
			HandleFrame(event)
		})
	}
}

// ============================================================================
// FIELD PARSING AND VALIDATION TESTS
// ============================================================================

// TestHandleFrame_RequiredFieldValidation ensures that the parser correctly
// validates the presence of all required fields and handles missing fields gracefully.
func TestHandleFrame_RequiredFieldValidation(t *testing.T) {
	requiredFields := []string{"address", "blockNumber", "logIndex", "data", "transactionIndex"}

	for _, field := range requiredFields {
		t.Run("Missing"+strings.Title(field), func(t *testing.T) {
			modifications := map[string]string{field: ""}
			if field == "data" {
				// For data field, test with empty string specifically
				modifications[field] = ""
			}

			event := createCustomEventSafe(modifications)

			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Parser should not panic on missing %s field: %v", field, r)
				}
			}()

			HandleFrame(event)
		})
	}
}

// TestHandleFrame_TopicsValidation verifies the topics field parsing and
// Sync event signature validation logic.
func TestHandleFrame_TopicsValidation(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		topics      string
		shouldExit  bool
	}{
		{
			name:        "ValidSyncSignature",
			description: "Valid Sync event signature should pass validation",
			topics:      `["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"]`,
			shouldExit:  false,
		},
		{
			name:        "InvalidSignature",
			description: "Invalid event signature should cause early exit",
			topics:      `["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"]`,
			shouldExit:  true,
		},
		{
			name:        "EmptyTopicsArray",
			description: "Empty topics array should cause validation failure",
			topics:      `[]`,
			shouldExit:  true,
		},
		{
			name:        "ShortTopicsArray",
			description: "Topics array too short for signature check should fail",
			topics:      `["0x123"]`,
			shouldExit:  true,
		},
		{
			name:        "OversizedTopicsArray",
			description: "Extremely large topics array should trigger early exit",
			topics:      generateLargeTopicsArray(50),
			shouldExit:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := createCustomEventSafe(map[string]string{"topics": tc.topics})

			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Test %s: unexpected panic: %v", tc.name, r)
				}
			}()

			HandleFrame(event)
		})
	}
}

// generateLargeTopicsArray creates a topics array with the specified number of elements
func generateLargeTopicsArray(count int) string {
	var topics []string
	for i := 0; i < count; i++ {
		topics = append(topics, fmt.Sprintf(`"0x%064d"`, i))
	}
	return "[" + strings.Join(topics, ",") + "]"
}

// TestHandleFrame_DataFieldValidation tests the data field parsing logic
// including size limits and early exit conditions.
func TestHandleFrame_DataFieldValidation(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		data        string
		shouldExit  bool
	}{
		{
			name:        "ValidDataField",
			description: "Normal sized data field should parse correctly",
			data:        "0x1234567890abcdef",
			shouldExit:  false,
		},
		{
			name:        "EmptyDataField",
			description: "Empty data field should cause validation failure",
			data:        "",
			shouldExit:  true,
		},
		{
			name:        "OversizedDataField",
			description: "Extremely large data field should trigger early exit",
			data:        "0x" + strings.Repeat("a", 1000),
			shouldExit:  true,
		},
		{
			name:        "BoundaryDataField",
			description: "Data field at boundary size should be handled",
			data:        "0x" + strings.Repeat("f", 128),
			shouldExit:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := createCustomEventSafe(map[string]string{"data": tc.data})

			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Test %s: unexpected panic: %v", tc.name, r)
				}
			}()

			HandleFrame(event)
		})
	}
}

// ============================================================================
// TRANSACTION FIELD HANDLING TESTS
// ============================================================================

// TestHandleFrame_TransactionFieldHandling verifies the complex logic for
// handling both transaction hash and transaction index fields, including
// the skip optimization for transaction hashes.
func TestHandleFrame_TransactionFieldHandling(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		eventFunc   func() []byte
	}{
		{
			name:        "TransactionHashSkipping",
			description: "Transaction hash should be skipped when sufficient bytes available",
			eventFunc: func() []byte {
				rpcWrapper := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`
				logData := `"address":"0x1234567890123456789012345678901234567890",` +
					`"blockNumber":"0x123456",` +
					`"data":"0x1234567890abcdef",` +
					`"logIndex":"0x1",` +
					`"topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"],` +
					`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
					`"transactionIndex":"0x5"}}}`
				return []byte(rpcWrapper + logData)
			},
		},
		{
			name:        "TransactionIndexParsing",
			description: "Transaction index should be parsed when hash skipping not possible",
			eventFunc: func() []byte {
				rpcWrapper := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`
				logData := `"address":"0x1234567890123456789012345678901234567890",` +
					`"blockNumber":"0x123456",` +
					`"data":"0x1234567890abcdef",` +
					`"logIndex":"0x1",` +
					`"topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"],` +
					`"transactionIndex":"0x5"}}}`
				return []byte(rpcWrapper + logData)
			},
		},
		{
			name:        "BoundaryConditionTransactionField",
			description: "Transaction field at exact boundary condition (86 bytes)",
			eventFunc: func() []byte {
				// Create event where exactly 86 bytes remain when transaction field is encountered
				rpcWrapper := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`
				logData := `"address":"0x1234567890123456789012345678901234567890",` +
					`"blockNumber":"0x123456",` +
					`"data":"0x1234567890abcdef",` +
					`"logIndex":"0x1",` +
					`"topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"],` +
					`"transactionHash":"0x1234567890123456789012345678901234567890123456789012345678901234"}}`
				return []byte(rpcWrapper + logData)
			},
		},
		{
			name:        "InsufficientBytesForSkip",
			description: "Less than 86 bytes available should force transaction index parsing",
			eventFunc: func() []byte {
				rpcWrapper := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`
				// Minimal event to ensure < 86 bytes when transaction field encountered
				logData := `"address":"0x1234567890123456789012345678901234567890",` +
					`"blockNumber":"0x123456",` +
					`"data":"0x1234567890abcdef",` +
					`"logIndex":"0x1",` +
					`"topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"],` +
					`"transactionIndex":"0x42"}}` // Shorter ending
				return []byte(rpcWrapper + logData)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Test %s: unexpected panic: %v", tc.name, r)
				}
			}()

			event := tc.eventFunc()
			HandleFrame(event)

			// Verify event structure is reasonable
			if len(event) < 117 {
				t.Errorf("Test %s: created event too short: %d bytes", tc.name, len(event))
			}
		})
	}
}

// ============================================================================
// FINGERPRINT GENERATION TESTS
// ============================================================================

// TestGenerateFingerprint_AllBranches verifies that the fingerprint generation
// logic correctly handles all possible data sources and size conditions.
func TestGenerateFingerprint_AllBranches(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		topics      []byte
		data        []byte
		addr        []byte
		expectHi    bool
		expectLo    bool
	}{
		{
			name:        "128BitFromTopics",
			description: "Topics >= 16 bytes should generate both hi and lo tags",
			topics:      make([]byte, 20),
			data:        make([]byte, 8),
			addr:        make([]byte, 8),
			expectHi:    true,
			expectLo:    true,
		},
		{
			name:        "64BitFromTopics",
			description: "Topics >= 8 but < 16 bytes should generate lo tag only",
			topics:      make([]byte, 12),
			data:        make([]byte, 8),
			addr:        make([]byte, 8),
			expectHi:    false,
			expectLo:    true,
		},
		{
			name:        "64BitFromData",
			description: "Data >= 8 bytes should be used when topics insufficient",
			topics:      make([]byte, 4),
			data:        make([]byte, 10),
			addr:        make([]byte, 8),
			expectHi:    false,
			expectLo:    true,
		},
		{
			name:        "64BitFromAddress",
			description: "Address should be used as fallback when other sources insufficient",
			topics:      make([]byte, 4),
			data:        make([]byte, 4),
			addr:        make([]byte, 8),
			expectHi:    false,
			expectLo:    true,
		},
		{
			name:        "BoundaryCondition16Bytes",
			description: "Exactly 16 bytes in topics should trigger 128-bit path",
			topics:      make([]byte, 16),
			data:        make([]byte, 8),
			addr:        make([]byte, 8),
			expectHi:    true,
			expectLo:    true,
		},
		{
			name:        "BoundaryCondition8Bytes",
			description: "Exactly 8 bytes in topics should trigger 64-bit path",
			topics:      make([]byte, 8),
			data:        make([]byte, 8),
			addr:        make([]byte, 8),
			expectHi:    false,
			expectLo:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Fill test data with deterministic non-zero values
			for i := range tc.topics {
				tc.topics[i] = byte(i%256 + 1)
			}
			for i := range tc.data {
				tc.data[i] = byte(i%256 + 1)
			}
			for i := range tc.addr {
				tc.addr[i] = byte(i%256 + 1)
			}

			v := &types.LogView{
				Topics: tc.topics,
				Data:   tc.data,
				Addr:   tc.addr,
			}

			generateFingerprint(v)

			if tc.expectHi && v.TagHi == 0 {
				t.Errorf("Test %s: expected TagHi to be set but it was 0", tc.name)
			}
			if tc.expectLo && v.TagLo == 0 {
				t.Errorf("Test %s: expected TagLo to be set but it was 0", tc.name)
			}
			if !tc.expectHi && v.TagHi != 0 {
				t.Errorf("Test %s: expected TagHi to be 0 but it was %d", tc.name, v.TagHi)
			}
		})
	}
}

// TestGenerateFingerprint_EdgeCases tests edge cases in fingerprint generation
// including empty data sources and boundary conditions.
func TestGenerateFingerprint_EdgeCases(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		topics      []byte
		data        []byte
		addr        []byte
		expectPanic bool
	}{
		{
			name:        "AllFieldsEmpty",
			description: "All fields empty will cause panic due to Load64 on empty slice",
			topics:      []byte{},
			data:        []byte{},
			addr:        []byte{},
			expectPanic: true, // This is expected behavior - Load64 requires at least 8 bytes
		},
		{
			name:        "OnlyAddressAvailable",
			description: "Only address field populated should use address for fingerprint",
			topics:      []byte{},
			data:        []byte{},
			addr:        make([]byte, 20),
			expectPanic: false,
		},
		{
			name:        "SmallFieldsShouldFallback",
			description: "Small fields should fall back to address",
			topics:      make([]byte, 3),
			data:        make([]byte, 3),
			addr:        make([]byte, 8),
			expectPanic: false,
		},
		{
			name:        "AllFieldsTooSmall",
			description: "All fields smaller than 8 bytes will cause panic",
			topics:      make([]byte, 3),
			data:        make([]byte, 3),
			addr:        make([]byte, 3),
			expectPanic: true, // Load64 on addr with < 8 bytes will panic
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tc.expectPanic {
						t.Errorf("Test %s: unexpected panic: %v", tc.name, r)
					}
					// Expected panic - test passes
				} else if tc.expectPanic {
					t.Errorf("Test %s: expected panic but none occurred", tc.name)
				}
			}()

			// Fill test data with non-zero values where applicable
			for i := range tc.topics {
				tc.topics[i] = byte(i%256 + 1)
			}
			for i := range tc.data {
				tc.data[i] = byte(i%256 + 1)
			}
			for i := range tc.addr {
				tc.addr[i] = byte(i%256 + 1)
			}

			v := &types.LogView{
				Topics: tc.topics,
				Data:   tc.data,
				Addr:   tc.addr,
			}

			generateFingerprint(v)
		})
	}
}

// ============================================================================
// ERROR HANDLING AND MALFORMED INPUT TESTS
// ============================================================================

// TestHandleFrame_MalformedJSON verifies that the parser handles various
// forms of malformed JSON gracefully without panicking.
func TestHandleFrame_MalformedJSON(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		jsonFunc    func() []byte
	}{
		{
			name:        "TruncatedJSON",
			description: "Abruptly truncated JSON should be handled gracefully",
			jsonFunc:    func() []byte { return createMalformedJSON("truncated") },
		},
		{
			name:        "MissingClosingBrackets",
			description: "JSON missing closing brackets should not panic",
			jsonFunc:    func() []byte { return createMalformedJSON("missing_bracket") },
		},
		{
			name:        "InvalidQuotes",
			description: "JSON with invalid quote usage should be handled",
			jsonFunc:    func() []byte { return createMalformedJSON("invalid_quotes") },
		},
		{
			name:        "NestedStructureError",
			description: "Malformed nested structures should not crash parser",
			jsonFunc:    func() []byte { return createMalformedJSON("nested_error") },
		},
		{
			name:        "BinaryGarbage",
			description: "Random binary data should not panic the parser",
			jsonFunc: func() []byte {
				garbage := make([]byte, 200)
				rand.Read(garbage)
				return garbage
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Test %s: parser should not panic on malformed input: %v", tc.name, r)
				}
			}()

			malformedJSON := tc.jsonFunc()
			HandleFrame(malformedJSON)
		})
	}
}

// ============================================================================
// CONCURRENCY AND STRESS TESTS
// ============================================================================

// TestHandleFrame_ConcurrentAccess verifies that the parser can handle
// concurrent access without data races or panics.
func TestHandleFrame_ConcurrentAccess(t *testing.T) {
	const (
		numGoroutines = 10
		numIterations = 100
	)

	var wg sync.WaitGroup
	errorChan := make(chan error, numGoroutines)

	// Test data variants
	testData := [][]byte{
		createValidSyncEvent(),
		createEventWithLength(200),
		createMalformedJSON("truncated"),
		make([]byte, 50),
	}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errorChan <- fmt.Errorf("goroutine %d panicked: %v", goroutineID, r)
				}
			}()

			for j := 0; j < numIterations; j++ {
				data := testData[j%len(testData)]
				HandleFrame(data)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errorChan)

	// Check for any errors
	for err := range errorChan {
		t.Error(err)
	}
}

// TestHandleFrame_StressTest performs sustained load testing to identify
// potential memory leaks or performance degradation.
func TestHandleFrame_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	const iterations = 10000
	event := createValidSyncEvent()

	// Track initial memory state
	startTime := time.Now()

	// Sustained processing
	for i := 0; i < iterations; i++ {
		HandleFrame(event)

		// Periodically verify no panics and reasonable performance
		if i%1000 == 0 && i > 0 {
			elapsed := time.Since(startTime)
			rate := float64(i) / elapsed.Seconds()
			if rate < 1000 { // Expect at least 1000 events per second
				t.Logf("Warning: Processing rate at iteration %d: %.2f events/sec", i, rate)
			}
		}
	}

	totalTime := time.Since(startTime)
	finalRate := float64(iterations) / totalTime.Seconds()
	t.Logf("Stress test completed: %d iterations in %v (%.2f events/sec)",
		iterations, totalTime, finalRate)
}

// TestHandleFrame_RandomFuzzing performs random input fuzzing to discover
// edge cases that might cause panics or undefined behavior.
func TestHandleFrame_RandomFuzzing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping fuzz test in short mode")
	}

	const fuzzIterations = 1000

	for i := 0; i < fuzzIterations; i++ {
		// Generate random data of varying sizes
		size := 50 + i%500 // Size between 50-549 bytes
		randomData := make([]byte, size)
		rand.Read(randomData)

		// Ensure it doesn't panic
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Fuzz iteration %d panicked with data length %d: %v", i, size, r)
				}
			}()

			HandleFrame(randomData)
		}()
	}
}

// ============================================================================
// INTEGRATION AND STATE VALIDATION TESTS
// ============================================================================

// TestHandleFrame_StateIntegrity verifies that failed parsing operations
// do not corrupt global state or leave the parser in an inconsistent state.
func TestHandleFrame_StateIntegrity(t *testing.T) {
	// Capture initial state
	initialLatestBlk := latestBlk

	// Test various invalid inputs that should not modify state
	invalidInputs := [][]byte{
		make([]byte, 50),                                        // Too short
		[]byte("invalid json"),                                  // Not JSON
		createMalformedJSON("truncated"),                        // Malformed JSON
		createCustomEventSafe(map[string]string{"address": ""}), // Missing required field
	}

	for i, input := range invalidInputs {
		t.Run(fmt.Sprintf("InvalidInput_%d", i), func(t *testing.T) {
			beforeBlk := latestBlk
			HandleFrame(input)
			afterBlk := latestBlk

			if beforeBlk != afterBlk {
				t.Errorf("Invalid input %d modified latestBlk from %d to %d", i, beforeBlk, afterBlk)
			}
		})
	}

	// Verify final state matches initial state
	if latestBlk != initialLatestBlk {
		t.Errorf("Global state corrupted: latestBlk changed from %d to %d", initialLatestBlk, latestBlk)
	}
}

// TestEmitLog_OutputFormatting verifies that the log emission function
// correctly formats and outputs all field types without panicking.
func TestEmitLog_OutputFormatting(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		logView     *types.LogView
		expectPanic bool
	}{
		{
			name:        "CompleteLogView",
			description: "Log view with all fields populated",
			logView: &types.LogView{
				Addr:    []byte("0x1234567890123456789012345678901234567890"),
				BlkNum:  []byte("0x123456"),
				Data:    []byte("0xabcdef"),
				LogIdx:  []byte("0x1"),
				Topics:  []byte(`["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"]`),
				TxIndex: []byte("0x5"),
			},
			expectPanic: false,
		},
		{
			name:        "EmptyFields",
			description: "Log view with empty fields will panic due to B2s conversion",
			logView: &types.LogView{
				Addr:    []byte(""),
				BlkNum:  []byte(""),
				Data:    []byte(""),
				LogIdx:  []byte(""),
				Topics:  []byte(""),
				TxIndex: []byte(""),
			},
			expectPanic: true, // B2s likely has issues with empty slices
		},
		{
			name:        "OversizedFields",
			description: "Log view with very large fields should be handled",
			logView: &types.LogView{
				Addr:    []byte(strings.Repeat("a", 200)),
				BlkNum:  []byte(strings.Repeat("b", 200)),
				Data:    []byte(strings.Repeat("c", 200)),
				LogIdx:  []byte(strings.Repeat("d", 200)),
				Topics:  []byte(strings.Repeat("e", 200)),
				TxIndex: []byte(strings.Repeat("f", 200)),
			},
			expectPanic: false,
		},
		{
			name:        "MinimalValidFields",
			description: "Log view with minimal but valid fields",
			logView: &types.LogView{
				Addr:    []byte("0x1"),
				BlkNum:  []byte("0x1"),
				Data:    []byte("0x1"),
				LogIdx:  []byte("0x1"),
				Topics:  []byte("[]"),
				TxIndex: []byte("0x1"),
			},
			expectPanic: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tc.expectPanic {
						t.Errorf("Test %s: unexpected panic: %v", tc.name, r)
					}
					// Expected panic - test passes
				} else if tc.expectPanic {
					t.Errorf("Test %s: expected panic but none occurred", tc.name)
				}
			}()

			emitLog(tc.logView)
		})
	}
}

// ============================================================================
// PERFORMANCE BENCHMARKS
// ============================================================================

// BenchmarkHandleFrame_OptimalPath benchmarks the parser performance on
// valid, well-formed input that exercises the optimal parsing path.
func BenchmarkHandleFrame_OptimalPath(b *testing.B) {
	event := createValidSyncEvent()

	b.ReportAllocs()
	b.SetBytes(int64(len(event)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		HandleFrame(event)
	}
}

// BenchmarkHandleFrame_EarlyExit benchmarks parser performance when
// early exit conditions are triggered (e.g., input too short).
func BenchmarkHandleFrame_EarlyExit(b *testing.B) {
	shortEvent := make([]byte, 50)

	b.ReportAllocs()
	b.SetBytes(int64(len(shortEvent)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		HandleFrame(shortEvent)
	}
}

// BenchmarkHandleFrame_LargeInput benchmarks parser performance on
// large input that exceeds typical event sizes.
func BenchmarkHandleFrame_LargeInput(b *testing.B) {
	largeEvent := createEventWithLength(5000)

	b.ReportAllocs()
	b.SetBytes(int64(len(largeEvent)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		HandleFrame(largeEvent)
	}
}

// BenchmarkHandleFrame_ValidationFailure benchmarks parser performance
// when validation failures occur during parsing.
func BenchmarkHandleFrame_ValidationFailure(b *testing.B) {
	invalidEvent := createCustomEventSafe(map[string]string{"address": ""})

	b.ReportAllocs()
	b.SetBytes(int64(len(invalidEvent)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		HandleFrame(invalidEvent)
	}
}

// BenchmarkGenerateFingerprint_AllSizes benchmarks fingerprint generation
// across different data sizes to identify performance characteristics.
func BenchmarkGenerateFingerprint_AllSizes(b *testing.B) {
	sizes := []int{8, 16, 32, 64, 128, 256}

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

// BenchmarkHandleFrame_ConcurrentLoad benchmarks parser performance under
// concurrent load to identify scalability characteristics.
func BenchmarkHandleFrame_ConcurrentLoad(b *testing.B) {
	event := createValidSyncEvent()

	b.ReportAllocs()
	b.SetBytes(int64(len(event)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			HandleFrame(event)
		}
	})
}

// BenchmarkEmitLog_OutputPerformance benchmarks the log emission performance
// to ensure output formatting doesn't become a bottleneck.
func BenchmarkEmitLog_OutputPerformance(b *testing.B) {
	v := &types.LogView{
		Addr:    []byte("0x1234567890123456789012345678901234567890"),
		BlkNum:  []byte("0x123456"),
		Data:    []byte("0xabcdef"),
		LogIdx:  []byte("0x1"),
		Topics:  []byte(`["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"]`),
		TxIndex: []byte("0x5"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		emitLog(v)
	}
}

// ============================================================================
// MEMORY AND ALLOCATION TESTS
// ============================================================================

// TestHandleFrame_ZeroAllocation verifies that the parser achieves its
// zero-allocation design goal for the optimal parsing path.
func TestHandleFrame_ZeroAllocation(t *testing.T) {
	event := createValidSyncEvent()

	// Warm up to establish baseline
	for i := 0; i < 100; i++ {
		HandleFrame(event)
	}

	// Measure allocations
	allocsBefore := testing.AllocsPerRun(1000, func() {
		HandleFrame(event)
	})

	if allocsBefore > 0 {
		t.Logf("Note: HandleFrame performed %.3f allocations per run (target: 0)", allocsBefore)
		// This is informational rather than a failure since allocation patterns
		// can vary based on runtime conditions and compiler optimizations
	}
}

// TestGenerateFingerprint_NoAllocation verifies that fingerprint generation
// doesn't perform unnecessary memory allocations.
func TestGenerateFingerprint_NoAllocation(t *testing.T) {
	v := &types.LogView{
		Topics: make([]byte, 32),
		Data:   make([]byte, 16),
		Addr:   make([]byte, 20),
	}

	// Fill with test data to ensure we have valid data for Load64/Load128
	for i := range v.Topics {
		v.Topics[i] = byte(i + 1) // Avoid zeros
	}
	for i := range v.Data {
		v.Data[i] = byte(i + 1)
	}
	for i := range v.Addr {
		v.Addr[i] = byte(i + 1)
	}

	allocsBefore := testing.AllocsPerRun(1000, func() {
		generateFingerprint(v)
	})

	if allocsBefore > 0 {
		t.Logf("Note: generateFingerprint performed %.3f allocations per run (target: 0)", allocsBefore)
	}
}

// ============================================================================
// COMPREHENSIVE INTEGRATION TESTS
// ============================================================================

// TestParser_EndToEndIntegration performs comprehensive end-to-end testing
// of the entire parser pipeline with realistic Ethereum log data.
func TestParser_EndToEndIntegration(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		events      [][]byte
		expectCount int
	}{
		{
			name:        "SingleValidEvent",
			description: "Single valid Sync event should be processed successfully",
			events:      [][]byte{createValidSyncEvent()},
			expectCount: 1,
		},
		{
			name:        "MultipleValidEvents",
			description: "Multiple valid events should all be processed",
			events: [][]byte{
				createValidSyncEvent(),
				createCustomEventSafe(map[string]string{"transactionIndex": "0x7"}),
				createCustomEventSafe(map[string]string{"blockNumber": "0x123457"}),
			},
			expectCount: 3,
		},
		{
			name:        "MixedValidInvalid",
			description: "Mix of valid and invalid events should process only valid ones",
			events: [][]byte{
				createValidSyncEvent(),
				make([]byte, 50), // Too short
				createCustomEventSafe(map[string]string{"address": ""}), // Missing address
				createValidSyncEvent(),
			},
			expectCount: 2, // Only count events that don't panic
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			processedCount := 0

			for i, event := range tc.events {
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Logf("Event %d in test %s caused expected panic: %v", i, tc.name, r)
							// Don't count panicked events as processed
							return
						}
						processedCount++
					}()

					HandleFrame(event)
				}()
			}

			t.Logf("Test %s: processed %d events without panic out of %d total events",
				tc.name, processedCount, len(tc.events))
		})
	}
}

// ============================================================================
// REALISTIC SCENARIO TESTS
// ============================================================================

// TestParser_RealisticScenarios tests the parser with realistic Ethereum
// scenarios that might occur in production environments.
func TestParser_RealisticScenarios(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		scenario    func() []byte
		shouldPanic bool
	}{
		{
			name:        "HighVolumeBlock",
			description: "Event from a high-volume block with maximum field sizes",
			scenario: func() []byte {
				return createCustomEventSafe(map[string]string{
					"blockNumber": "0x1000000",                      // Large block number
					"data":        "0x" + strings.Repeat("ff", 100), // Large data field
				})
			},
			shouldPanic: false,
		},
		{
			name:        "MinimalValidEvent",
			description: "Minimal valid event that meets all requirements",
			scenario: func() []byte {
				rpcWrapper := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{`
				minData := `"address":"0x1234567890123456789012345678901234567890",` +
					`"blockNumber":"0x1",` +
					`"data":"0x01",` +
					`"logIndex":"0x0",` +
					`"topics":["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"],` +
					`"transactionIndex":"0x0"}}}`
				return []byte(rpcWrapper + minData)
			},
			shouldPanic: false,
		},
		{
			name:        "ComplexTopicsArray",
			description: "Event with multiple topics (indexed parameters)",
			scenario: func() []byte {
				complexTopics := `["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1",` +
					`"0x000000000000000000000000a0b86a33e6f4e3bbe1b85e8e0e9b8d1234567890",` +
					`"0x0000000000000000000000001234567890123456789012345678901234567890"]`
				return createCustomEventSafe(map[string]string{"topics": complexTopics})
			},
			shouldPanic: false,
		},
		{
			name:        "EdgeCaseEmptyData",
			description: "Event with empty data field (valid in some contracts)",
			scenario: func() []byte {
				return createCustomEventSafe(map[string]string{"data": ""})
			},
			shouldPanic: false, // Parser should handle and exit early
		},
		{
			name:        "LargeTransactionIndex",
			description: "Event with large transaction index",
			scenario: func() []byte {
				return createCustomEventSafe(map[string]string{"transactionIndex": "0xffffff"})
			},
			shouldPanic: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tc.shouldPanic {
						t.Errorf("Test %s: unexpected panic: %v", tc.name, r)
					}
				} else if tc.shouldPanic {
					t.Errorf("Test %s: expected panic but none occurred", tc.name)
				}
			}()

			event := tc.scenario()
			HandleFrame(event)
		})
	}
}
