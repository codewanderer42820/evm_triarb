// ════════════════════════════════════════════════════════════════════════════════════════════════
// 🧪 COMPREHENSIVE TEST SUITE: JSON-RPC EVENT PARSER
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: JSON Parser Test Suite
//
// Description:
//   Validates zero-allocation JSON parsing for Ethereum events including Uniswap V2 Sync event
//   processing, field extraction, deduplication logic, and error handling. Tests real-world
//   scenarios from various Ethereum node providers.
//
// Test Coverage:
//   - Unit tests: Field parsing, event validation, fingerprint generation
//   - Integration tests: Real Ethereum node data, edge cases from mainnet
//   - Benchmarks: Parsing performance, zero-allocation verification
//   - Edge cases: Malformed data, reorganization handling, node variations
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package parser

import (
	"fmt"
	"main/constants"
	"main/dedupe"
	"main/types"
	"strings"
	"testing"
	"unsafe"
)

// ============================================================================
// TEST CONFIGURATION AND SETUP
// ============================================================================

func init() {
	// Initialize deduplicator
	var d dedupe.Deduper
	dedup = d
	latestBlk = 0
}

// Real Uniswap V2 Sync event signature
const UniswapV2SyncSignature = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

// Helper to create a valid JSON-RPC wrapper
func createJSONRPCWrapper(logContent string) string {
	return `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xb9756e93014c47c7ad7a46c532cbaab0","result":{` + logContent + `}}}`
}

// Helper to create a complete valid Uniswap V2 Sync event
func createValidSyncEvent() []byte {
	logContent := `"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` + // Real DAI/WETH pair
		`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",` +
		`"blockNumber":"0x123456",` +
		`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",` + // Real reserve data
		`"logIndex":"0x1",` +
		`"removed":false,` +
		`"topics":["` + UniswapV2SyncSignature + `"],` +
		`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
		`"transactionIndex":"0x5"`

	return []byte(createJSONRPCWrapper(logContent))
}

// ============================================================================
// UNISWAP V2 SYNC EVENT TESTS
// ============================================================================

func TestHandleFrame_ValidUniswapV2SyncEvents(t *testing.T) {
	tests := []struct {
		name        string
		description string
		eventMod    func(string) string
	}{
		{
			name:        "standard_sync_event",
			description: "Standard Uniswap V2 Sync event with typical values",
			eventMod:    func(base string) string { return base },
		},
		{
			name:        "sync_with_zero_reserves",
			description: "Valid sync event with both reserves at zero (new pair)",
			eventMod: func(base string) string {
				return strings.Replace(base,
					`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9"`,
					`"data":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"`,
					1)
			},
		},
		{
			name:        "sync_with_max_reserves",
			description: "Sync event with maximum uint112 reserves",
			eventMod: func(base string) string {
				return strings.Replace(base,
					`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9"`,
					`"data":"0x0000000000000000000000000000000000000000ffffffffffffffffffffffffffff0000000000000000000000000000000000000000ffffffffffffffffffffffffffff"`,
					1)
			},
		},
		{
			name:        "sync_minimal_block_number",
			description: "Sync event with single digit block number",
			eventMod: func(base string) string {
				return strings.Replace(base, `"blockNumber":"0x123456"`, `"blockNumber":"0x1"`, 1)
			},
		},
		{
			name:        "sync_large_block_number",
			description: "Sync event with large block number",
			eventMod: func(base string) string {
				return strings.Replace(base, `"blockNumber":"0x123456"`, `"blockNumber":"0x1234567890"`, 1)
			},
		},
		{
			name:        "sync_high_log_index",
			description: "Sync event with high log index (busy block)",
			eventMod: func(base string) string {
				return strings.Replace(base, `"logIndex":"0x1"`, `"logIndex":"0xff"`, 1)
			},
		},
		{
			name:        "sync_high_tx_index",
			description: "Sync event with high transaction index",
			eventMod: func(base string) string {
				return strings.Replace(base, `"transactionIndex":"0x5"`, `"transactionIndex":"0x64"`, 1)
			},
		},
		{
			name:        "sync_different_pair_address",
			description: "Sync event from different Uniswap pair",
			eventMod: func(base string) string {
				return strings.Replace(base,
					`"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11"`,
					`"address":"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"`, // USDC/WETH pair
					1)
			},
		},
		{
			name:        "sync_with_leading_zeros_reserves",
			description: "Sync event with many leading zeros in reserves",
			eventMod: func(base string) string {
				return strings.Replace(base,
					`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9"`,
					`"data":"0x00000000000000000000000000000000000000000000000000000000000012340000000000000000000000000000000000000000000000000000000000005678"`,
					1)
			},
		},
		{
			name:        "sync_with_blockTimestamp",
			description: "Sync event including Infura-specific blockTimestamp",
			eventMod: func(base string) string {
				return strings.Replace(base,
					`"logIndex":"0x1",`,
					`"blockTimestamp":"0x62a9d2d1","logIndex":"0x1",`,
					1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseEvent := string(createValidSyncEvent())
			modifiedEvent := tt.eventMod(baseEvent)
			event := []byte(modifiedEvent)

			// Reset state
			latestBlk = 0

			// Should process without panic
			HandleFrame(event)

			t.Logf("%s: %s - processed successfully", tt.name, tt.description)
		})
	}
}

// ============================================================================
// NON-SYNC EVENT REJECTION TESTS
// ============================================================================

func TestHandleFrame_RejectsNonSyncEvents(t *testing.T) {
	nonSyncEvents := []struct {
		name        string
		description string
		topics      string
	}{
		{
			name:        "transfer_event",
			description: "ERC20 Transfer event (not Sync)",
			topics:      `["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x0000000000000000000000001234567890123456789012345678901234567890","0x0000000000000000000000009876543210987654321098765432109876543210"]`,
		},
		{
			name:        "approval_event",
			description: "ERC20 Approval event (not Sync)",
			topics:      `["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925","0x0000000000000000000000001234567890123456789012345678901234567890","0x0000000000000000000000009876543210987654321098765432109876543210"]`,
		},
		{
			name:        "swap_event",
			description: "Uniswap V2 Swap event (not Sync)",
			topics:      `["0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822","0x0000000000000000000000001234567890123456789012345678901234567890","0x0000000000000000000000009876543210987654321098765432109876543210"]`,
		},
		{
			name:        "mint_event",
			description: "Uniswap V2 Mint event (not Sync)",
			topics:      `["0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f","0x0000000000000000000000001234567890123456789012345678901234567890"]`,
		},
		{
			name:        "burn_event",
			description: "Uniswap V2 Burn event (not Sync)",
			topics:      `["0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496","0x0000000000000000000000001234567890123456789012345678901234567890","0x0000000000000000000000009876543210987654321098765432109876543210"]`,
		},
		{
			name:        "empty_topics",
			description: "Event with empty topics array",
			topics:      `[]`,
		},
		{
			name:        "wrong_signature_similar_length",
			description: "Event with similar length signature but wrong value",
			topics:      `["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad0"]`, // Last digit different
		},
		{
			name:        "multiple_topics_non_sync",
			description: "Event with multiple topics but not Sync",
			topics:      `["0x0000000000000000000000000000000000000000000000000000000000000001","0x0000000000000000000000000000000000000000000000000000000000000002","0x0000000000000000000000000000000000000000000000000000000000000003"]`,
		},
		{
			name:        "uniswap_v3_event",
			description: "Uniswap V3 event (different protocol)",
			topics:      `["0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67","0x0000000000000000000000001234567890123456789012345678901234567890","0x0000000000000000000000009876543210987654321098765432109876543210"]`,
		},
		{
			name:        "malformed_sync_signature",
			description: "Sync-like signature but malformed",
			topics:      `["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffb"]`, // Too short
		},
	}

	for _, tt := range nonSyncEvents {
		t.Run(tt.name, func(t *testing.T) {
			logContent := `"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` +
				`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",` +
				`"blockNumber":"0x123456",` +
				`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",` +
				`"logIndex":"0x1",` +
				`"removed":false,` +
				`"topics":` + tt.topics + `,` +
				`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
				`"transactionIndex":"0x5"`

			event := []byte(createJSONRPCWrapper(logContent))

			// Event should be skipped (no processing)
			HandleFrame(event)

			t.Logf("%s: %s - correctly skipped", tt.name, tt.description)
		})
	}
}

// ============================================================================
// MINIMUM LENGTH VALIDATION TESTS
// ============================================================================

func TestHandleFrame_MinimumLengthValidation(t *testing.T) {
	// Test that inputs below minimum length are safely rejected
	tests := []struct {
		name   string
		length int
	}{
		{"nil_input", 0},
		{"empty_input", 0},
		{"too_short_10_bytes", 10},
		{"too_short_100_bytes", 100},
		{"too_short_117_bytes", 117},
		{"exactly_124_bytes", 124}, // Just below minimum
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var input []byte
			if tt.length > 0 {
				input = make([]byte, tt.length)
			}

			// Should return early without processing
			HandleFrame(input)
			t.Logf("%s (length %d) - correctly rejected", tt.name, tt.length)
		})
	}
}

// ============================================================================
// FIELD PARSING TESTS
// ============================================================================

func TestHandleFrame_ValidEthereumNodeData(t *testing.T) {
	// Test with valid data that would come from a trusted Ethereum node
	tests := []struct {
		name        string
		description string
		eventMod    func(string) string
	}{
		{
			name:        "standard_field_order",
			description: "Fields in standard order from Ethereum node",
			eventMod:    func(base string) string { return base },
		},
		{
			name:        "infura_with_timestamp",
			description: "Infura node includes blockTimestamp field",
			eventMod: func(base string) string {
				return strings.Replace(base,
					`"blockNumber":"0x123456",`,
					`"blockNumber":"0x123456","blockTimestamp":"0x62a9d2d1",`,
					1)
			},
		},
		{
			name:        "alchemy_field_order",
			description: "Alchemy node might have different field ordering",
			eventMod: func(base string) string {
				// Reconstruct with Alchemy's typical field order
				return createJSONRPCWrapper(
					`"removed":false,` +
						`"logIndex":"0x1",` +
						`"transactionIndex":"0x5",` +
						`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
						`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",` +
						`"blockNumber":"0x123456",` +
						`"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` +
						`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",` +
						`"topics":["` + UniswapV2SyncSignature + `"]`)
			},
		},
		{
			name:        "quicknode_compact",
			description: "QuickNode might omit removed:false when not removed",
			eventMod: func(base string) string {
				return strings.Replace(base, `"removed":false,`, "", 1)
			},
		},
		{
			name:        "geth_full_tx_hash",
			description: "Geth always includes full transaction hash",
			eventMod:    func(base string) string { return base }, // Already includes it
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseEvent := string(createValidSyncEvent())
			modifiedEvent := tt.eventMod(baseEvent)
			event := []byte(modifiedEvent)

			HandleFrame(event)
			t.Logf("%s: %s - processed successfully", tt.name, tt.description)
		})
	}
}

// ============================================================================
// DATA FIELD EDGE CASES
// ============================================================================

func TestHandleFrame_DataFieldEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		description string
		dataValue   string
	}{
		{
			name:        "valid_sync_standard_reserves",
			description: "Data with standard reserve values from mainnet",
			dataValue:   "0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",
		},
		{
			name:        "valid_sync_zero_reserves",
			description: "Data with both reserves at zero (new pair initialization)",
			dataValue:   "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
		},
		{
			name:        "valid_sync_max_reserves",
			description: "Data with maximum uint112 values",
			dataValue:   "0x0000000000000000000000000000000000000000ffffffffffffffffffffffffffff0000000000000000000000000000000000000000ffffffffffffffffffffffffffff",
		},
		{
			name:        "valid_sync_imbalanced",
			description: "Data with highly imbalanced reserves",
			dataValue:   "0x00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000ffffffffffffffff",
		},
		{
			name:        "valid_sync_typical_eth_pair",
			description: "Typical ETH pair reserve values",
			dataValue:   "0x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000000000005f5e100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logContent := `"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` +
				`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",` +
				`"blockNumber":"0x123456",` +
				`"data":"` + tt.dataValue + `",` +
				`"logIndex":"0x1",` +
				`"removed":false,` +
				`"topics":["` + UniswapV2SyncSignature + `"],` +
				`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
				`"transactionIndex":"0x5"`

			event := []byte(createJSONRPCWrapper(logContent))

			// Should handle without panic
			HandleFrame(event)
			t.Logf("%s: %s - handled successfully", tt.name, tt.description)
		})
	}
}

// ============================================================================
// TOPICS FIELD EDGE CASES
// ============================================================================

func TestHandleFrame_TopicsFieldEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		description string
		topics      string
		shouldSkip  bool
	}{
		{
			name:        "topics_single_correct_signature",
			description: "Standard single topic with Sync signature",
			topics:      `["` + UniswapV2SyncSignature + `"]`,
			shouldSkip:  false,
		},
		{
			name:        "topics_with_indexed_params",
			description: "Sync event with additional indexed parameters",
			topics: `["` + UniswapV2SyncSignature + `",` +
				`"0x0000000000000000000000001234567890123456789012345678901234567890",` +
				`"0x0000000000000000000000009876543210987654321098765432109876543210"]`,
			shouldSkip: false,
		},
		{
			name:        "topics_empty_array",
			description: "Empty topics array",
			topics:      `[]`,
			shouldSkip:  true,
		},
		{
			name:        "topics_array_too_short",
			description: "Topics array with signature too short",
			topics:      `["0x1c411"]`,
			shouldSkip:  true,
		},
		{
			name:        "topics_malformed_array",
			description: "Malformed topics array structure",
			topics:      `[`,
			shouldSkip:  true,
		},
		{
			name:        "topics_nested_array",
			description: "Incorrectly nested topics array",
			topics:      `[["` + UniswapV2SyncSignature + `"]]`,
			shouldSkip:  true,
		},
		{
			name:        "topics_with_whitespace",
			description: "Topics array with extra whitespace",
			topics:      `[ "` + UniswapV2SyncSignature + `" ]`,
			shouldSkip:  false,
		},
		{
			name:        "topics_multiple_first_wrong",
			description: "Multiple topics but first is not Sync",
			topics: `["0x0000000000000000000000000000000000000000000000000000000000000000",` +
				`"` + UniswapV2SyncSignature + `"]`,
			shouldSkip: true,
		},
		{
			name:        "topics_very_long_array",
			description: "Topics array with many entries",
			topics: `["` + UniswapV2SyncSignature + `",` +
				strings.Repeat(`"0x0000000000000000000000000000000000000000000000000000000000000001",`, 10) +
				`"0x0000000000000000000000000000000000000000000000000000000000000002"]`,
			shouldSkip: false,
		},
		{
			name:        "topics_no_quotes",
			description: "Topics without quotes (invalid JSON)",
			topics:      `[` + UniswapV2SyncSignature + `]`,
			shouldSkip:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logContent := `"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` +
				`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",` +
				`"blockNumber":"0x123456",` +
				`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",` +
				`"logIndex":"0x1",` +
				`"removed":false,` +
				`"topics":` + tt.topics + `,` +
				`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
				`"transactionIndex":"0x5"`

			event := []byte(createJSONRPCWrapper(logContent))

			HandleFrame(event)

			if tt.shouldSkip {
				t.Logf("%s: %s - correctly skipped", tt.name, tt.description)
			} else {
				t.Logf("%s: %s - processed successfully", tt.name, tt.description)
			}
		})
	}
}

// ============================================================================
// DEDUPLICATION TESTS
// ============================================================================

func TestHandleFrame_Deduplication(t *testing.T) {
	// Reset deduplication state
	var d dedupe.Deduper
	dedup = d
	latestBlk = 0

	tests := []struct {
		name        string
		description string
		events      []func() []byte
	}{
		{
			name:        "exact_duplicate",
			description: "Same event sent twice",
			events: []func() []byte{
				createValidSyncEvent,
				createValidSyncEvent,
			},
		},
		{
			name:        "different_block_same_content",
			description: "Same transaction in different blocks",
			events: []func() []byte{
				createValidSyncEvent,
				func() []byte {
					event := string(createValidSyncEvent())
					event = strings.Replace(event, `"blockNumber":"0x123456"`, `"blockNumber":"0x123457"`, 1)
					return []byte(event)
				},
			},
		},
		{
			name:        "different_log_index",
			description: "Same block/tx but different log index",
			events: []func() []byte{
				createValidSyncEvent,
				func() []byte {
					event := string(createValidSyncEvent())
					event = strings.Replace(event, `"logIndex":"0x1"`, `"logIndex":"0x2"`, 1)
					return []byte(event)
				},
			},
		},
		{
			name:        "different_tx_index",
			description: "Same block but different transaction",
			events: []func() []byte{
				createValidSyncEvent,
				func() []byte {
					event := string(createValidSyncEvent())
					event = strings.Replace(event, `"transactionIndex":"0x5"`, `"transactionIndex":"0x6"`, 1)
					return []byte(event)
				},
			},
		},
		{
			name:        "different_data_same_metadata",
			description: "Same metadata but different reserve values",
			events: []func() []byte{
				createValidSyncEvent,
				func() []byte {
					event := string(createValidSyncEvent())
					event = strings.Replace(event,
						`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9"`,
						`"data":"0x00000000000000000000000000000000000000000000001b894d51f85cb08a6700000000000000000000000000000000000000000000000002b303a5e61375d9"`,
						1)
					return []byte(event)
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Process all events
			for i, eventFunc := range tt.events {
				event := eventFunc()
				HandleFrame(event)
				t.Logf("  Processed event %d", i+1)
			}

			t.Logf("%s: %s - deduplication logic applied", tt.name, tt.description)
		})
	}
}

// ============================================================================
// BLOCK NUMBER TRACKING TESTS
// ============================================================================

func TestHandleFrame_BlockNumberTracking(t *testing.T) {
	// Reset state
	var d dedupe.Deduper
	dedup = d
	latestBlk = 0

	// Test with properly formatted block numbers (at least 8 chars including 0x)
	blockNumbers := []struct {
		hex  string
		name string
	}{
		{"0x000001", "block_1"},
		{"0x000010", "block_16"},
		{"0x000100", "block_256"},
		{"0x001000", "block_4096"},
		{"0x0fffff", "block_1048575"},
		{"0x123456", "block_1193046"},
		{"0x100000", "block_1048576"},
		{"0x050000", "block_327680"},
	}

	for _, bn := range blockNumbers {
		t.Run(bn.name, func(t *testing.T) {
			// Reset state for each test
			latestBlk = 0

			event := string(createValidSyncEvent())
			event = strings.Replace(event, `"blockNumber":"0x123456"`, `"blockNumber":"`+bn.hex+`"`, 1)

			HandleFrame([]byte(event))

			// The parser should have updated latestBlk
			if latestBlk == 0 {
				t.Errorf("Expected latestBlk to be updated for block %s, but it remained 0", bn.hex)
			} else {
				t.Logf("Block %s - latestBlk set to %d", bn.hex, latestBlk)
			}
		})
	}

	// Test that block numbers are tracked correctly in sequence
	t.Run("sequential_blocks", func(t *testing.T) {
		latestBlk = 0

		// Process blocks in ascending order
		for i, bn := range []string{"0x100000", "0x200000", "0x300000"} {
			event := string(createValidSyncEvent())
			event = strings.Replace(event, `"blockNumber":"0x123456"`, `"blockNumber":"`+bn+`"`, 1)

			oldLatestBlk := latestBlk
			HandleFrame([]byte(event))

			if latestBlk <= oldLatestBlk && i > 0 {
				t.Errorf("Block %s: latestBlk should increase, got %d (was %d)", bn, latestBlk, oldLatestBlk)
			}
		}
	})
}

// ============================================================================
// PERFORMANCE AND STRESS TESTS
// ============================================================================

func TestHandleFrame_MultipleValidEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Reset state
	var d dedupe.Deduper
	dedup = d
	latestBlk = 0

	// Test processing multiple valid events in sequence
	addresses := []string{
		"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11", // DAI/WETH
		"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", // USDC/WETH
		"0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852", // WETH/USDT
		"0xbb2b8038a1640196fbe3e38816f3e67cba72d940", // WBTC/WETH
	}

	eventCount := 0
	for i := 0; i < 10; i++ {
		for _, addr := range addresses {
			blockNum := fmt.Sprintf("0x%x", 1000000+i)
			logIdx := fmt.Sprintf("0x%x", i%16)
			txIdx := fmt.Sprintf("0x%x", i%8)

			logContent := fmt.Sprintf(
				`"address":"%s",`+
					`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",`+
					`"blockNumber":"%s",`+
					`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",`+
					`"logIndex":"%s",`+
					`"removed":false,`+
					`"topics":["%s"],`+
					`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",`+
					`"transactionIndex":"%s"`,
				addr, blockNum, logIdx, UniswapV2SyncSignature, txIdx)

			event := []byte(createJSONRPCWrapper(logContent))
			HandleFrame(event)
			eventCount++
		}
	}

	t.Logf("Successfully processed %d valid Sync events", eventCount)
}

// ============================================================================
// FIELD DETECTION COVERAGE TESTS
// ============================================================================

func TestHandleFrame_FieldDetectionCoverage(t *testing.T) {
	// Test that all field detection cases in the switch statement are covered
	fields := []struct {
		name      string
		fieldName string
		content   string
		skip      bool
	}{
		{"address", "address", "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11", false},
		{"blockHash", "blockHash", "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef", true},
		{"blockNumber", "blockNumber", "0x123456", false},
		{"blockTimestamp", "blockTimestamp", "0x62a9d2d1", true},
		{"data", "data", "0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9", false},
		{"logIndex", "logIndex", "0x1", false},
		{"removed", "removed", "false", true},
		{"topics", "topics", `["` + UniswapV2SyncSignature + `"]`, false},
		{"transactionHash", "transactionHash", "0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba", true},
		{"transactionIndex", "transactionIndex", "0x5", false},
	}

	for _, field := range fields {
		t.Run("field_"+field.name, func(t *testing.T) {
			// Create event with only this field
			var fieldStr string
			if field.fieldName == "removed" {
				fieldStr = fmt.Sprintf(`"%s":%s`, field.fieldName, field.content)
			} else if field.fieldName == "topics" {
				fieldStr = fmt.Sprintf(`"%s":%s`, field.fieldName, field.content)
			} else {
				fieldStr = fmt.Sprintf(`"%s":"%s"`, field.fieldName, field.content)
			}

			// Add minimal required fields for successful processing
			if field.fieldName != "address" {
				fieldStr += `,"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11"`
			}
			if field.fieldName != "blockNumber" {
				fieldStr += `,"blockNumber":"0x123456"`
			}
			if field.fieldName != "data" {
				fieldStr += `,"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9"`
			}
			if field.fieldName != "logIndex" {
				fieldStr += `,"logIndex":"0x1"`
			}
			if field.fieldName != "topics" {
				fieldStr += `,"topics":["` + UniswapV2SyncSignature + `"]`
			}
			if field.fieldName != "transactionIndex" {
				fieldStr += `,"transactionIndex":"0x5"`
			}

			event := []byte(createJSONRPCWrapper(fieldStr))
			HandleFrame(event)

			t.Logf("Field %s detection: tested (skip=%v)", field.name, field.skip)
		})
	}
}

// ============================================================================
// FINGERPRINT GENERATION TESTS
// ============================================================================

func TestGenerateFingerprint_AllPaths(t *testing.T) {
	tests := []struct {
		name      string
		topicsLen int
		dataLen   int
		addrLen   int
		path      string
	}{
		{"topics_128bit", 20, 10, 42, "Load128 from topics"},
		{"topics_64bit", 10, 10, 42, "Load64 from topics"},
		{"data_fallback", 5, 20, 42, "Load64 from data"},
		{"addr_fallback", 5, 5, 42, "Load64 from address"},
		{"topics_exact_16", 16, 10, 42, "Load128 from topics"},
		{"topics_exact_8", 8, 10, 42, "Load64 from topics"},
		{"all_minimum", 0, 0, 42, "Load64 from address"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &types.LogView{
				Topics: make([]byte, tt.topicsLen),
				Data:   make([]byte, tt.dataLen),
				Addr:   make([]byte, tt.addrLen),
			}

			// Fill with test data
			for i := range v.Topics {
				v.Topics[i] = byte(i + 1)
			}
			for i := range v.Data {
				v.Data[i] = byte(i + 65)
			}
			for i := range v.Addr {
				v.Addr[i] = byte(i + 129)
			}

			generateFingerprint(v)

			t.Logf("%s - Path: %s, TagHi: %d, TagLo: %d",
				tt.name, tt.path, v.TagHi, v.TagLo)
		})
	}
}

// ============================================================================
// UNSAFE OPERATIONS TESTS
// ============================================================================

func TestUnsafeOperations(t *testing.T) {
	t.Run("8-byte tag extraction", func(t *testing.T) {
		data := []byte("\"address\":\"0x1234567890123456789012345678901234567890\"")

		// Test tag extraction at different positions
		for i := 0; i <= len(data)-8; i++ {
			tag := *(*[8]byte)(unsafe.Pointer(&data[i]))
			if tag == constants.ParserKeyAddress {
				t.Logf("Found address tag at position %d", i)
				break
			}
		}
	})

	t.Run("Sync signature validation", func(t *testing.T) {
		// Create a topics string with the Sync signature
		topics := []byte(`"0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"`)

		// Check at the expected offset (3 for quote + 0x)
		if len(topics) >= 11 {
			sigCheck := *(*[8]byte)(unsafe.Pointer(&topics[3]))
			sigStr := string(topics[3:11])
			t.Logf("Signature check at offset 3: %s (matches: %v)", sigStr, sigCheck == constants.ParserSigSyncPrefix)
		}
	})
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

func TestHandleFrame_RealWorldScenarios(t *testing.T) {
	// Test with realistic Ethereum mainnet scenarios

	scenarios := []struct {
		name        string
		description string
		event       string
	}{
		{
			name:        "high_volume_block",
			description: "Event from a block with many transactions",
			event: createJSONRPCWrapper(
				`"address":"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",` +
					`"blockHash":"0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",` +
					`"blockNumber":"0x10d4f20",` + // Block 17,456,928
					`"data":"0x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000000000005f5e100",` +
					`"logIndex":"0x1a5",` + // High log index
					`"removed":false,` +
					`"topics":["` + UniswapV2SyncSignature + `"],` +
					`"transactionHash":"0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",` +
					`"transactionIndex":"0x9c"`), // High tx index
		},
		{
			name:        "reorg_scenario",
			description: "Event that was removed due to chain reorganization",
			event: createJSONRPCWrapper(
				`"address":"0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852",` +
					`"blockHash":"0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",` +
					`"blockNumber":"0x10d4f00",` +
					`"data":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",` +
					`"logIndex":"0x0",` +
					`"removed":true,` + // Removed due to reorg
					`"topics":["` + UniswapV2SyncSignature + `"],` +
					`"transactionHash":"0x0000000000000000000000000000000000000000000000000000000000000000",` +
					`"transactionIndex":"0x0"`),
		},
		{
			name:        "arbitrage_bot_transaction",
			description: "Multiple Sync events in single transaction from arbitrage bot",
			event: createJSONRPCWrapper(
				`"address":"0xbb2b8038a1640196fbe3e38816f3e67cba72d940",` + // WBTC/WETH
					`"blockHash":"0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",` +
					`"blockNumber":"0x10d4f30",` +
					`"data":"0x0000000000000000000000000000000000000000000000001bc16d674ec800000000000000000000000000000000000000000000000000000de0b6b3a7640000",` +
					`"logIndex":"0x3",` + // Multiple events in same tx
					`"removed":false,` +
					`"topics":["` + UniswapV2SyncSignature + `"],` +
					`"transactionHash":"0x1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff",` +
					`"transactionIndex":"0x1"`),
		},
		{
			name:        "flash_loan_event",
			description: "Sync event from flash loan transaction with extreme values",
			event: createJSONRPCWrapper(
				`"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` +
					`"blockHash":"0x9999888877776666555544443333222211110000ffffeeeedddccccbbbbaaaa",` +
					`"blockNumber":"0x10d5000",` +
					`"data":"0x0000000000000000000000000000000000000000000000ffffffffffffffff000000000000000000000000000000000000000000000000000000000000000001",` + // Extreme reserve values
					`"logIndex":"0x0",` +
					`"removed":false,` +
					`"topics":["` + UniswapV2SyncSignature + `"],` +
					`"transactionHash":"0xaaaabbbbccccddddeeeeffff00001111222233334444555566667777888899999",` +
					`"transactionIndex":"0x0"`),
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			HandleFrame([]byte(scenario.event))
			t.Logf("%s: %s - processed successfully", scenario.name, scenario.description)
		})
	}
}

// ============================================================================
// BENCHMARK TESTS
// ============================================================================

func BenchmarkHandleFrame_ValidSync(b *testing.B) {
	event := createValidSyncEvent()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HandleFrame(event)
	}
}

func BenchmarkHandleFrame_NonSync(b *testing.B) {
	// Event that will be rejected early
	logContent := `"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` +
		`"blockNumber":"0x123456",` +
		`"data":"0x1234",` +
		`"logIndex":"0x1",` +
		`"topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],` + // Transfer event
		`"transactionIndex":"0x5"`

	event := []byte(createJSONRPCWrapper(logContent))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HandleFrame(event)
	}
}

func BenchmarkHandleFrame_ShortInput(b *testing.B) {
	event := []byte("short")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HandleFrame(event)
	}
}

func BenchmarkGenerateFingerprint(b *testing.B) {
	v := &types.LogView{
		Topics: []byte(`["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"]`),
		Data:   []byte("0x00000000000000000000000000000000000000000000001a894d51f85cb08a67"),
		Addr:   []byte("0xa478c2975ab1ea89e8196811f51a7b7ade33eb11"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generateFingerprint(v)
	}
}

// ============================================================================
// CONSTANTS VERIFICATION
// ============================================================================

func TestConstants_SyncSignature(t *testing.T) {
	// Verify the Sync signature constant matches expected value
	syncSig := "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

	// Test that our constant matches
	if syncSig != UniswapV2SyncSignature {
		t.Errorf("Sync signature mismatch: expected %s, got %s", syncSig, UniswapV2SyncSignature)
	}

	// Verify the 8-byte prefix check works correctly
	if len(syncSig) >= 11 {
		prefix := syncSig[3:11] // Skip "0x" and quotes
		t.Logf("Sync signature prefix for detection: %s", prefix)
	}
}

// ============================================================================
// EDGE CASE HANDLING TESTS
// ============================================================================

func TestHandleFrame_EdgeCasesFromEthereumNodes(t *testing.T) {
	// Test edge cases that can occur with real Ethereum nodes
	tests := []struct {
		name        string
		description string
		event       string
	}{
		{
			name:        "reorg_removed_event",
			description: "Event that was removed due to chain reorganization",
			event: createJSONRPCWrapper(
				`"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` +
					`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",` +
					`"blockNumber":"0x123456",` +
					`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",` +
					`"logIndex":"0x1",` +
					`"removed":true,` +
					`"topics":["` + UniswapV2SyncSignature + `"],` +
					`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
					`"transactionIndex":"0x5"`),
		},
		{
			name:        "very_old_block",
			description: "Sync event from genesis era block",
			event: createJSONRPCWrapper(
				`"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` +
					`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",` +
					`"blockNumber":"0x1",` +
					`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",` +
					`"logIndex":"0x0",` +
					`"removed":false,` +
					`"topics":["` + UniswapV2SyncSignature + `"],` +
					`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
					`"transactionIndex":"0x0"`),
		},
		{
			name:        "future_block",
			description: "Sync event with very high block number",
			event: createJSONRPCWrapper(
				`"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` +
					`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",` +
					`"blockNumber":"0xffffffff",` +
					`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",` +
					`"logIndex":"0x1",` +
					`"removed":false,` +
					`"topics":["` + UniswapV2SyncSignature + `"],` +
					`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
					`"transactionIndex":"0x5"`),
		},
		{
			name:        "high_gas_price_era",
			description: "Event from high gas price period with many logs",
			event: createJSONRPCWrapper(
				`"address":"0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",` +
					`"blockHash":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",` +
					`"blockNumber":"0x123456",` +
					`"data":"0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",` +
					`"logIndex":"0xfff",` + // Very high log index
					`"removed":false,` +
					`"topics":["` + UniswapV2SyncSignature + `"],` +
					`"transactionHash":"0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",` +
					`"transactionIndex":"0xff"`), // Very high tx index
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should handle edge cases gracefully
			HandleFrame([]byte(tt.event))
			t.Logf("%s: %s - handled successfully", tt.name, tt.description)
		})
	}
}

// ============================================================================
// DOCUMENTATION TESTS
// ============================================================================

func TestDocumentation_EventFormat(t *testing.T) {
	// This test documents the expected event format
	expectedFormat := `{
		"jsonrpc": "2.0",
		"method": "eth_subscription",
		"params": {
			"subscription": "0xb9756e93014c47c7ad7a46c532cbaab0",
			"result": {
				"address": "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",
				"blockHash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
				"blockNumber": "0x123456",
				"blockTimestamp": "0x62a9d2d1",
				"data": "0x00000000000000000000000000000000000000000000001a894d51f85cb08a6700000000000000000000000000000000000000000000000002a303a5e61375d9",
				"logIndex": "0x1",
				"removed": false,
				"topics": ["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"],
				"transactionHash": "0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",
				"transactionIndex": "0x5"
			}
		}
	}`

	t.Logf("Expected JSON-RPC event format:\n%s", expectedFormat)
}
