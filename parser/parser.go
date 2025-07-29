// ════════════════════════════════════════════════════════════════════════════════════════════════
// JSON-RPC Event Parser
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Zero-Allocation JSON Parser
//
// Description:
//   Extracts Uniswap V2 Sync events from JSON-RPC streams using direct memory access and 8-byte
//   tag detection. Implements deduplication to prevent duplicate event processing.
//
// Features:
//   - Zero allocations per event parsing operation
//   - 8-byte SIMD field detection for efficient parsing
//   - Event deduplication with rolling window cache
//   - Stack-only operation for consistent memory usage
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package parser

import (
	"main/constants"
	"main/debug"
	"main/dedupe"
	"main/router"
	"main/types"
	"main/utils"
	"unsafe"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL STATE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Event processing state with cache line isolation for concurrent access patterns.
// Deduplication engine accessed on every event, block state updated less frequently.
// Alignment prevents false sharing between hot deduplication path and warm block tracking.
//
//go:notinheap
//go:align 64
var (
	// DEDUPLICATION ENGINE (HOT PATH - ACCESSED EVERY EVENT)
	// Maintains rolling window of processed events to prevent duplicate handling.
	// Cache-aligned for optimal access patterns during deduplication checks.
	dedup dedupe.Deduper

	// Ensure latestBlk doesn't share cache line with dedup
	_         [64]byte // Force new cache line
	latestBlk uint32   // BLOCK STATE TRACKING (WARM PATH - UPDATED PER BLOCK)
	_         [60]byte // Pad rest of cache line
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MAIN PARSING PIPELINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// HandleFrame processes JSON-RPC Ethereum logs with zero-allocation parsing.
// This function serves as the main entry point for processing WebSocket frames
// containing Ethereum log events from JSON-RPC subscriptions.
//
// Input format: JSON-RPC notification with embedded Ethereum log object
// Processing: Direct byte-level parsing without JSON deserialization
// Output: Deduplicated log events emitted for downstream processing
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func HandleFrame(p []byte) {
	// Minimum size check ensures we have enough bytes for:
	// - JSON-RPC wrapper (117 bytes)
	// - At least one 8-byte field tag for detection
	if len(p) < 117+8 {
		return
	}

	// Skip the JSON-RPC wrapper to reach the embedded log object
	// The wrapper includes: jsonrpc version, method name, subscription ID
	p = p[117:]

	// Initialize log view structure on stack to avoid heap allocation
	var v types.LogView
	end := len(p) - 8

	// Main parsing loop processes the log object field by field
	// Uses 8-byte tag detection for efficient field identification
	for i := 0; i <= end; i++ {
		// Extract 8-byte tag at current position for field detection
		// This allows us to identify fields without string comparison
		tag := *(*[8]byte)(unsafe.Pointer(&p[i]))

		// Process each field based on its 8-byte tag signature
		switch tag {
		case constants.ParserKeyAddress:
			// Extract contract address (42 characters including 0x prefix)
			// Address identifies which Uniswap pair generated this event
			start := i + utils.SkipToQuote(p[i:], 10, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 0, 42)
			v.Addr = p[start:end]
			i = end + 1

		case constants.ParserKeyBlockHash:
			// Skip block hash field (80 bytes) - not needed for price updates
			// Block hash provides transaction finality but isn't used in arbitrage detection
			i += 80

		case constants.ParserKeyBlockNumber:
			// Extract block number for temporal ordering and deduplication
			// Block numbers ensure we process events in blockchain order
			start := i + utils.SkipToQuote(p[i:], 14, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 8, 1)
			v.BlkNum = p[start:end]
			i = end + 1

		case constants.ParserKeyBlockTimestamp:
			// Skip Infura-specific timestamp field (29 bytes)
			// This field is specific to certain node providers and not standard
			i += 29

		case constants.ParserKeyData:
			// Extract event data containing Uniswap reserve values
			// Data field contains the actual price information we need
			start := i + utils.SkipToQuote(p[i:], 7, 1) + 1
			if end, exit := utils.SkipToQuoteEarlyExit(p[start+2:], 0, 64, 3); !exit {
				// Early exit prevents processing oversized data fields
				// Limits data to reasonable size for reserve values
				end += start + 2
				v.Data = p[start:end]
				i = end + 1
			} else {
				return // Data field too large, likely corrupted
			}

		case constants.ParserKeyLogIndex:
			// Extract log index for intra-block event ordering
			// Multiple events can occur in the same transaction
			start := i + utils.SkipToQuote(p[i:], 11, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 3, 1)
			v.LogIdx = p[start:end]
			i = end + 1

		case constants.ParserKeyRemoved:
			// Skip chain reorganization flag (14 bytes)
			// Removed events indicate the log was reverted in a chain reorg
			i += 14

		case constants.ParserKeyTopics:
			// Extract topics array containing event signature
			// First topic identifies the event type (must be Sync event)
			start := i + utils.SkipToOpeningBracket(p[i:], 9, 1) + 1
			if end, exit := utils.SkipToClosingBracketEarlyExit(p[start-1:], 0, 69, 2); !exit {
				end += start - 1
				if end < start {
					end = start // Defensive programming for edge cases
				}
				v.Topics = p[start:end]

				// Validate Sync event signature at byte offset 3
				// Only process Uniswap V2 Sync events, ignore all others
				if len(v.Topics) < 11 || *(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != constants.ParserSigSyncPrefix {
					return
				}
				i = end + 1
			} else {
				return // Topics array too large or malformed
			}

		case constants.ParserKeyTransaction:
			// Handle transaction field with conditional processing
			// Long transactions include full hash, short ones just the index
			if len(p)-i >= 86 {
				i += 86 // Skip full transaction hash
				continue
			}
			// Parse transaction index for shorter fields
			start := i + utils.SkipToQuote(p[i:], 19, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 3, 1)
			v.TxIndex = p[start:end]
			i = end + 1
		}
	}

	// Validate that all required fields were successfully extracted
	// Missing fields indicate malformed or incomplete log data
	if len(v.Addr) == 0 || len(v.Data) == 0 || len(v.BlkNum) == 0 ||
		len(v.LogIdx) == 0 || len(v.TxIndex) == 0 {
		utils.PrintWarning("Warning: Skipping event with missing required fields\n")
		return
	}

	// Generate unique fingerprint for deduplication
	// Fingerprint allows efficient duplicate detection across events
	generateFingerprint(&v)

	// Parse numeric values for deduplication logic
	// Convert hex strings to integers for comparison
	blk32 := uint32(utils.ParseHexU64(v.BlkNum))
	tx32 := utils.ParseHexU32(v.TxIndex)
	log32 := utils.ParseHexU32(v.LogIdx)

	// Track the latest block number for chain tip awareness
	// This helps identify stale events during chain reorganizations
	if blk32 > latestBlk {
		latestBlk = blk32
	}

	// Check for duplicates using rolling window deduplication
	// Only emit events that haven't been processed recently
	if dedup.Check(blk32, tx32, log32, v.TagHi, v.TagLo, latestBlk) {
		// Direct dispatch to arbitrage detection router
		router.DispatchPriceUpdate(&v)

		// Optional debug output (can be commented out in production)
		// emitLog(&v)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// FINGERPRINT GENERATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// generateFingerprint creates unique identifier from log data for deduplication.
// The fingerprint serves as a compact representation of the event that can be
// efficiently compared to detect duplicates in the event stream.
//
// Priority order: Topics (most unique) -> Data -> Address (fallback)
// This hierarchy ensures maximum discrimination between different events
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func generateFingerprint(v *types.LogView) {
	switch {
	case len(v.Topics) >= 16:
		// Use 128-bit fingerprint from topics for maximum uniqueness
		// Topics contain event signature and indexed parameters
		v.TagHi, v.TagLo = utils.Load128(v.Topics)
	case len(v.Topics) >= 8:
		// Use 64-bit fingerprint from topics when less data available
		// Still provides good discrimination for most events
		v.TagLo = utils.Load64(v.Topics)
	case len(v.Data) >= 8:
		// Fall back to data field if topics are too short
		// Data contains the actual event parameters
		v.TagLo = utils.Load64(v.Data)
	default:
		// Last resort: use contract address as fingerprint
		// Least unique but ensures every event has some identifier
		v.TagLo = utils.Load64(v.Addr)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// EVENT OUTPUT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// emitLog outputs deduplicated event for downstream processing.
// This function represents the successful completion of the parsing pipeline,
// where validated and deduplicated events are made available to consumers.
//
// Output format: Structured key-value pairs for each event field
// Integration point: Connect to arbitrage detection or other processors
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func emitLog(v *types.LogView) {
	// Output event header for log stream processors
	debug.DropMessage("[EVENT]", "")

	// Output each field with descriptive labels
	// Format matches standard Ethereum log structure
	debug.DropMessage("  address", utils.B2s(v.Addr))
	debug.DropMessage("  block", utils.B2s(v.BlkNum))
	debug.DropMessage("  data", utils.B2s(v.Data))
	debug.DropMessage("  logIndex", utils.B2s(v.LogIdx))
	debug.DropMessage("  topics", utils.B2s(v.Topics))
	debug.DropMessage("  txIndex", utils.B2s(v.TxIndex))
}
