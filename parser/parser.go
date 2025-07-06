package parser

import (
	"fmt"
	"main/constants"
	"main/dedupe"
	"main/types"
	"main/utils"
	"unsafe"
)

// ============================================================================
// ETHEREUM LOG PARSER - HIGH-PERFORMANCE JSON PROCESSING
// ============================================================================

var (
	dedup     dedupe.Deduper
	latestBlk uint32
)

// HandleFrame processes JSON-RPC subscription messages containing Ethereum log data.
// Optimized for UniswapV2 Sync() events with zero-allocation parsing.
//
//go:nosplit
//go:inline
//go:registerparams
func HandleFrame(p []byte) {
	// Basic sanity check for subscription messages
	if len(p) < 117 {
		return
	}

	// Skip JSON-RPC wrapper (117 bytes)
	p = p[117:]

	var v types.LogView
	end := len(p) - 8

	// Main scanning loop with 8-byte aligned field detection
	for i := 0; i <= end; i++ {
		tag := *(*[8]byte)(unsafe.Pointer(&p[i]))

		switch tag {
		case constants.KeyAddress:
			start := i + utils.SkipToQuote(p[i:], 10, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 0, 42)
			v.Addr = p[start:end]
			i = end + 1

		case constants.KeyBlockHash:
			i += 80 // Skip entire field

		case constants.KeyBlockNumber:
			start := i + utils.SkipToQuote(p[i:], 14, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 0, 1)
			v.BlkNum = p[start:end]
			i = end + 1

		case constants.KeyBlockTimestamp:
			i += 29 // Skip Infura-specific field

		case constants.KeyData:
			start := i + utils.SkipToQuote(p[i:], 7, 1) + 1
			if end, exit := utils.SkipToQuoteEarlyExit(p[start+2:], 0, 64, 3); !exit {
				end += start + 2
				v.Data = p[start:end]
				i = end + 1
			} else {
				return // Data field too large
			}

		case constants.KeyLogIndex:
			start := i + utils.SkipToQuote(p[i:], 11, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 0, 1)
			v.LogIdx = p[start:end]
			i = end + 1

		case constants.KeyRemoved:
			i += 14 // Skip boolean field

		case constants.KeyTopics:
			start := i + utils.SkipToOpeningBracket(p[i:], 9, 1) + 1
			if end, exit := utils.SkipToClosingBracketEarlyExit(p[start-1:], 0, 69, 2); !exit {
				end += start - 1
				if end < start {
					end = start
				}
				v.Topics = p[start:end]

				// Early Sync() event validation
				if len(v.Topics) < 11 || *(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != constants.SigSyncPrefix {
					return
				}
				i = end + 1
			} else {
				return // Topics array too large
			}

		case constants.KeyTransaction:
			if len(p)-i >= 86 {
				i += 86 // Skip transaction hash
				continue
			}
			// Parse transaction index
			start := i + utils.SkipToQuote(p[i:], 19, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 0, 1)
			v.TxIndex = p[start:end]
			i = end + 1
		}
	}

	// Unified validation following LogView field sequence
	if len(v.Addr) == 0 || len(v.Data) == 0 || len(v.Topics) == 0 || len(v.BlkNum) == 0 || len(v.LogIdx) == 0 {
		utils.PrintWarning("Warning: Skipping event with missing required fields\n")
		return
	}

	// Generate fingerprint for deduplication
	generateFingerprint(&v)

	// Parse numeric fields
	blk32 := uint32(utils.ParseHexU64(v.BlkNum))
	tx32 := utils.ParseHexU32(v.TxIndex)
	log32 := utils.ParseHexU32(v.LogIdx)

	if blk32 > latestBlk {
		latestBlk = blk32
	}

	// Check for duplicates and emit if unique
	if dedup.Check(blk32, tx32, log32, v.TagHi, v.TagLo, latestBlk) {
		emitLog(&v)
	}
}

// generateFingerprint creates a unique identifier from available data fields
//
//go:nosplit
//go:inline
//go:registerparams
func generateFingerprint(v *types.LogView) {
	switch {
	case len(v.Topics) >= 16:
		v.TagHi, v.TagLo = utils.Load128(v.Topics)
	case len(v.Topics) >= 8:
		v.TagLo = utils.Load64(v.Topics)
	case len(v.Data) >= 8:
		v.TagLo = utils.Load64(v.Data)
	default:
		v.TagLo = utils.Load64(v.Addr)
	}
}

// emitLog outputs a deduplicated event using zero-copy string conversion
//
//go:nosplit
//go:inline
//go:registerparams
func emitLog(v *types.LogView) {
	fmt.Println("[EVENT]")
	fmt.Println("  address   =", utils.B2s(v.Addr))
	fmt.Println("  block     =", utils.B2s(v.BlkNum))
	fmt.Println("  data      =", utils.B2s(v.Data))
	fmt.Println("  logIndex  =", utils.B2s(v.LogIdx))
	fmt.Println("  topics    =", utils.B2s(v.Topics))
	fmt.Println("  txIndex   =", utils.B2s(v.TxIndex))
}

// ============================================================================
// DESIGN NOTES
// ============================================================================

/*
OPTIMIZATION FEATURES:

1. ZERO-ALLOCATION PARSING:
   - Direct slice references into JSON buffer
   - No string allocations during field extraction
   - Unsafe pointer operations for speed

2. EARLY EXIT CONDITIONS:
   - Sync() signature validation prevents wrong event processing
   - Size limits on data/topics fields avoid expensive parsing
   - Fast validation of required fields

3. CACHE-FRIENDLY DESIGN:
   - 8-byte aligned field detection
   - Sequential scanning pattern
   - Minimal branching in hot paths

4. DEDUPLICATION STRATEGY:
   - Multi-level fingerprinting (128-bit -> 64-bit -> fallback)
   - Block tracking for sliding window management
   - Fast hash generation from available data

PERFORMANCE CHARACTERISTICS:
- Zero allocations during message processing
- Sub-microsecond parsing for typical events
- Scales linearly with JSON-RPC throughput
- Memory usage bounded by buffer size
*/
