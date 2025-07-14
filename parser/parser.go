package parser

import (
	"main/constants"
	"main/debug"
	"main/dedupe"
	"main/types"
	"main/utils"
	"unsafe"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL PARSER STATE - CACHE-OPTIMIZED AND ZERO-ALLOCATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════
//
// Parser state is minimized to two critical components:
// 1. Deduplication engine for preventing duplicate event processing
// 2. Block tracking for maintaining chain state consistency
//
// Both structures are cache-aligned and designed for maximum memory efficiency
// during high-frequency JSON parsing operations.

//go:notinheap
//go:align 64
var (
	// DEDUPLICATION ENGINE (HOT PATH - ACCESSED EVERY EVENT)
	// Maintains rolling window of processed events to prevent duplicate handling.
	// Cache-aligned for optimal access patterns during deduplication checks.
	dedup dedupe.Deduper

	// BLOCK STATE TRACKING (WARM PATH - UPDATED PER BLOCK)
	// Tracks the highest block number processed to maintain chain consistency.
	// Aligned to prevent false sharing with the deduplication engine.
	latestBlk uint32
)

// HandleFrame processes JSON-RPC Ethereum logs with zero-allocation parsing
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func HandleFrame(p []byte) {
	// Minimum size check: RPC wrapper + field detection
	if len(p) < 117+8 {
		return
	}

	// Skip JSON-RPC wrapper
	p = p[117:]

	var v types.LogView
	end := len(p) - 8

	// Main parsing loop with 8-byte field detection
	for i := 0; i <= end; i++ {
		tag := *(*[8]byte)(unsafe.Pointer(&p[i]))

		switch tag {
		case constants.KeyAddress:
			// Extract contract address
			start := i + utils.SkipToQuote(p[i:], 10, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 0, 42)
			v.Addr = p[start:end]
			i = end + 1

		case constants.KeyBlockHash:
			i += 80 // Skip unused field

		case constants.KeyBlockNumber:
			// Extract block number
			start := i + utils.SkipToQuote(p[i:], 14, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 8, 1)
			v.BlkNum = p[start:end]
			i = end + 1

		case constants.KeyBlockTimestamp:
			i += 29 // Skip Infura-specific field

		case constants.KeyData:
			// Extract event data
			start := i + utils.SkipToQuote(p[i:], 7, 1) + 1
			if end, exit := utils.SkipToQuoteEarlyExit(p[start+2:], 0, 64, 3); !exit {
				end += start + 2
				v.Data = p[start:end]
				i = end + 1
			} else {
				return // Data too large
			}

		case constants.KeyLogIndex:
			// Extract log index
			start := i + utils.SkipToQuote(p[i:], 11, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 3, 1)
			v.LogIdx = p[start:end]
			i = end + 1

		case constants.KeyRemoved:
			i += 14 // Skip boolean

		case constants.KeyTopics:
			// Extract topics array
			start := i + utils.SkipToOpeningBracket(p[i:], 9, 1) + 1
			if end, exit := utils.SkipToClosingBracketEarlyExit(p[start-1:], 0, 69, 2); !exit {
				end += start - 1
				if end < start {
					end = start
				}
				v.Topics = p[start:end]

				// Validate Sync event signature
				if len(v.Topics) < 11 || *(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != constants.SigSyncPrefix {
					return
				}
				i = end + 1
			} else {
				return // Topics too large
			}

		case constants.KeyTransaction:
			if len(p)-i >= 86 {
				i += 86 // Skip tx hash
				continue
			}
			// Parse tx index
			start := i + utils.SkipToQuote(p[i:], 19, 1) + 1
			end := start + utils.SkipToQuote(p[start:], 3, 1)
			v.TxIndex = p[start:end]
			i = end + 1
		}
	}

	// Validate required fields
	if len(v.Addr) == 0 || len(v.Data) == 0 || len(v.BlkNum) == 0 ||
		len(v.LogIdx) == 0 || len(v.TxIndex) == 0 {
		utils.PrintWarning("Warning: Skipping event with missing required fields\n")
		return
	}

	// Generate deduplication fingerprint
	generateFingerprint(&v)

	// Parse numeric values
	blk32 := uint32(utils.ParseHexU64(v.BlkNum))
	tx32 := utils.ParseHexU32(v.TxIndex)
	log32 := utils.ParseHexU32(v.LogIdx)

	// Track latest block
	if blk32 > latestBlk {
		latestBlk = blk32
	}

	// Check duplicates and emit
	if dedup.Check(blk32, tx32, log32, v.TagHi, v.TagLo, latestBlk) {
		// emitLog(&v)
	}
}

// generateFingerprint creates unique identifier from log data
//
//go:norace
//go:nocheckptr
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

// emitLog outputs deduplicated event
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func emitLog(v *types.LogView) {
	debug.DropMessage("[EVENT]", "")
	debug.DropMessage("  address", utils.B2s(v.Addr))
	debug.DropMessage("  block", utils.B2s(v.BlkNum))
	debug.DropMessage("  data", utils.B2s(v.Data))
	debug.DropMessage("  logIndex", utils.B2s(v.LogIdx))
	debug.DropMessage("  topics", utils.B2s(v.Topics))
	debug.DropMessage("  txIndex", utils.B2s(v.TxIndex))
}
