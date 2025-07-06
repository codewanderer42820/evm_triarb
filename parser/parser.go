package parser

import (
	"main/constants"
	"main/debug"
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
			end := start + utils.SkipToQuote(p[start:], 8, 1)
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
			end := start + utils.SkipToQuote(p[start:], 3, 1)
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
			end := start + utils.SkipToQuote(p[start:], 3, 1)
			v.TxIndex = p[start:end]
			i = end + 1
		}
	}

	// Modified validation - check required fields in struct order (Addr, Data, BlkNum, LogIdx, TxIndex)
	// Topics is optional and can be empty
	if len(v.Addr) == 0 || len(v.Data) == 0 || len(v.BlkNum) == 0 || len(v.LogIdx) == 0 || len(v.TxIndex) == 0 {
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
	debug.DropMessage("[EVENT]", "")
	debug.DropMessage("  address", utils.B2s(v.Addr))
	debug.DropMessage("  block", utils.B2s(v.BlkNum))
	debug.DropMessage("  data", utils.B2s(v.Data))
	debug.DropMessage("  logIndex", utils.B2s(v.LogIdx))
	debug.DropMessage("  topics", utils.B2s(v.Topics))
	debug.DropMessage("  txIndex", utils.B2s(v.TxIndex))
}
