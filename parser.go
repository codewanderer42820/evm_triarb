// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: parser.go — ISR-grade zero-alloc JSON log parser
//
// Purpose:
//   - Scans raw WebSocket JSON payloads for critical fields.
//   - Feeds the deduper with fingerprinted events, ensuring unique event processing.
//
// Notes:
//   - No allocations, no heap pressure, no string conversions.
//   - All field detection uses 8-byte aligned probes from constants.go for efficient memory access.
//   - LogView slices point directly into wsBuf, enabling zero-copy operation until overwritten.
//
// ⚠️ Must not retain LogView after wsBuf rotation — pointer invalidation risk
// ─────────────────────────────────────────────────────────────────────────────

package main

import (
	"fmt"
	"main/types"
	"main/utils"
	"unsafe"
)

var (
	deduper   Deduper
	latestBlk uint32
)

// handleFrame processes a raw WebSocket frame containing a single log.
// If the frame contains a valid UniswapV2 Sync() event, it is deduplicated and printed.
//
// The function parses critical fields from the JSON payload (e.g., Address, Block Number, Data, Topics),
// calculates a fingerprint for deduplication, and feeds it to the deduper. If no duplicate is found,
// the event is passed on for further processing (e.g., emission).
//
// This method efficiently processes each WebSocket frame by using bit flags to track the fields that
// have been parsed and directly accessing the raw payload with zero-copy slicing.
//
//go:inline
//go:registerparams
func handleFrame(p []byte) {
	// Early exit if the payload is too short to process
	if len(p) < 117 {
		return
	}

	// Skip the first 117 bytes of the payload as per protocol
	p = p[117:]

	// Initialize LogView to store the extracted fields
	var v types.LogView

	// Define bit flags to track which fields are required from the frame
	// Each field has a unique bit in this bitmask
	const (
		wantAddress = 1 << iota
		wantBlockHash
		wantBlockNumber
		wantData
		wantLogIndex
		wantRemoved
		wantTopics
		wantTransaction
	)

	// Start with all fields marked as missing (not yet parsed)
	missing := wantAddress | wantBlockHash | wantBlockNumber | wantData | wantLogIndex | wantRemoved | wantTopics | wantTransaction
	end := len(p) - 8 // Set the end offset for parsing the frame

	// Loop through the byte slice to extract key data
	for i := 0; i <= end && missing != 0; i++ {
		// Extract an 8-byte tag for comparison
		tag := *(*[8]byte)(unsafe.Pointer(&p[i]))

		// Handle each tag by matching it to predefined keys and parsing accordingly
		switch tag {
		case keyAddress:
			// Parse the Address field (assuming it follows the expected format)
			start := i + utils.SkipToQuote(p[i:], 9, 1) + 1    // Start after the first quote
			end := start + utils.SkipToQuote(p[start:], 0, 42) // The second quote marks the end
			v.Addr = p[start:end]
			i = end + 1             // Update index after parsing the Address field
			missing &^= wantAddress // Mark Address as successfully parsed

		case keyBlockHash:
			// Skip over the block hash (80 bytes for a 0x-prefixed hex string)
			i += 80
			missing &^= wantBlockHash

		case keyBlockNumber:
			// Parse the Block Number field
			start := i + utils.SkipToQuote(p[i:], 13, 1) + 1  // Start after the first quote
			end := start + utils.SkipToQuote(p[start:], 0, 1) // The second quote marks the end
			v.BlkNum = p[start:end]
			i = end + 1                 // Update index after parsing the Block Number field
			missing &^= wantBlockNumber // Mark Block Number as successfully parsed

		case keyBlockTimestamp:
			// Skip over the block timestamp (29 bytes for Infura's "blockTimestamp" field)
			// This field is specific to Infura and doesn't require missing bitmask tracking.
			i += 29

		case keyData:
			// Parse the Data field
			start := i + utils.SkipToQuote(p[i:], 6, 1) + 1          // Start after the first quote
			end := start + 2 + utils.SkipToQuote(p[start+2:], 0, 64) // The second quote marks the end
			v.Data = p[start:end]
			i = end + 1          // Update index after parsing the Data field
			missing &^= wantData // Mark Data as successfully parsed

		case keyLogIndex:
			// Parse the Log Index field
			start := i + utils.SkipToQuote(p[i:], 10, 1) + 1  // Start after the first quote
			end := start + utils.SkipToQuote(p[start:], 0, 1) // The second quote marks the end
			v.LogIdx = p[start:end]
			i = end + 1              // Update index after parsing the Log Index field
			missing &^= wantLogIndex // Mark Log Index as successfully parsed

		case keyRemoved:
			// Skip over the "removed":true field
			i += 14 // "removed":true
			missing &^= wantRemoved

		case keyTopics:
			// Parse the Topics field (JSON array)
			start := i + utils.SkipToOpeningBracket(p[i:], 8, 1) + 1          // Start after the opening bracket
			end := start - 1 + utils.SkipToClosingBracket(p[start-1:], 0, 69) // The closing bracket marks the end
			// Ensure end is not less than start (self-correcting)
			if end < start {
				end = start
			}
			v.Topics = p[start:end]
			// Early exit if the Sync() signature doesn't match
			if len(v.Topics) < 11 || *(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != sigSyncPrefix {
				return // Exit early if it doesn't match Sync()
			}
			i = end + 1            // Update index after parsing the Topics field
			missing &^= wantTopics // Mark Topics as successfully parsed

		case keyTransaction:
			// Skip over 86 bytes of Transaction Hash (this is to bypass the transaction hash field)
			if len(p)-i >= 86 {
				i += 86
				continue
			}
			// Parse the Transaction Index field
			start := i + utils.SkipToQuote(p[i:], 18, 1) + 1  // Start after the first quote
			end := start + utils.SkipToQuote(p[start:], 0, 1) // The second quote marks the end
			v.TxIndex = p[start:end]
			i = end + 1                 // Update index after parsing the Transaction Index field
			missing &^= wantTransaction // Mark Transaction Index as successfully parsed
		}
	}

	// Check if logIndex is empty
	if len(v.LogIdx) == 0 {
		// Handle empty logIndex field
		utils.PrintWarning("Warning: Skipping log due to empty logIndex. This may indicate corrupted data or an invalid log entry.\n")
		return
	}

	// ───── Derive fingerprint ─────
	// Use the Topics or Data fields to generate a 128-bit or 64-bit fingerprint
	switch {
	case len(v.Topics) >= 16:
		// If Topics has 16 or more entries, use a 128-bit fingerprint
		v.TagHi, v.TagLo = utils.Load128(v.Topics)
	case len(v.Topics) >= 8:
		// If Topics has at least 8 entries, use a 64-bit fingerprint
		v.TagLo = utils.Load64(v.Topics)
	case len(v.Data) >= 8:
		// If Data has at least 8 bytes, use a 64-bit fingerprint from Data
		v.TagLo = utils.Load64(v.Data)
	default:
		return // Exit early if no valid fingerprint is available
	}

	// ───── Parse numeric fields ─────
	// Convert the Block Number, Transaction Index, and Log Index to integers
	blk32 := uint32(utils.ParseHexU64(v.BlkNum))
	tx32 := utils.ParseHexU32(v.TxIndex)
	log32 := utils.ParseHexU32(v.LogIdx)

	// Update the latest block number if needed
	if blk32 > latestBlk {
		latestBlk = blk32
	}

	// ───── Dedupe + Emit ─────
	// Check if the event is a duplicate based on the parsed fields and deduper logic
	if deduper.Check(blk32, tx32, log32, v.TagHi, v.TagLo, latestBlk) {
		// If the log is not a duplicate, emit it
		emitLog(&v)
	}
}

// emitLog prints a deduplicated event in ASCII, converting all []byte to string.
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
