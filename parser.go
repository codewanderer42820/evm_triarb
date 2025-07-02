// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: parser.go — ISR-grade zero-alloc JSON log parser
//
// Purpose:
//   - Scans raw WebSocket JSON payloads for critical fields
//   - Feeds the deduper with fingerprinted events
//
// Notes:
//   - No allocations, no heap pressure, no string conversions
//   - All field detection uses 8-byte aligned probes from constants.go
//   - LogView slices point directly into wsBuf — zero-copy until overwritten
//
// Compiler Directives:
//   - //go:nosplit
//   - //go:registerparams
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

// skipToQuote finds the next '"' after a ':', using hop-based traversal for efficiency
func skipToQuote(p []byte, startIdx int, hopSize int) int {
	i := startIdx

	for ; i < len(p); i += hopSize {
		if p[i] == '"' {
			return i
		}
	}

	return -1
}

// skipToBracket finds the next '"' after a ':', using hop-based traversal for efficiency
func skipToBracket0(p []byte, startIdx int, hopSize int) int {
	i := startIdx

	for ; i < len(p); i += hopSize {
		if p[i] == '[' {
			return i
		}
	}

	return -1
}

// skipToBracket finds the next '"' after a ':', using hop-based traversal for efficiency
func skipToBracket1(p []byte, startIdx int, hopSize int) int {
	i := startIdx

	for ; i < len(p); i += hopSize {
		if p[i] == ']' {
			return i
		}
	}

	return -1
}

// handleFrame processes a raw WebSocket frame containing a single log.
// If the frame contains a valid UniswapV2 Sync() event, it is deduplicated and printed.
//
// The function scans for critical fields in the payload, updates the LogView,
// and derives a fingerprint if the event matches a valid Sync event.
// It then checks for duplicates and emits the event if it's not a duplicate.
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
	const (
		wantAddr = 1 << iota
		wantTopics
		wantData
		wantBlk
		wantTx
		wantLog
	)

	// Start with all fields marked as missing
	missing := wantAddr | wantTopics | wantData | wantBlk | wantTx | wantLog
	end := len(p) - 8 // Set the end offset for parsing the frame

	// Loop through the byte slice to extract key data
	for i := 0; i <= end && missing != 0; i++ {
		// Extract an 8-byte tag for comparison
		tag := *(*[8]byte)(unsafe.Pointer(&p[i]))

		// Handle each tag by matching it to predefined keys and parsing accordingly
		switch {
		case tag == keyAddress:
			// Parse the Address field (assuming it follows the expected format)
			start := i + skipToQuote(p[i:], 9, 1) + 1    // Start after the first quote
			end := start + skipToQuote(p[start:], 0, 42) // The second quote marks the end
			v.Addr = p[start:end]
			i = end + 1          // Update index after parsing the Address field
			missing &^= wantAddr // Mark Address as successfully parsed

		case tag == keyTopics:
			// Parse the Topics field
			start := i + skipToBracket0(p[i:], 8, 1) + 1          // Start after the first quote
			end := start - 1 + skipToBracket1(p[start-1:], 0, 69) // The second quote marks the end
			// Ensure end is not less than start (self-correcting)
			if end < start {
				end = start
			}
			v.Topics = p[start:end]
			// Early exit if the Sync() signature doesn't match
			if len(v.Topics) < 11 || *(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != sigSyncPrefix {
				return // Exit early if it doesn't match Sync()
			}
			i = end + 1            // Update index after parsing the Address field
			missing &^= wantTopics // Mark Topics as successfully parsed

		case tag == keyData:
			// Parse the Data field
			start := i + skipToQuote(p[i:], 6, 1) + 1          // Start after the first quote
			end := start + 2 + skipToQuote(p[start+2:], 0, 64) // The second quote marks the end
			v.Data = p[start:end]

			i = end + 1          // Update index after parsing the Address field
			missing &^= wantData // Mark Data as successfully parsed

		case tag == keyBlockNumber:
			// Parse the Block Number field
			base := i + 12
			v.BlkNum = utils.SliceASCII(p, base+utils.FindQuote(p[base:]))
			if len(v.BlkNum) == 0 {
				return // Exit early if Block Number is missing
			}
			i = base + len(v.BlkNum) + 3
			missing &^= wantBlk // Mark Block Number as successfully parsed

		case tag == keyTransactionIndex:
			// Skip over 85 bytes of Transaction Hash (this is to bypass the transaction hash field)
			if len(p)-i >= 85 {
				i += 85
				continue
			}
			// Parse the Transaction Index field
			base := i + 17
			v.TxIndex = utils.SliceASCII(p, base+utils.FindQuote(p[base:]))
			if len(v.TxIndex) == 0 {
				return // Exit early if Transaction Index is missing
			}
			missing &^= wantTx // Mark Transaction Index as successfully parsed

		case tag == keyLogIndex:
			// Parse the Log Index field
			base := i + 9
			v.LogIdx = utils.SliceASCII(p, base+utils.FindQuote(p[base:]))
			if len(v.LogIdx) == 0 {
				return // Exit early if Log Index is missing
			}
			i = base + len(v.LogIdx) + 3
			missing &^= wantLog // Mark Log Index as successfully parsed
		}
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
	fmt.Println("  data      =", utils.B2s(v.Data))
	fmt.Println("  topics    =", utils.B2s(v.Topics))
	fmt.Println("  block     =", utils.B2s(v.BlkNum))
	fmt.Println("  txIndex   =", utils.B2s(v.TxIndex))
	fmt.Println("  logIndex  =", utils.B2s(v.LogIdx))
}
