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
	// log.Printf("Starting skipToQuote: startIdx=%d, hopSize=%d, bufferLength=%d\n", startIdx, hopSize, len(p))

	// First, iterate over the buffer to get close to the quote
	for i = startIdx; i < len(p)-1; i++ {
		if p[i] == ':' {
			// log.Printf("Found ':' at index %d, start hopping\n", i)
			break // We found the colon, let's start hopping
		}
	}

	// Loop over the buffer in hops of hopSize, after we found the ':'
	for ; i < len(p); i += hopSize {
		// log.Printf("At index %d, byte: %v\n", i, utils.B2s(p[i:])) // Debug: Print current index and byte

		// Check if the current character is a quote
		if p[i] == '"' {
			// log.Printf("Found quote at index %d\n", i) // Debug: Found the quote
			return i // Return the index of the quote
		}
	}

	// log.Printf("No quote found, returning -1\n") // Debug: No quote found
	return -1 // Return -1 if no quote is found
}

// skipToBracket finds the first '[' for topics array using hop-based traversal for efficiency
func skipToBracket(p []byte, startIdx int, hopSize int) int {
	i := startIdx
	// log.Printf("Starting skipToBracket: startIdx=%d, hopSize=%d, bufferLength=%d\n", startIdx, hopSize, len(p))

	// First, iterate over the buffer to get close to the bracket
	for i = startIdx; i < len(p); i++ {
		if p[i] == '[' {
			// log.Printf("Found '[' at index %d, start hopping\n", i)
			break // We found the bracket, let's start hopping
		}
	}

	// Loop over the buffer in hops of hopSize, after we found the '['
	for ; i < len(p); i += hopSize {
		// log.Printf("At index %d, byte: %v\n", i, utils.B2s(p[i:])) // Debug: Print current index and byte

		// Check if the current character is a bracket
		if p[i] == '[' {
			// log.Printf("Found bracket at index %d\n", i) // Debug: Found the bracket
			return i // Return the index of the bracket
		}
	}

	// log.Printf("No bracket found, returning -1\n") // Debug: No bracket found
	return -1 // Return -1 if no bracket is found
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
			base := i + 8
			v.Addr = utils.SliceASCII(p, base+utils.FindQuote(p[base:]))
			i = base + len(v.Addr) + 3 // Update index after parsing the Address field
			missing &^= wantAddr       // Mark Address as successfully parsed

		case tag == keyTopics:
			// Parse the Topics field
			base := i + 7
			v.Topics = utils.SliceJSONArray(p, base+utils.FindBracket(p[base:]))
			// Early exit if the Sync() signature doesn't match
			if len(v.Topics) < 11 || *(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != sigSyncPrefix {
				return // Exit early if it doesn't match Sync()
			}
			i = base + len(v.Topics) + 2
			missing &^= wantTopics // Mark Topics as successfully parsed

		case tag == keyData:
			// Parse the Data field
			base := i + 7
			v.Data = utils.SliceASCII(p, base)
			i = base + len(v.Data) + 1
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
