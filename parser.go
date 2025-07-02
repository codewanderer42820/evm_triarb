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

// handleFrame processes a raw WebSocket frame containing a single log.
// If the frame contains a valid UniswapV2 Sync() event, it is deduplicated and printed.
//
// The function scans for critical fields in the payload and updates the LogView accordingly.
// If the frame matches a valid event (like Sync), it derives a fingerprint and processes it.
//
//go:inline
//go:registerparams
func handleFrame(p []byte) {
	// Early exit if payload is too short
	if len(p) < 117 {
		return
	}

	// Skip the initial 117 bytes as per protocol
	p = p[117:]

	var v types.LogView

	// Define masks for required fields to be extracted from the frame
	const (
		wantAddr = 1 << iota
		wantTopics
		wantData
		wantBlk
		wantTx
		wantLog
	)

	// Start with all fields required
	missing := wantAddr | wantTopics | wantData | wantBlk | wantTx | wantLog
	end := len(p) - 8 // Offset where we stop parsing

	// Loop through the byte slice to extract key data
	for i := 0; i <= end && missing != 0; i++ {
		// Extract 8-byte tag for comparison
		tag := *(*[8]byte)(unsafe.Pointer(&p[i]))

		// Handle each possible tag based on predefined keys (e.g., Address, Topics)
		switch {
		case tag == keyAddress:
			// Parse the Address field
			base := i + 8
			v.Addr = utils.SliceASCII(p, base+utils.FindQuote(p[base:]))
			i = base + len(v.Addr) + 3 // Update index after parsing Address
			missing &^= wantAddr       // Mark Address as extracted

		case tag == keyTopics:
			// Parse the Topics field
			base := i + 7
			v.Topics = utils.SliceJSONArray(p, base+utils.FindBracket(p[base:]))
			// Early exit if Sync() signature doesn't match
			if len(v.Topics) < 11 || *(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != sigSyncPrefix {
				return // Exit early as it doesn't match Sync()
			}
			i = base + len(v.Topics) + 2
			missing &^= wantTopics // Mark Topics as extracted

		case tag == keyData:
			// Parse the Data field
			base := i + 7
			v.Data = utils.SliceASCII(p, base)
			i = base + len(v.Data) + 1
			missing &^= wantData // Mark Data as extracted

		case tag == keyBlockNumber:
			// Parse Block Number field
			base := i + 12
			v.BlkNum = utils.SliceASCII(p, base+utils.FindQuote(p[base:]))
			if len(v.BlkNum) == 0 {
				return // If Block Number is missing, exit early
			}
			i = base + len(v.BlkNum) + 3
			missing &^= wantBlk // Mark Block Number as extracted

		case tag == keyTransactionIndex:
			if len(p)-i >= 85 {
				i += 85
				continue
			}
			base := i + 17
			v.TxIndex = utils.SliceASCII(p, base+utils.FindQuote(p[base:]))
			if len(v.TxIndex) == 0 {
				return // If TxIndex is missing, exit early
			}
			missing &^= wantTx // Mark Transaction Index as extracted

		case tag == keyLogIndex:
			// Parse Log Index field
			base := i + 9
			v.LogIdx = utils.SliceASCII(p, base+utils.FindQuote(p[base:]))
			if len(v.LogIdx) == 0 {
				return // If Log Index is missing, exit early
			}
			i = base + len(v.LogIdx) + 3
			missing &^= wantLog // Mark Log Index as extracted
		}
	}

	// ───── Derive fingerprint ─────
	switch {
	case len(v.Topics) >= 16:
		// If Topics have more than 16 entries, load 128-bit fingerprint
		v.TagHi, v.TagLo = utils.Load128(v.Topics)
	case len(v.Topics) >= 8:
		// If Topics have at least 8 entries, load 64-bit fingerprint
		v.TagLo = utils.Load64(v.Topics)
	case len(v.Data) >= 8:
		// If Data has at least 8 bytes, load 64-bit fingerprint from Data
		v.TagLo = utils.Load64(v.Data)
	default:
		return // No fingerprint available if no valid data found
	}

	// ───── Parse numeric fields ─────
	blk32 := uint32(utils.ParseHexU64(v.BlkNum))
	tx32 := utils.ParseHexU32(v.TxIndex)
	log32 := utils.ParseHexU32(v.LogIdx)

	// Update the latest block number
	if blk32 > latestBlk {
		latestBlk = blk32
	}

	// ───── Dedupe + Emit ─────
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
