// parser.go — zero-alloc JSON scanner that feeds deduper & emitter.
// Performs 8-byte aligned scanning with fixed probes and branch-minimized logic.

package main

import (
	"bytes"
	"fmt"
	"main/types"
	"main/utils"
	"unsafe"
)

// literal match for full "transactionIndex" string (18 bytes).
// Needed since tag prefix is non-unique and must be disambiguated by full match.
var litTxIdx = []byte(`"transactionIndex"`)

// deduper is the global instance of Deduper used for replay prevention.
// latestBlk tracks the latest seen block height for eviction control.
var (
	deduper   Deduper
	latestBlk uint32
)

// handleFrame scans a raw WebSocket payload for a single log entry.
// It extracts exactly six fields via aligned 8-byte probes and zero-copy slicing.
// Early exit on missing fields, malformed tags, or non-Sync events.
//
// Performance:
//   - Branch-free 8-byte window scan using constants from constants.go
//   - Uses zero-allocation slice windows into wsBuf
//   - Topics[0] hash is fingerprinted for dedupe
//
// Compiler Directives:
//   - nosplit         → ensures frame scan is stack-safe and fast
//   - registerparams  → ABI optimized for performance
//
//go:nosplit
//go:registerparams
func handleFrame(p []byte) {
	var v types.LogView

	// Bitmask to track missing fields
	const (
		wantAddr   = 1 << iota // "address"
		wantData               // "data"
		wantTopics             // "topics"
		wantBlk                // "blockNumber"
		wantTx                 // "transactionIndex"
		wantLog                // "logIndex"
	)
	missing := wantAddr | wantData | wantTopics | wantBlk | wantTx | wantLog

	// Slide across the payload using 8-byte window scans
	for i := 0; i <= len(p)-8 && missing != 0; i++ {
		tag := *(*[8]byte)(unsafe.Pointer(&p[i]))

		switch tag {
		case keyAddress:
			if missing&wantAddr != 0 {
				v.Addr = utils.SliceASCII(p, i+8+utils.FindQuote(p[i+8:]))
				missing &^= wantAddr
			}
		case keyData:
			if missing&wantData != 0 {
				v.Data = utils.SliceASCII(p, i+7)
				missing &^= wantData
			}
		case keyTopics:
			if missing&wantTopics != 0 {
				v.Topics = utils.SliceJSONArray(p, i+8+utils.FindBracket(p[i+8:]))

				// Fast path filter: early drop if not UniswapV2 Sync()
				if len(v.Topics) < 11 || *(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != sigSyncPrefix {
					return
				}
				missing &^= wantTopics
			}
		case keyBlockNumber:
			if missing&wantBlk != 0 {
				v.BlkNum = utils.SliceASCII(p, i+8+utils.FindQuote(p[i+8:]))
				missing &^= wantBlk
			}
		case keyTransactionIndex:
			// Match full 18-byte literal, not just prefix
			if missing&wantTx != 0 && bytes.Equal(p[i:i+18], litTxIdx) {
				v.TxIndex = utils.SliceASCII(p, i+18+utils.FindQuote(p[i+18:]))
				missing &^= wantTx
			}
		case keyLogIndex:
			if missing&wantLog != 0 {
				v.LogIdx = utils.SliceASCII(p, i+8+utils.FindQuote(p[i+8:]))
				missing &^= wantLog
			}
		}
	}

	// Drop if any numeric metadata field is missing
	if len(v.BlkNum) == 0 || len(v.TxIndex) == 0 || len(v.LogIdx) == 0 {
		return
	}

	// ───── Fingerprint logic for deduplication ─────

	// Entropy source: topics preferred > data fallback
	switch {
	case len(v.Topics) >= 16:
		v.TagHi, v.TagLo = utils.Load128(v.Topics)
	case len(v.Topics) >= 8:
		v.TagLo = utils.Load64(v.Topics)
	case len(v.Data) >= 8:
		v.TagLo = utils.Load64(v.Data)
	default:
		return // No stable fingerprint available
	}

	// Parse block/tx/log into numeric uint32s for dedupe key
	blk32 := uint32(utils.ParseHexU64(v.BlkNum))
	tx32 := utils.ParseHexU32(v.TxIndex)
	log32 := utils.ParseHexU32(v.LogIdx)

	// Update latest block for age tracking and reorg defense
	if blk32 > latestBlk {
		latestBlk = blk32
	}

	// Deduplication: emit only if unseen under current tip
	if deduper.Check(blk32, tx32, log32, v.TagHi, v.TagLo, latestBlk) {
		emitLog(&v)
	}
}

// emitLog prints a fully deduplicated event to stdout.
// Converts internal []byte to string using zero-copy B2s.
//
// Hot path safe only because it's rare — NOT called on every frame.
//
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
