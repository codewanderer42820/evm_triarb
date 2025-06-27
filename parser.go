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

var (
	// literal match for full "transactionIndex" string (18 bytes)
	litTxIdx = []byte(`"transactionIndex"`)

	deduper   Deduper // global deduplication engine
	latestBlk uint32  // tracks latest seen block for eviction logic
)

// handleFrame scans a raw WebSocket payload for a single log entry.
// It extracts exactly six fields via aligned 8-byte tags and zero-copy slicing.
// Early exit occurs on missing fields, malformed data, or unmatched Sync events.
//
//go:nosplit
func handleFrame(p []byte) {
	var v types.LogView

	// Define fields we want to extract (bitmap).
	const (
		wantAddr = 1 << iota
		wantData
		wantTopics
		wantBlk
		wantTx
		wantLog
	)
	missing := wantAddr | wantData | wantTopics | wantBlk | wantTx | wantLog

	// Slide window with 8-byte aligned loads into fixed tag matcher.
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

				// Apply fast path Sync-event filter (UniswapV2)
				if len(v.Topics) < 11 ||
					*(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != sigSyncPrefix {
					return // Not a Sync event ⇒ drop
				}
				missing &^= wantTopics
			}
		case keyBlockNumber:
			if missing&wantBlk != 0 {
				v.BlkNum = utils.SliceASCII(p, i+8+utils.FindQuote(p[i+8:]))
				missing &^= wantBlk
			}
		case keyTransactionIndex:
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

	// Bail if any numeric metadata field is still missing
	if len(v.BlkNum) == 0 || len(v.TxIndex) == 0 || len(v.LogIdx) == 0 {
		return
	}

	// ───── Fingerprint construction for deduplication ─────

	switch {
	case len(v.Topics) >= 16:
		v.TagHi, v.TagLo = utils.Load128(v.Topics)
	case len(v.Topics) >= 8:
		v.TagLo = utils.Load64(v.Topics)
	case len(v.Data) >= 8:
		v.TagLo = utils.Load64(v.Data)
	default:
		return // Not enough entropy to safely fingerprint
	}

	// Convert numeric fields (0x hex → uint32)
	blk32 := uint32(utils.ParseHexU64(v.BlkNum))
	tx32 := utils.ParseHexU32(v.TxIndex)
	log32 := utils.ParseHexU32(v.LogIdx)

	// Track latest block for staleness detection
	if blk32 > latestBlk {
		latestBlk = blk32
	}

	// Deduplication: only emit if event is new
	if deduper.Check(blk32, tx32, log32, v.TagHi, v.TagLo, latestBlk) {
		emitLog(&v)
	}
}

// emitLog prints a fully deduplicated event in human-readable form.
// Uses zero-allocation byte → string conversion for display only.
func emitLog(v *types.LogView) {
	fmt.Println("[EVENT]")
	fmt.Println("  address   =", utils.B2s(v.Addr))
	fmt.Println("  data      =", utils.B2s(v.Data))
	fmt.Println("  topics    =", utils.B2s(v.Topics))
	fmt.Println("  block     =", utils.B2s(v.BlkNum))
	fmt.Println("  txIndex   =", utils.B2s(v.TxIndex))
	fmt.Println("  logIndex  =", utils.B2s(v.LogIdx))
}
