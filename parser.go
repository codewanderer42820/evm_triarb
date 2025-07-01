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
	"bytes"
	"fmt"
	"main/types"
	"main/utils"
	"unsafe"
)

var (
	// Full literal match to disambiguate transactionIndex prefix
	litTxIdx = []byte(`"transactionIndex"`)

	deduper   Deduper
	latestBlk uint32
)

// handleFrame processes a raw WebSocket frame containing a single log.
// If the frame contains a valid UniswapV2 Sync() event, it is deduped and printed.
//
//go:nosplit
//go:inline
//go:registerparams
func handleFrame(p []byte) {
	var v types.LogView

	// Bitmask to track needed fields
	const (
		wantAddr = 1 << iota
		wantTopics
		wantData
		wantBlk
		wantTx
		wantLog
	)
	missing := wantAddr | wantTopics | wantData | wantBlk | wantTx | wantLog

	// 8-byte aligned scan across frame buffer
	for i := 0; i <= len(p)-8 && missing != 0; i++ {
		tag := *(*[8]byte)(unsafe.Pointer(&p[i]))

		switch tag {
		case keyAddress:
			if missing&wantAddr != 0 {
				v.Addr = utils.SliceASCII(p, i+8+utils.FindQuote(p[i+8:]))
				missing &^= wantAddr
			}
		case keyTopics:
			if missing&wantTopics != 0 {
				v.Topics = utils.SliceJSONArray(p, i+8+utils.FindBracket(p[i+8:]))
				if len(v.Topics) < 11 || *(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != sigSyncPrefix {
					return // early exit: not Sync()
				}
				missing &^= wantTopics
			}
		case keyData:
			if missing&wantData != 0 {
				v.Data = utils.SliceASCII(p, i+7)
				missing &^= wantData
			}
		case keyBlockNumber:
			if missing&wantBlk != 0 {
				v.BlkNum = utils.SliceASCII(p, i+8+utils.FindQuote(p[i+8:]))
				missing &^= wantBlk
			}
		case keyTransactionIndex:
			if missing&wantTx != 0 &&
				len(p)-i >= 18 && // 👈 bounds guard
				bytes.Equal(p[i:i+18], litTxIdx) {
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

	// Drop incomplete payloads
	if len(v.BlkNum) == 0 || len(v.TxIndex) == 0 || len(v.LogIdx) == 0 {
		return
	}

	// ───── Derive fingerprint ─────
	switch {
	case len(v.Topics) >= 16:
		v.TagHi, v.TagLo = utils.Load128(v.Topics)
	case len(v.Topics) >= 8:
		v.TagLo = utils.Load64(v.Topics)
	case len(v.Data) >= 8:
		v.TagLo = utils.Load64(v.Data)
	default:
		return // no fingerprint available
	}

	// ───── Parse numeric fields ─────
	blk32 := uint32(utils.ParseHexU64(v.BlkNum))
	tx32 := utils.ParseHexU32(v.TxIndex)
	log32 := utils.ParseHexU32(v.LogIdx)

	if blk32 > latestBlk {
		latestBlk = blk32
	}

	// ───── Dedupe + Emit ─────
	if deduper.Check(blk32, tx32, log32, v.TagHi, v.TagLo, latestBlk) {
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
