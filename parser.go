// parser.go — zero-alloc JSON scanner that feeds deduper & printer.
package main

import (
	"bytes"
	"fmt"
	"main/types"
	"main/utils"
	"unsafe"
)

var (
	litTxIdx  = []byte(`"transactionIndex"`)
	deduper   Deduper
	latestBlk uint32
)

// handleFrame scans a full WebSocket payload (result JSON) once, in 8-byte
// aligned strides, extracting only the six fields we care about.
func handleFrame(p []byte) {
	var v types.LogView

	const (
		wantAddr = 1 << iota
		wantData
		wantTopics
		wantBlk
		wantTx
		wantLog
	)
	missing := wantAddr | wantData | wantTopics | wantBlk | wantTx | wantLog

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
				// quick Uniswap-V2 Sync filter
				if len(v.Topics) < 11 ||
					*(*[8]byte)(unsafe.Pointer(&v.Topics[3])) != sigSyncPrefix {
					return // not a Sync event → drop early
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

	// all three numeric fields must be present – otherwise discard
	if len(v.BlkNum) == 0 || len(v.TxIndex) == 0 || len(v.LogIdx) == 0 {
		return
	}

	// fast 128-bit fingerprint for dedup
	switch {
	case len(v.Topics) >= 16:
		v.TagHi, v.TagLo = utils.Load128(v.Topics)
	case len(v.Topics) >= 8:
		v.TagLo = utils.Load64(v.Topics)
	case len(v.Data) >= 8:
		v.TagLo = utils.Load64(v.Data)
	default:
		return // insufficient entropy
	}

	blk32 := uint32(utils.ParseHexU64(v.BlkNum))
	tx32 := utils.ParseHexU32(v.TxIndex)
	log32 := utils.ParseHexU32(v.LogIdx)

	if blk32 > latestBlk {
		latestBlk = blk32
	}

	if deduper.Check(blk32, tx32, log32, v.TagHi, v.TagLo, latestBlk) {
		emitLog(&v)
	}
}

// emitLog prints one fully-deduped event in human-readable form.
func emitLog(v *types.LogView) {
	fmt.Println("[EVENT]")
	fmt.Println("  address   =", utils.B2s(v.Addr))
	fmt.Println("  data      =", utils.B2s(v.Data))
	fmt.Println("  topics    =", utils.B2s(v.Topics))
	fmt.Println("  block     =", utils.B2s(v.BlkNum))
	fmt.Println("  txIndex   =", utils.B2s(v.TxIndex))
	fmt.Println("  logIndex  =", utils.B2s(v.LogIdx))
}
