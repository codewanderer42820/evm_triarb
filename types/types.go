package types

// LogView provides a zero-allocation, pointer-stable view into a single Ethereum event log.
// All slices point directly into the global `wsBuf`, avoiding heap allocations entirely.
//
// This struct is the sole intermediate between frame decoding and deduplication:
//   - Written by `handleFrame()`
//   - Read by `deduper.Check()` and `emitLog()`
//
// ⚠️ Never allow LogView or its fields to escape beyond current frame lifecycle.
// Doing so would risk use-after-free if `wsBuf` is rotated.
//
// Memory layout is cache-optimized:
//   - Hot path fields come first: address, data, topics
//   - Numeric metadata fields follow (block/tx/log)
//   - Cold dedup-only fingerprint fields are last
//
//go:notinheap
//go:align 128
type LogView struct {
	// ───────────── Hot fields: accessed immediately in fast path ──────────────

	Addr   []byte // `"address"` field — 20-byte hex, 0x-prefixed (e.g. "0xabc...")
	Data   []byte // `"data"` field — calldata, 0x-prefixed hex string
	Topics []byte // `"topics"` field — JSON array of strings (slice of bytes inside [ ])

	// ───────────── Metadata fields: parsed and passed to deduper ──────────────

	BlkNum  []byte // `"blockNumber"` field — 0x-prefixed hex (parsed to uint32)
	TxIndex []byte // `"transactionIndex"` — 0x-prefixed hex
	LogIdx  []byte // `"logIndex"` — 0x-prefixed hex

	// ───────────── Cold fields: used only by deduper fingerprinting ───────────

	TagHi uint64 // upper 64 bits of hash fingerprint (topics[0] or data hash)
	TagLo uint64 // lower 64 bits of fingerprint (fallback to topic[0] or data)

	// NOTE: No padding required — memory alignment is already optimal.
	// The layout packs all []byte (2 words each) followed by 2× uint64.
	//
	// Final size: 6×(2×8) + 2×8 = 112 bytes total (fits in 2 L1 cache lines)
}
