package types

// LogView provides a zero-allocation, windowed view into an Ethereum event log
// directly over the WebSocket frame buffer. It avoids any heap allocations by
// keeping all slices pointing inside the global `wsBuf`. Never allow these
// slices to escape or be retained outside the immediate processing path.
//
// This struct is hot in the parsing + deduplication path, so memory layout is
// tuned for read locality on hot fields (addr, data, topics). Fields used only
// in dedup logic are kept cold.
//
//go:nosplit
type LogView struct {
	// ───────────── Hot fields: accessed immediately by fastpath ──────────────

	Addr   []byte // View of the "address" field — 20-byte ASCII hex (no "0x")
	Data   []byte // View of the "data" field — full calldata, 0x-prefixed
	Topics []byte // View of the "topics" field — JSON array of 0x-prefixed strings

	BlkNum  []byte // View of "blockNumber" field — 0x-prefixed, parsed as uint32
	TxIndex []byte // View of "transactionIndex" — 0x-prefixed, parsed as uint32
	LogIdx  []byte // View of "logIndex" — 0x-prefixed, parsed as uint32

	// ───────────── Cold fields: used only during dedup checks ───────────────

	TagHi uint64 // High 64 bits of the dedup fingerprint (topic0 or hash(data))
	TagLo uint64 // Low 64 bits of fingerprint (fallbacks to partial topic/data)

	// NOTE: No additional padding added as alignment is already optimal.
	// This layout keeps all []byte slices together (64-bit ptr + len each),
	// followed by 2 × uint64 fingerprint. Cache footprint: 6×16 + 16 = 112 bytes.
}
