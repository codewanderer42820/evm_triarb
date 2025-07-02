// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: types.go — ISR-safe view struct for decoded Ethereum logs
//
// Purpose:
//   - Holds zero-copy references to fields extracted from WebSocket JSON frames
//   - Serves as the transient intermediate for parsing → deduplication → emission
//
// Notes:
//   - Never escapes the scope of `handleFrame()` — all slices point into wsBuf
//   - Aligned and padded for cache efficiency, ABA safety, and prefetch locality
//   - 64-byte aligned for clean per-slot allocation in any struct ring or queue
//
// Compiler Directives:
//   - //go:notinheap
//   - //go:align 64
//
// ⚠️ Never store or reference LogView outside current tick — contents become invalid
// ─────────────────────────────────────────────────────────────────────────────

package types

// LogView provides a flat, pointer-stable reference to a decoded Ethereum event.
// All fields are directly sliced from wsBuf — no allocations occur.
//
//go:notinheap
//go:align 64
type LogView struct {
	// ───────────── Hot fields: parsed early and matched for Sync() ─────────────

	Addr   []byte // "address" field (20B hex, 0x-prefixed)
	Data   []byte // "data" field (0x-prefixed hex string)
	Topics []byte // "topics" field (JSON array — slice of `["..."]`)

	// ───────────── Metadata fields: passed to deduper for identity ─────────────

	BlkNum  []byte // "blockNumber" — parsed via ParseHexU64
	LogIdx  []byte // "logIndex" — parsed via ParseHexU32
	TxIndex []byte // "transactionIndex" — parsed via ParseHexU32

	// ───────────── Cold fingerprinting fields for deduplication ────────────────

	TagHi uint64 // upper 64 bits of tag (topics[0])
	TagLo uint64 // lower 64 bits (fallback to data)

	_ [4]uint64 // explicit 32B padding (future proof, avoids false sharing)
}
