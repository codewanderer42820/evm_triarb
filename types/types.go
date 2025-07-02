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
// ⚠️ Never store or reference LogView outside current tick — contents become invalid
// ─────────────────────────────────────────────────────────────────────────────

package types

// LogView provides a flat, pointer-stable reference to a decoded Ethereum event.
// All fields are directly sliced from wsBuf — no allocations occur.
//
// The LogView struct is designed to be efficient for real-time log parsing and processing.
// Each field is extracted directly from the WebSocket buffer, and it is kept immutable
// and transient within the context of a single processing tick.
//
//go:notinheap
//go:align 64
type LogView struct {
	// ───────────── Hot fields: parsed early and matched for Sync() ─────────────

	// Addr holds the "address" field from the Ethereum log.
	// It is a 20-byte hex string (0x-prefixed).
	Addr []byte

	// Data holds the "data" field from the Ethereum log.
	// It is a hex string, typically 0x-prefixed.
	Data []byte

	// Topics holds the "topics" field from the Ethereum log.
	// This is a JSON array, represented as a slice of strings `["..."]`.
	Topics []byte

	// ───────────── Metadata fields: passed to deduper for identity ─────────────

	// BlkNum holds the "blockNumber" field, parsed as a uint64.
	// It is used to identify the block in which the log was generated.
	BlkNum []byte

	// LogIdx holds the "logIndex" field, parsed as a uint32.
	// This helps identify the position of the log within the block.
	LogIdx []byte

	// TxIndex holds the "transactionIndex" field, parsed as a uint32.
	// This identifies the index of the transaction within the block.
	TxIndex []byte

	// ───────────── Cold fingerprinting fields for deduplication ────────────────

	// TagHi holds the upper 64 bits of the fingerprint derived from the first topic.
	TagHi uint64

	// TagLo holds the lower 64 bits of the fingerprint, derived from the data if needed.
	TagLo uint64

	// _ is used for padding to ensure proper alignment and prevent false sharing.
	_ [4]uint64 // Explicit 32-byte padding for future-proofing and cache-line alignment
}
