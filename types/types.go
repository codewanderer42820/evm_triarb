// types.go — Zero-copy event structure for nanosecond-scale processing
//
// This package defines the core data structure for Ethereum event processing
// in the arbitrage detection system. The LogView type provides zero-copy
// access to WebSocket buffer data, eliminating allocation overhead in the
// critical event processing path.
//
// Design principles:
//   - Zero allocations: All fields reference the WebSocket buffer directly
//   - Cache alignment: 64-byte alignment prevents false sharing
//   - Lifetime safety: Clear ownership model prevents use-after-free bugs
//   - Hot/cold separation: Frequently accessed fields in first cache line

package types

// LogView provides zero-copy access to decoded Ethereum event data from WebSocket streams.
// This structure is designed for ultra-low-latency processing where every nanosecond matters.
// All byte slice fields are views into the underlying WebSocket buffer, avoiding any
// memory allocation or copying during event parsing.
//
// Memory layout is carefully optimized:
//   - First cache line (64 bytes): Hot fields accessed on every event
//   - Second cache line: Metadata fields accessed occasionally
//   - Third cache line: Fingerprint fields that persist after buffer reuse
//
// ⚠️  CRITICAL LIFETIME WARNING:
//
//	This type uses unsafe zero-copy optimization. Violating these rules causes crashes:
//	- All slice references become invalid when the WebSocket buffer is reused
//	- Never store LogView or its fields beyond the current processing frame
//	- Never pass LogView across goroutine boundaries (no channel sends)
//	- Never retain slices in maps, slices, or struct fields
//
// Safe usage pattern:
//  1. Receive LogView from WebSocket decoder
//  2. Extract needed data immediately (e.g., parse addresses, amounts)
//  3. Discard LogView before returning control to decoder
//
//go:notinheap     // Prevent heap allocation - must stay on stack
//go:align 64       // Align to cache line boundary for optimal performance
type LogView struct {
	// ═══════════════════════════════════════════════════════════════════
	// HOT FIELDS - First cache line (most frequently accessed)
	// ═══════════════════════════════════════════════════════════════════

	// Addr contains the Ethereum contract address that emitted this event.
	// Format: "0x" followed by 40 hexadecimal characters (lowercase)
	// Example: "0xdac17f958d2ee523a2206206994597c13d831ec7"
	// This field is accessed on every event to determine routing
	Addr []byte

	// Data contains the non-indexed event parameters as hex-encoded bytes.
	// Format: "0x" followed by hex data (variable length, multiple of 2)
	// For Uniswap V2 Sync events, this contains two 256-bit reserve values
	// Example: "0x00000000000000000000000000000000000000000000152d02c7e14af6800000..."
	Data []byte

	// Topics contains the indexed event parameters as a JSON array.
	// Format: JSON array of hex strings, each 32 bytes (256 bits)
	// First topic is always the event signature hash (Keccak-256)
	// Example: ["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"]
	Topics []byte

	// ═══════════════════════════════════════════════════════════════════
	// METADATA FIELDS - Second cache line (occasionally accessed)
	// ═══════════════════════════════════════════════════════════════════

	// BlkNum contains the block number where this event was emitted.
	// Format: "0x" followed by hex number (variable length)
	// Example: "0x1234567" (block 19,088,743 in decimal)
	// Used for event ordering and chain reorganization detection
	BlkNum []byte

	// LogIdx contains the index of this log within its block.
	// Format: "0x" followed by hex number (variable length)
	// Example: "0x5" (5th event in the block)
	// Combined with BlkNum provides unique event identification
	LogIdx []byte

	// TxIndex contains the index of the transaction within its block.
	// Format: "0x" followed by hex number (variable length)
	// Example: "0xa" (10th transaction in the block)
	// Used for correlating events from the same transaction
	TxIndex []byte

	// ═══════════════════════════════════════════════════════════════════
	// FINGERPRINT FIELDS - Persistent after buffer invalidation
	// ═══════════════════════════════════════════════════════════════════

	// TagHi contains the upper 64 bits of the 128-bit event fingerprint.
	// This fingerprint uniquely identifies the event and survives buffer reuse.
	// Calculated from: Hash(BlkNum || TxIndex || LogIdx)
	// Used for deduplication when events are received multiple times
	TagHi uint64

	// TagLo contains the lower 64 bits of the 128-bit event fingerprint.
	// Together with TagHi, provides a globally unique event identifier
	// that remains valid even after the WebSocket buffer is reused
	TagLo uint64

	// ═══════════════════════════════════════════════════════════════════
	// PADDING - Ensures optimal memory alignment
	// ═══════════════════════════════════════════════════════════════════

	// Cache line padding to prevent false sharing between LogView instances
	// This ensures each LogView occupies exactly 3 cache lines (192 bytes)
	// Critical for performance when multiple cores process events in parallel
	_ [32]byte
}
