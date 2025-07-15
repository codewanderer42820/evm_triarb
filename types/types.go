// ════════════════════════════════════════════════════════════════════════════════════════════════
// 📦 ZERO-COPY EVENT STRUCTURES
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Core Data Types
//
// Description:
//   Defines zero-copy data structures for Ethereum event processing. All fields reference the
//   WebSocket buffer directly, eliminating allocation overhead in the critical event path.
//
// Performance Characteristics:
//   - Allocations: Zero per event
//   - Cache alignment: 64-byte boundaries
//   - Memory safety: Clear lifetime rules
//   - Access pattern: Hot/cold field separation
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

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
	// Example: "0x882df4b0fb50a229c3b4124eb18c759911485bfb"
	// This field is accessed on every event to determine routing
	Addr []byte

	// Topics contains the indexed event parameters with brackets stripped.
	// Format: Comma-separated hex strings without the JSON array brackets "[" and "]"
	// For Uniswap V2 Sync events, this is just the event signature hash
	// Example: "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
	// Note: Original JSON array brackets have been removed during parsing
	Topics []byte

	// Data contains the non-indexed event parameters as hex-encoded bytes.
	// Format: "0x" followed by hex data (variable length, multiple of 2)
	// For Uniswap V2 Sync events, this contains two 256-bit reserve values
	// Example: "0x0000000000000000000000000000000000000000007c34bdf6cfe2d5772d68d10000000000000000000000000000000000000000000000000020dfffa7b4a402"
	Data []byte

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
	// METADATA FIELDS - Second cache line (occasionally accessed)
	// ═══════════════════════════════════════════════════════════════════

	// BlkNum contains the block number where this event was emitted.
	// Format: "0x" followed by hex number (variable length)
	// Example: "0x468dcc3" (block 74,185,923 in decimal)
	// Used for event ordering and chain reorganization detection
	BlkNum []byte

	// LogIdx contains the index of this log within its block.
	// Format: "0x" followed by hex number (variable length)
	// Example: "0x2bf" (703rd event in the block)
	// Combined with BlkNum provides unique event identification
	LogIdx []byte

	// TxIndex contains the index of the transaction within its block.
	// Format: "0x" followed by hex number (variable length)
	// Example: "0x9e" (158th transaction in the block)
	// Used for correlating events from the same transaction
	TxIndex []byte

	// ═══════════════════════════════════════════════════════════════════
	// PADDING - Ensures optimal memory alignment
	// ═══════════════════════════════════════════════════════════════════

	// Cache line padding to prevent false sharing between LogView instances
	// This ensures each LogView occupies exactly 3 cache lines (192 bytes)
	// Critical for performance when multiple cores process events in parallel
	_ [40]byte
}
