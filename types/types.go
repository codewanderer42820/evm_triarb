package types

// ============================================================================
// ETHEREUM LOG VIEW - ZERO-COPY REFERENCE STRUCTURE
// ============================================================================

// LogView provides a zero-copy reference to decoded Ethereum event data.
// All fields reference slices directly from the WebSocket buffer without allocation.
// Designed for high-throughput log processing with cache-optimal memory layout.
//
// IMPORTANT: This struct contains transient references that become invalid
// when the underlying WebSocket buffer is reused. Never store LogView
// instances beyond the current processing frame.
//
//go:notinheap
//go:align 64
type LogView struct {
	// ========================================================================
	// HOT FIELDS - Frequently accessed during parsing and matching
	// ========================================================================

	// Addr contains the contract address from the Ethereum log
	// Format: 20-byte hex string with 0x prefix (e.g., "0x1234...abcd")
	Addr []byte

	// Data contains the event data payload from the Ethereum log
	// Format: Variable-length hex string with 0x prefix
	Data []byte

	// Topics contains the indexed event parameters as JSON array
	// Format: JSON array of hex strings (e.g., ["0xabc...", "0x123..."])
	Topics []byte

	// ========================================================================
	// METADATA FIELDS - Block and transaction positioning information
	// ========================================================================

	// BlkNum contains the block number where this log was generated
	// Format: Hex string representing uint64 block number
	BlkNum []byte

	// LogIdx contains the position of this log within the block
	// Format: Hex string representing uint32 log index
	LogIdx []byte

	// TxIndex contains the transaction index within the block
	// Format: Hex string representing uint32 transaction index
	TxIndex []byte

	// ========================================================================
	// FINGERPRINT FIELDS - Deduplication and identification data
	// ========================================================================

	// TagHi holds the upper 64 bits of the content fingerprint
	// Used for fast deduplication and routing decisions
	TagHi uint64

	// TagLo holds the lower 64 bits of the content fingerprint
	// Combined with TagHi forms a 128-bit unique identifier
	TagLo uint64

	// ========================================================================
	// PADDING - Cache alignment and future expansion
	// ========================================================================

	// Explicit padding ensures 64-byte alignment and prevents false sharing
	// Reserves space for future fields without breaking cache line boundaries
	_ [4]uint64
}
