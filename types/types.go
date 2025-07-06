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

// ============================================================================
// DESIGN NOTES
// ============================================================================

/*
MEMORY LAYOUT OPTIMIZATION:

1. CACHE LINE ALIGNMENT:
   - Struct aligned to 64-byte boundaries for optimal cache performance
   - Hot fields (Addr, Data, Topics) placed first for sequential access
   - Metadata fields grouped together for batch processing
   - Cold fields (fingerprints) placed last to minimize cache pollution

2. ZERO-COPY DESIGN:
   - All byte slices reference WebSocket buffer directly
   - No memory allocations during log processing
   - Transient lifetime prevents memory leaks
   - Direct slice references enable high-speed parsing

3. FALSE SHARING PREVENTION:
   - 64-byte alignment ensures each LogView occupies dedicated cache lines
   - Explicit padding prevents interference between adjacent structures
   - Future-proof design allows field additions without performance degradation

4. PROCESSING EFFICIENCY:
   - Hot fields accessed first during common operations
   - Metadata fields grouped for batch extraction
   - Fingerprint fields optimized for fast comparison operations
   - Memory layout matches typical access patterns

USAGE PATTERNS:
- Created during WebSocket frame parsing
- Passed through deduplication pipeline
- Used for event matching and filtering
- Destroyed when frame processing completes

PERFORMANCE CHARACTERISTICS:
- Zero allocations per log processed
- Cache-friendly sequential field access
- Minimal memory footprint per instance
- Optimized for high-throughput scenarios
*/
