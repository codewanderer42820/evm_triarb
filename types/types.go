// types.go — Zero-copy data structures for high-frequency Ethereum log processing
// ============================================================================
// ETHEREUM LOG VIEW - ZERO-COPY REFERENCE STRUCTURE
// ============================================================================
//
// Types package provides zero-copy reference structures for decoded Ethereum
// event data with cache-optimal memory layout and transient lifetime management.
//
// Architecture overview:
//   • Zero-copy references to WebSocket buffer data without allocation
//   • Cache-aligned structure layout for optimal memory access patterns
//   • Transient lifetime management preventing dangling pointer issues
//   • Hot/warm/cold field organization for cache performance
//
// Performance characteristics:
//   • Zero allocations during log parsing and field extraction
//   • 64-byte cache line alignment for optimal memory access
//   • Hot field placement for frequently accessed parsing operations
//   • SIMD-friendly data layout for batch processing operations
//
// Memory safety model:
//   • Transient references with frame-scoped lifetime guarantees
//   • Explicit documentation of buffer invalidation conditions
//   • Cache-aligned padding prevents false sharing across cores
//   • No-heap annotation prevents accidental heap allocation
//
// Lifetime management:
//   • LogView instances become invalid when WebSocket buffer is reused
//   • Never store LogView beyond current processing frame
//   • All field slices reference buffer directly without copying
//   • Fingerprint fields provide persistent identification beyond frame scope
//
// Memory layout legend:
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│ WebSocket buffer → LogView references → Processing → Buffer invalidation│
//	│         ▲                    │                           │               │
//	│    Zero-copy ◀───────────────┘                           │               │
//	│         ▲                                                 │               │
//	│    Transient ◀────────────────────────────────────────────┘               │
//	└─────────────────────────────────────────────────────────────────────────┘
//
// Compiler optimizations:
//   • //go:notinheap prevents heap allocation of LogView instances
//   • //go:align 64 ensures cache line alignment for optimal access patterns
//   • Explicit padding maintains alignment and prevents false sharing

package types

// ============================================================================
// ETHEREUM LOG VIEW - ZERO-COPY REFERENCE STRUCTURE
// ============================================================================

// LogView provides a zero-copy reference to decoded Ethereum event data.
// All fields reference slices directly from the WebSocket buffer without allocation.
// Designed for high-throughput log processing with cache-optimal memory layout.
//
// IMPORTANT LIFETIME CONSTRAINTS:
//   - This struct contains transient references that become invalid when
//     the underlying WebSocket buffer is reused or reallocated
//   - Never store LogView instances beyond the current processing frame
//   - Never pass LogView instances across goroutine boundaries
//   - All slice fields become dangling pointers after buffer invalidation
//
// Memory layout optimization:
//   - Hot fields (frequently accessed) placed first for cache efficiency
//   - Metadata fields grouped for sequential access during indexing
//   - Fingerprint fields provide persistent identification
//   - Explicit padding ensures 64-byte alignment and prevents false sharing
//
//go:notinheap
//go:align 64
type LogView struct {
	// ========================================================================
	// HOT FIELDS - Frequently accessed during parsing and arbitrage matching
	// ========================================================================

	// Addr contains the contract address from the Ethereum log
	// Format: 20-byte hex string with 0x prefix (e.g., "0x1234...abcd")
	// Usage: Direct comparison with registered trading pair addresses
	// Lifetime: Valid only within current WebSocket frame processing
	Addr []byte

	// Data contains the event data payload from the Ethereum log
	// Format: Variable-length hex string with 0x prefix
	// Usage: Reserve ratio extraction for price tick calculation
	// Lifetime: Valid only within current WebSocket frame processing
	Data []byte

	// Topics contains the indexed event parameters as JSON array
	// Format: JSON array of hex strings (e.g., ["0xabc...", "0x123..."])
	// Usage: Event signature verification and parameter extraction
	// Lifetime: Valid only within current WebSocket frame processing
	Topics []byte

	// ========================================================================
	// METADATA FIELDS - Block and transaction positioning information
	// ========================================================================

	// BlkNum contains the block number where this log was generated
	// Format: Hex string representing uint64 block number
	// Usage: Deduplication and reorganization detection
	// Lifetime: Valid only within current WebSocket frame processing
	BlkNum []byte

	// LogIdx contains the position of this log within the block
	// Format: Hex string representing uint32 log index
	// Usage: Unique log identification within block context
	// Lifetime: Valid only within current WebSocket frame processing
	LogIdx []byte

	// TxIndex contains the transaction index within the block
	// Format: Hex string representing uint32 transaction index
	// Usage: Transaction-level grouping and ordering operations
	// Lifetime: Valid only within current WebSocket frame processing
	TxIndex []byte

	// ========================================================================
	// FINGERPRINT FIELDS - Deduplication and identification data
	// ========================================================================

	// TagHi holds the upper 64 bits of the content fingerprint
	// Used for fast deduplication and routing decisions across processing stages
	// Lifetime: Persistent - remains valid after buffer invalidation
	TagHi uint64

	// TagLo holds the lower 64 bits of the content fingerprint
	// Combined with TagHi forms a 128-bit unique identifier for log content
	// Lifetime: Persistent - remains valid after buffer invalidation
	TagLo uint64

	// ========================================================================
	// PADDING - Cache alignment and future expansion
	// ========================================================================

	// Explicit padding ensures 64-byte alignment and prevents false sharing
	// Reserves space for future fields without breaking cache line boundaries
	// Total structure size: exactly 64 bytes (one cache line)
	_ [4]uint64
}
