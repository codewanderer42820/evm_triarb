package types

// LogView - Zero-copy reference to decoded Ethereum event data.
// All fields reference WebSocket buffer directly without allocation.
//
// ⚠️  LIFETIME WARNING:
//   - References become invalid when buffer is reused
//   - Never store beyond current processing frame
//   - Never pass across goroutine boundaries
//
//go:notinheap
//go:align 64
type LogView struct {
	// Hot fields - frequently accessed
	Addr   []byte // Contract address "0x..."
	Data   []byte // Event data payload
	Topics []byte // Indexed parameters JSON array

	// Metadata fields
	BlkNum  []byte // Block number (hex)
	LogIdx  []byte // Log index (hex)
	TxIndex []byte // Transaction index (hex)

	// Fingerprint fields - persist after buffer invalidation
	TagHi uint64 // Upper 64 bits of fingerprint
	TagLo uint64 // Lower 64 bits of fingerprint

	// Cache line padding
	_ [32]byte
}
