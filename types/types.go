package types

// logView is a zero-alloc window into a single JSON log message.
// Slices point directly into wsBuf → NEVER let them escape to the heap.
type LogView struct {
	// Hot-path slices (always accessed)
	Addr    []byte // "address"   – 20 B ASCII hex (no 0x)
	Data    []byte // "data"      – calldata hex (0x-prefixed)
	Topics  []byte // "topics"    – JSON array of 0x… strings
	BlkNum  []byte // "blockNumber"
	TxIndex []byte // "transactionIndex"
	LogIdx  []byte // "logIndex"

	// Cold fields (accessed only on dedup hit)
	TagHi uint64 // high 64 bits of entropy (topic0 or hash(data))
	TagLo uint64 // low  64 bits
}
