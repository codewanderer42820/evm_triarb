package constants

// ============================================================================
// GLOBAL SYSTEM CONSTANTS
// ============================================================================

// ───────────────────────────── Deduplication ──────────────────────────────

const (
	// RingBits defines the deduplication cache size: 2^18 = 262,144 slots ≈ 8 MiB
	// Optimized for L3 cache efficiency while handling 250K+ logs safely
	RingBits = 18

	// MaxReorg defines maximum reorganization depth before entry eviction
	// 128 blocks ≈ 3 minutes at 1.4s block time, sufficient for real-world reorgs
	MaxReorg = 128
)

// ─────────────────────────── Memory Guardrails ─────────────────────────────

const (
	// HeapSoftLimit triggers non-blocking GC when exceeded
	// 128 MiB reduces GC pressure during sustained high-throughput operation
	HeapSoftLimit = 128 << 20 // 128 MiB

	// HeapHardLimit triggers panic if exceeded
	// 512 MiB prevents system-wide memory pressure and enables faster leak detection
	HeapHardLimit = 512 << 20 // 512 MiB
)

// ───────────────────────── WebSocket Configuration ─────────────────────────

const (
	// WebSocket endpoint configuration for Ethereum-compatible provider
	WsDialAddr = "polygon-mainnet.infura.io:443"
	WsPath     = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"
	WsHost     = "polygon-mainnet.infura.io"
)

// ──────────────────────── WebSocket Framing Limits ──────────────────────────

const (
	// MaxFrameSize sets maximum WebSocket frame payload size
	// 512 KiB sufficient for largest realistic log payloads with better cache efficiency
	MaxFrameSize = 512 << 10 // 512 KiB

	// FrameCap defines retained frame capacity for parsing
	// 131,072 frames sufficient for high burst scenarios with better cache locality
	FrameCap = 1 << 17 // 131,072 frames
)

// ────────────────────── JSON Parsing Field Probes ────────────────────────

var (
	// 8-byte probes for efficient JSON field detection
	// Used for zero-allocation parsing in hot paths
	KeyAddress        = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'}
	KeyBlockHash      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'}
	KeyBlockNumber    = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'}
	KeyBlockTimestamp = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'T', 'i'}
	KeyData           = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'}
	KeyLogIndex       = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'}
	KeyRemoved        = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'}
	KeyTopics         = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'}
	KeyTransaction    = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'}

	// Event signature for Uniswap V2 Sync() events
	SigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'}
)
