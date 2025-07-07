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

// ============================================================================
// ARBITRAGE ROUTER CONSTANTS
// ============================================================================

// ────────────────────── Tick Quantization Parameters ──────────────────────

const (
	// TickClampingBound defines the domain constraints for tick values
	// Domain: [-128, +128] for numerical stability in arbitrage calculations
	TickClampingBound = 128.0

	// MaxQuantizedTick defines the 18-bit ceiling for quantized tick values
	// 2^18 - 1 = 262,143 provides sufficient precision for priority queues
	MaxQuantizedTick = 262_143

	// QuantizationScale maps floating-point tick domain to integer priority range
	// Calculated as (MaxQuantizedTick - 1) / (2 * TickClampingBound) ≈ 1023.99
	QuantizationScale = (MaxQuantizedTick - 1) / (2 * TickClampingBound)
)

// ─────────────────── Ethereum Address Parsing Boundaries ───────────────────

const (
	// AddressHexStart defines the starting index of Ethereum address hex in LogView.Addr
	// Format: "0x" + 40 hex characters, so address starts at index 2 (but we use 3 for implementation)
	AddressHexStart = 3

	// AddressHexEnd defines the ending index (exclusive) of Ethereum address hex
	// 40-byte hex representation ends at index 43
	AddressHexEnd = 43
)

// ────────────────────── Hash Table Configuration ──────────────────────────

const (
	// AddressTableCapacity defines the size of the Robin Hood hash table
	// 1M entries (2^20) provides excellent performance for production workloads
	AddressTableCapacity = 1 << 20

	// AddressTableMask provides efficient modulo operation for hash table indexing
	// Calculated as AddressTableCapacity - 1 for power-of-2 optimization
	AddressTableMask = AddressTableCapacity - 1
)

// ──────────────────── Core System Limits and Configuration ─────────────────

const (
	// MaxSupportedCores defines the maximum number of CPU cores supported
	// 64 cores sufficient for current high-end server hardware
	MaxSupportedCores = 64

	// DefaultRingSize defines the default capacity for inter-core ring buffers
	// 65,536 entries (2^16) provides excellent throughput with manageable memory usage
	DefaultRingSize = 1 << 16

	// DefaultLocalIdxSize defines the default capacity for local indexing structures
	// 65,536 entries (2^16) matches ring buffer size for optimal cache behavior
	DefaultLocalIdxSize = 1 << 16

	// MaxCyclesPerShard defines the maximum arbitrage cycles per shard bucket
	// 65,536 cycles (2^16) provides cache-friendly grouping for load balancing
	MaxCyclesPerShard = 1 << 16
)

// ──────────────────────── Initialization Parameters ────────────────────────

const (
	// MaxInitializationPriority defines the lowest priority value for cycle initialization
	// Uses MaxQuantizedTick (262,143) to ensure cycles start with minimum priority
	MaxInitializationPriority = MaxQuantizedTick

	// ShardChannelBufferSize defines the buffer depth for shard distribution channels
	// 1,024 entries (2^10) provides sufficient buffering for parallel initialization
	ShardChannelBufferSize = 1 << 10
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
