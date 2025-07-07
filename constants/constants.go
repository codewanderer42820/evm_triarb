// constants.go — System-wide constants for high-performance arbitrage detection
// ============================================================================
// GLOBAL SYSTEM CONSTANTS
// ============================================================================
//
// Centralized constant definitions for the triangular arbitrage detection system
// with optimal organization by functional domain and usage frequency.
//
// Organization principles:
//   • Core system limits and architecture constraints first
//   • Memory management and performance tuning parameters
//   • Network and communication configuration
//   • Domain-specific arbitrage and parsing constants
//   • Initialization and operational parameters last
//
// Performance considerations:
//   • Power-of-2 values for optimal bit manipulation and modular arithmetic
//   • Cache-line aligned sizes for memory access efficiency
//   • Constants sized for production workload characteristics
//   • Zero-allocation parsing optimization with byte array probes

package constants

// ============================================================================
// CORE SYSTEM ARCHITECTURE LIMITS
// ============================================================================

const (
	// MaxSupportedCores defines the maximum number of CPU cores supported
	// 64 cores accommodates current high-end server hardware configurations
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

// ============================================================================
// MEMORY MANAGEMENT AND PERFORMANCE GUARDRAILS
// ============================================================================

const (
	// HeapSoftLimit triggers non-blocking garbage collection when exceeded
	// 128 MiB reduces GC pressure during sustained high-throughput operation
	HeapSoftLimit = 128 << 20 // 128 MiB

	// HeapHardLimit triggers panic if exceeded to prevent system instability
	// 512 MiB prevents system-wide memory pressure and enables rapid leak detection
	HeapHardLimit = 512 << 20 // 512 MiB

	// RingBits defines the deduplication cache size: 2^18 = 262,144 slots ≈ 8 MiB
	// Optimized for L3 cache efficiency while handling 250K+ logs safely
	RingBits = 18

	// MaxReorg defines maximum blockchain reorganization depth before entry eviction
	// 128 blocks ≈ 3 minutes at 1.4s block time, sufficient for real-world reorganizations
	MaxReorg = 128
)

// ============================================================================
// HASH TABLE AND ADDRESS INDEXING CONFIGURATION
// ============================================================================

const (
	// AddressTableCapacity defines the size of the Robin Hood hash table
	// 1M entries (2^20) provides excellent performance for production workloads
	AddressTableCapacity = 1 << 20

	// AddressTableMask provides efficient modulo operation for hash table indexing
	// Calculated as AddressTableCapacity - 1 for power-of-2 optimization
	AddressTableMask = AddressTableCapacity - 1

	// PairRoutingTableCapacity defines the size of the pair-to-core routing table
	// 1M entries (2^20) matches AddressTableCapacity for consistent pair ID space
	PairRoutingTableCapacity = 1 << 20

	// AddressHexStart defines the starting index of Ethereum address hex in LogView.Addr
	// Format: "0x" + 40 hex characters, address hex begins at index 2 (implementation uses 3)
	AddressHexStart = 3

	// AddressHexEnd defines the ending index (exclusive) of Ethereum address hex
	// 40-character hex representation ends at index 43
	AddressHexEnd = 43
)

// ============================================================================
// ARBITRAGE TICK QUANTIZATION PARAMETERS
// ============================================================================

const (
	// TickClampingBound defines the domain constraints for tick values
	// Domain: [-128, +128] ensures numerical stability in arbitrage calculations
	TickClampingBound = 128.0

	// MaxQuantizedTick defines the 18-bit ceiling for quantized tick values
	// 2^18 - 1 = 262,143 provides sufficient precision for priority queue operations
	MaxQuantizedTick = 262_143

	// QuantizationScale maps floating-point tick domain to integer priority range
	// Calculated as (MaxQuantizedTick - 1) / (2 * TickClampingBound) ≈ 1023.99
	QuantizationScale = (MaxQuantizedTick - 1) / (2 * TickClampingBound)

	// MaxInitializationPriority defines the lowest priority value for cycle initialization
	// Uses MaxQuantizedTick (262,143) to ensure cycles start with minimum priority
	MaxInitializationPriority = MaxQuantizedTick
)

// ============================================================================
// WEBSOCKET COMMUNICATION CONFIGURATION
// ============================================================================

const (
	// WebSocket endpoint configuration for Ethereum-compatible provider
	WsDialAddr = "polygon-mainnet.infura.io:443"
	WsPath     = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"
	WsHost     = "polygon-mainnet.infura.io"

	// MaxFrameSize sets maximum WebSocket frame payload size
	// 512 KiB accommodates largest realistic log payloads with optimal cache efficiency
	MaxFrameSize = 512 << 10 // 512 KiB

	// FrameCap defines retained frame capacity for parsing operations
	// 131,072 frames accommodates high burst scenarios with optimal cache locality
	FrameCap = 1 << 17 // 131,072 frames
)

// ============================================================================
// SYSTEM INITIALIZATION AND OPERATIONAL PARAMETERS
// ============================================================================

const (
	// ShardChannelBufferSize defines the buffer depth for shard distribution channels
	// 1,024 entries (2^10) provides sufficient buffering for parallel initialization
	ShardChannelBufferSize = 1 << 10
)

// ============================================================================
// JSON PARSING OPTIMIZATION CONSTANTS
// ============================================================================

var (
	// 8-byte field detection probes for zero-allocation JSON parsing in hot paths
	// Optimized for efficient field identification without string allocation
	KeyAddress        = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'}
	KeyBlockHash      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'}
	KeyBlockNumber    = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'}
	KeyBlockTimestamp = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'T', 'i'}
	KeyData           = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'}
	KeyLogIndex       = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'}
	KeyRemoved        = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'}
	KeyTopics         = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'}
	KeyTransaction    = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'}

	// Event signature probe for Uniswap V2 Sync() event identification
	SigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'}
)
