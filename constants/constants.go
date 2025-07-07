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
//
// Memory layout legend:
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│ Constants → Compilation → Binary → Runtime → Cache-aligned structures   │
//	│                            ▲                              │             │
//	│                      Power-of-2 ◀──────────────────────────┘             │
//	└─────────────────────────────────────────────────────────────────────────┘
//
// Compiler optimizations:
//   • Compile-time constant folding for all arithmetic expressions
//   • Power-of-2 values enable bit manipulation optimization
//   • Cache-line aligned sizes optimize memory access patterns

package constants

// ============================================================================
// CORE SYSTEM ARCHITECTURE LIMITS
// ============================================================================

const (
	// MaxSupportedCores defines the maximum number of CPU cores supported
	// 64 cores accommodates current high-end server hardware configurations
	// and provides excellent parallelization for arbitrage detection workloads
	MaxSupportedCores = 64

	// DefaultRingSize defines the default capacity for inter-core ring buffers
	// 65,536 entries (2^16) provides excellent throughput with manageable memory usage
	// Sized for sustained high-frequency trading loads without buffer overflow
	DefaultRingSize = 1 << 16

	// DefaultLocalIdxSize defines the default capacity for local indexing structures
	// 65,536 entries (2^16) matches ring buffer size for optimal cache behavior
	// Ensures consistent performance characteristics across indexing operations
	DefaultLocalIdxSize = 1 << 16

	// MaxCyclesPerShard defines the maximum arbitrage cycles per shard bucket
	// 65,536 cycles (2^16) provides cache-friendly grouping for load balancing
	// Optimizes memory locality during cycle processing and fanout operations
	MaxCyclesPerShard = 1 << 16
)

// ============================================================================
// MEMORY MANAGEMENT AND PERFORMANCE GUARDRAILS
// ============================================================================

const (
	// HeapSoftLimit triggers non-blocking garbage collection when exceeded
	// 128 MiB reduces GC pressure during sustained high-throughput operation
	// Prevents performance degradation from excessive heap growth
	HeapSoftLimit = 128 << 20 // 128 MiB

	// HeapHardLimit triggers panic if exceeded to prevent system instability
	// 512 MiB prevents system-wide memory pressure and enables rapid leak detection
	// Ensures fail-fast behavior rather than gradual performance degradation
	HeapHardLimit = 512 << 20 // 512 MiB

	// RingBits defines the deduplication cache size: 2^18 = 262,144 slots ≈ 8 MiB
	// Optimized for L3 cache efficiency while handling 250K+ logs safely
	// Power-of-2 size enables efficient modular arithmetic for cache indexing
	RingBits = 18

	// MaxReorg defines maximum blockchain reorganization depth before entry eviction
	// 128 blocks ≈ 3 minutes at 1.4s block time, sufficient for real-world reorganizations
	// Balances memory usage with robustness against chain reorganizations
	MaxReorg = 128
)

// ============================================================================
// HASH TABLE AND ADDRESS INDEXING CONFIGURATION
// ============================================================================

const (
	// AddressTableCapacity defines the size of the Robin Hood hash table
	// 1M entries (2^20) provides excellent performance for production workloads
	// Sized to handle hundreds of thousands of unique trading pairs efficiently
	AddressTableCapacity = 1 << 20

	// AddressTableMask provides efficient modulo operation for hash table indexing
	// Calculated as AddressTableCapacity - 1 for power-of-2 optimization
	// Enables single-instruction modular arithmetic using bitwise AND operation
	AddressTableMask = AddressTableCapacity - 1

	// PairRoutingTableCapacity defines the size of the pair-to-core routing table
	// 1M entries (2^20) matches AddressTableCapacity for consistent pair ID space
	// Ensures uniform performance characteristics across address and routing lookups
	PairRoutingTableCapacity = 1 << 20

	// AddressHexStart defines the starting index of Ethereum address hex in LogView.Addr
	// Format: "0x" + 40 hex characters, address hex begins at index 2
	// Note: Implementation uses index 3 for specific parsing optimization
	AddressHexStart = 3

	// AddressHexEnd defines the ending index (exclusive) of Ethereum address hex
	// 40-character hex representation ends at index 43 (exclusive)
	// Enables efficient slice operations for address extraction
	AddressHexEnd = 43
)

// ============================================================================
// ARBITRAGE TICK QUANTIZATION PARAMETERS
// ============================================================================

const (
	// TickClampingBound defines the domain constraints for tick values
	// Domain: [-128, +128] ensures numerical stability in arbitrage calculations
	// Prevents overflow in fixed-point arithmetic and priority queue operations
	TickClampingBound = 128.0

	// MaxQuantizedTick defines the 18-bit ceiling for quantized tick values
	// 2^18 - 1 = 262,143 provides sufficient precision for priority queue operations
	// Matches QuantumQueue64 priority range for optimal performance
	MaxQuantizedTick = 262_143

	// QuantizationScale maps floating-point tick domain to integer priority range
	// Calculated as (MaxQuantizedTick - 1) / (2 * TickClampingBound) ≈ 1023.99
	// Provides linear mapping from tick space to priority space with maximum precision
	QuantizationScale = (MaxQuantizedTick - 1) / (2 * TickClampingBound)

	// MaxInitializationPriority defines the lowest priority value for cycle initialization
	// Uses MaxQuantizedTick (262,143) to ensure cycles start with minimum priority
	// Guarantees new cycles appear at bottom of priority queue until updated
	MaxInitializationPriority = MaxQuantizedTick
)

// ============================================================================
// WEBSOCKET COMMUNICATION CONFIGURATION
// ============================================================================

const (
	// WebSocket endpoint configuration for Ethereum-compatible provider
	// Polygon mainnet via Infura for high-frequency transaction log streaming
	WsDialAddr = "polygon-mainnet.infura.io:443"
	WsPath     = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"
	WsHost     = "polygon-mainnet.infura.io"

	// MaxFrameSize sets maximum WebSocket frame payload size
	// 512 KiB accommodates largest realistic log payloads with optimal cache efficiency
	// Prevents buffer overflow while maintaining memory efficiency
	MaxFrameSize = 512 << 10 // 512 KiB

	// FrameCap defines retained frame capacity for parsing operations
	// 131,072 frames accommodates high burst scenarios with optimal cache locality
	// Power-of-2 size enables efficient circular buffer operations
	FrameCap = 1 << 17 // 131,072 frames
)

// ============================================================================
// SYSTEM INITIALIZATION AND OPERATIONAL PARAMETERS
// ============================================================================

const (
	// ShardChannelBufferSize defines the buffer depth for shard distribution channels
	// 1,024 entries (2^10) provides sufficient buffering for parallel initialization
	// Prevents blocking during system startup while maintaining memory efficiency
	ShardChannelBufferSize = 1 << 10
)

// ============================================================================
// JSON PARSING OPTIMIZATION CONSTANTS
// ============================================================================

var (
	// 8-byte field detection probes for zero-allocation JSON parsing in hot paths
	// Optimized for efficient field identification without string allocation
	// Aligned for optimal memory access patterns during parsing operations

	// Core Ethereum log field identifiers - most frequently accessed
	KeyAddress = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'} // "address
	KeyData    = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'} // "data":"
	KeyTopics  = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'} // "topics"

	// Block metadata field identifiers - accessed during log context resolution
	KeyBlockHash      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'} // "blockHa
	KeyBlockNumber    = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'} // "blockNu
	KeyBlockTimestamp = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'T', 'i'} // "blockTi

	// Transaction and log positioning field identifiers - accessed during indexing
	KeyLogIndex    = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'} // "logInde
	KeyTransaction = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'} // "transac
	KeyRemoved     = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'} // "removed

	// Event signature probe for Uniswap V2 Sync() event identification
	// keccak256("Sync(uint112,uint112)") = 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1
	SigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'} // First 8 hex chars
)
