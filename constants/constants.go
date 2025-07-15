// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚙️ SYSTEM CONFIGURATION & CONSTANTS
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Global Configuration Parameters
//
// Description:
//   Defines system-wide constants and runtime parameters optimized for high-frequency trading.
//   Includes cache-aligned lookup tables and deployment profiles for different hardware.
//
// Configuration Categories:
//   - Core limits: CPU scaling parameters
//   - Memory: Cache sizes and buffers
//   - Networking: WebSocket configuration
//   - Timing: Virtual clock calibration
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package constants

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE SYSTEM LIMITS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Core system limits define the maximum scale and capacity of the arbitrage engine.
// These values are carefully chosen to balance memory usage with processing capability
// while maintaining optimal cache utilization on modern processors.
const (
	// MaxSupportedCores defines the maximum number of CPU cores the system can utilize.
	// Currently limited to 64 for 1D bitmapping, but architecture supports future
	// expansion to 64×64 (4,096 cores) via 2D bitmapping for massive parallelism.
	MaxSupportedCores = 64

	// DefaultRingSize sets the capacity of lock-free ring buffers for inter-core messaging.
	// 16,384 entries provides sufficient buffering for burst traffic while maintaining
	// cache locality and avoiding false sharing between cores.
	DefaultRingSize = 1 << 14

	// DefaultLocalIdxSize determines the capacity of per-core hash tables.
	// 65,536 entries accommodates growth patterns in trading pair assignments
	// while keeping the working set within L3 cache on most processors.
	DefaultLocalIdxSize = 1 << 16

	// MaxCyclesPerShard limits the number of arbitrage cycles assigned to a single shard.
	// 262,144 cycles per shard optimizes memory allocation and work distribution
	// across cores while preventing any single core from becoming overloaded.
	MaxCyclesPerShard = 1 << 18
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MEMORY MANAGEMENT PARAMETERS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Memory management constants control cache sizes and safety buffers for
// blockchain-specific requirements like reorganization handling.
const (
	// RingBits determines the size of the deduplication cache as 2^RingBits entries.
	// 18 bits = 262,144 slots provides high capacity for event deduplication
	// while maintaining sub-microsecond lookup times via direct mapping.
	RingBits = 18

	// MaxReorg defines the maximum blockchain reorganization depth to handle.
	// 256 blocks provides an ultra-conservative safety buffer that exceeds
	// any historically observed reorganization by a significant margin.
	MaxReorg = 256
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS RESOLUTION CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Hash table configuration for Ethereum address to trading pair resolution.
// These tables map contract addresses to internal pair IDs for rapid lookup
// during event processing.
const (
	// AddressTableCapacity sets the hash table size for address resolution.
	// 2 million entries supports massive scale deployments tracking hundreds
	// of thousands of trading pairs with room for growth.
	AddressTableCapacity = 1 << 21

	// AddressTableMask provides fast modulo via bitwise AND for hash table indexing.
	// Always set to capacity - 1 for power-of-2 sized tables.
	AddressTableMask = AddressTableCapacity - 1

	// PairRoutingTableCapacity matches address table size for consistent scaling.
	// Maps trading pair IDs to CPU core assignments for load distribution.
	PairRoutingTableCapacity = 1 << 21

	// AddressHexStart marks the beginning of hex address data after "0x" prefix.
	// Used for efficient parsing of Ethereum addresses from event logs.
	AddressHexStart = 2

	// AddressHexEnd marks the end of a standard Ethereum address.
	// 40 hex characters plus 2 for "0x" prefix = 42 total characters.
	AddressHexEnd = 42
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ARBITRAGE TICK QUANTIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Arbitrage detection parameters control how price movements are quantized
// and prioritized in the detection engine.
const (
	// TickClampingBound defines the valid range for tick values as [-128, +128].
	// This bound is derived from the maximum possible sum in triangular arbitrage:
	// - Each Uniswap V2 reserve is extracted as 16 hex chars (64 bits)
	// - Individual tick values range up to log₂(2⁶⁴-1) ≈ ±64
	// - Triangular arbitrage sums 2 ticks (3rd is always 0 for optimization)
	// - Maximum sum: 2 × 64 = ±128, requiring this exact bound
	TickClampingBound = 128.0

	// MaxQuantizedTick sets the ceiling for quantized priority values.
	// 262,143 (2^18 - 1) aligns with the number of priority queue buckets,
	// enabling efficient bucket selection via bit operations.
	MaxQuantizedTick = 262_143

	// QuantizationScale maps floating-point tick values to integer priorities.
	// Computed to utilize the full range of quantized values while preventing
	// overflow. The -1 adjustment ensures rounding never exceeds MaxQuantizedTick.
	QuantizationScale = (MaxQuantizedTick - 1) / (2 * TickClampingBound)
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// WEBSOCKET CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// WebSocket parameters for connecting to Ethereum node providers and
// managing high-volume event streams.
const (
	// WsDialAddr specifies the WebSocket endpoint for Polygon mainnet via Infura.
	// Port 443 ensures compatibility with restrictive firewall configurations.
	WsDialAddr = "polygon-mainnet.infura.io:443"

	// WsPath contains the WebSocket path including the Infura project ID.
	// This should be configured per deployment with appropriate credentials.
	WsPath = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"

	// WsHost identifies the target host for WebSocket handshake headers.
	WsHost = "polygon-mainnet.infura.io"

	// MaxFrameSize limits individual WebSocket frame size to 1MB.
	// Sufficient for event batching while preventing memory exhaustion.
	MaxFrameSize = 1 << 20

	// BufferSize allocates 128MB for WebSocket buffering.
	// Extreme capacity handles burst traffic during high-activity periods
	// without dropping events or causing backpressure.
	BufferSize = 1 << 27

	// HandshakeBufferSize provides space for WebSocket upgrade negotiation.
	// 512 bytes accommodates headers and protocol negotiation.
	HandshakeBufferSize = 512

	// MaxFrameHeaderSize reserves space for WebSocket frame headers.
	// 10 bytes covers all frame types including extended payload length.
	MaxFrameHeaderSize = 10
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION PARAMETERS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Initialization constants control system startup and work distribution.
const (
	// ShardChannelBufferSize sets the buffer for work distribution channels.
	// 1,024 entries prevents blocking during initialization while limiting
	// memory usage during the one-time setup phase.
	ShardChannelBufferSize = 1 << 10
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DEPLOYMENT CONFIGURATION PROFILES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Architecture-specific presets enable easy deployment across different
// hardware configurations without manual tuning.
const (
	// Intel/AMD x86_64 configurations
	ConfigIntelConservative = "intel_2ghz"   // 2GHz base clock, 500ms cooldown
	ConfigIntelTypical      = "intel_3_5ghz" // 3.5GHz typical, 500ms cooldown
	ConfigIntelAggressive   = "intel_4_5ghz" // 4.5GHz boost, 250ms cooldown

	// ARM configurations
	ConfigAppleSilicon = "apple_m4_pro" // Apple M4 Pro 3.2GHz, 500ms cooldown
	ConfigARMServer    = "arm_server"   // ARM server 2.8GHz, 500ms cooldown

	// Special purpose configurations
	ConfigLowLatency       = "low_latency" // Ultra-responsive 50ms cooldown
	ConfigBatteryOptimized = "battery_opt" // Power-saving 2s cooldown
	ConfigServerOptimized  = "server_opt"  // Balanced server deployment
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// VIRTUAL TIMING CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Virtual timing parameters enable syscall-free performance monitoring
// by counting CPU operations rather than querying system time.
const (
	// CPU polling rates define expected operations per second for different processors.
	// These values are calibrated to real-world CPU frequencies and used to
	// estimate elapsed time without system calls.
	PollRateConservative = uint64(2_000_000_000) // 2GHz baseline
	PollRateTypical      = uint64(3_500_000_000) // 3.5GHz typical modern CPU
	PollRateAggressive   = uint64(4_500_000_000) // 4.5GHz boost frequency
	PollRateAppleSilicon = uint64(3_200_000_000) // M4 Pro measured rate
	PollRateServer       = uint64(2_800_000_000) // Conservative server rate

	// Cooldown timing options control responsiveness vs CPU usage trade-offs.
	// Shorter cooldowns increase responsiveness but consume more CPU cycles.
	CooldownMs50   = uint64(50)   // Ultra-responsive for latency-critical deployments
	CooldownMs100  = uint64(100)  // Very responsive for active trading
	CooldownMs250  = uint64(250)  // Responsive for normal operations
	CooldownMs500  = uint64(500)  // Balanced for sustained operation
	CooldownMs1000 = uint64(1000) // Conservative for reduced CPU usage
	CooldownMs2000 = uint64(2000) // Power-saving for battery operation

	// Active configuration selects the deployment profile.
	// Change these values to match your hardware and requirements.
	ActivePollRate   = PollRateAppleSilicon // Current deployment: M4 Pro
	ActiveCooldownMs = CooldownMs500        // Current setting: Balanced
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COMPUTED RUNTIME VALUES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Runtime values computed during package initialization from configuration constants.
// These remain immutable after initialization and are cache-aligned for optimal access.
//
//go:notinheap
//go:align 64
var (
	// CooldownPolls converts millisecond cooldowns to CPU poll counts.
	// Formula: (milliseconds * polls_per_second) / 1000
	// Example: (500ms * 3.2GHz) / 1000 = 1.6 billion polls
	CooldownPolls = (ActiveCooldownMs * ActivePollRate) / 1000

	// Pre-computed cooldown values for runtime profile switching.
	// Enables dynamic adjustment without recalculation overhead.
	CooldownPolls50ms   = (CooldownMs50 * ActivePollRate) / 1000   // ~160M polls
	CooldownPolls100ms  = (CooldownMs100 * ActivePollRate) / 1000  // ~320M polls
	CooldownPolls250ms  = (CooldownMs250 * ActivePollRate) / 1000  // ~800M polls
	CooldownPolls500ms  = (CooldownMs500 * ActivePollRate) / 1000  // ~1.6B polls
	CooldownPolls1000ms = (CooldownMs1000 * ActivePollRate) / 1000 // ~3.2B polls
	CooldownPolls2000ms = (CooldownMs2000 * ActivePollRate) / 1000 // ~6.4B polls
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// JSON PARSING LOOKUP TABLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Pre-computed byte patterns for high-speed JSON field detection.
// These 8-byte aligned patterns enable SIMD-optimized parsing of Ethereum events.
//
//go:notinheap
//go:align 64
var (
	// Primary event data fields
	KeyAddress = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'} // "address" - Contract address
	KeyData    = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'} // "data":" - Event data payload
	KeyTopics  = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'} // "topics" - Event topic array

	// Blockchain context fields
	KeyBlockHash      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'} // "blockHa" - Block hash prefix
	KeyBlockNumber    = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'} // "blockNu" - Block number prefix
	KeyBlockTimestamp = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'T', 'i'} // "blockTi" - Timestamp prefix

	// Event positioning fields
	KeyLogIndex    = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'} // "logInde" - Log index prefix
	KeyTransaction = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'} // "transac" - Transaction prefix
	KeyRemoved     = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'} // "removed" - Reorg flag

	// Event signature detection
	// First 8 bytes of keccak256("Sync(uint112,uint112)") for Uniswap V2 Sync events
	SigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'} // Identifies arbitrage opportunities
)
