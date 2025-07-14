// constants.go — System-wide constants for high-performance arbitrage detection

package constants

// Core system limits
const (
	MaxSupportedCores   = 64      // Current 1D limit, future 2D bitmapping enables 64×64 = 4,096 cores
	DefaultRingSize     = 1 << 14 // 16,384 entries - optimized for high throughput
	DefaultLocalIdxSize = 1 << 16 // 65,536 entries - handles growth patterns
	MaxCyclesPerShard   = 1 << 18 // 262,144 cycles per shard - optimal bucket utilization
)

// Memory management limits
const (
	RingBits = 18  // 2^18 = 262,144 dedup cache slots - high volume capacity
	MaxReorg = 256 // Maximum blockchain reorg depth - ultra-safety buffer
)

// Hash table configuration
const (
	AddressTableCapacity     = 1 << 21 // 2M entries - massive scale support
	AddressTableMask         = AddressTableCapacity - 1
	PairRoutingTableCapacity = 1 << 21 // 2M entries - matches address table
	AddressHexStart          = 2       // Address parsing start (skip "0x")
	AddressHexEnd            = 42      // Address parsing end (40 hex chars)
)

// Arbitrage tick quantization
const (
	TickClampingBound = 128.0 // Tick domain bounds [-128, +128] for sums
	// CRITICAL CONSTRAINT: Extraction strategy extracts up to 16 hex characters
	// (64 bits) per reserve, yielding individual tick values up to log₂(2⁶⁴-1) ≈ ±64.
	// In triangular arbitrage, we sum 2 populated tick values (3rd tick is always 0).
	// Maximum possible tick sum: 2 × 64 = ±128, requiring TickClampingBound = 128.0

	MaxQuantizedTick  = 262_143                                          // 18-bit ceiling (2^18 - 1) - aligns with queue buckets
	QuantizationScale = (MaxQuantizedTick - 1) / (2 * TickClampingBound) // -1 prevents rounding overflow past MaxQuantizedTick
)

// WebSocket configuration
const (
	WsDialAddr   = "polygon-mainnet.infura.io:443"
	WsPath       = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"
	WsHost       = "polygon-mainnet.infura.io"
	MaxFrameSize = 1 << 20 // 1MB frame size - high-volume stream capacity
)

// System initialization
const (
	ShardChannelBufferSize = 1 << 10 // 1,024 entries - heavy distribution support
)

// Architecture-specific presets for easy deployment configuration
const (
	// Intel/AMD configurations
	ConfigIntelConservative = "intel_2ghz"   // 2GHz base, 500ms cooldown
	ConfigIntelTypical      = "intel_3_5ghz" // 3.5GHz typical, 500ms cooldown
	ConfigIntelAggressive   = "intel_4_5ghz" // 4.5GHz boost, 250ms cooldown

	// ARM configurations
	ConfigAppleSilicon = "apple_m4_pro" // 3.2GHz M4 Pro, 500ms cooldown
	ConfigARMServer    = "arm_server"   // 2.8GHz server, 500ms cooldown

	// Special purpose configurations
	ConfigLowLatency       = "low_latency" // High poll rate, 50ms cooldown
	ConfigBatteryOptimized = "battery_opt" // Conservative rate, 2s cooldown
	ConfigServerOptimized  = "server_opt"  // Balanced rate, 500ms cooldown
)

// Virtual timing configuration (syscall-free performance)
const (
	// CPU polling rates (polls per second) - pure integer arithmetic
	PollRateConservative = uint64(2_000_000_000) // 2GHz base rate
	PollRateTypical      = uint64(3_500_000_000) // 3.5GHz realistic typical (was 4GHz)
	PollRateAggressive   = uint64(4_500_000_000) // 4.5GHz realistic boost (was 5GHz)
	PollRateAppleSilicon = uint64(3_200_000_000) // M4 Pro measured rate (accurate)
	PollRateServer       = uint64(2_800_000_000) // Server conservative rate (more realistic)

	// Cooldown timing options (milliseconds)
	CooldownMs50   = uint64(50)   // 50ms cooldown (ultra-responsive)
	CooldownMs100  = uint64(100)  // 100ms cooldown (very responsive)
	CooldownMs250  = uint64(250)  // 250ms cooldown (responsive)
	CooldownMs500  = uint64(500)  // 500ms cooldown (balanced)
	CooldownMs1000 = uint64(1000) // 1 second cooldown (conservative)
	CooldownMs2000 = uint64(2000) // 2 second cooldown (very conservative)

	// Active configuration (easily changeable for different deployments)
	ActivePollRate   = PollRateAppleSilicon // M4 Pro optimized
	ActiveCooldownMs = CooldownMs500        // 500ms default (more responsive than 1s)
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COMPUTED RUNTIME VALUES AND LOOKUP TABLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════
//
// These values are computed from constants during package initialization
// and remain immutable throughout system operation. Cache-aligned for
// optimal memory access patterns during high-frequency operations.

// COMPUTED RUNTIME VALUES (READ-ONLY AFTER INITIALIZATION)
// These values are computed from constants during package initialization
// and remain immutable throughout system operation.
//
//go:notinheap
//go:align 64
var (
	// Cooldown in poll counts: (milliseconds * polls_per_second) / 1000
	// For M4 Pro default: (500ms * 3,200,000,000 polls/sec) / 1000 = 1,600,000,000 polls
	CooldownPolls = (ActiveCooldownMs * ActivePollRate) / 1000 // Primary cooldown configuration

	// Alternative cooldown configurations (pre-computed for fast switching)
	CooldownPolls50ms   = (CooldownMs50 * ActivePollRate) / 1000   // 160,000,000 polls (ultra-responsive)
	CooldownPolls100ms  = (CooldownMs100 * ActivePollRate) / 1000  // 320,000,000 polls (very responsive)
	CooldownPolls250ms  = (CooldownMs250 * ActivePollRate) / 1000  // 800,000,000 polls (responsive)
	CooldownPolls500ms  = (CooldownMs500 * ActivePollRate) / 1000  // 1,600,000,000 polls (balanced)
	CooldownPolls1000ms = (CooldownMs1000 * ActivePollRate) / 1000 // 3,200,000,000 polls (conservative)
	CooldownPolls2000ms = (CooldownMs2000 * ActivePollRate) / 1000 // 6,400,000,000 polls (very conservative)
)

// JSON PARSING LOOKUP TABLES (READ-ONLY, HOT PATH ACCESS)
// 8-byte aligned patterns for SIMD-optimized field detection during
// high-frequency JSON parsing operations.
//
//go:notinheap
//go:align 64
var (
	// Core log fields (primary event data)
	KeyAddress = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'} // "address
	KeyData    = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'} // "data":"
	KeyTopics  = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'} // "topics"

	// Block metadata (blockchain context)
	KeyBlockHash      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'} // "blockHa
	KeyBlockNumber    = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'} // "blockNu
	KeyBlockTimestamp = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'T', 'i'} // "blockTi

	// Transaction fields (event positioning)
	KeyLogIndex    = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'} // "logInde
	KeyTransaction = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'} // "transac
	KeyRemoved     = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'} // "removed

	// Uniswap V2 Sync event signature (arbitrage detection)
	SigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'} // keccak256("Sync(uint112,uint112)")
)
