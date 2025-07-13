// constants.go — System-wide constants for high-performance arbitrage detection
package constants

// Core system limits
const (
	MaxSupportedCores   = 64      // Maximum CPU cores for parallelization
	DefaultRingSize     = 1 << 16 // 65,536 entries for inter-core buffers
	DefaultLocalIdxSize = 1 << 16 // 65,536 entries for local indexing
	MaxCyclesPerShard   = 1 << 16 // 65,536 cycles per shard for cache locality
)

// Memory management limits
const (
	HeapSoftLimit = 128 << 20 // 128 MiB - triggers non-blocking GC
	HeapHardLimit = 512 << 20 // 512 MiB - triggers panic on breach
	RingBits      = 18        // 2^18 = 262,144 dedup cache slots
	MaxReorg      = 128       // Maximum blockchain reorg depth
)

// Virtual timing configuration (syscall-free performance)
const (
	// CPU polling rates (polls per second) - pure integer arithmetic
	PollRateConservative = uint64(2_000_000_000) // 2GHz base rate
	PollRateTypical      = uint64(4_000_000_000) // 4GHz typical rate
	PollRateAggressive   = uint64(5_000_000_000) // 5GHz boost rate
	PollRateAppleSilicon = uint64(3_200_000_000) // M4 Pro measured rate
	PollRateServer       = uint64(3_000_000_000) // Server conservative rate

	// Cooldown timing options (milliseconds)
	CooldownMs50   = uint64(50)   // 50ms cooldown (ultra-responsive)
	CooldownMs100  = uint64(100)  // 100ms cooldown (very responsive)
	CooldownMs250  = uint64(250)  // 250ms cooldown (responsive)
	CooldownMs500  = uint64(500)  // 500ms cooldown (balanced)
	CooldownMs1000 = uint64(1000) // 1 second cooldown (conservative)
	CooldownMs2000 = uint64(2000) // 2 second cooldown (very conservative)
	CooldownMs5000 = uint64(5000) // 5 second cooldown (ultra-conservative)

	// Active configuration (easily changeable for different deployments)
	ActivePollRate   = PollRateAppleSilicon // M4 Pro optimized
	ActiveCooldownMs = CooldownMs1000       // 1 second default cooldown
)

// Hash table configuration
const (
	AddressTableCapacity     = 1 << 20 // 1M entries for addresses
	AddressTableMask         = AddressTableCapacity - 1
	PairRoutingTableCapacity = 1 << 20 // 1M entries for routing
	AddressHexStart          = 2       // Address parsing start (skip "0x")
	AddressHexEnd            = 42      // Address parsing end (40 hex chars)
)

// Arbitrage tick quantization
const (
	TickClampingBound = 128.0 // Tick domain bounds [-128, +128] for sums
	// CRITICAL CONSTRAINT: The extraction strategy can extract up to 16 hex
	// characters (64 bits) per reserve, yielding individual tick values up to
	// log₂(2⁶⁴-1) ≈ ±64. In triangular arbitrage, we sum 2 populated tick
	// values (the 3rd tick is always 0 since it represents the direct pair
	// being updated). Therefore, the maximum possible tick sum is:
	// 2 × 64 = ±128, requiring TickClampingBound = 128.0
	//
	// FOOTGUN: The quantization formula assumes tick sums stay within
	// [-128, +128]. Values outside this range will overflow the quantized
	// integer representation, causing incorrect priority queue ordering.
	// However, this is now mathematically impossible given our 64-bit
	// extraction limit and 2-tick summation.

	MaxQuantizedTick  = 262_143 // 18-bit ceiling (2^18 - 1)
	QuantizationScale = (MaxQuantizedTick - 1) / (2 * TickClampingBound)
)

// WebSocket configuration
const (
	WsDialAddr   = "polygon-mainnet.infura.io:443"
	WsPath       = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"
	WsHost       = "polygon-mainnet.infura.io"
	MaxFrameSize = 512 << 10 // 512 KiB frame size
	FrameCap     = 1 << 17   // 131,072 frames
)

// System initialization
const (
	ShardChannelBufferSize = 1 << 10 // 1,024 entries for shard distribution
)

// Computed constants (pure integer arithmetic - no type conversion issues)
var (
	// Cooldown in poll counts: (milliseconds * polls_per_second) / 1000
	// For M4 Pro default: (1000ms * 3,200,000,000 polls/sec) / 1000 = 3,200,000,000 polls
	CooldownPolls = (ActiveCooldownMs * ActivePollRate) / 1000

	// Alternative configurations (computed from constants above)
	CooldownPolls50ms   = (CooldownMs50 * ActivePollRate) / 1000   // 160,000,000 polls
	CooldownPolls100ms  = (CooldownMs100 * ActivePollRate) / 1000  // 320,000,000 polls
	CooldownPolls250ms  = (CooldownMs250 * ActivePollRate) / 1000  // 800,000,000 polls
	CooldownPolls500ms  = (CooldownMs500 * ActivePollRate) / 1000  // 1,600,000,000 polls
	CooldownPolls1000ms = (CooldownMs1000 * ActivePollRate) / 1000 // 3,200,000,000 polls
	CooldownPolls2000ms = (CooldownMs2000 * ActivePollRate) / 1000 // 6,400,000,000 polls
	CooldownPolls5000ms = (CooldownMs5000 * ActivePollRate) / 1000 // 16,000,000,000 polls
)

// JSON parsing probes - 8-byte aligned for SIMD operations
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

// Architecture-specific presets for easy deployment configuration
const (
	// Intel/AMD configurations
	ConfigIntelConservative = "intel_2ghz" // 2GHz base, 1s cooldown
	ConfigIntelTypical      = "intel_4ghz" // 4GHz typical, 1s cooldown
	ConfigIntelAggressive   = "intel_5ghz" // 5GHz boost, 500ms cooldown

	// ARM configurations
	ConfigAppleSilicon = "apple_m4_pro" // 3.2GHz M4 Pro, 1s cooldown
	ConfigARMServer    = "arm_server"   // 3GHz server, 1s cooldown

	// Special purpose configurations
	ConfigLowLatency       = "low_latency" // High poll rate, 50ms cooldown
	ConfigBatteryOptimized = "battery_opt" // Conservative rate, 2s cooldown
	ConfigServerOptimized  = "server_opt"  // Balanced rate, 1s cooldown
)
