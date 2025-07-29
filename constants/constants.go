// ════════════════════════════════════════════════════════════════════════════════════════════════
// System Configuration & Constants
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Global Configuration Parameters
//
// Description:
//   Defines system-wide constants and runtime parameters optimized for arbitrage detection.
//   Includes cache-aligned lookup tables and deployment profiles for different hardware.
//
// Organization:
//   - WebSocket Protocol: Connection and frame handling
//   - Parser: JSON field detection and event processing
//   - Deduper: Event deduplication and filtering
//   - Router: Core arbitrage detection and event routing
//   - Aggregator: Opportunity collection and bundle extraction
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package constants

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// WEBSOCKET PROTOCOL CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	WsDialAddr          = "mainnet.infura.io:443"
	WsPath              = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126"
	WsHost              = "mainnet.infura.io"
	MaxFrameSize        = 1 << 20
	BufferSize          = 1 << 27
	HandshakeBufferSize = 512
	MaxFrameHeaderSize  = 10
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HARVESTER ENDPOINTS AND PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	HarvesterDialAddr        = "mainnet.infura.io:443"
	HarvesterPath            = "/v3/a2a3139d2ab24d59bed2dc3643664126"
	HarvesterHost            = "mainnet.infura.io"
	HarvesterDeploymentBlock = uint64(10000835)
	HarvesterOutputPath      = "uniswap_v2.csv"
	HarvesterTempPath        = "uniswap_v2.csv.tmp"
	HarvesterMetadataPath    = HarvesterOutputPath + ".meta"
	SyncEventSignature       = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
	OptimalBatchSize         = uint64(8_000)
	MinBatchSize             = uint64(1)
	MaxLogSliceSize          = 1_000_000
	DefaultConnections       = 2
	ResponseBufferSize       = 8 * 1024 * 1024
	CSVBufferSize            = 1024 * 1024
	ReadBufferSize           = 64 * 1024
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PARSER CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Address parsing constants (used in every event)
	AddressHexStart = 2
	AddressHexEnd   = 42
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DEDUPER CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Memory management for reorg protection
	RingBits = 18
	MaxReorg = 256
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ROUTER CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Core system limits
	MaxSupportedCores   = 64
	DefaultRingSize     = 1 << 14
	DefaultLocalIdxSize = 1 << 16
	MaxCyclesPerShard   = 1 << 18

	// Address resolution tables
	AddressTableCapacity     = 1 << 21
	AddressTableMask         = AddressTableCapacity - 1
	PairRoutingTableCapacity = 1 << 21

	// Router tick quantization for arbitrage calculations (used in every price update)
	RouterTickClampingBound = 128.0
	RouterMaxQuantizedTick  = 262_143
	RouterQuantizationScale = (RouterMaxQuantizedTick - 1) / (2 * RouterTickClampingBound)

	// Channel buffer sizing
	ShardChannelBufferSize = 1 << 10
)

// Legacy aliases removed - all components now use explicit prefixed constants

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// AGGREGATOR CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Hash table configuration optimized for arbitrage cycle storage and retrieval.
	// Table size chosen as power of 2 to enable efficient modulo operations via bitmasking.
	// Capacity of 65,536 slots provides sufficient space for expected cycle diversity
	// while maintaining cache-friendly memory access patterns.
	AggregatorTableCapacity = 1 << 16                     // 65,536 hash table slots for opportunity storage
	AggregatorTableMask     = AggregatorTableCapacity - 1 // 65,535 bitmask for hash table indexing operations

	// Occupancy bitmap for efficient hash table slot tracking.
	// Bitmap organized as 1,024 uint64 words, each covering 64 hash table slots.
	// This arrangement provides cache-efficient occupancy testing and bulk reset operations.
	AggregatorBitmapWords = 1 << 10 // 1,024 uint64 words for hash table occupancy tracking

	// Aggregator profitability quantization for priority queue mapping (used in every opportunity)
	AggregatorProfitabilityClampingBound = 192.0                                                                        // Input range limit: [-192, +192]
	AggregatorMaxPriorityTick            = 127                                                                          // Maximum priority tick value
	AggregatorProfitabilityScale         = (AggregatorMaxPriorityTick - 1) / (2 * AggregatorProfitabilityClampingBound) // Scale factor: 126 / 384

	// Liquidity stratification system based on reserve leading zero analysis.
	// Maximum of 96 strata covers the theoretical range of leading zeros in uint256 values
	// (3 pairs × 32 hex characters = 96), though practical reserves use uint112 precision.
	// Each stratum represents a distinct liquidity tier for execution risk assessment.
	AggregatorMaxStrata = 96 // Liquidity tier count covering full theoretical hex leading zero range

	// Execution bundle size limit balancing transaction efficiency with gas constraints.
	// Bundle size of 32 opportunities represents optimal balance between:
	// - Transaction gas limit utilization (approximately 12M gas per bundle)
	// - MEV extraction efficiency (diminishing returns beyond 32 opportunities)
	// - Block inclusion probability (larger bundles face higher inclusion risk)
	AggregatorMaxBundleSize = 32 // Maximum opportunities per execution bundle for gas optimization
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TIMING PROFILES AND ACTIVE CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Active timing configuration
	ActivePollRate   = uint64(3_200_000_000) // Apple M4 Pro optimized
	ActiveCooldownMs = uint64(500)           // Balanced setting

	// Timing profiles for different hardware configurations
	PollRateConservative = uint64(2_000_000_000)
	PollRateTypical      = uint64(3_500_000_000)
	PollRateAggressive   = uint64(4_500_000_000)
	PollRateAppleSilicon = uint64(3_200_000_000)
	PollRateServer       = uint64(2_800_000_000)

	CooldownMs50   = uint64(50)
	CooldownMs100  = uint64(100)
	CooldownMs250  = uint64(250)
	CooldownMs500  = uint64(500)
	CooldownMs1000 = uint64(1000)
	CooldownMs2000 = uint64(2000)
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DEPLOYMENT PROFILES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	ConfigIntelConservative = "intel_2ghz"
	ConfigIntelTypical      = "intel_3_5ghz"
	ConfigIntelAggressive   = "intel_4_5ghz"
	ConfigAppleSilicon      = "apple_m4_pro"
	ConfigARMServer         = "arm_server"
	ConfigLowLatency        = "low_latency"
	ConfigBatteryOptimized  = "battery_opt"
	ConfigServerOptimized   = "server_opt"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COMPUTED RUNTIME VALUES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Runtime configuration values with cache line optimization for frequent polling operations.
// Variables ordered by access frequency during polling loops and grouped to maximize
// cache line utilization. Alignment prevents false sharing with other global state.
//
//go:notinheap
//go:align 64
var (
	// CACHE LINE 1: Primary cooldown calculations (accessed every poll cycle)
	CooldownPolls       = (ActiveCooldownMs * ActivePollRate) / 1000 // 8B - Nuclear hot: main cooldown
	CooldownPolls500ms  = (CooldownMs500 * ActivePollRate) / 1000    // 8B - Hot: 500ms variant
	CooldownPolls250ms  = (CooldownMs250 * ActivePollRate) / 1000    // 8B - Hot: 250ms variant
	CooldownPolls100ms  = (CooldownMs100 * ActivePollRate) / 1000    // 8B - Warm: 100ms variant
	CooldownPolls50ms   = (CooldownMs50 * ActivePollRate) / 1000     // 8B - Warm: 50ms variant
	CooldownPolls1000ms = (CooldownMs1000 * ActivePollRate) / 1000   // 8B - Cold: 1000ms variant
	CooldownPolls2000ms = (CooldownMs2000 * ActivePollRate) / 1000   // 8B - Cold: 2000ms variant
	_                   [8]byte                                      // 8B - Padding to fill cache line
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// JSON PARSING LOOKUP TABLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// JSON parsing lookup tables with cache line optimization for hot path field detection.
// Key patterns ordered by access frequency during JSON parsing operations to maximize
// cache hit rates and minimize memory stalls during event processing.
//
//go:notinheap
//go:align 64
var (
	// CACHE LINE 1: Most frequently accessed keys during JSON parsing
	KeyAddress        = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'} // 8B - Nuclear hot: address field
	KeyData           = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'} // 8B - Nuclear hot: data field
	KeyTopics         = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'} // 8B - Nuclear hot: topics field
	KeyBlockNumber    = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'} // 8B - Hot: block number field
	KeyBlockHash      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'} // 8B - Warm: block hash field
	KeyBlockTimestamp = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'T', 'i'} // 8B - Warm: timestamp field
	KeyLogIndex       = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'} // 8B - Hot: log index field
	KeyTransaction    = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'} // 8B - Warm: transaction field

	// CACHE LINE 2: Less frequently accessed keys and signatures
	KeyRemoved    = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'} // 8B - Cold: removed flag field
	SigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'} // 8B - Hot: Sync event signature
	_             [48]byte                                          // 48B - Padding to fill cache line
)
