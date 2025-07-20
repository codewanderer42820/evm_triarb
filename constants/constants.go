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
// Features:
//   - Core limits: CPU scaling parameters
//   - Memory: Cache sizes and buffers
//   - Networking: WebSocket configuration
//   - Timing: Virtual clock calibration
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package constants

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RUNTIME CRITICAL CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Cache alignment for optimal memory access
	CacheLineSize = 64

	// Tick quantization for arbitrage calculations (used in every price update)
	TickClampingBound = 128.0
	MaxQuantizedTick  = 262_143
	QuantizationScale = (MaxQuantizedTick - 1) / (2 * TickClampingBound)

	// Address parsing constants (used in every event)
	AddressHexStart = 2
	AddressHexEnd   = 42
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE SYSTEM LIMITS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	MaxSupportedCores   = 64
	DefaultRingSize     = 1 << 14
	DefaultLocalIdxSize = 1 << 16
	MaxCyclesPerShard   = 1 << 18
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS RESOLUTION TABLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	AddressTableCapacity     = 1 << 21
	AddressTableMask         = AddressTableCapacity - 1
	PairRoutingTableCapacity = 1 << 21
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MEMORY MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	RingBits = 18
	MaxReorg = 256
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ACTIVE TIMING CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	ActivePollRate   = uint64(3_200_000_000) // Apple M4 Pro optimized
	ActiveCooldownMs = uint64(500)           // Balanced setting
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// WEBSOCKET PROTOCOL
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
// HARVESTER ENDPOINTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	HarvesterDialAddr = "mainnet.infura.io:443"
	HarvesterPath     = "/v3/a2a3139d2ab24d59bed2dc3643664126"
	HarvesterHost     = "mainnet.infura.io"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HARVESTER PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	HarvesterDeploymentBlock = uint64(10000835)
	HarvesterOutputPath      = "uniswap_v2.csv"
	HarvesterTempPath        = "uniswap_v2.csv.tmp" // Added for temporary sync file
	HarvesterMetadataPath    = HarvesterOutputPath + ".meta"
	SyncEventSignature       = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
	OptimalBatchSize         = uint64(8_000)
	MinBatchSize             = uint64(1)
	MaxLogSliceSize          = 1_000_000
	DefaultConnections       = 3
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// BUFFER SIZES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	ResponseBufferSize     = 8 * 1024 * 1024
	CSVBufferSize          = 1024 * 1024
	ReadBufferSize         = 64 * 1024
	ShardChannelBufferSize = 1 << 10
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TIMING PROFILES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
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

//go:notinheap
//go:align 64
var (
	// CACHE LINE 1: Primary cooldown calculations
	CooldownPolls       = (ActiveCooldownMs * ActivePollRate) / 1000 // 8B
	CooldownPolls500ms  = (CooldownMs500 * ActivePollRate) / 1000    // 8B
	CooldownPolls250ms  = (CooldownMs250 * ActivePollRate) / 1000    // 8B
	CooldownPolls100ms  = (CooldownMs100 * ActivePollRate) / 1000    // 8B
	CooldownPolls50ms   = (CooldownMs50 * ActivePollRate) / 1000     // 8B
	CooldownPolls1000ms = (CooldownMs1000 * ActivePollRate) / 1000   // 8B
	CooldownPolls2000ms = (CooldownMs2000 * ActivePollRate) / 1000   // 8B
	_                   [8]byte                                      // 8B - Padding to fill cache line
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// JSON PARSING LOOKUP TABLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 64
var (
	// CACHE LINE 1: Most frequently accessed keys during JSON parsing
	KeyAddress        = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'} // 8B
	KeyData           = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'} // 8B
	KeyTopics         = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'} // 8B
	KeyBlockNumber    = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'} // 8B
	KeyBlockHash      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'} // 8B
	KeyBlockTimestamp = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'T', 'i'} // 8B
	KeyLogIndex       = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'} // 8B
	KeyTransaction    = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'} // 8B

	// CACHE LINE 2: Less frequently accessed keys
	KeyRemoved    = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'} // 8B
	SigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'} // 8B
	_             [48]byte                                          // 48B - Padding to fill cache line
)
