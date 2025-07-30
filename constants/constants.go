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
//   - Harvester: Historical data collection and processing
//   - WebSocket Protocol: Real-time connection and frame handling
//   - Parser: JSON field detection and event processing
//   - Deduper: Event deduplication and filtering
//   - Router: Core arbitrage detection and event routing
//   - Aggregator: Opportunity collection and bundle extraction
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package constants

import "time"

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HARVESTER CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Historical data endpoint configuration for Ethereum JSON-RPC operations.
	// These settings define connection parameters for retrieving historical Uniswap V2 events
	// from Infura's mainnet archive nodes during the initial bootstrapping process.
	HarvesterDialAddr = "mainnet.infura.io:443"                // TLS connection endpoint for secure API access
	HarvesterPath     = "/v3/a2a3139d2ab24d59bed2dc3643664126" // Infura project API path with authentication key
	HarvesterHost     = "mainnet.infura.io"                    // HTTP Host header value for proper request routing

	// Historical synchronization parameters defining the scope and organization of data collection.
	// Block range and output file configuration for systematic historical event extraction.
	HarvesterDeploymentBlock = uint64(10000835)              // First block containing Uniswap V2 contracts (genesis deployment)
	HarvesterOutputPath      = "uniswap_v2.csv"              // Primary CSV output file for historical reserve data
	HarvesterTempPath        = "uniswap_v2.csv.tmp"          // Temporary file for atomic write operations
	HarvesterMetadataPath    = HarvesterOutputPath + ".meta" // Binary metadata file tracking processing progress

	// Event filtering and identification constants for precise log extraction.
	// Keccak-256 hash of Sync(uint112,uint112) event signature used by all Uniswap V2 pairs.
	HarvesterSyncEventSignature = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1" // Sync event topic[0] hash

	// Performance optimization parameters for batch processing and throughput maximization.
	// Dynamic batch sizing with convergence algorithms to optimize RPC request efficiency.
	HarvesterOptimalBatchSize   = uint64(8_000) // Target block range per RPC request for optimal throughput
	HarvesterMinBatchSize       = uint64(1)     // Minimum batch size when encountering rate limits
	HarvesterMaxLogSliceSize    = 1_000_000     // Maximum log entries per processing buffer allocation
	HarvesterDefaultConnections = 2             // Default parallel connection count for concurrent harvesting

	// Buffer allocation constants for zero-allocation processing and memory efficiency.
	// Pre-allocated buffer sizes optimized for typical Ethereum JSON-RPC response patterns.
	HarvesterResponseBufferSize = 8 * 1024 * 1024 // 8MB HTTP response buffer for large log batches
	HarvesterCSVBufferSize      = 1024 * 1024     // 1MB CSV output buffer for batched file writes
	HarvesterReadBufferSize     = 64 * 1024       // 64KB read buffer for streaming HTTP responses
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// WEBSOCKET PROTOCOL CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Real-time WebSocket connection parameters for live Ethereum event streaming.
	// Configuration optimized for sustained high-frequency message processing with minimal latency.
	WsDialAddr = "mainnet.infura.io:443"                   // TLS WebSocket endpoint for secure real-time connections
	WsPath     = "/ws/v3/a2a3139d2ab24d59bed2dc3643664126" // WebSocket upgrade path with Infura project credentials
	WsHost     = "mainnet.infura.io"                       // Host header for WebSocket handshake validation

	// WebSocket frame processing limits for memory safety and performance optimization.
	// Buffer sizes chosen to handle peak Ethereum network activity without memory exhaustion.
	WsMaxFrameSize        = 1 << 20 // 1MB maximum individual WebSocket frame size
	WsBufferSize          = 1 << 27 // 128MB message accumulation buffer for large JSON-RPC responses
	WsHandshakeBufferSize = 512     // 512B buffer for WebSocket upgrade handshake processing
	WsMaxFrameHeaderSize  = 10      // Maximum bytes in WebSocket frame header (including extended length)
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PARSER CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// JSON-RPC message parsing boundaries for zero-allocation field extraction.
	// Hex string parsing constants used in every Ethereum address and data field extraction operation.
	ParserAddressHexStart = 2  // Start offset for address parsing (skip "0x" prefix)
	ParserAddressHexEnd   = 42 // End offset for address parsing (40 hex characters + "0x")
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DEDUPER CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Blockchain reorganization protection parameters for event deduplication cache management.
	// Ring buffer configuration for maintaining processed event history during chain reorgs.
	DedupeRingBits = 18  // 2^18 = 262,144 cache entries for deduplication tracking
	DedupeMaxReorg = 256 // Maximum block depth for reorganization protection (conservative estimate)
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ROUTER CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Multi-core arbitrage processing system limits for optimal CPU utilization.
	// Core count and memory allocation parameters tuned for modern multi-core processors.
	RouterMaxSupportedCores   = 64      // Maximum CPU cores supported by arbitrage engine
	RouterDefaultRingSize     = 1 << 14 // 16,384 entry ring buffer for inter-core message passing
	RouterDefaultLocalIdxSize = 1 << 16 // 65,536 entry local index tables for core routing
	RouterMaxCyclesPerShard   = 1 << 18 // 262,144 maximum arbitrage cycles per workload shard

	// Address resolution hash table configuration for contract address to pair ID mapping.
	// Robin Hood hashing parameters optimized for Ethereum address distribution patterns.
	RouterAddressTableCapacity     = 1 << 21                        // 2,097,152 slots for address-to-pair mapping hash table
	RouterAddressTableMask         = RouterAddressTableCapacity - 1 // Bitmask for efficient hash table indexing
	RouterPairRoutingTableCapacity = 1 << 21                        // 2,097,152 entries for pair-to-core routing assignments

	// Arbitrage cycle profitability quantization for priority queue operations.
	// Mathematical constants for converting floating-point tick values to integer priorities.
	RouterTickClampingBound = 128.0                                                        // Input range limit for tick quantization: [-128, +128]
	RouterMaxQuantizedTick  = 262_143                                                      // Maximum quantized tick value for priority calculations
	RouterQuantizationScale = (RouterMaxQuantizedTick - 1) / (2 * RouterTickClampingBound) // Scale factor: 262142 / 256

	// Inter-core communication buffer sizing for workload distribution during initialization.
	// Channel capacity optimization to prevent blocking during shard distribution phase.
	RouterShardChannelBufferSize = 1 << 10 // 1,024 entry buffer for workload shard distribution channels
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// AGGREGATOR CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Multi-core communication infrastructure for opportunity collection from router cores.
	// Ring buffer array size must match router core count to enable direct core-to-aggregator messaging.
	AggregatorMaxSupportedCores = RouterMaxSupportedCores // Maximum cores supported by aggregator (matches router capacity)

	// Opportunity hash table configuration optimized for arbitrage cycle storage and retrieval.
	// Power-of-2 table size enables efficient modulo operations via bitmasking for fast indexing.
	// Capacity chosen to handle expected cycle diversity while maintaining cache-friendly access patterns.
	AggregatorTableCapacity = 1 << 16                     // 65,536 hash table slots for opportunity deduplication and storage
	AggregatorTableMask     = AggregatorTableCapacity - 1 // 65,535 bitmask for hash table indexing operations

	// Occupancy bitmap configuration for efficient hash table slot management and bulk operations.
	// Bitmap organized as uint64 words, each covering 64 hash table slots for cache-efficient access.
	// This arrangement provides rapid occupancy testing and efficient bulk reset during finalization.
	AggregatorBitmapWords = 1 << 10 // 1,024 uint64 words for hash table occupancy tracking (64 slots per word)

	// Profitability quantization system for dual-layer priority queue mapping and opportunity sorting.
	// Mathematical constants for converting floating-point profitability values to integer priorities.
	AggregatorProfitabilityClampingBound = 192.0                                                                        // Input range limit for profitability quantization: [-192, +192]
	AggregatorMaxPriorityTick            = 127                                                                          // Maximum priority tick value for queue operations
	AggregatorProfitabilityScale         = (AggregatorMaxPriorityTick - 1) / (2 * AggregatorProfitabilityClampingBound) // Scale factor: 126 / 384

	// Liquidity stratification system based on reserve leading zero analysis for execution risk assessment.
	// Maximum stratum count covers theoretical range of leading zeros in uint256 reserve values.
	// Each stratum represents a distinct liquidity tier: fewer leading zeros = higher liquidity = lower execution risk.
	AggregatorMaxStrata = 96 // Liquidity tier count covering full theoretical hex leading zero range (3 pairs × 32 hex chars)

	// Bundle extraction timing configuration for consistent finalization across diverse hardware configurations.
	// Target timing ensures regular opportunity extraction while preventing premature finalization during active periods.
	AggregatorTargetFinalizationTime = 1 * time.Millisecond // Target time interval for consistent finalization timing

	// Execution bundle size optimization balancing transaction efficiency with gas constraints and inclusion probability.
	// Bundle size represents optimal balance between gas limit utilization, MEV extraction efficiency, and block inclusion risk.
	AggregatorMaxBundleSize = 32 // Maximum opportunities per execution bundle (approximately 12M gas utilization)
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TIMING PROFILES AND ACTIVE CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Virtual timing system configuration for syscall-free coordination and activity detection.
	// Poll rate calibrated for Apple M4 Pro architecture with balanced performance and power efficiency.
	ControlActivePollRate   = uint64(3_200_000_000) // 3.2 billion polls per second (Apple M4 Pro optimized)
	ControlActiveCooldownMs = uint64(500)           // 500ms cooldown period for activity detection

	// Hardware-specific timing profiles for different processor architectures and performance targets.
	// Poll rates calibrated through empirical testing on representative hardware configurations.
	ControlPollRateConservative = uint64(2_000_000_000) // 2.0 GHz equivalent timing for power-constrained systems
	ControlPollRateTypical      = uint64(3_500_000_000) // 3.5 GHz equivalent timing for standard desktop processors
	ControlPollRateAggressive   = uint64(4_500_000_000) // 4.5 GHz equivalent timing for high-performance processors
	ControlPollRateAppleSilicon = uint64(3_200_000_000) // Apple Silicon optimized timing (M1/M2/M3/M4 series)
	ControlPollRateServer       = uint64(2_800_000_000) // Server-optimized timing with thermal consideration

	// Cooldown duration options for different activity detection sensitivity requirements.
	// Shorter cooldowns provide faster response to inactivity, longer cooldowns reduce false positives.
	ControlCooldownMs50   = uint64(50)   // 50ms ultra-responsive cooldown for latency-critical applications
	ControlCooldownMs100  = uint64(100)  // 100ms fast cooldown for real-time trading systems
	ControlCooldownMs250  = uint64(250)  // 250ms balanced cooldown for general high-frequency operations
	ControlCooldownMs500  = uint64(500)  // 500ms standard cooldown for arbitrage detection (default)
	ControlCooldownMs1000 = uint64(1000) // 1000ms conservative cooldown for batch processing systems
	ControlCooldownMs2000 = uint64(2000) // 2000ms long cooldown for low-frequency monitoring applications
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DEPLOYMENT PROFILES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Hardware deployment profile identifiers for system configuration management.
	// Profile strings used for automated hardware detection and performance optimization.
	ConfigIntelConservative = "intel_2ghz"   // Conservative Intel configuration for power-constrained environments
	ConfigIntelTypical      = "intel_3_5ghz" // Standard Intel desktop configuration (3.5GHz+ processors)
	ConfigIntelAggressive   = "intel_4_5ghz" // High-performance Intel configuration (4.5GHz+ processors)
	ConfigAppleSilicon      = "apple_m4_pro" // Apple Silicon configuration (M1/M2/M3/M4 series processors)
	ConfigARMServer         = "arm_server"   // ARM server configuration (AWS Graviton, etc.)
	ConfigLowLatency        = "low_latency"  // Ultra-low latency configuration for high-frequency trading
	ConfigBatteryOptimized  = "battery_opt"  // Battery-optimized configuration for portable systems
	ConfigServerOptimized   = "server_opt"   // Server-optimized configuration for sustained throughput
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
	// CACHE LINE 1: Primary cooldown calculations (accessed every poll cycle during coordination)
	// Virtual timing calculations derived from active configuration for syscall-free coordination.
	ControlCooldownPolls       = (ControlActiveCooldownMs * ControlActivePollRate) / 1000 // 8B - Nuclear hot: main cooldown calculation
	ControlCooldownPolls500ms  = (ControlCooldownMs500 * ControlActivePollRate) / 1000    // 8B - Hot: 500ms variant calculation
	ControlCooldownPolls250ms  = (ControlCooldownMs250 * ControlActivePollRate) / 1000    // 8B - Hot: 250ms variant calculation
	ControlCooldownPolls100ms  = (ControlCooldownMs100 * ControlActivePollRate) / 1000    // 8B - Warm: 100ms variant calculation
	ControlCooldownPolls50ms   = (ControlCooldownMs50 * ControlActivePollRate) / 1000     // 8B - Warm: 50ms variant calculation
	ControlCooldownPolls1000ms = (ControlCooldownMs1000 * ControlActivePollRate) / 1000   // 8B - Cold: 1000ms variant calculation
	ControlCooldownPolls2000ms = (ControlCooldownMs2000 * ControlActivePollRate) / 1000   // 8B - Cold: 2000ms variant calculation
	_                          [8]byte                                                    // 8B - Padding to complete cache line alignment
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
	// CACHE LINE 1: Most frequently accessed JSON field detection patterns (nuclear hot during parsing)
	// 8-byte signature patterns for zero-allocation field identification in Ethereum JSON-RPC log objects.
	ParserKeyAddress        = [8]byte{'"', 'a', 'd', 'd', 'r', 'e', 's', 's'} // 8B - Contract address field identifier
	ParserKeyData           = [8]byte{'"', 'd', 'a', 't', 'a', '"', ':', '"'} // 8B - Event data field identifier
	ParserKeyTopics         = [8]byte{'"', 't', 'o', 'p', 'i', 'c', 's', '"'} // 8B - Topics array field identifier
	ParserKeyBlockNumber    = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'N', 'u'} // 8B - Block number field identifier
	ParserKeyBlockHash      = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'H', 'a'} // 8B - Block hash field identifier
	ParserKeyBlockTimestamp = [8]byte{'"', 'b', 'l', 'o', 'c', 'k', 'T', 'i'} // 8B - Block timestamp field identifier
	ParserKeyLogIndex       = [8]byte{'"', 'l', 'o', 'g', 'I', 'n', 'd', 'e'} // 8B - Log index field identifier
	ParserKeyTransaction    = [8]byte{'"', 't', 'r', 'a', 'n', 's', 'a', 'c'} // 8B - Transaction field identifier

	// CACHE LINE 2: Less frequently accessed patterns and event signatures (warm during parsing)
	ParserKeyRemoved    = [8]byte{'"', 'r', 'e', 'm', 'o', 'v', 'e', 'd'} // 8B - Removed flag field identifier (chain reorg detection)
	ParserSigSyncPrefix = [8]byte{'1', 'c', '4', '1', '1', 'e', '9', 'a'} // 8B - Uniswap V2 Sync event signature prefix
	_                   [48]byte                                          // 48B - Padding to complete cache line alignment
)
