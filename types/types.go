// ════════════════════════════════════════════════════════════════════════════════════════════════
// Zero-Copy Event Data Structures and Arbitrage Types
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Core Data Types and Message Structures
//
// Description:
//   Defines zero-copy data structures for Ethereum event processing and arbitrage opportunity
//   management. All message types designed for high-frequency operation with cache-optimized
//   memory layouts and zero-allocation processing requirements.
//
// Features:
//   - Zero-allocation event processing with direct buffer references
//   - Cache-aligned structures optimized for multi-core access patterns
//   - Ring56-compatible message formats for lock-free inter-core communication
//   - Comprehensive lifetime safety documentation for zero-copy operations
//   - Database schema integration for trading pair identification
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package types

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TRADING PAIR IDENTIFICATION SYSTEM
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// TradingPairID uniquely identifies Uniswap V2/V3 liquidity pools across the arbitrage detection
// system. Each identifier corresponds to a specific liquidity pool contract deployed on Ethereum,
// enabling efficient cycle identification and execution path planning without address operations.
//
// Database Schema Integration:
//
//	Primary Key: pools.id (auto-incrementing) → TradingPairID value
//	Pool Address: pools.pool_address → Actual contract address on Ethereum
//	Token Composition: pools.token0_id, pools.token1_id → Constituent token references
//	Fee Structure: pools.fee_bps → Trading fee in basis points (V2: 30bps, V3: variable)
//	Exchange Classification: pools.exchange_id → Uniswap version identification
//
// Operational Examples:
//   - ETH/DAI V2 Pool: 0xa478c2975ab1ea89e8196811f51a7b7ade33eb11 (30 basis points)
//   - USDC/WETH V3 Pool: 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640 (5 basis points)
//   - DAI/USDC V3 Pool: 0x5777d92f208679db4b9778590fa3cab3ac9e2168 (1 basis point)
//
// Performance Optimization:
//
//	Sequential numeric identifiers enable efficient array indexing, cache-friendly memory access,
//	and rapid comparison operations compared to 20-byte Ethereum addresses. Database-assigned
//	identifiers provide consistent numbering across system restarts and configuration changes.
//
// Integration Requirements:
//   - Router: Uses TradingPairID for cycle construction and price update routing
//   - Aggregator: Uses TradingPairID for opportunity deduplication and priority management
//   - Execution: Uses TradingPairID for transaction construction and gas optimization
type TradingPairID uint64

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ARBITRAGE OPPORTUNITY MESSAGE STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ArbitrageOpportunity represents a profitable three-pair arbitrage cycle detected by router cores.
// Structure precisely sized to 56 bytes for ring56 compatibility, enabling zero-copy message
// transmission between router cores and aggregator without serialization overhead or allocation.
//
// Arbitrage Cycle Mathematics:
//
//	Complete arbitrage involves exactly three trading pairs forming a closed economic loop.
//	Profitability calculation aggregates logarithmic price ratios across all constituent pairs,
//	with negative values indicating profitable execution opportunities in log-space representation.
//
//	Example Cycle: ETH → DAI → USDC → ETH
//	  Pair 1: ETH/DAI   (exchange ETH for DAI at current market rate)
//	  Pair 2: DAI/USDC  (exchange DAI for USDC at current market rate)
//	  Pair 3: USDC/ETH  (exchange USDC for ETH at current market rate)
//
// Dual-Layer Priority System:
//
//	Primary Sorting: leadingZeroCount (execution risk assessment via liquidity analysis)
//	Secondary Sorting: totalProfitability (reward optimization within risk tolerance)
//
//	This hierarchy ensures execution safety precedence over pure profit maximization,
//	reducing slippage risk and improving execution success probability in volatile markets.
//
// Memory Layout Optimization:
//
//	Field ordering optimized for aggregator access patterns during high-frequency processing.
//	Cycle identification data placed first for rapid hash computation and deduplication,
//	followed by prioritization data for efficient queue management operations.
//
// Message Lifetime Management:
//
//	Ring56 messages maintain validity only during single processing cycle. Recipients must
//	extract required data immediately upon receipt without storing message references across
//	processing boundaries or goroutine transitions.
//
//go:notinheap
type ArbitrageOpportunity struct {
	// Cycle identification array defining the three constituent trading pairs.
	// Pairs stored in canonical forward direction regardless of optimal execution direction
	// to ensure consistent hash computation for deduplication across router cores.
	//
	// Canonical Representation Benefits:
	//   - Identical cycles produce identical hash values regardless of detection core
	//   - Efficient XOR-based hash computation with commutative property exploitation
	//   - Replacement-based collision handling maintains most recent opportunity data
	//
	// Storage Format: [primaryPair, secondaryPair, tertiaryPair]
	// Hash Computation: XOR(pair0, pair1, pair2) & mask for O(1) deduplication
	CyclePairs [3]TradingPairID // 24 bytes - Three trading pairs forming arbitrage cycle

	// Liquidity assessment derived from reserve leading zero analysis during price parsing.
	// Leading zero count provides instant liquidity approximation without additional on-chain queries,
	// enabling real-time execution risk assessment and slippage prediction capabilities.
	//
	// Technical Derivation:
	//   - Reserve values parsed as hex strings during Uniswap event processing
	//   - Leading zeros indicate magnitude: fewer zeros = larger reserves = higher liquidity
	//   - Range covers theoretical uint256 scope (96 hex chars) though reserves use uint112
	//
	// Risk Assessment Mapping:
	//   - Lower values: Higher liquidity, lower execution risk, preferred for large trades
	//   - Higher values: Lower liquidity, higher execution risk, suitable for small trades only
	//
	// Aggregator Integration:
	//   Used for stratum selection in dual-layer priority system, ensuring liquidity-first
	//   opportunity extraction for optimal execution success probability.
	LeadingZeroCount uint64 // 8 bytes - Liquidity tier classification for execution risk management

	// Total profitability calculation representing aggregate logarithmic profit across complete cycle.
	// Value computed by router cores as sum of individual pair logarithmic price ratios,
	// providing execution-ready profit assessment in log-space representation.
	//
	// Mathematical Foundation:
	//   - Individual pair ratios: log(reserveOut/reserveIn) for each trading pair
	//   - Cycle profitability: sum of all three pair ratios in execution sequence
	//   - Profitable threshold: negative values indicate positive execution profit potential
	//
	// Value Range and Scaling:
	//   - Theoretical range: -192.0 to +192.0 (3 pairs × 64-bit log ratio = 192)
	//   - Practical range: Constrained by realistic reserve ratios and market conditions
	//   - Resolution: Sufficient precision for basis-point profit margin detection
	//
	// Priority Queue Integration:
	//   Transformed to priority queue coordinates via linear scaling for efficient sorting.
	//   Within-stratum ordering ensures most profitable opportunities selected first while
	//   maintaining liquidity-based execution risk management.
	TotalProfitability float64 // 8 bytes - Logarithmic profit calculation for priority ordering

	// Execution direction optimization flag indicating optimal trading sequence direction.
	// Direction determination performed by router cores based on comparative profitability
	// analysis between forward and reverse execution paths for the identified cycle.
	//
	// Direction Semantics:
	//   - false: Execute forward sequence following cyclePairs array order
	//   - true: Execute reverse sequence opposite to cyclePairs array order
	//
	// Economic Rationale:
	//   Market conditions may favor one direction over another due to:
	//   - Asymmetric liquidity distribution across pairs
	//   - Directional trading pressure affecting individual pair rates
	//   - Gas optimization considerations for specific execution sequences
	//
	// Execution Planning:
	//   Flag preserved through aggregation pipeline to execution systems for
	//   optimal transaction construction and gas consumption minimization.
	IsReverseDirection bool // 1 byte - Optimal execution direction for profit maximization

	// Memory padding to achieve precise 56-byte structure size for ring56 compatibility.
	// Ring56 protocol requires exact message size alignment for optimal performance and
	// zero-copy message transmission between router cores and aggregator components.
	//
	// Protocol Requirements:
	//   - Exact 56-byte alignment for ring buffer slot compatibility
	//   - Cache line consideration: 56 bytes = 7/8 of 64-byte cache line
	//   - Memory efficiency: Minimizes padding overhead while maintaining alignment
	_ [15]byte // 15 bytes - Protocol alignment padding for ring56 message compatibility
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ETHEREUM EVENT PROCESSING INFRASTRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// LogView provides zero-copy access to decoded Ethereum event data from WebSocket streams.
// Structure designed for high-frequency event processing where memory allocation elimination
// is critical for maintaining consistent low-latency performance characteristics.
//
// Zero-Copy Architecture:
//
//	All byte slice fields reference underlying WebSocket buffer memory directly without
//	intermediate copying or allocation. This approach eliminates garbage collection pressure
//	and memory bandwidth consumption during high-frequency event processing.
//
// Cache Line Optimization Strategy:
//
//	Memory layout organized into three 64-byte cache lines based on empirical access patterns:
//	- Line 1: Event identification and routing data (accessed every event)
//	- Line 2: Event payload and block context (accessed during processing)
//	- Line 3: Transaction correlation data (accessed during analysis phases)
//
// Lifetime Safety Protocol:
//
//	CRITICAL: All slice references become invalid when WebSocket buffer reuse occurs.
//	Violation of lifetime constraints results in undefined behavior and potential crashes.
//
//	Safe Usage Pattern:
//	  1. Receive LogView from WebSocket decoder within single processing frame
//	  2. Extract required data immediately using dedicated parsing functions
//	  3. Discard LogView reference before returning control to decoder system
//	  4. Never store LogView or constituent slices beyond processing boundary
//
//	Prohibited Operations:
//	  - Storing LogView in data structures beyond current processing scope
//	  - Passing LogView across goroutine boundaries via channels or shared memory
//	  - Retaining slice references in maps, arrays, or persistent structures
//	  - Accessing LogView fields after WebSocket buffer reuse events
//
//go:notinheap
//go:align 64
type LogView struct {
	// ╔═══════════════════════════════════════════════════════════════════╗
	// ║ CACHE LINE 1: EVENT IDENTIFICATION AND ROUTING (64 bytes)        ║
	// ║ Primary access data for event classification and system routing   ║
	// ╚═══════════════════════════════════════════════════════════════════╝

	// Contract address identifying the Ethereum smart contract that emitted this event.
	// Address format: "0x" prefix followed by 40 lowercase hexadecimal characters
	// representing the 20-byte Ethereum contract address in standard encoding.
	//
	// Routing Applications:
	//   - Primary key for trading pair identification and system routing decisions
	//   - Database lookup key for TradingPairID resolution and configuration retrieval
	//   - Event filtering criteria for relevant contract monitoring and processing
	//
	// Example: "0x882df4b0fb50a229c3b4124eb18c759911485bfb" (Uniswap V2 pair contract)
	// Access Pattern: Read once per event for routing determination and pair identification
	Addr []byte // 24 bytes - Contract address for event routing and pair identification

	// Indexed event parameters containing event signature and additional indexed data.
	// Format: JSON array structure with brackets removed during parsing optimization,
	// containing comma-separated hexadecimal event signature and parameter values.
	//
	// Uniswap V2 Sync Event Structure:
	//   - Single topic: event signature hash for Sync event identification
	//   - Signature: "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
	//
	// Processing Optimization:
	//   Brackets pre-removed during WebSocket parsing to eliminate processing overhead
	//   during high-frequency event handling and signature verification operations.
	//
	// Access Pattern: Read once per event for event type classification and validation
	Topics []byte // 24 bytes - Event signature and indexed parameters for classification

	// Upper 64 bits of 128-bit event fingerprint for global event identification.
	// Fingerprint computed from cryptographic hash of block number, transaction index,
	// and log index to create globally unique event identifier across chain reorganizations.
	//
	// Deduplication Applications:
	//   - Event uniqueness verification across multiple WebSocket connections
	//   - Chain reorganization detection and event replay prevention
	//   - Distributed system coordination for event processing guarantees
	//
	// Persistence Characteristics:
	//   Fingerprint remains valid after WebSocket buffer reuse, enabling safe storage
	//   for deduplication and event tracking across processing boundaries.
	TagHi uint64 // 8 bytes - Upper fingerprint bits for global event identification

	// Lower 64 bits of 128-bit event fingerprint completing unique identification.
	// Combined with TagHi provides 128-bit identifier space with negligible collision
	// probability for practical event processing volumes and timeframes.
	//
	// Mathematical Properties:
	//   - 128-bit space: 2^128 ≈ 3.4×10^38 possible values
	//   - Collision probability: Negligible for expected event processing volumes
	//   - Cryptographic derivation: Ensures uniform distribution and unpredictability
	TagLo uint64 // 8 bytes - Lower fingerprint bits completing unique identification

	// Total cache line 1: 24 + 24 + 8 + 8 = 64 bytes (exactly one cache line)

	// ╔═══════════════════════════════════════════════════════════════════╗
	// ║ CACHE LINE 2: EVENT PAYLOAD AND BLOCK CONTEXT (64 bytes)         ║
	// ║ Event content and blockchain context for processing and analysis  ║
	// ╚═══════════════════════════════════════════════════════════════════╝

	// Non-indexed event parameters containing the primary event payload data.
	// Format: "0x" prefix followed by hexadecimal encoding of ABI-encoded parameter data
	// with variable length depending on event parameter types and values.
	//
	// Uniswap V2 Sync Event Payload Structure:
	//   - Two 256-bit values: reserve0 and reserve1 representing current pool reserves
	//   - Total length: 130 characters ("0x" + 128 hex characters = 64 bytes data)
	//   - Encoding: Standard Ethereum ABI encoding with 32-byte parameter alignment
	//
	// Example: "0x0000000000000000000000000000000000000000007c34bdf6cfe2d5772d68d1
	//               0000000000000000000000000000000000000000000000000020dfffa7b4a402"
	//
	// Processing Applications:
	//   - Reserve value extraction for price calculation and arbitrage detection
	//   - Parameter validation for event authenticity and format verification
	//   - Economic analysis for liquidity assessment and market monitoring
	Data []byte // 24 bytes - Event payload for parameter extraction and analysis

	// Block number indicating blockchain height where this event was emitted.
	// Format: "0x" prefix followed by variable-length hexadecimal encoding of
	// block number value without leading zeros for storage efficiency.
	//
	// Temporal Ordering Applications:
	//   - Event chronological ordering for time-series analysis and processing
	//   - Chain reorganization detection through block number consistency verification
	//   - Market timing analysis for price movement correlation and causation studies
	//
	// Example: "0x468dcc3" representing block 74,185,923 in decimal notation
	// Processing: Converted to uint64 for numerical comparison and ordering operations
	BlkNum []byte // 24 bytes - Block height for temporal ordering and reorganization detection

	// Memory alignment padding to complete cache line boundary alignment.
	// Ensures subsequent cache line begins at optimal memory boundary for
	// predictable cache performance and memory access pattern optimization.
	_ [16]byte // 16 bytes - Cache line completion padding for memory alignment

	// Total cache line 2: 24 + 24 + 16 = 64 bytes (exactly one cache line)

	// ╔═══════════════════════════════════════════════════════════════════╗
	// ║ CACHE LINE 3: TRANSACTION CORRELATION METADATA (64 bytes)        ║
	// ║ Additional context for transaction analysis and correlation       ║
	// ╚═══════════════════════════════════════════════════════════════════╝

	// Log index representing the position of this event within its containing block.
	// Format: "0x" prefix followed by variable-length hexadecimal encoding without
	// leading zeros representing zero-based index within block event sequence.
	//
	// Event Ordering Applications:
	//   - Intra-block event ordering for causation analysis and dependency tracking
	//   - Event uniqueness verification when combined with block number
	//   - Market microstructure analysis for atomic transaction effect measurement
	//
	// Example: "0x2bf" representing the 703rd event within the containing block
	// Combined Identifier: (BlkNum, LogIdx) provides unique event identification
	LogIdx []byte // 24 bytes - Intra-block event position for ordering and identification

	// Transaction index representing the position of the containing transaction
	// within its block, enabling transaction-level correlation and analysis.
	//
	// Transaction Correlation Applications:
	//   - Event grouping by originating transaction for atomic operation analysis
	//   - Gas consumption analysis for transaction efficiency measurement
	//   - MEV detection through transaction composition and ordering analysis
	//
	// Example: "0x9e" representing the 158th transaction within the containing block
	// Analysis Applications: Transaction-level event aggregation and correlation studies
	TxIndex []byte // 24 bytes - Transaction position for correlation and grouping analysis

	// Final cache line padding preventing false sharing between LogView instances.
	// Ensures each LogView structure occupies exactly three complete cache lines
	// for optimal memory access patterns during concurrent multi-core processing.
	//
	// False Sharing Prevention:
	//   - Eliminates cache line contention between concurrent LogView accesses
	//   - Ensures predictable cache performance under multi-core processing loads
	//   - Optimizes memory bandwidth utilization for high-frequency event processing
	_ [16]byte // 16 bytes - False sharing prevention and structure completion padding

	// Total cache line 3: 24 + 24 + 16 = 64 bytes (exactly one cache line)
	// Overall structure size: 192 bytes (exactly three cache lines)
}
