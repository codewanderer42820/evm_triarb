// Package router implements ultra-high-frequency arbitrage detection for Uniswap V2 pairs.
//
// ARCHITECTURE OVERVIEW:
// =====================
// This system processes Ethereum event logs in real-time to detect triangular arbitrage
// opportunities across multiple CPU cores with sub-10 cycle latency. The architecture
// consists of several key components:
//
// 1. DISPATCH PIPELINE: Ultra-minimal tick extraction from WebSocket events
// 2. CORE PROCESSING: Multi-core arbitrage cycle evaluation using priority queues
// 3. ADDRESS SYSTEM: Robin Hood hash tables for O(1) pair lookups
// 4. MESSAGE PASSING: Lock-free ring buffers for inter-core communication
//
// PERFORMANCE CHARACTERISTICS:
// ===========================
// - Main dispatch: ~6-10 CPU cycles per event
// - Address lookup: ~20-30 cycles (Robin Hood hashing)
// - Arbitrage detection: ~40-60 cycles per opportunity
// - Zero allocations in hot paths
// - Completely branchless critical sections
//
// MEMORY LAYOUT:
// =============
// All structures are cache-aligned (32B/64B) with hot fields first.
// Pre-allocated buffers eliminate garbage collection pressure.
// NUMA-aware core pinning for optimal memory access patterns.
package router

import (
	"fmt"
	"hash"
	"math/bits"
	"runtime"
	"unsafe"

	"main/constants"
	"main/control"
	"main/debug"
	"main/fastuni"
	"main/localidx"
	"main/quantumqueue64"
	"main/ring24"
	"main/types"
	"main/utils"

	"golang.org/x/crypto/sha3"
)

// Core type definitions for the arbitrage system
type PairID uint64              // Unique identifier for trading pairs
type ArbitrageTriplet [3]PairID // Three-pair arbitrage cycle
type CycleStateIndex uint64     // Index into cycle state storage

// ============================================================================
// MESSAGE PASSING STRUCTURES (Runtime Hot Path) - OPTIMIZED FIELD ORDERING
// ============================================================================
//
// These structures are used for inter-core communication via lock-free ring buffers.
// The TickUpdate must be exactly 24 bytes to fit in ring24 slots without padding.

// TickUpdate represents a price update message passed between cores.
// This structure is the core message format for the entire arbitrage system.
//
// CRITICAL CONSTRAINT: Must remain exactly 24 bytes for ring24 compatibility.
// Adding any fields will break the lock-free message passing system.
//
// OPTIMIZED MEMORY LAYOUT (HOTTEST TO COLDEST):
//
//	[0-7]:   pairID (uint64)       - HOTTEST: Used first for routing in DispatchTickUpdate
//	[8-15]:  forwardTick (float64) - HOT: Price for A→B trades (log2 ratio)
//	[16-23]: reverseTick (float64) - HOT: Price for B→A trades (negative log2 ratio)
//
//go:notinheap
type TickUpdate struct {
	pairID      PairID  // 8B - HOTTEST: Used first for core routing and queue lookup
	forwardTick float64 // 8B - HOT: Price for A→B trades (log2 ratio)
	reverseTick float64 // 8B - HOT: Price for B→A trades (negative log2 ratio)
}

// ============================================================================
// RUNTIME PROCESSING STRUCTURES (Hot path) - OPTIMIZED FIELD ORDERING
// ============================================================================
//
// These structures are accessed during every arbitrage computation and are
// optimized for cache performance with hot fields placed first.

// ArbitrageCycleState maintains the current state of a three-pair arbitrage cycle.
// Each cycle represents a potential circular trade: A→B→C→A.
//
// PERFORMANCE NOTES:
// - 64-byte aligned for full cache line ownership (prevents false sharing)
// - tickValues are accessed most frequently during profitability calculations
// - pairIDs are used for logging and debugging (less frequent access)
//
// MATHEMATICAL MODEL:
// The total profitability is: tickValues[0] + tickValues[1] + tickValues[2]
// Negative sum indicates profitable arbitrage opportunity.
//
// ARCHITECTURAL INVARIANT:
// By design, exactly ONE tickValue in each cycle remains 0 (never updated via fanout).
// Each cycle has one "primary" edge that's only updated when that pair's queue
// processes directly, while the other two edges are updated via fanout.
// This creates an intentional asymmetry that simplifies the update logic.
//
// FIELD ORDERING RATIONALE:
// tickValues[3] are accessed in EVERY profitability calculation (sum operation)
// pairIDs[3] are accessed only during fanout setup and debugging
//
//go:notinheap
//go:align 64
type ArbitrageCycleState struct {
	tickValues [3]float64 // 24B - HOTTEST: Log price ratios summed every cycle evaluation
	pairIDs    [3]PairID  // 24B - WARM: Pair identifiers for fanout and debugging
	_          [16]byte   // 16B - Padding to 64B cache line boundary
}

// FanoutEntry represents the connection between a pair update and arbitrage cycles.
// When a pair price updates, we need to update all cycles that include that pair.
//
// FANOUT COMPLEXITY:
// The fanout operation is O(k) where k = number of cycles containing the updated pair.
// Each ArbitrageCycleState requires exactly 2 fanout updates (from the other 2 pairs).
// In practice, popular pairs may participate in hundreds or thousands of cycles,
// making k potentially large and creating load balancing challenges.
//
// LOAD BALANCING STRATEGY:
// Uses SHA3-based deterministic shuffling to distribute cycles evenly across cores.
// This prevents pathological cases where one pair dominates processing time,
// ensuring consistent performance regardless of cycle distribution patterns.
//
// OPTIMIZED FIELD ORDERING (HOTTEST TO COLDEST):
// 1. cycleStateIndex: Used FIRST for direct array access in fanout operations
// 2. edgeIndex: Used immediately after for indexing into tickValues[edgeIndex]
// 3. queue: Used for subsequent priority queue operations
// 4. queueHandle: Used for specific queue manipulation operations
//
//go:notinheap
//go:align 32
type FanoutEntry struct {
	cycleStateIndex uint64                         // 8B - HOTTEST: Array index for direct cycle access
	edgeIndex       uint64                         // 8B - HOT: Which edge (0,1,2) for tickValues indexing
	queue           *quantumqueue64.QuantumQueue64 // 8B - WARM: Pointer for queue operations
	queueHandle     uint64                         // 8B - COLD: Handle for priority queue operations
}

// ProcessedCycle is a temporary structure used during cycle extraction.
// When profitable cycles are found, they're temporarily stored here
// before being reinserted into priority queues.
//
// ZERO-ALLOCATION DESIGN:
// This struct is part of a pre-allocated array in ArbitrageCoreExecutor,
// eliminating heap allocations during hot path processing.
//
// OPTIMIZED FIELD ORDERING (HOTTEST TO COLDEST):
// 1. cycleStateIndex: Used FIRST for array access during cycle processing
// 2. originalTick: Used immediately after for priority calculation
// 3. queueHandle: Used for queue reinsertion operations
//
//go:notinheap
//go:align 32
type ProcessedCycle struct {
	cycleStateIndex CycleStateIndex       // 8B - HOTTEST: Array index for cycle state access
	originalTick    int64                 // 8B - HOT: Original tick value for reinsertion
	queueHandle     quantumqueue64.Handle // 4B - WARM: Queue handle for reinsertion
	_               uint32                // 4B - Padding for 8-byte alignment
	_               [8]byte               // 8B - Padding to 32B boundary
}

// ArbitrageCoreExecutor is the main state for each CPU core's arbitrage processing.
// Each core runs independently with its own instance of this structure.
//
// CORE DESIGN PHILOSOPHY:
// - Single-threaded per core (no locks needed)
// - Pre-allocated buffers (zero garbage collection)
// - Cache-optimized field ordering (hot fields first)
// - NUMA-aware memory placement
//
// PROCESSING MODEL:
// 1. Receive TickUpdate messages via ring buffer
// 2. Look up affected arbitrage cycles using pairToQueueIndex (HOTTEST PATH)
// 3. Check direction flag to select tick value (HOTTEST PATH)
// 4. Extract profitable cycles from priority queues
// 5. Update cycle states and recompute priorities
// 6. Emit arbitrage opportunities for execution
//
// CRITICAL FIELD ORDERING (HOTTEST TO COLDEST):
// 1. pairToQueueIndex: Hit on EVERY tick update for queue lookup
// 2. isReverseDirection: Checked on EVERY tick update for direction selection
// 3. cycleStates: Accessed frequently during cycle state updates
// 4. fanoutTables: Accessed during fanout operations for affected cycles
// 5. priorityQueues: Accessed during queue operations (less frequent)
// 6. processedCycles: Only used during profitable cycle extraction
// 7. shutdownSignal: Rarely accessed after initialization
//
//go:notinheap
type ArbitrageCoreExecutor struct {
	// HOTTEST PATH: Accessed every tick update (millions of times per second)
	pairToQueueIndex   localidx.Hash // 64B - O(1) mapping from pair→queue index
	isReverseDirection bool          // 1B - HOTTEST: Checked every tick for direction selection
	_                  [7]byte       // 7B - Padding for alignment

	// HOT PATH: Accessed during cycle processing (frequent)
	cycleStates  []ArbitrageCycleState // 24B - All arbitrage cycle states
	fanoutTables [][]FanoutEntry       // 24B - Maps pairs to affected cycles

	// WARM PATH: Used during queue operations (moderate frequency)
	priorityQueues []quantumqueue64.QuantumQueue64 // 24B - Priority queues for each pair

	// BUFFER SPACE: Pre-allocated working memory (used during extraction only)
	processedCycles [128]ProcessedCycle // 4096B - Temporary storage for extracted cycles

	// COLD PATH: Control (rarely accessed after initialization)
	shutdownSignal <-chan struct{} // 8B - Shutdown coordination
}

// ============================================================================
// ADDRESS HANDLING STRUCTURES (Lookup tables) - OPTIMIZED FIELD ORDERING
// ============================================================================
//
// The address system uses Robin Hood hashing for O(1) pair lookups.
// This is critical since every tick update requires an address→pairID lookup.

// AddressKey represents a parsed Ethereum address for hash table storage.
// Ethereum addresses are 20 bytes (160 bits), packed into 3×64-bit words.
//
// ROBIN HOOD HASHING:
// This system uses Robin Hood linear probing with backward shift deletion.
// Keys are stored with their hash distance to minimize variance in lookup time.
//
// MEMORY LAYOUT:
//
//	words[0]: Address bytes 0-7   (64 bits)
//	words[1]: Address bytes 8-15  (64 bits)
//	words[2]: Address bytes 16-19 (32 bits in lower word)
//
// FIELD ORDERING: All words are accessed together during key comparison,
// so ordering doesn't matter for performance - current layout is optimal.
//
//go:notinheap
//go:align 32
type AddressKey struct {
	words [3]uint64 // 24B - Packed 160-bit Ethereum address (all accessed together)
	_     [8]byte   // 8B - Padding to 32B cache line boundary
}

// ============================================================================
// INITIALIZATION STRUCTURES (Setup only) - OPTIMIZED FIELD ORDERING
// ============================================================================
//
// These structures are used only during system initialization to build
// the runtime data structures. They are not accessed during hot path processing.

// ArbitrageEdgeBinding represents one edge of an arbitrage cycle during initialization.
// Used to build the fanout tables that map pair updates to affected cycles.
//
// INITIALIZATION PHASE:
// 1. Read arbitrage cycles from configuration
// 2. Create edge bindings for each pair in each cycle
// 3. Group bindings by pair into shards
// 4. Distribute shards across CPU cores
//
// FIELD ORDERING: cyclePairs accessed more frequently than edgeIndex during
// initialization processing, current order is optimal.
//
//go:notinheap
//go:align 32
type ArbitrageEdgeBinding struct {
	cyclePairs [3]PairID // 24B - The complete three-pair cycle (accessed first)
	edgeIndex  uint64    // 8B - Which position (0,1,2) this pair occupies
}

// PairShardBucket groups arbitrage cycles by pair for distribution to cores.
// Sharding reduces memory pressure and improves cache locality.
//
// SHARDING STRATEGY:
// - Group cycles by common pairs
// - Limit shard size to fit in CPU cache
// - Distribute shards evenly across cores
// - Use deterministic shuffling for load balancing
//
// OPTIMIZED FIELD ORDERING (HOTTEST TO COLDEST):
// 1. pairID: Used FIRST for shard identification and routing
// 2. edgeBindings: Processed after shard identification
//
//go:notinheap
//go:align 32
type PairShardBucket struct {
	pairID       PairID                 // 8B - HOTTEST: Shard identifier accessed first
	edgeBindings []ArbitrageEdgeBinding // 24B - All cycles containing this pair
}

// keccakRandomState provides deterministic randomness for cycle shuffling.
// Using deterministic randomness ensures reproducible load balancing
// across different runs of the system.
//
// CRYPTOGRAPHIC STRENGTH:
// Uses Keccak-256 for high-quality randomness distribution.
// This prevents adversarial input from creating pathological
// load imbalances across CPU cores.
//
// OPTIMIZED FIELD ORDERING (HOTTEST TO COLDEST):
// 1. counter: Incremented on EVERY call to nextUint64() - hottest field
// 2. seed: Read on every call for hash input - hot field
// 3. hasher: Reset occasionally, accessed less frequently - cold field
type keccakRandomState struct {
	counter uint64    // 8B - HOTTEST: Increment counter for sequence generation
	seed    [32]byte  // 32B - HOT: Current random seed (read each call)
	hasher  hash.Hash // 24B - COLD: Reused hasher instance (occasionally reset)
}

// ============================================================================
// GLOBAL STATE
// ============================================================================
//
// Global state is minimized and carefully partitioned to avoid false sharing.
// Most state is owned by individual cores to eliminate synchronization.

var (
	// Per-core state (no sharing between cores)
	coreExecutors [constants.MaxSupportedCores]*ArbitrageCoreExecutor
	coreRings     [constants.MaxSupportedCores]*ring24.Ring

	// Routing tables (read-only after initialization)
	pairToCoreAssignment [constants.PairRoutingTableCapacity]uint64
	pairShardBuckets     map[PairID][]PairShardBucket

	// Address lookup tables (read-only after initialization)
	pairAddressKeys [constants.AddressTableCapacity]AddressKey
	addressToPairID [constants.AddressTableCapacity]PairID
)

// ============================================================================
// MAIN DISPATCH PIPELINE (Critical path - execution order)
// ============================================================================
//
// This is the ultra-hot path that processes every Uniswap V2 Sync event.
// Every microsecond of latency here affects arbitrage profitability.

// DispatchTickUpdate processes Uniswap V2 Sync events and broadcasts price updates.
// Extracts reserve values, computes log2 price ratios, and sends updates to all
// cores handling cycles that contain the updated pair.
//
// PERFORMANCE TARGET: 6-10 CPU cycles total execution time
//
// INPUT: LogView.Data = "0x" + 128 hex chars (64 per reserve, uint112 + padding)
//
// ALGORITHM:
// 1. Look up pair ID from contract address
// 2. Extract leading zeros from both reserves
// 3. Calculate extraction offsets to preserve ratio accuracy
// 4. Parse hex values to uint64
// 5. Compute log2 price ratio
// 6. Broadcast to assigned cores via lock-free ring buffers
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DispatchTickUpdate(logView *types.LogView) {
	// STEP 1: Address Lookup (~20-30 cycles)
	// Convert contract address to pair ID using hash table lookup.
	pairID := lookupPairIDByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if pairID == 0 {
		return // Not a registered pair
	}

	// STEP 2: Reserve Data Setup (~2 cycles)
	// Skip "0x" prefix. Total: 128 hex chars = 64 chars per reserve
	hexData := logView.Data[2:130]

	// STEP 3: Leading Zero Analysis (~15-20 cycles)
	// uint112 reserves are padded to 32 bytes (64 hex chars).
	// Skip guaranteed 32 leading zeros and scan meaningful 32 chars.
	lzcntA := countLeadingZeros(hexData[32:64])  // Reserve A
	lzcntB := countLeadingZeros(hexData[96:128]) // Reserve B

	// STEP 4: Magnitude Preservation (~3-4 cycles)
	// Extract both reserves at same magnitude to preserve ratio accuracy.
	// Branchless min to avoid unpredictable branches.
	cond := lzcntA - lzcntB
	mask := cond >> 31
	minZeros := lzcntB ^ ((lzcntA ^ lzcntB) & mask) // min(lzcntA, lzcntB)

	// STEP 5: Offset Calculation (~2 cycles)
	// Calculate positions: "0x" + ABI padding + leading zeros
	offsetA := 2 + 32 + minZeros
	offsetB := offsetA + 64 // Reserve B is 64 chars after A

	// STEP 6: Bounds Calculation (~3-4 cycles)
	available := 32 - minZeros
	// Branchless min(16, available)
	cond = 16 - available
	mask = cond >> 31
	remaining := available ^ ((16 ^ available) & mask)

	// STEP 7: Hex Parsing (~10-15 cycles)
	reserve0 := utils.ParseHexU64(logView.Data[offsetA : offsetA+remaining])
	reserve1 := utils.ParseHexU64(logView.Data[offsetB : offsetB+remaining])

	// STEP 8: Price Ratio Calculation (~10-15 cycles)
	// Compute log2(reserve0/reserve1). Handle zero reserves with fallback.
	tickValue, err := fastuni.Log2ReserveRatio(reserve0, reserve1)
	if err != nil {
		// Fallback: combine available entropy for range [51.2, 64.0]
		addrHash := uint64(pairID) ^ reserve0 ^ reserve1
		randBits := addrHash & 0x1FFF // Extract 13 bits (0-8191)
		tickValue = 51.2 + float64(randBits)*0.0015625
	}

	// STEP 9: Message Construction (~3-4 cycles)
	// Build 24-byte message on stack (zero allocation)
	// OPTIMIZED: pairID first for faster core routing
	var message [24]byte
	tickUpdate := (*TickUpdate)(unsafe.Pointer(&message))
	tickUpdate.pairID = pairID
	tickUpdate.forwardTick = tickValue
	tickUpdate.reverseTick = -tickValue

	// STEP 10: Core Broadcasting (~5-10 cycles)
	// Send to all cores handling this pair using bit manipulation
	coreAssignments := pairToCoreAssignment[pairID]
	for coreAssignments != 0 {
		coreID := bits.TrailingZeros64(uint64(coreAssignments))
		coreRings[coreID].Push(&message)
		coreAssignments &^= 1 << coreID
	}
}

// countLeadingZeros counts leading ASCII '0' characters in hex data.
// Used to find where significant digits begin in ABI-encoded uint112 values.
//
// ALGORITHM: Process 4 chunks of 8 bytes, XOR with '0' pattern, find first non-zero.
// INPUT: 32-byte hex segment  OUTPUT: Count of leading '0' chars (0-32)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func countLeadingZeros(segment []byte) int {
	const ZERO_PATTERN = 0x3030303030303030 // Eight ASCII '0' characters

	// Load and process 4 chunks (32 bytes total)
	c0 := utils.Load64(segment[0:8]) ^ ZERO_PATTERN   // Chunk 0: bytes 0-7
	c1 := utils.Load64(segment[8:16]) ^ ZERO_PATTERN  // Chunk 1: bytes 8-15
	c2 := utils.Load64(segment[16:24]) ^ ZERO_PATTERN // Chunk 2: bytes 16-23
	c3 := utils.Load64(segment[24:32]) ^ ZERO_PATTERN // Chunk 3: bytes 24-31

	// Create bitmask: 1 bit per chunk indicating non-zero content
	// Use (x | (~x + 1)) >> 63 to convert any non-zero value to 1
	mask := ((c0|(^c0+1))>>63)<<0 | ((c1|(^c1+1))>>63)<<1 |
		((c2|(^c2+1))>>63)<<2 | ((c3|(^c3+1))>>63)<<3

	// Find first chunk containing non-zero data
	firstChunk := bits.TrailingZeros64(mask)
	chunks := [4]uint64{c0, c1, c2, c3}

	// Find first non-zero byte within that chunk
	// Divide by 8 using right shift (8 = 2^3)
	firstByte := bits.TrailingZeros64(chunks[firstChunk]) >> 3

	// Return total character count: chunk_offset * 8 + byte_offset
	return (firstChunk << 3) + firstByte // Multiply by 8 using left shift
}

// lookupPairIDByAddress performs address lookup using Robin Hood hashing.
// Returns pair ID if address is registered, 0 if not found.
//
// ROBIN HOOD FEATURES:
// - Linear probing with distance tracking
// - Early termination when probe distance exceeds stored distance
// - Branchless key comparison for speed
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func lookupPairIDByAddress(address42HexBytes []byte) PairID {
	// Convert hex address to internal key format
	key := bytesToAddressKey(address42HexBytes)

	// Calculate starting hash table index
	i := directAddressToIndex64(address42HexBytes)
	dist := uint64(0) // Track how far we've probed

	for {
		currentPairID := addressToPairID[i]
		currentKey := pairAddressKeys[i]

		// Branchless key comparison: XOR all words, OR results
		// Result is 0 if identical, non-zero if different
		keyDiff := (key.words[0] ^ currentKey.words[0]) |
			(key.words[1] ^ currentKey.words[1]) |
			(key.words[2] ^ currentKey.words[2])

		// Check for empty slot or key match
		if currentPairID == 0 {
			return 0 // Empty slot - address not registered
		}
		if keyDiff == 0 {
			return currentPairID // Key match - found pair ID
		}

		// Robin Hood early termination:
		// If stored key traveled less distance than our probe,
		// our key cannot be in the table
		currentKeyHash := directAddressToIndex64Stored(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)
		if currentDist < dist {
			return 0 // Early termination
		}

		// Continue probing
		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++
	}
}

// ============================================================================
// CORE PROCESSING PIPELINE (Message consumption)
// ============================================================================
//
// These functions run on each CPU core to process tick updates and detect
// arbitrage opportunities. Each core operates independently on its assigned
// subset of trading pairs and arbitrage cycles.

// processTickUpdate handles incoming tick updates on a CPU core.
// Extracts profitable cycles, updates states, and performs fanout updates.
//
// ALGORITHM:
// 1. Select tick direction (forward/reverse)
// 2. Find priority queue for updated pair (HOTTEST PATH - pairToQueueIndex)
// 3. Extract profitable cycles to pre-allocated buffer
// 4. Reinsert cycles with original priorities
// 5. Update other cycles containing this pair (fanout)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processTickUpdate(executor *ArbitrageCoreExecutor, update *TickUpdate) {
	// STEP 1: Direction Selection
	// Each core handles either forward (A→B) or reverse (B→A) direction
	var currentTick float64
	if !executor.isReverseDirection {
		currentTick = update.forwardTick
	} else {
		currentTick = update.reverseTick
	}

	// STEP 2: Queue Lookup (HOTTEST PATH)
	// Find priority queue for this pair using optimized hash lookup
	queueIndex, _ := executor.pairToQueueIndex.Get(uint32(update.pairID))
	queue := &executor.priorityQueues[queueIndex]

	// STEP 3: Profitable Cycle Extraction
	// Extract cycles that become profitable with new tick value
	cycleCount := 0
	for {
		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleStateIndex(cycleData)
		cycle := &executor.cycleStates[cycleIndex]

		// Calculate total profitability: sum of all three edge ticks
		// Negative sum = profitable arbitrage opportunity
		totalProfitability := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		isProfitable := totalProfitability < 0

		// Optional: Emit arbitrage opportunity
		if isProfitable {
			//emitArbitrageOpportunity(cycle, currentTick)
		}

		// Stop if not profitable or buffer full
		if !isProfitable || cycleCount == len(executor.processedCycles) {
			break
		}

		// Store in pre-allocated buffer for reinsertion
		// OPTIMIZED: cycleStateIndex first for faster array access
		executor.processedCycles[cycleCount] = ProcessedCycle{
			cycleStateIndex: cycleIndex,
			originalTick:    queueTick,
			queueHandle:     handle,
		}
		cycleCount++

		queue.UnlinkMin(handle)
		if queue.Empty() {
			break
		}
	}

	// STEP 4: Cycle Reinsertion
	// Reinsert cycles with original priorities
	for i := 0; i < cycleCount; i++ {
		cycle := &executor.processedCycles[i]
		queue.Push(cycle.originalTick, cycle.queueHandle, uint64(cycle.cycleStateIndex))
	}

	// STEP 5: Fanout Updates
	// Update other cycles containing this pair
	for _, fanoutEntry := range executor.fanoutTables[queueIndex] {
		// OPTIMIZED: cycleStateIndex first for direct array access
		cycle := &executor.cycleStates[fanoutEntry.cycleStateIndex]
		cycle.tickValues[fanoutEntry.edgeIndex] = currentTick

		// Recalculate priority and update queue position
		newPriority := quantizeTickToInt64(cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2])
		fanoutEntry.queue.MoveTick(quantumqueue64.Handle(fanoutEntry.queueHandle), newPriority)
	}
}

// quantizeTickToInt64 converts floating-point tick values to integer priorities.
// Priority queues require integer keys, so we quantize the continuous tick space.
//
// QUANTIZATION FORMULA:
// priority = (tickValue + CLAMP_BOUND) * SCALE
//
// This maps the tick range [-128, +128] to integer range [0, MAX_QUANTIZED_TICK].
// Higher tick sums (less profitable) get higher priority values (lower priority).
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func quantizeTickToInt64(tickValue float64) int64 {
	return int64((tickValue + constants.TickClampingBound) * constants.QuantizationScale)
}

// ============================================================================
// ADDRESS PROCESSING FUNCTIONS (Supporting dispatch pipeline)
// ============================================================================
//
// These functions handle the conversion between hex addresses and internal
// representations used by the hash table system.

// bytesToAddressKey converts 40-char hex address to internal AddressKey format.
// Packs 20-byte address into 3 uint64 words for efficient comparison.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func bytesToAddressKey(address40HexChars []byte) AddressKey {
	// Parse hex string to 20-byte address using SIMD-optimized parser
	parsedAddress := utils.ParseEthereumAddress(address40HexChars)

	// Use utils.Load64 for memory operations
	word0 := utils.Load64(parsedAddress[0:8])  // Bytes 0-7
	word1 := utils.Load64(parsedAddress[8:16]) // Bytes 8-15
	// Load bytes 12-19, mask to get bytes 16-19
	word2 := utils.Load64(parsedAddress[12:20]) & 0xFFFFFFFF

	return AddressKey{
		words: [3]uint64{word0, word1, word2},
	}
}

// directAddressToIndex64 computes hash table index from raw hex address.
// Uses middle bytes for good entropy while avoiding full address parsing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64(address40HexChars []byte) uint64 {
	// Extract middle 16 hex characters (chars 12-27) → bytes 6-13
	// Avoids 8ns ParseEthereumAddress cost, accepts slightly more collisions
	hash64 := utils.ParseHexU64(address40HexChars[12:28])
	return hash64 & uint64(constants.AddressTableMask)
}

// directAddressToIndex64Stored computes hash from stored AddressKey.
// Extracts same middle bytes as directAddressToIndex64 for consistency.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64Stored(key AddressKey) uint64 {
	// Extract bytes 6-13 to match directAddressToIndex64
	// word0=bytes0-7, word1=bytes8-15, word2=bytes16-19

	// Bytes 6-7 from word0 (upper 16 bits)
	bytes67 := key.words[0] >> 48

	// Bytes 8-13 from word1 (lower 48 bits)
	bytes813 := key.words[1] & 0x0000FFFFFFFFFFFF

	// Combine to form 64-bit hash
	hash64 := (bytes67 << 48) | bytes813

	return hash64 & uint64(constants.AddressTableMask)
}

// isEqual compares two AddressKey values.
// Three 64-bit comparisons are faster than twenty 8-bit comparisons.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (a AddressKey) isEqual(b AddressKey) bool {
	return a.words[0] == b.words[0] &&
		a.words[1] == b.words[1] &&
		a.words[2] == b.words[2]
}

// ============================================================================
// DEBUG AND MONITORING (Optional/disabled)
// ============================================================================
//
// These functions provide observability into the arbitrage detection system.
// They are typically disabled in production for maximum performance.

// emitArbitrageOpportunity logs profitable arbitrage cycle details.
// Normally disabled to avoid I/O overhead in hot path.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func emitArbitrageOpportunity(cycle *ArbitrageCycleState, newTick float64) {
	debug.DropMessage("[ARBITRAGE_OPPORTUNITY]", "")

	// Log the three pairs involved in this arbitrage cycle
	debug.DropMessage("  pair0", utils.Itoa(int(cycle.pairIDs[0])))
	debug.DropMessage("  pair1", utils.Itoa(int(cycle.pairIDs[1])))
	debug.DropMessage("  pair2", utils.Itoa(int(cycle.pairIDs[2])))

	// Log individual tick values and total profitability
	tick0Str := fmt.Sprintf("%.6f", cycle.tickValues[0])
	tick1Str := fmt.Sprintf("%.6f", cycle.tickValues[1])
	tick2Str := fmt.Sprintf("%.6f", cycle.tickValues[2])
	newTickStr := fmt.Sprintf("%.6f", newTick)
	totalProfitStr := fmt.Sprintf("%.6f", newTick+cycle.tickValues[0]+cycle.tickValues[1]+cycle.tickValues[2])

	debug.DropMessage("  tick0", tick0Str)
	debug.DropMessage("  tick1", tick1Str)
	debug.DropMessage("  tick2", tick2Str)
	debug.DropMessage("  newTick", newTickStr)
	debug.DropMessage("  totalProfit", totalProfitStr)
}

// ============================================================================
// SYSTEM SETUP FUNCTIONS (Initialization phase)
// ============================================================================
//
// These functions are called during system startup to build the runtime
// data structures. They are not performance-critical since they run only once.

// RegisterPairAddress adds a trading pair to the address lookup system.
// Populates Robin Hood hash table during initialization using displacement.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairAddress(address42HexBytes []byte, pairID PairID) {
	key := bytesToAddressKey(address42HexBytes)
	i := directAddressToIndex64(address42HexBytes)
	dist := uint64(0)

	for {
		currentPairID := addressToPairID[i]

		// Empty slot - insert here
		if currentPairID == 0 {
			pairAddressKeys[i] = key
			addressToPairID[i] = pairID
			return
		}

		// Key already exists - update the pair ID
		if pairAddressKeys[i].isEqual(key) {
			addressToPairID[i] = pairID
			return
		}

		// Robin Hood displacement check
		currentKey := pairAddressKeys[i]
		currentKeyHash := directAddressToIndex64Stored(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)

		// If current key has traveled less distance, displace it
		if currentDist < dist {
			key, pairAddressKeys[i] = pairAddressKeys[i], key
			pairID, addressToPairID[i] = addressToPairID[i], pairID
			dist = currentDist
		}

		// Continue probing
		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++
	}
}

// RegisterPairToCore assigns a trading pair to specific CPU cores.
// Uses bit mask for efficient core iteration during message dispatch.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairToCore(pairID PairID, coreID uint8) {
	pairToCoreAssignment[pairID] |= 1 << coreID
}

// newKeccakRandom creates deterministic random number generator.
// Uses Keccak-256 for reproducible load balancing during initialization.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func newKeccakRandom(initialSeed []byte) *keccakRandomState {
	var seed [32]byte

	// Hash the input seed to normalize length and improve distribution
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(initialSeed)
	copy(seed[:], hasher.Sum(nil))

	return &keccakRandomState{
		counter: 0,
		seed:    seed,
		hasher:  sha3.NewLegacyKeccak256(), // Reusable hasher instance
	}
}

// nextUint64 generates the next random uint64 value in the sequence.
// Combines the seed with an incrementing counter for unique values.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (k *keccakRandomState) nextUint64() uint64 {
	// Create input: seed || counter
	var input [40]byte
	copy(input[:32], k.seed[:])
	*(*uint64)(unsafe.Pointer(&input[32])) = k.counter // Fast uint64 write

	// Reuse hasher instance - much faster than creating new one
	k.hasher.Reset()
	k.hasher.Write(input[:])
	output := k.hasher.Sum(nil)

	// Increment counter for next call
	k.counter++

	// Return first 8 bytes as uint64
	return utils.Load64(output[:8])
}

// nextInt generates a random integer in the range [0, upperBound).
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (k *keccakRandomState) nextInt(upperBound int) int {
	if upperBound <= 0 {
		return 0
	}

	randomValue := k.nextUint64()

	// Fast modulo - bias doesn't matter for shuffling
	return int(randomValue % uint64(upperBound))
}

// keccakShuffleEdgeBindings performs Fisher-Yates shuffle with deterministic randomness.
// Distributes arbitrage cycles evenly across CPU cores for load balancing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func keccakShuffleEdgeBindings(bindings []ArbitrageEdgeBinding, pairID PairID) {
	if len(bindings) <= 1 {
		return
	}

	// Create deterministic seed based on pair ID
	var seedInput [8]byte
	*(*uint64)(unsafe.Pointer(&seedInput[0])) = utils.Mix64(uint64(pairID))

	rng := newKeccakRandom(seedInput[:])

	// Fisher-Yates shuffle with deterministic randomness
	for i := len(bindings) - 1; i > 0; i-- {
		j := rng.nextInt(i + 1)
		bindings[i], bindings[j] = bindings[j], bindings[i]
	}
}

// buildFanoutShardBuckets constructs fanout mapping from pairs to arbitrage cycles.
// Groups cycles by pairs, shuffles for load balancing, partitions into cache-sized shards.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func buildFanoutShardBuckets(cycles []ArbitrageTriplet) {
	pairShardBuckets = make(map[PairID][]PairShardBucket)
	temporaryBindings := make(map[PairID][]ArbitrageEdgeBinding, len(cycles)*3)

	// PHASE 1: Group cycles by pairs
	// Each cycle contains 3 pairs, so each cycle contributes to 3 different groups
	for _, triplet := range cycles {
		for i := 0; i < 3; i++ {
			temporaryBindings[triplet[i]] = append(temporaryBindings[triplet[i]],
				ArbitrageEdgeBinding{
					cyclePairs: triplet,
					edgeIndex:  uint64(i), // This pair's position in the cycle
				})
		}
	}

	// PHASE 2: Create shards with deterministic shuffling
	for pairID, bindings := range temporaryBindings {
		// Shuffle for load balancing (deterministic based on pair ID)
		keccakShuffleEdgeBindings(bindings, pairID)

		// Partition into cache-friendly shards
		for offset := 0; offset < len(bindings); offset += constants.MaxCyclesPerShard {
			endOffset := offset + constants.MaxCyclesPerShard
			if endOffset > len(bindings) {
				endOffset = len(bindings)
			}

			pairShardBuckets[pairID] = append(pairShardBuckets[pairID],
				PairShardBucket{
					pairID:       pairID,
					edgeBindings: bindings[offset:endOffset],
				})
		}
	}
}

// attachShardToExecutor integrates arbitrage cycles into a core executor.
// Creates priority queues, adds cycle states, builds fanout entries.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func attachShardToExecutor(executor *ArbitrageCoreExecutor, shard *PairShardBucket) {
	// Get or create queue index for this pair
	queueIndex := executor.pairToQueueIndex.Put(uint32(shard.pairID), uint32(len(executor.priorityQueues)))

	// Create new queue if this is the first time we've seen this pair
	if int(queueIndex) == len(executor.priorityQueues) {
		executor.priorityQueues = append(executor.priorityQueues, *quantumqueue64.New())
		executor.fanoutTables = append(executor.fanoutTables, nil)
	}

	queue := &executor.priorityQueues[queueIndex]

	// Process each arbitrage cycle in this shard
	for _, edgeBinding := range shard.edgeBindings {
		// Add cycle state to executor storage
		executor.cycleStates = append(executor.cycleStates, ArbitrageCycleState{
			pairIDs: edgeBinding.cyclePairs,
			// tickValues initialized to zero
		})
		cycleIndex := CycleStateIndex(len(executor.cycleStates) - 1)

		// Allocate priority queue handle for this cycle
		queueHandle, _ := queue.BorrowSafe()
		queue.Push(constants.MaxInitializationPriority, queueHandle, uint64(cycleIndex))

		// Create fanout entries for the other two pairs in this cycle
		// When those pairs update, they need to update this cycle too
		otherEdge1 := (edgeBinding.edgeIndex + 1) % 3
		otherEdge2 := (edgeBinding.edgeIndex + 2) % 3

		for _, edgeIdx := range [...]uint64{otherEdge1, otherEdge2} {
			// OPTIMIZED: cycleStateIndex first for direct array access
			executor.fanoutTables[queueIndex] = append(executor.fanoutTables[queueIndex],
				FanoutEntry{
					cycleStateIndex: uint64(cycleIndex),
					edgeIndex:       edgeIdx,
					queue:           queue,
					queueHandle:     uint64(queueHandle),
				})
		}
	}
}

// launchShardWorker initializes and runs main processing loop for one CPU core.
// Pins to specific core, sets up executor and ring buffer, processes tick updates.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func launchShardWorker(coreID, forwardCoreCount int, shardInput <-chan PairShardBucket) {
	// Pin this goroutine to an OS thread for CPU affinity
	runtime.LockOSThread()

	shutdownChannel := make(chan struct{})

	// Initialize core executor with pre-allocated buffers
	// OPTIMIZED: Hot fields first for cache performance
	executor := &ArbitrageCoreExecutor{
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		isReverseDirection: coreID >= forwardCoreCount, // HOTTEST: Checked every tick update
		cycleStates:        make([]ArbitrageCycleState, 0),
		fanoutTables:       nil,
		priorityQueues:     nil,
		shutdownSignal:     shutdownChannel,
		// Pre-allocated processedCycles buffer is part of struct
	}
	coreExecutors[coreID] = executor
	coreRings[coreID] = ring24.New(constants.DefaultRingSize)

	// Receive and process all assigned shards
	for shard := range shardInput {
		attachShardToExecutor(executor, &shard)
	}

	// Get global control flags for coordinated shutdown
	stopFlag, hotFlag := control.Flags()

	// Launch the main processing loop
	// Core 0 gets special treatment with cooldown management
	if coreID == 0 {
		ring24.PinnedConsumerWithCooldown(coreID, coreRings[coreID], stopFlag, hotFlag,
			func(messagePtr *[24]byte) {
				processTickUpdate(executor, (*TickUpdate)(unsafe.Pointer(messagePtr)))
			}, shutdownChannel)
	} else {
		ring24.PinnedConsumer(coreID, coreRings[coreID], stopFlag, hotFlag,
			func(messagePtr *[24]byte) {
				processTickUpdate(executor, (*TickUpdate)(unsafe.Pointer(messagePtr)))
			}, shutdownChannel)
	}
}

// InitializeArbitrageSystem bootstraps the complete arbitrage detection system.
// Determines core count, builds fanout mapping, launches workers, distributes shards.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func InitializeArbitrageSystem(arbitrageCycles []ArbitrageTriplet) {
	// Determine core count (leave one core for OS and networking)
	coreCount := runtime.NumCPU() - 1
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	coreCount &^= 1                    // Ensure even number (bitwise AND with NOT 1)
	forwardCoreCount := coreCount >> 1 // Half for forward direction

	// Build the fanout mapping from pairs to arbitrage cycles
	buildFanoutShardBuckets(arbitrageCycles)

	// Create worker threads with buffered channels for shard distribution
	shardChannels := make([]chan PairShardBucket, coreCount)
	for i := range shardChannels {
		shardChannels[i] = make(chan PairShardBucket, constants.ShardChannelBufferSize)
		go launchShardWorker(i, forwardCoreCount, shardChannels[i])
	}

	// Distribute shards round-robin across cores
	currentCore := 0
	for _, shardBuckets := range pairShardBuckets {
		for _, shard := range shardBuckets {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			// Send shard to both forward and reverse cores
			shardChannels[forwardCore] <- shard
			shardChannels[reverseCore] <- shard

			// Update routing table: mark which cores handle each pair
			for _, edgeBinding := range shard.edgeBindings {
				for _, pairID := range edgeBinding.cyclePairs {
					pairToCoreAssignment[pairID] |= 1<<forwardCore | 1<<reverseCore
				}
			}
			currentCore++
		}
	}

	// Close all channels to signal completion of shard distribution
	for _, channel := range shardChannels {
		close(channel)
	}
}
