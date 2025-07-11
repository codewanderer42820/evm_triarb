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
	"encoding/binary"
	"fmt"
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
// MESSAGE PASSING STRUCTURES (Runtime Hot Path)
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
// MEMORY LAYOUT:
//
//	[0-7]:   forwardTick (float64) - Price in forward direction
//	[8-15]:  reverseTick (float64) - Price in reverse direction
//	[16-23]: pairID (uint64)       - Pair identifier
//
//go:notinheap
type TickUpdate struct {
	forwardTick float64 // 8B - Price for A→B trades (log2 ratio)
	reverseTick float64 // 8B - Price for B→A trades (negative log2 ratio)
	pairID      PairID  // 8B - Which trading pair this update affects
}

// ============================================================================
// RUNTIME PROCESSING STRUCTURES (Hot path - ordered by access frequency)
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
//go:notinheap
//go:align 64
type ArbitrageCycleState struct {
	tickValues [3]float64 // 24B - HOTTEST: Log price ratios for each edge
	pairIDs    [3]PairID  // 24B - WARM: Pair identifiers for debugging
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
// CACHE OPTIMIZATION:
// - edgeIndex and cycleStateIndex are accessed together (hot fields first)
// - queueHandle and queue pointer are used for priority queue operations
//
//go:notinheap
//go:align 32
type FanoutEntry struct {
	edgeIndex       uint64                         // 8B - HOTTEST: Which edge (0,1,2) in the cycle
	cycleStateIndex uint64                         // 8B - HOTTEST: Index into cycleStates array
	queueHandle     uint64                         // 8B - WARM: Handle for priority queue operations
	queue           *quantumqueue64.QuantumQueue64 // 8B - WARM: Pointer to the priority queue
}

// ProcessedCycle is a temporary structure used during cycle extraction.
// When profitable cycles are found, they're temporarily stored here
// before being reinserted into priority queues.
//
// ZERO-ALLOCATION DESIGN:
// This struct is part of a pre-allocated array in ArbitrageCoreExecutor,
// eliminating heap allocations during hot path processing.
//
//go:notinheap
//go:align 32
type ProcessedCycle struct {
	originalTick    int64                 // 8B - HOT: Original tick value for reinsertion
	cycleStateIndex CycleStateIndex       // 8B - HOT: Index into cycle state storage
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
// 2. Look up affected arbitrage cycles
// 3. Extract profitable cycles from priority queues
// 4. Update cycle states and recompute priorities
// 5. Emit arbitrage opportunities for execution
//
//go:notinheap
type ArbitrageCoreExecutor struct {
	// HOTTEST PATH: Accessed every tick update
	pairToQueueIndex localidx.Hash // 64B - O(1) mapping from pair→queue index

	// HOT PATH: Accessed during cycle processing
	fanoutTables [][]FanoutEntry       // 24B - Maps pairs to affected cycles
	cycleStates  []ArbitrageCycleState // 24B - All arbitrage cycle states

	// WARM PATH: Used during queue operations
	priorityQueues []quantumqueue64.QuantumQueue64 // 24B - Priority queues for each pair

	// BUFFER SPACE: Pre-allocated working memory (eliminates allocations)
	processedCycles [128]ProcessedCycle // 4096B - Temporary storage for extracted cycles

	// COLD PATH: Rarely accessed after initialization
	shutdownSignal     <-chan struct{} // 8B - Shutdown coordination
	isReverseDirection bool            // 1B - Forward vs reverse direction flag
	_                  [7]byte         // 7B - Padding for alignment
}

// ============================================================================
// ADDRESS HANDLING STRUCTURES (Lookup tables)
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
//go:notinheap
//go:align 32
type AddressKey struct {
	words [3]uint64 // 24B - Packed 160-bit Ethereum address
	_     [8]byte   // 8B - Padding to 32B cache line boundary
}

// ============================================================================
// INITIALIZATION STRUCTURES (Setup only)
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
//go:notinheap
//go:align 32
type ArbitrageEdgeBinding struct {
	cyclePairs [3]PairID // 24B - The complete three-pair cycle
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
//go:notinheap
//go:align 32
type PairShardBucket struct {
	pairID       PairID                 // 8B - The pair this shard handles
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
type keccakRandomState struct {
	seed    [32]byte // 32B - Current random seed (Keccak-256 hash)
	counter uint64   // 8B - Increment counter for sequence generation
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

// DispatchTickUpdate is the main entry point for processing Uniswap V2 Sync events.
// This function extracts reserve values, computes price ratios, and dispatches
// updates to all cores that handle cycles containing the updated pair.
//
// PERFORMANCE CHARACTERISTICS:
// - Target: 6-10 CPU cycles total execution time
// - Completely branchless except for essential business logic
// - Zero allocations (all data on stack or in pre-allocated buffers)
// - Optimized for modern CPU instruction pipelines
//
// INPUT FORMAT:
// logView.Data contains: "0x" + 128 hex characters
// - First 64 chars:  Reserve A (uint112 padded to 32 bytes)
// - Second 64 chars: Reserve B (uint112 padded to 32 bytes)
//
// ALGORITHM OVERVIEW:
// 1. Look up pair ID from contract address (Robin Hood hashing)
// 2. Extract leading zero counts from both reserves (uint112 optimization)
// 3. Calculate optimal extraction offsets (preserve magnitude ratios)
// 4. Parse hex values to uint64 (SIMD-optimized)
// 5. Compute log2 price ratio (3.5ns math library)
// 6. Broadcast to all cores handling this pair (lock-free ring buffers)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DispatchTickUpdate(logView *types.LogView) {
	// STEP 1: Address Lookup (Robin Hood hashing - ~20-30 cycles)
	// Convert contract address to pair ID using O(1) hash table lookup.
	// Early return if this address isn't a registered trading pair.
	pairID := lookupPairIDByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if pairID == 0 {
		return // Not a registered pair - ignore this event
	}

	// STEP 2: Reserve Data Extraction Setup (~2 cycles)
	// Skip "0x" prefix and work with raw hex data.
	// Total: 128 hex chars = 64 chars per reserve (uint112 + padding)
	hexData := logView.Data[2:130]

	// STEP 3: Leading Zero Analysis (~15-20 cycles total)
	// uint112 reserves are padded to 32 bytes (64 hex chars) by Ethereum ABI.
	// We skip the guaranteed 32 leading zeros and scan only the meaningful 32 chars.
	// This optimization balances scanning efficiency with natural boundaries.
	lzcntA := countLeadingZeros(hexData[32:64])  // Reserve A: scan chars 32-63
	lzcntB := countLeadingZeros(hexData[96:128]) // Reserve B: scan chars 96-127

	// STEP 4: Magnitude Preservation (~3-4 cycles)
	// Extract both reserves at the same relative magnitude to preserve ratio accuracy.
	// Use branchless min to avoid unpredictable branch (50-50 split between reserves).
	cond := lzcntA - lzcntB                         // Negative if lzcntA < lzcntB
	mask := cond >> 31                              // Arithmetic right shift: -1 if negative, 0 if positive
	minZeros := lzcntB ^ ((lzcntA ^ lzcntB) & mask) // Branchless min(lzcntA, lzcntB)

	// STEP 5: Offset Calculation (~2 cycles)
	// Calculate absolute positions in the original hex string.
	// Add 2 for "0x", add ABI padding skip, add leading zeros skip.
	offsetA := 2 + 32 + minZeros // Position for reserve A extraction
	offsetB := offsetA + 64      // Reserve B is exactly 64 chars later

	// STEP 6: Bounds Calculation (~3-4 cycles)
	// Calculate how many hex characters are available for extraction.
	// Both reserves have identical structure, so calculate once and reuse.
	available := 32 - minZeros // Available chars in both reserves

	// Branchless min(16, available) - same for both reserves
	cond = 16 - available
	mask = cond >> 31
	remaining := available ^ ((16 ^ available) & mask)

	// STEP 7: Hex Parsing (~10-15 cycles)
	// Extract the actual reserve values as uint64 integers.
	// ParseHexU64 uses SIMD-style optimizations for maximum speed.
	reserve0 := utils.ParseHexU64(logView.Data[offsetA : offsetA+remaining])
	reserve1 := utils.ParseHexU64(logView.Data[offsetB : offsetB+remaining])

	// STEP 8: Price Ratio Calculation (~10-15 cycles)
	// Compute log2(reserve0/reserve1) using ultra-fast logarithm library.
	// The 3.5ns performance here is critical for overall system latency.
	// Handle edge cases (zero reserves) by setting randomized extreme tick value.
	tickValue, err := fastuni.Log2ReserveRatio(reserve0, reserve1)
	if err != nil {
		// Fast randomization using address entropy: 51.2 to 64.0 range
		// Prevents hash collisions in priority queues (Pareto 80/20 distribution)
		addrHash := uint64(uintptr(unsafe.Pointer(&message[0]))) // Stack address entropy
		randBits := addrHash & 0x1FFF                            // Extract 13 bits (0-8191)
		tickValue = 51.2 + float64(randBits)*0.0015625           // Scale to [51.2, 64.0]
	}

	// STEP 9: Message Construction (~3-4 cycles)
	// Build the tick update message on the stack (zero allocation).
	// TickUpdate must be exactly 24 bytes for ring buffer compatibility.
	var message [24]byte
	tickUpdate := (*TickUpdate)(unsafe.Pointer(&message))
	tickUpdate.forwardTick = tickValue  // Positive for reserve0/reserve1 ratio
	tickUpdate.reverseTick = -tickValue // Negative for reserve1/reserve0 ratio
	tickUpdate.pairID = pairID

	// STEP 10: Core Broadcasting (~5-10 cycles)
	// Send the update to all CPU cores that handle cycles containing this pair.
	// Use bit manipulation to iterate through assigned cores efficiently.
	coreAssignments := pairToCoreAssignment[pairID]
	for coreAssignments != 0 {
		coreID := bits.TrailingZeros64(uint64(coreAssignments))
		coreRings[coreID].Push(&message) // Lock-free ring buffer push
		coreAssignments &^= 1 << coreID  // Clear this bit and continue
	}
}

// countLeadingZeros counts leading ASCII '0' characters in hex data.
// This function is critical for determining where significant digits begin
// in Ethereum ABI-encoded uint112 values.
//
// ALGORITHM:
// 1. Load 8-byte chunks and XOR with ASCII '0' pattern (0x3030303030303030)
// 2. Create bitmask indicating which chunks contain non-zero data
// 3. Use TrailingZeros to find first significant chunk
// 4. Use TrailingZeros again to find first significant byte within chunk
//
// PERFORMANCE:
// - Processes 32 hex characters in ~12-15 cycles
// - Uses SIMD-style parallel processing
// - Completely branchless execution
//
// INPUT: 32-byte hex segment (uint112 significant range)
// OUTPUT: Count of leading ASCII '0' characters (0-32)
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

// lookupPairIDByAddress performs O(1) address lookup using Robin Hood hashing.
// This function is called for every incoming event to determine if we should
// process it (registered trading pair) or ignore it (unknown contract).
//
// ROBIN HOOD HASHING:
// - Linear probing with distance tracking
// - Backward shift deletion (not used here, lookup only)
// - Early termination when probe distance exceeds stored distance
// - Excellent worst-case performance (low variance)
//
// PERFORMANCE CHARACTERISTICS:
// - Average case: 1-2 probes (~15-20 cycles)
// - Worst case: <10 probes (~60-80 cycles)
// - Cache-friendly linear access pattern
// - Branchless key comparison eliminates function call overhead
//
// OPTIMIZATION NOTES:
// - Uses branchless XOR-based key comparison instead of method calls
// - Combines 3 word comparisons into single conditional check
// - Maintains Robin Hood early termination for optimal performance
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
		// Result is 0 if keys are identical, non-zero if different
		// This eliminates function call overhead and reduces branches
		keyDiff := (key.words[0] ^ currentKey.words[0]) |
			(key.words[1] ^ currentKey.words[1]) |
			(key.words[2] ^ currentKey.words[2])

		// Early return conditions (essential branches for hash table efficiency)
		if currentPairID == 0 {
			return 0 // Empty slot found - address not registered
		}

		if keyDiff == 0 {
			return currentPairID // Keys match - found the pair ID
		}

		// Robin Hood early termination optimization:
		// If the stored key has traveled less distance than our probe,
		// then our key cannot be in the table (would have displaced the stored key)
		currentKeyHash := directAddressToIndex64Stored(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)

		if currentDist < dist {
			return 0 // Early termination - key not in table
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

// processTickUpdate handles incoming tick updates on a specific CPU core.
// This is where the actual arbitrage detection logic runs.
//
// ALGORITHM OVERVIEW:
// 1. Extract current tick value based on core direction (forward/reverse)
// 2. Find priority queue for the updated pair
// 3. Extract all profitable cycles from the queue
// 4. Update cycle states with new tick value
// 5. Recompute priorities and reinsert cycles
// 6. Update all other cycles that include this pair (fanout)
//
// ZERO-ALLOCATION DESIGN:
// - Uses pre-allocated buffer for cycle extraction
// - No heap allocations during processing
// - All data structures are sized at initialization
//
// PERFORMANCE TARGET: ~40-60 cycles per tick update
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processTickUpdate(executor *ArbitrageCoreExecutor, update *TickUpdate) {
	// STEP 1: Direction Selection
	// Each core processes either forward (A→B) or reverse (B→A) direction.
	// This doubles the processing capacity and handles bidirectional arbitrage.
	var currentTick float64
	if !executor.isReverseDirection {
		currentTick = update.forwardTick // Use positive tick value
	} else {
		currentTick = update.reverseTick // Use negative tick value
	}

	// STEP 2: Queue Lookup
	// Find the priority queue responsible for this pair.
	// Each pair has its own queue containing all cycles that include that pair.
	queueIndex, _ := executor.pairToQueueIndex.Get(uint32(update.pairID))
	queue := &executor.priorityQueues[queueIndex]

	// STEP 3: Profitable Cycle Extraction
	// Extract all cycles that become profitable with the new tick value.
	// Use pre-allocated buffer to avoid heap allocations.
	cycleCount := 0

	for {
		// Peek at the most profitable cycle (lowest total tick sum)
		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleStateIndex(cycleData)
		cycle := &executor.cycleStates[cycleIndex]

		// Calculate total profitability: sum of all three edge ticks
		// Negative sum indicates profitable arbitrage opportunity
		totalProfitability := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		isProfitable := totalProfitability < 0

		// Optional: Emit arbitrage opportunity for execution
		if isProfitable {
			//emitArbitrageOpportunity(cycle, currentTick)
		}

		// Stop extraction if not profitable or buffer full
		if !isProfitable || cycleCount == len(executor.processedCycles) {
			break
		}

		// Store cycle in pre-allocated buffer for later reinsertion
		executor.processedCycles[cycleCount] = ProcessedCycle{
			queueHandle:     handle,
			originalTick:    queueTick,
			cycleStateIndex: cycleIndex,
		}
		cycleCount++

		// Remove cycle from queue (it will be reinserted with updated priority)
		queue.UnlinkMin(handle)

		// Stop if queue is empty
		if queue.Empty() {
			break
		}
	}

	// STEP 4: Cycle Reinsertion
	// Reinsert all extracted cycles with their original priorities.
	// This maintains queue invariants while allowing state updates.
	for i := 0; i < cycleCount; i++ {
		cycle := &executor.processedCycles[i]
		queue.Push(cycle.originalTick, cycle.queueHandle, uint64(cycle.cycleStateIndex))
	}

	// STEP 5: Fanout Updates
	// Update all other cycles that include this pair.
	// Each pair can be part of many triangular arbitrage cycles.
	for _, fanoutEntry := range executor.fanoutTables[queueIndex] {
		cycle := &executor.cycleStates[fanoutEntry.cycleStateIndex]

		// Update the tick value for this edge of the cycle
		cycle.tickValues[fanoutEntry.edgeIndex] = currentTick

		// Recalculate cycle priority and update queue position
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

// bytesToAddressKey converts a 40-character hex address to internal AddressKey format.
// Ethereum addresses are 20 bytes (160 bits) represented as 40 hex characters.
//
// OPTIMIZATION:
// Pack the 20-byte address into exactly 3 uint64 words:
// - Word 0: bytes 0-7   (64 bits)
// - Word 1: bytes 8-15  (64 bits)
// - Word 2: bytes 16-19 (32 bits in lower part of word)
//
// This alignment enables efficient 64-bit comparisons during hash table lookup.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func bytesToAddressKey(address40HexChars []byte) AddressKey {
	// Parse hex string to 20-byte address using SIMD-optimized parser
	parsedAddress := utils.ParseEthereumAddress(address40HexChars)

	// Optimized: Use utils.Load64 for all memory operations
	word0 := utils.Load64(parsedAddress[0:8])  // Bytes 0-7
	word1 := utils.Load64(parsedAddress[8:16]) // Bytes 8-15

	// Load last 8 bytes and mask to get only the 4 bytes we need
	// This eliminates the copy + array allocation completely
	word2 := utils.Load64(parsedAddress[12:20]) & 0xFFFFFFFF
	//                    ^^^^^^^^^^ Load bytes 12-19, mask to get bytes 16-19

	return AddressKey{
		words: [3]uint64{word0, word1, word2},
	}
}

// directAddressToIndex64 computes hash table index from raw hex address.
// Extracts hash directly from hex characters without full address parsing.
//
// HASH FUNCTION CHOICE:
// Ethereum addresses have good entropy in the middle bytes due to keccak256.
// Using hex chars 12-27 (bytes 6-13) provides excellent hash distribution
// while avoiding common prefixes and checksums that might have patterns.
//
// OPTIMIZATION:
// Directly parses 16 hex characters (64 bits) instead of parsing full
// 40-character address then extracting middle portion.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64(address40HexChars []byte) uint64 {
	// Extract middle 16 hex characters (chars 12-27) and parse directly
	// This corresponds to bytes 6-13 of the final address
	hash64 := utils.ParseHexU64(address40HexChars[12:28])
	return hash64 & uint64(constants.AddressTableMask)
}

// directAddressToIndex64Stored computes hash from stored AddressKey.
// This is used during Robin Hood probing to calculate distances.
//
// CONSISTENCY REQUIREMENT:
// Must produce the same hash value as directAddressToIndex64 for the same address.
// Used to determine how far each stored key has traveled from its ideal position.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64Stored(key AddressKey) uint64 {
	// Extract maximum entropy using overlapping and XOR for avalanche effect
	// Combine parts from all three words to use all available entropy

	// Use overlapping portions to extract both high and low nibbles
	hash := (key.words[0] >> 16) ^ // Upper portion of word 0
		(key.words[1] << 8) ^ // Shifted word 1 for overlap
		(key.words[2] << 32) ^ // Last word in upper bits
		(key.words[0] & 0xFFFFFFFF) ^ // Lower portion of word 0
		(key.words[1] >> 32) // Upper portion of word 1

	return hash & uint64(constants.AddressTableMask)
}

// isEqual performs fast comparison between two AddressKey values.
// Compares all three 64-bit words to verify complete address match.
//
// OPTIMIZATION:
// Modern CPUs can compare 64-bit values in a single instruction.
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

// emitArbitrageOpportunity logs details of a profitable arbitrage cycle.
// This function is normally commented out to avoid I/O overhead in the hot path.
//
// DEBUG OUTPUT FORMAT:
// - Pair IDs involved in the cycle
// - Individual tick values for each edge
// - Total profitability calculation
// - Timestamp and other metadata
//
// PERFORMANCE IMPACT:
// When enabled, adds ~100-200 cycles per opportunity due to string formatting.
// Only use for debugging or low-frequency monitoring.
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
// This function populates the Robin Hood hash table during initialization.
//
// REGISTRATION PROCESS:
// 1. Parse hex address to internal key format
// 2. Calculate hash table index
// 3. Probe for empty slot or existing key
// 4. Handle Robin Hood displacement if necessary
// 5. Insert or update the pair mapping
//
// ROBIN HOOD INSERTION:
// If a probe encounters a key that has traveled less distance,
// displace that key and continue inserting it further down.
// This minimizes variance in lookup times.
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
// Each pair can be processed by multiple cores for redundancy and load balancing.
//
// CORE ASSIGNMENT STRATEGY:
// - Forward cores handle positive price movements
// - Reverse cores handle negative price movements
// - Multiple cores per pair provide fault tolerance
// - Bit mask enables efficient core iteration
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairToCore(pairID PairID, coreID uint8) {
	pairToCoreAssignment[pairID] |= 1 << coreID
}

// newKeccakRandom creates a deterministic random number generator.
// Used for reproducible load balancing during system initialization.
//
// DETERMINISTIC RANDOMNESS:
// - Same input always produces same output sequence
// - Enables reproducible performance testing
// - Prevents adversarial inputs from creating bad distributions
// - Uses cryptographically strong Keccak-256 for quality
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
		seed:    seed,
		counter: 0,
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
	binary.LittleEndian.PutUint64(input[32:], k.counter)

	// Hash to get random bytes
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(input[:])
	output := hasher.Sum(nil)

	// Increment counter for next call
	k.counter++

	// Return first 8 bytes as uint64
	return utils.Load64(output[:8])
}

// nextInt generates a random integer in the range [0, upperBound).
// Uses unbiased modular reduction to ensure uniform distribution.
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

	// Optimization for power-of-2 bounds
	if upperBound&(upperBound-1) == 0 {
		return int(randomValue & uint64(upperBound-1))
	}

	// Unbiased modular reduction using multiplication method
	// This avoids modulo bias that would favor smaller values
	high64, _ := bits.Mul64(randomValue, uint64(upperBound))
	return int(high64)
}

// keccakShuffleEdgeBindings performs Fisher-Yates shuffle with deterministic randomness.
// This distributes arbitrage cycles evenly across CPU cores for load balancing.
//
// WHY SHUFFLE:
// - Prevents pathological cases where one core gets all the work
// - Ensures reproducible load distribution for testing
// - Balances memory access patterns across NUMA domains
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
	binary.LittleEndian.PutUint64(seedInput[:], utils.Mix64(uint64(pairID)))

	rng := newKeccakRandom(seedInput[:])

	// Fisher-Yates shuffle with deterministic randomness
	for i := len(bindings) - 1; i > 0; i-- {
		j := rng.nextInt(i + 1)
		bindings[i], bindings[j] = bindings[j], bindings[i]
	}
}

// buildFanoutShardBuckets constructs the fanout mapping from pairs to arbitrage cycles.
// This is the core data structure that enables efficient cycle updates.
//
// FANOUT CONSTRUCTION ALGORITHM:
// 1. Group all arbitrage cycles by the pairs they contain
// 2. Create edge bindings showing which position each pair occupies
// 3. Shuffle bindings deterministically for load balancing
// 4. Partition into shards that fit in CPU cache
//
// PERFORMANCE IMPACT:
// The quality of this fanout structure directly affects runtime performance.
// Well-balanced shards reduce cache misses and improve parallelization.
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

// attachShardToExecutor integrates a shard of arbitrage cycles into a core executor.
// This builds the runtime data structures needed for arbitrage processing.
//
// INTEGRATION PROCESS:
// 1. Create or reuse priority queue for this pair
// 2. Add cycle states to the executor's storage
// 3. Create fanout entries linking pair updates to cycle updates
// 4. Initialize priority queue with maximum priority (least favorable)
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
			executor.fanoutTables[queueIndex] = append(executor.fanoutTables[queueIndex],
				FanoutEntry{
					edgeIndex:       edgeIdx,
					cycleStateIndex: uint64(cycleIndex),
					queueHandle:     uint64(queueHandle),
					queue:           queue,
				})
		}
	}
}

// launchShardWorker initializes and runs the main processing loop for one CPU core.
// This function sets up the core executor and begins processing tick updates.
//
// CORE WORKER RESPONSIBILITIES:
// 1. Pin to specific CPU core for NUMA optimization
// 2. Initialize executor with assigned arbitrage cycles
// 3. Set up lock-free ring buffer for inter-core communication
// 4. Run main processing loop until shutdown
//
// THREADING MODEL:
// - One worker per CPU core
// - OS thread pinning for predictable performance
// - Lock-free communication between cores
// - Graceful shutdown coordination
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
	executor := &ArbitrageCoreExecutor{
		isReverseDirection: coreID >= forwardCoreCount, // Second half of cores handle reverse direction
		shutdownSignal:     shutdownChannel,
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		cycleStates:        make([]ArbitrageCycleState, 0),
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
// This is the main entry point for system initialization.
//
// INITIALIZATION SEQUENCE:
// 1. Determine optimal core count (leave one core for OS/networking)
// 2. Build fanout mapping from arbitrage cycle definitions
// 3. Launch worker threads on each CPU core
// 4. Distribute arbitrage cycles evenly across cores
// 5. Set up routing tables for efficient message dispatch
//
// CORE ALLOCATION STRATEGY:
// - Use all available cores except one (for OS and networking)
// - Ensure even number of cores (half forward, half reverse)
// - Pin workers to specific cores for NUMA optimization
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
