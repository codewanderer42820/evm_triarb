// ════════════════════════════════════════════════════════════════════════════════════════════════
// Multi-Core Arbitrage Detection Engine
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Event Router & Arbitrage Detector
//
// Description:
//   Lock-free multi-core arbitrage detection system using optimized parsing and zero-allocation
//   priority queues. Detects profitable opportunities across Uniswap V2 pairs in real-time.
//
// Features:
//   - Multi-core event dispatch with lock-free coordination
//   - Address resolution using Robin Hood hashing
//   - SIMD-optimized hex parsing operations
//   - Linear core scaling architecture
//   - Bit shift optimizations for all hot paths
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package router

import (
	"hash"
	"math/bits"
	"runtime"
	"sync"
	"unsafe"

	"main/constants"
	"main/debug"
	"main/fastuni"
	"main/localidx"
	"main/pooledquantumqueue"
	"main/ring24"
	"main/types"
	"main/utils"

	"golang.org/x/crypto/sha3"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL SYNCHRONIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// gcComplete is a channel used for two-stage initialization.
// Workers block on this channel after initialization until GC is complete.
// Closing this channel releases all workers to start hot spinning.
var gcComplete = make(chan struct{})

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// TradingPairID uniquely identifies a Uniswap V2 trading pair contract.
// Each pair represents a market between two tokens (e.g., ETH/DAI, USDC/WETH).
type TradingPairID uint64

// ArbitrageTriangle defines a three-pair arbitrage cycle.
// Example: [ETH/DAI, DAI/USDC, USDC/ETH] forms a complete arbitrage loop.
// The order matters as it defines the trading direction through the cycle.
type ArbitrageTriangle [3]TradingPairID

// CycleIndex provides typed access into the cycle state storage arrays.
// This prevents confusion between different types of indices in the system.
type CycleIndex uint64

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INTER-CORE MESSAGE STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// PriceUpdateMessage represents a price change notification sent between CPU cores.
// When a trading pair's price changes, this message is distributed to all cores
// that need to update arbitrage cycles involving that pair.
//
//go:notinheap
type PriceUpdateMessage struct {
	pairID      TradingPairID // 8B - Trading pair that experienced the price change
	forwardTick float64       // 8B - Logarithmic price ratio in forward direction
	reverseTick float64       // 8B - Same price change in opposite direction (negative of forwardTick)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ARBITRAGE CYCLE STATE STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ArbitrageCycleState maintains the real-time profitability calculation for a three-pair cycle.
// Each cycle consists of three trading pairs that form a complete loop.
// Profitability is determined by summing the logarithmic price ratios of all three pairs.
//
// Mathematical Foundation:
// If we have pairs A/B, B/C, C/A with price ratios r1, r2, r3, then:
// Total profit = log(r1) + log(r2) + log(r3) = log(r1 * r2 * r3)
// Profitable when: r1 * r2 * r3 > 1, or equivalently: log(r1 * r2 * r3) > 0
//
//go:notinheap
//go:align 64
type ArbitrageCycleState struct {
	// tickValues stores the logarithmic price ratios for each of the three pairs in the cycle.
	// One of these is intentionally always zero so that the common tick for the entire queue
	// can calculate and find the best min instantly. The zero position corresponds to the
	// queue's primary pair - its tick is added externally during profitability calculation.
	tickValues [3]float64 // 24B - Index corresponds to pair position, one always zero

	pairIDs [3]TradingPairID // 24B - The three trading pairs that form this arbitrage cycle
	_       [16]byte         // 16B - Padding for cache line boundary alignment
}

// CycleFanoutEntry defines how price updates propagate to affected arbitrage cycles.
// When a trading pair's price changes, we need to update all cycles that include that pair.
// This structure maps each price update to the specific cycles and queues that need updating.
//
//go:notinheap
//go:align 32
type CycleFanoutEntry struct {
	queueHandle pooledquantumqueue.Handle // 8B - Direct access to the cycle's position in the priority queue
	cycleIndex  uint64                    // 8B - Points to the specific arbitrage cycle that needs updating
	queueIndex  uint64                    // 8B - Which priority queue contains this cycle
	edgeIndex   uint64                    // 8B - Which position (0, 1, or 2) within the cycle this pair occupies
}

// ExtractedCycle provides temporary storage for cycles extracted during profitability analysis.
// When checking for profitable opportunities, we temporarily remove cycles from their
// priority queues. This structure remembers their original state for reinsertion.
//
//go:notinheap
//go:align 32
type ExtractedCycle struct {
	cycleIndex   CycleIndex                // 8B - Which cycle was extracted from the queue
	originalTick int64                     // 8B - The cycle's priority before extraction
	queueHandle  pooledquantumqueue.Handle // 8B - Queue handle for efficient reinsertion
	_            [8]byte                   // 8B - Padding for optimal memory alignment
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE PROCESSING ENGINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ArbitrageEngine orchestrates arbitrage detection for a single CPU core.
// Each core operates independently on its assigned subset of trading pairs.
// This design enables parallel processing while avoiding lock contention.
//
// Fields are ordered by access frequency and cache line optimization:
//
//go:notinheap
//go:align 64
type ArbitrageEngine struct {
	// CACHE LINE 1: Primary lookup table (64B)
	pairToQueueLookup localidx.Hash // 64B - Nuclear hot: used in every update

	// CACHE LINE 2: Secondary lookup table (64B)
	pairToFanoutIndex localidx.Hash // 64B - Nuclear hot: used in every update

	// CACHE LINE 3: Core processing control and arrays (64B)
	isReverseDirection bool                                    // 1B - Nuclear hot: read first in every update
	_                  [7]byte                                 // 7B - Alignment padding
	priorityQueues     []pooledquantumqueue.PooledQuantumQueue // 24B - Extremely hot: queue operations
	cycleStates        []ArbitrageCycleState                   // 24B - Extremely hot: tick calculations
	_                  [8]byte                                 // 8B - Cache line padding

	// CACHE LINE 4: Fanout processing and shared memory (64B)
	cycleFanoutTable [][]CycleFanoutEntry       // 24B - Extremely hot: fanout loops
	sharedArena      []pooledquantumqueue.Entry // 24B - Extremely hot: all queue memory operations
	_                [16]byte                   // 16B - Cache line padding

	// CACHE LINE 5+: Extraction buffer (1024B = 16 cache lines)
	extractedCycles [32]ExtractedCycle // 1024B - Warm: only used for profitable cycles

	// COLD: Initialization only
	nextHandle pooledquantumqueue.Handle // 8B - Cold: only during queue setup
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS RESOLUTION STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// PackedAddress represents an Ethereum address optimized for hash table operations.
// Ethereum addresses are 20 bytes (160 bits), which we pack into three 64-bit words
// for efficient comparison and hashing operations.
//
//go:notinheap
//go:align 32
type PackedAddress struct {
	words [3]uint64 // 24B - 160-bit Ethereum address as three 64-bit values
	_     [8]byte   // 8B - Padding for cache line optimization
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// CycleEdge represents a single edge within an arbitrage cycle during system construction.
// During initialization, we need to know which cycles each trading pair participates in
// and what position it holds within each cycle.
//
//go:notinheap
//go:align 32
type CycleEdge struct {
	cyclePairs [3]TradingPairID // 24B - Complete three-pair arbitrage cycle definition
	edgeIndex  uint64           // 8B - This pair's position (0, 1, or 2) within the cycle
}

// PairWorkloadShard groups arbitrage cycles by trading pair for efficient core distribution.
// During initialization, we organize all cycles by the pairs they contain, then distribute
// these shards across CPU cores for parallel processing.
//
//go:notinheap
//go:align 32
type PairWorkloadShard struct {
	pairID     TradingPairID // 8B - Trading pair that all cycles in this shard have in common
	cycleEdges []CycleEdge   // 24B - All arbitrage cycles that include this trading pair
}

// CryptoRandomGenerator provides deterministic randomness for load balancing.
// We use cryptographic-quality randomness to ensure even distribution of workload
// across CPU cores, preventing hot spots and ensuring optimal distribution.
//
//go:notinheap
//go:align 64
type CryptoRandomGenerator struct {
	counter uint64    // 8B - Unique sequence number for each random generation
	seed    [32]byte  // 32B - Cryptographic seed for deterministic random generation
	hasher  hash.Hash // 24B - Keccak-256 hash function for random number generation
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL STATE VARIABLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 64
var (
	// CACHE LINE GROUP 1: Address lookup (accessed together in hot path)
	addressToPairMap  [constants.AddressTableCapacity]TradingPairID
	packedAddressKeys [constants.AddressTableCapacity]PackedAddress

	// CACHE LINE GROUP 2: Core routing (accessed after address lookup)
	pairToCoreRouting [constants.PairRoutingTableCapacity]uint64

	// CACHE LINE GROUP 3: Ring buffers (NUCLEAR HOT - accessed after routing)
	coreRings [constants.MaxSupportedCores]*ring24.Ring

	// COLD: Only accessed during initialization
	coreEngines        [constants.MaxSupportedCores]*ArbitrageEngine
	pairWorkloadShards map[TradingPairID][]PairWorkloadShard
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE ENGINE METHODS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// allocateQueueHandle returns the next available handle from the shared arena.
// This implements simple sequential allocation without complex memory management.
// Each handle represents a position in the shared memory pool.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (engine *ArbitrageEngine) allocateQueueHandle() pooledquantumqueue.Handle {
	// Get the current handle value for this allocation
	handle := engine.nextHandle

	// Increment the counter for the next allocation request
	engine.nextHandle++

	return handle
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// EVENT DISPATCH PIPELINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// DispatchPriceUpdate processes Uniswap V2 Sync events and distributes price updates to cores.
// This is the main entry point for all price change events in the system.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DispatchPriceUpdate(logView *types.LogView) {
	// Convert the Ethereum contract address from the log event to our internal trading pair ID
	pairID := LookupPairByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if pairID == 0 {
		// This address is not registered in our system, so ignore this event
		return
	}

	// Extract the hex-encoded reserve data from the Uniswap V2 Sync event
	// Skip the "0x" prefix and take the next 128 hex characters containing both reserves
	hexData := logView.Data[2:130]

	// Count leading zeros in each reserve value to determine precision requirements
	leadingZerosA := countHexLeadingZeros(hexData[32:64])  // Analyze reserve A (first token)
	leadingZerosB := countHexLeadingZeros(hexData[96:128]) // Analyze reserve B (second token)

	// Calculate the minimum leading zeros to preserve maximum precision in both reserves
	// Using branchless programming to avoid CPU pipeline stalls from conditional branches
	cond := leadingZerosA - leadingZerosB
	mask := cond >> 31                                                   // Arithmetic right shift creates all-1s mask if cond is negative
	minZeros := leadingZerosB ^ ((leadingZerosA ^ leadingZerosB) & mask) // Branchless min()

	// Calculate where to start extracting meaningful digits from the hex data
	offsetA := 2 + 32 + minZeros // Position for reserve A extraction
	offsetB := offsetA + 64      // Reserve B is 64 hex characters after reserve A

	// Calculate how many hex characters we can meaningfully extract from each reserve
	available := 32 - minZeros                         // Available significant digits after leading zeros
	cond = 16 - available                              // Check if we have at least 16 digits available
	mask = cond >> 31                                  // Create mask for branchless selection
	remaining := available ^ ((16 ^ available) & mask) // Branchless min(16, available)

	// Convert the hex strings to 64-bit unsigned integers representing token reserves
	reserve0 := utils.ParseHexU64(logView.Data[offsetA : offsetA+remaining])
	reserve1 := utils.ParseHexU64(logView.Data[offsetB : offsetB+remaining])

	// Force LSB to 1 - elegantly prevents log(0) with minimal precision impact
	// Empty pools (0:0) become (1:1) representing balanced state
	// Near-empty pools (0:large) become (1:large) preserving extreme price ratios
	// The maximum error of 1 unit in millions is economically negligible
	reserve0 |= 1 // Ensure non-zero by setting lowest bit
	reserve1 |= 1 // Single-instruction performance vs complex masking

	// Calculate the logarithmic price ratio between the two token reserves
	tickValue, err := fastuni.Log2ReserveRatio(reserve0, reserve1)

	// Construct the price update message to send to processing cores
	var message PriceUpdateMessage
	if err != nil {
		// Fallback path for unexpected calculation failures
		// Generate deterministic pseudo-random tick to prevent systematic biases
		addrHash := utils.Mix64(uint64(pairID))           // Create hash from pair ID
		randBits := addrHash & 0x1FFF                     // Extract 13 bits (0-8191 range)
		placeholder := 50.2 + float64(randBits)*0.0015625 // Scale to reasonable tick range

		message = PriceUpdateMessage{
			pairID:      pairID,
			forwardTick: placeholder, // Positive values deprioritize cycles (less profitable)
			reverseTick: placeholder, // Same value for both directions maintains consistency
		}
	} else {
		// Normal case: use the calculated logarithmic ratio with opposite signs
		// Forward and reverse directions have opposite signs for bidirectional arbitrage detection
		message = PriceUpdateMessage{
			pairID:      pairID,
			forwardTick: tickValue,  // Actual calculated tick value
			reverseTick: -tickValue, // Negative for reverse direction processing
		}
	}

	// Distribute the price update message to all CPU cores that process this trading pair
	// We use guaranteed delivery to ensure no core misses important price updates
	coreAssignments := pairToCoreRouting[pairID]          // Get bitmask of target cores
	messageBytes := (*[24]byte)(unsafe.Pointer(&message)) // Convert to byte array for ring buffer

	// Continue delivery attempts until all cores have received the message
	for coreAssignments != 0 {
		failedCores := uint64(0) // Track cores that couldn't receive the message

		// Attempt delivery to all currently assigned cores in this round
		currentAssignments := coreAssignments
		for currentAssignments != 0 {
			// Find the next core to deliver to using bit manipulation
			coreID := bits.TrailingZeros64(currentAssignments)

			// Try to push the message to this core's lock-free ring buffer
			if !coreRings[coreID].Push(messageBytes) {
				// Ring buffer was full, mark this core for retry in next round
				failedCores |= 1 << coreID
			}

			// Clear this core from the current round assignments
			currentAssignments &^= 1 << coreID
		}

		// Only retry cores that failed delivery, successful cores are done
		coreAssignments = failedCores
	}
}

// countHexLeadingZeros performs efficient leading zero counting for hex-encoded numeric data.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func countHexLeadingZeros(segment []byte) int {
	// Define the 64-bit pattern representing eight consecutive ASCII '0' characters
	const ZERO_PATTERN = 0x3030303030303030

	// Process the 32-byte hex segment in four 8-byte chunks simultaneously
	// XOR with ZERO_PATTERN converts zero bytes to 0x00 and non-zero bytes to non-zero values
	c0 := utils.Load64(segment[0:8]) ^ ZERO_PATTERN   // Process bytes 0-7
	c1 := utils.Load64(segment[8:16]) ^ ZERO_PATTERN  // Process bytes 8-15
	c2 := utils.Load64(segment[16:24]) ^ ZERO_PATTERN // Process bytes 16-23
	c3 := utils.Load64(segment[24:32]) ^ ZERO_PATTERN // Process bytes 24-31

	// Create a bitmask indicating which 8-byte chunks contain only zero characters
	// The expression (x|(^x+1))>>63 produces 1 if any byte in x is non-zero, 0 if all bytes are zero
	mask := ((c0|(^c0+1))>>63)<<0 | ((c1|(^c1+1))>>63)<<1 |
		((c2|(^c2+1))>>63)<<2 | ((c3|(^c3+1))>>63)<<3

	// Find the first chunk that contains a non-zero character
	firstChunk := bits.TrailingZeros64(mask)

	// Handle the special case where all 32 hex characters are zeros
	if firstChunk == 64 {
		return 32 // All characters in the segment are zeros
	}

	// Create an array to access the processed chunks by index
	chunks := [4]uint64{c0, c1, c2, c3}

	// Within the first non-zero chunk, find the first non-zero byte
	// Divide by 8 to convert bit position to byte position within the chunk
	firstByte := bits.TrailingZeros64(chunks[firstChunk]) >> 3

	// Calculate the total number of leading zero characters
	return (firstChunk << 3) + firstByte
}

// LookupPairByAddress performs address resolution using Robin Hood hashing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LookupPairByAddress(address40HexChars []byte) TradingPairID {
	// Convert the hex address string to an optimized packed representation for comparison
	key := packEthereumAddress(address40HexChars)

	// Calculate the initial position in the hash table using the address hash
	i := hashAddressToIndex(address40HexChars)
	dist := uint64(0) // Track how far we've probed from the ideal position

	// Robin Hood hash table lookup with early termination
	for {
		// Get the current entry at this table position
		currentPairID := addressToPairMap[i]
		currentKey := packedAddressKeys[i]

		// Compare all three 64-bit words of the packed address simultaneously
		// If keyDiff is zero, all words match and we found our target address
		keyDiff := (key.words[0] ^ currentKey.words[0]) |
			(key.words[1] ^ currentKey.words[1]) |
			(key.words[2] ^ currentKey.words[2])

		// Check termination conditions for the search
		if currentPairID == 0 {
			// Empty slot encountered - our address is not in the table
			return 0
		}
		if keyDiff == 0 {
			// Exact address match found - return the associated trading pair ID
			return currentPairID
		}

		// Robin Hood early termination check
		// If the current entry has traveled less distance than us, our key cannot be in the table
		currentKeyHash := hashPackedAddressToIndex(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)
		if currentDist < dist {
			// The current entry is closer to its ideal position than we are to ours
			// This violates the Robin Hood invariant, so our key is not present
			return 0
		}

		// Continue probing to the next slot in the hash table
		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++ // Increment our probe distance
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE PROCESSING PIPELINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processArbitrageUpdate orchestrates arbitrage detection for incoming price updates.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processArbitrageUpdate(engine *ArbitrageEngine, update *PriceUpdateMessage) {
	// Select tick based on core direction - predictable per core
	var currentTick float64
	if engine.isReverseDirection {
		currentTick = update.reverseTick
	} else {
		currentTick = update.forwardTick
	}

	// Two lookups, both are predictable Robin Hood accesses
	queueIndex, hasQueue := engine.pairToQueueLookup.Get(uint32(update.pairID))
	fanoutIndex, hasFanout := engine.pairToFanoutIndex.Get(uint32(update.pairID))

	// Process queue if exists (predictable branch - consistent per pair)
	if hasQueue {
		queue := &engine.priorityQueues[queueIndex]

		// Extract profitable cycles from the queue for opportunity detection
		cycleCount := 0
		extractedCyclesLen := len(engine.extractedCycles)

		for {
			// Stop if we've processed all cycles or reached our extraction limit
			if queue.Empty() {
				break
			}

			// Examine the most profitable cycle without removing it yet
			handle, queueTick, cycleData := queue.PeepMin()
			cycleIndex := CycleIndex(cycleData)
			cycle := &engine.cycleStates[cycleIndex]

			// Calculate total profitability by adding the new tick to existing cycle ticks
			// The main pair's tick is always zero in storage, so we add currentTick
			totalProfitability := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
			isProfitable := totalProfitability < 0

			// Report profitable opportunities for potential execution
			if isProfitable {
				emitArbitrageOpportunity(cycle, currentTick)
			}

			// Stop extracting if we hit a non-profitable cycle or reached our extraction limit
			if !isProfitable || cycleCount == extractedCyclesLen {
				break
			}

			// Store the cycle's current state so we can restore it after processing
			engine.extractedCycles[cycleCount] = ExtractedCycle{
				cycleIndex:   cycleIndex,
				originalTick: queueTick,
				queueHandle:  handle,
			}
			cycleCount++

			// Temporarily remove the cycle from the queue
			queue.UnlinkMin(handle)
		}

		// Restore all extracted cycles back to the queue with their original priorities
		for i := 0; i < cycleCount; i++ {
			cycle := &engine.extractedCycles[i]
			queue.Push(cycle.originalTick, cycle.queueHandle, uint64(cycle.cycleIndex))
		}
	}

	// Only process fanout if this pair has fanout entries
	if hasFanout {
		tickClampingBound := constants.TickClampingBound
		quantizationScale := constants.QuantizationScale

		for _, fanoutEntry := range engine.cycleFanoutTable[fanoutIndex] {
			// Get the specific cycle that needs updating
			cycle := &engine.cycleStates[fanoutEntry.cycleIndex]

			// Update the tick value for this pair's position within the cycle
			// This is NOT the main pair position, so we store the actual tick value
			cycle.tickValues[fanoutEntry.edgeIndex] = currentTick

			// Recalculate the cycle's priority based on its new total profitability
			// Note: One of these tick values is always zero (the main pair's position)
			tickSum := cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
			newPriority := int64((tickSum + tickClampingBound) * quantizationScale)

			// Update the cycle's position in its priority queue to reflect new profitability
			engine.priorityQueues[fanoutEntry.queueIndex].MoveTick(fanoutEntry.queueHandle, newPriority)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS PROCESSING INFRASTRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// packEthereumAddress converts hex address strings to optimized internal representation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func packEthereumAddress(address40HexChars []byte) PackedAddress {
	// Parse the 40-character hex string into a 20-byte binary address
	parsedAddress := utils.ParseEthereumAddress(address40HexChars)

	// Pack the 20-byte address into three 64-bit words for efficient comparison
	word0 := utils.Load64(parsedAddress[0:8])                       // Load first 8 bytes
	word1 := utils.Load64(parsedAddress[8:16])                      // Load middle 8 bytes
	word2 := uint64(*(*uint32)(unsafe.Pointer(&parsedAddress[16]))) // Load last 4 bytes as uint64

	return PackedAddress{
		words: [3]uint64{word0, word1, word2},
	}
}

// hashAddressToIndex computes hash table indices from raw hex addresses.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func hashAddressToIndex(address40HexChars []byte) uint64 {
	// Parse the last 16 hex characters of the address as a 64-bit hash value
	// This provides good distribution across the hash table
	hash64 := utils.ParseHexU64(address40HexChars[12:28])

	// Mask the hash to fit within the hash table bounds
	return hash64 & uint64(constants.AddressTableMask)
}

// hashPackedAddressToIndex computes hash values from stored PackedAddress structures.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func hashPackedAddressToIndex(key PackedAddress) uint64 {
	// Use the third word as the hash value since it contains the address suffix
	// Mask it to fit within the hash table bounds
	return key.words[2] & uint64(constants.AddressTableMask)
}

// isEqual performs efficient comparison between PackedAddress structures.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (a PackedAddress) isEqual(b PackedAddress) bool {
	// Compare all three 64-bit words - addresses are equal only if all words match exactly
	return a.words[0] == b.words[0] &&
		a.words[1] == b.words[1] &&
		a.words[2] == b.words[2]
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MONITORING AND OBSERVABILITY
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// emitArbitrageOpportunity provides concise logging for profitable arbitrage cycles.
// Reports cycle details and total profitability in single-line format.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func emitArbitrageOpportunity(cycle *ArbitrageCycleState, newTick float64) {
	// Calculate total profitability for the opportunity
	totalProfit := newTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]

	// Single-line arbitrage opportunity report matching main.go style
	opportunity := "(" + utils.Itoa(int(cycle.pairIDs[0])) + ")→(" +
		utils.Itoa(int(cycle.pairIDs[1])) + ")→(" +
		utils.Itoa(int(cycle.pairIDs[2])) + ") profit=" +
		utils.Ftoa(totalProfit)

	debug.DropMessage("ARB", opportunity)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION AND CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// RegisterTradingPairAddress populates the Robin Hood hash table with address-to-pair mappings.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterTradingPairAddress(address40HexChars []byte, pairID TradingPairID) {
	// Convert the hex address to packed format for efficient storage and comparison
	key := packEthereumAddress(address40HexChars)

	// Calculate the initial position in the hash table
	i := hashAddressToIndex(address40HexChars)
	dist := uint64(0) // Track how far we've moved from the ideal position

	// Robin Hood hash table insertion with displacement
	for {
		currentPairID := addressToPairMap[i]

		// If we find an empty slot, insert our entry here
		if currentPairID == 0 {
			packedAddressKeys[i] = key
			addressToPairMap[i] = pairID
			return
		}

		// If we find an existing entry with the same key, update it
		if packedAddressKeys[i].isEqual(key) {
			addressToPairMap[i] = pairID // Update existing entry
			return
		}

		// Robin Hood displacement: check if we should displace the current entry
		// If the current entry has traveled less distance than us, we displace it
		currentKey := packedAddressKeys[i]
		currentKeyHash := hashPackedAddressToIndex(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)

		if currentDist < dist {
			// Displace the current entry (it's closer to its ideal position than we are)
			// Swap our entry with the current entry and continue inserting the displaced entry
			key, packedAddressKeys[i] = packedAddressKeys[i], key
			pairID, addressToPairMap[i] = addressToPairMap[i], pairID
			dist = currentDist
		}

		// Move to the next slot and increment our distance
		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++
	}
}

// RegisterPairToCoreRouting establishes pair-to-core routing assignments for load distribution.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairToCoreRouting(pairID TradingPairID, coreID uint8) {
	// Add this core to the bitmask of cores that should receive updates for this pair
	// Multiple cores can process the same pair for load balancing and redundancy
	pairToCoreRouting[pairID] |= 1 << coreID
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CRYPTOGRAPHIC RANDOM GENERATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// newCryptoRandomGenerator creates deterministic random number generators for load balancing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func newCryptoRandomGenerator(initialSeed []byte) *CryptoRandomGenerator {
	// Initialize a 256-bit seed array
	var seed [32]byte

	// Hash the initial seed to create a uniform 256-bit cryptographic seed
	// This ensures good randomness distribution regardless of input seed quality
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(initialSeed)
	copy(seed[:], hasher.Sum(nil))

	// Create and return the random generator with initialized state
	return &CryptoRandomGenerator{
		counter: 0,                         // Start with counter at zero
		seed:    seed,                      // Use the hashed seed
		hasher:  sha3.NewLegacyKeccak256(), // Create a fresh hasher for generation
	}
}

// generateRandomUint64 generates cryptographically strong random values in deterministic sequences.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (rng *CryptoRandomGenerator) generateRandomUint64() uint64 {
	// Create input buffer containing seed + counter for unique hash input
	var input [40]byte
	copy(input[:32], rng.seed[:])                        // Copy the 32-byte seed
	*(*uint64)(unsafe.Pointer(&input[32])) = rng.counter // Append the 8-byte counter

	// Generate a hash from the seed+counter combination
	rng.hasher.Reset()
	rng.hasher.Write(input[:])
	output := rng.hasher.Sum(nil)

	// Increment counter for the next generation to ensure unique sequences
	rng.counter++

	// Extract and return the first 64 bits from the hash output
	return utils.Load64(output[:8])
}

// generateRandomInt generates random integers within specified bounds for distribution algorithms.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (rng *CryptoRandomGenerator) generateRandomInt(upperBound int) int {
	// Generate a random 64-bit value and reduce it to the desired range
	// This provides uniform distribution across the range [0, upperBound)
	return int(rng.generateRandomUint64() % uint64(upperBound))
}

// shuffleCycleEdges performs deterministic Fisher-Yates shuffling for load balancing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func shuffleCycleEdges(cycleEdges []CycleEdge, pairID TradingPairID) {
	// Skip shuffling if there's nothing to shuffle or only one element
	if len(cycleEdges) <= 1 {
		return
	}

	// Create a deterministic seed based on the pair ID for reproducible shuffling
	// This ensures the same pair ID always produces the same shuffle order
	var seedInput [8]byte
	*(*uint64)(unsafe.Pointer(&seedInput[0])) = utils.Mix64(uint64(pairID))
	rng := newCryptoRandomGenerator(seedInput[:])

	// Perform Fisher-Yates shuffle algorithm for uniform random permutation
	// Start from the last element and work backwards to the second element
	for i := len(cycleEdges) - 1; i > 0; i-- {
		// Generate a random index in the range [0, i] inclusive
		j := rng.generateRandomInt(i + 1)
		// Swap the current element with the randomly selected element
		cycleEdges[i], cycleEdges[j] = cycleEdges[j], cycleEdges[i]
	}
}

// buildWorkloadShards distributes arbitrage cycles across processing cores for load balancing.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func buildWorkloadShards(arbitrageTriangles []ArbitrageTriangle) {
	// Count edges per pair through complete traversal
	edgeCounts := make(map[TradingPairID]int)
	for _, triangle := range arbitrageTriangles {
		for i := 0; i < 3; i++ {
			edgeCounts[triangle[i]]++
		}
	}

	// Calculate shard requirements per pair
	shardCounts := make(map[TradingPairID]int)
	for pairID, edgeCount := range edgeCounts {
		shardCount := (edgeCount + constants.MaxCyclesPerShard - 1) / constants.MaxCyclesPerShard
		shardCounts[pairID] = shardCount
	}

	// Pre-allocate all structures with exact capacities
	pairWorkloadShards = make(map[TradingPairID][]PairWorkloadShard, len(edgeCounts))
	temporaryEdges := make(map[TradingPairID][]CycleEdge, len(edgeCounts))

	for pairID, edgeCount := range edgeCounts {
		temporaryEdges[pairID] = make([]CycleEdge, edgeCount)
		shardCount := shardCounts[pairID]
		pairWorkloadShards[pairID] = make([]PairWorkloadShard, shardCount)
	}

	// Populate edges using direct indexing to avoid reallocations
	edgeIndices := make(map[TradingPairID]int, len(edgeCounts))

	for _, triangle := range arbitrageTriangles {
		for i := 0; i < 3; i++ {
			pairID := triangle[i]
			idx := edgeIndices[pairID]
			temporaryEdges[pairID][idx] = CycleEdge{
				cyclePairs: triangle,
				edgeIndex:  uint64(i),
			}
			edgeIndices[pairID]++
		}
	}

	// Create shards with deterministic shuffle for load balancing
	for pairID, cycleEdges := range temporaryEdges {
		shuffleCycleEdges(cycleEdges, pairID)

		shardIdx := 0
		for offset := 0; offset < len(cycleEdges); offset += constants.MaxCyclesPerShard {
			endOffset := offset + constants.MaxCyclesPerShard
			if endOffset > len(cycleEdges) {
				endOffset = len(cycleEdges)
			}

			pairWorkloadShards[pairID][shardIdx] = PairWorkloadShard{
				pairID:     pairID,
				cycleEdges: cycleEdges[offset:endOffset],
			}
			shardIdx++
		}
	}

	temporaryEdges = nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// QUEUE INITIALIZATION SYSTEM
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// initializeArbitrageQueues allocates priority queues and fanout tables for core arbitrage processing.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func initializeArbitrageQueues(engine *ArbitrageEngine, workloadShards []PairWorkloadShard) {
	if len(workloadShards) == 0 {
		return
	}

	// Count total cycles and identify all pairs through complete traversal
	totalCycles := 0
	allPairsSet := make(map[TradingPairID]struct{})
	pairsWithQueuesSet := make(map[TradingPairID]struct{})

	for _, shard := range workloadShards {
		pairsWithQueuesSet[shard.pairID] = struct{}{}
		totalCycles += len(shard.cycleEdges)

		for _, cycleEdge := range shard.cycleEdges {
			for i := 0; i < 3; i++ {
				allPairsSet[cycleEdge.cyclePairs[i]] = struct{}{}
			}
		}
	}

	totalQueues := len(pairsWithQueuesSet)
	totalFanoutSlots := len(allPairsSet)

	// Calculate exact fanout entries per slot
	fanoutCounts := make(map[TradingPairID]int)
	for _, shard := range workloadShards {
		for _, cycleEdge := range shard.cycleEdges {
			otherPair1 := cycleEdge.cyclePairs[(cycleEdge.edgeIndex+1)%3]
			otherPair2 := cycleEdge.cyclePairs[(cycleEdge.edgeIndex+2)%3]

			fanoutCounts[otherPair1]++
			fanoutCounts[otherPair2]++
		}
	}

	// Allocate all arrays with exact sizes
	engine.sharedArena = make([]pooledquantumqueue.Entry, totalCycles)
	engine.priorityQueues = make([]pooledquantumqueue.PooledQuantumQueue, totalQueues)
	engine.cycleFanoutTable = make([][]CycleFanoutEntry, totalFanoutSlots)
	engine.cycleStates = make([]ArbitrageCycleState, totalCycles)

	// Create deterministic ordering and pre-allocate fanout slices
	allPairsList := make([]TradingPairID, totalFanoutSlots)
	pairIdx := 0
	for pairID := range allPairsSet {
		allPairsList[pairIdx] = pairID
		pairIdx++
	}

	for i, pairID := range allPairsList {
		exactCount := fanoutCounts[pairID]
		engine.cycleFanoutTable[i] = make([]CycleFanoutEntry, exactCount)
		engine.pairToFanoutIndex.Put(uint32(pairID), uint32(i))
	}

	// Initialize arena entries with sentinel values
	nilHandle := pooledquantumqueue.Handle(^uint64(0))
	for i := range engine.sharedArena {
		engine.sharedArena[i].Tick = -1
		engine.sharedArena[i].Prev = nilHandle
		engine.sharedArena[i].Next = nilHandle
		engine.sharedArena[i].Data = 0
	}

	// Initialize priority queues with shared arena
	arenaPtr := unsafe.Pointer(&engine.sharedArena[0])
	for i := range engine.priorityQueues {
		newQueue := pooledquantumqueue.New(arenaPtr)
		engine.priorityQueues[i] = *newQueue
	}

	// Create deterministic ordering for queue assignment
	pairsWithQueuesList := make([]TradingPairID, totalQueues)
	queueIdx := 0
	for pairID := range pairsWithQueuesSet {
		pairsWithQueuesList[queueIdx] = pairID
		queueIdx++
	}

	for i, pairID := range pairsWithQueuesList {
		engine.pairToQueueLookup.Put(uint32(pairID), uint32(i))
	}

	// Populate cycles and fanout tables using direct indexing
	cycleStateIdx := 0
	fanoutIndices := make(map[TradingPairID]int, totalFanoutSlots)

	for _, shard := range workloadShards {
		queueIndex, _ := engine.pairToQueueLookup.Get(uint32(shard.pairID))
		queue := &engine.priorityQueues[queueIndex]

		for _, cycleEdge := range shard.cycleEdges {
			handle := engine.allocateQueueHandle()

			engine.cycleStates[cycleStateIdx] = ArbitrageCycleState{
				tickValues: [3]float64{64.0, 64.0, 64.0},
				pairIDs:    cycleEdge.cyclePairs,
			}
			engine.cycleStates[cycleStateIdx].tickValues[cycleEdge.edgeIndex] = 0.0

			cycleHash := utils.Mix64(uint64(cycleStateIdx))
			randBits := cycleHash & 0xFFFF
			initPriority := int64(196608 + randBits)

			queue.Push(initPriority, handle, uint64(cycleStateIdx))

			otherEdge1 := (cycleEdge.edgeIndex + 1) % 3
			otherEdge2 := (cycleEdge.edgeIndex + 2) % 3

			otherPairID1 := cycleEdge.cyclePairs[otherEdge1]
			otherFanoutIndex1, _ := engine.pairToFanoutIndex.Get(uint32(otherPairID1))

			idx1 := fanoutIndices[otherPairID1]
			engine.cycleFanoutTable[otherFanoutIndex1][idx1] = CycleFanoutEntry{
				queueHandle: handle,
				cycleIndex:  uint64(cycleStateIdx),
				queueIndex:  uint64(queueIndex),
				edgeIndex:   otherEdge1,
			}
			fanoutIndices[otherPairID1]++

			otherPairID2 := cycleEdge.cyclePairs[otherEdge2]
			otherFanoutIndex2, _ := engine.pairToFanoutIndex.Get(uint32(otherPairID2))

			idx2 := fanoutIndices[otherPairID2]
			engine.cycleFanoutTable[otherFanoutIndex2][idx2] = CycleFanoutEntry{
				queueHandle: handle,
				cycleIndex:  uint64(cycleStateIdx),
				queueIndex:  uint64(queueIndex),
				edgeIndex:   otherEdge2,
			}
			fanoutIndices[otherPairID2]++

			cycleStateIdx++
		}
	}

	// Calculate total fanout entries for logging
	totalFanoutEntries := 0
	for _, fanoutSlice := range engine.cycleFanoutTable {
		totalFanoutEntries += len(fanoutSlice)
	}

	debug.DropMessage("QUEUES", utils.Itoa(totalQueues)+"q "+utils.Itoa(totalCycles)+"c "+utils.Itoa(totalFanoutEntries)+"f")
}

// launchArbitrageWorker initializes and operates a processing core for arbitrage detection.
// Implements a dual-phase execution model to balance GC cooperation and performance:
//
// Phase 1: GC-Cooperative Processing (Yielding Loop)
//   - Processes events while yielding to scheduler every cycle via runtime.Gosched()
//   - Allows GC to run efficiently without interference from hot spinning cores
//   - Checks for GC completion signal on every iteration
//   - Maintains reasonable throughput while being GC-friendly
//
// Phase 2: Maximum Performance Processing (Hot Spinning Loop)
//   - Pure hot spinning with no yielding or blocking operations
//   - Activated only after main.go completes all GC operations and signals completion
//   - Maximizes throughput for real-time arbitrage detection
//   - GC is disabled in production mode (main.go sets GCPercent=-1)
//
// This approach prevents hot spinning cores from interfering with critical GC phases
// while ensuring maximum performance during production operation.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func launchArbitrageWorker(coreID, forwardCoreCount int, shardInput <-chan PairWorkloadShard, initWaitGroup *sync.WaitGroup) {
	// Lock this goroutine to the current OS thread for consistent NUMA locality
	runtime.LockOSThread()

	// Collect all workload shards assigned to this core before allocating any memory
	// This zero-fragmentation approach ensures we know exact requirements upfront
	var allShards []PairWorkloadShard

	// Receive all shards from the distribution channel
	for shard := range shardInput {
		allShards = append(allShards, shard)
	}

	// Initialize the core processing engine with exact memory allocations
	// This prevents any memory fragmentation during the operational phase
	engine := &ArbitrageEngine{
		pairToQueueLookup:  localidx.New(constants.DefaultLocalIdxSize),
		pairToFanoutIndex:  localidx.New(constants.DefaultLocalIdxSize << 1), // Use shift for doubling
		isReverseDirection: coreID >= forwardCoreCount,
		// Skip padding fields in initialization
		// priorityQueues: will be set in initializeArbitrageQueues
		// cycleStates: will be set in initializeArbitrageQueues
		// cycleFanoutTable: will be set in initializeArbitrageQueues
		// sharedArena: will be set in initializeArbitrageQueues
		// extractedCycles: zero value is fine
		// nextHandle: zero value is fine
	}

	// Register this engine in the global core array for message routing
	coreEngines[coreID] = engine
	coreRings[coreID] = ring24.New(constants.DefaultRingSize)

	// Perform zero-fragmentation initialization of all queue structures
	initializeArbitrageQueues(engine, allShards)

	// Release the workload shards immediately after initialization
	allShards = nil

	// Log successful core initialization with concise format
	debug.DropMessage("CORE", "Core "+utils.Itoa(coreID)+" ready")

	// Signal that this core's initialization is complete
	initWaitGroup.Done()

	// Cache the ring buffer reference for this core to avoid repeated array lookups
	// in the hot loops. This micro-optimization eliminates bounds checking and
	// pointer arithmetic on every message processing cycle.
	ring := coreRings[coreID]

gcCooperativeLoop:
	for {
		// Check if main.go has completed all GC operations and memory optimization
		// Non-blocking select ensures we don't stall event processing
		select {
		case <-gcComplete:
			// GC phase complete - transition to hot spinning mode
			break gcCooperativeLoop
		default:
			// GC still in progress - continue cooperative processing
		}

		// Process any available events during the GC phase
		// This maintains system responsiveness while GC operations complete
		if p := ring.Pop(); p != nil {
			processArbitrageUpdate(engine, (*PriceUpdateMessage)(unsafe.Pointer(p)))
		}

		// Yield to the Go scheduler to allow GC and other goroutines to run
		// This is critical - without yielding, hot spinning would starve GC
		runtime.Gosched()
	}

	for {
		// Process events without any yielding, blocking, or scheduler cooperation
		// This maximizes CPU utilization and minimizes latency for arbitrage detection
		if p := ring.Pop(); p != nil {
			processArbitrageUpdate(engine, (*PriceUpdateMessage)(unsafe.Pointer(p)))
		}
		// Pure hot spin - no runtime.Gosched(), no blocking, maximum performance
	}
}

// InitializeArbitrageSystem orchestrates complete system bootstrap and activation.
// Blocks until all cores are fully initialized and ready for event processing.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func InitializeArbitrageSystem(arbitrageTriangles []ArbitrageTriangle) {
	// Determine the optimal number of CPU cores to use for arbitrage processing
	coreCount := runtime.NumCPU() - 4
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	coreCount &^= 1                    // Ensure even number for paired forward/reverse processing
	forwardCoreCount := coreCount >> 1 // Half the cores handle forward direction arbitrage

	// Build the infrastructure for distributing workload across cores
	buildWorkloadShards(arbitrageTriangles)

	// Count exact total shards for perfect channel sizing
	totalShards := 0
	for _, shardBuckets := range pairWorkloadShards {
		totalShards += len(shardBuckets)
	}

	// Each shard goes to both forward and reverse cores, so multiply by 2
	totalShardDeliveries := totalShards << 1
	maxShardsPerCore := (totalShardDeliveries + coreCount - 1) / coreCount // Ceiling division

	// Create synchronization mechanism for core initialization
	var initWaitGroup sync.WaitGroup
	initWaitGroup.Add(coreCount)

	// Create communication channels with exact buffer capacity
	shardChannels := make([]chan PairWorkloadShard, coreCount)
	for i := range shardChannels {
		// Allocate exact capacity needed to prevent any blocking
		shardChannels[i] = make(chan PairWorkloadShard, maxShardsPerCore)
		go launchArbitrageWorker(i, forwardCoreCount, shardChannels[i], &initWaitGroup)
	}

	// Distribute workload shards across all available CPU cores
	currentCore := 0
	for _, shardBuckets := range pairWorkloadShards {
		for _, shard := range shardBuckets {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			// Send the same shard to both forward and reverse cores
			shardChannels[forwardCore] <- shard
			shardChannels[reverseCore] <- shard

			// Build the routing table that determines which cores receive price updates
			routingMask := uint64(1<<forwardCore | 1<<reverseCore)
			for _, cycleEdge := range shard.cycleEdges {
				for _, pairID := range cycleEdge.cyclePairs {
					pairToCoreRouting[pairID] |= routingMask
				}
			}
			currentCore++
		}
	}

	// Signal completion of workload distribution by closing all channels
	for _, channel := range shardChannels {
		close(channel)
	}

	// Wait for all cores to complete initialization
	initWaitGroup.Wait()

	// Clean up global workload data structures immediately after distribution
	pairWorkloadShards = nil

	debug.DropMessage("CORES", utils.Itoa(coreCount)+" cores ready")
}

// SignalGCComplete signals all worker cores that GC is complete and they can start hot spinning.
// This function should be called from main after GC operations are finished.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SignalGCComplete() {
	close(gcComplete)
}
