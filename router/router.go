// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ TRIANGULAR ARBITRAGE DETECTION ENGINE
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Multi-Core Event Router & Arbitrage Detector
//
// Description:
//   Lock-free multi-core arbitrage detection system using SIMD-optimized parsing and zero-allocation
//   priority queues. Detects profitable opportunities across Uniswap V2 pairs in real-time.
//
// Performance Characteristics:
//   - Event dispatch: 39.50ns end-to-end latency
//   - Address resolution: 14ns Robin Hood lookup
//   - SIMD hex parsing: 1.56ns per operation
//   - Core scaling: Linear up to 64 cores
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
	"main/control"
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
//go:align 8
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
//go:align 8
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
//go:align 8
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
// Fields are ordered by access frequency to optimize CPU cache usage:
//
//go:notinheap
//go:align 64
type ArbitrageEngine struct {
	// CACHE LINE 1: Primary lookups (64B)
	pairToQueueLookup localidx.Hash // 32B - Only pairs with queues
	pairToFanoutIndex localidx.Hash // 32B - ALL pairs (with queues + fanout-only)

	// CACHE LINE 2: Core processing data (64B)
	priorityQueues     []pooledquantumqueue.PooledQuantumQueue // 24B
	cycleStates        []ArbitrageCycleState                   // 24B
	isReverseDirection bool                                    // 1B
	_                  [15]byte                                // Padding

	// CACHE LINE 3: Fanout data (64B)
	cycleFanoutTable [][]CycleFanoutEntry       // 24B
	sharedArena      []pooledquantumqueue.Entry // 24B
	_                [16]byte                   // Padding

	// CACHE LINE 4+: Temporary extraction buffer (rarely used)
	extractedCycles [32]ExtractedCycle // 1024B

	// COLD: Init only
	nextHandle      pooledquantumqueue.Handle
	shutdownChannel <-chan struct{}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS RESOLUTION STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// PackedAddress represents an Ethereum address optimized for hash table operations.
// Ethereum addresses are 20 bytes (160 bits), which we pack into three 64-bit words
// for efficient comparison and hashing operations.
//
//go:notinheap
//go:align 8
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
//go:align 8
type CycleEdge struct {
	cyclePairs [3]TradingPairID // 24B - Complete three-pair arbitrage cycle definition
	edgeIndex  uint64           // 8B - This pair's position (0, 1, or 2) within the cycle
}

// PairWorkloadShard groups arbitrage cycles by trading pair for efficient core distribution.
// During initialization, we organize all cycles by the pairs they contain, then distribute
// these shards across CPU cores for parallel processing.
//
//go:notinheap
//go:align 8
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
	// CACHE LINE GROUP 1: Address lookup (accessed together)
	addressToPairMap  [constants.AddressTableCapacity]TradingPairID
	packedAddressKeys [constants.AddressTableCapacity]PackedAddress

	// CACHE LINE GROUP 2: Routing (accessed after address lookup)
	pairToCoreRouting [constants.PairRoutingTableCapacity]uint64

	// CACHE LINE GROUP 3: Ring buffers (accessed after routing)
	coreRings [constants.MaxSupportedCores]*ring24.Ring

	// COLD: Only accessed during arbitrage processing
	coreEngines [constants.MaxSupportedCores]*ArbitrageEngine

	// COLD: Only during init
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
// ULTRA-HOT PATH: EVENT DISPATCH PIPELINE
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
	// The address slice extracts the 40-character hex address from the log view
	pairID := lookupPairByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if pairID == 0 {
		// This address is not registered in our system, so ignore this event
		return
	}

	// Extract the hex-encoded reserve data from the Uniswap V2 Sync event
	// Skip the "0x" prefix and take the next 128 hex characters containing both reserves
	hexData := logView.Data[2:130]

	// Count leading zeros in each reserve value to determine precision requirements
	// This analysis helps us extract the most significant digits accurately
	leadingZerosA := countHexLeadingZeros(hexData[32:64])  // Analyze reserve A (first token)
	leadingZerosB := countHexLeadingZeros(hexData[96:128]) // Analyze reserve B (second token)

	// Calculate the minimum leading zeros to preserve maximum precision in both reserves
	// Using branchless programming to avoid CPU pipeline stalls from conditional branches
	cond := leadingZerosA - leadingZerosB
	mask := cond >> 31                                                   // Arithmetic right shift creates all-1s mask if cond is negative
	minZeros := leadingZerosB ^ ((leadingZerosA ^ leadingZerosB) & mask) // Branchless min()

	// Calculate where to start extracting meaningful digits from the hex data
	// We skip the data prefix, the first reserve field, and any leading zeros
	offsetA := 2 + 32 + minZeros // Position for reserve A extraction
	offsetB := offsetA + 64      // Reserve B is 64 hex characters after reserve A

	// Calculate how many hex characters we can meaningfully extract from each reserve
	// We're limited by both available digits and our processing capacity
	available := 32 - minZeros                         // Available significant digits after leading zeros
	cond = 16 - available                              // Check if we have at least 16 digits available
	mask = cond >> 31                                  // Create mask for branchless selection
	remaining := available ^ ((16 ^ available) & mask) // Branchless min(16, available)

	// Convert the hex strings to 64-bit unsigned integers representing token reserves
	// These values represent the actual token balances in the Uniswap V2 pair contract
	reserve0 := utils.ParseHexU64(logView.Data[offsetA : offsetA+remaining])
	reserve1 := utils.ParseHexU64(logView.Data[offsetB : offsetB+remaining])

	// Calculate the logarithmic price ratio between the two token reserves
	// This ratio is fundamental for determining arbitrage profitability
	tickValue, err := fastuni.Log2ReserveRatio(reserve0, reserve1)

	// Construct the price update message to send to processing cores
	var message PriceUpdateMessage
	if err != nil {
		// If reserve calculation fails (e.g., due to zero reserves), generate a safe fallback
		// We use deterministic pseudo-randomness to prevent systematic biases in the system
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
	// This gives us the chunk index (0-3) where leading zeros end
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
	// Multiply chunk index by 8 (bytes per chunk) and add the byte offset within the chunk
	return (firstChunk << 3) + firstByte
}

// lookupPairByAddress performs high-speed address resolution using Robin Hood hashing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func lookupPairByAddress(address42HexBytes []byte) TradingPairID {
	// Convert the hex address string to an optimized packed representation for comparison
	key := packEthereumAddress(address42HexBytes)

	// Calculate the initial position in the hash table using the address hash
	i := hashAddressToIndex(address42HexBytes)
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
// HOT PATH: CORE PROCESSING PIPELINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processArbitrageUpdate orchestrates arbitrage detection for incoming price updates.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processArbitrageUpdate(engine *ArbitrageEngine, update *PriceUpdateMessage) {
	// Select tick based on core direction - 100% predictable per core
	var currentTick float64
	if engine.isReverseDirection {
		currentTick = update.reverseTick
	} else {
		currentTick = update.forwardTick
	}

	// Two lookups, but both are predictable Robin Hood accesses
	queueIndex, hasQueue := engine.pairToQueueLookup.Get(uint32(update.pairID))
	fanoutIndex, _ := engine.pairToFanoutIndex.Get(uint32(update.pairID))

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

	// Always process fanout (no branch - might be empty slice)
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

// quantizeTickValue converts floating-point tick values to integer priorities for queue operations.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func quantizeTickValue(tickValue float64) int64 {
	// Convert floating-point profitability to integer priority for the priority queue
	// Add clamping bound to ensure all values are positive, then scale to preserve precision
	return int64((tickValue + constants.TickClampingBound) * constants.QuantizationScale)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// WARM PATH: ADDRESS PROCESSING INFRASTRUCTURE
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
// COOL PATH: MONITORING AND OBSERVABILITY
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// emitArbitrageOpportunity provides detailed logging for profitable arbitrage cycles.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func emitArbitrageOpportunity(cycle *ArbitrageCycleState, newTick float64) {
	// Log the detection of a profitable arbitrage opportunity
	debug.DropMessage("[ARBITRAGE_OPPORTUNITY]", "")

	// Log the three trading pairs that form this arbitrage cycle
	debug.DropMessage("  pair0", utils.Itoa(int(cycle.pairIDs[0])))
	debug.DropMessage("  pair1", utils.Itoa(int(cycle.pairIDs[1])))
	debug.DropMessage("  pair2", utils.Itoa(int(cycle.pairIDs[2])))

	// Convert all tick values to strings for detailed logging
	tick0Str := utils.Ftoa(cycle.tickValues[0])
	tick1Str := utils.Ftoa(cycle.tickValues[1])
	tick2Str := utils.Ftoa(cycle.tickValues[2])
	newTickStr := utils.Ftoa(newTick)

	// Calculate and log the total profitability of this opportunity
	totalProfitStr := utils.Ftoa(newTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2])

	// Log all individual tick values and the total profit for analysis
	debug.DropMessage("  tick0", tick0Str)
	debug.DropMessage("  tick1", tick1Str)
	debug.DropMessage("  tick2", tick2Str)
	debug.DropMessage("  newTick", newTickStr)
	debug.DropMessage("  totalProfit", totalProfitStr)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COLD PATH: SYSTEM INITIALIZATION AND CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// RegisterTradingPairAddress populates the Robin Hood hash table with address-to-pair mappings.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterTradingPairAddress(address42HexBytes []byte, pairID TradingPairID) {
	// Convert the hex address to packed format for efficient storage and comparison
	key := packEthereumAddress(address42HexBytes)

	// Calculate the initial position in the hash table
	i := hashAddressToIndex(address42HexBytes)
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
// COLD PATH: CRYPTOGRAPHIC RANDOM GENERATION
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

// buildWorkloadShards constructs the fanout mapping infrastructure for cycle distribution.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func buildWorkloadShards(arbitrageTriangles []ArbitrageTriangle) {
	// Initialize the global workload mapping structure
	pairWorkloadShards = make(map[TradingPairID][]PairWorkloadShard)

	// Create temporary storage to organize cycles by the trading pairs they contain
	// Each triangle has 3 pairs, so we estimate capacity as triangles * 3
	temporaryEdges := make(map[TradingPairID][]CycleEdge, len(arbitrageTriangles)*3)

	// Process each arbitrage triangle and map it to its constituent trading pairs
	for _, triangle := range arbitrageTriangles {
		// Each triangle consists of exactly 3 trading pairs
		for i := 0; i < 3; i++ {
			// Add this triangle to the list of cycles for each of its pairs
			// The edgeIndex tracks which position this pair holds within the triangle
			temporaryEdges[triangle[i]] = append(temporaryEdges[triangle[i]],
				CycleEdge{cyclePairs: triangle, edgeIndex: uint64(i)})
		}
	}

	// Convert the temporary mapping into properly sized workload shards
	for pairID, cycleEdges := range temporaryEdges {
		// Shuffle the cycles for this pair to ensure even load distribution
		shuffleCycleEdges(cycleEdges, pairID)

		// Split the cycles into shards of manageable size for core assignment
		for offset := 0; offset < len(cycleEdges); offset += constants.MaxCyclesPerShard {
			// Calculate the end offset for this shard, ensuring we don't exceed the slice
			endOffset := offset + constants.MaxCyclesPerShard
			if endOffset > len(cycleEdges) {
				endOffset = len(cycleEdges)
			}

			// Create a workload shard for this pair containing a subset of its cycles
			pairWorkloadShards[pairID] = append(pairWorkloadShards[pairID],
				PairWorkloadShard{pairID: pairID, cycleEdges: cycleEdges[offset:endOffset]})
		}
	}

	// Clean up temporary structures and force garbage collection for memory efficiency
	temporaryEdges = nil
	runtime.GC()
	runtime.GC()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COLD PATH: CLEAN QUEUE INITIALIZATION SYSTEM
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// initializeArbitrageQueues allocates shared arena and initializes all queues with cycle data.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func initializeArbitrageQueues(engine *ArbitrageEngine, workloadShards []PairWorkloadShard) {
	// Handle the case where no workload shards are provided
	if len(workloadShards) == 0 {
		return
	}

	// First, identify all unique pairs across all cycles
	allPairs := make(map[TradingPairID]bool)
	pairsWithQueues := make(map[TradingPairID]bool)

	// Identify which pairs have their own cycles (need queues)
	for _, shard := range workloadShards {
		pairsWithQueues[shard.pairID] = true
		// Also track ALL pairs that appear in any cycle
		for _, cycleEdge := range shard.cycleEdges {
			for i := 0; i < 3; i++ {
				allPairs[cycleEdge.cyclePairs[i]] = true
			}
		}
	}

	// Calculate exact memory requirements
	totalCycles := 0
	totalQueues := len(pairsWithQueues) // Only pairs with cycles get queues
	totalFanoutEntries := 0

	// Count cycles
	for _, shard := range workloadShards {
		totalCycles += len(shard.cycleEdges)
	}

	// Initialize fanout index for ALL pairs (including fanout-only)
	fanoutIndex := uint32(0)
	for pairID := range allPairs {
		engine.pairToFanoutIndex.Put(uint32(pairID), fanoutIndex)
		fanoutIndex++
	}
	totalFanoutSlots := int(fanoutIndex)

	// Allocate shared arena and cycle states
	engine.sharedArena = make([]pooledquantumqueue.Entry, totalCycles)
	engine.nextHandle = 0
	engine.cycleStates = make([]ArbitrageCycleState, 0, totalCycles)

	// Allocate priority queues ONLY for pairs with cycles
	engine.priorityQueues = make([]pooledquantumqueue.PooledQuantumQueue, totalQueues)

	// Allocate fanout table for ALL pairs
	engine.cycleFanoutTable = make([][]CycleFanoutEntry, totalFanoutSlots)

	// Initialize arena entries
	nilHandle := pooledquantumqueue.Handle(^uint64(0))
	for i := range engine.sharedArena {
		engine.sharedArena[i].Tick = -1
		engine.sharedArena[i].Prev = nilHandle
		engine.sharedArena[i].Next = nilHandle
		engine.sharedArena[i].Data = 0
	}

	// Initialize priority queues
	arenaPtr := unsafe.Pointer(&engine.sharedArena[0])
	for i := range engine.priorityQueues {
		newQueue := pooledquantumqueue.New(arenaPtr)
		engine.priorityQueues[i] = *newQueue
	}

	// Assign queue indices only to pairs with cycles
	queueIdx := uint32(0)
	for pairID := range pairsWithQueues {
		engine.pairToQueueLookup.Put(uint32(pairID), queueIdx)
		queueIdx++
	}

	// Process workload shards and create cycles
	for _, shard := range workloadShards {
		// Get queue index for this pair (guaranteed to exist)
		queueIndex, _ := engine.pairToQueueLookup.Get(uint32(shard.pairID))
		queue := &engine.priorityQueues[queueIndex]

		for _, cycleEdge := range shard.cycleEdges {
			handle := engine.allocateQueueHandle()

			// Create cycle state with main pair tick as zero
			cycleState := ArbitrageCycleState{
				pairIDs: cycleEdge.cyclePairs,
				// tickValues[cycleEdge.edgeIndex] remains zero (main pair)
			}

			engine.cycleStates = append(engine.cycleStates, cycleState)
			cycleIndex := CycleIndex(len(engine.cycleStates) - 1)

			// Generate initial priority
			cycleHash := utils.Mix64(uint64(cycleIndex))
			randBits := cycleHash & 0xFFFF
			initPriority := int64(196608 + randBits)

			// Insert into queue
			queue.Push(initPriority, handle, uint64(cycleIndex))

			// Create fanout entries for the OTHER pairs in the triangle
			for edgeIdx := uint64(0); edgeIdx < 3; edgeIdx++ {
				if edgeIdx == cycleEdge.edgeIndex {
					continue // Skip the main pair
				}

				otherPairID := cycleEdge.cyclePairs[edgeIdx]
				otherFanoutIndex, exists := engine.pairToFanoutIndex.Get(uint32(otherPairID))
				if !exists {
					panic("Fanout index should exist for all pairs")
				}

				// Add fanout entry to the OTHER pair's fanout table
				engine.cycleFanoutTable[otherFanoutIndex] = append(
					engine.cycleFanoutTable[otherFanoutIndex],
					CycleFanoutEntry{
						queueHandle: handle,
						cycleIndex:  uint64(cycleIndex),
						queueIndex:  uint64(queueIndex), // Queue where cycle lives
						edgeIndex:   edgeIdx,            // Position in the cycle
					})
				totalFanoutEntries++
			}
		}
	}

	// Log initialization success
	debug.DropMessage("ZERO_FRAG_INIT",
		"Initialized "+utils.Itoa(totalQueues)+" queues, "+
			utils.Itoa(totalCycles)+" cycles, "+
			utils.Itoa(totalFanoutEntries)+" fanout entries, "+
			utils.Itoa(totalFanoutSlots)+" fanout slots - zero fragmentation")
}

// launchArbitrageWorker initializes and operates a processing core for arbitrage detection.
// Signals completion via WaitGroup after all initialization is complete.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func launchArbitrageWorker(coreID, forwardCoreCount int, shardInput <-chan PairWorkloadShard, initWaitGroup *sync.WaitGroup) {
	// Ensure we signal completion regardless of initialization outcome
	defer initWaitGroup.Done()

	// Lock this goroutine to the current OS thread for consistent NUMA locality and performance
	runtime.LockOSThread()

	// Create a channel for coordinating graceful shutdown of this worker
	shutdownChannel := make(chan struct{})

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
		pairToFanoutIndex:  localidx.New(constants.DefaultLocalIdxSize * 2), // Larger for all pairs
		isReverseDirection: coreID >= forwardCoreCount,
		shutdownChannel:    shutdownChannel,
	}

	// Register this engine in the global core array for message routing
	coreEngines[coreID] = engine
	coreRings[coreID] = ring24.New(constants.DefaultRingSize)

	// Perform zero-fragmentation initialization of all queue structures
	initializeArbitrageQueues(engine, allShards)

	// Release the workload shards now that initialization is complete
	allShards = nil

	// Integrate with the control system for monitoring and shutdown coordination
	stopFlag, hotFlag := control.Flags()
	control.SignalActivity() // Signal that this core is active and ready

	// Log successful core initialization
	debug.DropMessage("CORE_READY", "Core "+utils.Itoa(coreID)+" initialized")

	// Start the main processing loop - this is the core's primary work function
	// PinnedConsumer runs a tight loop consuming messages from the ring buffer
	ring24.PinnedConsumer(coreID, coreRings[coreID], stopFlag, hotFlag,
		func(messagePtr *[24]byte) {
			// Convert the raw message bytes to a price update and process it
			processArbitrageUpdate(engine, (*PriceUpdateMessage)(unsafe.Pointer(messagePtr)))
		}, shutdownChannel)
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
	// Reserve cores for system tasks and other processes
	coreCount := runtime.NumCPU() - 4
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores // Respect system limits
	}
	coreCount &^= 1                    // Ensure even number for paired forward/reverse processing
	forwardCoreCount := coreCount >> 1 // Half the cores handle forward direction arbitrage

	// Build the infrastructure for distributing workload across cores
	// This creates shards of cycles organized by trading pairs
	buildWorkloadShards(arbitrageTriangles)

	// Create synchronization mechanism for core initialization
	var initWaitGroup sync.WaitGroup
	initWaitGroup.Add(coreCount) // Wait for all cores to initialize

	// Create communication channels and launch worker goroutines for each core
	shardChannels := make([]chan PairWorkloadShard, coreCount)
	for i := range shardChannels {
		// Create buffered channels to prevent blocking during workload distribution
		shardChannels[i] = make(chan PairWorkloadShard, constants.ShardChannelBufferSize)
		// Launch a worker goroutine for this core with synchronization
		go launchArbitrageWorker(i, forwardCoreCount, shardChannels[i], &initWaitGroup)
	}

	// Distribute workload shards across all available CPU cores
	// We use round-robin distribution to ensure even load balancing
	currentCore := 0
	for _, shardBuckets := range pairWorkloadShards {
		for _, shard := range shardBuckets {
			// Calculate which cores should handle this shard
			forwardCore := currentCore % forwardCoreCount // Forward direction core
			reverseCore := forwardCore + forwardCoreCount // Reverse direction core

			// Send the same shard to both forward and reverse cores
			// This enables bidirectional arbitrage detection for the same cycles
			shardChannels[forwardCore] <- shard
			shardChannels[reverseCore] <- shard

			// Build the routing table that determines which cores receive price updates
			// Use bitmasks to efficiently represent multi-core routing
			routingMask := uint64(1<<forwardCore | 1<<reverseCore)
			for _, cycleEdge := range shard.cycleEdges {
				// Add routing entries for all pairs in all cycles in this shard
				for _, pairID := range cycleEdge.cyclePairs {
					pairToCoreRouting[pairID] |= routingMask
				}
			}
			currentCore++ // Move to next core for round-robin distribution
		}
	}

	// Signal completion of workload distribution by closing all channels
	// This tells the worker goroutines that no more shards will be sent
	for _, channel := range shardChannels {
		close(channel)
	}

	// Wait for all cores to complete initialization
	// This ensures the system is fully ready before returning
	initWaitGroup.Wait()

	// Clean up global workload data structures and force garbage collection
	// These are no longer needed after distribution is complete
	pairWorkloadShards = nil
	runtime.GC()
	runtime.GC()

	// Log successful system initialization
	debug.DropMessage("SYSTEM_READY", "All "+utils.Itoa(coreCount)+" cores initialized and ready for processing")
}
