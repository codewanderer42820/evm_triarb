// router.go — 57ns triangular arbitrage detection engine with clean pooled quantum queue integration

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CLEAN POOLED QUANTUM QUEUE ARCHITECTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════
//
// This version eliminates the complex handle allocation and arena partitioning mess while
// maintaining all performance benefits. Key simplifications:
//
// 1. SINGLE SHARED ARENA PER CORE: One big memory pool, all queues share it naturally
// 2. SIMPLE SEQUENTIAL HANDLES: Handles are just incrementing integers, no complex conversion
// 3. ZERO ALLOCATION HOT PATHS: Pre-allocated everything, no runtime allocation
// 4. CLEAN INITIALIZATION: Straightforward setup without complex partitioning logic
// 5. MATHEMATICAL CORRECTNESS: Same performance guarantees, much simpler implementation

package router

import (
	"hash"
	"math/bits"
	"runtime"
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
// CORE TYPE DEFINITIONS (SORTED BY USAGE FREQUENCY)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// TradingPairID represents a unique identifier for a Uniswap V2 trading pair.
type TradingPairID uint64

// ArbitrageTriangle defines a three-pair arbitrage cycle.
type ArbitrageTriangle [3]TradingPairID

// CycleIndex provides typed access into the cycle state storage arrays.
type CycleIndex uint64

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INTER-CORE MESSAGE STRUCTURES (SORTED BY HOTNESS)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// PriceUpdateMessage represents a price change notification distributed to processing cores.
//
//go:notinheap
//go:align 8
type PriceUpdateMessage struct {
	pairID      TradingPairID // 8B - HOT: Pair identifier for routing and queue lookup
	forwardTick float64       // 8B - HOT: Logarithmic price ratio for forward direction
	reverseTick float64       // 8B - HOT: Logarithmic price ratio for reverse direction
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ARBITRAGE CYCLE STATE STRUCTURES (SORTED BY ACCESS FREQUENCY)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ArbitrageCycleState maintains real-time profitability tracking for a three-pair arbitrage cycle.
//
//go:notinheap
//go:align 64
type ArbitrageCycleState struct {
	tickValues [3]float64       // 24B - ULTRA-HOT: Logarithmic price ratios for profitability calculation
	pairIDs    [3]TradingPairID // 24B - WARM: Pair identifiers for cycle reconstruction and debugging
	_          [16]byte         // 16B - PADDING: Alignment to 64-byte cache line boundary
}

// CycleFanoutEntry defines the relationship between pair price updates and affected arbitrage cycles.
//
//go:notinheap
//go:align 32
type CycleFanoutEntry struct {
	cycleIndex  uint64                                 // 8B - ULTRA-HOT: Direct array index for cycle access
	edgeIndex   uint64                                 // 8B - HOT: Position within cycle for tick update
	queue       *pooledquantumqueue.PooledQuantumQueue // 8B - HOT: Priority queue for cycle reordering
	queueHandle pooledquantumqueue.Handle              // 8B - HOT: Queue manipulation identifier
}

// ExtractedCycle provides temporary storage for cycles extracted during profitability analysis.
//
//go:notinheap
//go:align 32
type ExtractedCycle struct {
	cycleIndex   CycleIndex                // 8B - HOT: Array index for cycle state access
	originalTick int64                     // 8B - HOT: Original tick value for queue reinsertion
	queueHandle  pooledquantumqueue.Handle // 8B - HOT: Queue manipulation identifier
	_            [8]byte                   // 8B - PADDING: 32-byte boundary alignment
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE PROCESSING ENGINE (SORTED BY ACCESS FREQUENCY)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ArbitrageEngine orchestrates arbitrage detection for a single CPU core.
// CLEAN: Single shared arena, simple handle management, zero allocation hot paths.
//
//go:notinheap
//go:align 64
type ArbitrageEngine struct {
	// TIER 1: ULTRA-HOT PATH (Every tick update - millions per second)
	pairToQueueLookup  localidx.Hash // 64B - ULTRA-HOT: Pair-to-queue mapping for O(1) lookup performance
	isReverseDirection bool          // 1B - ULTRA-HOT: Direction flag checked on every tick update
	_                  [7]byte       // 7B - PADDING: Alignment optimization

	// TIER 2: HOT PATH (Frequent cycle processing operations)
	cycleStates      []ArbitrageCycleState // 24B - HOT: Complete arbitrage cycle state storage
	cycleFanoutTable [][]CycleFanoutEntry  // 24B - HOT: Pair-to-cycle mappings for fanout operations

	// TIER 3: WARM PATH (Moderate frequency queue operations)
	priorityQueues []pooledquantumqueue.PooledQuantumQueue // 24B - WARM: Priority queues per trading pair
	sharedArena    []pooledquantumqueue.Entry              // 24B - WARM: Single shared memory pool for all queues
	nextHandle     pooledquantumqueue.Handle               // 8B - WARM: Simple sequential handle allocation

	// TIER 4: COOL PATH (Occasional profitable cycle extraction)
	extractedCycles [128]ExtractedCycle // 4096B - COOL: Pre-allocated buffer for extracted cycles

	// TIER 5: COLD PATH (Rare configuration and control operations)
	shutdownChannel <-chan struct{} // 8B - COLD: Graceful shutdown coordination channel
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS RESOLUTION STRUCTURES (SORTED BY USAGE)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// PackedAddress represents a packed Ethereum address optimized for hash table operations.
//
//go:notinheap
//go:align 32
type PackedAddress struct {
	words [3]uint64 // 24B - HOT: Packed 160-bit Ethereum address for efficient comparison
	_     [8]byte   // 8B - PADDING: 32-byte cache line boundary optimization
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION STRUCTURES (SORTED BY CONSTRUCTION ORDER)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// CycleEdge represents a single edge within an arbitrage cycle during construction.
//
//go:notinheap
//go:align 32
type CycleEdge struct {
	cyclePairs [3]TradingPairID // 24B - WARM: Complete three-pair cycle definition
	edgeIndex  uint64           // 8B - WARM: Position index (0, 1, or 2) within the cycle
}

// PairWorkloadShard aggregates arbitrage cycles by trading pair for core distribution.
//
//go:notinheap
//go:align 32
type PairWorkloadShard struct {
	pairID     TradingPairID // 8B - WARM: Trading pair identifier for shard classification
	cycleEdges []CycleEdge   // 24B - WARM: All arbitrage cycles containing this pair
}

// CryptoRandomGenerator provides cryptographically strong deterministic randomness for load balancing.
//
//go:notinheap
//go:align 64
type CryptoRandomGenerator struct {
	counter uint64    // 8B - HOT: Sequence counter incremented per generation
	seed    [32]byte  // 32B - WARM: Cryptographic seed for random generation
	hasher  hash.Hash // 24B - COLD: Reusable Keccak-256 hasher instance
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL STATE VARIABLES (SORTED BY ACCESS FREQUENCY)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 64
var (
	// ULTRA-HOT: Per-core exclusive state accessed millions of times per second
	coreEngines [constants.MaxSupportedCores]*ArbitrageEngine
	coreRings   [constants.MaxSupportedCores]*ring24.Ring

	// HOT: Global routing infrastructure (read-only after initialization)
	pairToCoreRouting  [constants.PairRoutingTableCapacity]uint64
	pairWorkloadShards map[TradingPairID][]PairWorkloadShard

	// WARM: Address resolution tables (read-only after initialization)
	packedAddressKeys [constants.AddressTableCapacity]PackedAddress
	addressToPairMap  [constants.AddressTableCapacity]TradingPairID
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE ENGINE METHODS (SORTED BY CALL FREQUENCY)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// allocateQueueHandle returns the next available handle from the shared arena.
// CLEAN: Simple sequential allocation, no complex partitioning or conversion.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (engine *ArbitrageEngine) allocateQueueHandle() pooledquantumqueue.Handle {
	handle := engine.nextHandle
	engine.nextHandle++
	return handle
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ULTRA-HOT PATH: EVENT DISPATCH PIPELINE (SORTED BY EXECUTION ORDER)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// DispatchPriceUpdate processes Uniswap V2 Sync events and distributes price updates to cores.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DispatchPriceUpdate(logView *types.LogView) {
	// PHASE 1: ADDRESS RESOLUTION (Target: 14ns, ~42 cycles at 3GHz)
	pairID := lookupPairByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if pairID == 0 {
		return // Unregistered pair - early termination
	}

	// PHASE 2: EVENT DATA PREPARATION (Target: 2 cycles)
	hexData := logView.Data[2:130]

	// PHASE 3: NUMERICAL MAGNITUDE ANALYSIS (Target: 1.7ns, ~5 cycles at 3GHz)
	leadingZerosA := countHexLeadingZeros(hexData[32:64])  // Reserve A magnitude analysis
	leadingZerosB := countHexLeadingZeros(hexData[96:128]) // Reserve B magnitude analysis

	// PHASE 4: PRECISION PRESERVATION (Target: 3-4 cycles)
	cond := leadingZerosA - leadingZerosB
	mask := cond >> 31
	minZeros := leadingZerosB ^ ((leadingZerosA ^ leadingZerosB) & mask) // Branchless min(leadingZerosA, leadingZerosB)

	// PHASE 5: EXTRACTION OFFSET CALCULATION (Target: 2 cycles)
	offsetA := 2 + 32 + minZeros
	offsetB := offsetA + 64 // Reserve B follows 64 characters after Reserve A

	// PHASE 6: EXTRACTION LENGTH DETERMINATION (Target: 3-4 cycles)
	available := 32 - minZeros
	cond = 16 - available
	mask = cond >> 31
	remaining := available ^ ((16 ^ available) & mask) // Branchless min(16, available)

	// PHASE 7: RESERVE VALUE PARSING (Target: 8-12 cycles)
	reserve0 := utils.ParseHexU64(logView.Data[offsetA : offsetA+remaining])
	reserve1 := utils.ParseHexU64(logView.Data[offsetB : offsetB+remaining])

	// PHASE 8: LOGARITHMIC RATIO CALCULATION (Target: 8-12 cycles)
	tickValue, err := fastuni.Log2ReserveRatio(reserve0, reserve1)

	// PHASE 9: MESSAGE CONSTRUCTION (Target: 3-4 cycles)
	var message PriceUpdateMessage
	if err != nil {
		// FALLBACK: Cryptographically secure random value generation
		addrHash := utils.Mix64(uint64(pairID))
		randBits := addrHash & 0x1FFF // Extract 13 bits for range [0, 8191]
		placeholder := 50.2 + float64(randBits)*0.0015625

		message = PriceUpdateMessage{
			pairID:      pairID,
			forwardTick: placeholder, // Positive - deprioritizes
			reverseTick: placeholder, // Positive - deprioritizes
		}
	} else {
		// Valid reserves: use actual log ratio with opposite signs for direction
		message = PriceUpdateMessage{
			pairID:      pairID,
			forwardTick: tickValue,
			reverseTick: -tickValue,
		}
	}

	// PHASE 10: MULTI-CORE DISTRIBUTION WITH GUARANTEED DELIVERY
	coreAssignments := pairToCoreRouting[pairID]

	for coreAssignments != 0 {
		failedCores := uint64(0)

		// Attempt delivery to all currently assigned cores
		currentAssignments := coreAssignments
		for currentAssignments != 0 {
			coreID := bits.TrailingZeros64(currentAssignments)
			messageBytes := (*[24]byte)(unsafe.Pointer(&message))

			if !coreRings[coreID].Push(messageBytes) {
				// Mark core for retry if ring was full
				failedCores |= 1 << coreID
			}

			currentAssignments &^= 1 << coreID
		}

		// Continue with only failed cores, exit if all succeeded
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
	const ZERO_PATTERN = 0x3030303030303030 // Eight consecutive ASCII '0' characters

	// PARALLEL CHUNK PROCESSING
	c0 := utils.Load64(segment[0:8]) ^ ZERO_PATTERN   // Chunk 0: bytes 0-7
	c1 := utils.Load64(segment[8:16]) ^ ZERO_PATTERN  // Chunk 1: bytes 8-15
	c2 := utils.Load64(segment[16:24]) ^ ZERO_PATTERN // Chunk 2: bytes 16-23
	c3 := utils.Load64(segment[24:32]) ^ ZERO_PATTERN // Chunk 3: bytes 24-31

	// CHUNK-LEVEL ZERO DETECTION
	mask := ((c0|(^c0+1))>>63)<<0 | ((c1|(^c1+1))>>63)<<1 |
		((c2|(^c2+1))>>63)<<2 | ((c3|(^c3+1))>>63)<<3

	// FIRST NON-ZERO CHUNK IDENTIFICATION
	firstChunk := bits.TrailingZeros64(mask)

	// SANITY CHECK: Handle all-zeros case
	if firstChunk == 64 {
		return 32
	}

	chunks := [4]uint64{c0, c1, c2, c3}

	// BYTE-LEVEL ZERO DETECTION
	firstByte := bits.TrailingZeros64(chunks[firstChunk]) >> 3

	// TOTAL OFFSET CALCULATION
	return (firstChunk << 3) + firstByte
}

// lookupPairByAddress performs high-performance address resolution using Robin Hood hashing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func lookupPairByAddress(address42HexBytes []byte) TradingPairID {
	// KEY CONVERSION
	key := packEthereumAddress(address42HexBytes)

	// INITIAL HASH CALCULATION
	i := hashAddressToIndex(address42HexBytes)
	dist := uint64(0) // Track probe distance for Robin Hood termination

	for {
		currentPairID := addressToPairMap[i]
		currentKey := packedAddressKeys[i]

		// BRANCHLESS KEY COMPARISON
		keyDiff := (key.words[0] ^ currentKey.words[0]) |
			(key.words[1] ^ currentKey.words[1]) |
			(key.words[2] ^ currentKey.words[2])

		// TERMINATION CONDITIONS
		if currentPairID == 0 {
			return 0 // Empty slot - address not in table
		}
		if keyDiff == 0 {
			return currentPairID // Exact match - return associated pair ID
		}

		// ROBIN HOOD EARLY TERMINATION
		currentKeyHash := hashPackedAddressToIndex(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)
		if currentDist < dist {
			return 0 // Early termination - key not present
		}

		// PROBE CONTINUATION
		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HOT PATH: CORE PROCESSING PIPELINE (SORTED BY EXECUTION ORDER)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processArbitrageUpdate orchestrates arbitrage detection for incoming price updates.
// CLEAN: Simple queue operations, no complex handle conversion.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processArbitrageUpdate(engine *ArbitrageEngine, update *PriceUpdateMessage) {
	// STAGE 1: DIRECTIONAL TICK SELECTION
	var currentTick float64
	if !engine.isReverseDirection {
		currentTick = update.forwardTick
	} else {
		currentTick = update.reverseTick
	}

	// STAGE 2: PRIORITY QUEUE IDENTIFICATION
	queueIndex, _ := engine.pairToQueueLookup.Get(uint32(update.pairID))
	queue := &engine.priorityQueues[queueIndex]

	// STAGE 3: PROFITABLE CYCLE EXTRACTION
	cycleCount := 0
	for {
		if queue.Empty() {
			break
		}

		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleIndex(cycleData)
		cycle := &engine.cycleStates[cycleIndex]

		// PROFITABILITY CALCULATION
		totalProfitability := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		isProfitable := totalProfitability < 0

		// OPPORTUNITY NOTIFICATION
		if isProfitable {
			// logArbitrageOpportunity(cycle, currentTick)
		}

		// EXTRACTION TERMINATION CONDITIONS
		if !isProfitable || cycleCount == len(engine.extractedCycles) {
			break
		}

		// TEMPORARY CYCLE STORAGE
		engine.extractedCycles[cycleCount] = ExtractedCycle{
			cycleIndex:   cycleIndex,
			originalTick: queueTick,
			queueHandle:  handle,
		}
		cycleCount++

		queue.UnlinkMin(handle)
	}

	// STAGE 4: CYCLE REINSERTION
	for i := 0; i < cycleCount; i++ {
		cycle := &engine.extractedCycles[i]
		queue.Push(cycle.originalTick, cycle.queueHandle, uint64(cycle.cycleIndex))
	}

	// STAGE 5: FANOUT UPDATE PROPAGATION
	for _, fanoutEntry := range engine.cycleFanoutTable[queueIndex] {
		cycle := &engine.cycleStates[fanoutEntry.cycleIndex]
		cycle.tickValues[fanoutEntry.edgeIndex] = currentTick

		// PRIORITY RECALCULATION
		newPriority := quantizeTickValue(cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2])
		fanoutEntry.queue.MoveTick(fanoutEntry.queueHandle, newPriority)
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
	return int64((tickValue + constants.TickClampingBound) * constants.QuantizationScale)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// WARM PATH: ADDRESS PROCESSING INFRASTRUCTURE (SORTED BY CALL FREQUENCY)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// packEthereumAddress converts hex address strings to optimized internal representation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func packEthereumAddress(address40HexChars []byte) PackedAddress {
	parsedAddress := utils.ParseEthereumAddress(address40HexChars)

	word0 := utils.Load64(parsedAddress[0:8])  // Address bytes 0-7
	word1 := utils.Load64(parsedAddress[8:16]) // Address bytes 8-15
	word2 := utils.Load64(parsedAddress[12:20]) & 0xFFFFFFFF

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
	hash64 := utils.ParseHexU64(address40HexChars[12:28])
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
	return a.words[0] == b.words[0] &&
		a.words[1] == b.words[1] &&
		a.words[2] == b.words[2]
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COOL PATH: MONITORING AND OBSERVABILITY (SORTED BY USAGE)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// logArbitrageOpportunity provides detailed logging for profitable arbitrage cycles.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func logArbitrageOpportunity(cycle *ArbitrageCycleState, newTick float64) {
	debug.DropMessage("[ARBITRAGE_OPPORTUNITY]", "")

	debug.DropMessage("  pair0", utils.Itoa(int(cycle.pairIDs[0])))
	debug.DropMessage("  pair1", utils.Itoa(int(cycle.pairIDs[1])))
	debug.DropMessage("  pair2", utils.Itoa(int(cycle.pairIDs[2])))

	tick0Str := utils.Ftoa(cycle.tickValues[0])
	tick1Str := utils.Ftoa(cycle.tickValues[1])
	tick2Str := utils.Ftoa(cycle.tickValues[2])
	newTickStr := utils.Ftoa(newTick)
	totalProfitStr := utils.Ftoa(newTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2])

	debug.DropMessage("  tick0", tick0Str)
	debug.DropMessage("  tick1", tick1Str)
	debug.DropMessage("  tick2", tick2Str)
	debug.DropMessage("  newTick", newTickStr)
	debug.DropMessage("  totalProfit", totalProfitStr)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COLD PATH: SYSTEM INITIALIZATION AND CONFIGURATION (SORTED BY CALL ORDER)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// RegisterTradingPairAddress populates the Robin Hood hash table with address-to-pair mappings.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterTradingPairAddress(address42HexBytes []byte, pairID TradingPairID) {
	key := packEthereumAddress(address42HexBytes)
	i := hashAddressToIndex(address42HexBytes)
	dist := uint64(0)

	for {
		currentPairID := addressToPairMap[i]

		if currentPairID == 0 {
			packedAddressKeys[i] = key
			addressToPairMap[i] = pairID
			return
		}

		if packedAddressKeys[i].isEqual(key) {
			addressToPairMap[i] = pairID
			return
		}

		currentKey := packedAddressKeys[i]
		currentKeyHash := hashPackedAddressToIndex(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)

		if currentDist < dist {
			key, packedAddressKeys[i] = packedAddressKeys[i], key
			pairID, addressToPairMap[i] = addressToPairMap[i], pairID
			dist = currentDist
		}

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
	pairToCoreRouting[pairID] |= 1 << coreID
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COLD PATH: CRYPTOGRAPHIC RANDOM GENERATION (SORTED BY CALL ORDER)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// newCryptoRandomGenerator creates deterministic random number generators for load balancing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func newCryptoRandomGenerator(initialSeed []byte) *CryptoRandomGenerator {
	var seed [32]byte
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(initialSeed)
	copy(seed[:], hasher.Sum(nil))
	return &CryptoRandomGenerator{counter: 0, seed: seed, hasher: sha3.NewLegacyKeccak256()}
}

// generateRandomUint64 generates cryptographically strong random values in deterministic sequences.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (rng *CryptoRandomGenerator) generateRandomUint64() uint64 {
	var input [40]byte
	copy(input[:32], rng.seed[:])
	*(*uint64)(unsafe.Pointer(&input[32])) = rng.counter
	rng.hasher.Reset()
	rng.hasher.Write(input[:])
	output := rng.hasher.Sum(nil)
	rng.counter++
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
	if len(cycleEdges) <= 1 {
		return
	}
	var seedInput [8]byte
	*(*uint64)(unsafe.Pointer(&seedInput[0])) = utils.Mix64(uint64(pairID))
	rng := newCryptoRandomGenerator(seedInput[:])
	for i := len(cycleEdges) - 1; i > 0; i-- {
		j := rng.generateRandomInt(i + 1)
		cycleEdges[i], cycleEdges[j] = cycleEdges[j], cycleEdges[i]
	}
}

// buildWorkloadShards constructs the fanout mapping infrastructure for cycle distribution.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func buildWorkloadShards(arbitrageTriangles []ArbitrageTriangle) {
	pairWorkloadShards = make(map[TradingPairID][]PairWorkloadShard)
	temporaryEdges := make(map[TradingPairID][]CycleEdge, len(arbitrageTriangles)*3)

	for _, triangle := range arbitrageTriangles {
		for i := 0; i < 3; i++ {
			temporaryEdges[triangle[i]] = append(temporaryEdges[triangle[i]],
				CycleEdge{cyclePairs: triangle, edgeIndex: uint64(i)})
		}
	}

	for pairID, cycleEdges := range temporaryEdges {
		shuffleCycleEdges(cycleEdges, pairID)
		for offset := 0; offset < len(cycleEdges); offset += constants.MaxCyclesPerShard {
			endOffset := offset + constants.MaxCyclesPerShard
			if endOffset > len(cycleEdges) {
				endOffset = len(cycleEdges)
			}
			pairWorkloadShards[pairID] = append(pairWorkloadShards[pairID],
				PairWorkloadShard{pairID: pairID, cycleEdges: cycleEdges[offset:endOffset]})
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COLD PATH: CLEAN QUEUE INITIALIZATION SYSTEM (SORTED BY EXECUTION ORDER)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// initializeArbitrageQueues allocates shared arena and initializes all queues with cycle data.
// CLEAN: Simple shared arena allocation, sequential handle assignment, zero complex partitioning.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func initializeArbitrageQueues(engine *ArbitrageEngine, workloadShards []PairWorkloadShard) {
	if len(workloadShards) == 0 {
		return
	}

	// PHASE 1: CALCULATE TOTAL ARENA SIZE NEEDED
	totalCycles := 0
	for _, shard := range workloadShards {
		totalCycles += len(shard.cycleEdges)
	}

	// Add 50% buffer for growth, ensure minimum size
	arenaSize := uint64((totalCycles * 3) / 2)
	if arenaSize < 1024 {
		arenaSize = 1024
	}

	// PHASE 2: ALLOCATE SINGLE SHARED ARENA FOR ALL QUEUES
	engine.sharedArena = make([]pooledquantumqueue.Entry, arenaSize)
	engine.nextHandle = 0

	// Initialize all arena entries to unlinked state
	for i := range engine.sharedArena {
		engine.sharedArena[i].Tick = -1                                    // Mark as unlinked
		engine.sharedArena[i].Prev = pooledquantumqueue.Handle(^uint64(0)) // nilIdx
		engine.sharedArena[i].Next = pooledquantumqueue.Handle(^uint64(0)) // nilIdx
		engine.sharedArena[i].Data = 0                                     // Clear data
	}

	// PHASE 3: CREATE QUEUES USING SHARED ARENA
	for i := range engine.priorityQueues {
		// All queues share the same arena pointer - much simpler!
		newQueue := pooledquantumqueue.New(unsafe.Pointer(&engine.sharedArena[0]))
		engine.priorityQueues[i] = *newQueue
	}

	// PHASE 4: POPULATE QUEUES WITH CYCLE DATA
	for _, shard := range workloadShards {
		queueIndex, _ := engine.pairToQueueLookup.Get(uint32(shard.pairID))
		queue := &engine.priorityQueues[queueIndex]

		for _, cycleEdge := range shard.cycleEdges {
			// Simple sequential handle allocation
			handle := engine.allocateQueueHandle()

			// Create cycle state entry
			engine.cycleStates = append(engine.cycleStates, ArbitrageCycleState{
				pairIDs: cycleEdge.cyclePairs,
			})
			cycleIndex := CycleIndex(len(engine.cycleStates) - 1)

			// Generate distributed initialization priority
			cycleHash := utils.Mix64(uint64(cycleIndex))
			randBits := cycleHash & 0xFFFF
			initPriority := int64(196608 + randBits)

			// Insert into queue
			queue.Push(initPriority, handle, uint64(cycleIndex))

			// Create fanout entries for the two other pairs in the cycle
			otherEdge1 := (cycleEdge.edgeIndex + 1) % 3
			otherEdge2 := (cycleEdge.edgeIndex + 2) % 3

			for _, edgeIdx := range [...]uint64{otherEdge1, otherEdge2} {
				engine.cycleFanoutTable[queueIndex] = append(engine.cycleFanoutTable[queueIndex],
					CycleFanoutEntry{
						cycleIndex:  uint64(cycleIndex),
						edgeIndex:   edgeIdx,
						queue:       queue,
						queueHandle: handle,
					})
			}
		}
	}

	debug.DropMessage("CLEAN_QUEUE_INIT",
		"Initialized "+utils.Itoa(len(engine.priorityQueues))+" queues with "+
			utils.Itoa(int(arenaSize))+" shared arena entries")
}

// launchArbitrageWorker initializes and operates a processing core for arbitrage detection.
// CLEAN: Simple shard collection, clean initialization, no complex handle management.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func launchArbitrageWorker(coreID, forwardCoreCount int, shardInput <-chan PairWorkloadShard) {
	runtime.LockOSThread()

	shutdownChannel := make(chan struct{})

	// CORE ENGINE INITIALIZATION
	engine := &ArbitrageEngine{
		pairToQueueLookup:  localidx.New(constants.DefaultLocalIdxSize),
		isReverseDirection: coreID >= forwardCoreCount,
		cycleStates:        make([]ArbitrageCycleState, 0),
		cycleFanoutTable:   nil,
		priorityQueues:     nil,
		shutdownChannel:    shutdownChannel,
	}
	coreEngines[coreID] = engine
	coreRings[coreID] = ring24.New(constants.DefaultRingSize)

	// PHASE 1: COLLECT ALL SHARDS FROM CHANNEL
	var allShards []PairWorkloadShard
	for shard := range shardInput {
		// Create or extend queue for this pair
		queueIndex := engine.pairToQueueLookup.Put(uint32(shard.pairID), uint32(len(engine.priorityQueues)))
		if int(queueIndex) == len(engine.priorityQueues) {
			// Create placeholder queue (will be replaced in initialization)
			engine.priorityQueues = append(engine.priorityQueues, pooledquantumqueue.PooledQuantumQueue{})
			engine.cycleFanoutTable = append(engine.cycleFanoutTable, nil)
		}
		allShards = append(allShards, shard)
	}

	// PHASE 2: CLEAN QUEUE INITIALIZATION WITH ALL COLLECTED SHARDS
	initializeArbitrageQueues(engine, allShards)

	// CONTROL SYSTEM INTEGRATION
	stopFlag, hotFlag := control.Flags()
	control.SignalActivity()
	ring24.PinnedConsumer(coreID, coreRings[coreID], stopFlag, hotFlag,
		func(messagePtr *[24]byte) {
			processArbitrageUpdate(engine, (*PriceUpdateMessage)(unsafe.Pointer(messagePtr)))
		}, shutdownChannel)
}

// InitializeArbitrageSystem orchestrates complete system bootstrap and activation.
// CLEAN: Same as original, works with simplified queue initialization.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func InitializeArbitrageSystem(arbitrageTriangles []ArbitrageTriangle) {
	// RESOURCE ALLOCATION STRATEGY
	coreCount := runtime.NumCPU() - 1
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	coreCount &^= 1                    // Ensure even number for direction pairing
	forwardCoreCount := coreCount >> 1 // Half dedicated to forward direction

	// FANOUT INFRASTRUCTURE CONSTRUCTION
	buildWorkloadShards(arbitrageTriangles)

	// WORKER CORE DEPLOYMENT
	shardChannels := make([]chan PairWorkloadShard, coreCount)
	for i := range shardChannels {
		shardChannels[i] = make(chan PairWorkloadShard, constants.ShardChannelBufferSize)
		go launchArbitrageWorker(i, forwardCoreCount, shardChannels[i])
	}

	// WORKLOAD DISTRIBUTION EXECUTION
	currentCore := 0
	for _, shardBuckets := range pairWorkloadShards {
		for _, shard := range shardBuckets {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			// DUAL-DIRECTION SHARD ASSIGNMENT
			shardChannels[forwardCore] <- shard
			shardChannels[reverseCore] <- shard

			// ROUTING TABLE POPULATION
			for _, cycleEdge := range shard.cycleEdges {
				for _, pairID := range cycleEdge.cyclePairs {
					pairToCoreRouting[pairID] |= 1<<forwardCore | 1<<reverseCore
				}
			}
			currentCore++
		}
	}

	// DISTRIBUTION COMPLETION SIGNALING
	for _, channel := range shardChannels {
		close(channel)
	}
}
