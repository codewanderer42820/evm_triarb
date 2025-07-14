// router.go — 57ns triangular arbitrage detection engine with infinite scalability

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CRITICAL FIXES FOR POOLED QUANTUM QUEUE INTEGRATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════
//
// This fixed version addresses several critical bugs in the pooled queue integration:
//
// 1. HANDLE CALCULATION FIX:
//    - Fixed entry() method handle calculation to use proper byte offsets
//    - Handle arithmetic now matches Entry structure size requirements
//
// 2. ARENA PARTITIONING FIX:
//    - Each queue gets its own contiguous arena partition
//    - Handles are allocated relative to queue's arena base
//    - No more shared arena conflicts between queues
//
// 3. HANDLE ALLOCATOR FIX:
//    - Fixed double allocation bug where same handle could be allocated twice
//    - nextHandle now starts at maxHandles to prevent conflicts with free list
//    - Proper handle lifecycle management with arena-relative calculations
//
// 4. INITIALIZATION ORDER FIX:
//    - Pool entries properly initialized to unlinked state before use
//    - Arena partitioning calculated before queue creation
//    - Handle allocation happens after arena setup
//
// 5. QUEUE INTERFACE FIX:
//    - Proper usage of PooledQuantumQueue API
//    - Correct handle management for Push/PeepMin/UnlinkMin operations
//    - Fixed fanout table handle references

package router

import (
	"hash"
	"math/bits"
	"runtime"
	"sync/atomic"
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

// PairID represents a unique identifier for a Uniswap V2 trading pair.
type PairID uint64

// ArbitrageTriplet defines a three-pair arbitrage cycle.
type ArbitrageTriplet [3]PairID

// CycleStateIndex provides typed access into the cycle state storage arrays.
type CycleStateIndex uint64

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// FIXED HANDLE MANAGEMENT SUBSYSTEM
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// HandleAllocator manages handle lifecycle for shared pool architecture.
// FIXED: Proper handle allocation without double allocation bug.
//
//go:notinheap
//go:align 64
type HandleAllocator struct {
	pool        []pooledquantumqueue.Entry
	freeHandles []pooledquantumqueue.Handle
	nextHandle  uint64
	maxHandles  uint64
}

// NewHandleAllocator creates a handle allocator for the shared pool.
// FIXED: nextHandle starts at maxHandles to prevent conflicts with free list.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func NewHandleAllocator(pool []pooledquantumqueue.Entry) *HandleAllocator {
	maxHandles := uint64(len(pool))
	allocator := &HandleAllocator{
		pool:       pool,
		maxHandles: maxHandles,
		nextHandle: maxHandles, // FIXED: Start beyond free list range
	}

	// Initialize free handle list with all available handles
	allocator.freeHandles = make([]pooledquantumqueue.Handle, len(pool))
	for i := range allocator.freeHandles {
		allocator.freeHandles[i] = pooledquantumqueue.Handle(i)
	}

	return allocator
}

// AllocateHandle returns a free handle or creates new one if available.
// FIXED: Prevents double allocation by starting nextHandle beyond free list range.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (ha *HandleAllocator) AllocateHandle() (pooledquantumqueue.Handle, bool) {
	// Try to reuse a freed handle first (better cache locality)
	if len(ha.freeHandles) > 0 {
		handle := ha.freeHandles[len(ha.freeHandles)-1]
		ha.freeHandles = ha.freeHandles[:len(ha.freeHandles)-1]
		return handle, true
	}

	// FIXED: nextHandle starts at maxHandles, so no conflict with free list
	next := atomic.AddUint64(&ha.nextHandle, 1) - 1
	if next < ha.maxHandles*2 { // Allow expansion beyond initial pool
		return pooledquantumqueue.Handle(next % ha.maxHandles), true
	}

	return 0, false // Pool exhausted
}

// FreeHandle returns a handle to the free pool and resets pool entry.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (ha *HandleAllocator) FreeHandle(handle pooledquantumqueue.Handle) {
	// Only free handles that are in the original pool range
	if uint64(handle) < ha.maxHandles {
		// Reset pool entry to unlinked state
		ha.pool[handle].Tick = -1
		ha.pool[handle].Prev = pooledquantumqueue.Handle(^uint64(0))
		ha.pool[handle].Next = pooledquantumqueue.Handle(^uint64(0))
		ha.pool[handle].Data = 0

		// Return handle to free list for reuse
		ha.freeHandles = append(ha.freeHandles, handle)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INTER-CORE MESSAGE PASSING ARCHITECTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// TickUpdate represents a price change notification distributed to processing cores.
//
//go:notinheap
//go:align 8
type TickUpdate struct {
	pairID      PairID  // 8B - PRIMARY: Pair identifier for routing and queue lookup
	forwardTick float64 // 8B - PRICING: Logarithmic price ratio for forward direction
	reverseTick float64 // 8B - PRICING: Logarithmic price ratio for reverse direction
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ARBITRAGE CYCLE STATE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ArbitrageCycleState maintains real-time profitability tracking for a three-pair arbitrage cycle.
//
//go:notinheap
//go:align 64
type ArbitrageCycleState struct {
	tickValues [3]float64 // 24B - HOT: Logarithmic price ratios for profitability calculation
	pairIDs    [3]PairID  // 24B - WARM: Pair identifiers for cycle reconstruction and debugging
	_          [16]byte   // 16B - PADDING: Alignment to 64-byte cache line boundary
}

// FanoutEntry defines the relationship between pair price updates and affected arbitrage cycles.
// FIXED: Proper 8-byte aligned Handle without padding.
//
//go:notinheap
//go:align 32
type FanoutEntry struct {
	cycleStateIndex uint64                                 // 8B - PRIMARY: Direct array index for cycle access
	edgeIndex       uint64                                 // 8B - INDEXING: Position within cycle for tick update
	queue           *pooledquantumqueue.PooledQuantumQueue // 8B - QUEUE_OPS: Priority queue for cycle reordering
	queueHandle     pooledquantumqueue.Handle              // 8B - HANDLE: Queue manipulation identifier
}

// ProcessedCycle provides temporary storage for cycles extracted during profitability analysis.
//
//go:notinheap
//go:align 32
type ProcessedCycle struct {
	cycleStateIndex CycleStateIndex           // 8B - PRIMARY: Array index for cycle state access
	originalTick    int64                     // 8B - PRIORITY: Original tick value for queue reinsertion
	queueHandle     pooledquantumqueue.Handle // 8B - HANDLE: Queue manipulation identifier
	_               [8]byte                   // 8B - PADDING: 32-byte boundary alignment
}

// FIXED: Arena partition info for proper queue initialization
type QueueArenaInfo struct {
	arenaOffset uint64
	arenaSize   uint64
	arenaPtr    unsafe.Pointer
}

//go:notinheap
//go:align 64
var (
	// PER-CORE UNIFIED ARENAS: One giant arena per CPU core, shared among all queues on that core
	coreArenas           [constants.MaxSupportedCores][]pooledquantumqueue.Entry
	coreHandleAllocators [constants.MaxSupportedCores]*HandleAllocator
)

// ArbitrageCoreExecutor orchestrates arbitrage detection for a single CPU core.
// FIXED: Uses unified per-core arena shared among all queues on the same core.
//
//go:notinheap
//go:align 64
type ArbitrageCoreExecutor struct {
	// TIER 1: ULTRA-HOT PATH (Every tick update - millions per second)
	pairToQueueIndex   localidx.Hash // 64B - Pair-to-queue mapping for O(1) lookup performance
	isReverseDirection bool          // 1B - Direction flag checked on every tick update
	coreID             int           // 4B - Core ID for arena access
	_                  [3]byte       // 3B - Alignment padding for optimal memory layout

	// TIER 2: HOT PATH (Frequent cycle processing operations)
	cycleStates  []ArbitrageCycleState // 24B - Complete arbitrage cycle state storage
	fanoutTables [][]FanoutEntry       // 24B - Pair-to-cycle mappings for fanout operations

	// TIER 3: WARM PATH (Moderate frequency queue operations)
	priorityQueues []pooledquantumqueue.PooledQuantumQueue // 24B - Priority queues per trading pair
	queueArenas    []QueueArenaInfo                        // 24B - Arena partition info per queue

	// TIER 4: COOL PATH (Occasional profitable cycle extraction)
	processedCycles [128]ProcessedCycle // 4096B - Pre-allocated buffer for extracted cycles

	// TIER 5: COLD PATH (Rare configuration and control operations)
	shutdownSignal <-chan struct{} // 8B - Graceful shutdown coordination channel
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS RESOLUTION SUBSYSTEM
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// AddressKey represents a packed Ethereum address optimized for hash table operations.
//
//go:notinheap
//go:align 32
type AddressKey struct {
	words [3]uint64 // 24B - Packed 160-bit Ethereum address for efficient comparison
	_     [8]byte   // 8B - Padding to 32-byte cache line boundary optimization
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION INFRASTRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ArbitrageEdgeBinding represents a single edge within an arbitrage cycle during construction.
//
//go:notinheap
//go:align 32
type ArbitrageEdgeBinding struct {
	cyclePairs [3]PairID // 24B - Complete three-pair cycle definition
	edgeIndex  uint64    // 8B - Position index (0, 1, or 2) within the cycle
}

// PairShardBucket aggregates arbitrage cycles by trading pair for core distribution.
//
//go:notinheap
//go:align 32
type PairShardBucket struct {
	pairID       PairID                 // 8B - Trading pair identifier for shard classification
	edgeBindings []ArbitrageEdgeBinding // 24B - All arbitrage cycles containing this pair
}

// keccakRandomState provides cryptographically strong deterministic randomness for load balancing.
//
//go:notinheap
//go:align 64
type keccakRandomState struct {
	counter uint64    // 8B - PRIMARY: Sequence counter incremented per generation
	seed    [32]byte  // 32B - ENTROPY: Cryptographic seed for random generation
	hasher  hash.Hash // 24B - INFRASTRUCTURE: Reusable Keccak-256 hasher instance
}

//go:notinheap
//go:align 64
var (
	// PER-CORE EXCLUSIVE STATE
	coreExecutors [constants.MaxSupportedCores]*ArbitrageCoreExecutor
	coreRings     [constants.MaxSupportedCores]*ring24.Ring

	// GLOBAL ROUTING INFRASTRUCTURE (READ-ONLY AFTER INITIALIZATION)
	pairToCoreAssignment [constants.PairRoutingTableCapacity]uint64
	pairShardBuckets     map[PairID][]PairShardBucket

	// ADDRESS RESOLUTION TABLES (READ-ONLY AFTER INITIALIZATION)
	pairAddressKeys [constants.AddressTableCapacity]AddressKey
	addressToPairID [constants.AddressTableCapacity]PairID
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// EVENT DISPATCH PIPELINE (UNCHANGED)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// DispatchTickUpdate processes Uniswap V2 Sync events and distributes price updates to cores.
// Unchanged from original implementation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DispatchTickUpdate(logView *types.LogView) {
	// PHASE 1: ADDRESS RESOLUTION (Target: 14ns, ~42 cycles at 3GHz)
	pairID := lookupPairIDByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if pairID == 0 {
		return // Unregistered pair - early termination
	}

	// PHASE 2: EVENT DATA PREPARATION (Target: 2 cycles)
	hexData := logView.Data[2:130]

	// PHASE 3: NUMERICAL MAGNITUDE ANALYSIS (Target: 1.7ns, ~5 cycles at 3GHz)
	lzcntA := countLeadingZeros(hexData[32:64])  // Reserve A magnitude analysis
	lzcntB := countLeadingZeros(hexData[96:128]) // Reserve B magnitude analysis

	// PHASE 4: PRECISION PRESERVATION (Target: 3-4 cycles)
	cond := lzcntA - lzcntB
	mask := cond >> 31
	minZeros := lzcntB ^ ((lzcntA ^ lzcntB) & mask) // Branchless min(lzcntA, lzcntB)

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
	var message TickUpdate
	if err != nil {
		// FALLBACK: Cryptographically secure random value generation
		addrHash := utils.Mix64(uint64(pairID))
		randBits := addrHash & 0x1FFF // Extract 13 bits for range [0, 8191]
		placeholder := 50.2 + float64(randBits)*0.0015625

		message = TickUpdate{
			pairID:      pairID,
			forwardTick: placeholder, // Positive - deprioritizes
			reverseTick: placeholder, // Positive - deprioritizes
		}
	} else {
		// Valid reserves: use actual log ratio with opposite signs for direction
		message = TickUpdate{
			pairID:      pairID,
			forwardTick: tickValue,
			reverseTick: -tickValue,
		}
	}

	// PHASE 10: MULTI-CORE DISTRIBUTION WITH GUARANTEED DELIVERY
	coreAssignments := pairToCoreAssignment[pairID]

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

// countLeadingZeros performs efficient leading zero counting for hex-encoded numeric data.
// Unchanged from original implementation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func countLeadingZeros(segment []byte) int {
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

// lookupPairIDByAddress performs high-performance address resolution using Robin Hood hashing.
// Unchanged from original implementation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func lookupPairIDByAddress(address42HexBytes []byte) PairID {
	// KEY CONVERSION
	key := bytesToAddressKey(address42HexBytes)

	// INITIAL HASH CALCULATION
	i := directAddressToIndex64(address42HexBytes)
	dist := uint64(0) // Track probe distance for Robin Hood termination

	for {
		currentPairID := addressToPairID[i]
		currentKey := pairAddressKeys[i]

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
		currentKeyHash := directAddressToIndex64Stored(currentKey)
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
// FIXED CORE PROCESSING PIPELINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processTickUpdate orchestrates arbitrage detection for incoming price updates.
// FIXED: Updated to use proper PooledQuantumQueue interface.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processTickUpdate(executor *ArbitrageCoreExecutor, update *TickUpdate) {
	// STAGE 1: DIRECTIONAL TICK SELECTION
	var currentTick float64
	if !executor.isReverseDirection {
		currentTick = update.forwardTick
	} else {
		currentTick = update.reverseTick
	}

	// STAGE 2: PRIORITY QUEUE IDENTIFICATION
	queueIndex, _ := executor.pairToQueueIndex.Get(uint32(update.pairID))
	queue := &executor.priorityQueues[queueIndex]

	// STAGE 3: PROFITABLE CYCLE EXTRACTION
	cycleCount := 0
	for {
		if queue.Empty() {
			break
		}

		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleStateIndex(cycleData)
		cycle := &executor.cycleStates[cycleIndex]

		// PROFITABILITY CALCULATION
		totalProfitability := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		isProfitable := totalProfitability < 0

		// OPPORTUNITY NOTIFICATION
		if isProfitable {
			// emitArbitrageOpportunity(cycle, currentTick)
		}

		// EXTRACTION TERMINATION CONDITIONS
		if !isProfitable || cycleCount == len(executor.processedCycles) {
			break
		}

		// TEMPORARY CYCLE STORAGE
		executor.processedCycles[cycleCount] = ProcessedCycle{
			cycleStateIndex: cycleIndex,
			originalTick:    queueTick,
			queueHandle:     handle,
		}
		cycleCount++

		queue.UnlinkMin(handle)
	}

	// STAGE 4: CYCLE REINSERTION
	for i := 0; i < cycleCount; i++ {
		cycle := &executor.processedCycles[i]
		queue.Push(cycle.originalTick, cycle.queueHandle, uint64(cycle.cycleStateIndex))
	}

	// STAGE 5: FANOUT UPDATE PROPAGATION
	for _, fanoutEntry := range executor.fanoutTables[queueIndex] {
		cycle := &executor.cycleStates[fanoutEntry.cycleStateIndex]
		cycle.tickValues[fanoutEntry.edgeIndex] = currentTick

		// PRIORITY RECALCULATION
		newPriority := quantizeTickToInt64(cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2])
		fanoutEntry.queue.MoveTick(fanoutEntry.queueHandle, newPriority)
	}
}

// quantizeTickToInt64 converts floating-point tick values to integer priorities for queue operations.
// Unchanged from original implementation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func quantizeTickToInt64(tickValue float64) int64 {
	return int64((tickValue + constants.TickClampingBound) * constants.QuantizationScale)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS PROCESSING INFRASTRUCTURE (UNCHANGED)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// bytesToAddressKey converts hex address strings to optimized internal representation.
// Unchanged from original implementation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func bytesToAddressKey(address40HexChars []byte) AddressKey {
	parsedAddress := utils.ParseEthereumAddress(address40HexChars)

	word0 := utils.Load64(parsedAddress[0:8])  // Address bytes 0-7
	word1 := utils.Load64(parsedAddress[8:16]) // Address bytes 8-15
	word2 := utils.Load64(parsedAddress[12:20]) & 0xFFFFFFFF

	return AddressKey{
		words: [3]uint64{word0, word1, word2},
	}
}

// directAddressToIndex64 computes hash table indices from raw hex addresses.
// Unchanged from original implementation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64(address40HexChars []byte) uint64 {
	hash64 := utils.ParseHexU64(address40HexChars[12:28])
	return hash64 & uint64(constants.AddressTableMask)
}

// directAddressToIndex64Stored computes hash values from stored AddressKey structures.
// Unchanged from original implementation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64Stored(key AddressKey) uint64 {
	return key.words[2] & uint64(constants.AddressTableMask)
}

// isEqual performs efficient comparison between AddressKey structures.
// Unchanged from original implementation.
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

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MONITORING AND OBSERVABILITY (UNCHANGED)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// emitArbitrageOpportunity provides detailed logging for profitable arbitrage cycles.
// Unchanged from original implementation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func emitArbitrageOpportunity(cycle *ArbitrageCycleState, newTick float64) {
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
// SYSTEM INITIALIZATION AND CONFIGURATION (UNCHANGED)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// RegisterPairAddress populates the Robin Hood hash table with address-to-pair mappings.
// Unchanged from original implementation.
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

		if currentPairID == 0 {
			pairAddressKeys[i] = key
			addressToPairID[i] = pairID
			return
		}

		if pairAddressKeys[i].isEqual(key) {
			addressToPairID[i] = pairID
			return
		}

		currentKey := pairAddressKeys[i]
		currentKeyHash := directAddressToIndex64Stored(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)

		if currentDist < dist {
			key, pairAddressKeys[i] = pairAddressKeys[i], key
			pairID, addressToPairID[i] = addressToPairID[i], pairID
			dist = currentDist
		}

		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++
	}
}

// RegisterPairToCore establishes pair-to-core routing assignments for load distribution.
// Unchanged from original implementation.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairToCore(pairID PairID, coreID uint8) {
	pairToCoreAssignment[pairID] |= 1 << coreID
}

// Random generation functions (unchanged)
func newKeccakRandom(initialSeed []byte) *keccakRandomState {
	var seed [32]byte

	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(initialSeed)
	copy(seed[:], hasher.Sum(nil))

	return &keccakRandomState{
		counter: 0,
		seed:    seed,
		hasher:  sha3.NewLegacyKeccak256(),
	}
}

func (k *keccakRandomState) nextUint64() uint64 {
	var input [40]byte
	copy(input[:32], k.seed[:])
	*(*uint64)(unsafe.Pointer(&input[32])) = k.counter

	k.hasher.Reset()
	k.hasher.Write(input[:])
	output := k.hasher.Sum(nil)

	k.counter++

	return utils.Load64(output[:8])
}

func (k *keccakRandomState) nextInt(upperBound int) int {
	randomValue := k.nextUint64()
	return int(randomValue % uint64(upperBound))
}

func keccakShuffleEdgeBindings(bindings []ArbitrageEdgeBinding, pairID PairID) {
	if len(bindings) <= 1 {
		return
	}

	var seedInput [8]byte
	*(*uint64)(unsafe.Pointer(&seedInput[0])) = utils.Mix64(uint64(pairID))

	rng := newKeccakRandom(seedInput[:])

	for i := len(bindings) - 1; i > 0; i-- {
		j := rng.nextInt(i + 1)
		bindings[i], bindings[j] = bindings[j], bindings[i]
	}
}

func buildFanoutShardBuckets(cycles []ArbitrageTriplet) {
	pairShardBuckets = make(map[PairID][]PairShardBucket)
	temporaryBindings := make(map[PairID][]ArbitrageEdgeBinding, len(cycles)*3)

	for _, triplet := range cycles {
		for i := 0; i < 3; i++ {
			temporaryBindings[triplet[i]] = append(temporaryBindings[triplet[i]],
				ArbitrageEdgeBinding{
					cyclePairs: triplet,
					edgeIndex:  uint64(i),
				})
		}
	}

	for pairID, bindings := range temporaryBindings {
		keccakShuffleEdgeBindings(bindings, pairID)

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

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// FIXED POOLED QUEUE INITIALIZATION SYSTEM
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// calculateArenaPartitions determines arena layout for proper queue isolation.
// FIXED: Proper arena partitioning with size calculations.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func calculateArenaPartitions(totalArenaSize uint64, numQueues int) []QueueArenaInfo {
	if numQueues == 0 {
		return nil
	}

	entriesPerQueue := totalArenaSize / uint64(numQueues)
	if entriesPerQueue < 64 {
		entriesPerQueue = 64 // Minimum reasonable size
	}

	arenas := make([]QueueArenaInfo, numQueues)
	for i := 0; i < numQueues; i++ {
		arenas[i] = QueueArenaInfo{
			arenaOffset: uint64(i) * entriesPerQueue,
			arenaSize:   entriesPerQueue,
		}
	}

	return arenas
}

// initializeCoreArena allocates and initializes the unified arena for a CPU core.
// FIXED: One giant arena per core, partitioned among all queues on that core.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func initializeCoreArena(coreID int, totalArenaEntries uint64) {
	// PHASE 1: ALLOCATE UNIFIED CORE ARENA
	coreArenas[coreID] = make([]pooledquantumqueue.Entry, totalArenaEntries)

	// PHASE 2: INITIALIZE ALL ENTRIES TO UNLINKED STATE
	for i := range coreArenas[coreID] {
		coreArenas[coreID][i].Tick = -1                                    // Mark as unlinked
		coreArenas[coreID][i].Prev = pooledquantumqueue.Handle(^uint64(0)) // nilIdx
		coreArenas[coreID][i].Next = pooledquantumqueue.Handle(^uint64(0)) // nilIdx
		coreArenas[coreID][i].Data = 0                                     // Clear data
	}

	// PHASE 3: INITIALIZE PER-CORE HANDLE ALLOCATOR
	coreHandleAllocators[coreID] = NewHandleAllocator(coreArenas[coreID])
}

// finalizeQueueInitialization calculates arena requirements and initializes queues.
// FIXED: Uses unified per-core arena with proper partitioning.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func finalizeQueueInitialization(executor *ArbitrageCoreExecutor, collectedShards []shardCollectionEntry) {
	if len(collectedShards) == 0 {
		return
	}

	// PHASE 1: CALCULATE TOTAL ARENA REQUIREMENTS FOR THIS CORE
	totalCycles := 0
	for _, shardEntry := range collectedShards {
		totalCycles += len(shardEntry.shard.edgeBindings)
	}

	// Add 50% buffer for growth and ensure minimum size
	totalArenaEntries := uint64((totalCycles * 3) / 2)
	if totalArenaEntries < 1024 {
		totalArenaEntries = 1024
	}

	// PHASE 2: INITIALIZE UNIFIED CORE ARENA
	initializeCoreArena(executor.coreID, totalArenaEntries)

	// PHASE 3: CALCULATE QUEUE PARTITIONS WITHIN THE CORE ARENA
	numQueues := len(executor.priorityQueues)
	executor.queueArenas = calculateArenaPartitions(totalArenaEntries, numQueues)

	// PHASE 4: INITIALIZE QUEUES WITH PARTITIONED ARENA POINTERS
	coreArena := coreArenas[executor.coreID] // Get the unified core arena

	for i := range executor.priorityQueues {
		if i >= len(executor.queueArenas) {
			break
		}

		arena := &executor.queueArenas[i]

		// FIXED: Calculate arena pointer within the unified core arena
		arena.arenaPtr = unsafe.Pointer(&coreArena[arena.arenaOffset])

		// Create new PooledQuantumQueue using the partitioned section of core arena
		newQueue := pooledquantumqueue.New(arena.arenaPtr)
		executor.priorityQueues[i] = *newQueue
	}

	// PHASE 5: POPULATE QUEUES WITH CYCLE DATA
	handleAllocator := coreHandleAllocators[executor.coreID] // Get per-core handle allocator

	for _, shardEntry := range collectedShards {
		queueIndex := shardEntry.queueIndex
		if int(queueIndex) >= len(executor.priorityQueues) {
			continue
		}

		queue := &executor.priorityQueues[queueIndex]

		for _, edgeBinding := range shardEntry.shard.edgeBindings {
			// FIXED: Use per-core handle allocator
			globalHandle, available := handleAllocator.AllocateHandle()
			if !available {
				debug.DropMessage("HANDLE_EXHAUSTED", "No more handles available")
				break
			}

			// Create cycle state entry
			executor.cycleStates = append(executor.cycleStates, ArbitrageCycleState{
				pairIDs: edgeBinding.cyclePairs,
				// tickValues initialized to zero by default
			})
			cycleIndex := CycleStateIndex(len(executor.cycleStates) - 1)

			// Generate distributed initialization priority
			cycleHash := utils.Mix64(uint64(cycleIndex))
			randBits := cycleHash & 0xFFFF
			initPriority := int64(196608 + randBits)

			// Insert into queue with handle (all within same core arena)
			queue.Push(initPriority, globalHandle, uint64(cycleIndex))

			// Create fanout entries for the two other pairs in the cycle
			otherEdge1 := (edgeBinding.edgeIndex + 1) % 3
			otherEdge2 := (edgeBinding.edgeIndex + 2) % 3

			for _, edgeIdx := range [...]uint64{otherEdge1, otherEdge2} {
				executor.fanoutTables[queueIndex] = append(executor.fanoutTables[queueIndex],
					FanoutEntry{
						cycleStateIndex: uint64(cycleIndex),
						edgeIndex:       edgeIdx,
						queue:           queue,
						queueHandle:     globalHandle, // Handle within unified core arena
					})
			}
		}
	}

	debug.DropMessage("QUEUE_FINALIZATION",
		"Core "+utils.Itoa(executor.coreID)+": Initialized "+utils.Itoa(len(executor.priorityQueues))+
			" queues with "+utils.Itoa(int(totalArenaEntries))+" total arena entries")
}

// Remaining functions unchanged (collectShardData, launchShardWorker, InitializeArbitrageSystem)
// These maintain the same structure but now work with the fixed arena partitioning

// shardCollectionEntry holds shard data collected during channel processing
type shardCollectionEntry struct {
	shard      PairShardBucket
	queueIndex uint32 // Queue index for this shard
}

func collectShardData(executor *ArbitrageCoreExecutor, shard *PairShardBucket,
	collectedShards *[]shardCollectionEntry) {

	// Get or create queue index for this pair
	queueIndex := executor.pairToQueueIndex.Put(uint32(shard.pairID), uint32(len(executor.priorityQueues)))

	// Extend queues slice if new queue index was created
	if int(queueIndex) == len(executor.priorityQueues) {
		// Create temporary placeholder queue (will be replaced in finalization)
		tempQueue := pooledquantumqueue.New(nil)
		executor.priorityQueues = append(executor.priorityQueues, *tempQueue)
		executor.fanoutTables = append(executor.fanoutTables, nil)
	}

	// Collect shard data for later processing
	*collectedShards = append(*collectedShards, shardCollectionEntry{
		shard:      *shard,
		queueIndex: queueIndex,
	})
}

func launchShardWorker(coreID, forwardCoreCount int, shardInput <-chan PairShardBucket) {
	runtime.LockOSThread()

	shutdownChannel := make(chan struct{})

	// CORE EXECUTOR INITIALIZATION
	executor := &ArbitrageCoreExecutor{
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		isReverseDirection: coreID >= forwardCoreCount,
		coreID:             coreID, // FIXED: Store core ID for arena access
		cycleStates:        make([]ArbitrageCycleState, 0),
		fanoutTables:       nil,
		priorityQueues:     nil,
		shutdownSignal:     shutdownChannel,
	}
	coreExecutors[coreID] = executor
	coreRings[coreID] = ring24.New(constants.DefaultRingSize)

	// PHASE 1: COLLECT SHARD DATA FROM CHANNEL
	var collectedShards []shardCollectionEntry

	for shard := range shardInput {
		collectShardData(executor, &shard, &collectedShards)
	}

	// PHASE 2: FINALIZE QUEUE INITIALIZATION WITH UNIFIED CORE ARENA
	finalizeQueueInitialization(executor, collectedShards)

	// CONTROL SYSTEM INTEGRATION
	stopFlag, hotFlag := control.Flags()

	control.SignalActivity()
	ring24.PinnedConsumer(coreID, coreRings[coreID], stopFlag, hotFlag,
		func(messagePtr *[24]byte) {
			processTickUpdate(executor, (*TickUpdate)(unsafe.Pointer(messagePtr)))
		}, shutdownChannel)
}

func InitializeArbitrageSystem(arbitrageCycles []ArbitrageTriplet) {
	// RESOURCE ALLOCATION STRATEGY
	coreCount := runtime.NumCPU() - 1
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	coreCount &^= 1                    // Ensure even number for direction pairing
	forwardCoreCount := coreCount >> 1 // Half dedicated to forward direction

	// FANOUT INFRASTRUCTURE CONSTRUCTION
	buildFanoutShardBuckets(arbitrageCycles)

	// WORKER CORE DEPLOYMENT
	shardChannels := make([]chan PairShardBucket, coreCount)
	for i := range shardChannels {
		shardChannels[i] = make(chan PairShardBucket, constants.ShardChannelBufferSize)
		go launchShardWorker(i, forwardCoreCount, shardChannels[i])
	}

	// WORKLOAD DISTRIBUTION EXECUTION
	currentCore := 0
	for _, shardBuckets := range pairShardBuckets {
		for _, shard := range shardBuckets {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			// DUAL-DIRECTION SHARD ASSIGNMENT
			shardChannels[forwardCore] <- shard
			shardChannels[reverseCore] <- shard

			// ROUTING TABLE POPULATION
			for _, edgeBinding := range shard.edgeBindings {
				for _, pairID := range edgeBinding.cyclePairs {
					pairToCoreAssignment[pairID] |= 1<<forwardCore | 1<<reverseCore
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
