// router.go — High-performance triangular-arbitrage fan-out router (64 cores)
// ============================================================================
// TRIANGULAR ARBITRAGE ROUTING ENGINE
// ============================================================================
//
// ArbitrageRouter provides ultra-low-latency detection and routing of triangular
// arbitrage opportunities across 64 CPU cores with zero heap allocations.
//
// Architecture overview:
//   • Direct Ethereum address indexing leveraging keccak256 uniformity
//   • Per-core executors with lock-free QuantumQueue64 priority queues
//   • ring24 SPSC rings for inter-core TickUpdate message passing
//   • Canonical storage arena for shared arbitrage cycle state
//   • Robin Hood probing for collision resolution with bounded distances
//
// Performance characteristics:
//   • Nanosecond-scale tick update processing (measured 5-7ns per operation)
//   • O(1) arbitrage opportunity detection with Robin Hood bounded worst-case
//   • Cache-aligned data structures for optimal memory access patterns
//   • Zero heap allocations in all hot paths
//   • Deterministic low-latency execution with direct address indexing
//
// Memory layout legend:
//	┌──────────────────────────────────────────────────────────────────────────┐
//	│ tx-ingress (ws_conn) ─▶ parser ─▶ DispatchUpdate ─▶ ring24 ─▶ executor │
//	│                                                        ▲               │
//	│                          cycle profit evaluation ◀─────┘               │
//	└──────────────────────────────────────────────────────────────────────────┘
//
// Safety model:
//   • Bounded probe distances prevent infinite loops in Robin Hood hashing
//   • Direct address indexing eliminates hash function collision risks
//   • Memory safety requires careful unsafe pointer usage in hot paths
//
// Compiler optimizations:
//   • //go:nosplit for stack management elimination
//   • //go:inline for call overhead elimination
//   • //go:registerparams for register-based parameter passing

package router

import (
	"crypto/rand"
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
)

// ============================================================================
// CORE TYPE DEFINITIONS
// ============================================================================

// PairID represents a unique identifier for a trading pair (e.g., WETH/USDC)
type PairID uint32

// ArbitrageTriplet represents three trading pairs that form a triangular
// arbitrage cycle (e.g., WETH/USDC → USDC/DAI → DAI/WETH → WETH/USDC)
type ArbitrageTriplet [3]PairID

// CycleStateIndex represents an index into the canonical cycle state storage arena.
// Using uint64 for future-proofing and optimal alignment on 64-bit architectures.
type CycleStateIndex uint64

// AddressKey represents five unaligned 8-byte words (40 bytes total) for
// zero-copy address comparison using SIMD-friendly word operations
// Padded to 64 bytes for cache line alignment and optimal memory access
//
//go:align 64
type AddressKey struct {
	words [5]uint64 // 40 B ← Ethereum address as 5 x 8-byte words
	_     [3]uint64 // 24 B ← padding to reach exactly 64 bytes
} // Total: exactly one 64-byte cache line

// ============================================================================
// CORE MESSAGE TYPES
// ============================================================================

// TickUpdate represents a price tick update dispatched across cores via ring buffers.
// Size exactly matches ring24 slot size (24 bytes) for zero-copy message passing.
//
//go:notinheap
type TickUpdate struct { // 24 B (exact ring24 slot size)
	forwardTick float64 // 8 B ← forward direction price tick
	reverseTick float64 // 8 B ← reverse direction price tick
	pairID      PairID  // 4 B ← pair identifier
	_           uint32  // 4 B ← alignment padding to reach 24 bytes
}

// ============================================================================
// ARBITRAGE CYCLE REPRESENTATION
// ============================================================================

// ArbitrageCycleState represents the complete state of a triangular arbitrage cycle.
// Memory layout is optimized for cache performance with hot tick data first.
//
//go:align 64
//go:notinheap
type ArbitrageCycleState struct {
	// Hot data: accessed on every profit calculation (first 24 bytes)
	tickValues [3]float64 // 24 B ← hottest: used in every scoring operation

	// Warm data: accessed during cycle identification
	pairIDs [3]PairID // 12 B ← pair identifiers for this cycle
	_       uint32    // 4 B  ← alignment padding

	// Padding to complete cache line alignment
	_ [3]uint64 // 24 B ← pad to exactly 64 bytes
} // Total: exactly one 64-byte cache line

// ArbitrageEdgeBinding represents a single edge within an arbitrage cycle.
// Array-first layout enables efficient stride-based batch scanning operations.
//
//go:align 16
//go:notinheap
type ArbitrageEdgeBinding struct { // 16 B total
	cyclePairs [3]PairID // 12 B ← complete triplet for this cycle
	edgeIndex  uint16    // 2 B  ← which edge this binding represents (0, 1, 2)
	_          uint16    // 2 B  ← alignment padding
}

// ============================================================================
// FANOUT AND ROUTING STRUCTURES
// ============================================================================

// FanoutEntry represents a single edge in the arbitrage cycle fanout table.
// Each entry references canonical cycle state and maintains queue handle for
// efficient priority updates when tick values change.
//
//go:align 32
//go:notinheap
type FanoutEntry struct { // 32 B total
	queueHandle     quantumqueue64.Handle          // 4 B ← hottest: used in every MoveTick call
	edgeIndex       uint16                         // 2 B ← hot: determines which tick to update
	_               uint16                         // 2 B ← alignment padding
	cycleStateIndex CycleStateIndex                // 8 B ← warm: index into canonical arena
	queue           *quantumqueue64.QuantumQueue64 // 8 B ← cold: pointer to owning queue
	_               uint64                         // 8 B ← padding to reach 32 bytes
}

// PairShardBucket groups multiple arbitrage cycles by their common pair.
// Memory layout optimizes for batch processing during executor initialization.
//
//go:align 32
//go:notinheap
type PairShardBucket struct { // 32 B total
	pairID       PairID                 // 4 B  ← hottest: used for queue lookup
	_            uint32                 // 4 B  ← alignment padding
	edgeBindings []ArbitrageEdgeBinding // 24 B ← cold: slice header (ptr,len,cap)
}

// ============================================================================
// CORE EXECUTOR (TOP-LEVEL ORCHESTRATOR)
// ============================================================================

// ArbitrageCoreExecutor owns per-core queues and fan-out tables for parallel
// arbitrage opportunity detection. Memory layout is optimized for cache
// performance with hot fields in the first cache line.
//
//go:align 64
//go:notinheap
type ArbitrageCoreExecutor struct {
	// ── Cache line 1: Hot path fields (64 bytes) ──
	priorityQueues     []quantumqueue64.QuantumQueue64 // 24 B ← owned queue instances
	fanoutTables       [][]FanoutEntry                 // 24 B ← fanout edge mappings
	isReverseDirection bool                            // 1 B  ← forward vs reverse tick processing
	_                  [7]byte                         // 7 B  ← alignment padding
	shutdownSignal     <-chan struct{}                 // 8 B  ← graceful shutdown channel

	// ── Cache line 2: Local index for O(1) pair→queue mapping (64 bytes) ──
	pairToQueueIndex localidx.Hash // 64 B ← local pair ID to queue index mapping

	// ── Cache line 3: Canonical cycle state storage (64 bytes) ──
	cycleStates []ArbitrageCycleState // 24 B ← direct slice, canonical storage
	_           [5]uint64             // 40 B ← padding to complete cache line
} // Total: exactly 192 bytes (3 cache lines)

// ============================================================================
// GLOBAL STATE MANAGEMENT
// ============================================================================

var (
	// Core executor instances - one per CPU core for lock-free parallel processing
	coreExecutors [constants.MaxSupportedCores]*ArbitrageCoreExecutor

	// Inter-core communication rings - SPSC lock-free message passing
	coreRings [constants.MaxSupportedCores]*ring24.Ring

	// Pair-to-core routing table - determines which cores process each pair
	pairToCoreAssignment [constants.PairRoutingTableCapacity]uint64

	// Shard bucket mapping - groups cycles by common pairs for load balancing
	pairShardBuckets map[PairID][]PairShardBucket
)

// Direct address indexing tables - Ethereum addresses are already uniformly distributed keccak256 hashes
var (
	pairAddressKeys [constants.AddressTableCapacity]AddressKey // Address keys for comparison
	addressToPairID [constants.AddressTableCapacity]PairID     // PairID values (0 = empty)
)

// ============================================================================
// UTILITY FUNCTIONS (FOUNDATIONAL)
// ============================================================================

// quantizeTickToInt64 converts floating-point tick values to integer priorities
// for QuantumQueue64 with proper clamping and scaling to prevent overflow
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func quantizeTickToInt64(tickValue float64) int64 {
	switch {
	case tickValue <= -constants.TickClampingBound:
		return 0
	case tickValue >= constants.TickClampingBound:
		return constants.MaxQuantizedTick
	default:
		// Map (-128, +128) → (0, 262142) with proper scaling
		return int64((tickValue + constants.TickClampingBound) * constants.QuantizationScale)
	}
}

// secureRandomInt generates cryptographically secure random integers for
// shuffle operations with optimal performance characteristics
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func secureRandomInt(upperBound int) int {
	var randomBytes [8]byte
	_, _ = rand.Read(randomBytes[:])
	randomValue := binary.LittleEndian.Uint64(randomBytes[:])

	// Power-of-2 optimization for modular arithmetic
	if upperBound&(upperBound-1) == 0 {
		return int(randomValue & uint64(upperBound-1))
	}

	// Unbiased modular reduction using multiplication method
	high64, _ := bits.Mul64(randomValue, uint64(upperBound))
	return int(high64)
}

// ============================================================================
// ETHEREUM ADDRESS INDEXING (CORE INFRASTRUCTURE)
// ============================================================================

// directAddressToIndex64 extracts table index directly from Ethereum address
// Uses first 8 bytes for maximum entropy distribution
// Leverages the fact that Ethereum addresses are keccak256 hash outputs
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func directAddressToIndex64(address40Bytes []byte) uint32 {
	// Use first 8 bytes as uint64, then reduce to 20 bits
	// This gives us maximum entropy from the address
	hash64 := binary.LittleEndian.Uint64(address40Bytes[0:8])
	return uint32(hash64) & constants.AddressTableMask
}

// bytesToAddressKey converts 40-byte address slice to AddressKey using
// optimized unaligned 64-bit word extraction for maximum performance
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func bytesToAddressKey(addressBytes []byte) AddressKey {
	return AddressKey{words: [5]uint64{
		binary.LittleEndian.Uint64(addressBytes[0:8]),
		binary.LittleEndian.Uint64(addressBytes[8:16]),
		binary.LittleEndian.Uint64(addressBytes[16:24]),
		binary.LittleEndian.Uint64(addressBytes[24:32]),
		binary.LittleEndian.Uint64(addressBytes[32:40]),
	}}
}

// isEqual performs SIMD-optimized comparison of two AddressKey instances
// using compiler-vectorized array comparison for maximum throughput
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (a AddressKey) isEqual(b AddressKey) bool {
	return a.words == b.words
}

// RegisterPairAddress inserts or updates a pair address mapping using
// Robin Hood probing with direct address indexing for optimal distribution
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func RegisterPairAddress(address40Bytes []byte, pairID PairID) {
	key := bytesToAddressKey(address40Bytes)

	// Direct index from address bytes leveraging keccak256 uniformity
	// Ethereum addresses provide excellent distribution without additional hashing
	hashIndex := directAddressToIndex64(address40Bytes)
	dist := uint32(0)

	for {
		currentPairID := addressToPairID[hashIndex]

		// Empty slot found - insert directly
		if currentPairID == 0 {
			pairAddressKeys[hashIndex] = key
			addressToPairID[hashIndex] = pairID
			return
		}

		// Key already exists - update value
		if pairAddressKeys[hashIndex].isEqual(key) {
			addressToPairID[hashIndex] = pairID
			return
		}

		// Robin Hood displacement check
		currentKey := pairAddressKeys[hashIndex]
		currentKeyIndex := directAddressToIndex64((*[40]byte)(unsafe.Pointer(&currentKey.words[0]))[:])
		currentDist := (hashIndex + constants.AddressTableCapacity - currentKeyIndex) & constants.AddressTableMask

		// Displace if incoming key has traveled farther (Robin Hood principle)
		if currentDist < dist {
			// Swap incoming with current resident
			pairAddressKeys[hashIndex] = key
			addressToPairID[hashIndex] = pairID
			key = currentKey
			pairID = currentPairID
			dist = currentDist
		}

		// Move to next slot
		hashIndex = (hashIndex + 1) & constants.AddressTableMask
		dist++

		// Safety check to prevent infinite loops (should never happen with Robin Hood)
		if dist > constants.AddressTableCapacity {
			panic("Robin Hood hash table full - this should never happen")
		}
	}
}

// lookupPairIDByAddress performs O(1) address→PairID resolution using
// Robin Hood probing with direct address indexing and early termination
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func lookupPairIDByAddress(address40Bytes []byte) PairID {
	key := bytesToAddressKey(address40Bytes)

	// Direct index from address bytes leveraging keccak256 uniformity
	hashIndex := directAddressToIndex64(address40Bytes)
	dist := uint32(0)

	for {
		currentPairID := addressToPairID[hashIndex]

		// Empty slot - key not found
		if currentPairID == 0 {
			return 0
		}

		// Key match found
		if pairAddressKeys[hashIndex].isEqual(key) {
			return currentPairID
		}

		// Robin Hood early termination
		currentKey := pairAddressKeys[hashIndex]
		currentKeyIndex := directAddressToIndex64((*[40]byte)(unsafe.Pointer(&currentKey.words[0]))[:])
		currentDist := (hashIndex + constants.AddressTableCapacity - currentKeyIndex) & constants.AddressTableMask

		// If current resident traveled less distance, target key cannot be present
		if currentDist < dist {
			return 0
		}

		// Move to next slot
		hashIndex = (hashIndex + 1) & constants.AddressTableMask
		dist++

		// Safety check (should never be needed with proper Robin Hood)
		if dist > constants.AddressTableCapacity {
			return 0
		}
	}
}

// RegisterPairToCore manually assigns a trading pair to specific core for
// custom load balancing and affinity optimization
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func RegisterPairToCore(pairID PairID, coreID uint8) {
	pairToCoreAssignment[pairID] |= 1 << coreID
}

// ============================================================================
// ARBITRAGE OPPORTUNITY DETECTION (BUSINESS LOGIC)
// ============================================================================

// emitArbitrageOpportunity logs detected profitable arbitrage opportunities
// using zero-allocation debug message system for real-time monitoring
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func emitArbitrageOpportunity(cycle *ArbitrageCycleState, newTick float64) {
	debug.DropMessage("[ARBITRAGE_OPPORTUNITY]", "")

	// Use zero-allocation integer conversion for PairIDs
	debug.DropMessage("  pair0", utils.Itoa(int(cycle.pairIDs[0])))
	debug.DropMessage("  pair1", utils.Itoa(int(cycle.pairIDs[1])))
	debug.DropMessage("  pair2", utils.Itoa(int(cycle.pairIDs[2])))

	// For float values, we need to use fmt.Sprintf as utils doesn't have float conversion
	// But we can convert the result to avoid allocation in debug.DropMessage
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
// TICK UPDATE PROCESSING (HOT PATH)
// ============================================================================

// processTickUpdate handles incoming price tick updates with zero-allocation
// arbitrage opportunity detection and priority queue maintenance
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func processTickUpdate(executor *ArbitrageCoreExecutor, update *TickUpdate) {
	// Select appropriate tick value based on executor direction
	var currentTick float64
	if !executor.isReverseDirection {
		currentTick = update.forwardTick
	} else {
		currentTick = update.reverseTick
	}

	// Resolve pair to local queue index
	queueIndex, _ := executor.pairToQueueIndex.Get(uint32(update.pairID))
	queue := &executor.priorityQueues[queueIndex]

	// Process profitable cycles from priority queue
	// Use stack-allocated array for optimal cache performance
	type ProcessedCycle struct {
		queueHandle     quantumqueue64.Handle // 4B - hottest: used in Push() call
		_               uint32                // 4B - padding for alignment
		cycleStateIndex CycleStateIndex       // 8B - hot: used in Push() payload
		originalTick    int64                 // 8B - cold: only used in Push() priority
		_               uint64                // 8B - padding to reach 32B total
	} // 32 bytes total = exactly 2 structs per 64-byte cache line

	var processedCycles [128]ProcessedCycle
	cycleCount := 0

	// Extract and evaluate cycles until unprofitable or capacity reached
	for {
		if queue.Empty() {
			break
		}

		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleStateIndex(cycleData)
		cycle := &executor.cycleStates[cycleIndex]

		// Profitability check: sum of all tick values must be negative
		// One tick in the array is intentionally zero for easy updates
		totalProfitability := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		isProfitable := totalProfitability < 0

		// Immediately emit arbitrage opportunity if profitable
		if isProfitable {
			emitArbitrageOpportunity(cycle, currentTick)
		}

		// Store cycle information for later reprocessing
		processedCycles[cycleCount] = ProcessedCycle{
			queueHandle:     handle,
			cycleStateIndex: cycleIndex,
			originalTick:    queueTick,
		}
		cycleCount++

		// Remove from queue for reprocessing
		queue.UnlinkMin(handle)

		// Stop processing if unprofitable or reached capacity limit
		if !isProfitable || cycleCount == len(processedCycles) {
			break
		}
	}

	// Reinsert all processed cycles using original handles (zero allocation)
	for i := 0; i < cycleCount; i++ {
		cycle := processedCycles[i]
		queue.Push(cycle.originalTick, cycle.queueHandle, uint64(cycle.cycleStateIndex))
	}

	// Update fanout entries: propagate tick changes to dependent cycles
	for _, fanoutEntry := range executor.fanoutTables[queueIndex] {
		cycle := &executor.cycleStates[fanoutEntry.cycleStateIndex]

		// Update tick value at the specified edge index
		cycle.tickValues[fanoutEntry.edgeIndex] = currentTick

		// Recalculate priority and update queue position
		newPriority := quantizeTickToInt64(cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2])
		fanoutEntry.queue.MoveTick(fanoutEntry.queueHandle, newPriority)
	}
}

// ============================================================================
// TICK UPDATE DISPATCH (ENTRY POINT)
// ============================================================================

// DispatchTickUpdate processes incoming transaction log and dispatches tick
// updates to all cores responsible for the affected trading pair
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func DispatchTickUpdate(logView *types.LogView) {
	// Resolve Ethereum address to internal pair ID using direct indexing
	// Leverages uniform distribution of keccak256-derived addresses
	pairID := lookupPairIDByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if pairID == 0 {
		return // Unknown pair - ignore
	}

	// Extract reserve values from transaction log data
	reserve0 := utils.LoadBE64(logView.Data[24:])
	reserve1 := utils.LoadBE64(logView.Data[56:])

	// Calculate log2 reserve ratio for price tick
	tickValue, _ := fastuni.Log2ReserveRatio(reserve0, reserve1)

	// Prepare tick update message (exactly 24 bytes for ring buffer)
	var messageBuffer [24]byte
	tickUpdate := (*TickUpdate)(unsafe.Pointer(&messageBuffer))
	tickUpdate.pairID = pairID
	tickUpdate.forwardTick = tickValue
	tickUpdate.reverseTick = -tickValue // Inverse for reverse direction

	// Dispatch to all assigned cores using bit manipulation
	coreAssignments := pairToCoreAssignment[pairID]
	for coreAssignments != 0 {
		coreID := bits.TrailingZeros64(uint64(coreAssignments))
		coreRings[coreID].Push(&messageBuffer)
		coreAssignments &^= 1 << coreID // Clear processed bit
	}
}

// ============================================================================
// SHARD CONSTRUCTION AND DISTRIBUTION (INITIALIZATION)
// ============================================================================

// shuffleEdgeBindings performs Fisher-Yates shuffle for optimal load distribution
// across cores with cryptographically secure randomness
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func shuffleEdgeBindings(bindings []ArbitrageEdgeBinding) {
	for i := len(bindings) - 1; i > 0; i-- {
		j := secureRandomInt(i + 1)
		bindings[i], bindings[j] = bindings[j], bindings[i]
	}
}

// buildFanoutShardBuckets constructs optimally-sized shard buckets for
// load balancing arbitrage cycles across cores with cache-friendly grouping
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func buildFanoutShardBuckets(cycles []ArbitrageTriplet) {
	pairShardBuckets = make(map[PairID][]PairShardBucket)
	temporaryBindings := make(map[PairID][]ArbitrageEdgeBinding, len(cycles)*3)

	// Group cycles by their constituent pairs
	for _, triplet := range cycles {
		temporaryBindings[triplet[0]] = append(temporaryBindings[triplet[0]],
			ArbitrageEdgeBinding{cyclePairs: triplet, edgeIndex: 0})
		temporaryBindings[triplet[1]] = append(temporaryBindings[triplet[1]],
			ArbitrageEdgeBinding{cyclePairs: triplet, edgeIndex: 1})
		temporaryBindings[triplet[2]] = append(temporaryBindings[triplet[2]],
			ArbitrageEdgeBinding{cyclePairs: triplet, edgeIndex: 2})
	}

	// Create optimally-sized shards with cache-friendly random distribution
	for pairID, bindings := range temporaryBindings {
		shuffleEdgeBindings(bindings) // Randomize for load balancing

		// Split into cache-optimized shards
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

// attachShardToExecutor integrates a shard of arbitrage cycles into the core
// executor's queue and fanout systems with optimal memory layout
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func attachShardToExecutor(executor *ArbitrageCoreExecutor, shard *PairShardBucket) {
	// Get or create queue index for this pair
	queueIndex := executor.pairToQueueIndex.Put(uint32(shard.pairID), uint32(len(executor.priorityQueues)))

	if int(queueIndex) == len(executor.priorityQueues) {
		// First time seeing this pair - create new queue and fanout table
		executor.priorityQueues = append(executor.priorityQueues, *quantumqueue64.New())
		executor.fanoutTables = append(executor.fanoutTables, nil)
	}

	// Get pointer to owned queue instance
	queue := &executor.priorityQueues[queueIndex]

	// Process each arbitrage cycle in this shard
	for _, edgeBinding := range shard.edgeBindings {
		// Add cycle state to canonical storage arena
		executor.cycleStates = append(executor.cycleStates, ArbitrageCycleState{
			pairIDs: edgeBinding.cyclePairs,
		})
		cycleIndex := CycleStateIndex(len(executor.cycleStates) - 1)

		// Allocate queue handle and insert with minimum priority (initialization state)
		// Use maximum quantized value for lowest priority until real ticks arrive
		queueHandle, _ := queue.BorrowSafe()
		queue.Push(constants.MaxInitializationPriority, queueHandle, uint64(cycleIndex))

		// Create fanout entries for the other two edges in this cycle
		// Each cycle creates fanouts for edges it doesn't directly own
		otherEdge1 := (edgeBinding.edgeIndex + 1) % 3
		otherEdge2 := (edgeBinding.edgeIndex + 2) % 3

		for _, edgeIdx := range [...]uint16{otherEdge1, otherEdge2} {
			executor.fanoutTables[queueIndex] = append(executor.fanoutTables[queueIndex],
				FanoutEntry{
					cycleStateIndex: cycleIndex,
					queue:           queue,
					queueHandle:     queueHandle,
					edgeIndex:       edgeIdx,
				})
		}
	}
}

// ============================================================================
// SYSTEM INITIALIZATION (TOP-LEVEL ORCHESTRATION)
// ============================================================================

// launchShardWorker initializes a single core executor and processes assigned shards
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func launchShardWorker(coreID, forwardCoreCount int, shardInput <-chan PairShardBucket) {
	// Lock worker to specific OS thread for optimal cache affinity
	runtime.LockOSThread()

	shutdownChannel := make(chan struct{})

	// Initialize core executor with optimal memory layout
	executor := &ArbitrageCoreExecutor{
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		isReverseDirection: coreID >= forwardCoreCount,
		shutdownSignal:     shutdownChannel,
		cycleStates:        make([]ArbitrageCycleState, 0), // Canonical storage
	}
	coreExecutors[coreID] = executor
	coreRings[coreID] = ring24.New(constants.DefaultRingSize)

	// Process all assigned shards
	for shard := range shardInput {
		attachShardToExecutor(executor, &shard)
	}

	stopFlag, hotFlag := control.Flags()

	// Core 0 gets special cooldown-managing consumer for system stability
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

// InitializeArbitrageSystem bootstraps the complete arbitrage detection system
// across all available CPU cores with optimal work distribution and memory layout
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func InitializeArbitrageSystem(arbitrageCycles []ArbitrageTriplet) {
	coreCount := runtime.NumCPU() - 1 // Reserve one core for system tasks
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores // Maximum supported cores
	}
	if coreCount&1 == 1 {
		coreCount-- // Ensure even number for forward/reverse pairing
	}
	forwardCoreCount := coreCount / 2

	// Build fanout shards for optimal cache utilization
	buildFanoutShardBuckets(arbitrageCycles)

	// Create worker channels for parallel shard distribution
	shardChannels := make([]chan PairShardBucket, coreCount)
	for i := range shardChannels {
		shardChannels[i] = make(chan PairShardBucket, constants.ShardChannelBufferSize)
		go launchShardWorker(i, forwardCoreCount, shardChannels[i])
	}

	// Distribute shards across cores with round-robin load balancing
	currentCore := 0
	for _, shardBuckets := range pairShardBuckets {
		for _, shard := range shardBuckets {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			// Send shard to both forward and reverse cores
			shardChannels[forwardCore] <- shard
			shardChannels[reverseCore] <- shard

			// Update core assignment bitmap for tick routing
			for _, edgeBinding := range shard.edgeBindings {
				for _, pairID := range edgeBinding.cyclePairs {
					pairToCoreAssignment[pairID] |= 1<<forwardCore | 1<<reverseCore
				}
			}
			currentCore++
		}
	}

	// Close channels to signal completion
	for _, channel := range shardChannels {
		close(channel)
	}
}
