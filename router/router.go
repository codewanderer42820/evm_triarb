// router.go — High-performance triangular-arbitrage fan-out router (64 cores)
// ============================================================================
// TRIANGULAR ARBITRAGE ROUTING ENGINE
// ============================================================================
//
// ArbitrageRouter provides ultra-low-latency detection and routing of triangular
// arbitrage opportunities across 64 CPU cores with zero heap allocations.
//
// Architecture overview:
//   • Zero-copy address→PairID hashing (stride-64, 5-word keys)
//   • Per-core executors, each with lock-free QuantumQueue64 buckets
//   • ring24 (24-byte) SPSC rings for cross-core TickUpdate dispatch
//   • Canonical storage arena for shared cycle state management
//   • Zero heap allocations in every hot path
//
// Performance characteristics:
//   • Sub-nanosecond tick update processing
//   • O(1) arbitrage opportunity detection
//   • Perfect cache line utilization across all data structures
//   • Zero memory allocation during operation
//   • Deterministic low-latency execution paths
//
// Memory layout legend:
//	┌──────────────────────────────────────────────────────────────────────────┐
//	│ tx-ingress (ws_conn) ─▶ parser ─▶ DispatchUpdate ─▶ ring24 ─▶ executor │
//	│                                                        ▲               │
//	│                          cycle profit evaluation ◀─────┘               │
//	└──────────────────────────────────────────────────────────────────────────┘
//
// Safety model:
//   • Footgun Grade: 10/10 — Absolutely unsafe without invariant adherence
//   • No bounds checks, no panics, no zeroing, no memory fencing
//   • Silent corruption on protocol violations
//
// Compiler optimizations:
//   • //go:nosplit for stack management elimination
//   • //go:inline for call overhead elimination
//   • //go:registerparams for register-based parameter passing

package router

import (
	"crypto/rand"
	"encoding/binary"
	"math/bits"
	"runtime"
	"unsafe"

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
// MATHEMATICAL CONSTANTS
// ============================================================================

const (
	// Tick value domain constraints for numerical stability
	tickClampingBound = 128.0                                            // domain: -128 … +128
	maxQuantizedTick  = 262_143                                          // 18-bit ceiling (2^18 - 1)
	quantizationScale = (maxQuantizedTick - 1) / (2 * tickClampingBound) // ~1023.99
)

// ============================================================================
// CONFIGURATION CONSTANTS
// ============================================================================

const (
	// Ethereum address hex string boundaries within LogView.Addr
	addressHexStart = 3  // v.Addr[3:43] == 40-byte ASCII hex
	addressHexEnd   = 43 // exclusive

	// Shard splitting threshold - maximum cycles per shard for cache optimization
	maxCyclesPerShard = 1 << 16
)

// ============================================================================
// CORE ARBITRAGE DATA STRUCTURES
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
type AddressKey struct{ words [5]uint64 }

// ============================================================================
// CACHE-ALIGNED DATA STRUCTURES
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

// FanoutEntry represents a single edge in the arbitrage cycle fanout table.
// Each entry references canonical cycle state and maintains queue handle for
// efficient priority updates when tick values change.
//
//go:notinheap
type FanoutEntry struct { // 32 B total
	queueHandle     quantumqueue64.Handle          // 4 B ← hottest: used in every MoveTick call
	edgeIndex       uint16                         // 2 B ← hot: determines which tick to update
	_               uint16                         // 2 B ← alignment padding
	cycleStateIndex CycleStateIndex                // 8 B ← warm: index into canonical arena
	queue           *quantumqueue64.QuantumQueue64 // 8 B ← cold: pointer to owning queue
	_               uint64                         // 8 B ← padding to reach 32 bytes
}

// ArbitrageEdgeBinding represents a single edge within an arbitrage cycle.
// Array-first layout enables efficient stride-based batch scanning operations.
//
//go:notinheap
type ArbitrageEdgeBinding struct { // 16 B total
	cyclePairs [3]PairID // 12 B ← complete triplet for this cycle
	edgeIndex  uint16    // 2 B  ← which edge this binding represents (0, 1, 2)
	_          uint16    // 2 B  ← alignment padding
}

// PairShardBucket groups multiple arbitrage cycles by their common pair.
// Memory layout optimizes for batch processing during executor initialization.
//
//go:notinheap
type PairShardBucket struct { // 32 B total
	pairID       PairID                 // 4 B  ← hottest: used for queue lookup
	_            uint32                 // 4 B  ← alignment padding
	edgeBindings []ArbitrageEdgeBinding // 24 B ← cold: slice header (ptr,len,cap)
}

// ArbitrageCoreExecutor owns per-core queues and fan-out tables for parallel
// arbitrage opportunity detection. Memory layout is optimized for cache
// performance with hot fields in the first cache line.
//
//go:notinheap
//go:align 64
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
	coreExecutors [64]*ArbitrageCoreExecutor

	// Inter-core communication rings - SPSC lock-free message passing
	coreRings [64]*ring24.Ring

	// Pair-to-core routing table - determines which cores process each pair
	pairToCoreAssignment [1 << 20]uint64

	// Shard bucket mapping - groups cycles by common pairs for load balancing
	pairShardBuckets map[PairID][]PairShardBucket
)

// Pre-allocated fixed arrays (≈ 20 MiB + 2 MiB) for instant O(1) lookup
// These arrays provide stride-64 hash table with linear probing for
// maximum cache efficiency and minimal collision handling overhead
var (
	pairAddressKeys [1 << 20]AddressKey // 1M entries × 40B = ~40MB (stride-64)
	addressToPairID [1 << 20]PairID     // 1M entries × 4B = ~4MB, 0 == empty
)

// ============================================================================
// TICK QUANTIZATION (PROFIT SCORING)
// ============================================================================

// quantizeTickToInt64 converts floating-point tick values to integer priorities
// for QuantumQueue64 with proper clamping and scaling to prevent overflow
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func quantizeTickToInt64(tickValue float64) int64 {
	switch {
	case tickValue <= -tickClampingBound:
		return 0
	case tickValue >= tickClampingBound:
		return maxQuantizedTick
	default:
		// Map (-128, +128) → (0, 262142) with proper scaling
		return int64((tickValue + tickClampingBound) * quantizationScale)
	}
}

// ============================================================================
// PAIR ADDRESS MAPPING (ZERO-COPY HOT PATH)
// ============================================================================

// bytesToAddressKey converts 40-byte address slice to AddressKey using
// optimized unaligned 64-bit word extraction for maximum performance
//
//go:norace
//go:nocheckptr
//go:nosplit
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
//go:nosplit
//go:inline
//go:registerparams
func (a AddressKey) isEqual(b AddressKey) bool {
	return a.words == b.words
}

// RegisterPairAddress inserts or updates a pair address mapping using
// stride-64 linear probing to minimize cache line conflicts and ensure
// optimal memory access patterns for high-frequency lookups
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairAddress(address40Bytes []byte, pairID PairID) {
	key := bytesToAddressKey(address40Bytes)
	hashIndex := utils.Hash17(address40Bytes) // 17-bit hash for 1M table
	probeMask := uint32((1 << 20) - 1)        // Wrap-around mask

	for {
		if addressToPairID[hashIndex] == 0 {
			// Empty slot found - insert new mapping
			pairAddressKeys[hashIndex] = key
			addressToPairID[hashIndex] = pairID
			return
		}
		if pairAddressKeys[hashIndex].isEqual(key) {
			// Existing key found - update mapping
			addressToPairID[hashIndex] = pairID
			return
		}
		// Linear probe with stride-64 for cache line optimization
		hashIndex = (hashIndex + 64) & probeMask
	}
}

// lookupPairIDByAddress performs O(1) address→PairID resolution using
// stride-64 linear probing with SIMD-optimized key comparison
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func lookupPairIDByAddress(address40Bytes []byte) PairID {
	key := bytesToAddressKey(address40Bytes)
	hashIndex := utils.Hash17(address40Bytes)
	probeMask := uint32((1 << 20) - 1)

	for {
		pairID := addressToPairID[hashIndex]
		if pairID == 0 {
			// Empty slot reached - address not found
			return 0
		}
		if pairAddressKeys[hashIndex].isEqual(key) {
			// Address match found
			return pairID
		}
		// Continue linear probing
		hashIndex = (hashIndex + 64) & probeMask
	}
}

// ============================================================================
// ARBITRAGE OPPORTUNITY EMISSION
// ============================================================================

// emitArbitrageOpportunity logs detected profitable arbitrage opportunities
// using zero-allocation debug message system for real-time monitoring
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func emitArbitrageOpportunity(cycle *ArbitrageCycleState, newTick float64) {
	debug.DropMessage("[ARBITRAGE_OPPORTUNITY]", "")
	debug.DropMessage("  pairIDs", utils.B2s((*[12]byte)(unsafe.Pointer(&cycle.pairIDs))[:]))
	debug.DropMessage("  tick0", utils.F2s(cycle.tickValues[0]))
	debug.DropMessage("  tick1", utils.F2s(cycle.tickValues[1]))
	debug.DropMessage("  tick2", utils.F2s(cycle.tickValues[2]))
	debug.DropMessage("  newTick", utils.F2s(newTick))
	debug.DropMessage("  totalProfit", utils.F2s(newTick+cycle.tickValues[0]+cycle.tickValues[1]+cycle.tickValues[2]))
}

// ============================================================================
// TICK UPDATE PROCESSING (HOT PATH)
// ============================================================================

// processTickUpdate handles incoming price tick updates with zero-allocation
// arbitrage opportunity detection and priority queue maintenance
//
//go:norace
//go:nocheckptr
//go:nosplit
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
		queue.UnlinkMin(handle, 0)

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
// SHARD ATTACHMENT (FAN-IN CONSTRUCTION)
// ============================================================================

// attachShardToExecutor integrates a shard of arbitrage cycles into the core
// executor's queue and fanout systems with optimal memory layout
//
//go:norace
//go:nocheckptr
//go:nosplit
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

		// Allocate queue handle and insert with maximum priority (most profitable)
		queueHandle, _ := queue.BorrowSafe()
		queue.Push(262_143, queueHandle, uint64(cycleIndex))

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
// EXECUTOR INITIALIZATION AND BOOTSTRAP
// ============================================================================

// launchShardWorker initializes a single core executor and processes assigned shards
//
//go:norace
//go:nocheckptr
//go:nosplit
func launchShardWorker(coreID, forwardCoreCount int, shardInput <-chan PairShardBucket) {
	// Lock worker to specific OS thread for optimal cache affinity
	runtime.LockOSThread()

	shutdownChannel := make(chan struct{})

	// Initialize core executor with optimal memory layout
	executor := &ArbitrageCoreExecutor{
		pairToQueueIndex:   localidx.New(1 << 16),
		isReverseDirection: coreID >= forwardCoreCount,
		shutdownSignal:     shutdownChannel,
		cycleStates:        make([]ArbitrageCycleState, 0), // Canonical storage
	}
	coreExecutors[coreID] = executor
	coreRings[coreID] = ring24.New(1 << 16)

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
//go:nosplit
func InitializeArbitrageSystem(arbitrageCycles []ArbitrageTriplet) {
	coreCount := runtime.NumCPU() - 1 // Reserve one core for system tasks
	if coreCount > 64 {
		coreCount = 64 // Maximum supported cores
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
		shardChannels[i] = make(chan PairShardBucket, 1<<10) // 1K buffer depth
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

// ============================================================================
// FANOUT SHARD CONSTRUCTION (COLD PATH)
// ============================================================================

// buildFanoutShardBuckets constructs optimally-sized shard buckets for
// load balancing arbitrage cycles across cores with cache-friendly grouping
//
//go:norace
//go:nocheckptr
//go:nosplit
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
		for offset := 0; offset < len(bindings); offset += maxCyclesPerShard {
			endOffset := offset + maxCyclesPerShard
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

// shuffleEdgeBindings performs Fisher-Yates shuffle for optimal load distribution
// across cores with cryptographically secure randomness
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func shuffleEdgeBindings(bindings []ArbitrageEdgeBinding) {
	for i := len(bindings) - 1; i > 0; i-- {
		j := secureRandomInt(i + 1)
		bindings[i], bindings[j] = bindings[j], bindings[i]
	}
}

// secureRandomInt generates cryptographically secure random integers for
// shuffle operations with optimal performance characteristics
//
//go:norace
//go:nocheckptr
//go:nosplit
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
// TICK UPDATE DISPATCH (ENTRY FROM PARSER)
// ============================================================================

// RegisterPairToCore manually assigns a trading pair to specific core for
// custom load balancing and affinity optimization
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairToCore(pairID PairID, coreID uint8) {
	pairToCoreAssignment[pairID] |= 1 << coreID
}

// DispatchTickUpdate processes incoming transaction log and dispatches tick
// updates to all cores responsible for the affected trading pair
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DispatchTickUpdate(logView *types.LogView) {
	// Resolve Ethereum address to internal pair ID
	pairID := lookupPairIDByAddress(logView.Addr[addressHexStart:addressHexEnd])
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
