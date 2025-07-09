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

// Core type definitions
type PairID uint32
type ArbitrageTriplet [3]PairID
type CycleStateIndex uint64

// AddressKey - 32-byte aligned for optimal cache performance
//
//go:notinheap
//go:align 32
type AddressKey struct {
	words [3]uint64 // 24B - Ethereum address (20B) + padding
	_     [8]byte   // 8B - Padding to 32 bytes
}

// TickUpdate - Price tick message (24 bytes for ring buffer)
//
//go:notinheap
type TickUpdate struct {
	forwardTick float64 // 8B - Forward direction price
	reverseTick float64 // 8B - Reverse direction price
	pairID      PairID  // 4B - Pair identifier
	_           [4]byte // 4B - Padding to 24 bytes
}

// ArbitrageCycleState - Cycle state with hot data first
//
//go:notinheap
//go:align 64
type ArbitrageCycleState struct {
	// Hot fields (24 bytes) - accessed every calculation
	tickValues [3]float64 // 24B - Price ticks for each edge

	// Warm fields (12 bytes)
	pairIDs [3]PairID // 12B - Pair identifiers

	// Padding to 64 bytes
	_ [28]byte // 28B - Padding to 64-byte cache line boundary
}

// ArbitrageEdgeBinding - Edge within arbitrage cycle
//
//go:notinheap
//go:align 16
type ArbitrageEdgeBinding struct {
	cyclePairs [3]PairID // 12B - Complete triplet
	edgeIndex  uint16    // 2B - Edge index (0-2)
	_          [2]byte   // 2B - Padding to 16 bytes
}

// FanoutEntry - Edge reference in fanout table
//
//go:notinheap
//go:align 32
type FanoutEntry struct {
	// Hot fields (8 bytes)
	queueHandle quantumqueue64.Handle // 4B - Used in MoveTick
	edgeIndex   uint16                // 2B - Tick selection
	_           [2]byte               // 2B - Padding

	// Warm fields (8 bytes)
	cycleStateIndex CycleStateIndex // 8B - State lookup

	// Cold fields (16 bytes)
	queue *quantumqueue64.QuantumQueue64 // 8B - Queue pointer
	_     [8]byte                        // 8B - Padding to 32 bytes
}

// PairShardBucket - Cycle grouping by common pair
//
//go:notinheap
//go:align 32
type PairShardBucket struct {
	// Hot fields (8 bytes)
	pairID PairID  // 4B - Queue lookup
	_      [4]byte // 4B - Padding

	// Cold fields (24 bytes)
	edgeBindings []ArbitrageEdgeBinding // 24B - Slice header (ptr,len,cap)
}

// ArbitrageCoreExecutor - Per-core queue and fanout owner
//
//go:notinheap
//go:align 64
type ArbitrageCoreExecutor struct {
	// Cache line 1: Hot fields (64 bytes)
	priorityQueues     []quantumqueue64.QuantumQueue64 // 24B - Owned queues
	fanoutTables       [][]FanoutEntry                 // 24B - Fanout mappings
	shutdownSignal     <-chan struct{}                 // 8B - Shutdown channel
	isReverseDirection bool                            // 1B - Direction flag
	_                  [7]byte                         // 7B - Padding to 64 bytes

	// Cache line 2: Local index (64 bytes)
	pairToQueueIndex localidx.Hash // 64B - O(1) pair→queue mapping

	// Cache line 3: Canonical storage (64 bytes)
	cycleStates []ArbitrageCycleState // 24B - Direct storage
	_           [40]byte              // 40B - Padding to 64 bytes
}

// Global state
var (
	coreExecutors        [constants.MaxSupportedCores]*ArbitrageCoreExecutor
	coreRings            [constants.MaxSupportedCores]*ring24.Ring
	pairToCoreAssignment [constants.PairRoutingTableCapacity]uint64
	pairShardBuckets     map[PairID][]PairShardBucket
)

// Direct address indexing - leverages keccak256 uniformity
var (
	pairAddressKeys [constants.AddressTableCapacity]AddressKey
	addressToPairID [constants.AddressTableCapacity]PairID
)

// quantizeTickToInt64 converts float tick to queue priority
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func quantizeTickToInt64(tickValue float64) int64 {
	switch {
	case tickValue <= -constants.TickClampingBound:
		return 0
	case tickValue >= constants.TickClampingBound:
		return constants.MaxQuantizedTick
	default:
		return int64((tickValue + constants.TickClampingBound) * constants.QuantizationScale)
	}
}

// secureRandomInt generates crypto-secure random for shuffling
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

	// Power-of-2 optimization
	if upperBound&(upperBound-1) == 0 {
		return int(randomValue & uint64(upperBound-1))
	}

	// Unbiased modular reduction
	high64, _ := bits.Mul64(randomValue, uint64(upperBound))
	return int(high64)
}

// directAddressToIndex32 extracts index from Ethereum address
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex32(address40Bytes []byte) uint32 {
	// Parse to get proper density (40 hex chars → 20 bytes)
	addressBytes := utils.ParseEthereumAddress(address40Bytes)

	// Use 8 bytes (64 bits) for maximum entropy without mixing
	hash64 := *(*uint64)(unsafe.Pointer(&addressBytes[0]))

	return uint32(hash64) & constants.AddressTableMask
}

// bytesToAddressKey converts 20-byte address to optimized AddressKey
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func bytesToAddressKey(address40HexBytes []byte) AddressKey {
	// Parse hex string to 20-byte address
	parsedAddress := utils.ParseEthereumAddress(address40HexBytes)

	// Pack 20 bytes into 3×8-byte words: 8+8+4 = 20 bytes exactly
	return AddressKey{
		words: [3]uint64{
			*(*uint64)(unsafe.Pointer(&parsedAddress[0])),                    // Bytes 0-7
			*(*uint64)(unsafe.Pointer(&parsedAddress[8])),                    // Bytes 8-15
			uint64(*(*uint32)(unsafe.Pointer(&parsedAddress[16]))) << 32,     // Bytes 16-19 in upper 32 bits
		},
	}
}

// isEqual performs optimized 20-byte address comparison
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (a AddressKey) isEqual(b AddressKey) bool {
	// Compare all 3 words - third word only uses upper 32 bits for address
	return a.words[0] == b.words[0] &&
		a.words[1] == b.words[1] &&
		a.words[2] == b.words[2]
}

// hashKey extracts hash from AddressKey for Robin Hood hashing
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (a AddressKey) hashKey() uint32 {
	return uint32(a.words[0]) & constants.AddressTableMask
}

// RegisterPairAddress inserts address→pair mapping with Robin Hood probing
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairAddress(address40HexBytes []byte, pairID PairID) {
	key := bytesToAddressKey(address40HexBytes)
	hashIndex := key.hashKey()
	dist := uint32(0)

	for {
		currentPairID := addressToPairID[hashIndex]

		// Empty slot
		if currentPairID == 0 {
			pairAddressKeys[hashIndex] = key
			addressToPairID[hashIndex] = pairID
			return
		}

		// Key exists - update
		if pairAddressKeys[hashIndex].isEqual(key) {
			addressToPairID[hashIndex] = pairID
			return
		}

		// Robin Hood displacement check
		currentKey := pairAddressKeys[hashIndex]
		currentKeyHash := currentKey.hashKey()
		currentDist := (hashIndex + constants.AddressTableCapacity - currentKeyHash) & constants.AddressTableMask

		if currentDist < dist {
			// Swap with resident entry
			pairAddressKeys[hashIndex] = key
			addressToPairID[hashIndex] = pairID
			key = currentKey
			pairID = currentPairID
			dist = currentDist
		}

		hashIndex = (hashIndex + 1) & constants.AddressTableMask
		dist++

		// Prevent infinite loops
		if dist > constants.AddressTableCapacity {
			panic("Robin Hood hash table full")
		}
	}
}

// lookupPairIDByAddress performs O(1) address lookup with Robin Hood early termination
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func lookupPairIDByAddress(address40HexBytes []byte) PairID {
	key := bytesToAddressKey(address40HexBytes)
	hashIndex := key.hashKey()
	dist := uint32(0)

	for {
		currentPairID := addressToPairID[hashIndex]

		// Empty slot - not found
		if currentPairID == 0 {
			return 0
		}

		// Key found
		if pairAddressKeys[hashIndex].isEqual(key) {
			return currentPairID
		}

		// Robin Hood early termination
		currentKey := pairAddressKeys[hashIndex]
		currentKeyHash := currentKey.hashKey()
		currentDist := (hashIndex + constants.AddressTableCapacity - currentKeyHash) & constants.AddressTableMask

		if currentDist < dist {
			return 0 // Key would have been placed here
		}

		hashIndex = (hashIndex + 1) & constants.AddressTableMask
		dist++

		// Prevent infinite loops
		if dist > constants.AddressTableCapacity {
			return 0
		}
	}
}

// RegisterPairToCore assigns pair to specific core with bit manipulation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairToCore(pairID PairID, coreID uint8) {
	if coreID < 64 { // Ensure valid core ID
		pairToCoreAssignment[pairID] |= 1 << coreID
	}
}

// emitArbitrageOpportunity logs profitable arbitrage opportunity
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func emitArbitrageOpportunity(cycle *ArbitrageCycleState, newTick float64) {
	debug.DropMessage("[ARBITRAGE_OPPORTUNITY]", "")

	// Log pair IDs
	debug.DropMessage("  pair0", utils.Itoa(int(cycle.pairIDs[0])))
	debug.DropMessage("  pair1", utils.Itoa(int(cycle.pairIDs[1])))
	debug.DropMessage("  pair2", utils.Itoa(int(cycle.pairIDs[2])))

	// Log tick values with proper formatting
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

// processTickUpdate handles price updates with arbitrage detection
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func processTickUpdate(executor *ArbitrageCoreExecutor, update *TickUpdate) {
	// Select tick based on direction
	var currentTick float64
	if !executor.isReverseDirection {
		currentTick = update.forwardTick
	} else {
		currentTick = update.reverseTick
	}

	// Get queue for pair
	queueIndex, exists := executor.pairToQueueIndex.Get(uint32(update.pairID))
	if !exists {
		return // Pair not assigned to this core
	}
	queue := &executor.priorityQueues[queueIndex]

	// Stack-allocated buffer for processed cycles
	type ProcessedCycle struct {
		queueHandle     quantumqueue64.Handle // 4B
		_               [4]byte               // 4B - Padding
		originalTick    int64                 // 8B
		cycleStateIndex CycleStateIndex       // 8B
		_               [8]byte               // 8B - Padding to 32B
	}

	var processedCycles [128]ProcessedCycle
	cycleCount := 0

	// Extract profitable cycles
	for !queue.Empty() && cycleCount < len(processedCycles) {
		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleStateIndex(cycleData)
		
		// Bounds check for cycle state
		if cycleIndex >= CycleStateIndex(len(executor.cycleStates)) {
			queue.UnlinkMin(handle)
			continue
		}
		
		cycle := &executor.cycleStates[cycleIndex]

		// Check profitability threshold
		totalProfitability := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		isProfitable := totalProfitability < 0

		if isProfitable {
			// Uncomment for debugging: emitArbitrageOpportunity(cycle, currentTick)
		}

		if !isProfitable {
			break // No more profitable cycles
		}

		// Store for reprocessing with unsafe pointer arithmetic for performance
		processedCycle := (*ProcessedCycle)(unsafe.Add(
			unsafe.Pointer(&processedCycles[0]),
			uintptr(cycleCount)*unsafe.Sizeof(ProcessedCycle{}),
		))
		*processedCycle = ProcessedCycle{
			queueHandle:     handle,
			originalTick:    queueTick,
			cycleStateIndex: cycleIndex,
		}
		cycleCount++

		queue.UnlinkMin(handle)
	}

	// Reinsert processed cycles with updated priorities
	for i := 0; i < cycleCount; i++ {
		cycle := (*ProcessedCycle)(unsafe.Add(
			unsafe.Pointer(&processedCycles[0]),
			uintptr(i)*unsafe.Sizeof(ProcessedCycle{}),
		))
		queue.Push(cycle.originalTick, cycle.queueHandle, uint64(cycle.cycleStateIndex))
	}

	// Update fanout entries efficiently
	fanoutEntries := executor.fanoutTables[queueIndex]
	for i := range fanoutEntries {
		fanoutEntry := &fanoutEntries[i]
		
		// Bounds check for cycle state
		if fanoutEntry.cycleStateIndex >= CycleStateIndex(len(executor.cycleStates)) {
			continue
		}
		
		cycle := &executor.cycleStates[fanoutEntry.cycleStateIndex]
		cycle.tickValues[fanoutEntry.edgeIndex] = currentTick

		// Recalculate priority for this cycle
		totalTick := cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		newPriority := quantizeTickToInt64(totalTick)
		fanoutEntry.queue.MoveTick(fanoutEntry.queueHandle, newPriority)
	}
}

// DispatchTickUpdate processes log and dispatches to assigned cores
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DispatchTickUpdate(logView *types.LogView) {
	// Bounds check for address extraction
	if len(logView.Addr) < constants.AddressHexEnd {
		return
	}

	// Resolve address to pair ID
	pairID := lookupPairIDByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if pairID == 0 {
		return // Unknown pair
	}

	// Bounds check for data extraction
	if len(logView.Data) < 64 {
		return // Insufficient data
	}

	// Extract reserves with big-endian conversion
	reserve0 := utils.LoadBE64(logView.Data[24:32])
	reserve1 := utils.LoadBE64(logView.Data[56:64])

	// Validate reserves
	if reserve0 == 0 || reserve1 == 0 {
		return // Invalid reserves
	}

	// Calculate logarithmic tick value
	tickValue, err := fastuni.Log2ReserveRatio(reserve0, reserve1)
	if err != nil {
		return // Failed to calculate ratio
	}

	// Prepare message with stack allocation
	var messageBuffer [24]byte
	tickUpdate := (*TickUpdate)(unsafe.Pointer(&messageBuffer))
	tickUpdate.forwardTick = tickValue
	tickUpdate.reverseTick = -tickValue
	tickUpdate.pairID = pairID

	// Dispatch to assigned cores using bit manipulation
	coreAssignments := pairToCoreAssignment[pairID]
	for coreAssignments != 0 {
		coreID := bits.TrailingZeros64(coreAssignments)
		if coreID < len(coreRings) && coreRings[coreID] != nil {
			coreRings[coreID].Push(&messageBuffer)
		}
		coreAssignments &^= 1 << coreID
	}
}

// shuffleEdgeBindings performs Fisher-Yates shuffle with crypto randomness
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

// buildFanoutShardBuckets constructs optimized shard buckets
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func buildFanoutShardBuckets(cycles []ArbitrageTriplet) {
	pairShardBuckets = make(map[PairID][]PairShardBucket)
	temporaryBindings := make(map[PairID][]ArbitrageEdgeBinding, len(cycles)*3)

	// Group edge bindings by pairs
	for cycleIdx, triplet := range cycles {
		for edgeIdx := 0; edgeIdx < 3; edgeIdx++ {
			pairID := triplet[edgeIdx]
			temporaryBindings[pairID] = append(temporaryBindings[pairID],
				ArbitrageEdgeBinding{
					cyclePairs: triplet,
					edgeIndex:  uint16(edgeIdx),
				})
		}
	}

	// Create shards with load balancing
	for pairID, bindings := range temporaryBindings {
		shuffleEdgeBindings(bindings)

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

// attachShardToExecutor integrates shard into core executor
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func attachShardToExecutor(executor *ArbitrageCoreExecutor, shard *PairShardBucket) {
	// Get or create queue index
	queueIndex := executor.pairToQueueIndex.Put(uint32(shard.pairID), uint32(len(executor.priorityQueues)))

	// Expand slices if needed
	if int(queueIndex) == len(executor.priorityQueues) {
		executor.priorityQueues = append(executor.priorityQueues, *quantumqueue64.New())
		executor.fanoutTables = append(executor.fanoutTables, nil)
	}

	queue := &executor.priorityQueues[queueIndex]

	// Process edge bindings
	for _, edgeBinding := range shard.edgeBindings {
		// Add to canonical cycle storage
		executor.cycleStates = append(executor.cycleStates, ArbitrageCycleState{
			pairIDs: edgeBinding.cyclePairs,
		})
		cycleIndex := CycleStateIndex(len(executor.cycleStates) - 1)

		// Allocate queue handle
		queueHandle, success := queue.BorrowSafe()
		if !success {
			continue // Skip if no handles available
		}

		// Initialize with maximum priority
		queue.Push(constants.MaxInitializationPriority, queueHandle, uint64(cycleIndex))

		// Create fanout entries for other edges in the cycle
		otherEdge1 := (edgeBinding.edgeIndex + 1) % 3
		otherEdge2 := (edgeBinding.edgeIndex + 2) % 3

		for _, edgeIdx := range [...]uint16{otherEdge1, otherEdge2} {
			executor.fanoutTables[queueIndex] = append(executor.fanoutTables[queueIndex],
				FanoutEntry{
					queueHandle:     queueHandle,
					edgeIndex:       edgeIdx,
					cycleStateIndex: cycleIndex,
					queue:           queue,
				})
		}
	}
}

// launchShardWorker initializes and runs core executor
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func launchShardWorker(coreID, forwardCoreCount int, shardInput <-chan PairShardBucket) {
	runtime.LockOSThread()

	shutdownChannel := make(chan struct{})

	// Initialize core executor
	executor := &ArbitrageCoreExecutor{
		isReverseDirection: coreID >= forwardCoreCount,
		shutdownSignal:     shutdownChannel,
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		cycleStates:        make([]ArbitrageCycleState, 0, 1024), // Pre-allocate capacity
	}
	coreExecutors[coreID] = executor
	coreRings[coreID] = ring24.New(constants.DefaultRingSize)

	// Process incoming shards
	for shard := range shardInput {
		attachShardToExecutor(executor, &shard)
	}

	// Get control flags
	stopFlag, hotFlag := control.Flags()

	// Launch appropriate consumer based on core ID
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

// InitializeArbitrageSystem bootstraps the complete arbitrage system
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func InitializeArbitrageSystem(arbitrageCycles []ArbitrageTriplet) {
	// Calculate optimal core count
	coreCount := runtime.NumCPU() - 1
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	if coreCount <= 0 {
		coreCount = 1
	}
	if coreCount&1 == 1 {
		coreCount-- // Ensure even number for forward/reverse pairing
	}
	if coreCount == 0 {
		coreCount = 2 // Minimum forward + reverse
	}
	forwardCoreCount := coreCount / 2

	// Build optimized shard buckets
	buildFanoutShardBuckets(arbitrageCycles)

	// Create worker channels
	shardChannels := make([]chan PairShardBucket, coreCount)
	for i := range shardChannels {
		shardChannels[i] = make(chan PairShardBucket, constants.ShardChannelBufferSize)
		go launchShardWorker(i, forwardCoreCount, shardChannels[i])
	}

	// Distribute shards across cores with load balancing
	currentCore := 0
	for _, shardBuckets := range pairShardBuckets {
		for _, shard := range shardBuckets {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			// Send to both forward and reverse cores
			shardChannels[forwardCore] <- shard
			shardChannels[reverseCore] <- shard

			// Update core routing assignments
			for _, edgeBinding := range shard.edgeBindings {
				for _, pairID := range edgeBinding.cyclePairs {
					RegisterPairToCore(pairID, uint8(forwardCore))
					RegisterPairToCore(pairID, uint8(reverseCore))
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