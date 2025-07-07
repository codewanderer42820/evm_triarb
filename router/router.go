// router.go — High-performance triangular arbitrage fan-out router (64 cores)
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

// AddressKey - 64-byte aligned for zero-copy address comparison
//
//go:notinheap
//go:align 64
type AddressKey struct {
	words [5]uint64 // 40B Ethereum address as 5×8-byte words
	_     [3]uint64 // 24B padding to 64 bytes
}

// TickUpdate - Price tick message (24 bytes for ring buffer)
//
//go:notinheap
type TickUpdate struct {
	forwardTick float64 // 8B - Forward direction price
	reverseTick float64 // 8B - Reverse direction price
	pairID      PairID  // 4B - Pair identifier
	_           uint32  // 4B - Padding to 24 bytes
}

// ArbitrageCycleState - Cycle state with hot data first
//
//go:notinheap
//go:align 64
type ArbitrageCycleState struct {
	// Hot fields (24 bytes) - accessed every calculation
	tickValues [3]float64 // 24B - Price ticks for each edge

	// Warm fields (16 bytes)
	pairIDs [3]PairID // 12B - Pair identifiers
	_       uint32    // 4B - Padding to 16-byte boundary

	// Padding to 64 bytes
	_ [3]uint64 // 24B - Cache line padding
}

// ArbitrageEdgeBinding - Edge within arbitrage cycle
//
//go:notinheap
//go:align 16
type ArbitrageEdgeBinding struct {
	cyclePairs [3]PairID // 12B - Complete triplet
	edgeIndex  uint16    // 2B - Edge index (0-2)
	_          uint16    // 2B - Padding to 16 bytes
}

// FanoutEntry - Edge reference in fanout table
//
//go:notinheap
//go:align 32
type FanoutEntry struct {
	// Hot fields (8 bytes)
	queueHandle quantumqueue64.Handle // 4B - Used in MoveTick
	edgeIndex   uint16                // 2B - Tick selection
	_           uint16                // 2B - Padding

	// Warm fields (8 bytes)
	cycleStateIndex CycleStateIndex // 8B - State lookup

	// Cold fields (16 bytes)
	queue *quantumqueue64.QuantumQueue64 // 8B - Queue pointer
	_     uint64                         // 8B - Padding to 32 bytes
}

// PairShardBucket - Cycle grouping by common pair
//
//go:notinheap
//go:align 32
type PairShardBucket struct {
	// Hot fields (8 bytes)
	pairID PairID // 4B - Queue lookup
	_      uint32 // 4B - Padding

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
	_           [5]uint64             // 40B - Padding to 64 bytes
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

// directAddressToIndex64 extracts index from Ethereum address
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64(address40Bytes []byte) uint32 {
	hash64 := binary.LittleEndian.Uint64(address40Bytes[0:8])
	return uint32(hash64) & constants.AddressTableMask
}

// bytesToAddressKey converts address bytes to AddressKey
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func bytesToAddressKey(addressBytes []byte) AddressKey {
	return AddressKey{
		words: [5]uint64{
			binary.LittleEndian.Uint64(addressBytes[0:8]),
			binary.LittleEndian.Uint64(addressBytes[8:16]),
			binary.LittleEndian.Uint64(addressBytes[16:24]),
			binary.LittleEndian.Uint64(addressBytes[24:32]),
			binary.LittleEndian.Uint64(addressBytes[32:40]),
		},
	}
}

// isEqual performs SIMD-optimized address comparison
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (a AddressKey) isEqual(b AddressKey) bool {
	return a.words == b.words
}

// RegisterPairAddress inserts address→pair mapping with Robin Hood
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairAddress(address40Bytes []byte, pairID PairID) {
	key := bytesToAddressKey(address40Bytes)
	hashIndex := directAddressToIndex64(address40Bytes)
	dist := uint32(0)

	for {
		currentPairID := addressToPairID[hashIndex]

		// Empty slot
		if currentPairID == 0 {
			pairAddressKeys[hashIndex] = key
			addressToPairID[hashIndex] = pairID
			return
		}

		// Key exists
		if pairAddressKeys[hashIndex].isEqual(key) {
			addressToPairID[hashIndex] = pairID
			return
		}

		// Robin Hood displacement
		currentKey := pairAddressKeys[hashIndex]
		currentKeyIndex := directAddressToIndex64((*[40]byte)(unsafe.Pointer(&currentKey.words[0]))[:])
		currentDist := (hashIndex + constants.AddressTableCapacity - currentKeyIndex) & constants.AddressTableMask

		if currentDist < dist {
			// Swap with resident
			pairAddressKeys[hashIndex] = key
			addressToPairID[hashIndex] = pairID
			key = currentKey
			pairID = currentPairID
			dist = currentDist
		}

		hashIndex = (hashIndex + 1) & constants.AddressTableMask
		dist++

		if dist > constants.AddressTableCapacity {
			panic("Robin Hood hash table full")
		}
	}
}

// lookupPairIDByAddress performs O(1) address lookup
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func lookupPairIDByAddress(address40Bytes []byte) PairID {
	key := bytesToAddressKey(address40Bytes)
	hashIndex := directAddressToIndex64(address40Bytes)
	dist := uint32(0)

	for {
		currentPairID := addressToPairID[hashIndex]

		// Empty slot
		if currentPairID == 0 {
			return 0
		}

		// Key found
		if pairAddressKeys[hashIndex].isEqual(key) {
			return currentPairID
		}

		// Robin Hood early termination
		currentKey := pairAddressKeys[hashIndex]
		currentKeyIndex := directAddressToIndex64((*[40]byte)(unsafe.Pointer(&currentKey.words[0]))[:])
		currentDist := (hashIndex + constants.AddressTableCapacity - currentKeyIndex) & constants.AddressTableMask

		if currentDist < dist {
			return 0
		}

		hashIndex = (hashIndex + 1) & constants.AddressTableMask
		dist++

		if dist > constants.AddressTableCapacity {
			return 0
		}
	}
}

// RegisterPairToCore assigns pair to specific core
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairToCore(pairID PairID, coreID uint8) {
	pairToCoreAssignment[pairID] |= 1 << coreID
}

// emitArbitrageOpportunity logs profitable arbitrage
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

	// Log tick values
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
	queueIndex, _ := executor.pairToQueueIndex.Get(uint32(update.pairID))
	queue := &executor.priorityQueues[queueIndex]

	// Process profitable cycles
	type ProcessedCycle struct {
		queueHandle     quantumqueue64.Handle // 4B
		_               uint32                // 4B - Padding
		originalTick    int64                 // 8B
		cycleStateIndex CycleStateIndex       // 8B
		_               uint64                // 8B - Padding to 32B
	}

	var processedCycles [128]ProcessedCycle
	cycleCount := 0

	// Extract profitable cycles
	for {
		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleStateIndex(cycleData)
		cycle := &executor.cycleStates[cycleIndex]

		// Check profitability
		totalProfitability := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		isProfitable := totalProfitability < 0

		if isProfitable {
			emitArbitrageOpportunity(cycle, currentTick)
		}

		if !isProfitable || cycleCount == len(processedCycles) {
			break
		}

		// Store for reprocessing
		*(*ProcessedCycle)(unsafe.Add(
			unsafe.Pointer(&processedCycles[0]),
			uintptr(cycleCount)*unsafe.Sizeof(ProcessedCycle{}),
		)) = ProcessedCycle{
			queueHandle:     handle,
			originalTick:    queueTick,
			cycleStateIndex: cycleIndex,
		}
		cycleCount++

		queue.UnlinkMin(handle)

		if queue.Empty() {
			break
		}
	}

	// Reinsert processed cycles
	for i := 0; i < cycleCount; i++ {
		cycle := (*ProcessedCycle)(unsafe.Add(
			unsafe.Pointer(&processedCycles[0]),
			uintptr(i)*unsafe.Sizeof(ProcessedCycle{}),
		))
		queue.Push(cycle.originalTick, cycle.queueHandle, uint64(cycle.cycleStateIndex))
	}

	// Update fanout entries
	for _, fanoutEntry := range executor.fanoutTables[queueIndex] {
		cycle := &executor.cycleStates[fanoutEntry.cycleStateIndex]
		cycle.tickValues[fanoutEntry.edgeIndex] = currentTick

		// Recalculate priority
		newPriority := quantizeTickToInt64(cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2])
		fanoutEntry.queue.MoveTick(fanoutEntry.queueHandle, newPriority)
	}
}

// DispatchTickUpdate processes log and dispatches to cores
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DispatchTickUpdate(logView *types.LogView) {
	// Resolve address to pair ID
	pairID := lookupPairIDByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if pairID == 0 {
		return
	}

	// Extract reserves
	reserve0 := utils.LoadBE64(logView.Data[24:])
	reserve1 := utils.LoadBE64(logView.Data[56:])

	// Calculate tick
	tickValue, _ := fastuni.Log2ReserveRatio(reserve0, reserve1)

	// Prepare message
	var messageBuffer [24]byte
	tickUpdate := (*TickUpdate)(unsafe.Pointer(&messageBuffer))
	tickUpdate.forwardTick = tickValue
	tickUpdate.reverseTick = -tickValue
	tickUpdate.pairID = pairID

	// Dispatch to assigned cores
	coreAssignments := pairToCoreAssignment[pairID]
	for coreAssignments != 0 {
		coreID := bits.TrailingZeros64(uint64(coreAssignments))
		coreRings[coreID].Push(&messageBuffer)
		coreAssignments &^= 1 << coreID
	}
}

// shuffleEdgeBindings performs Fisher-Yates shuffle
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

// buildFanoutShardBuckets constructs shard buckets
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func buildFanoutShardBuckets(cycles []ArbitrageTriplet) {
	pairShardBuckets = make(map[PairID][]PairShardBucket)
	temporaryBindings := make(map[PairID][]ArbitrageEdgeBinding, len(cycles)*3)

	// Group by pairs
	for _, triplet := range cycles {
		for i := 0; i < 3; i++ {
			temporaryBindings[triplet[i]] = append(temporaryBindings[triplet[i]],
				ArbitrageEdgeBinding{
					cyclePairs: triplet,
					edgeIndex:  uint16(i),
				})
		}
	}

	// Create shards
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

// attachShardToExecutor integrates shard into executor
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func attachShardToExecutor(executor *ArbitrageCoreExecutor, shard *PairShardBucket) {
	// Get/create queue index
	queueIndex := executor.pairToQueueIndex.Put(uint32(shard.pairID), uint32(len(executor.priorityQueues)))

	if int(queueIndex) == len(executor.priorityQueues) {
		executor.priorityQueues = append(executor.priorityQueues, *quantumqueue64.New())
		executor.fanoutTables = append(executor.fanoutTables, nil)
	}

	queue := &executor.priorityQueues[queueIndex]

	// Process cycles
	for _, edgeBinding := range shard.edgeBindings {
		// Add to canonical storage
		executor.cycleStates = append(executor.cycleStates, ArbitrageCycleState{
			pairIDs: edgeBinding.cyclePairs,
		})
		cycleIndex := CycleStateIndex(len(executor.cycleStates) - 1)

		// Allocate queue handle
		queueHandle, _ := queue.BorrowSafe()
		queue.Push(constants.MaxInitializationPriority, queueHandle, uint64(cycleIndex))

		// Create fanout entries
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

// launchShardWorker initializes core executor
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func launchShardWorker(coreID, forwardCoreCount int, shardInput <-chan PairShardBucket) {
	runtime.LockOSThread()

	shutdownChannel := make(chan struct{})

	// Initialize executor
	executor := &ArbitrageCoreExecutor{
		isReverseDirection: coreID >= forwardCoreCount,
		shutdownSignal:     shutdownChannel,
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		cycleStates:        make([]ArbitrageCycleState, 0),
	}
	coreExecutors[coreID] = executor
	coreRings[coreID] = ring24.New(constants.DefaultRingSize)

	// Process shards
	for shard := range shardInput {
		attachShardToExecutor(executor, &shard)
	}

	stopFlag, hotFlag := control.Flags()

	// Launch consumer
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

// InitializeArbitrageSystem bootstraps the complete system
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func InitializeArbitrageSystem(arbitrageCycles []ArbitrageTriplet) {
	coreCount := runtime.NumCPU() - 1
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	if coreCount&1 == 1 {
		coreCount-- // Ensure even
	}
	forwardCoreCount := coreCount / 2

	// Build shards
	buildFanoutShardBuckets(arbitrageCycles)

	// Create workers
	shardChannels := make([]chan PairShardBucket, coreCount)
	for i := range shardChannels {
		shardChannels[i] = make(chan PairShardBucket, constants.ShardChannelBufferSize)
		go launchShardWorker(i, forwardCoreCount, shardChannels[i])
	}

	// Distribute shards
	currentCore := 0
	for _, shardBuckets := range pairShardBuckets {
		for _, shard := range shardBuckets {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			// Send to both directions
			shardChannels[forwardCore] <- shard
			shardChannels[reverseCore] <- shard

			// Update routing
			for _, edgeBinding := range shard.edgeBindings {
				for _, pairID := range edgeBinding.cyclePairs {
					pairToCoreAssignment[pairID] |= 1<<forwardCore | 1<<reverseCore
				}
			}
			currentCore++
		}
	}

	// Close channels
	for _, channel := range shardChannels {
		close(channel)
	}
}
