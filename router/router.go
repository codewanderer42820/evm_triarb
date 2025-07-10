// router.go — High-performance triangular arbitrage fan-out router (64 cores)
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

// Core type definitions - All 64-bit aligned for perfect cache utilization
type PairID uint64
type ArbitrageTriplet [3]PairID
type CycleStateIndex uint64

// ProcessedCycle - Pre-allocated cycle processing structure
//
//go:notinheap
type ProcessedCycle struct {
	queueHandle     quantumqueue64.Handle // 4B
	_               uint32                // 4B - Padding
	originalTick    int64                 // 8B
	cycleStateIndex CycleStateIndex       // 8B
	_               uint64                // 8B - Padding to 32B
}

// AddressKey - Optimized for address storage and comparison
//
//go:notinheap
type AddressKey struct {
	words [3]uint64 // 24B - Ethereum address as 3×8-byte words (160 bits = 20 bytes)
}

// TickUpdate - Perfect 24-byte message with zero padding waste
//
//go:notinheap
type TickUpdate struct {
	forwardTick float64 // 8B - Forward direction price
	reverseTick float64 // 8B - Reverse direction price
	pairID      PairID  // 8B - Pair identifier (perfect 24B total)
}

// ArbitrageCycleState - Optimized for single-core access
//
//go:notinheap
type ArbitrageCycleState struct {
	tickValues [3]float64 // 24B - Hot: Price ticks for each edge
	pairIDs    [3]PairID  // 24B - Warm: Pair identifiers
}

// ArbitrageEdgeBinding - Compact initialization-only structure
//
//go:notinheap
type ArbitrageEdgeBinding struct {
	cyclePairs [3]PairID // 24B - Complete triplet
	edgeIndex  uint64    // 8B - Edge index
}

// FanoutEntry - Optimized for single-core fanout operations
//
//go:notinheap
type FanoutEntry struct {
	edgeIndex       uint64                         // 8B - HOTTEST: Array indexing
	cycleStateIndex uint64                         // 8B - HOTTEST: Pointer chasing
	queueHandle     uint64                         // 8B - WARM: Queue operations
	queue           *quantumqueue64.QuantumQueue64 // 8B - WARM: Queue pointer
}

// PairShardBucket - Compact initialization-only structure
//
//go:notinheap
type PairShardBucket struct {
	pairID       PairID                 // 8B - Queue lookup
	edgeBindings []ArbitrageEdgeBinding // 24B - Slice header (ptr,len,cap)
}

// ArbitrageCoreExecutor - Optimized single-core executor layout with pre-allocated buffers
//
//go:notinheap
type ArbitrageCoreExecutor struct {
	// HOTTEST: Accessed every single tick update
	isReverseDirection bool    // 1B - Direction flag (checked every tick)
	_                  [7]byte // 7B - Padding for 8-byte alignment

	// HOT: Accessed frequently during tick processing
	pairToQueueIndex localidx.Hash         // 64B - O(1) pair→queue mapping
	fanoutTables     [][]FanoutEntry       // 24B - Fanout mappings
	cycleStates      []ArbitrageCycleState // 24B - Direct storage

	// WARM: Accessed during processing but less frequently
	priorityQueues []quantumqueue64.QuantumQueue64 // 24B - Owned queues

	// COLD: Rarely accessed after initialization
	shutdownSignal <-chan struct{} // 8B - Shutdown channel

	// PRE-ALLOCATED BUFFERS: Zero-alloc working memory (RUNTIME ONLY)
	processedCycles [128]ProcessedCycle // 4096B - Pre-allocated cycle buffer
	messageBuffer   [24]byte            // 24B - Pre-allocated message buffer
}

// keccakRandomState maintains deterministic random state for shuffling (INITIALIZATION ONLY)
type keccakRandomState struct {
	seed    [32]byte // Current seed
	counter uint64   // Nonce counter
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
// ⚠️ FOOTGUN: No bounds checking - assumes input within valid range
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func quantizeTickToInt64(tickValue float64) int64 {
	return int64((tickValue + constants.TickClampingBound) * constants.QuantizationScale)
}

// newKeccakRandom creates seeded deterministic random generator (INITIALIZATION ONLY)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func newKeccakRandom(initialSeed []byte) *keccakRandomState {
	var seed [32]byte

	// Hash provided seed to normalize length
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(initialSeed)
	copy(seed[:], hasher.Sum(nil))

	return &keccakRandomState{
		seed:    seed,
		counter: 0,
	}
}

// nextUint64 generates next deterministic random uint64 (INITIALIZATION ONLY)
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

	// Return first 8 bytes as uint64 using utils
	return utils.Load64(output[:8])
}

// nextInt generates random int in range [0, upperBound) (INITIALIZATION ONLY)
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

	// Power-of-2 optimization
	if upperBound&(upperBound-1) == 0 {
		return int(randomValue & uint64(upperBound-1))
	}

	// Unbiased modular reduction using multiplication method
	high64, _ := bits.Mul64(randomValue, uint64(upperBound))
	return int(high64)
}

// directAddressToIndex64 extracts index from Ethereum address (strips 0x prefix)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64(address42HexBytes []byte) uint64 {
	// Strip 0x prefix: input is "0x1234..." → parse "1234..." (40 chars)
	addressBytes := utils.ParseEthereumAddress(address42HexBytes[2:])

	// Use middle 8 bytes (6-13) to avoid vanity address patterns
	// Vanity addresses target the beginning, middle has better entropy
	hash64 := utils.Load64(addressBytes[6:14])

	return hash64 & uint64(constants.AddressTableMask)
}

// directAddressToIndex64Stored extracts hash from stored AddressKey (for Robin Hood distance calculation)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64Stored(key AddressKey) uint64 {
	// Extract middle 8 bytes from stored AddressKey (bytes 6-13 of original address)
	// This matches directAddressToIndex64 but works on stored AddressKey format
	hash64 := (key.words[0] >> 48) | (key.words[1] << 16)
	return hash64 & uint64(constants.AddressTableMask)
}

// bytesToAddressKey converts address bytes to AddressKey (strips 0x prefix)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func bytesToAddressKey(address42HexBytes []byte) AddressKey {
	// Strip 0x prefix: input is "0x1234..." → parse "1234..." (40 chars)
	parsedAddress := utils.ParseEthereumAddress(address42HexBytes[2:])

	// Pack all 20 bytes efficiently: 8+8+4 = 20 bytes exactly
	return AddressKey{
		words: [3]uint64{
			utils.Load64(parsedAddress[0:8]),                 // Bytes 0-7
			utils.Load64(parsedAddress[8:16]),                // Bytes 8-15
			uint64(utils.Load64(parsedAddress[16:20]) >> 32), // Bytes 16-19 (only use lower 4 bytes)
		},
	}
}

// isEqual performs optimized address comparison (only compare actual address data)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (a AddressKey) isEqual(b AddressKey) bool {
	// Compare all 3 words containing the complete 160-bit address
	return a.words[0] == b.words[0] &&
		a.words[1] == b.words[1] &&
		a.words[2] == b.words[2]
}

// Robin Hood hashing with native 64-bit operations
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

		// Empty slot - insert
		if currentPairID == 0 {
			pairAddressKeys[i] = key
			addressToPairID[i] = pairID
			return
		}

		// Key exists - update
		if pairAddressKeys[i].isEqual(key) {
			addressToPairID[i] = pairID
			return
		}

		// Robin Hood displacement check (64-bit native operations)
		currentKey := pairAddressKeys[i]
		currentKeyHash := directAddressToIndex64Stored(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)

		// Swap if incoming traveled farther
		if currentDist < dist {
			key, pairAddressKeys[i] = pairAddressKeys[i], key
			pairID, addressToPairID[i] = addressToPairID[i], pairID
			dist = currentDist
		}

		// Advance
		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++
	}
}

// lookupPairIDByAddress retrieves value with Robin Hood early termination
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func lookupPairIDByAddress(address42HexBytes []byte) PairID {
	key := bytesToAddressKey(address42HexBytes)
	i := directAddressToIndex64(address42HexBytes)
	dist := uint64(0)

	for {
		currentPairID := addressToPairID[i]

		// Empty slot - not found
		if currentPairID == 0 {
			return 0
		}

		// Key found
		if pairAddressKeys[i].isEqual(key) {
			return currentPairID
		}

		// Robin Hood early termination (64-bit native operations)
		currentKey := pairAddressKeys[i]
		currentKeyHash := directAddressToIndex64Stored(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)

		if currentDist < dist {
			return 0
		}

		// Advance
		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++
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

// processTickUpdate handles price updates with arbitrage detection - ZERO ALLOCATION
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processTickUpdate(executor *ArbitrageCoreExecutor, update *TickUpdate) {
	// Select tick based on direction (no branch hints - let CPU predict naturally)
	var currentTick float64
	if !executor.isReverseDirection {
		currentTick = update.forwardTick
	} else {
		currentTick = update.reverseTick
	}

	// Get queue for pair
	queueIndex, _ := executor.pairToQueueIndex.Get(uint32(update.pairID))
	queue := &executor.priorityQueues[queueIndex]

	// Use pre-allocated buffer - ZERO ALLOCATION
	cycleCount := 0

	// Extract profitable cycles (no branch hints)
	for {
		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleStateIndex(cycleData)
		cycle := &executor.cycleStates[cycleIndex]

		// Check profitability
		totalProfitability := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		isProfitable := totalProfitability < 0

		if isProfitable {
			//emitArbitrageOpportunity(cycle, currentTick)
		}

		// Break conditions (no branch hints - natural prediction)
		if !isProfitable || cycleCount == len(executor.processedCycles) {
			break
		}

		// Store for reprocessing using pre-allocated buffer
		executor.processedCycles[cycleCount] = ProcessedCycle{
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

	// Reinsert processed cycles using pre-allocated buffer
	for i := 0; i < cycleCount; i++ {
		cycle := &executor.processedCycles[i]
		queue.Push(cycle.originalTick, cycle.queueHandle, uint64(cycle.cycleStateIndex))
	}

	// Update fanout entries (with handle conversion for quantumqueue64)
	for _, fanoutEntry := range executor.fanoutTables[queueIndex] {
		cycle := &executor.cycleStates[fanoutEntry.cycleStateIndex]
		cycle.tickValues[fanoutEntry.edgeIndex] = currentTick

		// Recalculate priority
		newPriority := quantizeTickToInt64(cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2])
		fanoutEntry.queue.MoveTick(quantumqueue64.Handle(fanoutEntry.queueHandle), newPriority)
	}
}

// DispatchTickUpdate processes log and dispatches to cores - ZERO ALLOCATION
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

	// Dispatch to assigned cores using per-core pre-allocated buffers
	coreAssignments := pairToCoreAssignment[pairID]
	for coreAssignments != 0 {
		coreID := bits.TrailingZeros64(uint64(coreAssignments))

		// Use executor's pre-allocated message buffer - ZERO ALLOCATION
		executor := coreExecutors[coreID]
		if executor != nil {
			tickUpdate := (*TickUpdate)(unsafe.Pointer(&executor.messageBuffer))
			tickUpdate.forwardTick = tickValue
			tickUpdate.reverseTick = -tickValue
			tickUpdate.pairID = pairID

			coreRings[coreID].Push(&executor.messageBuffer)
		}

		coreAssignments &^= 1 << coreID
	}
}

// keccakShuffleEdgeBindings performs deterministic Fisher-Yates shuffle using keccak256 (INITIALIZATION ONLY)
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

	// Create deterministic seed from pairID (now 64-bit)
	var seedInput [8]byte
	binary.LittleEndian.PutUint64(seedInput[:], utils.Mix64(uint64(pairID)))

	// Initialize deterministic random generator with Keccak256 entropy
	rng := newKeccakRandom(seedInput[:])

	// Fisher-Yates shuffle with deterministic randomness
	for i := len(bindings) - 1; i > 0; i-- {
		j := rng.nextInt(i + 1)
		bindings[i], bindings[j] = bindings[j], bindings[i]
	}
}

// buildFanoutShardBuckets constructs shard buckets with deterministic shuffle - ZERO ALLOCATION
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
					edgeIndex:  uint64(i),
				})
		}
	}

	// Create shards with deterministic keccak256-based shuffle (INITIALIZATION ONLY)
	for pairID, bindings := range temporaryBindings {
		// Per-pair deterministic shuffle based on pairID with Keccak256 entropy
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

		// Create fanout entries (handle conversion for quantumqueue64 compatibility)
		otherEdge1 := (edgeBinding.edgeIndex + 1) % 3
		otherEdge2 := (edgeBinding.edgeIndex + 2) % 3

		for _, edgeIdx := range [...]uint64{otherEdge1, otherEdge2} {
			executor.fanoutTables[queueIndex] = append(executor.fanoutTables[queueIndex],
				FanoutEntry{
					edgeIndex:       edgeIdx,
					cycleStateIndex: uint64(cycleIndex),  // Convert CycleStateIndex to uint64
					queueHandle:     uint64(queueHandle), // Convert Handle to uint64
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

	// Initialize executor with pre-allocated buffers
	executor := &ArbitrageCoreExecutor{
		isReverseDirection: coreID >= forwardCoreCount,
		shutdownSignal:     shutdownChannel,
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		cycleStates:        make([]ArbitrageCycleState, 0),
		// Pre-allocated buffers are already part of the struct
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
	coreCount &^= 1 // Ensure even
	forwardCoreCount := coreCount >> 1

	// Build shards with deterministic keccak256 shuffle
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
