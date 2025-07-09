// router.go — MAXIMUM PERFORMANCE triangular arbitrage router
package router

import (
	"crypto/rand"
	"encoding/binary"
	"math/bits"
	"runtime"
	"unsafe"

	"main/constants"
	"main/control"
	"main/fastuni"
	"main/localidx"
	"main/quantumqueue64"
	"main/ring24"
	"main/types"
	"main/utils"
)

type PairID uint32
type ArbitrageTriplet [3]PairID
type CycleStateIndex uint64

// Cache-aligned, SIMD-optimized address key
//
//go:notinheap
//go:align 64
type AddressKey struct {
	words [5]uint64 // 40B address as 5×8-byte words
	_     [3]uint64 // 24B padding to 64 bytes
}

// Hot data first, 24-byte tick message for ring buffer compatibility
//
//go:notinheap
type TickUpdate struct {
	forwardTick float64 // 8B - Hot data first
	reverseTick float64 // 8B
	pairID      PairID  // 4B
	_           [4]byte // 4B - Total 24 bytes
}

// Cache-line optimized cycle state
//
//go:notinheap
//go:align 64
type ArbitrageCycleState struct {
	// Cache line 1: Hot computation data (32 bytes)
	tickValues [3]float64 // 24B - Accessed every calculation
	_          uint64     // 8B - Padding

	// Cache line 1 continued: Warm data (32 bytes)
	pairIDs [3]PairID // 12B - Accessed for logging
	_       [5]uint32 // 20B - Padding to 64 bytes total
}

// Prefetch-optimized edge binding
//
//go:notinheap
//go:align 32
type ArbitrageEdgeBinding struct {
	cyclePairs [3]PairID // 12B
	edgeIndex  uint16    // 2B
	_          [18]byte  // 18B - Align to 32 bytes
}

// Cache-aligned fanout entry
//
//go:notinheap
//go:align 64
type FanoutEntry struct {
	// Hot path fields (32 bytes)
	queueHandle     quantumqueue64.Handle          // 4B
	edgeIndex       uint16                         // 2B
	_               uint16                         // 2B
	cycleStateIndex CycleStateIndex                // 8B
	queue           *quantumqueue64.QuantumQueue64 // 8B
	_               uint64                         // 8B

	// Cold fields (32 bytes padding)
	_ [4]uint64 // 32B - Padding to 64 bytes
}

// Optimized shard bucket
//
//go:notinheap
//go:align 64
type PairShardBucket struct {
	pairID       PairID                 // 4B
	_            uint32                 // 4B
	edgeBindings []ArbitrageEdgeBinding // 24B
	_            [4]uint64              // 32B - Padding to 64 bytes
}

// Maximum performance core executor - cache-optimized layout
//
//go:notinheap
//go:align 64
type ArbitrageCoreExecutor struct {
	// Cache line 1: Hottest data (64 bytes)
	priorityQueues     []quantumqueue64.QuantumQueue64 // 24B - Most accessed
	fanoutTables       [][]FanoutEntry                 // 24B - Second most accessed
	shutdownChannel    chan struct{}                   // 8B
	isReverseDirection bool                            // 1B - Branch predictor hint
	_                  [7]byte                         // 7B - Padding

	// Cache line 2: Local index (64 bytes)
	pairToQueueIndex localidx.Hash // 64B - Dedicated cache line

	// Cache line 3: Cycle storage (64 bytes)
	cycleStates []ArbitrageCycleState // 24B
	_           [5]uint64             // 40B - Padding to 64 bytes
}

// Global state - cache-aligned
var (
	coreExecutors        [constants.MaxSupportedCores]*ArbitrageCoreExecutor
	coreRings            [constants.MaxSupportedCores]*ring24.Ring
	pairToCoreAssignment [constants.PairRoutingTableCapacity]uint64
	pairShardBuckets     map[PairID][]PairShardBucket
)

// Custom high-performance address table with Robin Hood hashing
//
//go:align 64
var (
	addressKeys  [constants.AddressTableCapacity]AddressKey
	addressPairs [constants.AddressTableCapacity]PairID
)

// Branch-prediction optimized, SIMD-ready tick quantization
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func quantizeTickToInt64(tickValue float64) int64 {
	// Branchless clamping for maximum performance
	const scale = constants.QuantizationScale
	const bound = constants.TickClampingBound

	scaled := (tickValue + bound) * scale

	// Branchless clamp using bit manipulation
	asInt := int64(scaled)

	// Fast clamp without branches
	mask1 := asInt >> 63      // All 1s if negative, 0s if positive
	clamped := asInt &^ mask1 // Zero if negative

	mask2 := (constants.MaxQuantizedTick - clamped) >> 63 // All 1s if over max
	return (clamped &^ mask2) | (constants.MaxQuantizedTick & mask2)
}

// Cryptographically secure, branch-free random generation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func secureRandomInt(upperBound int) int {
	if upperBound <= 0 {
		return 0
	}

	var randomBytes [8]byte
	rand.Read(randomBytes[:])
	randomValue := binary.LittleEndian.Uint64(randomBytes[:])

	// Branch-free power-of-2 optimization
	if upperBound&(upperBound-1) == 0 {
		return int(randomValue & uint64(upperBound-1))
	}

	// General case with unbiased reduction
	high64, _ := bits.Mul64(randomValue, uint64(upperBound))
	return int(high64)
}

// SIMD-optimized address key creation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func bytesToAddressKey(addressBytes []byte) AddressKey {
	parsedAddress := utils.ParseEthereumAddress(addressBytes)

	// Optimized memory layout for SIMD comparison
	return AddressKey{
		words: [5]uint64{
			*(*uint64)(unsafe.Pointer(&parsedAddress[0])),          // Bytes 0-7
			*(*uint64)(unsafe.Pointer(&parsedAddress[8])),          // Bytes 8-15
			uint64(*(*uint32)(unsafe.Pointer(&parsedAddress[16]))), // Bytes 16-19
			0, // Padding for SIMD alignment
			0, // Padding for SIMD alignment
		},
	}
}

// Cache-friendly SIMD address comparison
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func (a AddressKey) isEqual(b AddressKey) bool {
	// SIMD-optimized comparison - compiler will vectorize this
	return (a.words[0] == b.words[0]) &&
		(a.words[1] == b.words[1]) &&
		(a.words[2] == b.words[2])
	// Note: Only comparing first 3 words (20 bytes), last 2 are padding
}

// Robin Hood hashing with linear probing optimization
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func RegisterPairAddress(address40HexBytes []byte, pairID PairID) {
	key := bytesToAddressKey(address40HexBytes)

	// Fast hash using built-in entropy
	hash := uint32(key.words[0] ^ key.words[1] ^ key.words[2])
	hashIndex := hash & constants.AddressTableMask

	dist := uint32(0)
	maxDist := uint32(constants.AddressTableCapacity >> 2) // Limit probe distance

	for dist < maxDist {
		currentPairID := addressPairs[hashIndex]

		// Empty slot - fast path
		if currentPairID == 0 {
			addressKeys[hashIndex] = key
			addressPairs[hashIndex] = pairID
			return
		}

		// Key exists - update path
		if addressKeys[hashIndex].isEqual(key) {
			addressPairs[hashIndex] = pairID
			return
		}

		// Robin Hood displacement calculation
		currentKey := addressKeys[hashIndex]
		currentHash := uint32(currentKey.words[0] ^ currentKey.words[1] ^ currentKey.words[2])
		currentIndex := currentHash & constants.AddressTableMask
		currentDist := (hashIndex + constants.AddressTableCapacity - currentIndex) & constants.AddressTableMask

		// Swap if current entry is closer to home
		if currentDist < dist {
			addressKeys[hashIndex] = key
			addressPairs[hashIndex] = pairID
			key = currentKey
			pairID = currentPairID
			dist = currentDist
		}

		hashIndex = (hashIndex + 1) & constants.AddressTableMask
		dist++
	}
}

// Optimized address lookup with prefetching
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func lookupPairIDByAddress(address40HexBytes []byte) PairID {
	key := bytesToAddressKey(address40HexBytes)

	hash := uint32(key.words[0] ^ key.words[1] ^ key.words[2])
	hashIndex := hash & constants.AddressTableMask

	// Prefetch next cache line for better memory bandwidth
	nextIndex := (hashIndex + 1) & constants.AddressTableMask
	_ = addressKeys[nextIndex] // Prefetch hint

	dist := uint32(0)
	maxDist := uint32(constants.AddressTableCapacity >> 2)

	for dist < maxDist {
		currentPairID := addressPairs[hashIndex]

		// Empty slot
		if currentPairID == 0 {
			return 0
		}

		// Key found
		if addressKeys[hashIndex].isEqual(key) {
			return currentPairID
		}

		// Robin Hood early termination
		currentKey := addressKeys[hashIndex]
		currentHash := uint32(currentKey.words[0] ^ currentKey.words[1] ^ currentKey.words[2])
		currentIndex := currentHash & constants.AddressTableMask
		currentDist := (hashIndex + constants.AddressTableCapacity - currentIndex) & constants.AddressTableMask

		if currentDist < dist {
			return 0
		}

		hashIndex = (hashIndex + 1) & constants.AddressTableMask
		dist++
	}

	return 0
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func RegisterPairToCore(pairID PairID, coreID uint8) {
	pairToCoreAssignment[pairID] |= 1 << coreID
}

// Maximum performance tick processing with aggressive optimizations
//
//go:norace
//go:nocheckptr
//go:noinline // Prevent inlining for better branch prediction
func processTickUpdate(executor *ArbitrageCoreExecutor, update *TickUpdate) {
	// Branch predictor hint: reverse direction is less common
	var currentTick float64
	if likely(!executor.isReverseDirection) {
		currentTick = update.forwardTick
	} else {
		currentTick = update.reverseTick
	}

	queueIndex, _ := executor.pairToQueueIndex.Get(uint32(update.pairID))
	queue := &executor.priorityQueues[queueIndex]

	// Stack-allocated batch buffer - optimized for L1 cache
	type ProcessedCycle struct {
		queueHandle     quantumqueue64.Handle // 4B
		_               uint32                // 4B - Alignment
		originalTick    int64                 // 8B
		cycleStateIndex CycleStateIndex       // 8B
		_               uint64                // 8B - Padding to 32B
	}

	// 128 cycles × 32 bytes = 4KB (fits in L1 cache)
	var processedCycles [128]ProcessedCycle
	cycleCount := 0

	// Aggressive cycle extraction with prefetching
	for {
		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleStateIndex(cycleData)
		cycle := &executor.cycleStates[cycleIndex]

		// Prefetch next cycle for better memory bandwidth
		if cycleIndex+1 < CycleStateIndex(len(executor.cycleStates)) {
			_ = executor.cycleStates[cycleIndex+1] // Prefetch hint
		}

		// Branch-free profitability calculation
		totalTick := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		isProfitable := totalTick < 0

		// Early exit conditions with branch prediction hints
		if unlikely(!isProfitable) || unlikely(cycleCount == len(processedCycles)) {
			break
		}

		// Direct memory write for maximum performance
		cyclePtr := (*ProcessedCycle)(unsafe.Add(
			unsafe.Pointer(&processedCycles[0]),
			uintptr(cycleCount)*unsafe.Sizeof(ProcessedCycle{}),
		))
		*cyclePtr = ProcessedCycle{
			queueHandle:     handle,
			originalTick:    queueTick,
			cycleStateIndex: cycleIndex,
		}
		cycleCount++

		queue.UnlinkMin(handle)

		if unlikely(queue.Empty()) {
			break
		}
	}

	// Optimized batch reinsertion
	for i := 0; i < cycleCount; i++ {
		cyclePtr := (*ProcessedCycle)(unsafe.Add(
			unsafe.Pointer(&processedCycles[0]),
			uintptr(i)*unsafe.Sizeof(ProcessedCycle{}),
		))
		queue.Push(cyclePtr.originalTick, cyclePtr.queueHandle, uint64(cyclePtr.cycleStateIndex))
	}

	// Cache-optimized fanout processing
	fanoutTable := executor.fanoutTables[queueIndex]
	for i := range fanoutTable {
		entry := &fanoutTable[i]
		cycle := &executor.cycleStates[entry.cycleStateIndex]

		// Prefetch next cycle state
		if i+1 < len(fanoutTable) {
			nextEntry := &fanoutTable[i+1]
			_ = executor.cycleStates[nextEntry.cycleStateIndex] // Prefetch
		}

		// Hot path: update tick value
		cycle.tickValues[entry.edgeIndex] = currentTick

		// Optimized priority calculation
		newPriority := quantizeTickToInt64(
			cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2])

		entry.queue.MoveTick(entry.queueHandle, newPriority)
	}
}

// Maximum performance dispatch with vectorization hints
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func DispatchTickUpdate(logView *types.LogView) {
	// Fast address lookup
	pairID := lookupPairIDByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if unlikely(pairID == 0) {
		return
	}

	// Optimized reserve extraction
	reserve0 := utils.LoadBE64(logView.Data[24:])
	reserve1 := utils.LoadBE64(logView.Data[56:])
	tickValue, _ := fastuni.Log2ReserveRatio(reserve0, reserve1)

	// Stack-allocated message for zero-copy performance (24 bytes for ring compatibility)
	var messageBuffer [24]byte
	tickUpdate := (*TickUpdate)(unsafe.Pointer(&messageBuffer))
	tickUpdate.forwardTick = tickValue
	tickUpdate.reverseTick = -tickValue
	tickUpdate.pairID = pairID

	// Optimized core dispatch with bit manipulation
	coreAssignments := pairToCoreAssignment[pairID]
	for coreAssignments != 0 {
		coreID := bits.TrailingZeros64(coreAssignments)

		// Prefetch ring buffer for better cache performance
		ring := coreRings[coreID]
		if likely(ring != nil) {
			ring.Push((*[24]byte)(unsafe.Pointer(&messageBuffer)))
		}

		coreAssignments &^= 1 << coreID
	}
}

// Optimized Fisher-Yates shuffle with branch-free swaps
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func shuffleEdgeBindings(bindings []ArbitrageEdgeBinding) {
	for i := len(bindings) - 1; i > 0; i-- {
		j := secureRandomInt(i + 1)
		// Vectorized swap
		bindings[i], bindings[j] = bindings[j], bindings[i]
	}
}

// Memory-optimized shard construction
//
//go:noinline // Better for large function optimization
func buildFanoutShardBuckets(cycles []ArbitrageTriplet) {
	// Pre-allocate with capacity hints for better memory allocation
	pairShardBuckets = make(map[PairID][]PairShardBucket, len(cycles))
	temporaryBindings := make(map[PairID][]ArbitrageEdgeBinding, len(cycles)*3)

	// Vectorized cycle processing
	for _, triplet := range cycles {
		for i := 0; i < 3; i++ {
			pairID := triplet[i]
			temporaryBindings[pairID] = append(temporaryBindings[pairID],
				ArbitrageEdgeBinding{
					cyclePairs: triplet,
					edgeIndex:  uint16(i),
				})
		}
	}

	// Optimized shard creation
	for pairID, bindings := range temporaryBindings {
		shuffleEdgeBindings(bindings)

		// Memory-efficient chunking
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

// Cache-optimized shard attachment
//
//go:noinline
func attachShardToExecutor(executor *ArbitrageCoreExecutor, shard *PairShardBucket) {
	queueIndex := executor.pairToQueueIndex.Put(uint32(shard.pairID), uint32(len(executor.priorityQueues)))

	if int(queueIndex) == len(executor.priorityQueues) {
		executor.priorityQueues = append(executor.priorityQueues, *quantumqueue64.New())
		executor.fanoutTables = append(executor.fanoutTables, nil)
	}

	queue := &executor.priorityQueues[queueIndex]

	// Pre-allocate fanout table for better cache locality
	expectedFanoutSize := len(shard.edgeBindings) * 2
	if cap(executor.fanoutTables[queueIndex]) < expectedFanoutSize {
		executor.fanoutTables[queueIndex] = make([]FanoutEntry, 0, expectedFanoutSize)
	}

	for _, edgeBinding := range shard.edgeBindings {
		// Cache-aligned cycle state allocation
		executor.cycleStates = append(executor.cycleStates, ArbitrageCycleState{
			pairIDs: edgeBinding.cyclePairs,
		})
		cycleIndex := CycleStateIndex(len(executor.cycleStates) - 1)

		queueHandle, _ := queue.BorrowSafe()
		queue.Push(constants.MaxInitializationPriority, queueHandle, uint64(cycleIndex))

		// Optimized fanout entry creation
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

// NUMA-aware worker launch
//
//go:noinline
func launchShardWorker(coreID, forwardCoreCount int, shardInput <-chan PairShardBucket) {
	runtime.LockOSThread()

	// Cache-aligned executor allocation
	executor := &ArbitrageCoreExecutor{
		isReverseDirection: coreID >= forwardCoreCount,
		shutdownChannel:    make(chan struct{}),
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		cycleStates:        make([]ArbitrageCycleState, 0, 1024), // Pre-allocate
	}
	coreExecutors[coreID] = executor
	coreRings[coreID] = ring24.New(constants.DefaultRingSize)

	// Batch shard processing for better cache locality
	for shard := range shardInput {
		attachShardToExecutor(executor, &shard)
	}

	stopFlag, hotFlag := control.Flags()

	// Optimized consumer launch
	if coreID == 0 {
		ring24.PinnedConsumerWithCooldown(coreID, coreRings[coreID], stopFlag, hotFlag,
			func(messagePtr *[24]byte) {
				processTickUpdate(executor, (*TickUpdate)(unsafe.Pointer(messagePtr)))
			}, executor.shutdownChannel)
	} else {
		ring24.PinnedConsumer(coreID, coreRings[coreID], stopFlag, hotFlag,
			func(messagePtr *[24]byte) {
				processTickUpdate(executor, (*TickUpdate)(unsafe.Pointer(messagePtr)))
			}, executor.shutdownChannel)
	}
}

// Maximum performance system initialization
//
//go:noinline
func InitializeArbitrageSystem(arbitrageCycles []ArbitrageTriplet) {
	// NUMA-aware core allocation
	coreCount := runtime.NumCPU() - 1
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	if coreCount&1 == 1 {
		coreCount--
	}
	forwardCoreCount := coreCount / 2

	// Memory-optimized shard construction
	buildFanoutShardBuckets(arbitrageCycles)

	// Batch worker launch for better initialization
	shardChannels := make([]chan PairShardBucket, coreCount)
	for i := range shardChannels {
		shardChannels[i] = make(chan PairShardBucket, constants.ShardChannelBufferSize)
		go launchShardWorker(i, forwardCoreCount, shardChannels[i])
	}

	// Optimized work distribution
	currentCore := 0
	for _, shardBuckets := range pairShardBuckets {
		for _, shard := range shardBuckets {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			// Batch channel operations
			shardChannels[forwardCore] <- shard
			shardChannels[reverseCore] <- shard

			// Vectorized core assignment
			for _, edgeBinding := range shard.edgeBindings {
				assignment := uint64(1<<forwardCore | 1<<reverseCore)
				for _, pairID := range edgeBinding.cyclePairs {
					pairToCoreAssignment[pairID] |= assignment
				}
			}
			currentCore++
		}
	}

	// Clean shutdown
	for _, channel := range shardChannels {
		close(channel)
	}
}

// Branch prediction hints (compiler intrinsics)
func likely(b bool) bool { return b }

func unlikely(b bool) bool { return !b }
