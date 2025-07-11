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

// Core types
type PairID uint64
type ArbitrageTriplet [3]PairID
type CycleStateIndex uint64

// TickUpdate - Message passing structure (MUST STAY 24B EXACT)
//
//go:notinheap
type TickUpdate struct {
	forwardTick float64 // 8B - HOT: Forward price
	reverseTick float64 // 8B - HOT: Reverse price
	pairID      PairID  // 8B - HOT: Pair identifier
}

// ArbitrageCycleState - Hot processing structure
//
//go:notinheap
//go:align 64
type ArbitrageCycleState struct {
	tickValues [3]float64 // 24B - HOTTEST: Price ticks
	pairIDs    [3]PairID  // 24B - WARM: Pair identifiers
	_          [16]byte   // 16B - Padding to 64B
}

// FanoutEntry - Core processing structure
//
//go:notinheap
//go:align 32
type FanoutEntry struct {
	edgeIndex       uint64                         // 8B - HOTTEST: Array indexing
	cycleStateIndex uint64                         // 8B - HOTTEST: Pointer chasing
	queueHandle     uint64                         // 8B - WARM: Queue operations
	queue           *quantumqueue64.QuantumQueue64 // 8B - WARM: Queue pointer
}

// ProcessedCycle - Buffer structure
//
//go:notinheap
//go:align 32
type ProcessedCycle struct {
	originalTick    int64                 // 8B - HOT: Tick value
	cycleStateIndex CycleStateIndex       // 8B - HOT: State index
	queueHandle     quantumqueue64.Handle // 4B - WARM: Queue handle
	_               uint32                // 4B - Padding
	_               [8]byte               // 8B - Padding to 32B
}

// AddressKey - Hash table key
//
//go:notinheap
//go:align 32
type AddressKey struct {
	words [3]uint64 // 24B - Address data
	_     [8]byte   // 8B - Padding to 32B
}

// ArbitrageEdgeBinding - Initialization structure
//
//go:notinheap
//go:align 32
type ArbitrageEdgeBinding struct {
	cyclePairs [3]PairID // 24B - Triplet data
	edgeIndex  uint64    // 8B - Edge index
}

// PairShardBucket - Initialization structure
//
//go:notinheap
//go:align 32
type PairShardBucket struct {
	pairID       PairID                 // 8B - Pair ID
	edgeBindings []ArbitrageEdgeBinding // 24B - Slice header
}

// ArbitrageCoreExecutor - Core executor state
//
//go:notinheap
type ArbitrageCoreExecutor struct {
	pairToQueueIndex   localidx.Hash                   // 64B - HOTTEST: O(1) mapping
	fanoutTables       [][]FanoutEntry                 // 24B - HOT: Fanout mappings
	cycleStates        []ArbitrageCycleState           // 24B - HOT: Direct storage
	priorityQueues     []quantumqueue64.QuantumQueue64 // 24B - WARM: Owned queues
	processedCycles    [128]ProcessedCycle             // 4096B - WARM: Pre-allocated buffer
	shutdownSignal     <-chan struct{}                 // 8B - COLD: Shutdown channel
	isReverseDirection bool                            // 1B - COLD: Direction flag
	_                  [7]byte                         // 7B - Padding
}

// keccakRandomState - Initialization only
type keccakRandomState struct {
	seed    [32]byte // 32B - Seed data
	counter uint64   // 8B - Counter
}

// Global state
var (
	coreExecutors        [constants.MaxSupportedCores]*ArbitrageCoreExecutor
	coreRings            [constants.MaxSupportedCores]*ring24.Ring
	pairToCoreAssignment [constants.PairRoutingTableCapacity]uint64
	pairShardBuckets     map[PairID][]PairShardBucket
	pairAddressKeys      [constants.AddressTableCapacity]AddressKey
	addressToPairID      [constants.AddressTableCapacity]PairID
)

// Address handling functions
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func bytesToAddressKey(address40HexChars []byte) AddressKey {
	parsedAddress := utils.ParseEthereumAddress(address40HexChars)

	var lastWordBytes [8]byte
	copy(lastWordBytes[:4], parsedAddress[16:20])
	lastWord := utils.Load64(lastWordBytes[:]) >> 32

	return AddressKey{
		words: [3]uint64{
			utils.Load64(parsedAddress[0:8]),
			utils.Load64(parsedAddress[8:16]),
			lastWord,
		},
	}
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64(address40HexChars []byte) uint64 {
	addressBytes := utils.ParseEthereumAddress(address40HexChars)
	hash64 := utils.Load64(addressBytes[6:14])
	return hash64 & uint64(constants.AddressTableMask)
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex64Stored(key AddressKey) uint64 {
	hash64 := (key.words[0] >> 48) | (key.words[1] << 16)
	return hash64 & uint64(constants.AddressTableMask)
}

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

// Pair registration functions
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

		if currentPairID == 0 {
			return 0
		}

		if pairAddressKeys[i].isEqual(key) {
			return currentPairID
		}

		currentKey := pairAddressKeys[i]
		currentKeyHash := directAddressToIndex64Stored(currentKey)
		currentDist := (i + uint64(constants.AddressTableCapacity) - currentKeyHash) & uint64(constants.AddressTableMask)

		if currentDist < dist {
			return 0
		}

		i = (i + 1) & uint64(constants.AddressTableMask)
		dist++
	}
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairToCore(pairID PairID, coreID uint8) {
	pairToCoreAssignment[pairID] |= 1 << coreID
}

// Reserve processing functions
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ultraFastZeroCount(segment []byte) int {
	const ZERO_PATTERN = 0x3030303030303030

	c0 := utils.Load64(segment[0:8]) ^ ZERO_PATTERN
	c1 := utils.Load64(segment[8:16]) ^ ZERO_PATTERN
	c2 := utils.Load64(segment[16:24]) ^ ZERO_PATTERN
	c3 := utils.Load64(segment[24:28]) ^ ZERO_PATTERN

	mask := ((c0|(^c0+1))>>63)<<0 | ((c1|(^c1+1))>>63)<<1 |
		((c2|(^c2+1))>>63)<<2 | ((c3|(^c3+1))>>63)<<3

	firstChunk := bits.TrailingZeros64(mask)
	chunks := [4]uint64{c0, c1, c2, c3}
	firstByte := bits.TrailingZeros64(chunks[firstChunk]) >> 3

	return (firstChunk << 3) + firstByte
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DispatchTickUpdate(logView *types.LogView) {
	pairID := lookupPairIDByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])
	if pairID == 0 {
		return
	}

	hexData := logView.Data[2:130]

	lzcntA := ultraFastZeroCount(hexData[36:64])
	lzcntB := ultraFastZeroCount(hexData[100:128])

	// Branchless min: convert bool to mask, then use XOR trick
	cond := lzcntA - lzcntB // Negative if lzcntA < lzcntB
	mask := cond >> 31      // Arithmetic right shift: -1 if negative, 0 if positive
	minZeros := lzcntB ^ ((lzcntA ^ lzcntB) & mask)

	offsetA := 2 + 36 + minZeros
	offsetB := 2 + 100 + minZeros

	availableA := 64 - 36 - minZeros   // 28 - minZeros
	availableB := 128 - 100 - minZeros // 28 - minZeros

	// Branchless min(16, availableA) and min(16, availableB)
	condA := 16 - availableA
	maskA := condA >> 31
	remainingA := availableA ^ ((16 ^ availableA) & maskA)

	condB := 16 - availableB
	maskB := condB >> 31
	remainingB := availableB ^ ((16 ^ availableB) & maskB)

	reserve0 := utils.ParseHexU64(logView.Data[offsetA : offsetA+remainingA])
	reserve1 := utils.ParseHexU64(logView.Data[offsetB : offsetB+remainingB])

	tickValue, _ := fastuni.Log2ReserveRatio(reserve0, reserve1)

	var message [24]byte
	tickUpdate := (*TickUpdate)(unsafe.Pointer(&message))
	tickUpdate.forwardTick = tickValue
	tickUpdate.reverseTick = -tickValue
	tickUpdate.pairID = pairID

	coreAssignments := pairToCoreAssignment[pairID]
	for coreAssignments != 0 {
		coreID := bits.TrailingZeros64(uint64(coreAssignments))
		coreRings[coreID].Push(&message)
		coreAssignments &^= 1 << coreID
	}
}

// Arbitrage processing functions
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func quantizeTickToInt64(tickValue float64) int64 {
	return int64((tickValue + constants.TickClampingBound) * constants.QuantizationScale)
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processTickUpdate(executor *ArbitrageCoreExecutor, update *TickUpdate) {
	var currentTick float64
	if !executor.isReverseDirection {
		currentTick = update.forwardTick
	} else {
		currentTick = update.reverseTick
	}

	queueIndex, _ := executor.pairToQueueIndex.Get(uint32(update.pairID))
	queue := &executor.priorityQueues[queueIndex]

	cycleCount := 0

	for {
		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleStateIndex(cycleData)
		cycle := &executor.cycleStates[cycleIndex]

		totalProfitability := currentTick + cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		isProfitable := totalProfitability < 0

		if isProfitable {
			//emitArbitrageOpportunity(cycle, currentTick)
		}

		if !isProfitable || cycleCount == len(executor.processedCycles) {
			break
		}

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

	for i := 0; i < cycleCount; i++ {
		cycle := &executor.processedCycles[i]
		queue.Push(cycle.originalTick, cycle.queueHandle, uint64(cycle.cycleStateIndex))
	}

	for _, fanoutEntry := range executor.fanoutTables[queueIndex] {
		cycle := &executor.cycleStates[fanoutEntry.cycleStateIndex]
		cycle.tickValues[fanoutEntry.edgeIndex] = currentTick

		newPriority := quantizeTickToInt64(cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2])
		fanoutEntry.queue.MoveTick(quantumqueue64.Handle(fanoutEntry.queueHandle), newPriority)
	}
}

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

// Random number generation functions
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func newKeccakRandom(initialSeed []byte) *keccakRandomState {
	var seed [32]byte

	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(initialSeed)
	copy(seed[:], hasher.Sum(nil))

	return &keccakRandomState{
		seed:    seed,
		counter: 0,
	}
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (k *keccakRandomState) nextUint64() uint64 {
	var input [40]byte
	copy(input[:32], k.seed[:])
	binary.LittleEndian.PutUint64(input[32:], k.counter)

	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(input[:])
	output := hasher.Sum(nil)

	k.counter++

	return utils.Load64(output[:8])
}

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

	if upperBound&(upperBound-1) == 0 {
		return int(randomValue & uint64(upperBound-1))
	}

	high64, _ := bits.Mul64(randomValue, uint64(upperBound))
	return int(high64)
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func keccakShuffleEdgeBindings(bindings []ArbitrageEdgeBinding, pairID PairID) {
	if len(bindings) <= 1 {
		return
	}

	var seedInput [8]byte
	binary.LittleEndian.PutUint64(seedInput[:], utils.Mix64(uint64(pairID)))

	rng := newKeccakRandom(seedInput[:])

	for i := len(bindings) - 1; i > 0; i-- {
		j := rng.nextInt(i + 1)
		bindings[i], bindings[j] = bindings[j], bindings[i]
	}
}

// System initialization functions
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
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

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func attachShardToExecutor(executor *ArbitrageCoreExecutor, shard *PairShardBucket) {
	queueIndex := executor.pairToQueueIndex.Put(uint32(shard.pairID), uint32(len(executor.priorityQueues)))

	if int(queueIndex) == len(executor.priorityQueues) {
		executor.priorityQueues = append(executor.priorityQueues, *quantumqueue64.New())
		executor.fanoutTables = append(executor.fanoutTables, nil)
	}

	queue := &executor.priorityQueues[queueIndex]

	for _, edgeBinding := range shard.edgeBindings {
		executor.cycleStates = append(executor.cycleStates, ArbitrageCycleState{
			pairIDs: edgeBinding.cyclePairs,
		})
		cycleIndex := CycleStateIndex(len(executor.cycleStates) - 1)

		queueHandle, _ := queue.BorrowSafe()
		queue.Push(constants.MaxInitializationPriority, queueHandle, uint64(cycleIndex))

		otherEdge1 := (edgeBinding.edgeIndex + 1) % 3
		otherEdge2 := (edgeBinding.edgeIndex + 2) % 3

		for _, edgeIdx := range [...]uint64{otherEdge1, otherEdge2} {
			executor.fanoutTables[queueIndex] = append(executor.fanoutTables[queueIndex],
				FanoutEntry{
					edgeIndex:       edgeIdx,
					cycleStateIndex: uint64(cycleIndex),
					queueHandle:     uint64(queueHandle),
					queue:           queue,
				})
		}
	}
}

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func launchShardWorker(coreID, forwardCoreCount int, shardInput <-chan PairShardBucket) {
	runtime.LockOSThread()

	shutdownChannel := make(chan struct{})

	executor := &ArbitrageCoreExecutor{
		isReverseDirection: coreID >= forwardCoreCount,
		shutdownSignal:     shutdownChannel,
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		cycleStates:        make([]ArbitrageCycleState, 0),
	}
	coreExecutors[coreID] = executor
	coreRings[coreID] = ring24.New(constants.DefaultRingSize)

	for shard := range shardInput {
		attachShardToExecutor(executor, &shard)
	}

	stopFlag, hotFlag := control.Flags()

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

//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func InitializeArbitrageSystem(arbitrageCycles []ArbitrageTriplet) {
	coreCount := runtime.NumCPU() - 1
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	coreCount &^= 1
	forwardCoreCount := coreCount >> 1

	buildFanoutShardBuckets(arbitrageCycles)

	shardChannels := make([]chan PairShardBucket, coreCount)
	for i := range shardChannels {
		shardChannels[i] = make(chan PairShardBucket, constants.ShardChannelBufferSize)
		go launchShardWorker(i, forwardCoreCount, shardChannels[i])
	}

	currentCore := 0
	for _, shardBuckets := range pairShardBuckets {
		for _, shard := range shardBuckets {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			shardChannels[forwardCore] <- shard
			shardChannels[reverseCore] <- shard

			for _, edgeBinding := range shard.edgeBindings {
				for _, pairID := range edgeBinding.cyclePairs {
					pairToCoreAssignment[pairID] |= 1<<forwardCore | 1<<reverseCore
				}
			}
			currentCore++
		}
	}

	for _, channel := range shardChannels {
		close(channel)
	}
}
