// router.go — FOOTGUN MODE: Zero-safety triangular arbitrage router
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

// Direct storage - no alignment directives, let the CPU suffer
type AddressKey [20]byte

type TickUpdate struct {
	forwardTick float64
	reverseTick float64
	pairID      PairID
	_           [4]byte
}

type ArbitrageCycleState struct {
	tickValues [3]float64
	pairIDs    [3]PairID
	_          uint32
}

type ArbitrageEdgeBinding struct {
	cyclePairs [3]PairID
	edgeIndex  uint16
	_          uint16
}

type FanoutEntry struct {
	queueHandle     quantumqueue64.Handle
	edgeIndex       uint16
	_               uint16
	cycleStateIndex CycleStateIndex
	queue           *quantumqueue64.QuantumQueue64
	_               uint64
}

type PairShardBucket struct {
	pairID       PairID
	_            uint32
	edgeBindings []ArbitrageEdgeBinding
}

type ArbitrageCoreExecutor struct {
	priorityQueues     []quantumqueue64.QuantumQueue64
	fanoutTables       [][]FanoutEntry
	shutdownChannel    chan struct{} // Changed to bidirectional
	isReverseDirection bool
	_                  [7]byte
	pairToQueueIndex   localidx.Hash
	cycleStates        []ArbitrageCycleState
	_                  [5]uint64
}

var (
	coreExecutors        [constants.MaxSupportedCores]*ArbitrageCoreExecutor
	coreRings            [constants.MaxSupportedCores]*ring24.Ring
	pairToCoreAssignment [constants.PairRoutingTableCapacity]uint64
	pairShardBuckets     map[PairID][]PairShardBucket
	addressToPairID      map[AddressKey]PairID
)

// Test-only helper functions
//
//go:nosplit
func bytesToAddressKey(addressBytes []byte) AddressKey {
	return AddressKey(utils.ParseEthereumAddress(addressBytes))
}

//go:nosplit
func directAddressToIndex64(address40Bytes []byte) uint32 {
	addressBytes := utils.ParseEthereumAddress(address40Bytes)
	hash64 := *(*uint64)(unsafe.Pointer(&addressBytes[0]))
	return uint32(hash64) & constants.AddressTableMask
}

//go:nosplit
func (a AddressKey) isEqual(b AddressKey) bool {
	return a == b
}

//go:nosplit
func quantizeTickToInt64(tickValue float64) int64 {
	return int64((tickValue + constants.TickClampingBound) * constants.QuantizationScale)
}

//go:nosplit
func secureRandomInt(upperBound int) int {
	var b [8]byte
	rand.Read(b[:])
	return int(binary.LittleEndian.Uint64(b[:]) % uint64(upperBound))
}

//go:nosplit
func RegisterPairAddress(address40HexBytes []byte, pairID PairID) {
	addressToPairID[AddressKey(utils.ParseEthereumAddress(address40HexBytes))] = pairID
}

//go:nosplit
func lookupPairIDByAddress(address40HexBytes []byte) PairID {
	return addressToPairID[AddressKey(utils.ParseEthereumAddress(address40HexBytes))]
}

//go:nosplit
func RegisterPairToCore(pairID PairID, coreID uint8) {
	pairToCoreAssignment[pairID] |= 1 << coreID
}

func processTickUpdate(executor *ArbitrageCoreExecutor, update *TickUpdate) {
	// Branchless tick selection using bit manipulation
	// isReverseDirection is bool, but we treat it as 0 or 1
	offset := uintptr(*(*uint8)(unsafe.Pointer(&executor.isReverseDirection))) << 3
	currentTick := *(*float64)(unsafe.Add(unsafe.Pointer(update), offset))

	queueIndex, _ := executor.pairToQueueIndex.Get(uint32(update.pairID))
	queue := &executor.priorityQueues[queueIndex]

	// Stack buffer - no struct, just raw memory
	var buffer [128 * 24]byte
	cycleCount := 0

	// Extract all profitable cycles
	for !queue.Empty() {
		handle, tick, data := queue.PeepMin()
		cycle := &executor.cycleStates[data]

		if currentTick+cycle.tickValues[0]+cycle.tickValues[1]+cycle.tickValues[2] >= 0 {
			break
		}

		// Direct memory write
		base := unsafe.Pointer(&buffer[cycleCount*24])
		*(*quantumqueue64.Handle)(base) = handle
		*(*int64)(unsafe.Add(base, 8)) = tick
		*(*uint64)(unsafe.Add(base, 16)) = data
		cycleCount++

		queue.UnlinkMin(handle)

		if cycleCount == 128 {
			break
		}
	}

	// Reinsert
	for i := 0; i < cycleCount; i++ {
		base := unsafe.Pointer(&buffer[i*24])
		queue.Push(
			*(*int64)(unsafe.Add(base, 8)),
			*(*quantumqueue64.Handle)(base),
			*(*uint64)(unsafe.Add(base, 16)),
		)
	}

	// Direct fanout update
	fanout := executor.fanoutTables[queueIndex]
	for i := 0; i < len(fanout); i++ {
		entry := &fanout[i]
		cycle := &executor.cycleStates[entry.cycleStateIndex]
		cycle.tickValues[entry.edgeIndex] = currentTick
		entry.queue.MoveTick(entry.queueHandle,
			quantizeTickToInt64(cycle.tickValues[0]+cycle.tickValues[1]+cycle.tickValues[2]))
	}
}

//go:nosplit
func DispatchTickUpdate(logView *types.LogView) {
	pairID := lookupPairIDByAddress(logView.Addr[constants.AddressHexStart:constants.AddressHexEnd])

	// Direct calculation
	tickValue, _ := fastuni.Log2ReserveRatio(
		*(*uint64)(unsafe.Pointer(&logView.Data[24])),
		*(*uint64)(unsafe.Pointer(&logView.Data[56])),
	)

	// Stack allocation
	update := TickUpdate{
		forwardTick: tickValue,
		reverseTick: -tickValue,
		pairID:      pairID,
	}

	// Direct dispatch
	coreAssignments := pairToCoreAssignment[pairID]
	for coreAssignments != 0 {
		coreID := bits.TrailingZeros64(coreAssignments)
		coreRings[coreID].Push((*[24]byte)(unsafe.Pointer(&update)))
		coreAssignments &^= 1 << coreID
	}
}

//go:nosplit
func shuffleEdgeBindings(bindings []ArbitrageEdgeBinding) {
	for i := len(bindings) - 1; i > 0; i-- {
		j := secureRandomInt(i + 1)
		bindings[i], bindings[j] = bindings[j], bindings[i]
	}
}

func buildFanoutShardBuckets(cycles []ArbitrageTriplet) {
	pairShardBuckets = make(map[PairID][]PairShardBucket)
	temporaryBindings := make(map[PairID][]ArbitrageEdgeBinding)

	for _, triplet := range cycles {
		for i := 0; i < 3; i++ {
			temporaryBindings[triplet[i]] = append(temporaryBindings[triplet[i]],
				ArbitrageEdgeBinding{
					cyclePairs: triplet,
					edgeIndex:  uint16(i),
				})
		}
	}

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

func launchShardWorker(coreID, forwardCoreCount int, shardInput <-chan PairShardBucket) {
	runtime.LockOSThread()

	executor := &ArbitrageCoreExecutor{
		isReverseDirection: coreID >= forwardCoreCount,
		shutdownChannel:    make(chan struct{}),
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
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
			}, executor.shutdownChannel)
	} else {
		ring24.PinnedConsumer(coreID, coreRings[coreID], stopFlag, hotFlag,
			func(messagePtr *[24]byte) {
				processTickUpdate(executor, (*TickUpdate)(unsafe.Pointer(messagePtr)))
			}, executor.shutdownChannel)
	}
}

func InitializeArbitrageSystem(arbitrageCycles []ArbitrageTriplet) {
	addressToPairID = make(map[AddressKey]PairID)

	coreCount := runtime.NumCPU() - 1
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	if coreCount&1 == 1 {
		coreCount--
	}
	forwardCoreCount := coreCount / 2

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
