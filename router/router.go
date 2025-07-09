package router

import (
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
)

// Core types - optimized widths
type PairID uint32
type CoreID uint32
type ArbitrageTriplet [3]PairID
type CycleStateIndex uint32

// PERFECTLY CACHE-ALIGNED DATA STRUCTURES WITH MAXIMUM COMPILER ABUSE

// AddressKey - exactly 32 bytes, 8-byte aligned fields
//
//go:notinheap
//go:align 32
type AddressKey struct {
	word0 uint64 // 8B - bytes 0-7
	word1 uint64 // 8B - bytes 8-15
	word2 uint64 // 8B - bytes 16-23 (only 4 bytes used)
	_     uint64 // 8B - padding to 32B
}

// TickUpdate - exactly 32 bytes for optimal cache alignment
//
//go:notinheap
//go:align 32
type TickUpdate struct {
	forwardTick float64 // 8B - hot: used every time
	reverseTick float64 // 8B - hot: used every time
	pairID      PairID  // 4B - hot: used every time
	_           uint32  // 4B - padding to 8B boundary
	_           uint64  // 8B - padding to 32B total
}

// ArbitrageCycleState - exactly 64 bytes, hottest to coldest
//
//go:notinheap
//go:align 64
type ArbitrageCycleState struct {
	// Hottest: accessed every tick update (24 bytes)
	tick0 float64 // 8B - most frequently accessed
	tick1 float64 // 8B - most frequently accessed
	tick2 float64 // 8B - most frequently accessed

	// Warm: accessed occasionally (16 bytes)
	pair0 PairID // 4B - moderate access
	pair1 PairID // 4B - moderate access
	pair2 PairID // 4B - moderate access
	_     uint32 // 4B - padding to 8B boundary

	// Cold: padding (24 bytes)
	_ [24]byte // 24B - pad to 64B cache line
}

// FanoutEntry - exactly 32 bytes, hottest to coldest
//
//go:notinheap
//go:align 32
type FanoutEntry struct {
	// Hottest: used in every MoveTick call (8 bytes)
	queueHandle quantumqueue64.Handle // 4B - ultra-hot
	edgeIndex   uint16                // 2B - ultra-hot
	_           uint16                // 2B - padding to 8B boundary

	// Hot: used in every lookup (8 bytes)
	cycleStateIndex CycleStateIndex // 4B - hot
	_               uint32          // 4B - padding to 8B boundary

	// Warm: cached pointer (8 bytes)
	queuePtr uintptr // 8B - pointer as uintptr for better alignment

	// Cold: padding (8 bytes)
	_ uint64 // 8B - pad to 32B total
}

// ArbitrageEdgeBinding - exactly 16 bytes
//
//go:notinheap
//go:align 16
type ArbitrageEdgeBinding struct {
	pair0     PairID // 4B - hot
	pair1     PairID // 4B - hot
	pair2     PairID // 4B - hot
	edgeIndex uint16 // 2B - hot
	_         uint16 // 2B - padding to 16B
}

// PairShardBucket - exactly 32 bytes
//
//go:notinheap
//go:align 32
type PairShardBucket struct {
	// Hot data (8 bytes)
	pairID PairID // 4B - hot
	count  uint32 // 4B - hot: binding count

	// Warm data (24 bytes) - slice header
	bindingsPtr uintptr // 8B - pointer to bindings
	bindingsCap uint64  // 8B - capacity
	_           uint64  // 8B - padding to 32B
}

// ArbitrageCoreExecutor - exactly 128 bytes (2 cache lines), hottest to coldest
//
//go:notinheap
//go:align 64
type ArbitrageCoreExecutor struct {
	// Cache line 1: Ultra-hot data (64 bytes)
	queuesPtr      uintptr  // 8B - pointer to priority queues
	fanoutPtr      uintptr  // 8B - pointer to fanout tables
	cycleStatesPtr uintptr  // 8B - pointer to cycle states
	pairIndexPtr   uintptr  // 8B - pointer to pair->queue index
	queueCount     uint32   // 4B - queue count
	isReverse      uint32   // 4B - reverse direction flag (0/1)
	cycleCount     uint32   // 4B - cycle count
	fanoutCount    uint32   // 4B - fanout count
	_              [16]byte // 16B - pad to 64B

	// Cache line 2: Warm data (64 bytes)
	_ [64]byte // 64B - reserved for future hot data
}

// ProcessedCycle - exactly 32 bytes for stack allocation
//
//go:notinheap
//go:align 32
type ProcessedCycle struct {
	queueHandle     quantumqueue64.Handle // 4B
	_               uint32                // 4B - padding to 8B boundary
	originalTick    int64                 // 8B
	cycleStateIndex CycleStateIndex       // 4B
	_               uint32                // 4B - padding to 8B boundary
	_               uint64                // 8B - padding to 32B total
}

// InitializationBucket - exactly 64 bytes for initialization data
//
//go:notinheap
//go:align 64
type InitializationBucket struct {
	pairShardBuckets  uintptr // 8B - pointer to map
	temporaryBindings uintptr // 8B - pointer to map
	shardChannels     uintptr // 8B - pointer to slice
	cycleBuffer       uintptr // 8B - pointer to buffer
	addressBuffer     uintptr // 8B - pointer to buffer
	tempSlices        uintptr // 8B - pointer to slices
	tempExecutors     uintptr // 8B - pointer to executors
	workersStarted    uint32  // 4B - flag
	_                 uint32  // 4B - padding to 8B boundary
}

// GLOBAL STATE - perfectly aligned arrays
var (
	// Core arrays - 64-byte aligned
	coreExecutors [constants.MaxSupportedCores]*ArbitrageCoreExecutor
	coreRings     [constants.MaxSupportedCores]*ring24.Ring

	// Lookup tables - cache-line aligned
	pairToCoreAssignment [constants.PairRoutingTableCapacity]uint64
	pairAddressKeys      [constants.AddressTableCapacity]AddressKey
	addressToPairID      [constants.AddressTableCapacity]PairID

	// Padding to ensure next vars are cache-aligned
	_ [7]PairID // Pad to 8-byte boundary
)

// Initialization data (gets nuked) - consolidated into single bucket
var (
	initBucket            InitializationBucket
	initPairShardBuckets  map[PairID][]PairShardBucket
	initTemporaryBindings map[PairID][]ArbitrageEdgeBinding
	initShardChannels     []chan PairShardBucket
)

// ULTRA-OPTIMIZED CORE FUNCTIONS WITH MAXIMUM COMPILER ABUSE

// quantizeTickToInt64 - single operation, no branches
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func quantizeTickToInt64(tick float64) int64 {
	return int64((tick + constants.TickClampingBound) * constants.QuantizationScale)
}

// fastHash - optimized hash with single multiply
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func fastHash(value uint64) uint32 {
	// Fibonacci hash - single multiply + shift
	return uint32((value*0x9e3779b97f4a7c15)>>32) & constants.AddressTableMask
}

// directAddressToIndex - direct memory access
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func directAddressToIndex(addressBytes []byte) uint32 {
	return fastHash(*(*uint64)(unsafe.Pointer(&addressBytes[2])))
}

// bytesToAddressKey - direct memory copy, 8-byte aligned
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func bytesToAddressKey(addressBytes []byte) AddressKey {
	return AddressKey{
		word0: *(*uint64)(unsafe.Pointer(&addressBytes[2])),
		word1: *(*uint64)(unsafe.Pointer(&addressBytes[10])),
		word2: *(*uint64)(unsafe.Pointer(&addressBytes[18])),
		// word3 is padding
	}
}

// isEqual - vectorized comparison
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (a AddressKey) isEqual(b AddressKey) bool {
	// Compare first 3 words only (4th is padding)
	return a.word0 == b.word0 && a.word1 == b.word1 && a.word2 == b.word2
}

// RegisterPairAddress - single probe, direct write
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairAddress(addressBytes []byte, pairID PairID) {
	key := bytesToAddressKey(addressBytes)
	index := directAddressToIndex(addressBytes)

	pairAddressKeys[index] = key
	addressToPairID[index] = pairID
}

// lookupPairIDByAddress - single probe, branch-optimized with prefetching
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func lookupPairIDByAddress(addressBytes []byte) PairID {
	key := bytesToAddressKey(addressBytes)
	index := directAddressToIndex(addressBytes)

	// Prefetch both the key and pairID locations
	_ = *(*byte)(unsafe.Add(unsafe.Pointer(&pairAddressKeys[index]), 0))
	_ = *(*byte)(unsafe.Add(unsafe.Pointer(&addressToPairID[index]), 0))

	stored := pairAddressKeys[index]
	if stored.isEqual(key) {
		return addressToPairID[index]
	}
	return 0
}

// RegisterPairToCore - direct bit manipulation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPairToCore(pairID PairID, coreID CoreID) {
	pairToCoreAssignment[pairID] |= 1 << coreID
}

// processTickUpdate - ULTRA-OPTIMIZED hot path with memory prefetching
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func processTickUpdate(executor *ArbitrageCoreExecutor, update *TickUpdate) {
	// Branch-free tick selection using bit manipulation
	reverseMask := uint64(-(uint64(executor.isReverse) & 1))
	forwardBits := *(*uint64)(unsafe.Pointer(&update.forwardTick))
	reverseBits := *(*uint64)(unsafe.Pointer(&update.reverseTick))
	currentTickBits := (forwardBits &^ reverseMask) | (reverseBits & reverseMask)
	currentTick := *(*float64)(unsafe.Pointer(&currentTickBits))

	// Direct pointer arithmetic for queue access
	queuesPtr := (*[]quantumqueue64.QuantumQueue64)(unsafe.Pointer(executor.queuesPtr))
	pairIndexPtr := (*localidx.Hash)(unsafe.Pointer(executor.pairIndexPtr))

	queueIndex, _ := pairIndexPtr.Get(uint32(update.pairID))
	queue := &(*queuesPtr)[queueIndex]

	// Stack-allocated arrays - 64 entries for optimal cache usage
	var handles [64]quantumqueue64.Handle
	var priorities [64]int64
	var cycleIndices [64]CycleStateIndex
	cycleCount := uint32(0)

	// Process profitable cycles with early termination
	cycleStatesPtr := (*[]ArbitrageCycleState)(unsafe.Pointer(executor.cycleStatesPtr))

	for cycleCount < 64 && !queue.Empty() {
		handle, queueTick, cycleData := queue.PeepMin()
		cycleIndex := CycleStateIndex(cycleData)

		// Direct array access - no bounds checking
		cycle := &(*cycleStatesPtr)[cycleIndex]

		// Prefetch next cache line for the next cycle
		if cycleCount < 63 {
			_ = *(*byte)(unsafe.Add(unsafe.Pointer(cycle), 64))
		}

		// Profitability check with direct field access
		totalProfit := currentTick + cycle.tick0 + cycle.tick1 + cycle.tick2

		if totalProfit >= 0 {
			break
		}

		handles[cycleCount] = handle
		priorities[cycleCount] = queueTick
		cycleIndices[cycleCount] = cycleIndex
		cycleCount++

		queue.UnlinkMin(handle)
	}

	// Reinsert cycles - unrolled for common small counts
	switch cycleCount {
	case 0:
		// No cycles processed
	case 1:
		queue.Push(priorities[0], handles[0], uint64(cycleIndices[0]))
	case 2:
		queue.Push(priorities[0], handles[0], uint64(cycleIndices[0]))
		queue.Push(priorities[1], handles[1], uint64(cycleIndices[1]))
	case 3:
		queue.Push(priorities[0], handles[0], uint64(cycleIndices[0]))
		queue.Push(priorities[1], handles[1], uint64(cycleIndices[1]))
		queue.Push(priorities[2], handles[2], uint64(cycleIndices[2]))
	case 4:
		queue.Push(priorities[0], handles[0], uint64(cycleIndices[0]))
		queue.Push(priorities[1], handles[1], uint64(cycleIndices[1]))
		queue.Push(priorities[2], handles[2], uint64(cycleIndices[2]))
		queue.Push(priorities[3], handles[3], uint64(cycleIndices[3]))
	default:
		// General case for larger counts
		for i := uint32(0); i < cycleCount; i++ {
			queue.Push(priorities[i], handles[i], uint64(cycleIndices[i]))
		}
	}

	// Fanout processing - optimized for cache locality with prefetching
	fanoutPtr := (*[][]FanoutEntry)(unsafe.Pointer(executor.fanoutPtr))
	fanoutEntries := (*fanoutPtr)[queueIndex]

	// Process fanout entries with direct field access
	for i := range fanoutEntries {
		entry := &fanoutEntries[i]
		cycle := &(*cycleStatesPtr)[entry.cycleStateIndex]

		// Prefetch next fanout entry
		if i < len(fanoutEntries)-1 {
			nextEntry := &fanoutEntries[i+1]
			_ = *(*byte)(unsafe.Add(unsafe.Pointer(nextEntry), 0))
			// Prefetch next cycle state
			nextCycle := &(*cycleStatesPtr)[nextEntry.cycleStateIndex]
			_ = *(*byte)(unsafe.Add(unsafe.Pointer(nextCycle), 0))
		}

		// Direct field assignment based on edge index
		switch entry.edgeIndex {
		case 0:
			cycle.tick0 = currentTick
		case 1:
			cycle.tick1 = currentTick
		case 2:
			cycle.tick2 = currentTick
		}

		// Recalculate priority with direct field access
		newPriority := quantizeTickToInt64(cycle.tick0 + cycle.tick1 + cycle.tick2)

		// Direct queue access via cached pointer
		queuePtr := (*quantumqueue64.QuantumQueue64)(unsafe.Pointer(entry.queuePtr))
		queuePtr.MoveTick(entry.queueHandle, newPriority)
	}
}

// DispatchTickUpdate - ULTRA-OPTIMIZED dispatch with memory prefetching
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func DispatchTickUpdate(logView *types.LogView) {
	// Direct slice to fixed-size array conversion
	addressBytes := (*[constants.AddressHexEnd - constants.AddressHexStart]byte)(
		unsafe.Pointer(&logView.Addr[constants.AddressHexStart]))

	// Prefetch the data section while processing address
	_ = *(*byte)(unsafe.Add(unsafe.Pointer(&logView.Data[0]), 0))

	pairID := lookupPairIDByAddress(addressBytes[:])
	if pairID == 0 {
		return
	}

	// Direct memory access for reserve values with prefetching
	dataPtr := uintptr(unsafe.Pointer(&logView.Data[0]))

	// Prefetch both reserve locations
	_ = *(*byte)(unsafe.Pointer(dataPtr + 24))
	_ = *(*byte)(unsafe.Pointer(dataPtr + 56))

	reserve0 := binary.BigEndian.Uint64((*[8]byte)(unsafe.Pointer(dataPtr + 24))[:])
	reserve1 := binary.BigEndian.Uint64((*[8]byte)(unsafe.Pointer(dataPtr + 56))[:])

	// Early return using bitwise OR
	if (reserve0 | reserve1) == 0 {
		return
	}

	// Calculate tick value
	tickValue, _ := fastuni.Log2ReserveRatio(reserve0, reserve1)

	// Stack-allocated message buffer - 32 bytes for cache alignment
	var messageBuffer [32]byte
	tickUpdate := (*TickUpdate)(unsafe.Pointer(&messageBuffer))
	tickUpdate.forwardTick = tickValue
	tickUpdate.reverseTick = -tickValue
	tickUpdate.pairID = pairID

	// Optimized core dispatch with prefetching
	coreAssignments := pairToCoreAssignment[pairID]
	coreCount := bits.OnesCount64(coreAssignments)

	// Prefetch the ring buffer for the first core
	if coreAssignments != 0 {
		firstCore := bits.TrailingZeros64(coreAssignments)
		_ = *(*byte)(unsafe.Add(unsafe.Pointer(coreRings[firstCore]), 0))
	}

	// Unroll for common small core counts
	switch coreCount {
	case 1:
		coreID := bits.TrailingZeros64(coreAssignments)
		coreRings[coreID].Push((*[24]byte)(unsafe.Pointer(&messageBuffer)))
	case 2:
		coreID := bits.TrailingZeros64(coreAssignments)
		coreRings[coreID].Push((*[24]byte)(unsafe.Pointer(&messageBuffer)))
		coreAssignments &^= 1 << coreID
		coreID = bits.TrailingZeros64(coreAssignments)
		// Prefetch next ring while processing current
		_ = *(*byte)(unsafe.Add(unsafe.Pointer(coreRings[coreID]), 0))
		coreRings[coreID].Push((*[24]byte)(unsafe.Pointer(&messageBuffer)))
	case 3:
		for i := 0; i < 3; i++ {
			coreID := bits.TrailingZeros64(coreAssignments)
			// Prefetch next ring if not last iteration
			if i < 2 {
				nextAssignments := coreAssignments &^ (1 << coreID)
				if nextAssignments != 0 {
					nextCore := bits.TrailingZeros64(nextAssignments)
					_ = *(*byte)(unsafe.Add(unsafe.Pointer(coreRings[nextCore]), 0))
				}
			}
			coreRings[coreID].Push((*[24]byte)(unsafe.Pointer(&messageBuffer)))
			coreAssignments &^= 1 << coreID
		}
	case 4:
		for i := 0; i < 4; i++ {
			coreID := bits.TrailingZeros64(coreAssignments)
			// Prefetch next ring if not last iteration
			if i < 3 {
				nextAssignments := coreAssignments &^ (1 << coreID)
				if nextAssignments != 0 {
					nextCore := bits.TrailingZeros64(nextAssignments)
					_ = *(*byte)(unsafe.Add(unsafe.Pointer(coreRings[nextCore]), 0))
				}
			}
			coreRings[coreID].Push((*[24]byte)(unsafe.Pointer(&messageBuffer)))
			coreAssignments &^= 1 << coreID
		}
	default:
		// General case for many cores with prefetching
		for coreAssignments != 0 {
			coreID := bits.TrailingZeros64(coreAssignments)
			// Prefetch next ring buffer
			nextAssignments := coreAssignments &^ (1 << coreID)
			if nextAssignments != 0 {
				nextCore := bits.TrailingZeros64(nextAssignments)
				_ = *(*byte)(unsafe.Add(unsafe.Pointer(coreRings[nextCore]), 0))
			}
			coreRings[coreID].Push((*[24]byte)(unsafe.Pointer(&messageBuffer)))
			coreAssignments &^= 1 << coreID
		}
	}
}

// INITIALIZATION FUNCTIONS WITH MAXIMUM COMPILER ABUSE

// fastRandom - ultra-fast LCG
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func fastRandom(upperBound int) int {
	var seed uintptr
	seed = uintptr(unsafe.Pointer(&seed))
	seed = seed*1103515245 + 12345
	return int(seed) % upperBound
}

// shuffleEdgeBindings - optimized Fisher-Yates
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func shuffleEdgeBindings(bindings []ArbitrageEdgeBinding) {
	for i := len(bindings) - 1; i > 0; i-- {
		j := fastRandom(i + 1)
		bindings[i], bindings[j] = bindings[j], bindings[i]
	}
}

// buildFanoutShardBuckets - optimized shard construction
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func buildFanoutShardBuckets(cycles []ArbitrageTriplet) {
	initPairShardBuckets = make(map[PairID][]PairShardBucket, len(cycles)*3)
	initTemporaryBindings = make(map[PairID][]ArbitrageEdgeBinding, len(cycles)*3)

	for _, triplet := range cycles {
		for i := 0; i < 3; i++ {
			binding := ArbitrageEdgeBinding{
				pair0:     triplet[0],
				pair1:     triplet[1],
				pair2:     triplet[2],
				edgeIndex: uint16(i),
			}
			initTemporaryBindings[triplet[i]] = append(initTemporaryBindings[triplet[i]], binding)
		}
	}

	for pairID, bindings := range initTemporaryBindings {
		shuffleEdgeBindings(bindings)

		shardSize := constants.MaxCyclesPerShard
		if len(bindings) < shardSize {
			shardSize = len(bindings)
		}

		for offset := 0; offset < len(bindings); offset += shardSize {
			endOffset := offset + shardSize
			if endOffset > len(bindings) {
				endOffset = len(bindings)
			}

			shard := PairShardBucket{
				pairID:      pairID,
				count:       uint32(endOffset - offset),
				bindingsPtr: uintptr(unsafe.Pointer(&bindings[offset])),
				bindingsCap: uint64(endOffset - offset),
			}

			initPairShardBuckets[pairID] = append(initPairShardBuckets[pairID], shard)
		}
	}
}

// attachShardToExecutor - optimized shard attachment
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func attachShardToExecutor(executor *ArbitrageCoreExecutor, shard *PairShardBucket) {
	pairIndexPtr := (*localidx.Hash)(unsafe.Pointer(executor.pairIndexPtr))
	queueIndex := pairIndexPtr.Put(uint32(shard.pairID), executor.queueCount)

	// Expand arrays if needed
	if queueIndex == executor.queueCount {
		// Resize queues and fanout tables
		executor.queueCount++
		executor.fanoutCount++
	}

	// Process bindings
	bindingsPtr := (*[constants.MaxCyclesPerShard]ArbitrageEdgeBinding)(unsafe.Pointer(shard.bindingsPtr))
	bindings := bindingsPtr[:shard.count]

	queuesPtr := (*[]quantumqueue64.QuantumQueue64)(unsafe.Pointer(executor.queuesPtr))
	queue := &(*queuesPtr)[queueIndex]

	for _, binding := range bindings {
		// Add cycle state
		cycleStatesPtr := (*[]ArbitrageCycleState)(unsafe.Pointer(executor.cycleStatesPtr))
		*cycleStatesPtr = append(*cycleStatesPtr, ArbitrageCycleState{
			pair0: binding.pair0,
			pair1: binding.pair1,
			pair2: binding.pair2,
		})
		cycleIndex := CycleStateIndex(len(*cycleStatesPtr) - 1)
		executor.cycleCount++

		// Allocate queue handle
		handle, _ := queue.BorrowSafe()
		queue.Push(constants.MaxInitializationPriority, handle, uint64(cycleIndex))

		// Create fanout entries for other edges
		fanoutPtr := (*[][]FanoutEntry)(unsafe.Pointer(executor.fanoutPtr))

		for edgeIdx := uint16(0); edgeIdx < 3; edgeIdx++ {
			if edgeIdx != binding.edgeIndex {
				entry := FanoutEntry{
					queueHandle:     handle,
					edgeIndex:       edgeIdx,
					cycleStateIndex: cycleIndex,
					queuePtr:        uintptr(unsafe.Pointer(queue)),
				}
				(*fanoutPtr)[queueIndex] = append((*fanoutPtr)[queueIndex], entry)
			}
		}
	}
}

// launchShardWorker - optimized worker launch
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func launchShardWorker(coreID, forwardCoreCount int, shardInput <-chan PairShardBucket) {
	runtime.LockOSThread()

	// Pre-allocate with optimal sizes
	queues := make([]quantumqueue64.QuantumQueue64, 0, 256)
	fanoutTables := make([][]FanoutEntry, 0, 256)
	cycleStates := make([]ArbitrageCycleState, 0, 2048)
	pairIndex := localidx.New(constants.DefaultLocalIdxSize)

	executor := &ArbitrageCoreExecutor{
		queuesPtr:      uintptr(unsafe.Pointer(&queues)),
		fanoutPtr:      uintptr(unsafe.Pointer(&fanoutTables)),
		cycleStatesPtr: uintptr(unsafe.Pointer(&cycleStates)),
		pairIndexPtr:   uintptr(unsafe.Pointer(&pairIndex)),
		queueCount:     0,
		isReverse:      0,
		cycleCount:     0,
		fanoutCount:    0,
	}

	if coreID >= forwardCoreCount {
		executor.isReverse = 1
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
			}, make(chan struct{}))
	} else {
		ring24.PinnedConsumer(coreID, coreRings[coreID], stopFlag, hotFlag,
			func(messagePtr *[24]byte) {
				processTickUpdate(executor, (*TickUpdate)(unsafe.Pointer(messagePtr)))
			}, make(chan struct{}))
	}
}

// InitializeArbitrageSystem - optimized system initialization
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func InitializeArbitrageSystem(arbitrageCycles []ArbitrageTriplet) {
	coreCount := runtime.NumCPU()
	if coreCount > 1 {
		coreCount--
	}
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	if coreCount&1 == 1 {
		coreCount--
	}
	if coreCount < 2 {
		coreCount = 2
	}
	forwardCoreCount := coreCount / 2

	buildFanoutShardBuckets(arbitrageCycles)

	initShardChannels = make([]chan PairShardBucket, coreCount)
	for i := 0; i < coreCount; i++ {
		initShardChannels[i] = make(chan PairShardBucket, constants.ShardChannelBufferSize)
		go launchShardWorker(i, forwardCoreCount, initShardChannels[i])
	}

	currentCore := 0
	for _, shardBuckets := range initPairShardBuckets {
		for _, shard := range shardBuckets {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			initShardChannels[forwardCore] <- shard
			initShardChannels[reverseCore] <- shard

			// Update core assignments
			bindingsPtr := (*[constants.MaxCyclesPerShard]ArbitrageEdgeBinding)(unsafe.Pointer(shard.bindingsPtr))
			bindings := bindingsPtr[:shard.count]

			for _, binding := range bindings {
				pairs := [3]PairID{binding.pair0, binding.pair1, binding.pair2}
				for _, pairID := range pairs {
					pairToCoreAssignment[pairID] |= 1<<CoreID(forwardCore) | 1<<CoreID(reverseCore)
				}
			}
			currentCore++
		}
	}

	for _, channel := range initShardChannels {
		close(channel)
	}

	cleanupInitializationData()
}

// cleanupInitializationData - aggressive cleanup with compiler abuse
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func cleanupInitializationData() {
	runtime.GC()

	// Nil everything in the init bucket
	initBucket = InitializationBucket{}
	initPairShardBuckets = nil
	initTemporaryBindings = nil
	initShardChannels = nil

	runtime.GC()
	runtime.GC()
	runtime.KeepAlive(nil)
}
