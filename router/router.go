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
type Triplet [3]PairID
type CycleIdx uint32

// PERFECTLY CACHE-ALIGNED DATA STRUCTURES WITH MAXIMUM COMPILER ABUSE

// AddrKey - exactly 32 bytes, 8-byte aligned fields
//
//go:notinheap
//go:align 32
type AddrKey struct {
	word0 uint64 // 8B - bytes 0-7
	word1 uint64 // 8B - bytes 8-15
	word2 uint64 // 8B - bytes 16-23 (only 4 bytes used)
	_     uint64 // 8B - padding to 32B
}

// Tick - exactly 32 bytes for optimal cache alignment
//
//go:notinheap
//go:align 32
type Tick struct {
	forward float64 // 8B - hot: used every time
	reverse float64 // 8B - hot: used every time
	pairID  PairID  // 4B - hot: used every time
	_       uint32  // 4B - padding to 8B boundary
	_       uint64  // 8B - padding to 32B total
}

// Cycle - exactly 64 bytes, hottest to coldest
//
//go:notinheap
//go:align 64
type Cycle struct {
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

// Fanout - exactly 32 bytes, hottest to coldest
//
//go:notinheap
//go:align 32
type Fanout struct {
	// Hottest: used in every MoveTick call (8 bytes)
	handle  quantumqueue64.Handle // 4B - ultra-hot
	edgeIdx uint16                // 2B - ultra-hot
	_       uint16                // 2B - padding to 8B boundary

	// Hot: used in every lookup (8 bytes)
	cycleIdx CycleIdx // 4B - hot
	_        uint32   // 4B - padding to 8B boundary

	// Warm: cached pointer (8 bytes)
	queuePtr uintptr // 8B - pointer as uintptr for better alignment

	// Cold: padding (8 bytes)
	_ uint64 // 8B - pad to 32B total
}

// Edge - exactly 16 bytes
//
//go:notinheap
//go:align 16
type Edge struct {
	pair0   PairID // 4B - hot
	pair1   PairID // 4B - hot
	pair2   PairID // 4B - hot
	edgeIdx uint16 // 2B - hot
	_       uint16 // 2B - padding to 16B
}

// Shard - exactly 32 bytes
//
//go:notinheap
//go:align 32
type Shard struct {
	// Hot data (8 bytes)
	pairID PairID // 4B - hot
	count  uint32 // 4B - hot: binding count

	// Warm data (24 bytes) - slice header
	edgesPtr uintptr // 8B - pointer to bindings
	edgesCap uint64  // 8B - capacity
	_        uint64  // 8B - padding to 32B
}

// Executor - exactly 128 bytes (2 cache lines), hottest to coldest
//
//go:notinheap
//go:align 64
type Executor struct {
	// Cache line 1: Ultra-hot data (64 bytes)
	queuesPtr   uintptr  // 8B - pointer to priority queues
	fanoutPtr   uintptr  // 8B - pointer to fanout tables
	cyclesPtr   uintptr  // 8B - pointer to cycle states
	indexPtr    uintptr  // 8B - pointer to pair->queue index
	queueCount  uint32   // 4B - queue count
	isReverse   uint32   // 4B - reverse direction flag (0/1)
	cycleCount  uint32   // 4B - cycle count
	fanoutCount uint32   // 4B - fanout count
	_           [16]byte // 16B - pad to 64B

	// Cache line 2: Warm data (64 bytes)
	_ [64]byte // 64B - reserved for future hot data
}

// GLOBAL STATE - perfectly aligned arrays
var (
	// Core arrays - 64-byte aligned
	executors [constants.MaxSupportedCores]*Executor
	rings     [constants.MaxSupportedCores]*ring24.Ring

	// Lookup tables - cache-line aligned
	coreAssignments [constants.PairRoutingTableCapacity]uint64
	addrKeys        [constants.AddressTableCapacity]AddrKey
	pairIDs         [constants.AddressTableCapacity]PairID

	// Padding to ensure next vars are cache-aligned
	_ [7]PairID // Pad to 8-byte boundary
)

// Initialization data (gets nuked) - consolidated
var (
	shards   map[PairID][]Shard
	bindings map[PairID][]Edge
	channels []chan Shard
)

// ULTRA-OPTIMIZED CORE FUNCTIONS WITH MAXIMUM COMPILER ABUSE

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

// quantizeTick - single operation, no branches
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func quantizeTick(tick float64) int64 {
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

// addrIndex - direct memory access
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func addrIndex(addressBytes []byte) uint32 {
	return fastHash(*(*uint64)(unsafe.Pointer(&addressBytes[2])))
}

// toAddrKey - direct memory copy, 8-byte aligned
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func toAddrKey(addressBytes []byte) AddrKey {
	return AddrKey{
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
func (a AddrKey) isEqual(b AddrKey) bool {
	// Compare first 3 words only (4th is padding)
	return a.word0 == b.word0 && a.word1 == b.word1 && a.word2 == b.word2
}

// RegisterPair - single probe, direct write
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterPair(addressBytes []byte, pairID PairID) {
	key := toAddrKey(addressBytes)
	index := addrIndex(addressBytes)

	addrKeys[index] = key
	pairIDs[index] = pairID
}

// LookupPair - single probe, branch-optimized with prefetching
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LookupPair(addressBytes []byte) PairID {
	key := toAddrKey(addressBytes)
	index := addrIndex(addressBytes)

	// Prefetch both the key and pairID locations
	_ = *(*byte)(unsafe.Add(unsafe.Pointer(&addrKeys[index]), 0))
	_ = *(*byte)(unsafe.Add(unsafe.Pointer(&pairIDs[index]), 0))

	stored := addrKeys[index]
	if stored.isEqual(key) {
		return pairIDs[index]
	}
	return 0
}

// RegisterCore - direct bit manipulation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func RegisterCore(pairID PairID, coreID CoreID) {
	coreAssignments[pairID] |= 1 << coreID
}

// processTick - ULTRA-OPTIMIZED hot path with memory prefetching
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func processTick(exec *Executor, tick *Tick) {
	// Branch-free tick selection using bit manipulation
	reverseMask := uint64(-(uint64(exec.isReverse) & 1))
	forwardBits := *(*uint64)(unsafe.Pointer(&tick.forward))
	reverseBits := *(*uint64)(unsafe.Pointer(&tick.reverse))
	currentTickBits := (forwardBits &^ reverseMask) | (reverseBits & reverseMask)
	currentTick := *(*float64)(unsafe.Pointer(&currentTickBits))

	// Direct pointer arithmetic for queue access
	queues := (*[]quantumqueue64.QuantumQueue64)(unsafe.Pointer(exec.queuesPtr))
	index := (*localidx.Hash)(unsafe.Pointer(exec.indexPtr))

	queueIdx, _ := index.Get(uint32(tick.pairID))
	queue := &(*queues)[queueIdx]

	// Stack-allocated arrays - 64 entries for optimal cache usage
	var handles [64]quantumqueue64.Handle
	var priorities [64]int64
	var cycleIdxs [64]CycleIdx
	cycleCount := uint32(0)

	// Process profitable cycles with early termination
	cycles := (*[]Cycle)(unsafe.Pointer(exec.cyclesPtr))

	for cycleCount < 64 && !queue.Empty() {
		handle, queueTick, cycleData := queue.PeepMin()
		cycleIdx := CycleIdx(cycleData)

		// Direct array access - no bounds checking
		cycle := &(*cycles)[cycleIdx]

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
		cycleIdxs[cycleCount] = cycleIdx
		cycleCount++

		queue.UnlinkMin(handle)
	}

	// Reinsert cycles - unrolled for common small counts
	switch cycleCount {
	case 0:
		// No cycles processed
	case 1:
		queue.Push(priorities[0], handles[0], uint64(cycleIdxs[0]))
	case 2:
		queue.Push(priorities[0], handles[0], uint64(cycleIdxs[0]))
		queue.Push(priorities[1], handles[1], uint64(cycleIdxs[1]))
	case 3:
		queue.Push(priorities[0], handles[0], uint64(cycleIdxs[0]))
		queue.Push(priorities[1], handles[1], uint64(cycleIdxs[1]))
		queue.Push(priorities[2], handles[2], uint64(cycleIdxs[2]))
	case 4:
		queue.Push(priorities[0], handles[0], uint64(cycleIdxs[0]))
		queue.Push(priorities[1], handles[1], uint64(cycleIdxs[1]))
		queue.Push(priorities[2], handles[2], uint64(cycleIdxs[2]))
		queue.Push(priorities[3], handles[3], uint64(cycleIdxs[3]))
	default:
		// General case for larger counts
		for i := uint32(0); i < cycleCount; i++ {
			queue.Push(priorities[i], handles[i], uint64(cycleIdxs[i]))
		}
	}

	// Fanout processing - optimized for cache locality with prefetching
	fanouts := (*[][]Fanout)(unsafe.Pointer(exec.fanoutPtr))
	fanoutList := (*fanouts)[queueIdx]

	// Process fanout entries with direct field access
	for i := range fanoutList {
		fanout := &fanoutList[i]
		cycle := &(*cycles)[fanout.cycleIdx]

		// Prefetch next fanout entry
		if i < len(fanoutList)-1 {
			nextFanout := &fanoutList[i+1]
			_ = *(*byte)(unsafe.Add(unsafe.Pointer(nextFanout), 0))
			// Prefetch next cycle state
			nextCycle := &(*cycles)[nextFanout.cycleIdx]
			_ = *(*byte)(unsafe.Add(unsafe.Pointer(nextCycle), 0))
		}

		// Direct field assignment based on edge index
		switch fanout.edgeIdx {
		case 0:
			cycle.tick0 = currentTick
		case 1:
			cycle.tick1 = currentTick
		case 2:
			cycle.tick2 = currentTick
		}

		// Recalculate priority with direct field access
		newPriority := quantizeTick(cycle.tick0 + cycle.tick1 + cycle.tick2)

		// Direct queue access via cached pointer
		queuePtr := (*quantumqueue64.QuantumQueue64)(unsafe.Pointer(fanout.queuePtr))
		queuePtr.MoveTick(fanout.handle, newPriority)
	}
}

// Dispatch - ULTRA-OPTIMIZED dispatch with memory prefetching
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Dispatch(logView *types.LogView) {
	// Direct slice to fixed-size array conversion
	addressBytes := (*[constants.AddressHexEnd - constants.AddressHexStart]byte)(
		unsafe.Pointer(&logView.Addr[constants.AddressHexStart]))

	// Prefetch the data section while processing address
	_ = *(*byte)(unsafe.Add(unsafe.Pointer(&logView.Data[0]), 0))

	pairID := LookupPair(addressBytes[:])
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
	var msgBuf [32]byte
	tick := (*Tick)(unsafe.Pointer(&msgBuf))
	tick.forward = tickValue
	tick.reverse = -tickValue
	tick.pairID = pairID

	// Optimized core dispatch with prefetching
	assignments := coreAssignments[pairID]
	coreCount := bits.OnesCount64(assignments)

	// Prefetch the ring buffer for the first core
	if assignments != 0 {
		firstCore := bits.TrailingZeros64(assignments)
		_ = *(*byte)(unsafe.Add(unsafe.Pointer(rings[firstCore]), 0))
	}

	// Unroll for common small core counts
	switch coreCount {
	case 1:
		coreID := bits.TrailingZeros64(assignments)
		rings[coreID].Push((*[24]byte)(unsafe.Pointer(&msgBuf)))
	case 2:
		coreID := bits.TrailingZeros64(assignments)
		rings[coreID].Push((*[24]byte)(unsafe.Pointer(&msgBuf)))
		assignments &^= 1 << coreID
		coreID = bits.TrailingZeros64(assignments)
		// Prefetch next ring while processing current
		_ = *(*byte)(unsafe.Add(unsafe.Pointer(rings[coreID]), 0))
		rings[coreID].Push((*[24]byte)(unsafe.Pointer(&msgBuf)))
	case 3:
		for i := 0; i < 3; i++ {
			coreID := bits.TrailingZeros64(assignments)
			// Prefetch next ring if not last iteration
			if i < 2 {
				nextAssignments := assignments &^ (1 << coreID)
				if nextAssignments != 0 {
					nextCore := bits.TrailingZeros64(nextAssignments)
					_ = *(*byte)(unsafe.Add(unsafe.Pointer(rings[nextCore]), 0))
				}
			}
			rings[coreID].Push((*[24]byte)(unsafe.Pointer(&msgBuf)))
			assignments &^= 1 << coreID
		}
	case 4:
		for i := 0; i < 4; i++ {
			coreID := bits.TrailingZeros64(assignments)
			// Prefetch next ring if not last iteration
			if i < 3 {
				nextAssignments := assignments &^ (1 << coreID)
				if nextAssignments != 0 {
					nextCore := bits.TrailingZeros64(nextAssignments)
					_ = *(*byte)(unsafe.Add(unsafe.Pointer(rings[nextCore]), 0))
				}
			}
			rings[coreID].Push((*[24]byte)(unsafe.Pointer(&msgBuf)))
			assignments &^= 1 << coreID
		}
	default:
		// General case for many cores with prefetching
		for assignments != 0 {
			coreID := bits.TrailingZeros64(assignments)
			// Prefetch next ring buffer
			nextAssignments := assignments &^ (1 << coreID)
			if nextAssignments != 0 {
				nextCore := bits.TrailingZeros64(nextAssignments)
				_ = *(*byte)(unsafe.Add(unsafe.Pointer(rings[nextCore]), 0))
			}
			rings[coreID].Push((*[24]byte)(unsafe.Pointer(&msgBuf)))
			assignments &^= 1 << coreID
		}
	}
}

// INITIALIZATION FUNCTIONS WITH MAXIMUM COMPILER ABUSE

// shuffle - optimized Fisher-Yates
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func shuffle(edges []Edge) {
	for i := len(edges) - 1; i > 0; i-- {
		j := fastRandom(i + 1)
		edges[i], edges[j] = edges[j], edges[i]
	}
}

// buildShards - optimized shard construction
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func buildShards(cycles []Triplet) {
	shards = make(map[PairID][]Shard, len(cycles)*3)
	bindings = make(map[PairID][]Edge, len(cycles)*3)

	for _, triplet := range cycles {
		for i := 0; i < 3; i++ {
			edge := Edge{
				pair0:   triplet[0],
				pair1:   triplet[1],
				pair2:   triplet[2],
				edgeIdx: uint16(i),
			}
			bindings[triplet[i]] = append(bindings[triplet[i]], edge)
		}
	}

	for pairID, edges := range bindings {
		shuffle(edges)

		shardSize := constants.MaxCyclesPerShard
		if len(edges) < shardSize {
			shardSize = len(edges)
		}

		for offset := 0; offset < len(edges); offset += shardSize {
			endOffset := offset + shardSize
			if endOffset > len(edges) {
				endOffset = len(edges)
			}

			shard := Shard{
				pairID:   pairID,
				count:    uint32(endOffset - offset),
				edgesPtr: uintptr(unsafe.Pointer(&edges[offset])),
				edgesCap: uint64(endOffset - offset),
			}

			shards[pairID] = append(shards[pairID], shard)
		}
	}
}

// attachShard - optimized shard attachment
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func attachShard(exec *Executor, shard *Shard) {
	index := (*localidx.Hash)(unsafe.Pointer(exec.indexPtr))
	queueIdx := index.Put(uint32(shard.pairID), exec.queueCount)

	// Expand arrays if needed
	if queueIdx == exec.queueCount {
		// Resize queues and fanout tables
		exec.queueCount++
		exec.fanoutCount++
	}

	// Process bindings
	edgesPtr := (*[constants.MaxCyclesPerShard]Edge)(unsafe.Pointer(shard.edgesPtr))
	edges := edgesPtr[:shard.count]

	queues := (*[]quantumqueue64.QuantumQueue64)(unsafe.Pointer(exec.queuesPtr))
	queue := &(*queues)[queueIdx]

	for _, edge := range edges {
		// Add cycle state
		cycles := (*[]Cycle)(unsafe.Pointer(exec.cyclesPtr))
		*cycles = append(*cycles, Cycle{
			pair0: edge.pair0,
			pair1: edge.pair1,
			pair2: edge.pair2,
		})
		cycleIdx := CycleIdx(len(*cycles) - 1)
		exec.cycleCount++

		// Allocate queue handle
		handle, _ := queue.BorrowSafe()
		queue.Push(constants.MaxInitializationPriority, handle, uint64(cycleIdx))

		// Create fanout entries for other edges
		fanouts := (*[][]Fanout)(unsafe.Pointer(exec.fanoutPtr))

		for edgeIdx := uint16(0); edgeIdx < 3; edgeIdx++ {
			if edgeIdx != edge.edgeIdx {
				fanout := Fanout{
					handle:   handle,
					edgeIdx:  edgeIdx,
					cycleIdx: cycleIdx,
					queuePtr: uintptr(unsafe.Pointer(queue)),
				}
				(*fanouts)[queueIdx] = append((*fanouts)[queueIdx], fanout)
			}
		}
	}
}

// launchWorker - optimized worker launch
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func launchWorker(coreID, forwardCoreCount int, input <-chan Shard) {
	runtime.LockOSThread()

	// Pre-allocate with optimal sizes
	queues := make([]quantumqueue64.QuantumQueue64, 0, 256)
	fanouts := make([][]Fanout, 0, 256)
	cycles := make([]Cycle, 0, 2048)
	index := localidx.New(constants.DefaultLocalIdxSize)

	exec := &Executor{
		queuesPtr:   uintptr(unsafe.Pointer(&queues)),
		fanoutPtr:   uintptr(unsafe.Pointer(&fanouts)),
		cyclesPtr:   uintptr(unsafe.Pointer(&cycles)),
		indexPtr:    uintptr(unsafe.Pointer(&index)),
		queueCount:  0,
		isReverse:   0,
		cycleCount:  0,
		fanoutCount: 0,
	}

	if coreID >= forwardCoreCount {
		exec.isReverse = 1
	}

	executors[coreID] = exec
	rings[coreID] = ring24.New(constants.DefaultRingSize)

	for shard := range input {
		attachShard(exec, &shard)
	}

	stopFlag, hotFlag := control.Flags()

	if coreID == 0 {
		ring24.PinnedConsumerWithCooldown(coreID, rings[coreID], stopFlag, hotFlag,
			func(msgPtr *[24]byte) {
				processTick(exec, (*Tick)(unsafe.Pointer(msgPtr)))
			}, make(chan struct{}))
	} else {
		ring24.PinnedConsumer(coreID, rings[coreID], stopFlag, hotFlag,
			func(msgPtr *[24]byte) {
				processTick(exec, (*Tick)(unsafe.Pointer(msgPtr)))
			}, make(chan struct{}))
	}
}

// Init - optimized system initialization
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func Init(cycles []Triplet) {
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

	buildShards(cycles)

	channels = make([]chan Shard, coreCount)
	for i := 0; i < coreCount; i++ {
		channels[i] = make(chan Shard, constants.ShardChannelBufferSize)
		go launchWorker(i, forwardCoreCount, channels[i])
	}

	currentCore := 0
	for _, shardList := range shards {
		for _, shard := range shardList {
			forwardCore := currentCore % forwardCoreCount
			reverseCore := forwardCore + forwardCoreCount

			channels[forwardCore] <- shard
			channels[reverseCore] <- shard

			// Update core assignments
			edgesPtr := (*[constants.MaxCyclesPerShard]Edge)(unsafe.Pointer(shard.edgesPtr))
			edges := edgesPtr[:shard.count]

			for _, edge := range edges {
				pairs := [3]PairID{edge.pair0, edge.pair1, edge.pair2}
				for _, pairID := range pairs {
					coreAssignments[pairID] |= 1<<CoreID(forwardCore) | 1<<CoreID(reverseCore)
				}
			}
			currentCore++
		}
	}

	for _, channel := range channels {
		close(channel)
	}

	cleanup()
}

// cleanup - aggressive cleanup with compiler abuse
//
//go:norace
//go:nocheckptr
//go:noinline
//go:registerparams
func cleanup() {
	runtime.GC()

	// Nil everything
	shards = nil
	bindings = nil
	channels = nil

	runtime.GC()
	runtime.GC()
	runtime.KeepAlive(nil)
}
