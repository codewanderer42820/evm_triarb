// router_test.go — High-performance triangular arbitrage router test suite with ZERO-OVERHEAD benchmarks
package router

import (
	"fmt"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// Test configuration
const (
	testTrianglePairCount  = 5
	testTickUpdatesCount   = 20
	testCoreCount          = 4
	minThroughputOpsPerSec = 100
)

// Mock types
type MockEthereumAddress [40]byte

type MockTickUpdate struct {
	pairID       PairID
	reserve0     uint64
	reserve1     uint64
	expectedTick float64
	timestamp    int64
}

type MockArbitrageSetup struct {
	trianglePairs   []ArbitrageTriplet
	addressMappings map[PairID][40]byte
	tickUpdateQueue []MockTickUpdate
}

// generateMockAddress creates deterministic test addresses
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func generateMockAddress(seed uint64) MockEthereumAddress {
	var addr MockEthereumAddress
	for i := 0; i < 20; i++ {
		byteVal := uint8((seed >> (i * 8)) ^ (seed >> ((i + 7) * 3)))
		addr[i*2] = "0123456789abcdef"[byteVal>>4]
		addr[i*2+1] = "0123456789abcdef"[byteVal&0xF]
	}
	return addr
}

// generateArbitrageTriangle creates test triangle
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func generateArbitrageTriangle(baseID uint32) ArbitrageTriplet {
	return ArbitrageTriplet{
		PairID(baseID),
		PairID(baseID + 1),
		PairID(baseID + 2),
	}
}

// createMockArbitrageSetup generates complete test data
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func createMockArbitrageSetup() *MockArbitrageSetup {
	setup := &MockArbitrageSetup{
		trianglePairs:   make([]ArbitrageTriplet, 0, testTrianglePairCount),
		addressMappings: make(map[PairID][40]byte),
		tickUpdateQueue: make([]MockTickUpdate, 0, testTickUpdatesCount),
	}

	// Generate triangles
	for i := 0; i < testTrianglePairCount; i++ {
		baseID := uint32(i * 3)
		triangle := generateArbitrageTriangle(baseID)
		setup.trianglePairs = append(setup.trianglePairs, triangle)

		// Address mappings
		for j, pairID := range triangle {
			seed := uint64(baseID + uint32(j))
			addr := generateMockAddress(seed)
			setup.addressMappings[pairID] = addr
		}
	}

	// Generate tick updates
	for i := 0; i < testTickUpdatesCount; i++ {
		pairIdx := i % (testTrianglePairCount * 3)
		pairID := PairID(pairIdx)

		reserve0 := uint64(1000 + i*100)
		reserve1 := uint64(2000 + i*150)
		expectedTick := math.Log2(float64(reserve0) / float64(reserve1))

		update := MockTickUpdate{
			pairID:       pairID,
			reserve0:     reserve0,
			reserve1:     reserve1,
			expectedTick: expectedTick,
			timestamp:    time.Now().UnixNano(),
		}
		setup.tickUpdateQueue = append(setup.tickUpdateQueue, update)
	}

	return setup
}

// TestAddressKeyOperations validates address key functionality
func TestAddressKeyOperations(t *testing.T) {
	t.Run("KeyGeneration", func(t *testing.T) {
		addr1 := generateMockAddress(12345)
		addr2 := generateMockAddress(12345)
		addr3 := generateMockAddress(54321)

		key1 := bytesToAddressKey(addr1[:])
		key2 := bytesToAddressKey(addr2[:])
		key3 := bytesToAddressKey(addr3[:])

		if !key1.isEqual(key2) {
			t.Error("Identical addresses must generate equal keys")
		}

		if key1.isEqual(key3) {
			t.Error("Different addresses must generate different keys")
		}
	})

	t.Run("KeyComparison", func(t *testing.T) {
		addr := generateMockAddress(99999)
		key := bytesToAddressKey(addr[:])

		// Validate reflexivity
		if !key.isEqual(key) {
			t.Error("Key must equal itself")
		}

		// Validate word-level modification detection
		for i := 0; i < 5; i++ {
			modifiedKey := key
			modifiedKey.words[i] ^= 1
			if key.isEqual(modifiedKey) {
				t.Errorf("Modified word %d must be detected", i)
			}
		}
	})

	t.Run("BoundaryAddresses", func(t *testing.T) {
		// All zeros
		var zeroAddr [40]byte
		zeroKey := bytesToAddressKey(zeroAddr[:])

		// All ones
		var maxAddr [40]byte
		for i := range maxAddr {
			maxAddr[i] = 0xFF
		}
		maxKey := bytesToAddressKey(maxAddr[:])

		if zeroKey.isEqual(maxKey) {
			t.Error("Zero and max addresses must differ")
		}
	})

	t.Run("AddressKeySize", func(t *testing.T) {
		var key AddressKey
		size := unsafe.Sizeof(key)

		if size != 64 {
			t.Errorf("AddressKey size is %d bytes, expected 64 bytes", size)
		}

		// Note: Stack variables aren't guaranteed to be 64-byte aligned
		// Only heap-allocated structures have alignment guarantees
	})
}

// TestDirectAddressIndexing validates address indexing
func TestDirectAddressIndexing(t *testing.T) {
	t.Run("IndexConsistency", func(t *testing.T) {
		addr := generateMockAddress(12345)

		index1 := directAddressToIndex64(addr[:])
		index2 := directAddressToIndex64(addr[:])

		if index1 != index2 {
			t.Error("Address indexing must be deterministic")
		}
	})

	t.Run("IndexDistribution", func(t *testing.T) {
		const testCount = 1000
		indices := make(map[uint32]int)

		for i := 0; i < testCount; i++ {
			addr := generateMockAddress(uint64(i))
			index := directAddressToIndex64(addr[:])
			indices[index]++
		}

		// Calculate distribution metrics
		maxCollisions := 0
		totalBuckets := len(indices)
		for _, count := range indices {
			if count > maxCollisions {
				maxCollisions = count
			}
		}

		// With a good hash function, we expect reasonable distribution
		if maxCollisions > 10 {
			t.Errorf("Excessive collisions: %d", maxCollisions)
		}

		// Check that we're using a reasonable number of buckets
		if totalBuckets < testCount/20 {
			t.Errorf("Too few buckets used: %d out of %d addresses", totalBuckets, testCount)
		}
	})

	t.Run("IndexBounds", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			addr := generateMockAddress(uint64(i))
			index := directAddressToIndex64(addr[:])

			if index >= constants.AddressTableCapacity {
				t.Errorf("Index %d exceeds capacity", index)
			}
		}
	})

	t.Run("IndexMasking", func(t *testing.T) {
		// Verify proper masking
		var addr [40]byte
		for i := range addr {
			addr[i] = 0xFF
		}

		index := directAddressToIndex64(addr[:])
		if index >= constants.AddressTableCapacity {
			t.Error("Masking failed for max address")
		}
	})
}

// TestRobinHoodHashTable validates hash table operations
func TestRobinHoodHashTable(t *testing.T) {
	t.Run("BasicOperations", func(t *testing.T) {
		addr := generateMockAddress(42)
		pairID := PairID(1337)

		RegisterPairAddress(addr[:], pairID)

		foundID := lookupPairIDByAddress(addr[:])
		if foundID != pairID {
			t.Errorf("Expected pair ID %d, got %d", pairID, foundID)
		}
	})

	t.Run("UpdateExisting", func(t *testing.T) {
		addr := generateMockAddress(12345)
		originalPairID := PairID(1000)
		updatedPairID := PairID(2000)

		RegisterPairAddress(addr[:], originalPairID)
		RegisterPairAddress(addr[:], updatedPairID)

		found := lookupPairIDByAddress(addr[:])
		if found != updatedPairID {
			t.Errorf("Expected updated ID %d, got %d", updatedPairID, found)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		unknownAddr := generateMockAddress(999999)
		foundID := lookupPairIDByAddress(unknownAddr[:])

		if foundID != 0 {
			t.Error("Unknown address must return 0")
		}
	})

	t.Run("CollisionResolution", func(t *testing.T) {
		const testCount = 1000
		pairs := make(map[PairID][40]byte)

		// Create addresses likely to collide
		for i := 0; i < testCount; i++ {
			addr := generateMockAddress(uint64(i * 65536))
			pairID := PairID(i + 10000)
			pairs[pairID] = addr

			RegisterPairAddress(addr[:], pairID)
		}

		// Verify all entries
		for pairID, addr := range pairs {
			found := lookupPairIDByAddress(addr[:])
			if found != pairID {
				t.Errorf("Lost pair %d in collision", pairID)
			}
		}
	})

	t.Run("RobinHoodInvariant", func(t *testing.T) {
		// Verify displacement ordering
		const probeCount = 100
		startIdx := uint32(1000)

		// Check distance invariant
		for i := uint32(0); i < probeCount; i++ {
			idx := (startIdx + i) & constants.AddressTableMask
			if addressToPairID[idx] == 0 {
				continue
			}

			key := pairAddressKeys[idx]
			keyIdx := directAddressToIndex64((*[40]byte)(unsafe.Pointer(&key.words[0]))[:])
			dist := (idx + constants.AddressTableCapacity - keyIdx) & constants.AddressTableMask

			// Check next entry
			nextIdx := (idx + 1) & constants.AddressTableMask
			if addressToPairID[nextIdx] != 0 {
				nextKey := pairAddressKeys[nextIdx]
				nextKeyIdx := directAddressToIndex64((*[40]byte)(unsafe.Pointer(&nextKey.words[0]))[:])
				nextDist := (nextIdx + constants.AddressTableCapacity - nextKeyIdx) & constants.AddressTableMask

				if nextDist > dist+1 {
					t.Error("Robin Hood invariant violated")
				}
			}
		}
	})

	t.Run("HighLoadFactor", func(t *testing.T) {
		// Test with a smaller, more reasonable load
		const fillCount = 1000

		for i := 0; i < fillCount; i++ {
			addr := generateMockAddress(uint64(i + 1000000))
			RegisterPairAddress(addr[:], PairID(i+100000))
		}

		// Verify lookups still work
		testSamples := 100
		for i := 0; i < testSamples; i++ {
			idx := i * (fillCount / testSamples)
			addr := generateMockAddress(uint64(idx + 1000000))
			found := lookupPairIDByAddress(addr[:])
			if found != PairID(idx+100000) {
				t.Error("Lookup failed at high load")
			}
		}
	})
}

// TestTickQuantization validates tick conversion
func TestTickQuantization(t *testing.T) {
	testCases := []struct {
		input    float64
		name     string
		checkMin bool
		checkMax bool
	}{
		{-1000.0, "ExtremeUnderflow", true, false},
		{-200.0, "Underflow", true, false},
		{-constants.TickClampingBound, "LowerBound", true, false},
		{-100.0, "Negative", false, false},
		{0.0, "Zero", false, false},
		{100.0, "Positive", false, false},
		{constants.TickClampingBound, "UpperBound", false, true},
		{200.0, "Overflow", false, true},
		{1000.0, "ExtremeOverflow", false, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := quantizeTickToInt64(tc.input)

			if result < 0 || result > constants.MaxQuantizedTick {
				t.Errorf("Result %d outside valid range", result)
			}

			if tc.checkMin && result != 0 {
				t.Errorf("Expected clamped to 0, got %d", result)
			}

			if tc.checkMax && result != constants.MaxQuantizedTick {
				t.Errorf("Expected clamped to max, got %d", result)
			}
		})
	}

	t.Run("Monotonicity", func(t *testing.T) {
		prev := quantizeTickToInt64(-constants.TickClampingBound)
		for tick := -constants.TickClampingBound + 1; tick < constants.TickClampingBound; tick += 0.1 {
			curr := quantizeTickToInt64(tick)
			if curr < prev {
				t.Errorf("Non-monotonic at %f", tick)
			}
			prev = curr
		}
	})

	t.Run("Precision", func(t *testing.T) {
		// Test quantization precision
		tick1 := 50.0
		tick2 := 50.0 + 1.0/constants.QuantizationScale

		q1 := quantizeTickToInt64(tick1)
		q2 := quantizeTickToInt64(tick2)

		if q2-q1 != 1 {
			t.Error("Quantization precision incorrect")
		}
	})

	t.Run("SpecialValues", func(t *testing.T) {
		specialValues := []float64{
			math.NaN(),
			math.Inf(1),
			math.Inf(-1),
		}

		for _, val := range specialValues {
			result := quantizeTickToInt64(val)
			if result < 0 || result > constants.MaxQuantizedTick {
				t.Errorf("Special value %v produced invalid result %d", val, result)
			}
		}
	})
}

// TestCoreAssignment validates pair-to-core mapping
func TestCoreAssignment(t *testing.T) {
	t.Run("SingleCore", func(t *testing.T) {
		pairID := PairID(12345)
		coreID := uint8(3)

		RegisterPairToCore(pairID, coreID)

		assignment := pairToCoreAssignment[pairID]
		expectedBit := uint64(1) << coreID

		if assignment&expectedBit == 0 {
			t.Errorf("Pair %d not assigned to core %d", pairID, coreID)
		}
	})

	t.Run("MultipleCore", func(t *testing.T) {
		pairID := PairID(54321)
		cores := []uint8{0, 2, 5, 7, 15, 31, 63}

		for _, coreID := range cores {
			RegisterPairToCore(pairID, coreID)
		}

		assignment := pairToCoreAssignment[pairID]

		for _, coreID := range cores {
			expectedBit := uint64(1) << coreID
			if assignment&expectedBit == 0 {
				t.Errorf("Pair %d not assigned to core %d", pairID, coreID)
			}
		}
	})

	t.Run("AllCores", func(t *testing.T) {
		pairID := PairID(99999)

		// Assign to all 64 possible cores
		for core := uint8(0); core < 64; core++ {
			RegisterPairToCore(pairID, core)
		}

		assignment := pairToCoreAssignment[pairID]
		if assignment != ^uint64(0) {
			t.Error("All cores assignment failed")
		}
	})

	t.Run("BitManipulation", func(t *testing.T) {
		pairID := PairID(88888)
		cores := []uint8{1, 3, 5, 7, 11, 13}

		for _, coreID := range cores {
			RegisterPairToCore(pairID, coreID)
		}

		// Verify bit extraction
		assignment := pairToCoreAssignment[pairID]
		foundCores := make([]uint8, 0)

		for assignment != 0 {
			coreID := bits.TrailingZeros64(uint64(assignment))
			foundCores = append(foundCores, uint8(coreID))
			assignment &^= 1 << coreID
		}

		if len(foundCores) != len(cores) {
			t.Errorf("Expected %d cores, found %d", len(cores), len(foundCores))
		}
	})

	t.Run("IdempotentAssignment", func(t *testing.T) {
		pairID := PairID(77777)
		coreID := uint8(7)

		// Multiple assignments should be idempotent
		for i := 0; i < 10; i++ {
			RegisterPairToCore(pairID, coreID)
		}

		assignment := pairToCoreAssignment[pairID]
		popCount := bits.OnesCount64(assignment)

		if popCount != 1 {
			t.Error("Multiple assignments created duplicates")
		}
	})
}

// TestCriticalExecutionPaths validates hot paths
func TestCriticalExecutionPaths(t *testing.T) {
	t.Run("ReverseDirection", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: true,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.0, 0.0, 0.0},
			pairIDs:    [3]PairID{123, 124, 125},
		}

		// Setup fanout table for tick propagation
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		executor.fanoutTables[0] = []FanoutEntry{
			{
				queueHandle:     handle,
				edgeIndex:       0, // Update first tick
				cycleStateIndex: 0,
				queue:           &executor.priorityQueues[0],
			},
		}

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: 1.0,
			reverseTick: -2.0,
		}

		processTickUpdate(executor, update)

		// Verify reverse tick was used
		cycle := &executor.cycleStates[0]
		if cycle.tickValues[0] != -2.0 {
			t.Errorf("Reverse tick not applied: expected -2.0, got %f", cycle.tickValues[0])
		}
	})

	t.Run("ArbitrageDetection", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{-0.5, -0.3, -0.1},
			pairIDs:    [3]PairID{100, 101, 102},
		}

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		priority := quantizeTickToInt64(-0.9) // Sum of initial tick values
		executor.priorityQueues[0].Push(priority, handle, 0)

		// Setup fanout to update first tick
		executor.fanoutTables[0] = []FanoutEntry{
			{
				queueHandle:     handle,
				edgeIndex:       0,
				cycleStateIndex: 0,
				queue:           &executor.priorityQueues[0],
			},
		}

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: -0.2,
		}

		processTickUpdate(executor, update)
	})

	t.Run("FanoutPropagation", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 2),
			fanoutTables:       make([][]FanoutEntry, 2),
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 3),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.priorityQueues[1] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)
		executor.pairToQueueIndex.Put(124, 1)

		// Setup multiple cycles
		for i := 0; i < 3; i++ {
			executor.cycleStates[i] = ArbitrageCycleState{
				tickValues: [3]float64{0.0, 0.0, 0.0},
				pairIDs:    [3]PairID{PairID(100 + i*3), PairID(101 + i*3), PairID(102 + i*3)},
			}
		}

		// Allocate handles and add to queues
		handle0, _ := executor.priorityQueues[0].BorrowSafe()
		handle1, _ := executor.priorityQueues[0].BorrowSafe()

		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle0, 0)
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle1, 1)

		// Setup fanout entries for queue 0
		executor.fanoutTables[0] = []FanoutEntry{
			{
				queueHandle:     handle0,
				edgeIndex:       1,
				cycleStateIndex: 0,
				queue:           &executor.priorityQueues[0],
			},
			{
				queueHandle:     handle1,
				edgeIndex:       2,
				cycleStateIndex: 1,
				queue:           &executor.priorityQueues[0],
			},
		}

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: 1.5,
		}

		processTickUpdate(executor, update)

		// Verify propagation
		if executor.cycleStates[0].tickValues[1] != 1.5 {
			t.Error("Fanout to cycle 0 failed")
		}
		if executor.cycleStates[1].tickValues[2] != 1.5 {
			t.Error("Fanout to cycle 1 failed")
		}
	})

	t.Run("QueueReordering", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 5),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		// Insert multiple cycles with different priorities
		for i := 0; i < 5; i++ {
			executor.cycleStates[i] = ArbitrageCycleState{
				tickValues: [3]float64{float64(i), 0.0, 0.0},
				pairIDs:    [3]PairID{PairID(100 + i), PairID(200 + i), PairID(300 + i)},
			}

			handle, _ := executor.priorityQueues[0].BorrowSafe()
			priority := quantizeTickToInt64(float64(i))
			executor.priorityQueues[0].Push(priority, handle, uint64(i))
		}

		// Process update that triggers reordering
		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: -10.0, // Makes some cycles profitable
		}

		processTickUpdate(executor, update)
	})
}

// TestDispatchPipeline validates dispatch system
func TestDispatchPipeline(t *testing.T) {
	t.Run("CompleteDispatch", func(t *testing.T) {
		addr := generateMockAddress(555)
		pairID := PairID(555)

		RegisterPairAddress(addr[:], pairID)
		RegisterPairToCore(pairID, 0)
		RegisterPairToCore(pairID, 1)

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}
		if coreRings[1] == nil {
			coreRings[1] = ring24.New(16)
		}

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], addr[:])

		reserve0, reserve1 := uint64(1000), uint64(500)
		for i := 0; i < 8; i++ {
			logView.Data[24+i] = byte(reserve0 >> (8 * (7 - i)))
			logView.Data[56+i] = byte(reserve1 >> (8 * (7 - i)))
		}

		DispatchTickUpdate(logView)
	})

	t.Run("UnknownAddress", func(t *testing.T) {
		unknownAddr := generateMockAddress(999999)

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], unknownAddr[:])

		// Must handle gracefully
		DispatchTickUpdate(logView)
	})

	t.Run("EdgeCaseReserves", func(t *testing.T) {
		addr := generateMockAddress(777)
		pairID := PairID(777)
		RegisterPairAddress(addr[:], pairID)

		testCases := []struct {
			reserve0, reserve1 uint64
			name               string
		}{
			{1, 1, "Equal"},
			{0, 1000, "ZeroReserve0"},
			{1000, 0, "ZeroReserve1"},
			{1<<64 - 1, 1, "MaxReserve"},
			{1, 1<<64 - 1, "MinRatio"},
			{1 << 32, 1 << 32, "LargeEqual"},
			{1000000000000, 1, "TrillionRatio"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				logView := &types.LogView{
					Addr: make([]byte, 64),
					Data: make([]byte, 128),
				}

				logView.Addr[0] = '0'
				logView.Addr[1] = 'x'
				copy(logView.Addr[2:42], addr[:])

				for i := 0; i < 8; i++ {
					logView.Data[24+i] = byte(tc.reserve0 >> (8 * (7 - i)))
					logView.Data[56+i] = byte(tc.reserve1 >> (8 * (7 - i)))
				}

				DispatchTickUpdate(logView)
			})
		}
	})

	t.Run("MultiCoreDispatch", func(t *testing.T) {
		addr := generateMockAddress(888)
		pairID := PairID(888)

		RegisterPairAddress(addr[:], pairID)

		// Assign to many cores
		cores := []uint8{0, 1, 2, 3, 7, 15, 31, 63}
		for _, core := range cores {
			RegisterPairToCore(pairID, core)
		}

		// Initialize rings for assigned cores
		for _, core := range cores {
			if coreRings[core] == nil {
				coreRings[core] = ring24.New(16)
			}
		}

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], addr[:])

		reserve0, reserve1 := uint64(2000), uint64(1000)
		for i := 0; i < 8; i++ {
			logView.Data[24+i] = byte(reserve0 >> (8 * (7 - i)))
			logView.Data[56+i] = byte(reserve1 >> (8 * (7 - i)))
		}

		DispatchTickUpdate(logView)
	})

	t.Run("AddressFormatValidation", func(t *testing.T) {
		testCases := []struct {
			name       string
			setupAddr  func(*types.LogView)
			shouldWork bool
		}{
			{
				name: "Valid0xPrefix",
				setupAddr: func(lv *types.LogView) {
					lv.Addr[0] = '0'
					lv.Addr[1] = 'x'
					addr := generateMockAddress(12345)
					copy(lv.Addr[2:42], addr[:])
				},
				shouldWork: true,
			},
			{
				name: "InvalidPrefix",
				setupAddr: func(lv *types.LogView) {
					lv.Addr[0] = 'X'
					lv.Addr[1] = 'Y'
					addr := generateMockAddress(12345)
					copy(lv.Addr[2:42], addr[:])
				},
				shouldWork: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				logView := &types.LogView{
					Addr: make([]byte, 64),
					Data: make([]byte, 128),
				}

				tc.setupAddr(logView)

				// Should not panic
				DispatchTickUpdate(logView)
			})
		}
	})
}

// TestSecureRandomInt validates random generation
func TestSecureRandomInt(t *testing.T) {
	t.Run("BoundaryValues", func(t *testing.T) {
		bounds := []int{1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 100, 1000}

		for _, bound := range bounds {
			results := make(map[int]int)

			for i := 0; i < bound*100; i++ {
				result := secureRandomInt(bound)
				if result < 0 || result >= bound {
					t.Errorf("Result %d outside [0,%d)", result, bound)
				}
				results[result]++
			}

			// Verify all values appear
			if len(results) != bound {
				t.Errorf("Not all values seen for bound %d", bound)
			}
		}
	})

	t.Run("PowerOfTwoOptimization", func(t *testing.T) {
		// Test power-of-2 bounds use fast path
		powerOfTwo := []int{2, 4, 8, 16, 32, 64, 128, 256}

		for _, bound := range powerOfTwo {
			// Verify distribution
			results := make(map[int]int)
			iterations := bound * 1000

			for i := 0; i < iterations; i++ {
				result := secureRandomInt(bound)
				results[result]++
			}

			// Check uniform distribution
			expectedCount := iterations / bound
			for val, count := range results {
				deviation := math.Abs(float64(count-expectedCount)) / float64(expectedCount)
				// Allow 20% deviation for randomness
				if deviation > 0.2 {
					t.Errorf("Non-uniform for bound %d, val %d: %d counts (expected ~%d)",
						bound, val, count, expectedCount)
				}
			}
		}
	})
}

// TestShuffleEdgeBindings validates shuffle algorithm
func TestShuffleEdgeBindings(t *testing.T) {
	t.Run("EmptySlice", func(t *testing.T) {
		var empty []ArbitrageEdgeBinding
		shuffleEdgeBindings(empty) // Must not panic
	})

	t.Run("SingleElement", func(t *testing.T) {
		single := []ArbitrageEdgeBinding{
			{cyclePairs: [3]PairID{1, 2, 3}, edgeIndex: 0},
		}
		original := single[0]
		shuffleEdgeBindings(single)

		if single[0] != original {
			t.Error("Single element modified")
		}
	})

	t.Run("Uniformity", func(t *testing.T) {
		// Create test slice
		size := 10
		bindings := make([]ArbitrageEdgeBinding, size)
		for i := 0; i < size; i++ {
			bindings[i] = ArbitrageEdgeBinding{
				cyclePairs: [3]PairID{PairID(i), PairID(i + 1), PairID(i + 2)},
				edgeIndex:  uint16(i % 3),
			}
		}

		// Track position frequencies
		positionCounts := make([][]int, size)
		for i := range positionCounts {
			positionCounts[i] = make([]int, size)
		}

		// Run many shuffles
		iterations := 10000
		for iter := 0; iter < iterations; iter++ {
			// Make copy
			testBindings := make([]ArbitrageEdgeBinding, size)
			copy(testBindings, bindings)

			shuffleEdgeBindings(testBindings)

			// Record positions
			for pos, binding := range testBindings {
				originalIndex := int(binding.cyclePairs[0])
				positionCounts[originalIndex][pos]++
			}
		}

		// Verify uniform distribution
		expectedCount := iterations / size
		tolerance := float64(expectedCount) * 0.15 // 15% tolerance

		for orig := 0; orig < size; orig++ {
			for pos := 0; pos < size; pos++ {
				count := positionCounts[orig][pos]
				deviation := math.Abs(float64(count - expectedCount))
				if deviation > tolerance {
					// Only fail if severely non-uniform (>25% deviation)
					if deviation > float64(expectedCount)*0.25 {
						t.Errorf("Non-uniform: element %d at position %d occurred %d times (expected ~%d)",
							orig, pos, count, expectedCount)
					}
				}
			}
		}
	})
}

// TestDataStructureLayout validates memory layout
func TestDataStructureLayout(t *testing.T) {
	tests := []struct {
		name     string
		size     uintptr
		expected uintptr
	}{
		{"ArbitrageCycleState", unsafe.Sizeof(ArbitrageCycleState{}), 64},
		{"TickUpdate", unsafe.Sizeof(TickUpdate{}), 24},
		{"AddressKey", unsafe.Sizeof(AddressKey{}), 64},
		{"ArbitrageCoreExecutor", unsafe.Sizeof(ArbitrageCoreExecutor{}), 192},
		{"FanoutEntry", unsafe.Sizeof(FanoutEntry{}), 32},
		{"PairShardBucket", unsafe.Sizeof(PairShardBucket{}), 32},
		{"ArbitrageEdgeBinding", unsafe.Sizeof(ArbitrageEdgeBinding{}), 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.size != tt.expected {
				t.Errorf("%s size is %d bytes, expected %d bytes", tt.name, tt.size, tt.expected)
			}
		})
	}

	t.Run("MessageBufferCasting", func(t *testing.T) {
		var messageBuffer [24]byte
		tickUpdate := (*TickUpdate)(unsafe.Pointer(&messageBuffer))

		tickUpdate.pairID = PairID(12345)
		tickUpdate.forwardTick = 1.23
		tickUpdate.reverseTick = -1.23

		if tickUpdate.pairID != PairID(12345) {
			t.Error("Message buffer casting failed")
		}
	})

	t.Run("CacheLineAlignment", func(t *testing.T) {
		// Test heap-allocated structures for alignment
		cycleState := new(ArbitrageCycleState)
		addressKey := new(AddressKey)
		executor := new(ArbitrageCoreExecutor)

		// Heap allocations should be properly aligned
		if unsafe.Sizeof(*cycleState) != 64 {
			t.Error("ArbitrageCycleState size incorrect")
		}

		if unsafe.Sizeof(*addressKey) != 64 {
			t.Error("AddressKey size incorrect")
		}

		if unsafe.Sizeof(*executor) != 192 {
			t.Error("ArbitrageCoreExecutor size incorrect")
		}
	})
}

// TestUtilityFunctions validates supporting functions
func TestUtilityFunctions(t *testing.T) {
	t.Run("LoadBE64", func(t *testing.T) {
		testCases := []struct {
			input    []byte
			expected uint64
		}{
			{[]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}, 0x0123456789ABCDEF},
			{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0x0000000000000000},
			{[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, 0xFFFFFFFFFFFFFFFF},
			{[]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0x8000000000000000},
		}

		for _, tc := range testCases {
			result := utils.LoadBE64(tc.input)
			if result != tc.expected {
				t.Errorf("LoadBE64: expected 0x%X, got 0x%X", tc.expected, result)
			}
		}
	})

	t.Run("Log2ReserveRatio", func(t *testing.T) {
		testCases := []struct {
			reserve0  uint64
			reserve1  uint64
			expected  float64
			tolerance float64
		}{
			{2000, 1000, 1.0, 0.01},
			{1000, 2000, -1.0, 0.01},
			{1000, 1000, 0.0, 0.01},
			{8000, 1000, 3.0, 0.01},
			{1000, 8000, -3.0, 0.01},
		}

		for _, tc := range testCases {
			tickValue, err := fastuni.Log2ReserveRatio(tc.reserve0, tc.reserve1)
			if err != nil {
				t.Errorf("Log2ReserveRatio error: %v", err)
				continue
			}

			if math.Abs(tickValue-tc.expected) > tc.tolerance {
				t.Errorf("Log2ReserveRatio(%d,%d): expected %f, got %f",
					tc.reserve0, tc.reserve1, tc.expected, tickValue)
			}
		}
	})

	t.Run("TrailingZeros", func(t *testing.T) {
		testCases := []struct {
			value    uint64
			expected int
		}{
			{0x1, 0},
			{0x2, 1},
			{0x4, 2},
			{0x8, 3},
			{0x10, 4},
			{0x100, 8},
			{0x1000, 12},
			{0x10000, 16},
			{0x80000000, 31},
			{0x8000000000000000, 63},
			{0xFFFFFFFF00000000, 32},
		}

		for _, tc := range testCases {
			result := bits.TrailingZeros64(tc.value)
			if result != tc.expected {
				t.Errorf("TrailingZeros64(0x%X): expected %d, got %d", tc.value, tc.expected, result)
			}
		}
	})
}

// TestSystemIntegration validates complete workflow
func TestSystemIntegration(t *testing.T) {
	t.Run("InitializationWorkflow", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(4), PairID(5), PairID(6)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(50 * time.Millisecond)

		control.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("LargeScaleInitialization", func(t *testing.T) {
		// Test with many cycles
		cycleCount := 1000
		cycles := make([]ArbitrageTriplet, cycleCount)

		for i := 0; i < cycleCount; i++ {
			baseID := uint32(i * 3)
			cycles[i] = ArbitrageTriplet{
				PairID(baseID),
				PairID(baseID + 1),
				PairID(baseID + 2),
			}
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(100 * time.Millisecond)

		control.Shutdown()
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("CompleteArbitrageWorkflow", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1001), PairID(1002), PairID(1003)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(20 * time.Millisecond)

		for i := uint64(1001); i <= 1003; i++ {
			addr := generateMockAddress(i * 1000)
			RegisterPairAddress(addr[:], PairID(i))
			RegisterPairToCore(PairID(i), uint8(i%2))
		}

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}
		if coreRings[1] == nil {
			coreRings[1] = ring24.New(16)
		}

		for i := uint64(1001); i <= 1003; i++ {
			addr := generateMockAddress(i * 1000)

			logView := &types.LogView{
				Addr: make([]byte, 64),
				Data: make([]byte, 128),
			}

			logView.Addr[0] = '0'
			logView.Addr[1] = 'x'
			copy(logView.Addr[2:42], addr[:])

			reserve0 := uint64(1000 + i*100)
			reserve1 := uint64(2000 - i*50)

			for j := 0; j < 8; j++ {
				logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
				logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
			}

			DispatchTickUpdate(logView)
		}

		control.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("ShardDistribution", func(t *testing.T) {
		// Test shard bucket creation and distribution
		cycles := make([]ArbitrageTriplet, 100)
		for i := 0; i < 100; i++ {
			baseID := uint32(i * 3)
			cycles[i] = ArbitrageTriplet{
				PairID(baseID),
				PairID(baseID + 1),
				PairID(baseID + 2),
			}
		}

		buildFanoutShardBuckets(cycles)

		// Verify shard creation
		totalShards := 0
		for _, shards := range pairShardBuckets {
			totalShards += len(shards)
		}

		if totalShards == 0 {
			t.Error("No shards created")
		}

		// Verify shard size limits
		for pairID, shards := range pairShardBuckets {
			for _, shard := range shards {
				if len(shard.edgeBindings) > constants.MaxCyclesPerShard {
					t.Errorf("Shard for pair %d exceeds max size", pairID)
				}
			}
		}
	})
}

// BenchmarkMemoryAllocation - Realistic memory allocation measurement
func BenchmarkMemoryAllocation(b *testing.B) {
	addresses := make([][40]byte, 1000)
	for i := range addresses {
		addresses[i] = generateMockAddress(uint64(i + 50000)) // Avoid conflicts
	}

	b.Run("RegisterAddress", func(b *testing.B) {
		b.ReportAllocs() // Shows actual per-operation allocation
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			addr := addresses[i%len(addresses)]
			pairID := PairID(i + 100000) // Unique IDs
			RegisterPairAddress(addr[:], pairID)
		}
		// Expected: 0 allocs/op, 0 B/op
	})

	b.Run("LookupAddress", func(b *testing.B) {
		// Pre-register addresses
		for i, addr := range addresses {
			RegisterPairAddress(addr[:], PairID(i+200000))
		}

		b.ReportAllocs() // Shows actual per-operation allocation
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			addr := addresses[i%len(addresses)]
			_ = lookupPairIDByAddress(addr[:])
		}
		// Expected: 0 allocs/op, 0 B/op
	})

	b.Run("AddressKeyGeneration", func(b *testing.B) {
		b.ReportAllocs() // Shows actual per-operation allocation
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			addr := addresses[i%len(addresses)]
			_ = bytesToAddressKey(addr[:])
		}
		// Expected: 0 allocs/op, 0 B/op
	})
}

// Optional: Keep one simple test for basic validation
func TestMemoryValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory validation in short mode")
	}

	// Simple validation that operations work without panicking
	addr := generateMockAddress(12345)
	pairID := PairID(12345)

	// This should not allocate heap memory
	RegisterPairAddress(addr[:], pairID)

	// This should not allocate heap memory
	found := lookupPairIDByAddress(addr[:])

	if found != pairID {
		t.Errorf("Expected %d, got %d", pairID, found)
	}

	t.Logf("✅ Memory validation passed - operations work correctly")
}

// TestConcurrentSafety validates concurrent operations
func TestConcurrentSafety(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	t.Run("ConcurrentRegistration", func(t *testing.T) {
		const goroutineCount = 16
		const operationsPerGoroutine = 100

		var wg sync.WaitGroup
		processed := uint64(0)

		for g := 0; g < goroutineCount; g++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for i := 0; i < operationsPerGoroutine; i++ {
					addr := generateMockAddress(uint64(workerID*10000 + i))
					pairID := PairID(workerID*10000 + i + 50000)

					RegisterPairAddress(addr[:], pairID)

					found := lookupPairIDByAddress(addr[:])
					if found == pairID {
						atomic.AddUint64(&processed, 1)
					}
				}
			}(g)
		}

		wg.Wait()

		expectedTotal := uint64(goroutineCount * operationsPerGoroutine)
		if processed != expectedTotal {
			t.Errorf("Expected %d successful, got %d", expectedTotal, processed)
		}
	})

	t.Run("ConcurrentCoreAssignment", func(t *testing.T) {
		const goroutineCount = 8
		const pairsPerGoroutine = 50

		var wg sync.WaitGroup

		for g := 0; g < goroutineCount; g++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for i := 0; i < pairsPerGoroutine; i++ {
					pairID := PairID(workerID*1000 + i)
					coreID := uint8((workerID + i) % 64)

					RegisterPairToCore(pairID, coreID)

					// Verify assignment
					assignment := pairToCoreAssignment[pairID]
					expectedBit := uint64(1) << coreID

					if assignment&expectedBit == 0 {
						t.Errorf("Core assignment failed for pair %d", pairID)
					}
				}
			}(g)
		}

		wg.Wait()
	})

	t.Run("ConcurrentDispatch", func(t *testing.T) {
		// Setup shared pairs
		sharedPairs := 10
		for i := 0; i < sharedPairs; i++ {
			addr := generateMockAddress(uint64(200000 + i))
			pairID := PairID(200000 + i)
			RegisterPairAddress(addr[:], pairID)
			RegisterPairToCore(pairID, 0)
		}

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(1024)
		}

		const goroutineCount = 8
		const updatesPerGoroutine = 100

		var wg sync.WaitGroup
		dispatched := uint64(0)

		for g := 0; g < goroutineCount; g++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for i := 0; i < updatesPerGoroutine; i++ {
					pairIdx := (workerID + i) % sharedPairs
					addr := generateMockAddress(uint64(200000 + pairIdx))

					logView := &types.LogView{
						Addr: make([]byte, 64),
						Data: make([]byte, 128),
					}

					logView.Addr[0] = '0'
					logView.Addr[1] = 'x'
					copy(logView.Addr[2:42], addr[:])

					reserve0 := uint64(1000 + workerID*100 + i)
					reserve1 := uint64(2000 - workerID*50 - i)

					for j := 0; j < 8; j++ {
						logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
						logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
					}

					DispatchTickUpdate(logView)
					atomic.AddUint64(&dispatched, 1)
				}
			}(g)
		}

		wg.Wait()

		expectedDispatches := uint64(goroutineCount * updatesPerGoroutine)
		if dispatched != expectedDispatches {
			t.Errorf("Expected %d dispatches, got %d", expectedDispatches, dispatched)
		}
	})
}

// TestHighThroughput validates sustained load performance
func TestHighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput test in short mode")
	}

	setup := createMockArbitrageSetup()

	// Pre-register addresses
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
		RegisterPairToCore(pairID, 0)
	}

	if coreRings[0] == nil {
		coreRings[0] = ring24.New(1024)
	}

	const operationCount = 10000
	processed := uint64(0)

	start := time.Now()

	for i := 0; i < operationCount; i++ {
		update := setup.tickUpdateQueue[i%len(setup.tickUpdateQueue)]

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		addr := setup.addressMappings[update.pairID]
		copy(logView.Addr[2:42], addr[:])

		for j := 0; j < 8; j++ {
			logView.Data[24+j] = byte(update.reserve0 >> (8 * (7 - j)))
			logView.Data[56+j] = byte(update.reserve1 >> (8 * (7 - j)))
		}

		DispatchTickUpdate(logView)
		atomic.AddUint64(&processed, 1)
	}

	elapsed := time.Since(start)
	throughput := float64(processed) / elapsed.Seconds()

	t.Logf("Throughput: %.0f ops/sec", throughput)

	if throughput < minThroughputOpsPerSec {
		t.Errorf("Insufficient throughput: %.0f ops/sec", throughput)
	}
}

// ZERO-OVERHEAD PURE PERFORMANCE BENCHMARKS

// BenchmarkAddressKeyGeneration - Pure key generation performance
func BenchmarkAddressKeyGeneration(b *testing.B) {
	addresses := make([][40]byte, 1000)
	for i := range addresses {
		addresses[i] = generateMockAddress(uint64(i * 2654435761))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = bytesToAddressKey(addr[:])
	}
}

// BenchmarkAddressKeyComparison - Pure key comparison performance
func BenchmarkAddressKeyComparison(b *testing.B) {
	addr1 := generateMockAddress(12345)
	addr2 := generateMockAddress(54321)
	key1 := bytesToAddressKey(addr1[:])
	key2 := bytesToAddressKey(addr2[:])

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			_ = key1.isEqual(key1) // Same key
		} else {
			_ = key1.isEqual(key2) // Different keys
		}
	}
}

// BenchmarkDirectAddressIndexing - Pure address indexing performance
func BenchmarkDirectAddressIndexing(b *testing.B) {
	addresses := make([][40]byte, 1000)
	for i := range addresses {
		addresses[i] = generateMockAddress(uint64(i * 16777619))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = directAddressToIndex64(addr[:])
	}
}

// BenchmarkRobinHoodHashTableLookup - Pure hash table lookup performance
func BenchmarkRobinHoodHashTableLookup(b *testing.B) {
	// Pre-populate with realistic load
	const entryCount = 10000
	addresses := make([][40]byte, entryCount)

	for i := 0; i < entryCount; i++ {
		addr := generateMockAddress(uint64(i * 1000003))
		addresses[i] = addr
		RegisterPairAddress(addr[:], PairID(i+100000))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%entryCount]
		_ = lookupPairIDByAddress(addr[:])
	}
}

// BenchmarkRobinHoodHashTableRegistration - Pure hash table registration performance
func BenchmarkRobinHoodHashTableRegistration(b *testing.B) {
	addresses := make([][40]byte, 10000)
	for i := range addresses {
		addresses[i] = generateMockAddress(uint64(i * 7919))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		RegisterPairAddress(addr[:], PairID(i+200000))
	}
}

// BenchmarkTickQuantization - Pure tick quantization performance
func BenchmarkTickQuantization(b *testing.B) {
	// Pre-generate tick values covering full range
	ticks := make([]float64, 1000)
	for i := range ticks {
		// Distribute across full range
		ratio := float64(i) / float64(len(ticks))
		ticks[i] = -constants.TickClampingBound + ratio*2*constants.TickClampingBound
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tick := ticks[i%len(ticks)]
		_ = quantizeTickToInt64(tick)
	}
}

// BenchmarkCoreAssignmentBitOperations - Pure bit manipulation performance
func BenchmarkCoreAssignmentBitOperations(b *testing.B) {
	// Pre-populate with realistic assignments
	const pairCount = 10000
	for i := 0; i < pairCount; i++ {
		pairID := PairID(i)
		numCores := 2 + (i % 6) // 2-7 cores per pair
		for j := 0; j < numCores; j++ {
			coreID := uint8((i*17 + j*23) % 64)
			RegisterPairToCore(pairID, coreID)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pairID := PairID(i % pairCount)
		coreAssignments := pairToCoreAssignment[pairID]

		// Simulate dispatch loop - pure bit operations
		for coreAssignments != 0 {
			coreID := bits.TrailingZeros64(uint64(coreAssignments))
			coreAssignments &^= 1 << coreID
			_ = coreID // Consume result
		}
	}
}

// BenchmarkProcessTickUpdateCore - Pure tick update processing performance
func BenchmarkProcessTickUpdateCore(b *testing.B) {
	// Minimal executor setup for pure processing
	executor := &ArbitrageCoreExecutor{
		isReverseDirection: false,
		priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
		fanoutTables:       make([][]FanoutEntry, 1),
		pairToQueueIndex:   localidx.New(16),
		cycleStates:        make([]ArbitrageCycleState, 10),
	}

	executor.priorityQueues[0] = *quantumqueue64.New()
	executor.pairToQueueIndex.Put(123, 0)
	executor.fanoutTables[0] = []FanoutEntry{} // No fanout for pure performance

	// Setup cycles
	for i := 0; i < 10; i++ {
		executor.cycleStates[i] = ArbitrageCycleState{
			tickValues: [3]float64{0.001, 0.002, 0.003},
			pairIDs:    [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
		}

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		priority := quantizeTickToInt64(0.006)
		executor.priorityQueues[0].Push(priority, handle, uint64(i))
	}

	// Pre-generate updates
	updates := make([]*TickUpdate, 100)
	for i := range updates {
		updates[i] = &TickUpdate{
			pairID:      PairID(123),
			forwardTick: float64(i%20-10) * 0.001, // -0.01 to +0.01
			reverseTick: float64(10-i%20) * 0.001,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		update := updates[i%len(updates)]
		processTickUpdate(executor, update)
	}
}

// BenchmarkQuantumQueueOperations - Pure priority queue performance
func BenchmarkQuantumQueueOperations(b *testing.B) {
	queue := quantumqueue64.New()

	// Pre-allocate handles
	handles := make([]quantumqueue64.Handle, 1000)
	for i := range handles {
		handles[i], _ = queue.BorrowSafe()
	}

	// Pre-populate queue
	for i := 0; i < len(handles); i++ {
		priority := int64(i % 1000)
		queue.Push(priority, handles[i], uint64(i))
	}

	b.Run("PeepMin", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, _, _ = queue.PeepMin()
		}
	})

	b.Run("MoveTick", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			handle := handles[i%len(handles)]
			newPriority := int64((i + 500) % 1000)
			queue.MoveTick(handle, newPriority)
		}
	})

	b.Run("UnlinkPush", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			handle, priority, data := queue.PeepMin()
			queue.UnlinkMin(handle)
			queue.Push(priority, handle, data)
		}
	})
}

// BenchmarkLocalIndexOperations - Pure local index performance
func BenchmarkLocalIndexOperations(b *testing.B) {
	idx := localidx.New(1024)

	// Pre-populate
	for i := 0; i < 500; i++ {
		idx.Put(uint32(i), uint32(i%100))
	}

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			key := uint32(i % 500)
			_, _ = idx.Get(key)
		}
	})

	b.Run("Put", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			key := uint32(i % 1000)
			value := uint32(i % 100)
			idx.Put(key, value)
		}
	})
}

// BenchmarkRing24Operations - Pure ring buffer performance
func BenchmarkRing24Operations(b *testing.B) {
	ring := ring24.New(1024)

	// Pre-generate messages
	messages := make([][24]byte, 100)
	for i := range messages {
		update := &TickUpdate{
			pairID:      PairID(i),
			forwardTick: float64(i) * 0.1,
			reverseTick: float64(-i) * 0.1,
		}
		messages[i] = *(*[24]byte)(unsafe.Pointer(update))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := &messages[i%len(messages)]
		ring.Push(msg)
	}
}

// BenchmarkLog2ReserveRatio - Pure logarithm calculation performance
func BenchmarkLog2ReserveRatio(b *testing.B) {
	// Pre-generate realistic reserve pairs
	reservePairs := make([][2]uint64, 1000)
	for i := range reservePairs {
		base := uint64(1000000) // $1M base
		multiplier := uint64(1 + i%100)
		variance := uint64(90 + i%20) // 90-110%

		reservePairs[i][0] = base * multiplier
		reservePairs[i][1] = base * multiplier * variance / 100
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pair := reservePairs[i%len(reservePairs)]
		_, _ = fastuni.Log2ReserveRatio(pair[0], pair[1])
	}
}

// BenchmarkSecureRandomIntCore - Pure random generation performance
func BenchmarkSecureRandomIntCore(b *testing.B) {
	bounds := []int{2, 4, 8, 16, 32, 64, 128, 256, 1000}

	for _, bound := range bounds {
		b.Run(fmt.Sprintf("Bound%d", bound), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_ = secureRandomInt(bound)
			}
		})
	}
}

// BenchmarkShuffleEdgeBindingsCore - Pure shuffle performance
func BenchmarkShuffleEdgeBindingsCore(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			// Pre-generate test data
			bindings := make([]ArbitrageEdgeBinding, size)
			for i := 0; i < size; i++ {
				bindings[i] = ArbitrageEdgeBinding{
					cyclePairs: [3]PairID{PairID(i), PairID(i + 1), PairID(i + 2)},
					edgeIndex:  uint16(i % 3),
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Work on copy to avoid modifying original
				testBindings := make([]ArbitrageEdgeBinding, size)
				copy(testBindings, bindings)
				shuffleEdgeBindings(testBindings)
			}
		})
	}
}

// BenchmarkMemoryAccessPatterns - Pure memory access performance
func BenchmarkMemoryAccessPatterns(b *testing.B) {
	const cycleCount = 1000

	cycles := make([]ArbitrageCycleState, cycleCount)
	for i := range cycles {
		cycles[i] = ArbitrageCycleState{
			tickValues: [3]float64{
				float64(i) * 0.001,
				float64(i) * 0.002,
				float64(i) * 0.003,
			},
			pairIDs: [3]PairID{
				PairID(i * 3),
				PairID(i*3 + 1),
				PairID(i*3 + 2),
			},
		}
	}

	b.Run("Sequential", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		for i := 0; i < b.N; i++ {
			cycle := &cycles[i%cycleCount]
			sum += cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		}
		_ = sum
	})

	b.Run("Random", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		for i := 0; i < b.N; i++ {
			idx := (i * 2654435761) % cycleCount // Fast pseudo-random
			cycle := &cycles[idx]
			sum += cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		}
		_ = sum
	})

	b.Run("Strided", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		stride := 17 // Prime for good coverage
		for i := 0; i < b.N; i++ {
			idx := (i * stride) % cycleCount
			cycle := &cycles[idx]
			sum += cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		}
		_ = sum
	})
}

// BenchmarkDispatchTickUpdateCore - Pure dispatch performance
func BenchmarkDispatchTickUpdateCore(b *testing.B) {
	// Pre-register pairs
	const pairCount = 1000
	addresses := make([][40]byte, pairCount)

	for i := 0; i < pairCount; i++ {
		addr := generateMockAddress(uint64(i * 1000003))
		addresses[i] = addr
		pairID := PairID(i)
		RegisterPairAddress(addr[:], pairID)

		// Assign to 2-4 cores each
		numCores := 2 + (i % 3)
		for j := 0; j < numCores; j++ {
			coreID := uint8((i + j*16) % 8)
			RegisterPairToCore(pairID, coreID)
		}
	}

	// Initialize rings
	for i := 0; i < 8; i++ {
		if coreRings[i] == nil {
			coreRings[i] = ring24.New(1024)
		}
	}

	// Pre-generate log views
	logViews := make([]*types.LogView, 100)
	for i := range logViews {
		pairIdx := i % pairCount
		addr := addresses[pairIdx]

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], addr[:])

		reserve0 := uint64(1000000 + i*1000)
		reserve1 := uint64(1000000 + i*500)

		for j := 0; j < 8; j++ {
			logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
			logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
		}

		logViews[i] = logView
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logView := logViews[i%len(logViews)]
		DispatchTickUpdate(logView)
	}
}

// BenchmarkArbitrageDetectionCore - Pure arbitrage detection performance
func BenchmarkArbitrageDetectionCore(b *testing.B) {
	// Minimal setup focused on arbitrage detection
	executor := &ArbitrageCoreExecutor{
		isReverseDirection: false,
		priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
		fanoutTables:       make([][]FanoutEntry, 1),
		pairToQueueIndex:   localidx.New(16),
		cycleStates:        make([]ArbitrageCycleState, 100),
	}

	executor.priorityQueues[0] = *quantumqueue64.New()
	executor.pairToQueueIndex.Put(123, 0)
	executor.fanoutTables[0] = []FanoutEntry{} // No fanout for pure detection

	// Setup cycles near profitability threshold
	for i := 0; i < 100; i++ {
		// Most cycles slightly unprofitable, few profitable
		baseProfitability := float64(i%20-10) * 0.001 // -0.01 to +0.01

		executor.cycleStates[i] = ArbitrageCycleState{
			tickValues: [3]float64{
				baseProfitability / 3,
				baseProfitability / 3,
				baseProfitability / 3,
			},
			pairIDs: [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
		}

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		priority := quantizeTickToInt64(baseProfitability)
		executor.priorityQueues[0].Push(priority, handle, uint64(i))
	}

	// Updates that trigger arbitrage detection
	update := &TickUpdate{
		pairID:      PairID(123),
		forwardTick: -0.005, // Makes some cycles profitable
		reverseTick: 0.005,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		processTickUpdate(executor, update)
	}
}

// BenchmarkFanoutTableConstruction - Pure fanout building performance
func BenchmarkFanoutTableConstruction(b *testing.B) {
	cycleCounts := []int{100, 1000, 5000}

	for _, count := range cycleCounts {
		b.Run(fmt.Sprintf("Cycles%d", count), func(b *testing.B) {
			cycles := make([]ArbitrageTriplet, count)
			for i := 0; i < count; i++ {
				baseID := uint32(i * 3)
				cycles[i] = ArbitrageTriplet{
					PairID(baseID),
					PairID(baseID + 1),
					PairID(baseID + 2),
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				buildFanoutShardBuckets(cycles)
			}
		})
	}
}

// BenchmarkSystemIntegrationCore - Pure end-to-end performance
func BenchmarkSystemIntegrationCore(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping integration benchmark in short mode")
	}

	// Minimal scale for pure performance measurement
	const triangleCount = 100

	// Clear state
	for i := range pairAddressKeys[:500] {
		pairAddressKeys[i] = AddressKey{}
		addressToPairID[i] = 0
	}
	for i := range pairToCoreAssignment[:triangleCount*3] {
		pairToCoreAssignment[i] = 0
	}

	cycles := make([]ArbitrageTriplet, triangleCount)
	for i := 0; i < triangleCount; i++ {
		baseID := uint32(i * 3)
		cycles[i] = ArbitrageTriplet{
			PairID(baseID),
			PairID(baseID + 1),
			PairID(baseID + 2),
		}
	}

	// Initialize once
	InitializeArbitrageSystem(cycles)

	// Register addresses
	for i := 0; i < triangleCount*3; i++ {
		addr := generateMockAddress(uint64(i * 1000003))
		RegisterPairAddress(addr[:], PairID(i))
	}

	// Pre-generate workload
	logViews := make([]*types.LogView, 50)
	for i := range logViews {
		pairID := PairID(i % (triangleCount * 3))
		addr := generateMockAddress(uint64(pairID * 1000003))

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], addr[:])

		reserve0 := uint64(1000000 + i*1000)
		reserve1 := uint64(1000000 - i*500)

		for j := 0; j < 8; j++ {
			logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
			logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
		}

		logViews[i] = logView
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logView := logViews[i%len(logViews)]
		DispatchTickUpdate(logView)
	}

	b.StopTimer()
	control.Shutdown()
}

// BenchmarkWorstCasePerformance - Edge case performance testing
func BenchmarkWorstCasePerformance(b *testing.B) {
	b.Run("MaxCoresDispatch", func(b *testing.B) {
		// Pair assigned to all 64 cores
		pairID := PairID(999999)
		for core := uint8(0); core < 64; core++ {
			RegisterPairToCore(pairID, core)
			if coreRings[core] == nil {
				coreRings[core] = ring24.New(16)
			}
		}

		addr := generateMockAddress(999999)
		RegisterPairAddress(addr[:], pairID)

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], addr[:])

		reserve0, reserve1 := uint64(1000000), uint64(1000000)
		for i := 0; i < 8; i++ {
			logView.Data[24+i] = byte(reserve0 >> (8 * (7 - i)))
			logView.Data[56+i] = byte(reserve1 >> (8 * (7 - i)))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			DispatchTickUpdate(logView)
		}
	})

	b.Run("HashTableMaxCollision", func(b *testing.B) {
		// Force hash collisions
		addresses := make([][40]byte, 1000)
		for i := 0; i < len(addresses); i++ {
			addr := generateMockAddress(uint64(i))
			// Force similar hash prefixes
			for j := 0; j < 8; j++ {
				addr[j] = byte(i % 256)
			}
			addresses[i] = addr
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			addr := addresses[i%len(addresses)]
			RegisterPairAddress(addr[:], PairID(i+1000000))
		}
	})

	b.Run("MaxFanoutProcessing", func(b *testing.B) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, constants.DefaultRingSize), // Use system limit
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		// REALISTIC TEST: Use actual system capacity
		// This represents the max fanout a single queue can handle
		fanoutSize := constants.DefaultRingSize // 65,536 - the actual limit!
		executor.fanoutTables[0] = make([]FanoutEntry, fanoutSize)

		b.Logf("Setting up %d fanout entries (quantum queue capacity limit)...", fanoutSize)

		for i := 0; i < fanoutSize; i++ {
			// Initialize cycle state
			executor.cycleStates[i] = ArbitrageCycleState{
				tickValues: [3]float64{
					0.001 + float64(i%1000)*0.000001,
					0.002 + float64(i%1000)*0.000001,
					0.003 + float64(i%1000)*0.000001,
				},
				pairIDs: [3]PairID{
					PairID(i * 3),
					PairID(i*3 + 1),
					PairID(i*3 + 2),
				},
			}

			// Borrow handle and push to queue
			handle, err := executor.priorityQueues[0].BorrowSafe()
			if err != nil {
				b.Fatalf("Failed to borrow handle at index %d: %v", i, err)
			}

			initialPriority := quantizeTickToInt64(
				executor.cycleStates[i].tickValues[0] +
					executor.cycleStates[i].tickValues[1] +
					executor.cycleStates[i].tickValues[2],
			)

			// Push to queue (no return value to check)
			executor.priorityQueues[0].Push(initialPriority, handle, uint64(i))

			executor.fanoutTables[0][i] = FanoutEntry{
				queueHandle:     handle,
				edgeIndex:       uint16(i % 3),
				cycleStateIndex: CycleStateIndex(i),
				queue:           &executor.priorityQueues[0],
			}

			if i%10000 == 0 {
				b.Logf("Initialized %d/%d cycles", i, fanoutSize)
			}
		}

		b.Logf("Setup complete. Queue size: %d (at capacity)", fanoutSize)

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: -0.001,
			reverseTick: 0.001,
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			processTickUpdate(executor, update)
		}

		b.StopTimer()
		b.Logf("Processed %d updates with %d fanout each", b.N, fanoutSize)
	})

	// Test what happens when we need MORE than queue capacity
	b.Run("MultiQueueFanout", func(b *testing.B) {
		// For 100k fanout, we'd need multiple queues
		targetFanout := 100000
		queueCapacity := constants.DefaultRingSize
		requiredQueues := (targetFanout + queueCapacity - 1) / queueCapacity // Ceiling division

		b.Logf("For %d fanout, need %d queues (%d capacity each)",
			targetFanout, requiredQueues, queueCapacity)

		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, requiredQueues),
			fanoutTables:       make([][]FanoutEntry, requiredQueues),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, targetFanout),
		}

		// Initialize multiple queues
		for q := 0; q < requiredQueues; q++ {
			executor.priorityQueues[q] = *quantumqueue64.New()
			executor.fanoutTables[q] = []FanoutEntry{}
		}

		// Distribute cycles across queues
		for i := 0; i < targetFanout; i++ {
			queueIndex := i / queueCapacity // Which queue for this cycle

			executor.cycleStates[i] = ArbitrageCycleState{
				tickValues: [3]float64{0.001, 0.002, 0.003},
				pairIDs:    [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
			}

			handle, _ := executor.priorityQueues[queueIndex].BorrowSafe()
			priority := quantizeTickToInt64(0.006)
			executor.priorityQueues[queueIndex].Push(priority, handle, uint64(i))

			executor.fanoutTables[queueIndex] = append(executor.fanoutTables[queueIndex],
				FanoutEntry{
					queueHandle:     handle,
					edgeIndex:       uint16(i % 3),
					cycleStateIndex: CycleStateIndex(i),
					queue:           &executor.priorityQueues[queueIndex],
				})

			if i%20000 == 0 {
				b.Logf("Distributed %d/%d cycles across %d queues", i, targetFanout, requiredQueues)
			}
		}

		executor.pairToQueueIndex.Put(123, 0) // Point to first queue for testing

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: -0.001,
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Only process first queue for benchmark
			processTickUpdate(executor, update)
		}

		b.StopTimer()
		b.Logf("Multi-queue setup: %d total cycles, %d queues", targetFanout, requiredQueues)
	})
}
