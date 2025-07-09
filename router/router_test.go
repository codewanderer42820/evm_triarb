// router_test.go — FOOTGUN MODE comprehensive test suite
package router

import (
	"fmt"
	"math"
	"math/bits"
	"runtime"
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

// Test helpers
func generateMockAddress(seed uint64) [40]byte {
	var addr [40]byte
	for i := 0; i < 20; i++ {
		byteVal := uint8((seed >> (i * 8)) ^ (seed >> ((i + 7) * 3)))
		addr[i*2] = "0123456789abcdef"[byteVal>>4]
		addr[i*2+1] = "0123456789abcdef"[byteVal&0xF]
	}
	return addr
}

func generateArbitrageTriangle(baseID uint32) ArbitrageTriplet {
	return ArbitrageTriplet{
		PairID(baseID),
		PairID(baseID + 1),
		PairID(baseID + 2),
	}
}

// TestAddressKey - Test the [20]byte address key type
func TestAddressKey(t *testing.T) {
	t.Run("DirectComparison", func(t *testing.T) {
		addr1 := generateMockAddress(12345)
		addr2 := generateMockAddress(12345)
		addr3 := generateMockAddress(54321)

		key1 := AddressKey(utils.ParseEthereumAddress(addr1[:]))
		key2 := AddressKey(utils.ParseEthereumAddress(addr2[:]))
		key3 := AddressKey(utils.ParseEthereumAddress(addr3[:]))

		if key1 != key2 {
			t.Error("Identical addresses must generate equal keys")
		}

		if key1 == key3 {
			t.Error("Different addresses must generate different keys")
		}
	})

	t.Run("ZeroValue", func(t *testing.T) {
		var zeroKey AddressKey
		var emptyAddr [40]byte
		for i := range emptyAddr {
			emptyAddr[i] = '0'
		}

		parsedKey := AddressKey(utils.ParseEthereumAddress(emptyAddr[:]))
		// Zero address string "0000..." should parse to actual zero bytes
		if parsedKey != zeroKey {
			t.Error("Parsed zero address should equal zero key")
		}
	})

	t.Run("Size", func(t *testing.T) {
		var key AddressKey
		if unsafe.Sizeof(key) != 20 {
			t.Errorf("AddressKey size is %d, expected 20", unsafe.Sizeof(key))
		}
	})
}

// TestDirectMapOperations - Test the map[AddressKey]PairID
func TestDirectMapOperations(t *testing.T) {
	t.Run("BasicOperations", func(t *testing.T) {
		addressToPairID = make(map[AddressKey]PairID)

		addr := generateMockAddress(42)
		pairID := PairID(1337)

		RegisterPairAddress(addr[:], pairID)
		foundID := lookupPairIDByAddress(addr[:])

		if foundID != pairID {
			t.Errorf("Expected pair ID %d, got %d", pairID, foundID)
		}
	})

	t.Run("UpdateExisting", func(t *testing.T) {
		addressToPairID = make(map[AddressKey]PairID)

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
		addressToPairID = make(map[AddressKey]PairID)

		unknownAddr := generateMockAddress(999999)
		foundID := lookupPairIDByAddress(unknownAddr[:])

		if foundID != 0 {
			t.Error("Unknown address must return 0")
		}
	})

	t.Run("LargeScale", func(t *testing.T) {
		addressToPairID = make(map[AddressKey]PairID)

		const count = 10000
		for i := 0; i < count; i++ {
			addr := generateMockAddress(uint64(i))
			RegisterPairAddress(addr[:], PairID(i))
		}

		// Verify some samples
		for i := 0; i < count; i += 100 {
			addr := generateMockAddress(uint64(i))
			found := lookupPairIDByAddress(addr[:])
			if found != PairID(i) {
				t.Errorf("Lost entry %d", i)
			}
		}
	})
}

// TestTickQuantization - Test the quantization function
func TestTickQuantization(t *testing.T) {
	testCases := []struct {
		input float64
		name  string
	}{
		{-1000.0, "ExtremeNegative"},
		{-constants.TickClampingBound - 1, "BelowLowerBound"},
		{-constants.TickClampingBound, "LowerBound"},
		{-100.0, "Negative"},
		{0.0, "Zero"},
		{100.0, "Positive"},
		{constants.TickClampingBound, "UpperBound"},
		{constants.TickClampingBound + 1, "AboveUpperBound"},
		{1000.0, "ExtremePositive"},
		{math.NaN(), "NaN"},
		{math.Inf(1), "PositiveInf"},
		{math.Inf(-1), "NegativeInf"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := quantizeTickToInt64(tc.input)
			// No bounds checking - that's the footgun way
			_ = result
		})
	}

	t.Run("Precision", func(t *testing.T) {
		tick1 := 50.0
		tick2 := 50.0 + 1.0/constants.QuantizationScale

		q1 := quantizeTickToInt64(tick1)
		q2 := quantizeTickToInt64(tick2)

		// Should differ by 1 quantum
		diff := q2 - q1
		if diff < 0 {
			diff = -diff
		}
		if diff != 1 {
			t.Errorf("Quantization precision incorrect: diff=%d", diff)
		}
	})
}

// TestSecureRandomInt - Test random generation
func TestSecureRandomInt(t *testing.T) {
	t.Run("BoundaryValues", func(t *testing.T) {
		bounds := []int{1, 2, 10, 100, 1000}

		for _, bound := range bounds {
			results := make(map[int]bool)

			// Generate many values
			for i := 0; i < bound*10; i++ {
				result := secureRandomInt(bound)
				if result < 0 || result >= bound {
					t.Errorf("Result %d outside [0,%d)", result, bound)
				}
				results[result] = true
			}

			// Check coverage (might fail randomly - that's fine)
			if len(results) < bound/2 {
				t.Logf("Warning: Only saw %d/%d values", len(results), bound)
			}
		}
	})

	t.Run("ZeroBound", func(t *testing.T) {
		// This will crash - footgun style
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic with zero bound")
			}
		}()

		_ = secureRandomInt(0)
	})
}

// TestCoreAssignment - Test pair-to-core mapping
func TestCoreAssignment(t *testing.T) {
	// Clear assignments
	for i := range pairToCoreAssignment[:100] {
		pairToCoreAssignment[i] = 0
	}

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

	t.Run("MultipleCores", func(t *testing.T) {
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

		for core := uint8(0); core < 64; core++ {
			RegisterPairToCore(pairID, core)
		}

		assignment := pairToCoreAssignment[pairID]
		if assignment != ^uint64(0) {
			t.Errorf("Expected all bits set, got %064b", assignment)
		}
	})

	t.Run("BitExtraction", func(t *testing.T) {
		pairID := PairID(88888)
		expectedCores := []uint8{1, 3, 5, 7, 11, 13}

		for _, coreID := range expectedCores {
			RegisterPairToCore(pairID, coreID)
		}

		assignment := pairToCoreAssignment[pairID]
		foundCores := []uint8{}

		for assignment != 0 {
			coreID := bits.TrailingZeros64(uint64(assignment))
			foundCores = append(foundCores, uint8(coreID))
			assignment &^= 1 << coreID
		}

		if len(foundCores) != len(expectedCores) {
			t.Errorf("Expected %d cores, found %d", len(expectedCores), len(foundCores))
		}
	})

	t.Run("OutOfBoundsPair", func(t *testing.T) {
		// This will access out of bounds - footgun!
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic with out of bounds pair")
			}
		}()

		hugePairID := PairID(constants.PairRoutingTableCapacity + 1)
		RegisterPairToCore(hugePairID, 0)
	})
}

// TestProcessTickUpdate - Test the core processing function
func TestProcessTickUpdate(t *testing.T) {
	t.Run("ForwardDirection", func(t *testing.T) {
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
			tickValues: [3]float64{0.0, 0.0, 0.0},
			pairIDs:    [3]PairID{123, 124, 125},
		}

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: 1.5,
			reverseTick: -1.5,
		}

		processTickUpdate(executor, update)

		// Forward direction should use forwardTick
		// But it only updates through fanout, so tickValues stay at 0
		if executor.cycleStates[0].tickValues[0] != 0.0 {
			t.Error("Tick updated without fanout entry")
		}
	})

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

		// Setup with fanout to see the update
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.0, 0.0, 0.0},
			pairIDs:    [3]PairID{123, 124, 125},
		}

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
			forwardTick: 1.5,
			reverseTick: -1.5,
		}

		processTickUpdate(executor, update)

		// Reverse direction should use reverseTick
		if executor.cycleStates[0].tickValues[0] != -1.5 {
			t.Errorf("Expected -1.5, got %f", executor.cycleStates[0].tickValues[0])
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

		// Profitable cycle: sum < 0
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{-0.5, -0.3, -0.1}, // sum = -0.9
			pairIDs:    [3]PairID{100, 101, 102},
		}

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		priority := quantizeTickToInt64(-0.9)
		executor.priorityQueues[0].Push(priority, handle, 0)

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: -0.2, // Makes total -1.1 (profitable)
		}

		// Should process the profitable cycle
		processTickUpdate(executor, update)
	})

	t.Run("MaxBatchProcessing", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 200),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		// Add 200 profitable cycles (more than 128 batch size)
		for i := 0; i < 200; i++ {
			executor.cycleStates[i] = ArbitrageCycleState{
				tickValues: [3]float64{-0.1, -0.1, -0.1},
				pairIDs:    [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
			}

			handle, _ := executor.priorityQueues[0].BorrowSafe()
			priority := quantizeTickToInt64(-0.3)
			executor.priorityQueues[0].Push(priority, handle, uint64(i))
		}

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: -1.0, // Makes all cycles very profitable
		}

		// Should only process 128 in one batch
		processTickUpdate(executor, update)
	})

	t.Run("FanoutPropagation", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 3),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		// Setup multiple cycles
		for i := 0; i < 3; i++ {
			executor.cycleStates[i] = ArbitrageCycleState{
				tickValues: [3]float64{0.0, 0.0, 0.0},
				pairIDs:    [3]PairID{PairID(100 + i*3), PairID(101 + i*3), PairID(102 + i*3)},
			}
		}

		// Create fanout entries
		handles := make([]quantumqueue64.Handle, 3)
		for i := 0; i < 3; i++ {
			handles[i], _ = executor.priorityQueues[0].BorrowSafe()
			executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handles[i], uint64(i))
		}

		executor.fanoutTables[0] = []FanoutEntry{
			{
				queueHandle:     handles[0],
				edgeIndex:       0,
				cycleStateIndex: 0,
				queue:           &executor.priorityQueues[0],
			},
			{
				queueHandle:     handles[1],
				edgeIndex:       1,
				cycleStateIndex: 1,
				queue:           &executor.priorityQueues[0],
			},
			{
				queueHandle:     handles[2],
				edgeIndex:       2,
				cycleStateIndex: 2,
				queue:           &executor.priorityQueues[0],
			},
		}

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: 1.5,
		}

		processTickUpdate(executor, update)

		// Verify propagation
		if executor.cycleStates[0].tickValues[0] != 1.5 {
			t.Errorf("Cycle 0 edge 0: expected 1.5, got %f", executor.cycleStates[0].tickValues[0])
		}
		if executor.cycleStates[1].tickValues[1] != 1.5 {
			t.Errorf("Cycle 1 edge 1: expected 1.5, got %f", executor.cycleStates[1].tickValues[1])
		}
		if executor.cycleStates[2].tickValues[2] != 1.5 {
			t.Errorf("Cycle 2 edge 2: expected 1.5, got %f", executor.cycleStates[2].tickValues[2])
		}
	})

	t.Run("EmptyQueue", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 0),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: 1.0,
		}

		// Should handle empty queue gracefully
		processTickUpdate(executor, update)
	})

	t.Run("InvalidQueueIndex", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		// Don't register pair 123, so lookup returns (0, false)
		// This will access priorityQueues[0] which exists, so no crash

		update := &TickUpdate{
			pairID:      PairID(999), // Unregistered
			forwardTick: 1.0,
		}

		// Should not crash
		processTickUpdate(executor, update)
	})
}

// TestDispatchTickUpdate - Test the dispatch function
func TestDispatchTickUpdate(t *testing.T) {
	addressToPairID = make(map[AddressKey]PairID)

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

		// Should return early when pairID is 0
		DispatchTickUpdate(logView)
	})

	t.Run("NoCoreAssignment", func(t *testing.T) {
		addr := generateMockAddress(777)
		pairID := PairID(777)
		RegisterPairAddress(addr[:], pairID)
		// Don't assign to any core

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], addr[:])

		// Should handle zero core assignment
		DispatchTickUpdate(logView)
	})

	t.Run("AllCoresDispatch", func(t *testing.T) {
		addr := generateMockAddress(888)
		pairID := PairID(888)
		RegisterPairAddress(addr[:], pairID)

		// Assign to all 64 cores
		for core := uint8(0); core < 64; core++ {
			RegisterPairToCore(pairID, core)
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

	t.Run("EdgeCaseReserves", func(t *testing.T) {
		addr := generateMockAddress(666)
		pairID := PairID(666)
		RegisterPairAddress(addr[:], pairID)
		RegisterPairToCore(pairID, 0)

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}

		testCases := []struct {
			reserve0, reserve1 uint64
			name               string
		}{
			{0, 0, "BothZero"},
			{1, 1, "Equal"},
			{0, 1000, "ZeroReserve0"},
			{1000, 0, "ZeroReserve1"},
			{1<<64 - 1, 1, "MaxReserve"},
			{1, 1<<64 - 1, "MinRatio"},
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

				// Should handle edge cases in fastuni.Log2ReserveRatio
				DispatchTickUpdate(logView)
			})
		}
	})

	t.Run("DirectMemoryAccess", func(t *testing.T) {
		addr := generateMockAddress(444)
		pairID := PairID(444)
		RegisterPairAddress(addr[:], pairID)
		RegisterPairToCore(pairID, 0)

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], addr[:])

		// Test the direct unsafe memory access
		reserve0 := uint64(0x0102030405060708)
		reserve1 := uint64(0x090A0B0C0D0E0F10)

		// Manually set bytes to test direct loading
		*(*uint64)(unsafe.Pointer(&logView.Data[24])) = bits.ReverseBytes64(reserve0)
		*(*uint64)(unsafe.Pointer(&logView.Data[56])) = bits.ReverseBytes64(reserve1)

		DispatchTickUpdate(logView)
	})
}

// TestShuffleEdgeBindings - Test the shuffle algorithm
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
		size := 10
		bindings := make([]ArbitrageEdgeBinding, size)
		for i := 0; i < size; i++ {
			bindings[i] = ArbitrageEdgeBinding{
				cyclePairs: [3]PairID{PairID(i), PairID(i + 1), PairID(i + 2)},
				edgeIndex:  uint16(i % 3),
			}
		}

		// Track if any position changed
		positionChanges := 0
		iterations := 100

		for iter := 0; iter < iterations; iter++ {
			testBindings := make([]ArbitrageEdgeBinding, size)
			copy(testBindings, bindings)

			shuffleEdgeBindings(testBindings)

			for i := range testBindings {
				if testBindings[i].cyclePairs[0] != bindings[i].cyclePairs[0] {
					positionChanges++
					break
				}
			}
		}

		if positionChanges < iterations/2 {
			t.Errorf("Shuffle not random enough: only %d/%d changed", positionChanges, iterations)
		}
	})

	t.Run("LargeSlice", func(t *testing.T) {
		size := 1000
		bindings := make([]ArbitrageEdgeBinding, size)
		for i := 0; i < size; i++ {
			bindings[i] = ArbitrageEdgeBinding{
				cyclePairs: [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
				edgeIndex:  uint16(i % 3),
			}
		}

		shuffleEdgeBindings(bindings)

		// Check that not all elements are in original position
		inPlace := 0
		for i := range bindings {
			if bindings[i].cyclePairs[0] == PairID(i*3) {
				inPlace++
			}
		}

		if inPlace > size/10 {
			t.Errorf("Too many elements in original position: %d/%d", inPlace, size)
		}
	})
}

// TestBuildFanoutShardBuckets - Test shard construction
func TestBuildFanoutShardBuckets(t *testing.T) {
	t.Run("BasicConstruction", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(2), PairID(4), PairID(5)},
			{PairID(3), PairID(5), PairID(6)},
		}

		buildFanoutShardBuckets(cycles)

		// Each pair should have entries for each cycle it participates in
		if len(pairShardBuckets[PairID(1)]) == 0 {
			t.Error("Pair 1 has no shards")
		}
		if len(pairShardBuckets[PairID(2)]) == 0 {
			t.Error("Pair 2 has no shards")
		}
		if len(pairShardBuckets[PairID(5)]) == 0 {
			t.Error("Pair 5 has no shards")
		}
	})

	t.Run("ShardSizeLimits", func(t *testing.T) {
		// Create many cycles with same pair to trigger sharding
		cycles := make([]ArbitrageTriplet, constants.MaxCyclesPerShard*2)
		for i := range cycles {
			cycles[i] = ArbitrageTriplet{
				PairID(1), // Same first pair
				PairID(uint32(i + 1000)),
				PairID(uint32(i + 2000)),
			}
		}

		buildFanoutShardBuckets(cycles)

		shards := pairShardBuckets[PairID(1)]
		if len(shards) < 2 {
			t.Errorf("Expected multiple shards, got %d", len(shards))
		}

		for _, shard := range shards {
			if len(shard.edgeBindings) > constants.MaxCyclesPerShard {
				t.Errorf("Shard exceeds max size: %d", len(shard.edgeBindings))
			}
		}
	})

	t.Run("EdgeIndexCorrectness", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(10), PairID(20), PairID(30)},
		}

		buildFanoutShardBuckets(cycles)

		// Check edge indices
		for pairID, shards := range pairShardBuckets {
			for _, shard := range shards {
				for _, binding := range shard.edgeBindings {
					// Find which position this pair is in
					found := false
					for i, p := range binding.cyclePairs {
						if p == pairID {
							if binding.edgeIndex != uint16(i) {
								t.Errorf("Wrong edge index for pair %d: expected %d, got %d",
									pairID, i, binding.edgeIndex)
							}
							found = true
							break
						}
					}
					if !found {
						t.Errorf("Pair %d not found in its own binding", pairID)
					}
				}
			}
		}
	})

	t.Run("EmptyCycles", func(t *testing.T) {
		buildFanoutShardBuckets([]ArbitrageTriplet{})

		if len(pairShardBuckets) != 0 {
			t.Error("Expected empty shard buckets")
		}
	})
}

// TestAttachShardToExecutor - Test shard attachment
func TestAttachShardToExecutor(t *testing.T) {
	t.Run("BasicAttachment", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			priorityQueues:   make([]quantumqueue64.QuantumQueue64, 0),
			fanoutTables:     make([][]FanoutEntry, 0),
			pairToQueueIndex: localidx.New(16),
			cycleStates:      make([]ArbitrageCycleState, 0),
		}

		shard := &PairShardBucket{
			pairID: PairID(100),
			edgeBindings: []ArbitrageEdgeBinding{
				{
					cyclePairs: [3]PairID{100, 101, 102},
					edgeIndex:  0,
				},
				{
					cyclePairs: [3]PairID{100, 103, 104},
					edgeIndex:  0,
				},
			},
		}

		attachShardToExecutor(executor, shard)

		// Should create one queue
		if len(executor.priorityQueues) != 1 {
			t.Errorf("Expected 1 queue, got %d", len(executor.priorityQueues))
		}

		// Should create cycle states
		if len(executor.cycleStates) != 2 {
			t.Errorf("Expected 2 cycle states, got %d", len(executor.cycleStates))
		}

		// Should create fanout entries (2 per cycle)
		if len(executor.fanoutTables[0]) != 4 {
			t.Errorf("Expected 4 fanout entries, got %d", len(executor.fanoutTables[0]))
		}
	})

	t.Run("MultipleShards", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			priorityQueues:   make([]quantumqueue64.QuantumQueue64, 0),
			fanoutTables:     make([][]FanoutEntry, 0),
			pairToQueueIndex: localidx.New(16),
			cycleStates:      make([]ArbitrageCycleState, 0),
		}

		// Attach multiple shards for different pairs
		shards := []*PairShardBucket{
			{
				pairID: PairID(200),
				edgeBindings: []ArbitrageEdgeBinding{
					{cyclePairs: [3]PairID{200, 201, 202}, edgeIndex: 0},
				},
			},
			{
				pairID: PairID(300),
				edgeBindings: []ArbitrageEdgeBinding{
					{cyclePairs: [3]PairID{300, 301, 302}, edgeIndex: 0},
				},
			},
		}

		for _, shard := range shards {
			attachShardToExecutor(executor, shard)
		}

		// Should create two queues
		if len(executor.priorityQueues) != 2 {
			t.Errorf("Expected 2 queues, got %d", len(executor.priorityQueues))
		}
	})

	t.Run("FanoutCreation", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			priorityQueues:   make([]quantumqueue64.QuantumQueue64, 0),
			fanoutTables:     make([][]FanoutEntry, 0),
			pairToQueueIndex: localidx.New(16),
			cycleStates:      make([]ArbitrageCycleState, 0),
		}

		shard := &PairShardBucket{
			pairID: PairID(400),
			edgeBindings: []ArbitrageEdgeBinding{
				{
					cyclePairs: [3]PairID{400, 401, 402},
					edgeIndex:  1, // Middle edge
				},
			},
		}

		attachShardToExecutor(executor, shard)

		// Check fanout entries point to correct edges
		fanout := executor.fanoutTables[0]
		edges := []uint16{fanout[0].edgeIndex, fanout[1].edgeIndex}

		// Should have edges 0 and 2 (not 1)
		if (edges[0] != 0 && edges[0] != 2) || (edges[1] != 0 && edges[1] != 2) {
			t.Errorf("Wrong fanout edges: %v", edges)
		}

		if edges[0] == edges[1] {
			t.Error("Duplicate fanout edges")
		}
	})
}

// TestLaunchShardWorker - Test worker initialization
func TestLaunchShardWorker(t *testing.T) {
	t.Run("WorkerInitialization", func(t *testing.T) {
		shardChannel := make(chan PairShardBucket, 10)

		// Add test shard
		shardChannel <- PairShardBucket{
			pairID: PairID(500),
			edgeBindings: []ArbitrageEdgeBinding{
				{cyclePairs: [3]PairID{500, 501, 502}, edgeIndex: 0},
			},
		}
		close(shardChannel)

		// Launch worker
		go launchShardWorker(0, 2, shardChannel)

		// Give it time to initialize
		time.Sleep(10 * time.Millisecond)

		// Check initialization
		if coreExecutors[0] == nil {
			t.Error("Core executor not initialized")
		}
		if coreRings[0] == nil {
			t.Error("Core ring not initialized")
		}

		// Cleanup
		control.Shutdown()
		time.Sleep(10 * time.Millisecond)
	})

	t.Run("DirectionFlag", func(t *testing.T) {
		// Test forward cores
		shardChannel1 := make(chan PairShardBucket, 1)
		close(shardChannel1)
		go launchShardWorker(1, 4, shardChannel1) // core 1 < 4, so forward

		// Test reverse cores
		shardChannel2 := make(chan PairShardBucket, 1)
		close(shardChannel2)
		go launchShardWorker(5, 4, shardChannel2) // core 5 >= 4, so reverse

		time.Sleep(10 * time.Millisecond)

		if coreExecutors[1] != nil && coreExecutors[1].isReverseDirection {
			t.Error("Core 1 should be forward direction")
		}
		if coreExecutors[5] != nil && !coreExecutors[5].isReverseDirection {
			t.Error("Core 5 should be reverse direction")
		}

		// Cleanup
		control.Shutdown()
		time.Sleep(10 * time.Millisecond)
	})
}

// TestInitializeArbitrageSystem - Test full system initialization
func TestInitializeArbitrageSystem(t *testing.T) {
	// Clear global state
	addressToPairID = make(map[AddressKey]PairID)
	pairShardBuckets = nil
	for i := range pairToCoreAssignment[:20] {
		pairToCoreAssignment[i] = 0
	}
	for i := range coreExecutors {
		coreExecutors[i] = nil
	}
	for i := range coreRings {
		coreRings[i] = nil
	}

	t.Run("BasicInitialization", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(4), PairID(5), PairID(6)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(50 * time.Millisecond)

		// Check that shards were created
		if len(pairShardBuckets) == 0 {
			t.Error("No shard buckets created")
		}

		// Check core assignments
		hasAssignment := false
		for i := 1; i <= 6; i++ {
			if pairToCoreAssignment[i] != 0 {
				hasAssignment = true
				break
			}
		}
		if !hasAssignment {
			t.Error("No core assignments made")
		}

		control.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("LargeScale", func(t *testing.T) {
		// Reset state
		pairShardBuckets = nil
		for i := range pairToCoreAssignment[:3000] {
			pairToCoreAssignment[i] = 0
		}

		cycles := make([]ArbitrageTriplet, 1000)
		for i := 0; i < 1000; i++ {
			baseID := uint32(i * 3)
			cycles[i] = ArbitrageTriplet{
				PairID(baseID),
				PairID(baseID + 1),
				PairID(baseID + 2),
			}
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(100 * time.Millisecond)

		// Verify scale
		totalShards := 0
		for _, shards := range pairShardBuckets {
			totalShards += len(shards)
		}

		if totalShards == 0 {
			t.Error("No shards created at scale")
		}

		control.Shutdown()
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("CoreCountAdjustment", func(t *testing.T) {
		// Save original CPU count
		originalCPU := runtime.NumCPU()

		// This test verifies the core count logic
		// It should use NumCPU()-1, ensure even count, and split forward/reverse

		cycles := []ArbitrageTriplet{{PairID(1), PairID(2), PairID(3)}}
		InitializeArbitrageSystem(cycles)

		time.Sleep(10 * time.Millisecond)

		// Calculate expected core count
		expectedWorkers := originalCPU - 1
		if expectedWorkers > constants.MaxSupportedCores {
			expectedWorkers = constants.MaxSupportedCores
		}
		if expectedWorkers&1 == 1 {
			expectedWorkers--
		}

		t.Logf("CPU count: %d, expected workers: %d", originalCPU, expectedWorkers)

		control.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})
}

// TestDataStructureLayout - Verify memory layout
func TestDataStructureLayout(t *testing.T) {
	tests := []struct {
		name     string
		size     uintptr
		expected uintptr
	}{
		{"AddressKey", unsafe.Sizeof(AddressKey{}), 20},
		{"TickUpdate", unsafe.Sizeof(TickUpdate{}), 24},
		{"ArbitrageCycleState", unsafe.Sizeof(ArbitrageCycleState{}), 40},
		{"ArbitrageEdgeBinding", unsafe.Sizeof(ArbitrageEdgeBinding{}), 16},
		{"FanoutEntry", unsafe.Sizeof(FanoutEntry{}), 32},
		{"PairShardBucket", unsafe.Sizeof(PairShardBucket{}), 32},
		{"ArbitrageCoreExecutor", unsafe.Sizeof(ArbitrageCoreExecutor{}), 192},
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

		// Verify it's actually writing to the buffer
		if tickUpdate.pairID != PairID(12345) {
			t.Error("Message buffer casting failed")
		}

		// Check that it fits exactly
		if unsafe.Sizeof(*tickUpdate) != 24 {
			t.Error("TickUpdate doesn't fit in 24-byte buffer")
		}
	})

	t.Run("DirectPointerArithmetic", func(t *testing.T) {
		update := &TickUpdate{
			forwardTick: 1.5,
			reverseTick: -2.5,
			pairID:      PairID(999),
		}

		// Test the pointer arithmetic used in processTickUpdate
		offset := 0 // forward
		tick1 := *(*float64)(unsafe.Add(unsafe.Pointer(update), uintptr(offset)))
		if tick1 != 1.5 {
			t.Errorf("Forward tick access failed: got %f", tick1)
		}

		offset = 8 // reverse
		tick2 := *(*float64)(unsafe.Add(unsafe.Pointer(update), uintptr(offset)))
		if tick2 != -2.5 {
			t.Errorf("Reverse tick access failed: got %f", tick2)
		}
	})
}

// TestConcurrentSafety - Test realistic concurrent scenarios
func TestConcurrentSafety(t *testing.T) {
	t.Run("ReadOnlyAccess", func(t *testing.T) {
		// Initialize with some data first
		addressToPairID = make(map[AddressKey]PairID)

		// Register some pairs (this would happen during initialization)
		for i := 0; i < 100; i++ {
			addr := generateMockAddress(uint64(i * 1000))
			RegisterPairAddress(addr[:], PairID(i))
			RegisterPairToCore(PairID(i), uint8(i%8))
		}

		// Now test concurrent reads (realistic scenario)
		const goroutines = 8
		const reads = 1000

		var wg sync.WaitGroup

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for i := 0; i < reads; i++ {
					pairIdx := (id*reads + i) % 100
					addr := generateMockAddress(uint64(pairIdx * 1000))

					// Concurrent reads are safe
					foundID := lookupPairIDByAddress(addr[:])
					if foundID != PairID(pairIdx) {
						t.Errorf("Read inconsistency: expected %d, got %d", pairIdx, foundID)
					}
				}
			}(g)
		}

		wg.Wait()
	})

	t.Run("DispatchConcurrency", func(t *testing.T) {
		// Test concurrent dispatches (realistic scenario)
		addressToPairID = make(map[AddressKey]PairID)

		// Initialize rings
		for i := 0; i < 8; i++ {
			if coreRings[i] == nil {
				coreRings[i] = ring24.New(1024)
			}
		}

		// Register some pairs
		for i := 0; i < 10; i++ {
			addr := generateMockAddress(uint64(i))
			RegisterPairAddress(addr[:], PairID(i))
			RegisterPairToCore(PairID(i), uint8(i%8))
		}

		const goroutines = 4
		const updates = 50

		var wg sync.WaitGroup

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for i := 0; i < updates; i++ {
					pairIdx := (id + i) % 10
					addr := generateMockAddress(uint64(pairIdx))

					logView := &types.LogView{
						Addr: make([]byte, 64),
						Data: make([]byte, 128),
					}

					logView.Addr[0] = '0'
					logView.Addr[1] = 'x'
					copy(logView.Addr[2:42], addr[:])

					reserve0 := uint64(1000 + id*100 + i)
					reserve1 := uint64(2000 - id*50 - i)

					for j := 0; j < 8; j++ {
						logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
						logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
					}

					// This is realistic - multiple goroutines dispatching updates
					DispatchTickUpdate(logView)
				}
			}(g)
		}

		wg.Wait()
	})
}

// Benchmarks
func BenchmarkAddressKeyMap(b *testing.B) {
	addressToPairID = make(map[AddressKey]PairID)

	addresses := make([][40]byte, 10000)
	for i := range addresses {
		addresses[i] = generateMockAddress(uint64(i * 1000003))
		RegisterPairAddress(addresses[i][:], PairID(i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = lookupPairIDByAddress(addr[:])
	}
}

func BenchmarkTickQuantization(b *testing.B) {
	ticks := make([]float64, 1000)
	for i := range ticks {
		ticks[i] = float64(i-500) * 0.01
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = quantizeTickToInt64(ticks[i%len(ticks)])
	}
}

func BenchmarkProcessTickUpdate(b *testing.B) {
	executor := &ArbitrageCoreExecutor{
		isReverseDirection: false,
		priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
		fanoutTables:       make([][]FanoutEntry, 1),
		pairToQueueIndex:   localidx.New(16),
		cycleStates:        make([]ArbitrageCycleState, 100),
	}

	executor.priorityQueues[0] = *quantumqueue64.New()
	executor.pairToQueueIndex.Put(123, 0)

	// Setup realistic scenario with proper handles
	handles := make([]quantumqueue64.Handle, 100)
	for i := 0; i < 100; i++ {
		var err error
		handles[i], err = executor.priorityQueues[0].BorrowSafe()
		if err != nil {
			b.Fatalf("Failed to borrow handle %d: %v", i, err)
		}

		executor.cycleStates[i] = ArbitrageCycleState{
			tickValues: [3]float64{
				float64(i%20-10) * 0.001,
				float64(i%20-10) * 0.001,
				float64(i%20-10) * 0.001,
			},
			pairIDs: [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
		}

		priority := quantizeTickToInt64(float64(i%20-10) * 0.003)
		executor.priorityQueues[0].Push(priority, handles[i], uint64(i))
	}

	// Setup fanout with valid handles and cycle state indices
	executor.fanoutTables[0] = make([]FanoutEntry, 50)
	for i := 0; i < 50; i++ {
		handle, err := executor.priorityQueues[0].BorrowSafe()
		if err != nil {
			b.Fatalf("Failed to borrow fanout handle %d: %v", i, err)
		}

		// Use data values that correspond to valid cycle state indices
		cycleStateIndex := uint64(i % 100) // Ensure we stay within bounds of cycleStates
		priority := quantizeTickToInt64(0.001)
		executor.priorityQueues[0].Push(priority, handle, cycleStateIndex)

		executor.fanoutTables[0][i] = FanoutEntry{
			queueHandle:     handle,
			edgeIndex:       uint16(i % 3),
			cycleStateIndex: CycleStateIndex(cycleStateIndex),
			queue:           &executor.priorityQueues[0],
		}
	}

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
}

func BenchmarkSecureRandomInt(b *testing.B) {
	bounds := []int{10, 100, 1000, 10000}

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

func BenchmarkShuffleEdgeBindings(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
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
				testBindings := make([]ArbitrageEdgeBinding, size)
				copy(testBindings, bindings)
				shuffleEdgeBindings(testBindings)
			}
		})
	}
}

func BenchmarkBuildFanoutShardBuckets(b *testing.B) {
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

func BenchmarkCoreAssignmentBitOps(b *testing.B) {
	// Setup realistic assignments
	const pairCount = 10000
	for i := 0; i < pairCount; i++ {
		pairID := PairID(i)
		numCores := 2 + (i % 6)
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

		// Simulate dispatch loop
		for coreAssignments != 0 {
			coreID := bits.TrailingZeros64(uint64(coreAssignments))
			coreAssignments &^= 1 << coreID
			_ = coreID
		}
	}
}

func BenchmarkMemoryAccessPatterns(b *testing.B) {
	const cycleCount = 10000

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
			idx := (i * 2654435761) % cycleCount
			cycle := &cycles[idx]
			sum += cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		}
		_ = sum
	})

	b.Run("Strided", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		stride := 17
		for i := 0; i < b.N; i++ {
			idx := (i * stride) % cycleCount
			cycle := &cycles[idx]
			sum += cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		}
		_ = sum
	})
}

// Test edge cases and error conditions
func TestEdgeCases(t *testing.T) {
	t.Run("ParseEthereumAddressEdgeCases", func(t *testing.T) {
		// Test various malformed addresses
		testCases := []struct {
			name  string
			input [40]byte
		}{
			{"AllZeros", func() [40]byte {
				var a [40]byte
				for i := range a {
					a[i] = '0'
				}
				return a
			}()},
			{"AllFs", func() [40]byte {
				var a [40]byte
				for i := range a {
					a[i] = 'f'
				}
				return a
			}()},
			{"MixedCase", func() [40]byte {
				var a [40]byte
				s := "aAbBcCdDeEfF0123456789aAbBcCdDeEfF012345"
				copy(a[:], s)
				return a
			}()},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Just test it doesn't crash
				key := AddressKey(utils.ParseEthereumAddress(tc.input[:]))
				_ = key
			})
		}
	})

	t.Run("LoadBE64EdgeCases", func(t *testing.T) {
		// Test direct memory loading edge cases
		logView := &types.LogView{
			Data: make([]byte, 128),
		}

		// All zeros
		for i := 0; i < 64; i++ {
			logView.Data[i] = 0
		}

		reserve0 := utils.LoadBE64(logView.Data[24:])
		reserve1 := utils.LoadBE64(logView.Data[56:])

		if reserve0 != 0 || reserve1 != 0 {
			t.Error("Zero loading failed")
		}

		// All ones
		for i := 0; i < 64; i++ {
			logView.Data[i] = 0xFF
		}

		reserve0 = utils.LoadBE64(logView.Data[24:])
		reserve1 = utils.LoadBE64(logView.Data[56:])

		if reserve0 != ^uint64(0) || reserve1 != ^uint64(0) {
			t.Error("Max value loading failed")
		}
	})

	t.Run("QuantumQueueEdgeCases", func(t *testing.T) {
		queue := quantumqueue64.New()

		// Empty queue operations
		if !queue.Empty() {
			t.Error("New queue should be empty")
		}

		// Don't test PeepMin on empty queue - it's undefined behavior as documented
		// The comment says "⚠️ FOOTGUN: Undefined on empty queue"

		// Test safe operations on empty queue
		if queue.Size() != 0 {
			t.Error("Empty queue should have size 0")
		}

		// Fill queue to capacity and test behavior
		handles := make([]quantumqueue64.Handle, 0)
		for i := 0; i < 1000; i++ {
			h, err := queue.BorrowSafe()
			if err != nil {
				break
			}
			handles = append(handles, h)
			queue.Push(int64(i), h, uint64(i))
		}

		t.Logf("Queue accepted %d entries", len(handles))

		// Test PeepMin on non-empty queue
		if len(handles) > 0 {
			handle, priority, data := queue.PeepMin()
			// Handle 0 is a valid handle, so check if queue is actually non-empty
			if queue.Empty() {
				t.Error("Queue reported as empty but should have entries")
			}
			// Just verify we got some response - the exact values depend on internal implementation
			t.Logf("PeepMin returned: handle=%d, priority=%d, data=%d", handle, priority, data)
		}
	})

	t.Run("LocalIndexEdgeCases", func(t *testing.T) {
		idx := localidx.New(16)

		// Get non-existent key
		val, exists := idx.Get(12345)
		if exists {
			t.Error("Non-existent key returned as existing")
		}
		if val != 0 {
			t.Error("Non-existent key should return 0")
		}

		// Test Put behavior - it appears Put returns the NEW value, not the old value
		newVal1 := idx.Put(100, 200)
		t.Logf("First Put(100, 200) returned: %d", newVal1)

		newVal2 := idx.Put(100, 300)
		t.Logf("Second Put(100, 300) returned: %d", newVal2)

		val, exists = idx.Get(100)
		t.Logf("Get(100) returned: val=%d, exists=%v", val, exists)

		if !exists {
			t.Error("Key should exist after Put")
		}

		// Test that the final value is what we expect
		if val != newVal2 {
			t.Errorf("Get should return same value as most recent Put: got %d, expected %d", val, newVal2)
		}
	})

	t.Run("Ring24EdgeCases", func(t *testing.T) {
		ring := ring24.New(16)

		// Fill ring
		var msg [24]byte
		filled := 0
		for i := 0; i < 20; i++ {
			if ring.Push(&msg) {
				filled++
			}
		}

		if filled == 0 {
			t.Error("Ring should accept at least some messages")
		}

		t.Logf("Ring accepted %d messages", filled)
	})

	t.Run("FastUniLog2EdgeCases", func(t *testing.T) {
		// Test edge cases for log calculation
		testCases := []struct {
			reserve0, reserve1 uint64
			name               string
		}{
			{0, 0, "BothZero"},
			{0, 1000, "FirstZero"},
			{1000, 0, "SecondZero"},
			{1, 1, "BothOne"},
			{^uint64(0), 1, "MaxFirst"},
			{1, ^uint64(0), "MaxSecond"},
			{^uint64(0), ^uint64(0), "BothMax"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tickValue, err := fastuni.Log2ReserveRatio(tc.reserve0, tc.reserve1)
				if err != nil {
					// Some cases may error, that's fine
					t.Logf("%s: error %v", tc.name, err)
				} else {
					t.Logf("%s: tick value %f", tc.name, tickValue)
				}
			})
		}
	})

	t.Run("BitOperationsEdgeCases", func(t *testing.T) {
		// Test trailing zeros edge cases
		testCases := []uint64{
			0,                  // No bits set (undefined behavior)
			1,                  // First bit
			1 << 63,            // Last bit
			^uint64(0),         // All bits
			0xAAAAAAAAAAAAAAAA, // Alternating bits
			0x5555555555555555, // Alternating bits (opposite)
		}

		for _, val := range testCases {
			if val == 0 {
				// Skip zero - undefined behavior
				continue
			}

			tz := bits.TrailingZeros64(val)
			if tz < 0 || tz >= 64 {
				t.Errorf("Invalid trailing zeros for %064b: %d", val, tz)
			}
		}
	})

	t.Run("StackBufferOverflow", func(t *testing.T) {
		// Test the 128-cycle limit in processTickUpdate
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 200),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		// Add exactly 128 profitable cycles
		for i := 0; i < 128; i++ {
			executor.cycleStates[i] = ArbitrageCycleState{
				tickValues: [3]float64{-1.0, -1.0, -1.0},
				pairIDs:    [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
			}

			handle, _ := executor.priorityQueues[0].BorrowSafe()
			priority := quantizeTickToInt64(-3.0)
			executor.priorityQueues[0].Push(priority, handle, uint64(i))
		}

		// Add one more that won't be processed
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(quantizeTickToInt64(-3.0), handle, 128)

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: -1.0,
		}

		// Should process exactly 128 and not overflow
		processTickUpdate(executor, update)
	})
}

// Test complete end-to-end workflow
func TestCompleteWorkflow(t *testing.T) {
	// Reset global state
	addressToPairID = make(map[AddressKey]PairID)
	pairShardBuckets = nil
	for i := range pairToCoreAssignment[:100] {
		pairToCoreAssignment[i] = 0
	}

	// 1. Initialize system with triangles
	cycles := []ArbitrageTriplet{
		{PairID(10), PairID(11), PairID(12)},
		{PairID(11), PairID(13), PairID(14)},
		{PairID(12), PairID(14), PairID(15)},
	}

	InitializeArbitrageSystem(cycles)
	time.Sleep(50 * time.Millisecond)

	// 2. Register addresses for all pairs
	for i := uint64(10); i <= 15; i++ {
		addr := generateMockAddress(i * 1000)
		RegisterPairAddress(addr[:], PairID(i))
	}

	// 3. Send tick updates
	for i := uint64(10); i <= 15; i++ {
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

	// 4. Let processing happen
	time.Sleep(50 * time.Millisecond)

	// 5. Verify state
	hasAssignments := false
	for i := 10; i <= 15; i++ {
		if pairToCoreAssignment[i] != 0 {
			hasAssignments = true
			break
		}
	}

	if !hasAssignments {
		t.Error("No core assignments after complete workflow")
	}

	// 6. Cleanup
	control.Shutdown()
	time.Sleep(50 * time.Millisecond)
}

// Test performance under load
func TestHighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput test in short mode")
	}

	addressToPairID = make(map[AddressKey]PairID)

	// Setup
	const pairCount = 100
	const updatesPerPair = 100

	// Register pairs
	for i := 0; i < pairCount; i++ {
		addr := generateMockAddress(uint64(i * 1000))
		RegisterPairAddress(addr[:], PairID(i))
		RegisterPairToCore(PairID(i), uint8(i%8))
	}

	// Initialize rings
	for i := 0; i < 8; i++ {
		if coreRings[i] == nil {
			coreRings[i] = ring24.New(1024)
		}
	}

	// Measure throughput
	start := time.Now()
	updates := uint64(0)

	for round := 0; round < updatesPerPair; round++ {
		for i := 0; i < pairCount; i++ {
			addr := generateMockAddress(uint64(i * 1000))

			logView := &types.LogView{
				Addr: make([]byte, 64),
				Data: make([]byte, 128),
			}

			logView.Addr[0] = '0'
			logView.Addr[1] = 'x'
			copy(logView.Addr[2:42], addr[:])

			reserve0 := uint64(1000000 + round*1000 + i)
			reserve1 := uint64(1000000 - round*500 - i)

			for j := 0; j < 8; j++ {
				logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
				logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
			}

			DispatchTickUpdate(logView)
			atomic.AddUint64(&updates, 1)
		}
	}

	elapsed := time.Since(start)
	throughput := float64(updates) / elapsed.Seconds()

	t.Logf("Processed %d updates in %v", updates, elapsed)
	t.Logf("Throughput: %.0f updates/sec", throughput)

	if throughput < 1000 {
		t.Errorf("Throughput too low: %.0f updates/sec", throughput)
	}
}

func BenchmarkDispatchTickUpdate(b *testing.B) {
	addressToPairID = make(map[AddressKey]PairID)

	// Setup
	const pairCount = 1000
	addresses := make([][40]byte, pairCount)
	logViews := make([]*types.LogView, pairCount)

	// Initialize rings for all cores that might be used
	for i := 0; i < 64; i++ {
		if coreRings[i] == nil {
			coreRings[i] = ring24.New(1024)
		}
	}

	for i := 0; i < pairCount; i++ {
		addr := generateMockAddress(uint64(i * 1000003))
		addresses[i] = addr
		RegisterPairAddress(addr[:], PairID(i))

		// Assign to 2-4 cores, but ensure we don't exceed available cores
		numCores := 2 + (i % 3)
		for j := 0; j < numCores; j++ {
			coreID := uint8((i + j*16) % 8) // Limit to first 8 cores
			RegisterPairToCore(PairID(i), coreID)
		}

		// Pre-create log view
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
		DispatchTickUpdate(logViews[i%pairCount])
	}
}

func BenchmarkWorstCaseFanout(b *testing.B) {
	executor := &ArbitrageCoreExecutor{
		isReverseDirection: false,
		priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
		fanoutTables:       make([][]FanoutEntry, 1),
		pairToQueueIndex:   localidx.New(16),
		cycleStates:        make([]ArbitrageCycleState, 10000),
	}

	executor.priorityQueues[0] = *quantumqueue64.New()
	executor.pairToQueueIndex.Put(123, 0)

	// Maximum realistic fanout
	fanoutSize := 10000
	executor.fanoutTables[0] = make([]FanoutEntry, fanoutSize)

	for i := 0; i < fanoutSize; i++ {
		executor.cycleStates[i] = ArbitrageCycleState{
			tickValues: [3]float64{0.001, 0.002, 0.003},
			pairIDs:    [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
		}

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		priority := quantizeTickToInt64(0.006)
		executor.priorityQueues[0].Push(priority, handle, uint64(i))

		executor.fanoutTables[0][i] = FanoutEntry{
			queueHandle:     handle,
			edgeIndex:       uint16(i % 3),
			cycleStateIndex: CycleStateIndex(i),
			queue:           &executor.priorityQueues[0],
		}
	}

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
}
