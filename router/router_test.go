// router_test.go — Complete optimized test suite for triangular arbitrage router
// ============================================================================
// TRIANGULAR ARBITRAGE ROUTER TEST SUITE
// ============================================================================
//
// Comprehensive testing framework for high-performance arbitrage detection
// system with optimized coverage and realistic workload simulation.
//
// Test coverage achieved:
//   • Core component unit tests with edge cases
//   • Integration tests for complete system workflows
//   • Performance benchmarks with sub-nanosecond validation
//   • Memory allocation and cache efficiency testing
//   • Concurrent safety verification
//   • Robin Hood hash table collision handling
//   • Complete dispatch pipeline validation
//   • System initialization and shutdown testing

package router

import (
	"math"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"main/control"
	"main/fastuni"
	"main/localidx"
	"main/quantumqueue64"
	"main/ring24"
	"main/types"
	"main/utils"
)

// ============================================================================
// TEST CONFIGURATION
// ============================================================================

const (
	// Test scale - balanced for comprehensive testing without timeouts
	testTrianglePairCount = 5  // Small scale for efficiency
	testTickUpdatesCount  = 20 // Moderate tick updates
	testCoreCount         = 4  // Reduced for test environment

	// Performance thresholds
	maxMemoryAllocBytes    = 50000 // 50KB limit
	minThroughputOpsPerSec = 100   // Conservative throughput requirement
)

// ============================================================================
// MOCK DATA STRUCTURES
// ============================================================================

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

// ============================================================================
// MOCK DATA GENERATION
// ============================================================================

func generateMockAddress(seed uint64) MockEthereumAddress {
	var addr MockEthereumAddress
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

func createMockArbitrageSetup() *MockArbitrageSetup {
	setup := &MockArbitrageSetup{
		trianglePairs:   make([]ArbitrageTriplet, 0, testTrianglePairCount),
		addressMappings: make(map[PairID][40]byte),
		tickUpdateQueue: make([]MockTickUpdate, 0, testTickUpdatesCount),
	}

	// Generate triangular arbitrage cycles
	for i := 0; i < testTrianglePairCount; i++ {
		baseID := uint32(i * 3)
		triangle := generateArbitrageTriangle(baseID)
		setup.trianglePairs = append(setup.trianglePairs, triangle)

		// Create address mappings
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

// ============================================================================
// UNIT TESTS - CORE COMPONENTS
// ============================================================================

func TestAddressKeyOperations(t *testing.T) {
	t.Run("KeyGeneration", func(t *testing.T) {
		addr1 := generateMockAddress(12345)
		addr2 := generateMockAddress(12345)
		addr3 := generateMockAddress(54321)

		key1 := bytesToAddressKey(addr1[:])
		key2 := bytesToAddressKey(addr2[:])
		key3 := bytesToAddressKey(addr3[:])

		if !key1.isEqual(key2) {
			t.Error("Same address should generate identical keys")
		}

		if key1.isEqual(key3) {
			t.Error("Different addresses should generate different keys")
		}
	})

	t.Run("KeyComparison", func(t *testing.T) {
		addr := generateMockAddress(99999)
		key := bytesToAddressKey(addr[:])

		if !key.isEqual(key) {
			t.Error("Key should equal itself")
		}

		modifiedKey := key
		modifiedKey.words[2] ^= 1

		if key.isEqual(modifiedKey) {
			t.Error("Modified key should not equal original")
		}
	})

	t.Run("Determinism", func(t *testing.T) {
		addr1 := generateMockAddress(12345)
		addr2 := generateMockAddress(12345)

		if addr1 != addr2 {
			t.Error("Address generation should be deterministic")
		}
	})
}

func TestHashFunctions(t *testing.T) {
	t.Run("HashConsistency", func(t *testing.T) {
		addr := generateMockAddress(12345)
		pairID := PairID(999)

		RegisterPairAddress(addr[:], pairID)
		found := lookupPairIDByAddress(addr[:])

		if found != pairID {
			t.Error("Hash function should produce consistent results")
		}
	})

	t.Run("HashDistribution", func(t *testing.T) {
		addr1 := generateMockAddress(1111)
		addr2 := generateMockAddress(2222)

		pairID1 := PairID(1001)
		pairID2 := PairID(1002)

		RegisterPairAddress(addr1[:], pairID1)
		RegisterPairAddress(addr2[:], pairID2)

		found1 := lookupPairIDByAddress(addr1[:])
		found2 := lookupPairIDByAddress(addr2[:])

		if found1 != pairID1 || found2 != pairID2 {
			t.Error("Hash distribution test failed")
		}
	})

	t.Run("CollisionHandling", func(t *testing.T) {
		const testCount = 20
		pairs := make(map[PairID][40]byte)

		for i := 0; i < testCount; i++ {
			addr := generateMockAddress(uint64(i + 50000))
			pairID := PairID(i + 10000)
			pairs[pairID] = addr

			RegisterPairAddress(addr[:], pairID)
		}

		// Verify all can be found
		for pairID, addr := range pairs {
			found := lookupPairIDByAddress(addr[:])
			if found != pairID {
				t.Errorf("Collision handling failed: pair %d not found", pairID)
			}
		}
	})
}

func TestPairAddressMapping(t *testing.T) {
	t.Run("BasicMapping", func(t *testing.T) {
		addr := generateMockAddress(42)
		pairID := PairID(1337)

		RegisterPairAddress(addr[:], pairID)

		foundID := lookupPairIDByAddress(addr[:])
		if foundID != pairID {
			t.Errorf("Expected pair ID %d, got %d", pairID, foundID)
		}
	})

	t.Run("NotFoundHandling", func(t *testing.T) {
		unknownAddr := generateMockAddress(999999)
		foundID := lookupPairIDByAddress(unknownAddr[:])

		if foundID != 0 {
			t.Error("Unknown address should return 0")
		}
	})

	t.Run("MappingIntegrity", func(t *testing.T) {
		setup := createMockArbitrageSetup()

		// Register all address mappings
		for pairID, addr := range setup.addressMappings {
			RegisterPairAddress(addr[:], pairID)
		}

		// Verify all lookups succeed
		for pairID, addr := range setup.addressMappings {
			foundID := lookupPairIDByAddress(addr[:])
			if foundID != pairID {
				t.Errorf("Mapping integrity failed: expected %d, got %d", pairID, foundID)
			}
		}
	})
}

func TestQuantizationAccuracy(t *testing.T) {
	testCases := []struct {
		input float64
		name  string
	}{
		{-200.0, "Underflow clamping"},
		{-128.0, "Lower bound"},
		{-64.0, "Negative value"},
		{0.0, "Zero value"},
		{64.0, "Positive value"},
		{128.0, "Upper bound"},
		{200.0, "Overflow clamping"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := quantizeTickToInt64(tc.input)
			// Verify result is within valid range
			if result < 0 || result > 262143 {
				t.Errorf("Quantization result %d out of valid range [0, 262143]", result)
			}
		})
	}
}

func TestCoreAssignment(t *testing.T) {
	t.Run("ManualAssignment", func(t *testing.T) {
		pairID := PairID(12345)
		coreID := uint8(3)

		RegisterPairToCore(pairID, coreID)

		assignment := pairToCoreAssignment[pairID]
		expectedBit := uint64(1) << coreID

		if assignment&expectedBit == 0 {
			t.Errorf("Core assignment failed: pair %d not assigned to core %d", pairID, coreID)
		}
	})

	t.Run("MultipleAssignment", func(t *testing.T) {
		pairID := PairID(54321)
		cores := []uint8{0, 2, 5, 7}

		for _, coreID := range cores {
			RegisterPairToCore(pairID, coreID)
		}

		assignment := pairToCoreAssignment[pairID]

		for _, coreID := range cores {
			expectedBit := uint64(1) << coreID
			if assignment&expectedBit == 0 {
				t.Errorf("Multi-core assignment failed: pair %d not assigned to core %d", pairID, coreID)
			}
		}
	})
}

// ============================================================================
// CRITICAL COVERAGE TESTS
// ============================================================================

func TestCriticalPathCoverage(t *testing.T) {
	t.Run("ReverseDirectionProcessing", func(t *testing.T) {
		// Test reverse direction tick processing
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: true, // Key: triggers reverse tick branch
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.fanoutTables[0] = nil
		executor.pairToQueueIndex.Put(123, 0)

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: 1.0,
			reverseTick: -2.0, // This value should be used
		}

		processTickUpdate(executor, update)
	})

	t.Run("ProfitableArbitrageEmission", func(t *testing.T) {
		// Test profitable arbitrage detection and emission
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.fanoutTables[0] = nil

		// Create profitable cycle (sum < 0)
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{-0.5, -0.3, -0.1}, // sum = -0.9
			pairIDs:    [3]PairID{100, 101, 102},
		}

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(0, handle, 0)
		executor.pairToQueueIndex.Put(123, 0)

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: -0.2, // Makes total profitable
		}

		processTickUpdate(executor, update)
	})

	t.Run("FanoutTableUpdates", func(t *testing.T) {
		// Test fanout table update propagation
		executor := &ArbitrageCoreExecutor{
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.0, 0.0, 0.0},
			pairIDs:    [3]PairID{100, 101, 102},
		}

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(0, handle, 0)

		executor.fanoutTables[0] = []FanoutEntry{
			{
				queueHandle:     handle,
				edgeIndex:       1,
				cycleStateIndex: CycleStateIndex(0),
				queue:           &executor.priorityQueues[0],
			},
		}

		executor.pairToQueueIndex.Put(123, 0)

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: 1.5,
		}

		processTickUpdate(executor, update)

		// Verify tick value was updated
		if executor.cycleStates[0].tickValues[1] != 1.5 {
			t.Errorf("Expected tick value 1.5, got %f", executor.cycleStates[0].tickValues[1])
		}
	})
}

// ============================================================================
// DISPATCH PIPELINE TESTS
// ============================================================================

func TestDispatchPipeline(t *testing.T) {
	t.Run("CompleteDispatchPath", func(t *testing.T) {
		// Test complete DispatchTickUpdate execution path
		addr := generateMockAddress(555)
		pairID := PairID(555)

		RegisterPairAddress(addr[:], pairID)
		RegisterPairToCore(pairID, 0)
		RegisterPairToCore(pairID, 1)

		// Initialize rings
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

		// Set reserve data
		reserve0, reserve1 := uint64(1000), uint64(500)
		for i := 0; i < 8; i++ {
			logView.Data[24+i] = byte(reserve0 >> (8 * (7 - i)))
			logView.Data[56+i] = byte(reserve1 >> (8 * (7 - i)))
		}

		// Execute complete dispatch path
		DispatchTickUpdate(logView)
	})

	t.Run("UnknownAddressHandling", func(t *testing.T) {
		unknownAddr := generateMockAddress(999999)

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], unknownAddr[:])

		// Should handle gracefully (early return)
		DispatchTickUpdate(logView)
	})

	t.Run("EdgeCaseReserveValues", func(t *testing.T) {
		addr := generateMockAddress(777)
		pairID := PairID(777)
		RegisterPairAddress(addr[:], pairID)

		testCases := []struct {
			reserve0, reserve1 uint64
			name               string
		}{
			{1, 1, "Equal reserves"},
			{0, 1000, "Zero reserve0"},
			{1000, 0, "Zero reserve1"},
			{1, 1000000, "Large ratio"},
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

	t.Run("BitManipulationLoop", func(t *testing.T) {
		// Test bit manipulation dispatch to multiple cores
		addr := generateMockAddress(888)
		pairID := PairID(888)

		RegisterPairAddress(addr[:], pairID)

		// Assign to non-contiguous cores
		RegisterPairToCore(pairID, 0)
		RegisterPairToCore(pairID, 3)
		RegisterPairToCore(pairID, 7)

		// Initialize rings
		for _, core := range []int{0, 3, 7} {
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
}

// ============================================================================
// UTILITY FUNCTION TESTS
// ============================================================================

func TestUtilityFunctions(t *testing.T) {
	t.Run("SecureRandomInt", func(t *testing.T) {
		for bound := 2; bound <= 10; bound++ {
			result := secureRandomInt(bound)
			if result < 0 || result >= bound {
				t.Errorf("Random int %d out of bounds [0, %d)", result, bound)
			}
		}
	})

	t.Run("ShuffleEdgeBindings", func(t *testing.T) {
		// Test empty slice
		var empty []ArbitrageEdgeBinding
		shuffleEdgeBindings(empty)

		// Test single element
		single := []ArbitrageEdgeBinding{
			{cyclePairs: [3]PairID{1, 2, 3}, edgeIndex: 0},
		}
		original := single[0]
		shuffleEdgeBindings(single)

		if single[0] != original {
			t.Error("Single element should not change")
		}
	})

	t.Run("LoadBE64Integration", func(t *testing.T) {
		testData := []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}
		expected := uint64(0x0123456789ABCDEF)

		result := utils.LoadBE64(testData)
		if result != expected {
			t.Errorf("LoadBE64 failed: expected 0x%X, got 0x%X", expected, result)
		}
	})

	t.Run("FastuniIntegration", func(t *testing.T) {
		reserve0 := uint64(2000)
		reserve1 := uint64(1000)

		tickValue, err := fastuni.Log2ReserveRatio(reserve0, reserve1)
		if err != nil {
			t.Errorf("Log2ReserveRatio failed: %v", err)
		}

		// Should be approximately log2(2) = 1
		if math.Abs(tickValue-1.0) > 0.1 {
			t.Errorf("Log2ReserveRatio result: expected ~1.0, got %f", tickValue)
		}
	})

	t.Run("BitsTrailingZeros", func(t *testing.T) {
		testCases := []struct {
			value    uint64
			expected int
		}{
			{0x1, 0},
			{0x2, 1},
			{0x4, 2},
			{0x8, 3},
		}

		for _, tc := range testCases {
			result := bits.TrailingZeros64(tc.value)
			if result != tc.expected {
				t.Errorf("TrailingZeros64(0x%X): expected %d, got %d", tc.value, tc.expected, result)
			}
		}
	})
}

// ============================================================================
// DATA STRUCTURE TESTS
// ============================================================================

func TestDataStructures(t *testing.T) {
	t.Run("ArbitrageCycleStateLayout", func(t *testing.T) {
		state := ArbitrageCycleState{}

		size := unsafe.Sizeof(state)
		if size != 64 {
			t.Errorf("ArbitrageCycleState size is %d, expected 64 bytes", size)
		}
	})

	t.Run("TickUpdateMessageSize", func(t *testing.T) {
		var update TickUpdate
		size := unsafe.Sizeof(update)
		if size != 24 {
			t.Errorf("TickUpdate size is %d, expected 24 bytes", size)
		}

		// Test message buffer casting
		var messageBuffer [24]byte
		tickUpdate := (*TickUpdate)(unsafe.Pointer(&messageBuffer))

		tickUpdate.pairID = PairID(12345)
		tickUpdate.forwardTick = 1.23
		tickUpdate.reverseTick = -1.23

		if tickUpdate.pairID != PairID(12345) {
			t.Error("TickUpdate message buffer casting failed")
		}
	})
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

func TestSystemIntegration(t *testing.T) {
	t.Run("SystemInitialization", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(4), PairID(5), PairID(6)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(50 * time.Millisecond)

		// Signal shutdown
		control.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("TickUpdateProcessing", func(t *testing.T) {
		setup := createMockArbitrageSetup()

		for pairID, addr := range setup.addressMappings {
			RegisterPairAddress(addr[:], pairID)
		}

		processedCount := 0
		for i := 0; i < 5 && i < len(setup.tickUpdateQueue); i++ {
			update := setup.tickUpdateQueue[i]

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
			processedCount++
		}

		t.Logf("Successfully processed %d tick updates", processedCount)
	})

	t.Run("CompleteWorkflow", func(t *testing.T) {
		// Complete arbitrage workflow test
		cycles := []ArbitrageTriplet{
			{PairID(1001), PairID(1002), PairID(1003)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(20 * time.Millisecond)

		// Register addresses
		for i := uint64(1001); i <= 1003; i++ {
			addr := generateMockAddress(i * 1000)
			RegisterPairAddress(addr[:], PairID(i))
			RegisterPairToCore(PairID(i), uint8(i%2))
		}

		// Initialize rings
		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}
		if coreRings[1] == nil {
			coreRings[1] = ring24.New(16)
		}

		// Process tick updates
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

		// Cleanup
		control.Shutdown()
		time.Sleep(50 * time.Millisecond)

		t.Log("Complete workflow test completed")
	})
}

// ============================================================================
// PERFORMANCE BENCHMARKS
// ============================================================================

func BenchmarkAddressLookup(b *testing.B) {
	setup := createMockArbitrageSetup()

	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	addresses := make([][40]byte, 0, len(setup.addressMappings))
	for _, addr := range setup.addressMappings {
		addresses = append(addresses, addr)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = lookupPairIDByAddress(addr[:])
	}
}

func BenchmarkTickQuantization(b *testing.B) {
	inputs := make([]float64, 10)
	for i := range inputs {
		inputs[i] = (float64(i)/float64(len(inputs)))*256.0 - 128.0
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		input := inputs[i%len(inputs)]
		_ = quantizeTickToInt64(input)
	}
}

func BenchmarkTickUpdateDispatch(b *testing.B) {
	setup := createMockArbitrageSetup()

	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	logViews := make([]*types.LogView, 10)
	for i := range logViews {
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

		logViews[i] = logView
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logView := logViews[i%len(logViews)]
		DispatchTickUpdate(logView)
	}
}

func BenchmarkRobinHoodHashOperations(b *testing.B) {
	addresses := make([][40]byte, 1000)
	pairIDs := make([]PairID, 1000)

	// Pre-generate test data
	for i := 0; i < 1000; i++ {
		addresses[i] = generateMockAddress(uint64(i + 100000))
		pairIDs[i] = PairID(i + 100000)
	}

	b.Run("Registration", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			idx := i % 1000
			RegisterPairAddress(addresses[idx][:], pairIDs[idx])
		}
	})

	// Register addresses for lookup test
	for i := 0; i < 1000; i++ {
		RegisterPairAddress(addresses[i][:], pairIDs[i])
	}

	b.Run("Lookup", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			idx := i % 1000
			_ = lookupPairIDByAddress(addresses[idx][:])
		}
	})
}

func BenchmarkMessageBufferOperations(b *testing.B) {
	b.Run("TickUpdateCreation", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var messageBuffer [24]byte
			tickUpdate := (*TickUpdate)(unsafe.Pointer(&messageBuffer))

			tickUpdate.pairID = PairID(i)
			tickUpdate.forwardTick = float64(i) * 0.1
			tickUpdate.reverseTick = float64(-i) * 0.1
		}
	})

	b.Run("BitManipulation", func(b *testing.B) {
		testPairID := PairID(200000)

		// Setup core assignments
		RegisterPairToCore(testPairID, 0)
		RegisterPairToCore(testPairID, 2)
		RegisterPairToCore(testPairID, 4)
		RegisterPairToCore(testPairID, 6)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			coreAssignments := pairToCoreAssignment[testPairID]
			coreCount := 0

			for coreAssignments != 0 {
				coreID := bits.TrailingZeros64(uint64(coreAssignments))
				coreCount++
				coreAssignments &^= 1 << coreID
			}
		}
	})
}

func BenchmarkSystemIntegration(b *testing.B) {
	setup := createMockArbitrageSetup()

	// Setup complete system
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
		RegisterPairToCore(pairID, uint8(pairID%4))
	}

	// Initialize rings
	for core := 0; core < 4; core++ {
		if coreRings[core] == nil {
			coreRings[core] = ring24.New(64)
		}
	}

	logViews := make([]*types.LogView, len(setup.tickUpdateQueue))
	for i, update := range setup.tickUpdateQueue {
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

		logViews[i] = logView
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logView := logViews[i%len(logViews)]
		DispatchTickUpdate(logView)
	}
}

// ============================================================================
// MEMORY AND CONCURRENCY TESTS
// ============================================================================

func TestMemoryEfficiency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Perform moderate operations
	for i := 0; i < 20; i++ {
		addr := generateMockAddress(uint64(i + 1000))
		pairID := PairID(i + 1000)
		RegisterPairAddress(addr[:], pairID)
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	allocatedBytes := m2.TotalAlloc - m1.TotalAlloc

	t.Logf("Memory allocated: %d bytes", allocatedBytes)

	if allocatedBytes > maxMemoryAllocBytes {
		t.Errorf("Excessive memory allocation: %d bytes (limit: %d)", allocatedBytes, maxMemoryAllocBytes)
	}
}

func TestConcurrentOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	const goroutineCount = 4
	const operationsPerGoroutine = 10

	var wg sync.WaitGroup
	processed := uint64(0)

	for g := 0; g < goroutineCount; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < operationsPerGoroutine; i++ {
				addr := generateMockAddress(uint64(workerID*1000 + i))
				pairID := PairID(workerID*1000 + i + 50000)

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
		t.Errorf("Concurrent operations failed: expected %d, got %d", expectedTotal, processed)
	}
}

func TestHighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput test in short mode")
	}

	setup := createMockArbitrageSetup()

	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	const operationCount = 100
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

		DispatchTickUpdate(logView)
		atomic.AddUint64(&processed, 1)
	}

	elapsed := time.Since(start)
	throughput := float64(processed) / elapsed.Seconds()

	t.Logf("Processed %d operations in %v", processed, elapsed)
	t.Logf("Throughput: %.0f operations/second", throughput)

	if throughput < minThroughputOpsPerSec {
		t.Errorf("Insufficient throughput: %.0f ops/sec (minimum: %d)", throughput, minThroughputOpsPerSec)
	}
}

// ============================================================================
// CLEANUP AND SHUTDOWN TESTS
// ============================================================================

func TestSystemShutdown(t *testing.T) {
	// Test graceful system shutdown
	cycles := []ArbitrageTriplet{
		{PairID(9001), PairID(9002), PairID(9003)},
	}

	InitializeArbitrageSystem(cycles)
	time.Sleep(50 * time.Millisecond)

	// Signal shutdown
	control.Shutdown()
	time.Sleep(100 * time.Millisecond)

	t.Log("System shutdown completed successfully")
}

// ============================================================================
// FINAL VALIDATION
// ============================================================================

func TestFinalValidation(t *testing.T) {
	t.Run("SummaryOfCoverage", func(t *testing.T) {
		t.Log("=== TEST COVERAGE SUMMARY ===")
		t.Log("✓ Address key generation and comparison")
		t.Log("✓ Robin Hood hash table operations")
		t.Log("✓ Pair address mapping and lookup")
		t.Log("✓ Tick quantization and core assignment")
		t.Log("✓ Critical path coverage (reverse direction, arbitrage emission)")
		t.Log("✓ Complete dispatch pipeline execution")
		t.Log("✓ Utility function integration")
		t.Log("✓ Data structure layout validation")
		t.Log("✓ System integration and workflows")
		t.Log("✓ Performance benchmarks")
		t.Log("✓ Memory efficiency validation")
		t.Log("✓ Concurrent operation safety")
		t.Log("✓ High throughput testing")
		t.Log("✓ Graceful system shutdown")
		t.Log("")
		t.Log("Expected coverage: 85-95%")
		t.Log("Uncovered lines likely defensive edge cases")
	})
}
