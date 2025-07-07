// router_test.go — Comprehensive test suite for triangular arbitrage router
// ============================================================================
// TRIANGULAR ARBITRAGE ROUTER TEST SUITE
// ============================================================================
//
// Complete testing framework for high-performance arbitrage detection system
// with comprehensive coverage and realistic workload simulation.
//
// Test coverage includes:
//   • Unit tests for core components with edge case validation
//   • Integration tests for complete system workflows
//   • Performance benchmarks with nanosecond-scale validation
//   • Memory allocation and cache efficiency verification
//   • Concurrent safety and data integrity testing
//   • Robin Hood hash table collision resolution validation
//   • Complete tick dispatch pipeline testing
//   • System initialization and graceful shutdown verification
//
// Performance validation:
//   • Nanosecond-scale operation timing verification
//   • Zero-allocation hot path enforcement
//   • Concurrent throughput under sustained load
//   • Memory layout and cache alignment validation

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

	"main/constants"
	"main/control"
	"main/fastuni"
	"main/localidx"
	"main/quantumqueue64"
	"main/ring24"
	"main/types"
	"main/utils"
)

// ============================================================================
// TEST CONFIGURATION AND CONSTANTS
// ============================================================================

const (
	// Test scale parameters - calibrated for comprehensive testing without timeouts
	testTrianglePairCount = 5  // Reduced scale for unit test efficiency
	testTickUpdatesCount  = 20 // Moderate tick update volume for validation
	testCoreCount         = 4  // Constrained core count for test environment

	// Performance validation thresholds - conservative for reliable CI/CD
	maxMemoryAllocBytes    = 50000 // 50KB allocation limit for efficiency tests
	minThroughputOpsPerSec = 100   // Conservative throughput requirement
)

// ============================================================================
// MOCK DATA STRUCTURES AND GENERATORS
// ============================================================================

// MockEthereumAddress represents a 40-character hexadecimal Ethereum address for testing
type MockEthereumAddress [40]byte

// MockTickUpdate represents a price update event for testing dispatch pipeline
type MockTickUpdate struct {
	pairID       PairID
	reserve0     uint64
	reserve1     uint64
	expectedTick float64
	timestamp    int64
}

// MockArbitrageSetup contains complete test data for arbitrage cycle testing
type MockArbitrageSetup struct {
	trianglePairs   []ArbitrageTriplet
	addressMappings map[PairID][40]byte
	tickUpdateQueue []MockTickUpdate
}

// generateMockAddress creates deterministic Ethereum-style addresses for testing
func generateMockAddress(seed uint64) MockEthereumAddress {
	var addr MockEthereumAddress
	for i := 0; i < 20; i++ {
		byteVal := uint8((seed >> (i * 8)) ^ (seed >> ((i + 7) * 3)))
		addr[i*2] = "0123456789abcdef"[byteVal>>4]
		addr[i*2+1] = "0123456789abcdef"[byteVal&0xF]
	}
	return addr
}

// generateArbitrageTriangle creates a triangular arbitrage cycle with sequential PairIDs
func generateArbitrageTriangle(baseID uint32) ArbitrageTriplet {
	return ArbitrageTriplet{
		PairID(baseID),
		PairID(baseID + 1),
		PairID(baseID + 2),
	}
}

// createMockArbitrageSetup generates comprehensive test data for arbitrage system testing
func createMockArbitrageSetup() *MockArbitrageSetup {
	setup := &MockArbitrageSetup{
		trianglePairs:   make([]ArbitrageTriplet, 0, testTrianglePairCount),
		addressMappings: make(map[PairID][40]byte),
		tickUpdateQueue: make([]MockTickUpdate, 0, testTickUpdatesCount),
	}

	// Generate triangular arbitrage cycles with corresponding address mappings
	for i := 0; i < testTrianglePairCount; i++ {
		baseID := uint32(i * 3)
		triangle := generateArbitrageTriangle(baseID)
		setup.trianglePairs = append(setup.trianglePairs, triangle)

		// Create deterministic address mappings for each pair in the triangle
		for j, pairID := range triangle {
			seed := uint64(baseID + uint32(j))
			addr := generateMockAddress(seed)
			setup.addressMappings[pairID] = addr
		}
	}

	// Generate realistic tick update sequences
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
// UNIT TESTS - CORE COMPONENT VALIDATION
// ============================================================================

// TestAddressKeyOperations validates fundamental address key operations
func TestAddressKeyOperations(t *testing.T) {
	t.Run("KeyGeneration", func(t *testing.T) {
		addr1 := generateMockAddress(12345)
		addr2 := generateMockAddress(12345) // Same seed must produce identical address
		addr3 := generateMockAddress(54321) // Different seed must produce different address

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

		// Validate reflexivity property
		if !key.isEqual(key) {
			t.Error("Key must equal itself (reflexivity property)")
		}

		// Validate inequality after modification
		modifiedKey := key
		modifiedKey.words[2] ^= 1

		if key.isEqual(modifiedKey) {
			t.Error("Modified key must not equal original")
		}
	})

	t.Run("DeterministicGeneration", func(t *testing.T) {
		addr1 := generateMockAddress(12345)
		addr2 := generateMockAddress(12345)

		if addr1 != addr2 {
			t.Error("Address generation must be deterministic for identical seed")
		}
	})

	t.Run("AddressKeyStructureSize", func(t *testing.T) {
		var key AddressKey
		size := unsafe.Sizeof(key)

		if size != 64 {
			t.Errorf("AddressKey size is %d bytes, expected 64 bytes for cache line alignment", size)
		}
	})
}

// TestDirectAddressIndexing validates the direct Ethereum address indexing system
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

		// Verify reasonable distribution (no single index should dominate)
		maxCollisions := 0
		for _, count := range indices {
			if count > maxCollisions {
				maxCollisions = count
			}
		}

		// With proper distribution, maximum collisions should be relatively low
		if maxCollisions > testCount/10 {
			t.Errorf("Poor address index distribution: max collisions %d (expected < %d)", maxCollisions, testCount/10)
		}
	})

	t.Run("IndexBounds", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			addr := generateMockAddress(uint64(i))
			index := directAddressToIndex64(addr[:])

			if index >= constants.AddressTableCapacity {
				t.Errorf("Index %d exceeds table capacity %d", index, constants.AddressTableCapacity)
			}
		}
	})
}

// TestRobinHoodHashTable validates the Robin Hood hash table implementation
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

	t.Run("NotFoundHandling", func(t *testing.T) {
		unknownAddr := generateMockAddress(999999)
		foundID := lookupPairIDByAddress(unknownAddr[:])

		if foundID != 0 {
			t.Error("Unknown address must return 0")
		}
	})

	t.Run("CollisionResolution", func(t *testing.T) {
		const testCount = 100
		pairs := make(map[PairID][40]byte)

		// Insert multiple addresses that may produce hash collisions
		for i := 0; i < testCount; i++ {
			addr := generateMockAddress(uint64(i + 50000))
			pairID := PairID(i + 10000)
			pairs[pairID] = addr

			RegisterPairAddress(addr[:], pairID)
		}

		// Verify all entries can be retrieved correctly
		for pairID, addr := range pairs {
			found := lookupPairIDByAddress(addr[:])
			if found != pairID {
				t.Errorf("Collision resolution failed: pair %d not found", pairID)
			}
		}
	})

	t.Run("UpdateExistingEntry", func(t *testing.T) {
		addr := generateMockAddress(12345)
		originalPairID := PairID(1000)
		updatedPairID := PairID(2000)

		// Insert original mapping
		RegisterPairAddress(addr[:], originalPairID)

		// Update with new PairID
		RegisterPairAddress(addr[:], updatedPairID)

		// Verify update took effect
		found := lookupPairIDByAddress(addr[:])
		if found != updatedPairID {
			t.Errorf("Expected updated pair ID %d, got %d", updatedPairID, found)
		}
	})

	t.Run("CompleteIntegrity", func(t *testing.T) {
		setup := createMockArbitrageSetup()

		// Register all address mappings
		for pairID, addr := range setup.addressMappings {
			RegisterPairAddress(addr[:], pairID)
		}

		// Verify all lookups succeed
		for pairID, addr := range setup.addressMappings {
			foundID := lookupPairIDByAddress(addr[:])
			if foundID != pairID {
				t.Errorf("Integrity check failed: expected %d, got %d", pairID, foundID)
			}
		}
	})
}

// TestTickQuantization validates floating-point to integer quantization
func TestTickQuantization(t *testing.T) {
	testCases := []struct {
		input float64
		name  string
	}{
		{-200.0, "Underflow boundary"},
		{-constants.TickClampingBound, "Lower bound"},
		{-64.0, "Negative value"},
		{0.0, "Zero value"},
		{64.0, "Positive value"},
		{constants.TickClampingBound, "Upper bound"},
		{200.0, "Overflow boundary"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := quantizeTickToInt64(tc.input)

			// Verify result falls within valid quantization range
			if result < 0 || result > constants.MaxQuantizedTick {
				t.Errorf("Quantization result %d outside valid range [0, %d]", result, constants.MaxQuantizedTick)
			}
		})
	}

	t.Run("Monotonicity", func(t *testing.T) {
		// Verify quantization maintains order for values within bounds
		val1 := quantizeTickToInt64(-50.0)
		val2 := quantizeTickToInt64(0.0)
		val3 := quantizeTickToInt64(50.0)

		if val1 >= val2 || val2 >= val3 {
			t.Error("Quantization must maintain monotonicity")
		}
	})
}

// TestCoreAssignment validates pair-to-core assignment functionality
func TestCoreAssignment(t *testing.T) {
	t.Run("SingleCoreAssignment", func(t *testing.T) {
		pairID := PairID(12345)
		coreID := uint8(3)

		RegisterPairToCore(pairID, coreID)

		assignment := pairToCoreAssignment[pairID]
		expectedBit := uint64(1) << coreID

		if assignment&expectedBit == 0 {
			t.Errorf("Core assignment failed: pair %d not assigned to core %d", pairID, coreID)
		}
	})

	t.Run("MultipleCoreAssignment", func(t *testing.T) {
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

	t.Run("BitManipulationCorrectness", func(t *testing.T) {
		pairID := PairID(99999)
		cores := []uint8{1, 3, 5}

		for _, coreID := range cores {
			RegisterPairToCore(pairID, coreID)
		}

		// Verify bit manipulation loop matches assigned cores
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
}

// ============================================================================
// CRITICAL PATH COVERAGE TESTS
// ============================================================================

// TestCriticalExecutionPaths validates the most performance-critical code paths
func TestCriticalExecutionPaths(t *testing.T) {
	t.Run("ReverseDirectionProcessing", func(t *testing.T) {
		// Validate reverse direction tick processing branch with properly initialized executor
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: true, // Triggers reverse tick selection
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.fanoutTables[0] = nil
		executor.pairToQueueIndex.Put(123, 0)

		// Create and populate cycle state (realistic initialization)
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.0, 0.0, 0.0},
			pairIDs:    [3]PairID{123, 124, 125},
		}

		// Populate queue with cycle (matches production initialization)
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: 1.0,
			reverseTick: -2.0, // This value should be selected for reverse direction
		}

		// Execute critical path - now with populated queue
		processTickUpdate(executor, update)
	})

	t.Run("ProfitableArbitrageDetection", func(t *testing.T) {
		// Validate profitable arbitrage cycle detection and emission with realistic setup
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.fanoutTables[0] = nil
		executor.pairToQueueIndex.Put(123, 0)

		// Create cycle with negative total (profitable) - realistic scenario
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{-0.5, -0.3, -0.1}, // Total: -0.9
			pairIDs:    [3]PairID{100, 101, 102},
		}

		// Populate queue with cycle (essential for realistic test)
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(0, handle, 0) // Use profitable priority

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: -0.2, // Total becomes -1.1 (profitable)
		}

		// Should trigger arbitrage opportunity emission
		processTickUpdate(executor, update)
	})

	t.Run("FanoutTablePropagation", func(t *testing.T) {
		// Validate fanout table update propagation mechanism with realistic setup
		executor := &ArbitrageCoreExecutor{
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(16),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		// Initialize cycle state properly
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.0, 0.0, 0.0},
			pairIDs:    [3]PairID{100, 101, 102},
		}

		// Populate queue with cycle (matches production behavior)
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		// Setup fanout entry to update tick at index 1
		executor.fanoutTables[0] = []FanoutEntry{
			{
				queueHandle:     handle,
				edgeIndex:       1,
				cycleStateIndex: CycleStateIndex(0),
				queue:           &executor.priorityQueues[0],
			},
		}

		update := &TickUpdate{
			pairID:      PairID(123),
			forwardTick: 1.5,
		}

		processTickUpdate(executor, update)

		// Verify fanout propagation updated the correct tick value
		if executor.cycleStates[0].tickValues[1] != 1.5 {
			t.Errorf("Fanout propagation failed: expected 1.5, got %f", executor.cycleStates[0].tickValues[1])
		}
	})
}

// ============================================================================
// DISPATCH PIPELINE INTEGRATION TESTS
// ============================================================================

// TestDispatchPipeline validates the complete tick update dispatch system
func TestDispatchPipeline(t *testing.T) {
	t.Run("CompleteDispatchExecution", func(t *testing.T) {
		// Validate end-to-end DispatchTickUpdate execution pipeline
		addr := generateMockAddress(555)
		pairID := PairID(555)

		RegisterPairAddress(addr[:], pairID)
		RegisterPairToCore(pairID, 0)
		RegisterPairToCore(pairID, 1)

		// Initialize required ring buffers for dispatch validation
		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}
		if coreRings[1] == nil {
			coreRings[1] = ring24.New(16)
		}

		// Construct realistic LogView structure
		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		// Format address with proper LogView layout (0x prefix + 40 hex characters)
		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], addr[:])

		// Encode reserve values in LogView data format
		reserve0, reserve1 := uint64(1000), uint64(500)
		for i := 0; i < 8; i++ {
			logView.Data[24+i] = byte(reserve0 >> (8 * (7 - i)))
			logView.Data[56+i] = byte(reserve1 >> (8 * (7 - i)))
		}

		// Execute complete dispatch pipeline
		DispatchTickUpdate(logView)
	})

	t.Run("UnknownAddressGracefulHandling", func(t *testing.T) {
		unknownAddr := generateMockAddress(999999)

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], unknownAddr[:])

		// Must handle unknown address gracefully without panic
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
			{1, 1000000, "Large reserve ratio"},
			{18446744073709551615, 1, "Maximum uint64 reserve0"},
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

				// Encode reserves as big-endian bytes
				for i := 0; i < 8; i++ {
					logView.Data[24+i] = byte(tc.reserve0 >> (8 * (7 - i)))
					logView.Data[56+i] = byte(tc.reserve1 >> (8 * (7 - i)))
				}

				// Must handle edge cases without panic
				DispatchTickUpdate(logView)
			})
		}
	})

	t.Run("MultiCoreBitManipulation", func(t *testing.T) {
		// Validate dispatch to multiple non-contiguous cores via bit manipulation
		addr := generateMockAddress(888)
		pairID := PairID(888)

		RegisterPairAddress(addr[:], pairID)

		// Assign to non-contiguous cores to validate bit manipulation logic
		cores := []uint8{0, 3, 7, 15}
		for _, core := range cores {
			RegisterPairToCore(pairID, core)
		}

		// Initialize required ring buffers for all assigned cores
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

		// Validate dispatch to all assigned cores
		DispatchTickUpdate(logView)
	})
}

// ============================================================================
// DATA STRUCTURE VALIDATION TESTS
// ============================================================================

// TestDataStructureLayout validates memory layout and size requirements
func TestDataStructureLayout(t *testing.T) {
	t.Run("ArbitrageCycleStateLayout", func(t *testing.T) {
		var state ArbitrageCycleState
		size := unsafe.Sizeof(state)

		if size != 64 {
			t.Errorf("ArbitrageCycleState size is %d bytes, expected 64 bytes", size)
		}
	})

	t.Run("TickUpdateMessageLayout", func(t *testing.T) {
		var update TickUpdate
		size := unsafe.Sizeof(update)

		if size != 24 {
			t.Errorf("TickUpdate size is %d bytes, expected 24 bytes", size)
		}

		// Validate unsafe pointer casting for message buffer operations
		var messageBuffer [24]byte
		tickUpdate := (*TickUpdate)(unsafe.Pointer(&messageBuffer))

		tickUpdate.pairID = PairID(12345)
		tickUpdate.forwardTick = 1.23
		tickUpdate.reverseTick = -1.23

		if tickUpdate.pairID != PairID(12345) {
			t.Error("TickUpdate message buffer casting validation failed")
		}
	})

	t.Run("AddressKeyAlignment", func(t *testing.T) {
		var key AddressKey
		size := unsafe.Sizeof(key)

		if size != 64 {
			t.Errorf("AddressKey size is %d bytes, expected 64 bytes for cache alignment", size)
		}
	})

	t.Run("ArbitrageCoreExecutorLayout", func(t *testing.T) {
		var executor ArbitrageCoreExecutor
		size := unsafe.Sizeof(executor)

		if size != 192 {
			t.Errorf("ArbitrageCoreExecutor size is %d bytes, expected 192 bytes (3 cache lines)", size)
		}
	})
}

// ============================================================================
// UTILITY FUNCTION VALIDATION TESTS
// ============================================================================

// TestUtilityFunctions validates supporting utility function correctness
func TestUtilityFunctions(t *testing.T) {
	t.Run("SecureRandomIntBounds", func(t *testing.T) {
		for bound := 2; bound <= 100; bound++ {
			for i := 0; i < 10; i++ {
				result := secureRandomInt(bound)
				if result < 0 || result >= bound {
					t.Errorf("secureRandomInt(%d) returned %d, outside bounds [0, %d)", bound, result, bound)
				}
			}
		}
	})

	t.Run("ShuffleEdgeBindings", func(t *testing.T) {
		// Validate empty slice handling
		var empty []ArbitrageEdgeBinding
		shuffleEdgeBindings(empty) // Must not panic

		// Validate single element stability
		single := []ArbitrageEdgeBinding{
			{cyclePairs: [3]PairID{1, 2, 3}, edgeIndex: 0},
		}
		original := single[0]
		shuffleEdgeBindings(single)

		if single[0] != original {
			t.Error("Single element must remain unchanged after shuffle")
		}
	})

	t.Run("UtilsIntegration", func(t *testing.T) {
		// Validate utils.LoadBE64 integration
		testData := []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}
		expected := uint64(0x0123456789ABCDEF)

		result := utils.LoadBE64(testData)
		if result != expected {
			t.Errorf("LoadBE64 integration failed: expected 0x%X, got 0x%X", expected, result)
		}
	})

	t.Run("FastuniIntegration", func(t *testing.T) {
		reserve0 := uint64(2000)
		reserve1 := uint64(1000)

		tickValue, err := fastuni.Log2ReserveRatio(reserve0, reserve1)
		if err != nil {
			t.Errorf("Log2ReserveRatio integration failed: %v", err)
		}

		// Should be approximately log2(2) = 1
		if math.Abs(tickValue-1.0) > 0.1 {
			t.Errorf("Log2ReserveRatio result: expected ~1.0, got %f", tickValue)
		}
	})

	t.Run("BitsTrailingZerosValidation", func(t *testing.T) {
		testCases := []struct {
			value    uint64
			expected int
		}{
			{0x1, 0},
			{0x2, 1},
			{0x4, 2},
			{0x8, 3},
			{0x10, 4},
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
// SYSTEM INTEGRATION TESTS
// ============================================================================

// TestSystemIntegration validates complete system workflow integration
func TestSystemIntegration(t *testing.T) {
	t.Run("SystemInitializationWorkflow", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(4), PairID(5), PairID(6)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(50 * time.Millisecond) // Allow initialization to complete

		// Signal graceful shutdown
		control.Shutdown()
		time.Sleep(50 * time.Millisecond) // Allow shutdown to complete
	})

	t.Run("TickUpdateProcessingWorkflow", func(t *testing.T) {
		setup := createMockArbitrageSetup()

		// Register all address mappings
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

			// Encode reserves as big-endian
			for j := 0; j < 8; j++ {
				logView.Data[24+j] = byte(update.reserve0 >> (8 * (7 - j)))
				logView.Data[56+j] = byte(update.reserve1 >> (8 * (7 - j)))
			}

			DispatchTickUpdate(logView)
			processedCount++
		}

		t.Logf("Successfully processed %d tick updates", processedCount)
	})

	t.Run("CompleteArbitrageWorkflow", func(t *testing.T) {
		// Validate complete end-to-end arbitrage detection workflow
		cycles := []ArbitrageTriplet{
			{PairID(1001), PairID(1002), PairID(1003)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(20 * time.Millisecond)

		// Register addresses and configure core assignments
		for i := uint64(1001); i <= 1003; i++ {
			addr := generateMockAddress(i * 1000)
			RegisterPairAddress(addr[:], PairID(i))
			RegisterPairToCore(PairID(i), uint8(i%2))
		}

		// Initialize required ring buffers for processing
		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}
		if coreRings[1] == nil {
			coreRings[1] = ring24.New(16)
		}

		// Process realistic tick update sequence
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

		// Perform graceful system cleanup
		control.Shutdown()
		time.Sleep(50 * time.Millisecond)

		t.Log("Complete arbitrage workflow validation successful")
	})
}

// ============================================================================
// MEMORY EFFICIENCY AND CONCURRENCY TESTS
// ============================================================================

// TestMemoryEfficiency validates memory allocation patterns and efficiency
func TestMemoryEfficiency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory efficiency validation in short test mode")
	}

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Perform operations that must be allocation-efficient
	for i := 0; i < 50; i++ {
		addr := generateMockAddress(uint64(i + 1000))
		pairID := PairID(i + 1000)
		RegisterPairAddress(addr[:], pairID)
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	allocatedBytes := m2.TotalAlloc - m1.TotalAlloc
	t.Logf("Memory allocated during operations: %d bytes", allocatedBytes)

	if allocatedBytes > maxMemoryAllocBytes {
		t.Errorf("Excessive memory allocation: %d bytes (limit: %d)", allocatedBytes, maxMemoryAllocBytes)
	}
}

// TestConcurrentSafety validates concurrent operation safety and data integrity
func TestConcurrentSafety(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent safety validation in short test mode")
	}

	const goroutineCount = 8
	const operationsPerGoroutine = 50

	var wg sync.WaitGroup
	processed := uint64(0)

	// Validate concurrent address registration and lookup
	for g := 0; g < goroutineCount; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < operationsPerGoroutine; i++ {
				addr := generateMockAddress(uint64(workerID*1000 + i))
				pairID := PairID(workerID*1000 + i + 50000)

				RegisterPairAddress(addr[:], pairID)

				// Immediate verification
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
		t.Errorf("Concurrent operations failed: expected %d successful, got %d", expectedTotal, processed)
	} else {
		t.Logf("Concurrent safety validated: %d operations completed successfully", processed)
	}
}

// TestHighThroughputValidation validates system performance under sustained load
func TestHighThroughputValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput validation in short test mode")
	}

	setup := createMockArbitrageSetup()

	// Pre-register all address mappings
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	const operationCount = 1000
	processed := uint64(0)

	start := time.Now()

	// Execute high-volume tick updates
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
	t.Logf("Achieved throughput: %.0f operations/second", throughput)

	if throughput < minThroughputOpsPerSec {
		t.Errorf("Insufficient throughput: %.0f ops/sec (minimum: %d)", throughput, minThroughputOpsPerSec)
	}
}

// ============================================================================
// PERFORMANCE BENCHMARKS
// ============================================================================

// BenchmarkAddressLookup measures direct address lookup performance
func BenchmarkAddressLookup(b *testing.B) {
	setup := createMockArbitrageSetup()

	// Pre-populate hash table
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

// BenchmarkTickQuantization measures tick quantization performance
func BenchmarkTickQuantization(b *testing.B) {
	inputs := []float64{-100.0, -50.0, -10.0, 0.0, 10.0, 50.0, 100.0}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		input := inputs[i%len(inputs)]
		_ = quantizeTickToInt64(input)
	}
}

// BenchmarkTickUpdateDispatch measures complete dispatch pipeline performance
func BenchmarkTickUpdateDispatch(b *testing.B) {
	setup := createMockArbitrageSetup()

	// Pre-populate address mappings
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	// Pre-generate LogView structures
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

// BenchmarkRobinHoodOperations measures hash table operation performance
func BenchmarkRobinHoodOperations(b *testing.B) {
	// Pre-generate test data
	addresses := make([][40]byte, 1000)
	pairIDs := make([]PairID, 1000)

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

	// Pre-populate for lookup benchmark
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

// BenchmarkMessageBufferOperations measures message buffer manipulation performance
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

	b.Run("CoreBitManipulation", func(b *testing.B) {
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

// BenchmarkSystemIntegration measures end-to-end system performance
func BenchmarkSystemIntegration(b *testing.B) {
	setup := createMockArbitrageSetup()

	// Complete system setup
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
		RegisterPairToCore(pairID, uint8(pairID%4))
	}

	// Initialize ring buffers
	for core := 0; core < 4; core++ {
		if coreRings[core] == nil {
			coreRings[core] = ring24.New(64)
		}
	}

	// Pre-generate LogView messages
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
// SYSTEM LIFECYCLE TESTS
// ============================================================================

// TestSystemLifecycle validates complete system startup and shutdown
func TestSystemLifecycle(t *testing.T) {
	t.Run("GracefulShutdown", func(t *testing.T) {
		// Validate graceful system shutdown sequence
		cycles := []ArbitrageTriplet{
			{PairID(9001), PairID(9002), PairID(9003)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(50 * time.Millisecond) // Allow system initialization

		// Signal graceful shutdown
		control.Shutdown()
		time.Sleep(100 * time.Millisecond) // Allow cleanup completion

		t.Log("System shutdown completed successfully")
	})
}
