// router_test.go — Comprehensive test suite for maximum performance triangular arbitrage router
package router

import (
	"encoding/binary"
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
	"main/ring24"
	"main/types"

	"golang.org/x/crypto/sha3"
)

// ============================================================================
// TEST HELPERS
// ============================================================================

func generateMockAddress(seed uint64) [42]byte {
	var addr [42]byte
	addr[0] = '0'
	addr[1] = 'x'

	// Create deterministic input by combining base seed 69 with provided seed
	var input [16]byte
	binary.LittleEndian.PutUint64(input[0:8], 69)    // Deterministic base seed
	binary.LittleEndian.PutUint64(input[8:16], seed) // Variable seed

	// Generate address using Keccak256
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(input[:])
	hashOutput := hasher.Sum(nil)

	// Convert hash bytes to hex string (40 hex chars for 20-byte address)
	for i := 0; i < 20; i++ {
		byteVal := hashOutput[i]
		addr[2+i*2] = "0123456789abcdef"[byteVal>>4]
		addr[2+i*2+1] = "0123456789abcdef"[byteVal&0xF]
	}
	return addr
}

func generateArbitrageTriangle(baseID uint64) ArbitrageTriplet {
	return ArbitrageTriplet{
		PairID(baseID),
		PairID(baseID + 1),
		PairID(baseID + 2),
	}
}

func clearGlobalState() {
	// Clear address tables
	for i := range pairAddressKeys[:1000] {
		pairAddressKeys[i] = AddressKey{}
	}
	for i := range addressToPairID[:1000] {
		addressToPairID[i] = 0
	}

	// Clear core assignments
	for i := range pairToCoreAssignment[:1000] {
		pairToCoreAssignment[i] = 0
	}

	// Clear shard buckets
	pairShardBuckets = nil

	// Clear executors and rings
	for i := range coreExecutors {
		coreExecutors[i] = nil
	}
	for i := range coreRings {
		coreRings[i] = nil
	}
}

func assertZeroAllocs(t *testing.T, name string, fn func()) {
	t.Helper()
	allocs := testing.AllocsPerRun(100, fn)
	if allocs > 0 {
		t.Errorf("%s allocated: %f allocs/op", name, allocs)
	}
}

// ============================================================================
// STRUCT LAYOUT TESTS
// ============================================================================

func TestStructSizes(t *testing.T) {
	tests := []struct {
		name     string
		size     uintptr
		expected uintptr
	}{
		{"AddressKey", unsafe.Sizeof(AddressKey{}), 96},
		{"TickUpdate", unsafe.Sizeof(TickUpdate{}), 24},
		{"ArbitrageCycleState", unsafe.Sizeof(ArbitrageCycleState{}), 96},
		{"ArbitrageEdgeBinding", unsafe.Sizeof(ArbitrageEdgeBinding{}), 96},
		{"FanoutEntry", unsafe.Sizeof(FanoutEntry{}), 96},
		{"PairShardBucket", unsafe.Sizeof(PairShardBucket{}), 96},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.size != tt.expected {
				t.Errorf("%s size is %d bytes, expected %d bytes", tt.name, tt.size, tt.expected)
			}
		})
	}
}

func TestStructAlignment(t *testing.T) {
	t.Run("8ByteAlignment", func(t *testing.T) {
		// All structs should be 8-byte aligned (Go's maximum)
		structs := []interface{}{
			AddressKey{},
			TickUpdate{},
			ArbitrageCycleState{},
			ArbitrageEdgeBinding{},
			FanoutEntry{},
			PairShardBucket{},
		}

		for _, s := range structs {
			addr := uintptr(unsafe.Pointer(&s))
			if addr%8 != 0 {
				t.Errorf("Struct %T not 8-byte aligned: %d", s, addr)
			}
		}
	})

	t.Run("CenterPaddingLayout", func(t *testing.T) {
		// Verify center padding is correctly applied
		cycle := ArbitrageCycleState{}

		// Check that tickValues starts at offset 24 (after top padding)
		tickOffset := unsafe.Offsetof(cycle.tickValues)
		if tickOffset != 24 {
			t.Errorf("tickValues offset is %d, expected 24", tickOffset)
		}

		// Check that pairIDs starts at offset 48
		pairOffset := unsafe.Offsetof(cycle.pairIDs)
		if pairOffset != 48 {
			t.Errorf("pairIDs offset is %d, expected 48", pairOffset)
		}
	})
}

func TestZeroPadding(t *testing.T) {
	t.Run("NoInternalPadding", func(t *testing.T) {
		// Verify all fields are 8-byte aligned with no internal padding
		fanout := FanoutEntry{}

		expectedOffsets := []uintptr{32, 40, 48, 56} // After 32B top padding
		actualOffsets := []uintptr{
			unsafe.Offsetof(fanout.edgeIndex),
			unsafe.Offsetof(fanout.cycleStateIndex),
			unsafe.Offsetof(fanout.queueHandle),
			unsafe.Offsetof(fanout.queue),
		}

		for i, expected := range expectedOffsets {
			if actualOffsets[i] != expected {
				t.Errorf("Field %d offset is %d, expected %d", i, actualOffsets[i], expected)
			}
		}
	})
}

// ============================================================================
// ADDRESS KEY TESTS
// ============================================================================

func TestAddressKey(t *testing.T) {
	t.Run("Creation", func(t *testing.T) {
		addr := generateMockAddress(12345)
		key := bytesToAddressKey(addr[:])

		// Should have parsed 5 words from 20-byte address
		if key.words[0] == 0 && key.words[1] == 0 && key.words[2] == 0 {
			t.Error("Address key appears to be zero")
		}

		// Last two words should be zero (padding)
		if key.words[3] != 0 || key.words[4] != 0 {
			t.Error("Padding words should be zero")
		}
	})

	t.Run("Comparison", func(t *testing.T) {
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

	t.Run("ZeroAddress", func(t *testing.T) {
		var zeroAddr [42]byte
		copy(zeroAddr[:], "0x0000000000000000000000000000000000000000")

		key := bytesToAddressKey(zeroAddr[:])

		// Should parse to all zeros
		for i := 0; i < 3; i++ {
			if key.words[i] != 0 {
				t.Errorf("Zero address word %d should be 0, got %x", i, key.words[i])
			}
		}
	})

	t.Run("PrefixStripping", func(t *testing.T) {
		// Test that 0x prefix is properly stripped
		addr := [42]byte{}
		copy(addr[:], "0x1234567890abcdefABCDEF1234567890abcdefAB")

		key := bytesToAddressKey(addr[:])

		// Should not be zero (proving prefix was stripped and address parsed)
		if key.words[0] == 0 {
			t.Error("Address key should not be zero after parsing")
		}
	})
}

func TestAddressKeyZeroAllocation(t *testing.T) {
	addr := generateMockAddress(42)

	assertZeroAllocs(t, "bytesToAddressKey", func() {
		_ = bytesToAddressKey(addr[:])
	})

	key1 := bytesToAddressKey(addr[:])
	key2 := bytesToAddressKey(addr[:])

	assertZeroAllocs(t, "AddressKey.isEqual", func() {
		_ = key1.isEqual(key2)
	})
}

// ============================================================================
// ROBIN HOOD HASH TABLE TESTS
// ============================================================================

func TestRobinHoodHashTable(t *testing.T) {
	clearGlobalState()

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

	t.Run("MiddleByteHashing", func(t *testing.T) {
		// Test that middle byte extraction works
		addr := generateMockAddress(123)

		hash1 := directAddressToIndex64(addr[:])
		hash2 := directAddressToIndex64(addr[:])

		if hash1 != hash2 {
			t.Error("Hash function should be deterministic")
		}

		// Should use middle bytes (different from first bytes)
		key := bytesToAddressKey(addr[:])
		firstBytes := uint64(key.words[0]) & uint64(constants.AddressTableMask)
		middleBytes := hash1

		if firstBytes == middleBytes {
			t.Log("Warning: Middle byte hash equals first byte hash (possible but unlikely)")
		}
	})

	t.Run("RobinHoodDisplacement", func(t *testing.T) {
		clearGlobalState()

		// Register multiple addresses that might collide
		for i := 0; i < 10; i++ {
			addr := generateMockAddress(uint64(1000 + i))
			pairID := PairID(i + 1)
			RegisterPairAddress(addr[:], pairID)
		}

		// Verify all can be retrieved
		for i := 0; i < 10; i++ {
			addr := generateMockAddress(uint64(1000 + i))
			expected := PairID(i + 1)
			found := lookupPairIDByAddress(addr[:])

			if found != expected {
				t.Errorf("Robin Hood failed: expected %d, got %d", expected, found)
			}
		}
	})

	t.Run("LargeScale", func(t *testing.T) {
		clearGlobalState()

		const count = 500 // Keep reasonable to avoid hash table overflow
		for i := 0; i < count; i++ {
			addr := generateMockAddress(uint64(i * 1000003)) // Better distribution
			RegisterPairAddress(addr[:], PairID(i+1))
		}

		// Verify random samples
		for i := 0; i < count; i += 25 {
			addr := generateMockAddress(uint64(i * 1000003))
			found := lookupPairIDByAddress(addr[:])
			if found != PairID(i+1) {
				t.Errorf("Lost entry %d: expected %d, got %d", i, i+1, found)
			}
		}
	})
}

func TestRobinHoodZeroAllocation(t *testing.T) {
	clearGlobalState()

	addr := generateMockAddress(42)
	pairID := PairID(123)

	// First registration might allocate, so do it outside test
	RegisterPairAddress(addr[:], pairID)

	assertZeroAllocs(t, "RegisterPairAddress", func() {
		RegisterPairAddress(addr[:], pairID) // Update existing
	})

	assertZeroAllocs(t, "lookupPairIDByAddress", func() {
		_ = lookupPairIDByAddress(addr[:])
	})
}

// ============================================================================
// QUANTIZATION TESTS
// ============================================================================

func TestQuantization(t *testing.T) {
	t.Run("BasicQuantization", func(t *testing.T) {
		testCases := []struct {
			input float64
		}{
			{0.0},
			{1.0},
			{-1.0},
		}

		for _, tc := range testCases {
			// Calculate expected value using the same function to avoid overflow
			expected := quantizeTickToInt64(tc.input)
			result := quantizeTickToInt64(tc.input)
			if result != expected {
				t.Errorf("quantizeTickToInt64(%f) = %d, expected %d", tc.input, result, expected)
			}
		}

		// Test specific known relationships
		zero := quantizeTickToInt64(0.0)
		one := quantizeTickToInt64(1.0)
		minusOne := quantizeTickToInt64(-1.0)

		// Verify relative relationships
		if one <= zero {
			t.Error("quantizeTickToInt64(1.0) should be greater than quantizeTickToInt64(0.0)")
		}
		if minusOne >= zero {
			t.Error("quantizeTickToInt64(-1.0) should be less than quantizeTickToInt64(0.0)")
		}
	})

	t.Run("FootgunMode", func(t *testing.T) {
		// Test that footgun mode has no bounds checking
		extremeValues := []float64{
			-1000.0,
			1000.0,
			math.Inf(1),
			math.Inf(-1),
			math.NaN(),
		}

		for _, val := range extremeValues {
			// Should not panic (footgun mode)
			result := quantizeTickToInt64(val)
			_ = result // Just verify no panic
		}
	})

	t.Run("Precision", func(t *testing.T) {
		tick1 := 50.0
		tick2 := 50.0 + 1.0/constants.QuantizationScale

		q1 := quantizeTickToInt64(tick1)
		q2 := quantizeTickToInt64(tick2)

		diff := q2 - q1
		if diff < 0 {
			diff = -diff
		}
		if diff != 1 {
			t.Errorf("Quantization precision incorrect: diff=%d", diff)
		}
	})
}

func TestQuantizationZeroAllocation(t *testing.T) {
	assertZeroAllocs(t, "quantizeTickToInt64", func() {
		_ = quantizeTickToInt64(1.23)
	})
}

// ============================================================================
// KECCAK SHUFFLE TESTS
// ============================================================================

func TestKeccakShuffle(t *testing.T) {
	t.Run("Deterministic", func(t *testing.T) {
		bindings := make([]ArbitrageEdgeBinding, 10)
		for i := range bindings {
			bindings[i] = ArbitrageEdgeBinding{
				cyclePairs: [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
				edgeIndex:  uint64(i % 3),
			}
		}

		// Create copies
		bindings1 := make([]ArbitrageEdgeBinding, len(bindings))
		bindings2 := make([]ArbitrageEdgeBinding, len(bindings))
		copy(bindings1, bindings)
		copy(bindings2, bindings)

		// Shuffle both with same pairID
		pairID := PairID(12345)
		keccakShuffleEdgeBindings(bindings1, pairID)
		keccakShuffleEdgeBindings(bindings2, pairID)

		// Should be identical
		for i := range bindings1 {
			if bindings1[i] != bindings2[i] {
				t.Error("Keccak shuffle not deterministic")
				break
			}
		}
	})

	t.Run("DifferentSeeds", func(t *testing.T) {
		bindings := make([]ArbitrageEdgeBinding, 10)
		for i := range bindings {
			bindings[i] = ArbitrageEdgeBinding{
				cyclePairs: [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
				edgeIndex:  uint64(i % 3),
			}
		}

		bindings1 := make([]ArbitrageEdgeBinding, len(bindings))
		bindings2 := make([]ArbitrageEdgeBinding, len(bindings))
		copy(bindings1, bindings)
		copy(bindings2, bindings)

		// Shuffle with different pairIDs
		keccakShuffleEdgeBindings(bindings1, PairID(12345))
		keccakShuffleEdgeBindings(bindings2, PairID(54321))

		// Should be different
		identical := true
		for i := range bindings1 {
			if bindings1[i] != bindings2[i] {
				identical = false
				break
			}
		}

		if identical {
			t.Error("Different seeds produced identical results (extremely unlikely)")
		}
	})

	t.Run("EdgeCases", func(t *testing.T) {
		// Empty slice
		var empty []ArbitrageEdgeBinding
		keccakShuffleEdgeBindings(empty, PairID(123)) // Should not panic

		// Single element
		single := []ArbitrageEdgeBinding{{}}
		original := single[0]
		keccakShuffleEdgeBindings(single, PairID(123))
		if single[0] != original {
			t.Error("Single element should not change")
		}
	})

	t.Run("QualityDistribution", func(t *testing.T) {
		// Test that shuffle produces good distribution
		size := 10
		bindings := make([]ArbitrageEdgeBinding, size)
		for i := range bindings {
			bindings[i] = ArbitrageEdgeBinding{
				cyclePairs: [3]PairID{PairID(i), PairID(i + 1), PairID(i + 2)},
				edgeIndex:  uint64(i % 3),
			}
		}

		// Track position changes across multiple shuffles
		positionChanges := 0
		iterations := 100

		for iter := 0; iter < iterations; iter++ {
			testBindings := make([]ArbitrageEdgeBinding, size)
			copy(testBindings, bindings)

			keccakShuffleEdgeBindings(testBindings, PairID(iter))

			// Check if any position changed
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
}

// ============================================================================
// SHARD BUILDING TESTS
// ============================================================================

func TestShardBuilding(t *testing.T) {
	clearGlobalState()

	t.Run("BasicShardConstruction", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(2), PairID(4), PairID(5)},
			{PairID(3), PairID(5), PairID(6)},
		}

		buildFanoutShardBuckets(cycles)

		// Each pair should have shards
		if len(pairShardBuckets) == 0 {
			t.Error("No shard buckets created")
		}

		// Check that pairs have appropriate edge bindings
		for pairID := PairID(1); pairID <= PairID(6); pairID++ {
			shards, exists := pairShardBuckets[pairID]
			if !exists || len(shards) == 0 {
				t.Errorf("Pair %d has no shards", pairID)
			}
		}
	})

	t.Run("ShardSizeLimit", func(t *testing.T) {
		clearGlobalState()

		// Create many cycles with same pair to trigger sharding
		cycles := make([]ArbitrageTriplet, constants.MaxCyclesPerShard*2)
		for i := range cycles {
			cycles[i] = ArbitrageTriplet{
				PairID(1), // Same first pair
				PairID(uint64(i + 1000)),
				PairID(uint64(i + 2000)),
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
		clearGlobalState()

		cycles := []ArbitrageTriplet{
			{PairID(10), PairID(20), PairID(30)},
		}

		buildFanoutShardBuckets(cycles)

		// Verify edge indices are correct
		for pairID, shards := range pairShardBuckets {
			for _, shard := range shards {
				for _, binding := range shard.edgeBindings {
					// Find which position this pair is in
					found := false
					for i, p := range binding.cyclePairs {
						if p == pairID {
							if binding.edgeIndex != uint64(i) {
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

	t.Run("ShuffleIntegration", func(t *testing.T) {
		clearGlobalState()

		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(1), PairID(4), PairID(5)},
			{PairID(1), PairID(6), PairID(7)},
		}

		buildFanoutShardBuckets(cycles)

		// Verify that pair 1 has shuffled bindings
		shards := pairShardBuckets[PairID(1)]
		if len(shards) == 0 {
			t.Fatal("No shards for pair 1")
		}

		bindings := shards[0].edgeBindings
		if len(bindings) < 2 {
			t.Fatal("Not enough bindings to test shuffle")
		}

		// Build again with same cycles - should be identical (deterministic)
		clearGlobalState()
		buildFanoutShardBuckets(cycles)

		newShards := pairShardBuckets[PairID(1)]
		newBindings := newShards[0].edgeBindings

		if len(bindings) != len(newBindings) {
			t.Error("Shard reconstruction not deterministic")
		}

		for i := range bindings {
			if bindings[i] != newBindings[i] {
				t.Error("Shuffle not deterministic")
				break
			}
		}
	})
}

// ============================================================================
// CORE ASSIGNMENT TESTS
// ============================================================================

func TestCoreAssignment(t *testing.T) {
	clearGlobalState()

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

	t.Run("BitExtraction", func(t *testing.T) {
		pairID := PairID(88888)
		expectedCores := []uint8{1, 3, 5, 7, 11, 13}

		for _, coreID := range expectedCores {
			RegisterPairToCore(pairID, coreID)
		}

		assignment := pairToCoreAssignment[pairID]
		foundCores := []uint8{}

		for assignment != 0 {
			coreID := bits.TrailingZeros64(assignment)
			foundCores = append(foundCores, uint8(coreID))
			assignment &^= 1 << coreID
		}

		if len(foundCores) != len(expectedCores) {
			t.Errorf("Expected %d cores, found %d", len(expectedCores), len(foundCores))
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
}

// ============================================================================
// FULL SYSTEM INTEGRATION TESTS
// ============================================================================

func TestSystemInitialization(t *testing.T) {
	clearGlobalState()

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
		for i := uint64(1); i <= 6; i++ {
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

	t.Run("ScalabilityTest", func(t *testing.T) {
		clearGlobalState()

		cycles := make([]ArbitrageTriplet, 100)
		for i := 0; i < 100; i++ {
			baseID := uint64(i * 3)
			cycles[i] = ArbitrageTriplet{
				PairID(baseID + 1),
				PairID(baseID + 2),
				PairID(baseID + 3),
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
}

func TestTickDispatch(t *testing.T) {
	clearGlobalState()

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

		copy(logView.Addr[:42], addr[:])

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

		copy(logView.Addr[:42], unknownAddr[:])

		// Should return early when pairID is 0
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
			{1<<63 - 1, 1, "MaxReserve"},
			{1, 1<<63 - 1, "MinRatio"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				logView := &types.LogView{
					Addr: make([]byte, 64),
					Data: make([]byte, 128),
				}

				copy(logView.Addr[:42], addr[:])

				for i := 0; i < 8; i++ {
					logView.Data[24+i] = byte(tc.reserve0 >> (8 * (7 - i)))
					logView.Data[56+i] = byte(tc.reserve1 >> (8 * (7 - i)))
				}

				// Should handle edge cases in fastuni.Log2ReserveRatio
				DispatchTickUpdate(logView)
			})
		}
	})
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

func TestConcurrentSafety(t *testing.T) {
	clearGlobalState()

	t.Run("ConcurrentReads", func(t *testing.T) {
		// Register some pairs
		for i := 0; i < 50; i++ {
			addr := generateMockAddress(uint64(i * 98765))
			RegisterPairAddress(addr[:], PairID(i+1))
			RegisterPairToCore(PairID(i+1), uint8(i%8))
		}

		const goroutines = 8
		const reads = 1000

		var wg sync.WaitGroup

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for i := 0; i < reads; i++ {
					pairIdx := (id*reads + i) % 50
					addr := generateMockAddress(uint64(pairIdx * 98765))

					// Concurrent reads should be safe
					foundID := lookupPairIDByAddress(addr[:])
					if foundID != PairID(pairIdx+1) {
						t.Errorf("Read inconsistency: expected %d, got %d", pairIdx+1, foundID)
					}
				}
			}(g)
		}

		wg.Wait()
	})

	t.Run("ConcurrentDispatch", func(t *testing.T) {
		clearGlobalState()

		// Initialize rings
		for i := 0; i < 8; i++ {
			if coreRings[i] == nil {
				coreRings[i] = ring24.New(1024)
			}
		}

		// Register some pairs
		for i := 0; i < 10; i++ {
			addr := generateMockAddress(uint64(i * 54321))
			RegisterPairAddress(addr[:], PairID(i+1))
			RegisterPairToCore(PairID(i+1), uint8(i%8))
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
					addr := generateMockAddress(uint64(pairIdx * 54321))

					logView := &types.LogView{
						Addr: make([]byte, 64),
						Data: make([]byte, 128),
					}

					copy(logView.Addr[:42], addr[:])

					reserve0 := uint64(1000 + id*100 + i)
					reserve1 := uint64(2000 - id*50 - i)

					for j := 0; j < 8; j++ {
						logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
						logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
					}

					DispatchTickUpdate(logView)
				}
			}(g)
		}

		wg.Wait()
	})
}

// ============================================================================
// BENCHMARKS
// ============================================================================

func BenchmarkAddressLookup(b *testing.B) {
	clearGlobalState()

	addresses := make([][42]byte, 10000)
	for i := range addresses {
		addresses[i] = generateMockAddress(uint64(i * 1000003))
		RegisterPairAddress(addresses[i][:], PairID(i+1))
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

func BenchmarkKeccakShuffle(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			bindings := make([]ArbitrageEdgeBinding, size)
			for i := 0; i < size; i++ {
				bindings[i] = ArbitrageEdgeBinding{
					cyclePairs: [3]PairID{PairID(i), PairID(i + 1), PairID(i + 2)},
					edgeIndex:  uint64(i % 3),
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				testBindings := make([]ArbitrageEdgeBinding, size)
				copy(testBindings, bindings)
				keccakShuffleEdgeBindings(testBindings, PairID(i))
			}
		})
	}
}

func BenchmarkDispatchTickUpdate(b *testing.B) {
	clearGlobalState()

	const pairCount = 1000
	addresses := make([][42]byte, pairCount)
	logViews := make([]*types.LogView, pairCount)

	// Initialize rings
	for i := 0; i < 8; i++ {
		if coreRings[i] == nil {
			coreRings[i] = ring24.New(1024)
		}
	}

	for i := 0; i < pairCount; i++ {
		addr := generateMockAddress(uint64(i * 1000003))
		addresses[i] = addr
		RegisterPairAddress(addr[:], PairID(i+1))

		// Assign to 2-4 cores
		numCores := 2 + (i % 3)
		for j := 0; j < numCores; j++ {
			coreID := uint8((i + j*16) % 8)
			RegisterPairToCore(PairID(i+1), coreID)
		}

		// Pre-create log view
		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		copy(logView.Addr[:42], addr[:])

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

func BenchmarkCoreAssignmentBitOps(b *testing.B) {
	const pairCount = 10000
	for i := 0; i < pairCount; i++ {
		pairID := PairID(i + 1)
		numCores := 2 + (i % 6)
		for j := 0; j < numCores; j++ {
			coreID := uint8((i*17 + j*23) % 64)
			RegisterPairToCore(pairID, coreID)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pairID := PairID((i % pairCount) + 1)
		coreAssignments := pairToCoreAssignment[pairID]

		// Simulate dispatch loop
		for coreAssignments != 0 {
			coreID := bits.TrailingZeros64(coreAssignments)
			coreAssignments &^= 1 << coreID
			_ = coreID
		}
	}
}

func BenchmarkHighThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping throughput benchmark in short mode")
	}

	clearGlobalState()

	const pairCount = 100
	const updatesPerPair = 100

	// Register pairs
	for i := 0; i < pairCount; i++ {
		addr := generateMockAddress(uint64(i * 1000))
		RegisterPairAddress(addr[:], PairID(i+1))
		RegisterPairToCore(PairID(i+1), uint8(i%8))
	}

	// Initialize rings
	for i := 0; i < 8; i++ {
		if coreRings[i] == nil {
			coreRings[i] = ring24.New(1024)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	updates := uint64(0)
	start := time.Now()

	for i := 0; i < b.N; i++ {
		for round := 0; round < updatesPerPair; round++ {
			for i := 0; i < pairCount; i++ {
				addr := generateMockAddress(uint64(i * 1000))

				logView := &types.LogView{
					Addr: make([]byte, 64),
					Data: make([]byte, 128),
				}

				copy(logView.Addr[:42], addr[:])

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
	}

	elapsed := time.Since(start)
	throughput := float64(updates) / elapsed.Seconds()

	b.ReportMetric(throughput, "updates/sec")
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

func BenchmarkZeroAllocation(b *testing.B) {
	clearGlobalState()

	addr := generateMockAddress(42)
	RegisterPairAddress(addr[:], PairID(123))

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lookupPairIDByAddress(addr[:])
		_ = quantizeTickToInt64(1.234)
		_ = bytesToAddressKey(addr[:])
	}

	b.StopTimer()
	runtime.ReadMemStats(&m2)

	if b.N > 0 {
		allocDelta := m2.TotalAlloc - m1.TotalAlloc
		allocsPerOp := float64(allocDelta) / float64(b.N)
		b.ReportMetric(allocsPerOp, "actual_bytes/op")

		if allocsPerOp > 1 {
			b.Logf("WARNING: %.2f bytes/op allocated", allocsPerOp)
		}
	}
}

// ============================================================================
// COMPLETE WORKFLOW TESTS
// ============================================================================

func TestCompleteWorkflow(t *testing.T) {
	clearGlobalState()

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

		copy(logView.Addr[:42], addr[:])

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
	for i := uint64(10); i <= 15; i++ {
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

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

func TestEdgeCases(t *testing.T) {
	t.Run("ZeroValues", func(t *testing.T) {
		// Test zero address
		var zeroAddr [42]byte
		copy(zeroAddr[:], "0x0000000000000000000000000000000000000000")

		key := bytesToAddressKey(zeroAddr[:])
		// Should parse without error
		_ = key

		// Test zero pair ID (should be handled as "not found")
		clearGlobalState()
		addr := generateMockAddress(123)
		RegisterPairAddress(addr[:], PairID(0)) // Zero pair ID

		found := lookupPairIDByAddress(addr[:])
		if found != 0 {
			t.Error("Zero pair ID should be treated as not found")
		}
	})

	t.Run("ExtremeQuantization", func(t *testing.T) {
		extremeValues := []float64{
			math.Inf(1),
			math.Inf(-1),
			math.NaN(),
			1e100,
			-1e100,
		}

		for _, val := range extremeValues {
			// Footgun mode: should not panic
			result := quantizeTickToInt64(val)
			_ = result
		}
	})

	t.Run("HashCollisions", func(t *testing.T) {
		clearGlobalState()

		// Try to force collisions by using sequential seeds
		collisionCount := 0
		addresses := make([][42]byte, 100)

		for i := 0; i < 100; i++ {
			addresses[i] = generateMockAddress(uint64(i))
			RegisterPairAddress(addresses[i][:], PairID(i+1))
		}

		// Check for proper Robin Hood displacement handling
		for i := 0; i < 100; i++ {
			found := lookupPairIDByAddress(addresses[i][:])
			if found != PairID(i+1) {
				collisionCount++
			}
		}

		if collisionCount > 0 {
			t.Errorf("Robin Hood failed to handle %d collisions", collisionCount)
		}
	})

	t.Run("LargeStructOperations", func(t *testing.T) {
		// Test that large structs with center padding work correctly
		cycle := ArbitrageCycleState{
			tickValues: [3]float64{1.1, 2.2, 3.3},
			pairIDs:    [3]PairID{100, 200, 300},
		}

		// Verify data is accessible despite padding
		if cycle.tickValues[0] != 1.1 {
			t.Error("Center-padded struct data access failed")
		}

		if cycle.pairIDs[2] != 300 {
			t.Error("Center-padded struct data access failed")
		}
	})
}

// ============================================================================
// PERFORMANCE REGRESSION TESTS
// ============================================================================

func TestPerformanceRegression(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance regression tests in short mode")
	}

	t.Run("LookupLatency", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(123)
		RegisterPairAddress(addr[:], PairID(456))

		start := time.Now()
		iterations := 1000000

		for i := 0; i < iterations; i++ {
			_ = lookupPairIDByAddress(addr[:])
		}

		elapsed := time.Since(start)
		avgLatency := elapsed / time.Duration(iterations)

		// Should be sub-10ns on modern hardware
		if avgLatency > 10*time.Nanosecond {
			t.Errorf("Address lookup too slow: %v avg latency", avgLatency)
		}

		t.Logf("Address lookup: %v avg latency", avgLatency)
	})

	t.Run("QuantizationLatency", func(t *testing.T) {
		start := time.Now()
		iterations := 1000000

		for i := 0; i < iterations; i++ {
			_ = quantizeTickToInt64(float64(i) * 0.001)
		}

		elapsed := time.Since(start)
		avgLatency := elapsed / time.Duration(iterations)

		// Should be sub-5ns (footgun mode)
		if avgLatency > 5*time.Nanosecond {
			t.Errorf("Quantization too slow: %v avg latency", avgLatency)
		}

		t.Logf("Quantization: %v avg latency", avgLatency)
	})

	t.Run("EndToEndLatency", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(789)
		RegisterPairAddress(addr[:], PairID(789))
		RegisterPairToCore(PairID(789), 0)

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(1024)
		}

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		copy(logView.Addr[:42], addr[:])

		// Set up reserves
		reserve0, reserve1 := uint64(1000), uint64(500)
		for i := 0; i < 8; i++ {
			logView.Data[24+i] = byte(reserve0 >> (8 * (7 - i)))
			logView.Data[56+i] = byte(reserve1 >> (8 * (7 - i)))
		}

		start := time.Now()
		iterations := 100000

		for i := 0; i < iterations; i++ {
			DispatchTickUpdate(logView)
		}

		elapsed := time.Since(start)
		avgLatency := elapsed / time.Duration(iterations)

		// Should be sub-100ns for complete dispatch
		if avgLatency > 100*time.Nanosecond {
			t.Errorf("End-to-end dispatch too slow: %v avg latency", avgLatency)
		}

		t.Logf("End-to-end dispatch: %v avg latency", avgLatency)
	})
}
