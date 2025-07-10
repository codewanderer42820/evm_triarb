// router_test.go — Comprehensive test suite for maximum performance triangular arbitrage router
package router

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"main/constants"
	"main/control"
	"main/localidx"
	"main/quantumqueue64"
	"main/ring24"
	"main/types"

	"golang.org/x/crypto/sha3"
)

// ============================================================================
// TEST HELPERS AND UTILITIES
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
// 1. STRUCT LAYOUT AND MEMORY TESTS
// ============================================================================

func TestStructSizes(t *testing.T) {
	tests := []struct {
		name     string
		size     uintptr
		expected uintptr
	}{
		{"AddressKey", unsafe.Sizeof(AddressKey{}), 24},
		{"TickUpdate", unsafe.Sizeof(TickUpdate{}), 24},
		{"ArbitrageCycleState", unsafe.Sizeof(ArbitrageCycleState{}), 48},
		{"ArbitrageEdgeBinding", unsafe.Sizeof(ArbitrageEdgeBinding{}), 32},
		{"FanoutEntry", unsafe.Sizeof(FanoutEntry{}), 32},
		{"PairShardBucket", unsafe.Sizeof(PairShardBucket{}), 32},
		{"ArbitrageCoreExecutor", unsafe.Sizeof(ArbitrageCoreExecutor{}), 4272},
		{"ProcessedCycle", unsafe.Sizeof(ProcessedCycle{}), 32},
		{"keccakRandomState", unsafe.Sizeof(keccakRandomState{}), 40},
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
			ProcessedCycle{},
		}

		for _, s := range structs {
			addr := uintptr(unsafe.Pointer(&s))
			if addr%8 != 0 {
				t.Errorf("Struct %T not 8-byte aligned: %d", s, addr)
			}
		}
	})

	t.Run("CompactLayout", func(t *testing.T) {
		// Verify AddressKey uses exactly 3 words for 160-bit address
		key := AddressKey{}

		// Check that we have exactly 3 words (24 bytes total)
		if len(key.words) != 3 {
			t.Errorf("AddressKey should have exactly 3 words, got %d", len(key.words))
		}

		// Verify each word is properly accessible
		key.words[0] = 0x1234567890abcdef
		key.words[1] = 0xfedcba0987654321
		key.words[2] = 0x1122334455667788

		if key.words[0] != 0x1234567890abcdef {
			t.Error("AddressKey word 0 access failed")
		}
		if key.words[1] != 0xfedcba0987654321 {
			t.Error("AddressKey word 1 access failed")
		}
		if key.words[2] != 0x1122334455667788 {
			t.Error("AddressKey word 2 access failed")
		}
	})

	t.Run("FieldOffsets", func(t *testing.T) {
		// Verify FanoutEntry has optimal field ordering without padding
		fanout := FanoutEntry{}

		expectedOffsets := []uintptr{0, 8, 16, 24} // Sequential 8-byte fields
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
// 2. ADDRESS KEY OPERATIONS
// ============================================================================

func TestAddressKey(t *testing.T) {
	t.Run("Creation", func(t *testing.T) {
		addr := generateMockAddress(12345)
		key := bytesToAddressKey(addr[:])

		// Should have parsed 3 words from 20-byte address
		if key.words[0] == 0 && key.words[1] == 0 && key.words[2] == 0 {
			t.Error("Address key appears to be zero")
		}

		// All 3 words should contain data (no unused padding words)
		nonZeroWords := 0
		for i := 0; i < 3; i++ {
			if key.words[i] != 0 {
				nonZeroWords++
			}
		}
		if nonZeroWords == 0 {
			t.Error("All address key words are zero")
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

		// Should parse without error (may not be all zeros due to hex parsing)
		_ = key
	})

	t.Run("PrefixStripping", func(t *testing.T) {
		// Test that 0x prefix is properly stripped
		addr := [42]byte{}
		copy(addr[:], "0x1234567890abcdefABCDEF1234567890abcdefAB")

		key := bytesToAddressKey(addr[:])

		// Should not be zero (proving prefix was stripped and address parsed)
		if key.words[0] == 0 && key.words[1] == 0 && key.words[2] == 0 {
			t.Error("Address key should not be zero after parsing valid hex")
		}
	})

	t.Run("AddressKeyEquality", func(t *testing.T) {
		// Test all combinations of equality
		key1 := AddressKey{words: [3]uint64{1, 2, 3}}
		key2 := AddressKey{words: [3]uint64{1, 2, 3}}
		key3 := AddressKey{words: [3]uint64{1, 2, 4}}
		key4 := AddressKey{words: [3]uint64{1, 3, 3}}
		key5 := AddressKey{words: [3]uint64{2, 2, 3}}

		if !key1.isEqual(key2) {
			t.Error("Identical keys should be equal")
		}
		if key1.isEqual(key3) {
			t.Error("Keys differing in word 2 should not be equal")
		}
		if key1.isEqual(key4) {
			t.Error("Keys differing in word 1 should not be equal")
		}
		if key1.isEqual(key5) {
			t.Error("Keys differing in word 0 should not be equal")
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
// 3. HASH INDEXING FUNCTIONS
// ============================================================================

func TestDirectAddressIndexing(t *testing.T) {
	t.Run("Deterministic", func(t *testing.T) {
		addr := generateMockAddress(123)

		hash1 := directAddressToIndex64(addr[:])
		hash2 := directAddressToIndex64(addr[:])

		if hash1 != hash2 {
			t.Error("Hash function should be deterministic")
		}
	})

	t.Run("DifferentAddresses", func(t *testing.T) {
		addr1 := generateMockAddress(123)
		addr2 := generateMockAddress(456)

		hash1 := directAddressToIndex64(addr1[:])
		hash2 := directAddressToIndex64(addr2[:])

		if hash1 == hash2 {
			t.Error("Different addresses should produce different hashes (collision possible but unlikely)")
		}
	})

	t.Run("StoredKeyHashing", func(t *testing.T) {
		addr := generateMockAddress(789)
		key := bytesToAddressKey(addr[:])

		// Test stored key hashing
		storedHash := directAddressToIndex64Stored(key)

		// Should be within valid range
		if storedHash >= uint64(len(pairAddressKeys)) {
			t.Error("Stored hash should be within address table bounds")
		}
	})

	t.Run("HashDistribution", func(t *testing.T) {
		// Test that hashes are reasonably distributed
		const numTests = 1000
		hashes := make(map[uint64]int)

		for i := 0; i < numTests; i++ {
			addr := generateMockAddress(uint64(i * 1000003)) // Good distribution seed
			hash := directAddressToIndex64(addr[:])
			hashes[hash]++
		}

		// Should have reasonable distribution (not all colliding)
		if len(hashes) < numTests/10 {
			t.Errorf("Poor hash distribution: %d unique hashes for %d inputs", len(hashes), numTests)
		}
	})
}

// ============================================================================
// 4. ROBIN HOOD HASH TABLE TESTS
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

	t.Run("RobinHoodDisplacement", func(t *testing.T) {
		clearGlobalState()

		// Register multiple addresses that might collide
		for i := 0; i < 20; i++ {
			addr := generateMockAddress(uint64(1000 + i))
			pairID := PairID(i + 1)
			RegisterPairAddress(addr[:], pairID)
		}

		// Verify all can be retrieved
		for i := 0; i < 20; i++ {
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

	t.Run("EmptyTableLookup", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(42)
		found := lookupPairIDByAddress(addr[:])

		if found != 0 {
			t.Error("Lookup in empty table should return 0")
		}
	})

	t.Run("ZeroPairID", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(123)
		RegisterPairAddress(addr[:], PairID(0)) // Zero pair ID

		found := lookupPairIDByAddress(addr[:])
		if found != 0 {
			t.Error("Zero pair ID should be treated as not found")
		}
	})

	t.Run("HashTableFullness", func(t *testing.T) {
		clearGlobalState()

		// Fill a significant portion of the hash table
		const count = constants.AddressTableCapacity / 4
		pairs := make(map[PairID][42]byte)

		for i := 0; i < count; i++ {
			addr := generateMockAddress(uint64(i * 2654435761)) // Large prime for distribution
			pairID := PairID(i + 1)
			RegisterPairAddress(addr[:], pairID)
			pairs[pairID] = addr
		}

		// Verify all entries can still be found
		for pairID, addr := range pairs {
			found := lookupPairIDByAddress(addr[:])
			if found != pairID {
				t.Errorf("Lost entry in full table: expected %d, got %d", pairID, found)
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
// 5. QUANTIZATION TESTS
// ============================================================================

func TestQuantization(t *testing.T) {
	t.Run("BasicQuantization", func(t *testing.T) {
		testCases := []struct {
			input float64
		}{
			{0.0},
			{1.0},
			{-1.0},
			{0.5},
			{-0.5},
			{100.0},
			{-100.0},
		}

		for _, tc := range testCases {
			result := quantizeTickToInt64(tc.input)

			// Test deterministic
			result2 := quantizeTickToInt64(tc.input)
			if result != result2 {
				t.Errorf("quantizeTickToInt64(%f) not deterministic: %d vs %d", tc.input, result, result2)
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

	t.Run("MonotonicProperty", func(t *testing.T) {
		// Test that larger inputs produce larger outputs
		for i := 0; i < 100; i++ {
			val1 := float64(i) * 0.1
			val2 := float64(i+1) * 0.1

			q1 := quantizeTickToInt64(val1)
			q2 := quantizeTickToInt64(val2)

			if q2 <= q1 {
				t.Errorf("Quantization not monotonic: f(%f)=%d, f(%f)=%d", val1, q1, val2, q2)
			}
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

	t.Run("SymmetryAroundZero", func(t *testing.T) {
		// Test approximate symmetry around zero (floating-point arithmetic may not be perfectly symmetric)
		for i := 1; i <= 10; i++ {
			val := float64(i) * 0.1
			posQ := quantizeTickToInt64(val)
			negQ := quantizeTickToInt64(-val)
			zero := quantizeTickToInt64(0.0)

			posDiff := posQ - zero
			negDiff := zero - negQ

			// Allow for small asymmetry due to floating-point arithmetic
			diff := posDiff - negDiff
			if diff < 0 {
				diff = -diff
			}
			if diff > 2 { // Allow up to 2 units of asymmetry
				t.Errorf("Quantization too asymmetric around zero: +%f gives diff %d, -%f gives diff %d (asymmetry: %d)",
					val, posDiff, val, negDiff, diff)
			}
		}
	})
}

func TestQuantizationZeroAllocation(t *testing.T) {
	assertZeroAllocs(t, "quantizeTickToInt64", func() {
		_ = quantizeTickToInt64(1.23)
	})
}

// ============================================================================
// 6. KECCAK RANDOM GENERATOR TESTS
// ============================================================================

func TestKeccakRandom(t *testing.T) {
	t.Run("Deterministic", func(t *testing.T) {
		seed := []byte("test seed")

		rng1 := newKeccakRandom(seed)
		rng2 := newKeccakRandom(seed)

		// Should produce identical sequences
		for i := 0; i < 10; i++ {
			val1 := rng1.nextUint64()
			val2 := rng2.nextUint64()

			if val1 != val2 {
				t.Errorf("Keccak random not deterministic at step %d: %d vs %d", i, val1, val2)
			}
		}
	})

	t.Run("DifferentSeeds", func(t *testing.T) {
		rng1 := newKeccakRandom([]byte("seed1"))
		rng2 := newKeccakRandom([]byte("seed2"))

		// Should produce different sequences
		same := 0
		for i := 0; i < 10; i++ {
			val1 := rng1.nextUint64()
			val2 := rng2.nextUint64()

			if val1 == val2 {
				same++
			}
		}

		if same > 2 {
			t.Errorf("Different seeds produced too many identical values: %d/10", same)
		}
	})

	t.Run("UniformDistribution", func(t *testing.T) {
		rng := newKeccakRandom([]byte("distribution test"))

		// Test nextInt with small bounds
		const bound = 10
		const samples = 10000
		counts := make([]int, bound)

		for i := 0; i < samples; i++ {
			val := rng.nextInt(bound)
			if val < 0 || val >= bound {
				t.Errorf("nextInt(%d) returned out-of-bounds value: %d", bound, val)
			}
			counts[val]++
		}

		// Check reasonable distribution (chi-square test would be better)
		expected := samples / bound
		for i, count := range counts {
			if count < expected/2 || count > expected*2 {
				t.Errorf("Poor distribution for value %d: %d occurrences (expected ~%d)", i, count, expected)
			}
		}
	})

	t.Run("EdgeCases", func(t *testing.T) {
		rng := newKeccakRandom([]byte("edge cases"))

		// Test edge cases for nextInt
		val := rng.nextInt(0)
		if val != 0 {
			t.Errorf("nextInt(0) should return 0, got %d", val)
		}

		val = rng.nextInt(1)
		if val != 0 {
			t.Errorf("nextInt(1) should return 0, got %d", val)
		}

		val = rng.nextInt(-1)
		if val != 0 {
			t.Errorf("nextInt(-1) should return 0, got %d", val)
		}
	})

	t.Run("PowerOfTwoOptimization", func(t *testing.T) {
		rng := newKeccakRandom([]byte("power of two"))

		// Test power-of-2 bounds (should use bitwise AND)
		powerOfTwoBounds := []int{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}

		for _, bound := range powerOfTwoBounds {
			for i := 0; i < 100; i++ {
				val := rng.nextInt(bound)
				if val < 0 || val >= bound {
					t.Errorf("nextInt(%d) returned out-of-bounds value: %d", bound, val)
				}
			}
		}
	})
}

// ============================================================================
// 7. KECCAK SHUFFLE TESTS
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

		// Two elements
		two := []ArbitrageEdgeBinding{
			{cyclePairs: [3]PairID{1, 2, 3}, edgeIndex: 0},
			{cyclePairs: [3]PairID{4, 5, 6}, edgeIndex: 1},
		}
		original1, original2 := two[0], two[1]
		keccakShuffleEdgeBindings(two, PairID(456))

		// Should contain same elements (possibly reordered)
		found1, found2 := false, false
		for _, elem := range two {
			if elem == original1 {
				found1 = true
			}
			if elem == original2 {
				found2 = true
			}
		}
		if !found1 || !found2 {
			t.Error("Shuffle lost elements")
		}
	})

	t.Run("ShuffleQuality", func(t *testing.T) {
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
			changed := false
			for i := range testBindings {
				if testBindings[i].cyclePairs[0] != bindings[i].cyclePairs[0] {
					changed = true
					break
				}
			}
			if changed {
				positionChanges++
			}
		}

		if positionChanges < iterations/2 {
			t.Errorf("Shuffle not random enough: only %d/%d changed", positionChanges, iterations)
		}
	})

	t.Run("PreservesElements", func(t *testing.T) {
		original := []ArbitrageEdgeBinding{
			{cyclePairs: [3]PairID{1, 2, 3}, edgeIndex: 0},
			{cyclePairs: [3]PairID{4, 5, 6}, edgeIndex: 1},
			{cyclePairs: [3]PairID{7, 8, 9}, edgeIndex: 2},
		}

		shuffled := make([]ArbitrageEdgeBinding, len(original))
		copy(shuffled, original)

		keccakShuffleEdgeBindings(shuffled, PairID(789))

		// Verify all original elements are still present
		for _, origElem := range original {
			found := false
			for _, shuffElem := range shuffled {
				if origElem == shuffElem {
					found = true
					break
				}
			}
			if !found {
				t.Error("Shuffle lost an element")
			}
		}

		// Verify no duplicate elements
		for i := 0; i < len(shuffled); i++ {
			for j := i + 1; j < len(shuffled); j++ {
				if shuffled[i] == shuffled[j] {
					t.Error("Shuffle created duplicate elements")
				}
			}
		}
	})
}

// ============================================================================
// 8. SHARD BUILDING TESTS
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

	t.Run("EmptyCycles", func(t *testing.T) {
		clearGlobalState()

		var cycles []ArbitrageTriplet
		buildFanoutShardBuckets(cycles)

		if len(pairShardBuckets) != 0 {
			t.Error("Empty cycles should produce no shard buckets")
		}
	})

	t.Run("SingleCycle", func(t *testing.T) {
		clearGlobalState()

		cycles := []ArbitrageTriplet{
			{PairID(100), PairID(200), PairID(300)},
		}

		buildFanoutShardBuckets(cycles)

		// Should have exactly 3 pairs with shards
		if len(pairShardBuckets) != 3 {
			t.Errorf("Expected 3 shard buckets, got %d", len(pairShardBuckets))
		}

		// Each pair should have exactly 1 binding
		for pairID := PairID(100); pairID <= PairID(300); pairID += 100 {
			shards := pairShardBuckets[pairID]
			if len(shards) != 1 {
				t.Errorf("Pair %d should have 1 shard, got %d", pairID, len(shards))
			}
			if len(shards[0].edgeBindings) != 1 {
				t.Errorf("Pair %d should have 1 binding, got %d", pairID, len(shards[0].edgeBindings))
			}
		}
	})

	t.Run("OverlappingCycles", func(t *testing.T) {
		clearGlobalState()

		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(1), PairID(2), PairID(4)}, // Overlaps with first
			{PairID(1), PairID(3), PairID(4)}, // Overlaps with both
		}

		buildFanoutShardBuckets(cycles)

		// Pair 1 should appear in all 3 cycles
		shards := pairShardBuckets[PairID(1)]
		totalBindings := 0
		for _, shard := range shards {
			totalBindings += len(shard.edgeBindings)
		}
		if totalBindings != 3 {
			t.Errorf("Pair 1 should have 3 bindings, got %d", totalBindings)
		}

		// Pair 2 should appear in 2 cycles
		shards = pairShardBuckets[PairID(2)]
		totalBindings = 0
		for _, shard := range shards {
			totalBindings += len(shard.edgeBindings)
		}
		if totalBindings != 2 {
			t.Errorf("Pair 2 should have 2 bindings, got %d", totalBindings)
		}
	})
}

// ============================================================================
// 9. CORE ASSIGNMENT TESTS
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

		for i, expected := range expectedCores {
			if i >= len(foundCores) || foundCores[i] != expected {
				t.Errorf("Core mismatch at position %d: expected %d, got %d", i, expected, foundCores[i])
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

	t.Run("NoCoreAssignment", func(t *testing.T) {
		pairID := PairID(11111)
		// Don't assign to any cores

		assignment := pairToCoreAssignment[pairID]
		if assignment != 0 {
			t.Errorf("Expected no assignment, got %064b", assignment)
		}
	})

	t.Run("CoreAssignmentPersistence", func(t *testing.T) {
		pairID := PairID(22222)
		coreID := uint8(5)

		RegisterPairToCore(pairID, coreID)

		// Multiple reads should be consistent
		for i := 0; i < 10; i++ {
			assignment := pairToCoreAssignment[pairID]
			expectedBit := uint64(1) << coreID
			if assignment&expectedBit == 0 {
				t.Errorf("Core assignment not persistent on read %d", i)
			}
		}
	})

	t.Run("MultiplePairsSameCore", func(t *testing.T) {
		coreID := uint8(7)
		pairIDs := []PairID{1001, 1002, 1003, 1004}

		for _, pairID := range pairIDs {
			RegisterPairToCore(pairID, coreID)
		}

		for _, pairID := range pairIDs {
			assignment := pairToCoreAssignment[pairID]
			expectedBit := uint64(1) << coreID
			if assignment&expectedBit == 0 {
				t.Errorf("Pair %d not assigned to core %d", pairID, coreID)
			}
		}
	})
}

// ============================================================================
// 10. TICK DISPATCH TESTS
// ============================================================================

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

		// Create mock executor for core 0
		coreExecutors[0] = &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
		}
		coreExecutors[1] = &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
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

		coreExecutors[0] = &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
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

	t.Run("NoCoreAssignment", func(t *testing.T) {
		addr := generateMockAddress(777)
		pairID := PairID(777)
		RegisterPairAddress(addr[:], pairID)
		// Don't assign to any cores

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		copy(logView.Addr[:42], addr[:])

		// Should handle gracefully when no cores assigned
		DispatchTickUpdate(logView)
	})

	t.Run("MissingExecutor", func(t *testing.T) {
		addr := generateMockAddress(888)
		pairID := PairID(888)
		RegisterPairAddress(addr[:], pairID)
		RegisterPairToCore(pairID, 2)

		// Don't create executor for core 2
		coreExecutors[2] = nil
		coreRings[2] = ring24.New(16)

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		copy(logView.Addr[:42], addr[:])

		// Should handle gracefully when executor is nil
		DispatchTickUpdate(logView)
	})

	t.Run("MessageBufferOverlay", func(t *testing.T) {
		addr := generateMockAddress(999)
		pairID := PairID(999)
		RegisterPairAddress(addr[:], pairID)
		RegisterPairToCore(pairID, 3)

		if coreRings[3] == nil {
			coreRings[3] = ring24.New(16)
		}

		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
		}
		coreExecutors[3] = executor

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		copy(logView.Addr[:42], addr[:])

		reserve0, reserve1 := uint64(2000), uint64(1000)
		for i := 0; i < 8; i++ {
			logView.Data[24+i] = byte(reserve0 >> (8 * (7 - i)))
			logView.Data[56+i] = byte(reserve1 >> (8 * (7 - i)))
		}

		// Clear message buffer before test
		for i := range executor.messageBuffer {
			executor.messageBuffer[i] = 0
		}

		DispatchTickUpdate(logView)

		// Test the overlay capability directly (the main purpose of message buffer)
		tickUpdate := (*TickUpdate)(unsafe.Pointer(&executor.messageBuffer))
		tickUpdate.pairID = pairID
		tickUpdate.forwardTick = 1.23
		tickUpdate.reverseTick = -1.23

		// Verify the overlay works correctly
		if tickUpdate.pairID != pairID || tickUpdate.forwardTick != 1.23 || tickUpdate.reverseTick != -1.23 {
			t.Error("Message buffer TickUpdate overlay failed")
		}

		// Verify we can read the data back through the overlay
		readBack := (*TickUpdate)(unsafe.Pointer(&executor.messageBuffer))
		if readBack.pairID != pairID || readBack.forwardTick != 1.23 || readBack.reverseTick != -1.23 {
			t.Error("Message buffer TickUpdate overlay read-back failed")
		}
	})
}

// ============================================================================
// 11. FULL SYSTEM INTEGRATION TESTS
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

	t.Run("EmptyInitialization", func(t *testing.T) {
		clearGlobalState()

		var cycles []ArbitrageTriplet
		InitializeArbitrageSystem(cycles)
		time.Sleep(50 * time.Millisecond)

		// Should handle empty cycles gracefully
		if len(pairShardBuckets) != 0 {
			t.Error("Empty cycles should create no shard buckets")
		}

		control.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("CoreCountHandling", func(t *testing.T) {
		clearGlobalState()

		cycles := []ArbitrageTriplet{
			{PairID(10), PairID(11), PairID(12)},
		}

		// Test the core count calculation and even number enforcement
		originalCPU := runtime.NumCPU()

		InitializeArbitrageSystem(cycles)
		time.Sleep(50 * time.Millisecond)

		// Should have created some executors
		activeExecutors := 0
		for i := range coreExecutors {
			if coreExecutors[i] != nil {
				activeExecutors++
			}
		}

		if activeExecutors == 0 {
			t.Error("No executors created")
		}

		// Should be even number
		if activeExecutors%2 != 0 {
			t.Errorf("Executor count should be even, got %d", activeExecutors)
		}

		control.Shutdown()
		time.Sleep(50 * time.Millisecond)

		_ = originalCPU // Use variable to avoid unused warning
	})
}

// ============================================================================
// 12. ZERO-ALLOCATION RUNTIME TESTS
// ============================================================================

func TestZeroAllocationRuntime(t *testing.T) {
	clearGlobalState()

	t.Run("MockTickProcessing", func(t *testing.T) {
		// Create a minimal executor setup
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(10),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		// Initialize one queue
		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		// Add one cycle state
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.1, 0.2, 0.3},
			pairIDs:    [3]PairID{PairID(100), PairID(200), PairID(300)},
		}

		// Measure memory before operations
		var memStatsBefore, memStatsAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memStatsBefore)

		// Simulate tick processing using pre-allocated buffers
		tickUpdate := (*TickUpdate)(unsafe.Pointer(&executor.messageBuffer))
		tickUpdate.forwardTick = 1.5
		tickUpdate.reverseTick = -1.5
		tickUpdate.pairID = PairID(123)

		// Use processedCycles buffer
		executor.processedCycles[0] = ProcessedCycle{
			originalTick:    quantizeTickToInt64(1.5),
			cycleStateIndex: CycleStateIndex(0),
		}

		// Simulate some processing
		for i := 0; i < 10; i++ {
			executor.processedCycles[i%3] = ProcessedCycle{
				originalTick:    quantizeTickToInt64(float64(i) * 0.1),
				cycleStateIndex: CycleStateIndex(i % 2),
			}
		}

		runtime.ReadMemStats(&memStatsAfter)

		// Should have zero heap allocations
		allocDelta := memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc
		if allocDelta > 0 {
			t.Errorf("Tick processing simulation caused %d bytes of allocation", allocDelta)
		}
	})

	t.Run("MessageBufferReuse", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{}

		var memStatsBefore, memStatsAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memStatsBefore)

		// Simulate multiple message preparations using the same buffer
		for i := 0; i < 1000; i++ {
			tickUpdate := (*TickUpdate)(unsafe.Pointer(&executor.messageBuffer))
			tickUpdate.forwardTick = float64(i) * 0.01
			tickUpdate.reverseTick = float64(-i) * 0.01
			tickUpdate.pairID = PairID(i)
		}

		runtime.ReadMemStats(&memStatsAfter)

		// Should have zero heap allocations
		allocDelta := memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc
		if allocDelta > 0 {
			t.Errorf("Message buffer reuse caused %d bytes of allocation", allocDelta)
		}
	})

	t.Run("PreAllocatedBuffers", func(t *testing.T) {
		executor := ArbitrageCoreExecutor{}

		// Verify buffer size is exactly 128 elements
		if len(executor.processedCycles) != 128 {
			t.Errorf("processedCycles buffer should have 128 elements, got %d", len(executor.processedCycles))
		}

		// Test buffer accessibility
		executor.processedCycles[0] = ProcessedCycle{
			originalTick:    12345,
			cycleStateIndex: CycleStateIndex(67890),
		}

		if executor.processedCycles[0].originalTick != 12345 {
			t.Error("processedCycles buffer access failed")
		}
		if executor.processedCycles[0].cycleStateIndex != CycleStateIndex(67890) {
			t.Error("processedCycles buffer state access failed")
		}

		// Verify buffer size is exactly 24 bytes
		if len(executor.messageBuffer) != 24 {
			t.Errorf("messageBuffer should be 24 bytes, got %d", len(executor.messageBuffer))
		}

		// Test TickUpdate overlay
		tickUpdate := (*TickUpdate)(unsafe.Pointer(&executor.messageBuffer))
		tickUpdate.forwardTick = 1.23
		tickUpdate.reverseTick = -4.56
		tickUpdate.pairID = PairID(789)

		// Verify the overlay works correctly
		if tickUpdate.forwardTick != 1.23 {
			t.Error("messageBuffer TickUpdate overlay failed for forwardTick")
		}
		if tickUpdate.reverseTick != -4.56 {
			t.Error("messageBuffer TickUpdate overlay failed for reverseTick")
		}
		if tickUpdate.pairID != PairID(789) {
			t.Error("messageBuffer TickUpdate overlay failed for pairID")
		}
	})
}

// ============================================================================
// 13. CORE ISOLATION AND ARCHITECTURE TESTS
// ============================================================================

func TestCoreIsolation(t *testing.T) {
	clearGlobalState()

	t.Run("PerCorePrivateState", func(t *testing.T) {
		// Test that initialization properly isolates core state
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(4), PairID(5), PairID(6)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(50 * time.Millisecond)

		// Each core should have independent executors
		coreCount := 0
		for i := 0; i < 8; i++ {
			if coreExecutors[i] != nil {
				coreCount++
			}
		}

		if coreCount == 0 {
			t.Error("No core executors were created")
		}

		t.Logf("Created %d isolated core executors", coreCount)

		control.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("AddressRegistrationGlobal", func(t *testing.T) {
		// Address registration is global (shared read-only after init)
		clearGlobalState()

		addresses := make([][42]byte, 10)
		for i := range addresses {
			addresses[i] = generateMockAddress(uint64(i * 1000))
			RegisterPairAddress(addresses[i][:], PairID(i+1))
		}

		// All addresses should be findable
		for i := range addresses {
			found := lookupPairIDByAddress(addresses[i][:])
			if found != PairID(i+1) {
				t.Errorf("Address %d: expected %d, got %d", i, i+1, found)
			}
		}
	})

	t.Run("CoreAssignmentGlobal", func(t *testing.T) {
		// Core assignments are global (shared read-only after init)
		clearGlobalState()

		testPairs := []PairID{100, 200, 300}
		testCores := []uint8{0, 1, 2}

		for _, pairID := range testPairs {
			for _, coreID := range testCores {
				RegisterPairToCore(pairID, coreID)
			}
		}

		// All assignments should be readable
		for _, pairID := range testPairs {
			assignment := pairToCoreAssignment[pairID]
			for _, coreID := range testCores {
				expectedBit := uint64(1) << coreID
				if assignment&expectedBit == 0 {
					t.Errorf("Pair %d not assigned to core %d", pairID, coreID)
				}
			}
		}
	})

	t.Run("ExecutorHotnessOrdering", func(t *testing.T) {
		executor := ArbitrageCoreExecutor{}

		// isReverseDirection should be at offset 0 (hottest field)
		isReverseOffset := unsafe.Offsetof(executor.isReverseDirection)
		if isReverseOffset != 0 {
			t.Errorf("isReverseDirection should be at offset 0, got %d", isReverseOffset)
		}

		// pairToQueueIndex should come early (hot field)
		pairToQueueOffset := unsafe.Offsetof(executor.pairToQueueIndex)
		shutdownOffset := unsafe.Offsetof(executor.shutdownSignal)

		if pairToQueueOffset >= shutdownOffset {
			t.Error("Hot field (pairToQueueIndex) should come before cold field (shutdownSignal)")
		}

		// Pre-allocated buffers should be at the end
		processedCyclesOffset := unsafe.Offsetof(executor.processedCycles)
		messageBufferOffset := unsafe.Offsetof(executor.messageBuffer)

		if processedCyclesOffset < shutdownOffset || messageBufferOffset < shutdownOffset {
			t.Error("Pre-allocated buffers should come after all operational fields")
		}
	})

	t.Run("BufferSizes", func(t *testing.T) {
		executor := ArbitrageCoreExecutor{}

		// Check processedCycles is exactly 128 * 32 bytes = 4096 bytes
		processedCyclesSize := unsafe.Sizeof(executor.processedCycles)
		expectedProcessedSize := uintptr(128 * 32) // 128 elements * 32 bytes each
		if processedCyclesSize != expectedProcessedSize {
			t.Errorf("processedCycles size is %d bytes, expected %d bytes", processedCyclesSize, expectedProcessedSize)
		}

		// Check messageBuffer is exactly 24 bytes
		messageBufferSize := unsafe.Sizeof(executor.messageBuffer)
		if messageBufferSize != 24 {
			t.Errorf("messageBuffer size is %d bytes, expected 24 bytes", messageBufferSize)
		}
	})
}

// ============================================================================
// 14. PROCESSED CYCLE STRUCT TESTS
// ============================================================================

func TestProcessedCycleStruct(t *testing.T) {
	t.Run("StructSize", func(t *testing.T) {
		size := unsafe.Sizeof(ProcessedCycle{})
		expectedSize := uintptr(32) // 4+4+8+8+8 = 32 bytes with padding

		if size != expectedSize {
			t.Errorf("ProcessedCycle size is %d bytes, expected %d bytes", size, expectedSize)
		}
	})

	t.Run("FieldAccess", func(t *testing.T) {
		cycle := ProcessedCycle{
			originalTick:    9876543210,
			cycleStateIndex: CycleStateIndex(1234567890),
		}

		if cycle.originalTick != 9876543210 {
			t.Error("ProcessedCycle originalTick field access failed")
		}
		if cycle.cycleStateIndex != CycleStateIndex(1234567890) {
			t.Error("ProcessedCycle cycleStateIndex field access failed")
		}
	})

	t.Run("ArrayAccess", func(t *testing.T) {
		cycles := [3]ProcessedCycle{
			{originalTick: 100, cycleStateIndex: CycleStateIndex(1)},
			{originalTick: 200, cycleStateIndex: CycleStateIndex(2)},
			{originalTick: 300, cycleStateIndex: CycleStateIndex(3)},
		}

		for i, expected := range []int64{100, 200, 300} {
			if cycles[i].originalTick != expected {
				t.Errorf("ProcessedCycle array access failed at index %d", i)
			}
		}
	})
}

// ============================================================================
// 15. OPTIMIZED STRUCT LAYOUT TESTS
// ============================================================================

func TestOptimizedStructLayout(t *testing.T) {
	t.Run("MemoryEfficiency", func(t *testing.T) {
		// Test that we've eliminated unnecessary padding while adding essential buffers
		totalCoreStructSize := unsafe.Sizeof(ArbitrageCoreExecutor{})

		// Should be around 4272 bytes (includes 4096B + 24B pre-allocated buffers)
		expectedMinSize := uintptr(4200) // At least 4200 bytes with buffers
		expectedMaxSize := uintptr(4300) // But not more than 4300 bytes

		if totalCoreStructSize < expectedMinSize || totalCoreStructSize > expectedMaxSize {
			t.Errorf("ArbitrageCoreExecutor size is %d bytes, expected between %d-%d bytes",
				totalCoreStructSize, expectedMinSize, expectedMaxSize)
		}

		// Test that smaller structs are indeed compact
		compactStructs := []struct {
			name        string
			size        uintptr
			maxExpected uintptr
		}{
			{"AddressKey", unsafe.Sizeof(AddressKey{}), 32},
			{"ArbitrageCycleState", unsafe.Sizeof(ArbitrageCycleState{}), 64},
			{"ArbitrageEdgeBinding", unsafe.Sizeof(ArbitrageEdgeBinding{}), 48},
			{"FanoutEntry", unsafe.Sizeof(FanoutEntry{}), 48},
			{"PairShardBucket", unsafe.Sizeof(PairShardBucket{}), 48},
		}

		for _, s := range compactStructs {
			if s.size > s.maxExpected {
				t.Errorf("%s is %d bytes, should be ≤ %d bytes (too much padding)",
					s.name, s.size, s.maxExpected)
			}
		}

		t.Logf("ArbitrageCoreExecutor total size: %d bytes (includes 4120B of pre-allocated buffers)", totalCoreStructSize)
	})

	t.Run("CompactAddressStorage", func(t *testing.T) {
		// Test that AddressKey efficiently stores 160-bit addresses
		addr1 := generateMockAddress(0x1111111111111111)
		addr2 := generateMockAddress(0x2222222222222222)

		key1 := bytesToAddressKey(addr1[:])
		key2 := bytesToAddressKey(addr2[:])

		// Keys should be different
		if key1.isEqual(key2) {
			t.Error("Different addresses should produce different keys")
		}

		// Should use all available space efficiently
		usedWords := 0
		for i := 0; i < 3; i++ {
			if key1.words[i] != 0 {
				usedWords++
			}
		}

		if usedWords < 2 {
			t.Error("AddressKey should efficiently use available words")
		}
	})
}

// ============================================================================
// 16. EDGE CASE TESTS
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
		// Test that large structs work correctly
		cycle := ArbitrageCycleState{
			tickValues: [3]float64{1.1, 2.2, 3.3},
			pairIDs:    [3]PairID{100, 200, 300},
		}

		// Verify data is accessible
		if cycle.tickValues[0] != 1.1 {
			t.Error("Struct data access failed")
		}

		if cycle.pairIDs[2] != 300 {
			t.Error("Struct data access failed")
		}
	})

	t.Run("MaximumAddressTableUsage", func(t *testing.T) {
		clearGlobalState()

		// Test near-maximum usage of address table
		maxEntries := constants.AddressTableCapacity / 8 // Use 1/8 to avoid overflow

		for i := 0; i < maxEntries; i++ {
			addr := generateMockAddress(uint64(i * 2654435761)) // Large prime
			RegisterPairAddress(addr[:], PairID(i+1))
		}

		// Verify a sample of entries
		for i := 0; i < maxEntries; i += maxEntries / 10 {
			addr := generateMockAddress(uint64(i * 2654435761))
			found := lookupPairIDByAddress(addr[:])
			if found != PairID(i+1) {
				t.Errorf("Failed to find entry %d in maximum usage scenario", i)
			}
		}
	})

	t.Run("InvalidAddressFormats", func(t *testing.T) {
		// Test various invalid address formats
		invalidAddresses := []string{
			"invalid_address",
			"0x", // Too short
			"1234567890abcdef1234567890abcdef12345678",   // Missing 0x
			"0xGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG", // Invalid hex
		}

		for _, invalidAddr := range invalidAddresses {
			var addr [42]byte
			if len(invalidAddr) <= 42 {
				copy(addr[:], invalidAddr)
				// Should not panic
				key := bytesToAddressKey(addr[:])
				_ = key
			}
		}
	})
}

// ============================================================================
// 17. PERFORMANCE REGRESSION TESTS
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

		// Should be sub-50ns on modern hardware
		if avgLatency > 50*time.Nanosecond {
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

		coreExecutors[0] = &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
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

// ============================================================================
// 18. COMPLETE WORKFLOW TESTS
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
// 20. ARBITRAGE OPPORTUNITY EMISSION TESTS
// ============================================================================

func TestArbitrageOpportunityEmission(t *testing.T) {
	t.Run("EmitOpportunity", func(t *testing.T) {
		cycle := &ArbitrageCycleState{
			tickValues: [3]float64{-0.1, -0.2, -0.3},
			pairIDs:    [3]PairID{PairID(100), PairID(200), PairID(300)},
		}

		newTick := -0.5

		// Should not panic
		emitArbitrageOpportunity(cycle, newTick)
	})

	t.Run("EmitWithZeroValues", func(t *testing.T) {
		cycle := &ArbitrageCycleState{
			tickValues: [3]float64{0.0, 0.0, 0.0},
			pairIDs:    [3]PairID{PairID(0), PairID(0), PairID(0)},
		}

		newTick := 0.0

		// Should not panic
		emitArbitrageOpportunity(cycle, newTick)
	})

	t.Run("EmitWithExtremeValues", func(t *testing.T) {
		cycle := &ArbitrageCycleState{
			tickValues: [3]float64{-1000.0, 1000.0, -500.0},
			pairIDs:    [3]PairID{PairID(1), PairID(2), PairID(3)},
		}

		newTick := math.Inf(1)

		// Should not panic
		emitArbitrageOpportunity(cycle, newTick)
	})
}

// ============================================================================
// 21. TICK PROCESSING TESTS
// ============================================================================

func TestTickProcessing(t *testing.T) {
	clearGlobalState()

	t.Run("ProcessTickUpdateBasic", func(t *testing.T) {
		// Create a minimal but functional executor
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(10),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		// Initialize queue
		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		// Add cycle state
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.1, 0.2, 0.3},
			pairIDs:    [3]PairID{PairID(100), PairID(200), PairID(300)},
		}

		// Add an entry to the queue so PeepMin doesn't fail
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		// Create tick update
		update := &TickUpdate{
			forwardTick: 1.5,
			reverseTick: -1.5,
			pairID:      PairID(123),
		}

		// Should not panic
		processTickUpdate(executor, update)
	})

	t.Run("ProcessTickUpdateReverse", func(t *testing.T) {
		// Test reverse direction
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: true, // Reverse direction
			pairToQueueIndex:   localidx.New(10),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(456, 0)

		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{-0.1, -0.2, -0.3},
			pairIDs:    [3]PairID{PairID(400), PairID(500), PairID(600)},
		}

		// Add an entry to the queue so PeepMin doesn't fail
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		update := &TickUpdate{
			forwardTick: 2.0,
			reverseTick: -2.0,
			pairID:      PairID(456),
		}

		// Should not panic and should use reverse tick
		processTickUpdate(executor, update)
	})

	t.Run("ProcessTickUpdateUnknownPair", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(10),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		// Initialize one queue with an entry (since processTickUpdate expects non-empty queues)
		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.fanoutTables[0] = []FanoutEntry{} // Empty fanout table

		// Add a cycle state
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.0, 0.0, 0.0},
			pairIDs:    [3]PairID{PairID(1), PairID(2), PairID(3)},
		}

		// Add an entry to the queue so PeepMin doesn't panic
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		// Don't register pair 999 in pairToQueueIndex, so it defaults to queue 0
		// This tests the case where a pair uses the default queue

		update := &TickUpdate{
			forwardTick: 1.0,
			reverseTick: -1.0,
			pairID:      PairID(999), // Unknown pair -> defaults to queue index 0
		}

		// Should handle gracefully - unknown pairs default to queue index 0
		processTickUpdate(executor, update)
	})

	t.Run("ProcessTickUpdateWithFanout", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(10),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			cycleStates:        make([]ArbitrageCycleState, 2),
		}

		// Initialize queue
		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(789, 0)

		// Initialize cycle states
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.1, 0.2, 0.3},
			pairIDs:    [3]PairID{PairID(700), PairID(800), PairID(900)},
		}
		executor.cycleStates[1] = ArbitrageCycleState{
			tickValues: [3]float64{-0.1, -0.2, -0.3},
			pairIDs:    [3]PairID{PairID(701), PairID(801), PairID(901)},
		}

		// Create fanout entries
		handle1, _ := executor.priorityQueues[0].BorrowSafe()
		handle2, _ := executor.priorityQueues[0].BorrowSafe()

		// Add entries to queue so PeepMin doesn't fail
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle1, 0)
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle2, 1)

		executor.fanoutTables[0] = []FanoutEntry{
			{
				edgeIndex:       1,
				cycleStateIndex: 0,
				queueHandle:     uint64(handle1),
				queue:           &executor.priorityQueues[0],
			},
			{
				edgeIndex:       2,
				cycleStateIndex: 1,
				queueHandle:     uint64(handle2),
				queue:           &executor.priorityQueues[0],
			},
		}

		update := &TickUpdate{
			forwardTick: 0.5,
			reverseTick: -0.5,
			pairID:      PairID(789),
		}

		// Should update fanout entries
		processTickUpdate(executor, update)

		// Verify cycle states were updated
		if executor.cycleStates[0].tickValues[1] != 0.5 {
			t.Error("Fanout update failed for cycle 0")
		}
		if executor.cycleStates[1].tickValues[2] != 0.5 {
			t.Error("Fanout update failed for cycle 1")
		}
	})
}

// ============================================================================
// 22. ADDITIONAL COVERAGE TESTS
// ============================================================================

func TestAdditionalCoverage(t *testing.T) {
	t.Run("ArbitrageTripletType", func(t *testing.T) {
		// Test ArbitrageTriplet type
		triplet := ArbitrageTriplet{PairID(1), PairID(2), PairID(3)}

		if triplet[0] != PairID(1) || triplet[1] != PairID(2) || triplet[2] != PairID(3) {
			t.Error("ArbitrageTriplet access failed")
		}
	})

	t.Run("CycleStateIndexType", func(t *testing.T) {
		// Test CycleStateIndex type
		index := CycleStateIndex(42)

		if uint64(index) != 42 {
			t.Error("CycleStateIndex conversion failed")
		}
	})

	t.Run("PairIDType", func(t *testing.T) {
		// Test PairID type operations
		id1 := PairID(100)
		id2 := PairID(200)

		if id1 >= id2 {
			t.Error("PairID comparison failed")
		}

		if uint64(id1) != 100 {
			t.Error("PairID conversion failed")
		}
	})

	t.Run("EmptyShardAttachment", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
			priorityQueues:   make([]quantumqueue64.QuantumQueue64, 0),
			fanoutTables:     make([][]FanoutEntry, 0),
			cycleStates:      make([]ArbitrageCycleState, 0),
		}

		shard := &PairShardBucket{
			pairID:       PairID(1000),
			edgeBindings: []ArbitrageEdgeBinding{},
		}

		// Should handle empty shard gracefully
		attachShardToExecutor(executor, shard)
	})

	t.Run("ShardWorkerLaunch", func(t *testing.T) {
		// Test shard worker launch parameters
		shardChannel := make(chan PairShardBucket, 1)

		// Send a test shard
		shard := PairShardBucket{
			pairID: PairID(2000),
			edgeBindings: []ArbitrageEdgeBinding{
				{
					cyclePairs: [3]PairID{PairID(2000), PairID(2001), PairID(2002)},
					edgeIndex:  0,
				},
			},
		}

		shardChannel <- shard
		close(shardChannel)

		// Launch worker (this will exit quickly due to closed channel)
		go launchShardWorker(0, 1, shardChannel)

		// Give it time to process
		time.Sleep(10 * time.Millisecond)
	})

	t.Run("CoreCountEdgeCases", func(t *testing.T) {
		// Test core count manipulation
		testCases := []int{1, 2, 3, 4, 5, 15, 16, 17, 63, 64, 65}

		for _, count := range testCases {
			// Test the even count enforcement
			result := count
			result &^= 1 // Force even

			if result%2 != 0 {
				t.Errorf("Even enforcement failed for %d: got %d", count, result)
			}

			if result > count {
				t.Errorf("Even enforcement increased value: %d -> %d", count, result)
			}

			if count > 0 && result < count-1 {
				t.Errorf("Even enforcement decreased too much: %d -> %d", count, result)
			}
		}
	})
}

// ============================================================================
// 23. BENCHMARKS - ORGANIZED BY COMPONENT
// ============================================================================

// Micro-operation benchmarks
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

// Keccak shuffle benchmarks
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

// System-level benchmarks
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
		coreExecutors[i] = &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
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

	// Initialize rings and executors
	for i := 0; i < 8; i++ {
		if coreRings[i] == nil {
			coreRings[i] = ring24.New(1024)
		}
		coreExecutors[i] = &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
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

// Memory access pattern benchmarks
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

// Zero allocation verification benchmark
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
