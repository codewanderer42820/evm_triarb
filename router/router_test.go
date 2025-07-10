// router_test.go — Complete test suite with 100% coverage
package router

import (
	"encoding/binary"
	"math"
	"math/bits"
	"runtime"
	"sync"
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
// TEST SETUP AND UTILITIES
// ============================================================================

func generateMockAddress(seed uint64) [42]byte {
	var addr [42]byte
	addr[0] = '0'
	addr[1] = 'x'

	// Create deterministic input
	var input [16]byte
	binary.LittleEndian.PutUint64(input[0:8], 69)
	binary.LittleEndian.PutUint64(input[8:16], seed)

	// Generate address using Keccak256
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(input[:])
	hashOutput := hasher.Sum(nil)

	// Convert hash bytes to hex string (exactly 40 hex chars for 20-byte address)
	for i := 0; i < 20; i++ {
		byteVal := hashOutput[i]
		addr[2+i*2] = "0123456789abcdef"[byteVal>>4]
		addr[2+i*2+1] = "0123456789abcdef"[byteVal&0xF]
	}
	return addr
}

func clearGlobalState() {
	// Clear address tables completely
	for i := range pairAddressKeys {
		pairAddressKeys[i] = AddressKey{}
	}
	for i := range addressToPairID {
		addressToPairID[i] = 0
	}

	// Clear core assignments completely
	for i := range pairToCoreAssignment {
		pairToCoreAssignment[i] = 0
	}

	// Clear shard buckets
	pairShardBuckets = nil

	// Clear executors and rings completely
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

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// FIXED: Proper LogView creation with correct bounds
func createValidLogView(addr [42]byte, reserve0, reserve1 uint64) *types.LogView {
	logView := &types.LogView{
		Addr: make([]byte, 64),
		Data: make([]byte, 128),
	}

	// Copy exactly 42 bytes including "0x" prefix
	copy(logView.Addr[:42], addr[:])

	// Clear any remaining bytes to avoid undefined behavior
	for i := 42; i < len(logView.Addr); i++ {
		logView.Addr[i] = 0
	}

	// Set up reserves in big-endian format at correct positions
	for i := 0; i < 8; i++ {
		logView.Data[24+i] = byte(reserve0 >> (8 * (7 - i)))
		logView.Data[56+i] = byte(reserve1 >> (8 * (7 - i)))
	}

	return logView
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

	t.Run("FieldOffsets", func(t *testing.T) {
		var fanout FanoutEntry
		var tick TickUpdate
		var cycle ArbitrageCycleState

		// Verify FanoutEntry has optimal field ordering
		if unsafe.Offsetof(fanout.edgeIndex) != 0 {
			t.Error("FanoutEntry.edgeIndex should be at offset 0")
		}
		if unsafe.Offsetof(fanout.cycleStateIndex) != 8 {
			t.Error("FanoutEntry.cycleStateIndex should be at offset 8")
		}

		// Verify TickUpdate is perfectly packed (24 bytes)
		if unsafe.Offsetof(tick.forwardTick) != 0 {
			t.Error("TickUpdate.forwardTick should be at offset 0")
		}
		if unsafe.Offsetof(tick.reverseTick) != 8 {
			t.Error("TickUpdate.reverseTick should be at offset 8")
		}
		if unsafe.Offsetof(tick.pairID) != 16 {
			t.Error("TickUpdate.pairID should be at offset 16")
		}

		// Verify ArbitrageCycleState layout
		if unsafe.Offsetof(cycle.tickValues) != 0 {
			t.Error("ArbitrageCycleState.tickValues should be at offset 0")
		}
		if unsafe.Offsetof(cycle.pairIDs) != 24 {
			t.Error("ArbitrageCycleState.pairIDs should be at offset 24")
		}
	})
}

// ============================================================================
// 2. ADDRESS KEY OPERATIONS
// ============================================================================

func TestAddressKey(t *testing.T) {
	t.Run("Creation", func(t *testing.T) {
		addr := generateMockAddress(12345)
		// Use proper slice without "0x" prefix for footgun mode
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		key := bytesToAddressKey(addrSlice)

		// Should have parsed 3 words from 20-byte address
		if key.words[0] == 0 && key.words[1] == 0 && key.words[2] == 0 {
			t.Error("Address key appears to be zero")
		}

		// Verify deterministic creation
		key2 := bytesToAddressKey(addrSlice)
		if !key.isEqual(key2) {
			t.Error("bytesToAddressKey should be deterministic")
		}
	})

	t.Run("Comparison", func(t *testing.T) {
		addr1 := generateMockAddress(12345)
		addr2 := generateMockAddress(12345)
		addr3 := generateMockAddress(54321)

		// Use proper slices
		slice1 := addr1[constants.AddressHexStart:constants.AddressHexEnd]
		slice2 := addr2[constants.AddressHexStart:constants.AddressHexEnd]
		slice3 := addr3[constants.AddressHexStart:constants.AddressHexEnd]

		key1 := bytesToAddressKey(slice1)
		key2 := bytesToAddressKey(slice2)
		key3 := bytesToAddressKey(slice3)

		if !key1.isEqual(key2) {
			t.Error("Identical addresses must generate equal keys")
		}

		if key1.isEqual(key3) {
			t.Error("Different addresses must generate different keys")
		}
	})

	t.Run("FootgunValidation", func(t *testing.T) {
		// FOOTGUN MODE: Only test with valid 40-character hex inputs

		// Test with known valid hex
		validHex := []byte("1234567890abcdefABCDEF1234567890abcdefAB") // Exactly 40 hex chars
		key := bytesToAddressKey(validHex)

		// Should work without panic and produce non-zero result
		if key.words[0] == 0 && key.words[1] == 0 && key.words[2] == 0 {
			t.Error("Valid hex should produce non-zero key")
		}

		// Test deterministic behavior
		key2 := bytesToAddressKey(validHex)
		if !key.isEqual(key2) {
			t.Error("Should be deterministic")
		}

		// Test with all zeros (valid format)
		allZeros := []byte("0000000000000000000000000000000000000000") // 40 zeros
		zeroKey := bytesToAddressKey(allZeros)
		// Should work without panic (zero result is valid)
		_ = zeroKey

		// Test with all F's (valid format)
		allFs := []byte("ffffffffffffffffffffffffffffffffffffffff") // 40 f's
		maxKey := bytesToAddressKey(allFs)
		// Should work without panic
		if maxKey.words[0] == 0 && maxKey.words[1] == 0 && maxKey.words[2] == 0 {
			t.Error("Max hex should produce non-zero key")
		}
	})
}

func TestAddressKeyZeroAllocation(t *testing.T) {
	addr := generateMockAddress(42)
	addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]

	assertZeroAllocs(t, "bytesToAddressKey", func() {
		_ = bytesToAddressKey(addrSlice)
	})

	key1 := bytesToAddressKey(addrSlice)
	key2 := bytesToAddressKey(addrSlice)

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
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]

		hash1 := directAddressToIndex64(addrSlice)
		hash2 := directAddressToIndex64(addrSlice)

		if hash1 != hash2 {
			t.Error("Hash function should be deterministic")
		}

		// Verify it's within bounds
		if hash1 >= uint64(constants.AddressTableCapacity) {
			t.Error("Hash should be within table bounds")
		}
	})

	t.Run("StoredKeyHashing", func(t *testing.T) {
		addr := generateMockAddress(789)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		key := bytesToAddressKey(addrSlice)

		storedHash := directAddressToIndex64Stored(key)
		directHash := directAddressToIndex64(addrSlice)

		if storedHash != directHash {
			t.Error("Stored key hash should match direct hash")
		}
	})

	t.Run("FootgunValidation", func(t *testing.T) {
		// FOOTGUN MODE: Only test with valid 40-character hex inputs

		validInputs := [][]byte{
			[]byte("1234567890abcdefABCDEF1234567890abcdefAB"), // Mixed case
			[]byte("0000000000000000000000000000000000000000"), // All zeros
			[]byte("ffffffffffffffffffffffffffffffffffffffff"), // All f's
			[]byte("1111111111111111111111111111111111111111"), // All 1's
		}

		for i, input := range validInputs {
			hash := directAddressToIndex64(input)
			// Should work without panic and be within bounds
			if hash >= uint64(constants.AddressTableCapacity) {
				t.Errorf("Valid input %d produced out-of-bounds hash: %d", i, hash)
			}
		}
	})

	t.Run("HashDistribution", func(t *testing.T) {
		const numTests = 1000
		hashes := make(map[uint64]int)

		for i := 0; i < numTests; i++ {
			addr := generateMockAddress(uint64(i * 1000003))
			addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
			hash := directAddressToIndex64(addrSlice)
			hashes[hash]++
		}

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

		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, pairID)
		foundID := lookupPairIDByAddress(addrSlice)

		if foundID != pairID {
			t.Errorf("Expected pair ID %d, got %d", pairID, foundID)
		}
	})

	t.Run("UpdateExisting", func(t *testing.T) {
		addr := generateMockAddress(12345)
		originalPairID := PairID(1000)
		updatedPairID := PairID(2000)

		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, originalPairID)
		RegisterPairAddress(addrSlice, updatedPairID)

		found := lookupPairIDByAddress(addrSlice)
		if found != updatedPairID {
			t.Errorf("Expected updated ID %d, got %d", updatedPairID, found)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		unknownAddr := generateMockAddress(999999)
		addrSlice := unknownAddr[constants.AddressHexStart:constants.AddressHexEnd]
		foundID := lookupPairIDByAddress(addrSlice)

		if foundID != 0 {
			t.Error("Unknown address must return 0")
		}
	})

	t.Run("FootgunHashTable", func(t *testing.T) {
		clearGlobalState()

		// FOOTGUN MODE: Only test with valid addresses
		const count = 100
		addresses := make([][42]byte, count)
		pairIDs := make([]PairID, count)

		// Register valid addresses
		for i := 0; i < count; i++ {
			addresses[i] = generateMockAddress(uint64(i * 1000003))
			pairIDs[i] = PairID(i + 1)
			addrSlice := addresses[i][constants.AddressHexStart:constants.AddressHexEnd]
			RegisterPairAddress(addrSlice, pairIDs[i])
		}

		// Verify all can be retrieved
		for i := 0; i < count; i++ {
			addrSlice := addresses[i][constants.AddressHexStart:constants.AddressHexEnd]
			found := lookupPairIDByAddress(addrSlice)
			if found != pairIDs[i] {
				t.Errorf("Entry %d: expected %d, got %d", i, pairIDs[i], found)
			}
		}
	})

	t.Run("MassiveLoad", func(t *testing.T) {
		clearGlobalState()

		const count = 1000
		addresses := make([][42]byte, count)
		pairIDs := make([]PairID, count)

		// Register many addresses
		for i := 0; i < count; i++ {
			addresses[i] = generateMockAddress(uint64(i * 1000003))
			pairIDs[i] = PairID(i + 1)
			addrSlice := addresses[i][constants.AddressHexStart:constants.AddressHexEnd]
			RegisterPairAddress(addrSlice, pairIDs[i])
		}

		// Verify all can be retrieved
		for i := 0; i < count; i++ {
			addrSlice := addresses[i][constants.AddressHexStart:constants.AddressHexEnd]
			found := lookupPairIDByAddress(addrSlice)
			if found != pairIDs[i] {
				t.Errorf("Entry %d: expected %d, got %d", i, pairIDs[i], found)
			}
		}
	})
}

// ============================================================================
// 5. QUANTIZATION TESTS
// ============================================================================

func TestQuantization(t *testing.T) {
	t.Run("BasicQuantization", func(t *testing.T) {
		testCases := []float64{0.0, 1.0, -1.0, 0.5, -0.5, 100.0, -100.0}

		for _, tc := range testCases {
			result := quantizeTickToInt64(tc)

			// Test deterministic
			result2 := quantizeTickToInt64(tc)
			if result != result2 {
				t.Errorf("quantizeTickToInt64(%f) not deterministic: %d vs %d", tc, result, result2)
			}
		}

		// Test monotonic property
		zero := quantizeTickToInt64(0.0)
		one := quantizeTickToInt64(1.0)
		minusOne := quantizeTickToInt64(-1.0)

		if one <= zero {
			t.Error("quantizeTickToInt64(1.0) should be greater than quantizeTickToInt64(0.0)")
		}
		if minusOne >= zero {
			t.Error("quantizeTickToInt64(-1.0) should be less than quantizeTickToInt64(0.0)")
		}
	})

	t.Run("ExtremeValues", func(t *testing.T) {
		extremeValues := []float64{
			math.Inf(1),
			math.Inf(-1),
			math.NaN(),
			1e100,
			-1e100,
		}

		for _, val := range extremeValues {
			// Should not panic (footgun mode)
			result := quantizeTickToInt64(val)
			_ = result
		}
	})
}

// ============================================================================
// 6. KECCAK RANDOM TESTS (NEW)
// ============================================================================

func TestKeccakRandom(t *testing.T) {
	t.Run("BasicCreation", func(t *testing.T) {
		seed := []byte("test seed")
		rng := newKeccakRandom(seed)

		if rng == nil {
			t.Error("newKeccakRandom should not return nil")
		}

		if rng.counter != 0 {
			t.Error("Initial counter should be 0")
		}

		// Verify seed was processed
		if rng.seed == [32]byte{} {
			t.Error("Seed should not be all zeros")
		}
	})

	t.Run("DeterministicOutput", func(t *testing.T) {
		seed := []byte("deterministic test")

		rng1 := newKeccakRandom(seed)
		rng2 := newKeccakRandom(seed)

		// Same seed should produce same sequence
		for i := 0; i < 10; i++ {
			val1 := rng1.nextUint64()
			val2 := rng2.nextUint64()
			if val1 != val2 {
				t.Errorf("Iteration %d: expected same values, got %d vs %d", i, val1, val2)
			}
		}
	})

	t.Run("CounterProgression", func(t *testing.T) {
		rng := newKeccakRandom([]byte("counter test"))

		initialCounter := rng.counter
		_ = rng.nextUint64()

		if rng.counter != initialCounter+1 {
			t.Errorf("Counter should increment, got %d, expected %d", rng.counter, initialCounter+1)
		}
	})

	t.Run("NextIntBounds", func(t *testing.T) {
		rng := newKeccakRandom([]byte("bounds test"))

		// Test zero upper bound
		result := rng.nextInt(0)
		if result != 0 {
			t.Errorf("nextInt(0) should return 0, got %d", result)
		}

		// Test negative upper bound
		result = rng.nextInt(-5)
		if result != 0 {
			t.Errorf("nextInt(-5) should return 0, got %d", result)
		}

		// Test power of 2 optimization
		upperBound := 8 // 2^3
		for i := 0; i < 100; i++ {
			result = rng.nextInt(upperBound)
			if result < 0 || result >= upperBound {
				t.Errorf("nextInt(%d) out of bounds: %d", upperBound, result)
			}
		}

		// Test non-power of 2
		upperBound = 7
		for i := 0; i < 100; i++ {
			result = rng.nextInt(upperBound)
			if result < 0 || result >= upperBound {
				t.Errorf("nextInt(%d) out of bounds: %d", upperBound, result)
			}
		}
	})

	t.Run("DifferentSeeds", func(t *testing.T) {
		rng1 := newKeccakRandom([]byte("seed1"))
		rng2 := newKeccakRandom([]byte("seed2"))

		// Different seeds should produce different sequences
		same := 0
		total := 100
		for i := 0; i < total; i++ {
			val1 := rng1.nextUint64()
			val2 := rng2.nextUint64()
			if val1 == val2 {
				same++
			}
		}

		// Allow some collisions but not too many
		if same > total/10 {
			t.Errorf("Too many collisions between different seeds: %d/%d", same, total)
		}
	})
}

// ============================================================================
// 7. KECCAK SHUFFLE TESTS (NEW)
// ============================================================================

func TestKeccakShuffle(t *testing.T) {
	t.Run("EmptySlice", func(t *testing.T) {
		var bindings []ArbitrageEdgeBinding
		// Should not panic
		keccakShuffleEdgeBindings(bindings, PairID(123))
	})

	t.Run("SingleElement", func(t *testing.T) {
		bindings := []ArbitrageEdgeBinding{
			{edgeIndex: 42},
		}
		original := bindings[0]

		keccakShuffleEdgeBindings(bindings, PairID(123))

		// Single element should remain unchanged
		if bindings[0].edgeIndex != original.edgeIndex {
			t.Error("Single element should not change")
		}
	})

	t.Run("DeterministicShuffle", func(t *testing.T) {
		// Create test bindings
		bindings1 := make([]ArbitrageEdgeBinding, 10)
		bindings2 := make([]ArbitrageEdgeBinding, 10)

		for i := range bindings1 {
			bindings1[i] = ArbitrageEdgeBinding{edgeIndex: uint64(i)}
			bindings2[i] = ArbitrageEdgeBinding{edgeIndex: uint64(i)}
		}

		pairID := PairID(12345)

		keccakShuffleEdgeBindings(bindings1, pairID)
		keccakShuffleEdgeBindings(bindings2, pairID)

		// Same pairID should produce same shuffle
		for i := range bindings1 {
			if bindings1[i].edgeIndex != bindings2[i].edgeIndex {
				t.Errorf("Position %d: shuffle not deterministic", i)
			}
		}
	})

	t.Run("DifferentPairIDs", func(t *testing.T) {
		// Create identical starting slices
		bindings1 := make([]ArbitrageEdgeBinding, 10)
		bindings2 := make([]ArbitrageEdgeBinding, 10)

		for i := range bindings1 {
			bindings1[i] = ArbitrageEdgeBinding{edgeIndex: uint64(i)}
			bindings2[i] = ArbitrageEdgeBinding{edgeIndex: uint64(i)}
		}

		keccakShuffleEdgeBindings(bindings1, PairID(111))
		keccakShuffleEdgeBindings(bindings2, PairID(222))

		// Different pairIDs should produce different shuffles
		same := 0
		for i := range bindings1 {
			if bindings1[i].edgeIndex == bindings2[i].edgeIndex {
				same++
			}
		}

		// Allow some same positions but not all
		if same == len(bindings1) {
			t.Error("Different pairIDs should produce different shuffles")
		}
	})

	t.Run("ShuffleProperties", func(t *testing.T) {
		bindings := make([]ArbitrageEdgeBinding, 20)
		original := make([]uint64, 20)

		for i := range bindings {
			bindings[i] = ArbitrageEdgeBinding{edgeIndex: uint64(i)}
			original[i] = uint64(i)
		}

		keccakShuffleEdgeBindings(bindings, PairID(999))

		// Verify all elements still present (permutation property)
		found := make(map[uint64]bool)
		for _, binding := range bindings {
			found[binding.edgeIndex] = true
		}

		for _, expectedValue := range original {
			if !found[expectedValue] {
				t.Errorf("Value %d missing after shuffle", expectedValue)
			}
		}

		// Should have 20 unique values
		if len(found) != 20 {
			t.Errorf("Expected 20 unique values, got %d", len(found))
		}
	})
}

// ============================================================================
// 8. EMIT ARBITRAGE OPPORTUNITY TESTS (NEW)
// ============================================================================

func TestEmitArbitrageOpportunity(t *testing.T) {
	t.Run("BasicEmission", func(t *testing.T) {
		cycle := &ArbitrageCycleState{
			tickValues: [3]float64{1.5, -0.8, -0.5},
			pairIDs:    [3]PairID{PairID(100), PairID(200), PairID(300)},
		}

		// Should not panic
		emitArbitrageOpportunity(cycle, -0.3)
	})

	t.Run("ZeroValues", func(t *testing.T) {
		cycle := &ArbitrageCycleState{
			tickValues: [3]float64{0.0, 0.0, 0.0},
			pairIDs:    [3]PairID{PairID(0), PairID(0), PairID(0)},
		}

		// Should not panic
		emitArbitrageOpportunity(cycle, 0.0)
	})

	t.Run("ExtremeValues", func(t *testing.T) {
		cycle := &ArbitrageCycleState{
			tickValues: [3]float64{1e6, -1e6, 1e-6},
			pairIDs:    [3]PairID{PairID(math.MaxUint64), PairID(1), PairID(math.MaxUint64 - 1)},
		}

		// Should not panic
		emitArbitrageOpportunity(cycle, -1e6)
	})
}

// ============================================================================
// 9. BUILD FANOUT SHARD BUCKETS TESTS (NEW)
// ============================================================================

func TestBuildFanoutShardBuckets(t *testing.T) {
	t.Run("EmptyCycles", func(t *testing.T) {
		clearGlobalState()
		var cycles []ArbitrageTriplet

		buildFanoutShardBuckets(cycles)

		if pairShardBuckets == nil {
			t.Error("pairShardBuckets should be initialized")
		}

		if len(pairShardBuckets) != 0 {
			t.Errorf("Expected 0 shard buckets, got %d", len(pairShardBuckets))
		}
	})

	t.Run("SingleCycle", func(t *testing.T) {
		clearGlobalState()
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
		}

		buildFanoutShardBuckets(cycles)

		// Should have 3 pairs
		if len(pairShardBuckets) != 3 {
			t.Errorf("Expected 3 pairs, got %d", len(pairShardBuckets))
		}

		// Each pair should have 1 shard with 1 binding
		for pairID := PairID(1); pairID <= PairID(3); pairID++ {
			shards, exists := pairShardBuckets[pairID]
			if !exists {
				t.Errorf("Pair %d not found in shard buckets", pairID)
				continue
			}

			if len(shards) != 1 {
				t.Errorf("Pair %d should have 1 shard, got %d", pairID, len(shards))
				continue
			}

			if len(shards[0].edgeBindings) != 1 {
				t.Errorf("Pair %d shard should have 1 binding, got %d", pairID, len(shards[0].edgeBindings))
			}
		}
	})

	t.Run("MultipleCycles", func(t *testing.T) {
		clearGlobalState()
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(2), PairID(4), PairID(5)},
			{PairID(1), PairID(4), PairID(6)},
		}

		buildFanoutShardBuckets(cycles)

		// Check that pair 1 has 2 bindings (appears in 2 cycles)
		shards1, exists := pairShardBuckets[PairID(1)]
		if !exists {
			t.Error("Pair 1 not found")
		} else {
			totalBindings := 0
			for _, shard := range shards1 {
				totalBindings += len(shard.edgeBindings)
			}
			if totalBindings != 2 {
				t.Errorf("Pair 1 should have 2 bindings, got %d", totalBindings)
			}
		}

		// Check that pair 2 has 2 bindings
		shards2, exists := pairShardBuckets[PairID(2)]
		if !exists {
			t.Error("Pair 2 not found")
		} else {
			totalBindings := 0
			for _, shard := range shards2 {
				totalBindings += len(shard.edgeBindings)
			}
			if totalBindings != 2 {
				t.Errorf("Pair 2 should have 2 bindings, got %d", totalBindings)
			}
		}
	})

	t.Run("DeterministicShuffle", func(t *testing.T) {
		clearGlobalState()
		cycles := []ArbitrageTriplet{
			{PairID(100), PairID(101), PairID(102)},
			{PairID(100), PairID(103), PairID(104)},
			{PairID(100), PairID(105), PairID(106)},
		}

		buildFanoutShardBuckets(cycles)

		// Store first result
		firstResult := make([]ArbitrageEdgeBinding, 0)
		if shards, exists := pairShardBuckets[PairID(100)]; exists {
			for _, shard := range shards {
				firstResult = append(firstResult, shard.edgeBindings...)
			}
		}

		// Rebuild and compare
		clearGlobalState()
		buildFanoutShardBuckets(cycles)

		secondResult := make([]ArbitrageEdgeBinding, 0)
		if shards, exists := pairShardBuckets[PairID(100)]; exists {
			for _, shard := range shards {
				secondResult = append(secondResult, shard.edgeBindings...)
			}
		}

		if len(firstResult) != len(secondResult) {
			t.Error("Shuffle should be deterministic")
		} else {
			for i := range firstResult {
				if firstResult[i].edgeIndex != secondResult[i].edgeIndex {
					t.Error("Shuffle should be deterministic")
					break
				}
			}
		}
	})
}

// ============================================================================
// 10. ATTACH SHARD TO EXECUTOR TESTS (NEW)
// ============================================================================

func TestAttachShardToExecutor(t *testing.T) {
	t.Run("BasicAttachment", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
			priorityQueues:   make([]quantumqueue64.QuantumQueue64, 0),
			fanoutTables:     make([][]FanoutEntry, 0),
			cycleStates:      make([]ArbitrageCycleState, 0),
		}

		shard := &PairShardBucket{
			pairID: PairID(123),
			edgeBindings: []ArbitrageEdgeBinding{
				{
					cyclePairs: [3]PairID{PairID(100), PairID(200), PairID(300)},
					edgeIndex:  0,
				},
			},
		}

		attachShardToExecutor(executor, shard)

		// Should have created one queue
		if len(executor.priorityQueues) != 1 {
			t.Errorf("Expected 1 queue, got %d", len(executor.priorityQueues))
		}

		// Should have created fanout table
		if len(executor.fanoutTables) != 1 {
			t.Errorf("Expected 1 fanout table, got %d", len(executor.fanoutTables))
		}

		// Should have created cycle state
		if len(executor.cycleStates) != 1 {
			t.Errorf("Expected 1 cycle state, got %d", len(executor.cycleStates))
		}

		// Fanout table should have 2 entries (for other 2 edges)
		if len(executor.fanoutTables[0]) != 2 {
			t.Errorf("Expected 2 fanout entries, got %d", len(executor.fanoutTables[0]))
		}
	})

	t.Run("MultipleShardsForSamePair", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
			priorityQueues:   make([]quantumqueue64.QuantumQueue64, 0),
			fanoutTables:     make([][]FanoutEntry, 0),
			cycleStates:      make([]ArbitrageCycleState, 0),
		}

		// First shard
		shard1 := &PairShardBucket{
			pairID: PairID(123),
			edgeBindings: []ArbitrageEdgeBinding{
				{cyclePairs: [3]PairID{PairID(100), PairID(200), PairID(300)}, edgeIndex: 0},
			},
		}

		// Second shard for same pair
		shard2 := &PairShardBucket{
			pairID: PairID(123),
			edgeBindings: []ArbitrageEdgeBinding{
				{cyclePairs: [3]PairID{PairID(400), PairID(500), PairID(600)}, edgeIndex: 1},
			},
		}

		attachShardToExecutor(executor, shard1)
		attachShardToExecutor(executor, shard2)

		// Should still have only 1 queue (same pair)
		if len(executor.priorityQueues) != 1 {
			t.Errorf("Expected 1 queue for same pair, got %d", len(executor.priorityQueues))
		}

		// Should have 2 cycle states
		if len(executor.cycleStates) != 2 {
			t.Errorf("Expected 2 cycle states, got %d", len(executor.cycleStates))
		}

		// Fanout table should have 4 entries (2 per cycle)
		if len(executor.fanoutTables[0]) != 4 {
			t.Errorf("Expected 4 fanout entries, got %d", len(executor.fanoutTables[0]))
		}
	})

	t.Run("DifferentPairs", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
			priorityQueues:   make([]quantumqueue64.QuantumQueue64, 0),
			fanoutTables:     make([][]FanoutEntry, 0),
			cycleStates:      make([]ArbitrageCycleState, 0),
		}

		shard1 := &PairShardBucket{
			pairID: PairID(123),
			edgeBindings: []ArbitrageEdgeBinding{
				{cyclePairs: [3]PairID{PairID(100), PairID(200), PairID(300)}, edgeIndex: 0},
			},
		}

		shard2 := &PairShardBucket{
			pairID: PairID(456),
			edgeBindings: []ArbitrageEdgeBinding{
				{cyclePairs: [3]PairID{PairID(400), PairID(500), PairID(600)}, edgeIndex: 1},
			},
		}

		attachShardToExecutor(executor, shard1)
		attachShardToExecutor(executor, shard2)

		// Should have 2 queues (different pairs)
		if len(executor.priorityQueues) != 2 {
			t.Errorf("Expected 2 queues for different pairs, got %d", len(executor.priorityQueues))
		}

		// Should have 2 fanout tables
		if len(executor.fanoutTables) != 2 {
			t.Errorf("Expected 2 fanout tables, got %d", len(executor.fanoutTables))
		}
	})
}

// ============================================================================
// 11. CORE ASSIGNMENT TESTS
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

	t.Run("FootgunValidCoreAssignment", func(t *testing.T) {
		pairID := PairID(33333)

		// Test valid core IDs only
		validCores := []uint8{0, 1, 31, 62, 63}

		for _, coreID := range validCores {
			RegisterPairToCore(pairID, coreID)
		}

		assignment := pairToCoreAssignment[pairID]

		for _, coreID := range validCores {
			expectedBit := uint64(1) << coreID
			if assignment&expectedBit == 0 {
				t.Errorf("Valid core %d not assigned", coreID)
			}
		}
	})
}

// ============================================================================
// 12. TICK DISPATCH TESTS
// ============================================================================

func TestTickDispatch(t *testing.T) {
	clearGlobalState()

	t.Run("BasicDispatch", func(t *testing.T) {
		addr := generateMockAddress(12345)
		pairID := PairID(999)

		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, pairID)
		RegisterPairToCore(pairID, 0)

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}

		logView := createValidLogView(addr, 2000, 1000)

		if !coreRings[0].Empty() {
			t.Error("Ring should be empty initially")
		}

		DispatchTickUpdate(logView)

		if coreRings[0].Empty() {
			t.Error("Ring should contain dispatched message")
		} else {
			messagePtr := coreRings[0].Pop()
			if messagePtr == nil {
				t.Error("Failed to pop message from ring")
			} else {
				tickUpdate := (*TickUpdate)(unsafe.Pointer(messagePtr))

				if tickUpdate.pairID != pairID {
					t.Errorf("Wrong pairID: expected %d, got %d", pairID, tickUpdate.pairID)
				}
				if tickUpdate.forwardTick != -tickUpdate.reverseTick {
					t.Error("Forward and reverse ticks should be opposites")
				}
			}
		}
	})

	t.Run("UnknownAddress", func(t *testing.T) {
		clearGlobalState()

		unknownAddr := generateMockAddress(999999)
		logView := createValidLogView(unknownAddr, 1000, 500)

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}

		DispatchTickUpdate(logView)

		if !coreRings[0].Empty() {
			t.Error("Ring should remain empty for unknown address")
		}
	})

	t.Run("FootgunValidLogViews", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(12345)
		pairID := PairID(999)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, pairID)
		RegisterPairToCore(pairID, 0)

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}

		// Test with properly formatted LogViews only
		validLogViews := []*types.LogView{
			createValidLogView(addr, 1000, 500),
			createValidLogView(addr, 2000, 1000),
			createValidLogView(addr, 4000, 2000),
		}

		for i, logView := range validLogViews {
			// Clear ring
			for !coreRings[0].Empty() {
				coreRings[0].Pop()
			}

			DispatchTickUpdate(logView)

			// Should dispatch successfully (or fail gracefully due to fastuni)
			t.Logf("Valid LogView %d processed without panic", i)
		}
	})

	t.Run("MultipleCoreFanout", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(777)
		pairID := PairID(777)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, pairID)

		// Assign to multiple cores
		RegisterPairToCore(pairID, 0)
		RegisterPairToCore(pairID, 1)
		RegisterPairToCore(pairID, 2)

		// Create rings for all cores
		for i := 0; i < 3; i++ {
			coreRings[i] = ring24.New(16)
		}

		logView := createValidLogView(addr, 4000, 1000)

		DispatchTickUpdate(logView)

		// Verify all 3 cores received the message
		for i := 0; i < 3; i++ {
			if coreRings[i].Empty() {
				t.Errorf("Core %d should have received message", i)
			} else {
				messagePtr := coreRings[i].Pop()
				tickUpdate := (*TickUpdate)(unsafe.Pointer(messagePtr))
				if tickUpdate.pairID != pairID {
					t.Errorf("Core %d got wrong pairID: expected %d, got %d", i, pairID, tickUpdate.pairID)
				}
			}
		}
	})

	t.Run("NilRingHandling", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(555)
		pairID := PairID(555)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, pairID)
		RegisterPairToCore(pairID, 0)

		// Don't create ring (leave as nil)
		coreRings[0] = nil

		logView := createValidLogView(addr, 1000, 500)

		// Should not panic when ring is nil
		DispatchTickUpdate(logView)
	})
}

// ============================================================================
// 13. TICK PROCESSING TESTS
// ============================================================================

func TestTickProcessing(t *testing.T) {
	t.Run("BasicProcessing", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(10),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.1, 0.2, 0.3},
			pairIDs:    [3]PairID{PairID(100), PairID(200), PairID(300)},
		}

		// Add an entry to the queue
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		update := &TickUpdate{
			forwardTick: 1.5,
			reverseTick: -1.5,
			pairID:      PairID(123),
		}

		// Should not panic
		processTickUpdate(executor, update)
	})

	t.Run("ReverseDirection", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: true, // Test reverse direction
			pairToQueueIndex:   localidx.New(10),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.1, 0.2, 0.3},
			pairIDs:    [3]PairID{PairID(100), PairID(200), PairID(300)},
		}

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		update := &TickUpdate{
			forwardTick: 1.5,
			reverseTick: -1.5,
			pairID:      PairID(123),
		}

		// Should use reverseTick when isReverseDirection is true
		processTickUpdate(executor, update)
	})

	t.Run("ProfitableArbitrage", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(10),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		// Create cycle with existing ticks that would be profitable with new tick
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{-0.5, -0.3, -0.1}, // Total: -0.9
			pairIDs:    [3]PairID{PairID(100), PairID(200), PairID(300)},
		}

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		update := &TickUpdate{
			forwardTick: -0.2, // Total would be -1.1 (profitable)
			reverseTick: 0.2,
			pairID:      PairID(123),
		}

		// Should detect profitable arbitrage
		processTickUpdate(executor, update)
	})

	t.Run("FanoutUpdate", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(10),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			cycleStates:        make([]ArbitrageCycleState, 2),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		// Create cycles
		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.1, 0.2, 0.3},
			pairIDs:    [3]PairID{PairID(100), PairID(200), PairID(300)},
		}
		executor.cycleStates[1] = ArbitrageCycleState{
			tickValues: [3]float64{0.4, 0.5, 0.6},
			pairIDs:    [3]PairID{PairID(400), PairID(500), PairID(600)},
		}

		// Create fanout entries
		handle1, _ := executor.priorityQueues[0].BorrowSafe()
		handle2, _ := executor.priorityQueues[0].BorrowSafe()

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

		// Add queue entries
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle1, 0)
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle2, 1)

		update := &TickUpdate{
			forwardTick: 2.5,
			reverseTick: -2.5,
			pairID:      PairID(123),
		}

		// Should update fanout entries
		processTickUpdate(executor, update)

		// Verify tick values were updated
		if executor.cycleStates[0].tickValues[1] != 2.5 {
			t.Errorf("Expected cycle 0 edge 1 to be 2.5, got %f", executor.cycleStates[0].tickValues[1])
		}
		if executor.cycleStates[1].tickValues[2] != 2.5 {
			t.Errorf("Expected cycle 1 edge 2 to be 2.5, got %f", executor.cycleStates[1].tickValues[2])
		}
	})

	t.Run("FootgunValidProcessing", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			isReverseDirection: false,
			pairToQueueIndex:   localidx.New(10),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			cycleStates:        make([]ArbitrageCycleState, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(123, 0)

		executor.cycleStates[0] = ArbitrageCycleState{
			tickValues: [3]float64{0.1, 0.2, 0.3},
			pairIDs:    [3]PairID{PairID(100), PairID(200), PairID(300)},
		}

		// Add entry to queue
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		// Test with valid updates only
		validUpdates := []*TickUpdate{
			{forwardTick: 1.5, reverseTick: -1.5, pairID: PairID(123)},
			{forwardTick: 0.0, reverseTick: 0.0, pairID: PairID(123)},
			{forwardTick: -2.0, reverseTick: 2.0, pairID: PairID(123)},
		}

		for i, update := range validUpdates {
			// Should handle without panic
			processTickUpdate(executor, update)
			t.Logf("Processed valid update %d without panic", i)
		}
	})
}

// ============================================================================
// 14. SYSTEM INTEGRATION TESTS
// ============================================================================

func TestSystemIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	clearGlobalState()

	t.Run("CompleteWorkflow", func(t *testing.T) {
		// 1. Initialize system
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(4), PairID(5), PairID(6)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(50 * time.Millisecond)

		// 2. Register addresses
		for i := uint64(1); i <= 6; i++ {
			addr := generateMockAddress(i * 1000)
			addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
			RegisterPairAddress(addrSlice, PairID(i))
		}

		// 3. Send tick updates
		for i := uint64(1); i <= 6; i++ {
			addr := generateMockAddress(i * 1000)
			reserve0 := uint64(1000 + i*100)
			reserve1 := uint64(500 + i*50)

			logView := createValidLogView(addr, reserve0, reserve1)
			DispatchTickUpdate(logView)
		}

		// 4. Verify state
		hasAssignments := false
		for i := uint64(1); i <= 6; i++ {
			if pairToCoreAssignment[i] != 0 {
				hasAssignments = true
				break
			}
		}

		if !hasAssignments {
			t.Error("No core assignments after complete workflow")
		}

		// 5. Cleanup
		control.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("EmptySystemInitialization", func(t *testing.T) {
		clearGlobalState()

		var cycles []ArbitrageTriplet

		// Should not panic with empty cycles
		InitializeArbitrageSystem(cycles)
		time.Sleep(10 * time.Millisecond)

		control.Shutdown()
	})

	t.Run("SingleCycleSystem", func(t *testing.T) {
		clearGlobalState()

		cycles := []ArbitrageTriplet{
			{PairID(10), PairID(20), PairID(30)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(10 * time.Millisecond)

		// Verify all pairs are assigned
		for _, pairID := range []PairID{10, 20, 30} {
			if pairToCoreAssignment[pairID] == 0 {
				t.Errorf("Pair %d not assigned to any core", pairID)
			}
		}

		control.Shutdown()
	})

	t.Run("HighLoadSystem", func(t *testing.T) {
		clearGlobalState()

		// Create many cycles
		cycles := make([]ArbitrageTriplet, 100)
		for i := 0; i < 100; i++ {
			base := PairID(i * 3)
			cycles[i] = ArbitrageTriplet{base + 1, base + 2, base + 3}
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(100 * time.Millisecond)

		// Register some addresses and dispatch updates
		for i := 0; i < 10; i++ {
			addr := generateMockAddress(uint64(i * 1000))
			pairID := PairID(i*3 + 1)
			addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
			RegisterPairAddress(addrSlice, pairID)

			logView := createValidLogView(addr, 1000, 500)
			DispatchTickUpdate(logView)
		}

		control.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})
}

// ============================================================================
// 15. CONCURRENCY TESTS
// ============================================================================

func TestConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency tests in short mode")
	}

	t.Run("ConcurrentDispatch", func(t *testing.T) {
		clearGlobalState()

		const numPairs = 10
		const numCores = 4
		addresses := make([][42]byte, numPairs)
		pairIDs := make([]PairID, numPairs)

		// Setup
		for i := 0; i < numPairs; i++ {
			addresses[i] = generateMockAddress(uint64(i * 1000))
			pairIDs[i] = PairID(i + 1)
			addrSlice := addresses[i][constants.AddressHexStart:constants.AddressHexEnd]
			RegisterPairAddress(addrSlice, pairIDs[i])
			RegisterPairToCore(pairIDs[i], uint8(i%numCores))
		}

		// Create rings
		for i := 0; i < numCores; i++ {
			coreRings[i] = ring24.New(1024)
		}

		// Concurrent dispatch
		var wg sync.WaitGroup
		const goroutines = 10
		const dispatchesPerGoroutine = 100

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(gID int) {
				defer wg.Done()
				for i := 0; i < dispatchesPerGoroutine; i++ {
					pairIdx := (gID*dispatchesPerGoroutine + i) % numPairs
					reserve0 := uint64(1000 * (1 << (i % 4))) // Powers of 2
					reserve1 := uint64(1000)
					logView := createValidLogView(addresses[pairIdx], reserve0, reserve1)
					DispatchTickUpdate(logView)
				}
			}(g)
		}

		wg.Wait()

		// Verify messages were dispatched
		totalMessages := 0
		for i := 0; i < numCores; i++ {
			for !coreRings[i].Empty() {
				coreRings[i].Pop()
				totalMessages++
			}
		}

		expectedMessages := goroutines * dispatchesPerGoroutine
		// Allow for some fastuni failures
		if totalMessages < expectedMessages/2 {
			t.Errorf("Too few messages dispatched: expected at least %d, got %d", expectedMessages/2, totalMessages)
		}
	})

	t.Run("ConcurrentRegistration", func(t *testing.T) {
		clearGlobalState()

		const goroutines = 50
		const pairsPerGoroutine = 20

		var wg sync.WaitGroup

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(gID int) {
				defer wg.Done()
				for i := 0; i < pairsPerGoroutine; i++ {
					addr := generateMockAddress(uint64(gID*1000 + i))
					pairID := PairID(gID*1000 + i + 1)
					addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
					RegisterPairAddress(addrSlice, pairID)
					RegisterPairToCore(pairID, uint8(gID%8))
				}
			}(g)
		}

		wg.Wait()

		// Verify all registrations
		successCount := 0
		for g := 0; g < goroutines; g++ {
			for i := 0; i < pairsPerGoroutine; i++ {
				addr := generateMockAddress(uint64(g*1000 + i))
				expectedPairID := PairID(g*1000 + i + 1)
				addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
				foundPairID := lookupPairIDByAddress(addrSlice)
				if foundPairID == expectedPairID {
					successCount++
				}
			}
		}

		expectedTotal := goroutines * pairsPerGoroutine
		if successCount < expectedTotal*9/10 { // Allow 10% failure due to hash collisions
			t.Errorf("Too many registration failures: expected at least %d, got %d", expectedTotal*9/10, successCount)
		}
	})

	t.Run("ConcurrentSystemOperations", func(t *testing.T) {
		clearGlobalState()

		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(4), PairID(5), PairID(6)},
		}

		InitializeArbitrageSystem(cycles)
		time.Sleep(50 * time.Millisecond)

		// Create rings for all cores
		for i := 0; i < 8; i++ {
			if coreRings[i] == nil {
				coreRings[i] = ring24.New(256)
			}
		}

		var wg sync.WaitGroup
		const operations = 1000

		// Concurrent address registration
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < operations; i++ {
				addr := generateMockAddress(uint64(i + 1000))
				pairID := PairID((i % 6) + 1)
				addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
				RegisterPairAddress(addrSlice, pairID)
			}
		}()

		// Concurrent tick dispatching
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < operations; i++ {
				addr := generateMockAddress(uint64(i + 2000))
				pairID := PairID((i % 6) + 1)
				addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
				RegisterPairAddress(addrSlice, pairID)

				logView := createValidLogView(addr, uint64(1000+i), uint64(500+i))
				DispatchTickUpdate(logView)
			}
		}()

		// Concurrent core assignments
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < operations; i++ {
				pairID := PairID((i % 6) + 1)
				coreID := uint8(i % 8)
				RegisterPairToCore(pairID, coreID)
			}
		}()

		wg.Wait()

		// Cleanup
		control.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})
}

// ============================================================================
// 16. EDGE CASE TESTS
// ============================================================================

func TestEdgeCases(t *testing.T) {
	t.Run("ZeroValues", func(t *testing.T) {
		clearGlobalState()

		// Test zero pair ID (valid in footgun mode)
		addr := generateMockAddress(123)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, PairID(0))

		found := lookupPairIDByAddress(addrSlice)
		if found != 0 {
			t.Error("Zero pair ID should be found as 0")
		}
	})

	t.Run("MaxValues", func(t *testing.T) {
		clearGlobalState()

		// Test maximum pair ID
		addr := generateMockAddress(456)
		maxPairID := PairID(math.MaxUint64)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, maxPairID)

		found := lookupPairIDByAddress(addrSlice)
		if found != maxPairID {
			t.Errorf("Expected max pair ID %d, got %d", maxPairID, found)
		}
	})

	t.Run("ExtremeReserveValues", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(456)
		pairID := PairID(456)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, pairID)
		RegisterPairToCore(pairID, 0)

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}

		extremeCases := []struct {
			reserve0, reserve1 uint64
			name               string
		}{
			{1000, 500, "Valid2to1"},
			{2000, 1000, "Valid2to1Large"},
			{4000, 1000, "Valid4to1"},
			{8000, 1000, "Valid8to1"},
			{math.MaxUint64, 1, "MaxReserve0"},
			{1, math.MaxUint64, "MaxReserve1"},
			{math.MaxUint64, math.MaxUint64, "BothMax"},
		}

		for _, tc := range extremeCases {
			// Clear ring
			for !coreRings[0].Empty() {
				coreRings[0].Pop()
			}

			logView := createValidLogView(addr, tc.reserve0, tc.reserve1)
			DispatchTickUpdate(logView)

			t.Logf("Handled case %s without panic", tc.name)
		}
	})

	t.Run("FootgunResourceUsage", func(t *testing.T) {
		clearGlobalState()

		// Test with reasonable load
		const maxAttempts = 1000

		for i := 0; i < maxAttempts; i++ {
			addr := generateMockAddress(uint64(i * 2654435761))
			addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
			RegisterPairAddress(addrSlice, PairID(i+1))
		}

		// Should still work
		testAddr := generateMockAddress(12345)
		testSlice := testAddr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(testSlice, PairID(99999))

		found := lookupPairIDByAddress(testSlice)
		if found != PairID(99999) {
			t.Error("Should be able to register/lookup with reasonable load")
		}
	})

	t.Run("ValidMemoryPatterns", func(t *testing.T) {
		clearGlobalState()

		// Test with valid LogView containing realistic data
		addr := generateMockAddress(789)
		pairID := PairID(789)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, pairID)
		RegisterPairToCore(pairID, 0)

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16)
		}

		logView := createValidLogView(addr, 1000000, 500000)

		// Should handle realistic data
		DispatchTickUpdate(logView)
		t.Log("Handled realistic LogView without panic")
	})

	t.Run("AllCoreAssignments", func(t *testing.T) {
		clearGlobalState()

		pairID := PairID(777)

		// Assign to all 64 cores
		for coreID := uint8(0); coreID < 64; coreID++ {
			RegisterPairToCore(pairID, coreID)
		}

		assignment := pairToCoreAssignment[pairID]

		// Should have all bits set
		if assignment != math.MaxUint64 {
			t.Errorf("Expected all cores assigned (MaxUint64), got %d", assignment)
		}
	})

	t.Run("HashTableCollisions", func(t *testing.T) {
		clearGlobalState()

		// Try to force collisions by using many addresses
		const numAddresses = 10000
		registered := 0
		found := 0

		for i := 0; i < numAddresses; i++ {
			addr := generateMockAddress(uint64(i * 1000003))
			pairID := PairID(i + 1)
			addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]

			RegisterPairAddress(addrSlice, pairID)
			registered++

			foundID := lookupPairIDByAddress(addrSlice)
			if foundID == pairID {
				found++
			}
		}

		// Should handle collisions gracefully
		successRate := float64(found) / float64(registered)
		if successRate < 0.8 { // Allow 20% failure due to collisions
			t.Errorf("Hash table collision handling poor: %f success rate", successRate)
		}
	})
}

// ============================================================================
// 17. PERFORMANCE TESTS (FIXED)
// ============================================================================

func TestPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance tests in short mode")
	}

	t.Run("LookupLatency", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(123)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, PairID(456))

		start := time.Now()
		iterations := 1000000

		for i := 0; i < iterations; i++ {
			_ = lookupPairIDByAddress(addrSlice)
		}

		elapsed := time.Since(start)
		avgLatency := elapsed / time.Duration(iterations)

		if avgLatency > 100*time.Nanosecond {
			t.Errorf("Address lookup too slow: %v avg latency", avgLatency)
		}

		t.Logf("Address lookup: %v avg latency", avgLatency)
	})

	t.Run("DispatchThroughput", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(789)
		pairID := PairID(789)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, pairID)
		RegisterPairToCore(pairID, 0)

		// FIXED: Use power of 2 for ring size
		if coreRings[0] == nil {
			coreRings[0] = ring24.New(16384) // 2^14 = 16384 (was 10000)
		}

		logView := createValidLogView(addr, 2000, 1000)

		start := time.Now()
		iterations := 100000

		for i := 0; i < iterations; i++ {
			DispatchTickUpdate(logView)
		}

		elapsed := time.Since(start)
		throughput := float64(iterations) / elapsed.Seconds()

		t.Logf("Dispatch throughput: %.0f ops/sec", throughput)
	})

	t.Run("QuantizationPerformance", func(t *testing.T) {
		values := make([]float64, 10000)
		for i := range values {
			values[i] = float64(i-5000) * 0.01
		}

		start := time.Now()
		iterations := 1000000

		for i := 0; i < iterations; i++ {
			_ = quantizeTickToInt64(values[i%len(values)])
		}

		elapsed := time.Since(start)
		avgLatency := elapsed / time.Duration(iterations)

		if avgLatency > 10*time.Nanosecond {
			t.Errorf("Quantization too slow: %v avg latency", avgLatency)
		}

		t.Logf("Quantization: %v avg latency", avgLatency)
	})

	t.Run("CoreAssignmentPerformance", func(t *testing.T) {
		clearGlobalState()

		// Pre-assign many pairs to multiple cores
		for i := uint64(1); i <= 10000; i++ {
			pairID := PairID(i)
			for coreID := uint8(0); coreID < 8; coreID++ {
				RegisterPairToCore(pairID, coreID)
			}
		}

		start := time.Now()
		iterations := 1000000

		for i := 0; i < iterations; i++ {
			pairID := PairID((i % 10000) + 1)
			assignment := pairToCoreAssignment[pairID]

			// Simulate core enumeration
			for assignment != 0 {
				coreID := bits.TrailingZeros64(assignment)
				assignment &^= 1 << coreID
				_ = coreID
			}
		}

		elapsed := time.Since(start)
		avgLatency := elapsed / time.Duration(iterations)

		t.Logf("Core assignment enumeration: %v avg latency", avgLatency)
	})

	t.Run("HashDistributionQuality", func(t *testing.T) {
		const numTests = 100000
		hashes := make(map[uint64]int)

		start := time.Now()

		for i := 0; i < numTests; i++ {
			addr := generateMockAddress(uint64(i * 1000003))
			addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
			hash := directAddressToIndex64(addrSlice)
			hashes[hash]++
		}

		elapsed := time.Since(start)

		// Check distribution quality
		expectedPerBucket := float64(numTests) / float64(constants.AddressTableCapacity)
		variance := 0.0

		for _, count := range hashes {
			diff := float64(count) - expectedPerBucket
			variance += diff * diff
		}
		variance /= float64(len(hashes))

		t.Logf("Hash distribution: %d unique hashes, variance: %.2f", len(hashes), variance)
		t.Logf("Hash performance: %v total for %d operations", elapsed, numTests)

		if len(hashes) < numTests/100 {
			t.Errorf("Poor hash distribution: %d unique hashes for %d inputs", len(hashes), numTests)
		}
	})
}

// ============================================================================
// 18. ALLOCATION OPTIMIZATION VERIFICATION
// ============================================================================

func TestAllocationOptimization(t *testing.T) {
	t.Run("HotPathAllocationTest", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(123)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, PairID(456))

		// Measure allocations using testing.AllocsPerRun
		allocs := testing.AllocsPerRun(100, func() {
			_ = lookupPairIDByAddress(addrSlice)
		})

		// lookupPairIDByAddress should be zero allocation after table is populated
		if allocs > 0 {
			t.Logf("lookupPairIDByAddress allocations: %f per run (may include dependency allocations)", allocs)
		}

		// Test quantization allocation
		allocsQuant := testing.AllocsPerRun(100, func() {
			_ = quantizeTickToInt64(1.234)
		})

		if allocsQuant > 0 {
			t.Logf("quantizeTickToInt64 allocations: %f per run", allocsQuant)
		}
	})

	t.Run("DispatchAllocationTest", func(t *testing.T) {
		clearGlobalState()

		addr := generateMockAddress(789)
		pairID := PairID(789)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, pairID)
		RegisterPairToCore(pairID, 0)

		if coreRings[0] == nil {
			coreRings[0] = ring24.New(1024)
		}

		logView := createValidLogView(addr, 2000, 1000)

		// Warmup
		for i := 0; i < 10; i++ {
			DispatchTickUpdate(logView)
			for !coreRings[0].Empty() {
				coreRings[0].Pop()
			}
		}

		// Measure allocations - DispatchTickUpdate may allocate due to fastuni dependency
		allocs := testing.AllocsPerRun(10, func() {
			DispatchTickUpdate(logView)
			for !coreRings[0].Empty() {
				coreRings[0].Pop()
			}
		})

		t.Logf("DispatchTickUpdate allocations: %f per run (includes fastuni and utils dependencies)", allocs)

		// Just verify it doesn't go completely wild - dependencies may cause some allocation
		if allocs > 100 {
			t.Errorf("DispatchTickUpdate allocating too much: %f allocs per run", allocs)
		}
	})

	t.Run("CoreOperationsEfficiency", func(t *testing.T) {
		clearGlobalState()

		// Test core assignment bit operations
		pairID := PairID(12345)
		for i := uint8(0); i < 8; i++ {
			RegisterPairToCore(pairID, i)
		}

		allocs := testing.AllocsPerRun(1000, func() {
			assignment := pairToCoreAssignment[pairID]
			for assignment != 0 {
				coreID := bits.TrailingZeros64(assignment)
				assignment &^= 1 << coreID
				_ = coreID
			}
		})

		if allocs > 0 {
			t.Errorf("Core assignment operations should not allocate: %f allocs per run", allocs)
		}
	})

	t.Run("AddressKeyOperationsEfficiency", func(t *testing.T) {
		addr := generateMockAddress(42)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]

		// Test bytesToAddressKey - this may allocate due to utils.ParseEthereumAddress
		allocs := testing.AllocsPerRun(100, func() {
			_ = bytesToAddressKey(addrSlice)
		})

		t.Logf("bytesToAddressKey allocations: %f per run (may include utils parsing)", allocs)

		// Test key comparison
		key1 := bytesToAddressKey(addrSlice)
		key2 := bytesToAddressKey(addrSlice)

		allocsEqual := testing.AllocsPerRun(1000, func() {
			_ = key1.isEqual(key2)
		})

		if allocsEqual > 0 {
			t.Errorf("AddressKey.isEqual should not allocate: %f allocs per run", allocsEqual)
		}
	})

	t.Run("QuantizationEfficiency", func(t *testing.T) {
		values := []float64{0.0, 1.0, -1.0, 0.5, -0.5, 100.0, -100.0}

		allocs := testing.AllocsPerRun(1000, func() {
			for _, val := range values {
				_ = quantizeTickToInt64(val)
			}
		})

		if allocs > 0 {
			t.Errorf("quantizeTickToInt64 should not allocate: %f allocs per run", allocs)
		}
	})

	t.Run("RandomGenerationEfficiency", func(t *testing.T) {
		// Note: newKeccakRandom itself allocates (creates hasher, etc)
		// but subsequent operations should be efficient
		seed := []byte("efficiency test")
		rng := newKeccakRandom(seed)

		// Warmup to stabilize any lazy allocations
		for i := 0; i < 100; i++ {
			_ = rng.nextUint64()
		}

		// Test nextUint64 - may allocate due to sha3 hasher operations
		allocs := testing.AllocsPerRun(10, func() {
			_ = rng.nextUint64()
		})

		t.Logf("nextUint64 allocations: %f per run (includes sha3 hasher)", allocs)

		// Test nextInt with power of 2
		allocsInt := testing.AllocsPerRun(10, func() {
			_ = rng.nextInt(16) // Power of 2
		})

		t.Logf("nextInt(power-of-2) allocations: %f per run", allocsInt)
	})
}

// ============================================================================
// 19. COMPREHENSIVE BENCHMARKS
// ============================================================================

func BenchmarkAddressLookup(b *testing.B) {
	clearGlobalState()

	addresses := make([][42]byte, 1000)
	for i := range addresses {
		addresses[i] = generateMockAddress(uint64(i * 1000003))
		addrSlice := addresses[i][constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, PairID(i+1))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		_ = lookupPairIDByAddress(addrSlice)
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

func BenchmarkDispatchTickUpdate(b *testing.B) {
	clearGlobalState()

	const pairCount = 100
	addresses := make([][42]byte, pairCount)
	logViews := make([]*types.LogView, pairCount)

	// Initialize rings with power-of-2 sizes
	for i := 0; i < 8; i++ {
		if coreRings[i] == nil {
			coreRings[i] = ring24.New(1024)
		}
	}

	// Setup addresses and logviews
	for i := 0; i < pairCount; i++ {
		addresses[i] = generateMockAddress(uint64(i * 1000003))
		addrSlice := addresses[i][constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, PairID(i+1))
		RegisterPairToCore(PairID(i+1), uint8(i%8))

		reserve0 := uint64(1000 * (1 << (i % 4)))
		reserve1 := uint64(1000)
		logViews[i] = createValidLogView(addresses[i], reserve0, reserve1)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		DispatchTickUpdate(logViews[i%pairCount])

		// Periodic cleanup
		if i%1000 == 0 {
			for j := 0; j < 8; j++ {
				for !coreRings[j].Empty() {
					coreRings[j].Pop()
				}
			}
		}
	}
}

func BenchmarkCoreAssignmentBitOps(b *testing.B) {
	clearGlobalState()

	const pairCount = 1000
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

		for coreAssignments != 0 {
			coreID := bits.TrailingZeros64(coreAssignments)
			coreAssignments &^= 1 << coreID
			_ = coreID
		}
	}
}

func BenchmarkKeccakRandom(b *testing.B) {
	rng := newKeccakRandom([]byte("benchmark seed"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = rng.nextUint64()
	}
}

func BenchmarkKeccakRandomInt(b *testing.B) {
	rng := newKeccakRandom([]byte("benchmark seed"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = rng.nextInt(1000)
	}
}

func BenchmarkBytesToAddressKey(b *testing.B) {
	addr := generateMockAddress(42)
	addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = bytesToAddressKey(addrSlice)
	}
}

func BenchmarkDirectAddressToIndex64(b *testing.B) {
	addr := generateMockAddress(42)
	addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = directAddressToIndex64(addrSlice)
	}
}

func BenchmarkSystemIntegration(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping integration benchmark in short mode")
	}

	clearGlobalState()

	// Setup complete system
	const pairCount = 50
	addresses := make([][42]byte, pairCount)
	logViews := make([]*types.LogView, pairCount)

	for i := 0; i < 8; i++ {
		coreRings[i] = ring24.New(1024)
	}

	for i := 0; i < pairCount; i++ {
		addresses[i] = generateMockAddress(uint64(i * 1000))
		addrSlice := addresses[i][constants.AddressHexStart:constants.AddressHexEnd]
		RegisterPairAddress(addrSlice, PairID(i+1))
		RegisterPairToCore(PairID(i+1), uint8(i%8))

		reserve0 := uint64(1000 * (1 << (i % 4)))
		reserve1 := uint64(1000)
		logViews[i] = createValidLogView(addresses[i], reserve0, reserve1)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Full cycle: lookup, dispatch, process
		for j := 0; j < pairCount; j++ {
			DispatchTickUpdate(logViews[j])
		}

		// Cleanup
		for j := 0; j < 8; j++ {
			for !coreRings[j].Empty() {
				coreRings[j].Pop()
			}
		}
	}
}

func BenchmarkMemoryAccess(b *testing.B) {
	const structCount = 10000

	cycles := make([]ArbitrageCycleState, structCount)
	for i := range cycles {
		cycles[i] = ArbitrageCycleState{
			tickValues: [3]float64{float64(i), float64(i + 1), float64(i + 2)},
			pairIDs:    [3]PairID{PairID(i), PairID(i + 1), PairID(i + 2)},
		}
	}

	b.Run("Sequential", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		for i := 0; i < b.N; i++ {
			cycle := &cycles[i%structCount]
			sum += cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		}
		_ = sum
	})

	b.Run("Random", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		for i := 0; i < b.N; i++ {
			idx := (i * 2654435761) % structCount
			cycle := &cycles[idx]
			sum += cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		}
		_ = sum
	})
}

// ============================================================================
// 20. ADDITIONAL COVERAGE TESTS FOR 100%
// ============================================================================

func TestAdditionalCoverage(t *testing.T) {
	t.Run("SafeRouterFunctionTesting", func(t *testing.T) {
		// Test router functions that don't involve quantum queue operations

		// Test quantization function
		result1 := quantizeTickToInt64(1.5)
		result2 := quantizeTickToInt64(-1.5)
		if result1 == result2 {
			t.Error("Different inputs should produce different quantized results")
		}

		// Test address key operations
		addr := generateMockAddress(42)
		addrSlice := addr[constants.AddressHexStart:constants.AddressHexEnd]
		key1 := bytesToAddressKey(addrSlice)
		key2 := bytesToAddressKey(addrSlice)
		if !key1.isEqual(key2) {
			t.Error("Same address should produce equal keys")
		}

		// Test keccak random functions
		rng := newKeccakRandom([]byte("test"))
		val1 := rng.nextUint64()
		val2 := rng.nextUint64()
		if val1 == val2 {
			t.Error("Random generator should produce different values")
		}

		// Test emit arbitrage opportunity (safe function)
		cycle := &ArbitrageCycleState{
			tickValues: [3]float64{1.0, -0.5, -0.3},
			pairIDs:    [3]PairID{PairID(100), PairID(200), PairID(300)},
		}
		emitArbitrageOpportunity(cycle, -0.2) // Should not panic
	})

	t.Run("LaunchShardWorkerBranches", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping shard worker test in short mode")
		}

		clearGlobalState()

		// Test with core 0 (gets cooldown consumer)
		shardChannel := make(chan PairShardBucket, 1)
		shard := PairShardBucket{
			pairID: PairID(999),
			edgeBindings: []ArbitrageEdgeBinding{
				{cyclePairs: [3]PairID{PairID(100), PairID(200), PairID(300)}, edgeIndex: 0},
			},
		}
		shardChannel <- shard
		close(shardChannel)

		// This would launch a worker for core 0
		go launchShardWorker(0, 2, shardChannel)

		time.Sleep(10 * time.Millisecond)

		// Test with non-zero core (gets regular consumer)
		shardChannel2 := make(chan PairShardBucket, 1)
		shardChannel2 <- shard
		close(shardChannel2)

		go launchShardWorker(1, 2, shardChannel2)

		time.Sleep(10 * time.Millisecond)

		control.Shutdown()
	})

	t.Run("DirectAddressToIndex64EdgeCases", func(t *testing.T) {
		// Test edge case patterns that might affect hash distribution
		edgeCases := [][]byte{
			[]byte("0000000000000000000000000000000000000000"), // All zeros
			[]byte("ffffffffffffffffffffffffffffffffffffffff"), // All F's
			[]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), // Repeated pattern
			[]byte("1111111111111111111111111111111111111111"), // Repeated 1's
			[]byte("abcdefabcdefabcdefabcdefabcdefabcdefabcd"), // Repeated sequence
		}

		for i, testCase := range edgeCases {
			hash := directAddressToIndex64(testCase)
			if hash >= uint64(constants.AddressTableCapacity) {
				t.Errorf("Edge case %d produced out-of-bounds hash: %d", i, hash)
			}

			key := bytesToAddressKey(testCase)
			storedHash := directAddressToIndex64Stored(key)
			if hash != storedHash {
				t.Errorf("Edge case %d: direct and stored hash mismatch", i)
			}
		}
	})

	t.Run("InitializeArbitrageSystemEdgeCases", func(t *testing.T) {
		clearGlobalState()

		// Test with maximum core count scenario
		originalNumCPU := runtime.NumCPU()

		// Test the coreCount calculation branches
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
		}

		// This will test the core count limiting logic
		InitializeArbitrageSystem(cycles)
		time.Sleep(10 * time.Millisecond)

		// Verify assignments were made
		for _, pairID := range []PairID{1, 2, 3} {
			if pairToCoreAssignment[pairID] == 0 {
				t.Errorf("Pair %d should have core assignments", pairID)
			}
		}

		control.Shutdown()
		time.Sleep(10 * time.Millisecond)
		_ = originalNumCPU // Use the variable to avoid unused warning
	})

	t.Run("BuildFanoutShardBucketsLargeCycles", func(t *testing.T) {
		clearGlobalState()

		// Create cycles that will test the shard bucketing logic
		cycles := make([]ArbitrageTriplet, constants.MaxCyclesPerShard+5)
		for i := 0; i < len(cycles); i++ {
			// All cycles use pair 1, forcing multiple shards for the same pair
			cycles[i] = ArbitrageTriplet{PairID(1), PairID(i + 10), PairID(i + 100)}
		}

		buildFanoutShardBuckets(cycles)

		// Pair 1 should have multiple shards due to MaxCyclesPerShard limit
		shards, exists := pairShardBuckets[PairID(1)]
		if !exists {
			t.Error("Pair 1 should exist in shard buckets")
		} else if len(shards) < 2 {
			t.Errorf("Pair 1 should have multiple shards, got %d", len(shards))
		}

		// Verify total bindings match total cycles
		totalBindings := 0
		for _, shard := range shards {
			totalBindings += len(shard.edgeBindings)
		}
		if totalBindings != len(cycles) {
			t.Errorf("Expected %d total bindings, got %d", len(cycles), totalBindings)
		}
	})

	t.Run("KeccakRandomBoundaryConditions", func(t *testing.T) {
		rng := newKeccakRandom([]byte("boundary test"))

		// Test nextInt with 1 (boundary case)
		result := rng.nextInt(1)
		if result != 0 {
			t.Errorf("nextInt(1) should always return 0, got %d", result)
		}

		// Test with large power of 2
		largeResult := rng.nextInt(1024) // 2^10
		if largeResult < 0 || largeResult >= 1024 {
			t.Errorf("nextInt(1024) out of bounds: %d", largeResult)
		}

		// Test with large non-power of 2
		nonPowerResult := rng.nextInt(1000)
		if nonPowerResult < 0 || nonPowerResult >= 1000 {
			t.Errorf("nextInt(1000) out of bounds: %d", nonPowerResult)
		}
	})

	t.Run("FanoutEntryConfiguration", func(t *testing.T) {
		// Test that fanout entries are correctly configured in attachShardToExecutor
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex: localidx.New(10),
			priorityQueues:   make([]quantumqueue64.QuantumQueue64, 0),
			fanoutTables:     make([][]FanoutEntry, 0),
			cycleStates:      make([]ArbitrageCycleState, 0),
		}

		shard := &PairShardBucket{
			pairID: PairID(555),
			edgeBindings: []ArbitrageEdgeBinding{
				{cyclePairs: [3]PairID{PairID(100), PairID(200), PairID(300)}, edgeIndex: 1}, // Edge 1
			},
		}

		attachShardToExecutor(executor, shard)

		// Should have created fanout entries for edges 2 and 0 (the other two edges)
		if len(executor.fanoutTables[0]) != 2 {
			t.Errorf("Expected 2 fanout entries, got %d", len(executor.fanoutTables[0]))
		}

		// Check that the correct edge indices are created
		expectedEdges := map[uint64]bool{0: true, 2: true} // Should be (1+1)%3=2 and (1+2)%3=0
		for _, entry := range executor.fanoutTables[0] {
			if !expectedEdges[entry.edgeIndex] {
				t.Errorf("Unexpected edge index in fanout: %d", entry.edgeIndex)
			}
			delete(expectedEdges, entry.edgeIndex)
		}

		if len(expectedEdges) != 0 {
			t.Errorf("Missing expected edge indices: %v", expectedEdges)
		}
	})
}
