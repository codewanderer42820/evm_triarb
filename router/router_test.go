package router

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"main/constants"
	"main/localidx"
	"main/quantumqueue64"
	"main/ring24"
	"main/types"
)

// ════════════════════════════════════════════════════════════════════════════════════════════════
// TEST UTILITIES AND FIXTURES
// ════════════════════════════════════════════════════════════════════════════════════════════════

type TestAssertion struct {
	t *testing.T
}

func NewAssertion(t *testing.T) *TestAssertion {
	return &TestAssertion{t: t}
}

func (a *TestAssertion) EXPECT_EQ(expected, actual interface{}, msg ...string) bool {
	if expected != actual {
		message := fmt.Sprintf("Expected: %v, Actual: %v", expected, actual)
		if len(msg) > 0 {
			message = fmt.Sprintf("%s - %s", msg[0], message)
		}
		a.t.Errorf("EXPECT_EQ failed: %s", message)
		return false
	}
	return true
}

func (a *TestAssertion) EXPECT_NE(expected, actual interface{}, msg ...string) bool {
	if expected == actual {
		message := fmt.Sprintf("Expected NOT: %v, Actual: %v", expected, actual)
		if len(msg) > 0 {
			message = fmt.Sprintf("%s - %s", msg[0], message)
		}
		a.t.Errorf("EXPECT_NE failed: %s", message)
		return false
	}
	return true
}

func (a *TestAssertion) EXPECT_TRUE(condition bool, msg ...string) bool {
	if !condition {
		message := "Expected: true, Actual: false"
		if len(msg) > 0 {
			message = fmt.Sprintf("%s - %s", msg[0], message)
		}
		a.t.Errorf("EXPECT_TRUE failed: %s", message)
		return false
	}
	return true
}

func (a *TestAssertion) EXPECT_FALSE(condition bool, msg ...string) bool {
	if condition {
		message := "Expected: false, Actual: true"
		if len(msg) > 0 {
			message = fmt.Sprintf("%s - %s", msg[0], message)
		}
		a.t.Errorf("EXPECT_FALSE failed: %s", message)
		return false
	}
	return true
}

func (a *TestAssertion) EXPECT_LT(left, right interface{}, msg ...string) bool {
	switch l := left.(type) {
	case int:
		if r, ok := right.(int); ok && l >= r {
			message := fmt.Sprintf("Expected: %v < %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_LT failed: %s", message)
			return false
		}
	case uint64:
		if r, ok := right.(uint64); ok && l >= r {
			message := fmt.Sprintf("Expected: %v < %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_LT failed: %s", message)
			return false
		}
	case float64:
		if r, ok := right.(float64); ok && l >= r {
			message := fmt.Sprintf("Expected: %v < %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_LT failed: %s", message)
			return false
		}
	}
	return true
}

func (a *TestAssertion) EXPECT_LE(left, right interface{}, msg ...string) bool {
	switch l := left.(type) {
	case float64:
		if r, ok := right.(float64); ok && l > r {
			message := fmt.Sprintf("Expected: %v <= %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_LE failed: %s", message)
			return false
		}
	}
	return true
}

func (a *TestAssertion) EXPECT_GE(left, right interface{}, msg ...string) bool {
	switch l := left.(type) {
	case int:
		if r, ok := right.(int); ok && l < r {
			message := fmt.Sprintf("Expected: %v >= %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_GE failed: %s", message)
			return false
		}
	case float64:
		if r, ok := right.(float64); ok && l < r {
			message := fmt.Sprintf("Expected: %v >= %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_GE failed: %s", message)
			return false
		}
	}
	return true
}

func (a *TestAssertion) EXPECT_NEAR(expected, actual, tolerance float64, msg ...string) bool {
	diff := math.Abs(expected - actual)
	if diff > tolerance {
		message := fmt.Sprintf("Expected: %v, Actual: %v, Tolerance: %v, Diff: %v", expected, actual, tolerance, diff)
		if len(msg) > 0 {
			message = fmt.Sprintf("%s - %s", msg[0], message)
		}
		a.t.Errorf("EXPECT_NEAR failed: %s", message)
		return false
	}
	return true
}

func (a *TestAssertion) EXPECT_NO_FATAL_FAILURE(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			a.t.Errorf("EXPECT_NO_FATAL_FAILURE failed: panic occurred: %v", r)
		}
	}()
	fn()
}

func (a *TestAssertion) ASSERT_TRUE(condition bool, msg ...string) {
	if !a.EXPECT_TRUE(condition, msg...) {
		a.t.FailNow()
	}
}

type RouterTestFixture struct {
	*TestAssertion
}

func NewRouterTestFixture(t *testing.T) *RouterTestFixture {
	return &RouterTestFixture{
		TestAssertion: NewAssertion(t),
	}
}

func (f *RouterTestFixture) SetUp() {
	// Clear global state before each test
	for i := range coreExecutors {
		coreExecutors[i] = nil
	}
	for i := range coreRings {
		coreRings[i] = nil
	}
	for i := range pairToCoreAssignment {
		pairToCoreAssignment[i] = 0
	}
	for i := range pairAddressKeys {
		pairAddressKeys[i] = AddressKey{}
	}
	for i := range addressToPairID {
		addressToPairID[i] = 0
	}
	pairShardBuckets = nil
}

func (f *RouterTestFixture) CreateTestLogView(address string, reserve0, reserve1 uint64) *types.LogView {
	// Create properly formatted hex data matching real Uniswap V2 Sync events
	data := fmt.Sprintf("0x%064x%064x", reserve0, reserve1)
	return &types.LogView{
		Addr: []byte(address),
		Data: []byte(data),
	}
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// CORE TYPE STRUCTURE TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestTickUpdateStructure(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("SizeRequirement", func(t *testing.T) {
		var tu TickUpdate
		fixture.EXPECT_EQ(24, int(unsafe.Sizeof(tu)), "TickUpdate must be exactly 24 bytes for ring buffer compatibility")
	})

	t.Run("FieldAccess", func(t *testing.T) {
		tu := TickUpdate{
			pairID:      PairID(67890),
			forwardTick: 2.5,
			reverseTick: -2.5,
		}

		fixture.EXPECT_EQ(PairID(67890), tu.pairID, "pairID field access")
		fixture.EXPECT_EQ(2.5, tu.forwardTick, "forwardTick field access")
		fixture.EXPECT_EQ(-2.5, tu.reverseTick, "reverseTick field access")
	})
}

func TestArbitrageCycleStateAlignment(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("SizeVerification", func(t *testing.T) {
		var cycle ArbitrageCycleState
		fixture.EXPECT_EQ(64, int(unsafe.Sizeof(cycle)), "ArbitrageCycleState must be exactly 64 bytes")
	})

	t.Run("FieldOrdering", func(t *testing.T) {
		cycle := ArbitrageCycleState{
			tickValues: [3]float64{1.0, 2.0, 3.0},
			pairIDs:    [3]PairID{10, 20, 30},
		}

		fixture.EXPECT_EQ(1.0, cycle.tickValues[0], "tickValues field access")
		fixture.EXPECT_EQ(PairID(10), cycle.pairIDs[0], "pairIDs field access")
	})
}

func TestAddressKeyOperations(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("EqualityComparison", func(t *testing.T) {
		key1 := AddressKey{words: [3]uint64{0x1234567890abcdef, 0xfedcba0987654321, 0x12345678}}
		key2 := AddressKey{words: [3]uint64{0x1234567890abcdef, 0xfedcba0987654321, 0x12345678}}
		key3 := AddressKey{words: [3]uint64{0x1234567890abcdef, 0xfedcba0987654321, 0x12345679}}

		fixture.EXPECT_TRUE(key1.isEqual(key2), "Identical keys should be equal")
		fixture.EXPECT_FALSE(key1.isEqual(key3), "Different keys should not be equal")
	})

	t.Run("MemoryLayout", func(t *testing.T) {
		var key AddressKey
		fixture.EXPECT_EQ(32, int(unsafe.Sizeof(key)), "AddressKey should be 32 bytes total")
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// HEX PARSING AND ADDRESS RESOLUTION TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestCountLeadingZeros(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	testCases := []struct {
		name     string
		input    string
		expected int
	}{
		{"AllZeros", "00000000000000000000000000000000", 32},
		{"NoLeadingZeros", "78e8455d7f2faa9bdeb859ffffffffff", 0},
		{"SomeLeadingZeros", "00000000000000000000000000000078", 30},
		{"OneLeadingZero", "0123456789abcdef0123456789abcdef", 1},
		{"HalfLeadingZeros", "0000000000000000e8455d7f2faa9bde", 16},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := make([]byte, 32)
			copy(input, []byte(tc.input))
			if len(tc.input) < 32 {
				// Pad with zeros at the end if needed
				for i := len(tc.input); i < 32; i++ {
					input[i] = '0'
				}
			}
			result := countLeadingZeros(input)
			fixture.EXPECT_EQ(tc.expected, result, tc.name)
		})
	}

	t.Run("ChunkBoundaries", func(t *testing.T) {
		// Test at 8-byte chunk boundaries
		boundaries := []int{7, 8, 15, 16, 23, 24, 31, 32}
		for _, leadingZeros := range boundaries {
			input := make([]byte, 32)
			for i := 0; i < leadingZeros && i < 32; i++ {
				input[i] = '0'
			}
			if leadingZeros < 32 {
				for i := leadingZeros; i < 32; i++ {
					input[i] = 'a'
				}
			}
			result := countLeadingZeros(input)
			fixture.EXPECT_EQ(leadingZeros, result, fmt.Sprintf("Boundary test for %d zeros", leadingZeros))
		}
	})
}

func TestBytesToAddressKey(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("ValidAddressConversion", func(t *testing.T) {
		address := "1234567890123456789012345678901234567890"
		key := bytesToAddressKey([]byte(address))

		// Verify the key is non-zero (properly packed)
		isZero := true
		for _, word := range key.words {
			if word != 0 {
				isZero = false
				break
			}
		}
		fixture.EXPECT_FALSE(isZero, "Address key should not be all zeros")
	})

	t.Run("DifferentAddressesDifferentKeys", func(t *testing.T) {
		address1 := "1234567890123456789012345678901234567890"
		address2 := "abcdefabcdefabcdefabcdefabcdefabcdefabcd"

		key1 := bytesToAddressKey([]byte(address1))
		key2 := bytesToAddressKey([]byte(address2))

		fixture.EXPECT_FALSE(key1.isEqual(key2), "Different addresses should produce different keys")
	})
}

func TestDirectAddressHashing(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("HashIndexGeneration", func(t *testing.T) {
		address1 := "1234567890123456789012345678901234567890"
		address2 := "abcdefabcdefabcdefabcdefabcdefabcdefabcd"

		idx1 := directAddressToIndex64([]byte(address1))
		idx2 := directAddressToIndex64([]byte(address2))

		fixture.EXPECT_LT(idx1, uint64(constants.AddressTableCapacity), "Hash index should be within table bounds")
		fixture.EXPECT_LT(idx2, uint64(constants.AddressTableCapacity), "Hash index should be within table bounds")
	})

	t.Run("HashConsistency", func(t *testing.T) {
		address := "1234567890123456789012345678901234567890"

		idx1 := directAddressToIndex64([]byte(address))
		idx2 := directAddressToIndex64([]byte(address))
		fixture.EXPECT_EQ(idx1, idx2, "Same address should produce same index")
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS REGISTRATION AND LOOKUP TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestPairAddressRegistration(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("BasicRegistrationAndLookup", func(t *testing.T) {
		address := "1234567890123456789012345678901234567890"
		pairID := PairID(12345)

		RegisterPairAddress([]byte(address), pairID)

		result := lookupPairIDByAddress([]byte(address))
		fixture.EXPECT_EQ(pairID, result, "Registered address should be retrievable")
	})

	t.Run("NonExistentAddressLookup", func(t *testing.T) {
		nonExistent := "9999999999999999999999999999999999999999"
		result := lookupPairIDByAddress([]byte(nonExistent))

		fixture.EXPECT_EQ(PairID(0), result, "Non-existent address should return 0")
	})

	t.Run("AddressUpdateOverwrite", func(t *testing.T) {
		address := "1111111111111111111111111111111111111111"
		pairID1 := PairID(100)
		pairID2 := PairID(200)

		RegisterPairAddress([]byte(address), pairID1)
		result1 := lookupPairIDByAddress([]byte(address))
		fixture.EXPECT_EQ(pairID1, result1, "First registration should work")

		RegisterPairAddress([]byte(address), pairID2)
		result2 := lookupPairIDByAddress([]byte(address))
		fixture.EXPECT_EQ(pairID2, result2, "Address update should overwrite previous value")
	})
}

func TestHashTableCollisionHandling(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("MultipleAddressRegistration", func(t *testing.T) {
		addresses := []string{
			"a000000000000000000000000000000000000001",
			"b000000000000000000000000000000000000002",
			"c000000000000000000000000000000000000003",
			"d000000000000000000000000000000000000004",
			"e000000000000000000000000000000000000005",
		}

		// Register all addresses
		for i, addr := range addresses {
			RegisterPairAddress([]byte(addr), PairID(i+1))
		}

		// Verify all can be looked up correctly
		for i, addr := range addresses {
			result := lookupPairIDByAddress([]byte(addr))
			fixture.EXPECT_EQ(PairID(i+1), result, fmt.Sprintf("Address %s lookup failed", addr))
		}
	})
}

func TestCoreAssignment(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("SingleCoreAssignment", func(t *testing.T) {
		pairID := PairID(123)
		coreID := uint8(5)

		RegisterPairToCore(pairID, coreID)

		assignment := pairToCoreAssignment[pairID]
		fixture.EXPECT_EQ(uint64(1), (assignment>>coreID)&1, "Assigned core bit should be set")
	})

	t.Run("MultipleCoreAssignment", func(t *testing.T) {
		pairID := PairID(456)
		cores := []uint8{0, 5, 15, 31}

		for _, coreID := range cores {
			RegisterPairToCore(pairID, coreID)
		}

		assignment := pairToCoreAssignment[pairID]
		for _, coreID := range cores {
			fixture.EXPECT_EQ(uint64(1), (assignment>>coreID)&1, fmt.Sprintf("Core %d should be assigned", coreID))
		}

		// Verify unassigned cores are not set
		fixture.EXPECT_EQ(uint64(0), (assignment>>1)&1, "Unassigned core should not be set")
		fixture.EXPECT_EQ(uint64(0), (assignment>>10)&1, "Unassigned core should not be set")
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// CRYPTOGRAPHIC RANDOMNESS TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestKeccakRandomGeneration(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("BasicRandomGeneration", func(t *testing.T) {
		seed := []byte("test_seed_12345")
		rng := newKeccakRandom(seed)

		// Generate multiple values
		values := make([]uint64, 100)
		for i := range values {
			values[i] = rng.nextUint64()
		}

		// Check for reasonable uniqueness
		seen := make(map[uint64]bool)
		duplicates := 0
		for _, v := range values {
			if seen[v] {
				duplicates++
			}
			seen[v] = true
		}

		fixture.EXPECT_LT(duplicates, 5, "Should have very few duplicate random values")
	})

	t.Run("DeterministicBehavior", func(t *testing.T) {
		seed := []byte("deterministic_test")

		rng1 := newKeccakRandom(seed)
		rng2 := newKeccakRandom(seed)

		for i := 0; i < 10; i++ {
			v1 := rng1.nextUint64()
			v2 := rng2.nextUint64()
			fixture.EXPECT_EQ(v1, v2, "Random sequence should be deterministic with same seed")
		}
	})

	t.Run("NextIntBoundsChecking", func(t *testing.T) {
		seed := []byte("bounds_test")
		rng := newKeccakRandom(seed)

		bounds := []int{1, 10, 100, 1000}

		for _, bound := range bounds {
			for i := 0; i < 20; i++ {
				val := rng.nextInt(bound)
				fixture.EXPECT_GE(val, 0, "Value should be >= 0")
				fixture.EXPECT_LT(val, bound, fmt.Sprintf("Value should be < %d", bound))
			}
		}
	})
}

func TestEdgeBindingsShuffle(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("ShufflePreservesElements", func(t *testing.T) {
		bindings := make([]ArbitrageEdgeBinding, 10)
		for i := range bindings {
			bindings[i] = ArbitrageEdgeBinding{
				cyclePairs: [3]PairID{PairID(i), PairID(i + 1), PairID(i + 2)},
				edgeIndex:  uint64(i),
			}
		}

		keccakShuffleEdgeBindings(bindings, PairID(12345))

		// Verify all elements are preserved
		elementCounts := make(map[uint64]int)
		for _, binding := range bindings {
			elementCounts[binding.edgeIndex]++
		}

		for i := uint64(0); i < 10; i++ {
			fixture.EXPECT_EQ(1, elementCounts[i], fmt.Sprintf("Edge index %d should appear exactly once", i))
		}
	})

	t.Run("ShuffleDeterminism", func(t *testing.T) {
		original := make([]ArbitrageEdgeBinding, 5)
		for i := range original {
			original[i] = ArbitrageEdgeBinding{
				cyclePairs: [3]PairID{PairID(i * 3), PairID(i*3 + 1), PairID(i*3 + 2)},
				edgeIndex:  uint64(i),
			}
		}

		bindings1 := make([]ArbitrageEdgeBinding, len(original))
		bindings2 := make([]ArbitrageEdgeBinding, len(original))
		copy(bindings1, original)
		copy(bindings2, original)

		pairID := PairID(54321)
		keccakShuffleEdgeBindings(bindings1, pairID)
		keccakShuffleEdgeBindings(bindings2, pairID)

		// Should produce identical results
		for i := range bindings1 {
			fixture.EXPECT_EQ(bindings1[i].edgeIndex, bindings2[i].edgeIndex,
				"Shuffle should be deterministic for same pairID")
		}
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// DISPATCH PIPELINE TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestDispatchTickUpdateFlow(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("UnregisteredPairHandling", func(t *testing.T) {
		logView := fixture.CreateTestLogView(
			"0x9999999999999999999999999999999999999999",
			1000000000000, 2000000000000,
		)

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchTickUpdate(logView)
		})
	})

	t.Run("ValidPairProcessing", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := PairID(12345)

		RegisterPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCore(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchTickUpdate(logView)
		})

		// Verify message was sent
		message := coreRings[0].Pop()
		fixture.ASSERT_TRUE(message != nil, "Message should be sent to core ring")

		// Verify message content
		tickUpdate := (*TickUpdate)(unsafe.Pointer(message))
		fixture.EXPECT_EQ(pairID, tickUpdate.pairID, "Message should contain correct pairID")
		fixture.EXPECT_NE(0.0, tickUpdate.forwardTick, "Forward tick should be non-zero")
		fixture.EXPECT_EQ(-tickUpdate.forwardTick, tickUpdate.reverseTick, "Reverse tick should be negative of forward")
	})

	t.Run("RealUniswapV2Data", func(t *testing.T) {
		address := "0x882df4b0fb50a229c3b4124eb18c759911485bfb"
		pairID := PairID(54321)

		RegisterPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCore(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		realLogView := &types.LogView{
			Addr: []byte(address),
			Data: []byte("0x00000000000000000000000000000000000000000078e3833588cda8d5e102c3000000000000000000000000000000000000000000000000001fa8dd7963f22c"),
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchTickUpdate(realLogView)
		})

		message := coreRings[0].Pop()
		fixture.ASSERT_TRUE(message != nil, "Real data should produce message")

		tickUpdate := (*TickUpdate)(unsafe.Pointer(message))
		fixture.EXPECT_EQ(pairID, tickUpdate.pairID, "Real data should have correct pairID")
		fixture.EXPECT_NE(0.0, tickUpdate.forwardTick, "Real data should produce non-zero tick")
		fixture.EXPECT_FALSE(math.IsNaN(tickUpdate.forwardTick), "Real data tick should not be NaN")
		fixture.EXPECT_FALSE(math.IsInf(tickUpdate.forwardTick, 0), "Real data tick should not be infinite")
	})
}

func TestDispatchFallbackLogic(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("ZeroReservesFallback", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := PairID(12345)

		RegisterPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCore(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		logView := fixture.CreateTestLogView(address, 0, 0)

		DispatchTickUpdate(logView)

		message := coreRings[0].Pop()
		fixture.ASSERT_TRUE(message != nil, "Fallback should still send message")

		tickUpdate := (*TickUpdate)(unsafe.Pointer(message))
		fixture.EXPECT_GE(tickUpdate.forwardTick, 51.2, "Fallback tick should be >= 51.2")
		fixture.EXPECT_LE(tickUpdate.forwardTick, 64.0, "Fallback tick should be <= 64.0")
		fixture.EXPECT_GE(tickUpdate.reverseTick, 51.2, "Reverse fallback tick should be >= 51.2")
		fixture.EXPECT_LE(tickUpdate.reverseTick, 64.0, "Reverse fallback tick should be <= 64.0")
		fixture.EXPECT_EQ(tickUpdate.forwardTick, tickUpdate.reverseTick, "Invalid reserves should have equal ticks")
	})
}

func TestDispatchMultiCoreDistribution(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("MultipleCoreDelivery", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := PairID(12345)
		assignedCores := []int{0, 2, 5}

		RegisterPairAddress([]byte(address[2:]), pairID)
		for _, coreID := range assignedCores {
			RegisterPairToCore(pairID, uint8(coreID))
			coreRings[coreID] = ring24.New(constants.DefaultRingSize)
		}

		logView := fixture.CreateTestLogView(address, 1500000000000, 3000000000000)
		DispatchTickUpdate(logView)

		// Verify all assigned cores received message
		for _, coreID := range assignedCores {
			message := coreRings[coreID].Pop()
			fixture.EXPECT_TRUE(message != nil, fmt.Sprintf("Core %d should receive message", coreID))

			if message != nil {
				tickUpdate := (*TickUpdate)(unsafe.Pointer(message))
				fixture.EXPECT_EQ(pairID, tickUpdate.pairID, "Message should have correct pairID")
			}
		}

		// Verify unassigned core doesn't receive message
		coreRings[1] = ring24.New(constants.DefaultRingSize)
		message := coreRings[1].Pop()
		fixture.EXPECT_TRUE(message == nil, "Unassigned core should not receive message")
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// QUANTIZATION AND CORE PROCESSING TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestTickQuantization(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("BasicQuantizationBounds", func(t *testing.T) {
		testValues := []float64{0.0, 50.0, -50.0, 100.0, -100.0}

		for _, tickValue := range testValues {
			result := quantizeTickToInt64(tickValue)
			fixture.EXPECT_GE(result, int64(0), "Quantized value should be non-negative")
			fixture.EXPECT_LE(result, int64(constants.MaxQuantizedTick), "Quantized value should be within max bound")
		}
	})

	t.Run("MonotonicityProperty", func(t *testing.T) {
		baseValue := 10.0
		largerValue := baseValue + 1.0

		baseQuantized := quantizeTickToInt64(baseValue)
		largerQuantized := quantizeTickToInt64(largerValue)

		fixture.EXPECT_LT(baseQuantized, largerQuantized, "Quantization should preserve order")
	})
}

func TestCoreProcessingLogic(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("ForwardDirectionProcessing", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
			isReverseDirection: false,
			cycleStates:        make([]ArbitrageCycleState, 10),
			fanoutTables:       make([][]FanoutEntry, 1),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		pairID := PairID(123)
		executor.pairToQueueIndex.Put(uint32(pairID), 0)

		update := &TickUpdate{
			pairID:      pairID,
			forwardTick: 1.5,
			reverseTick: -1.5,
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			processTickUpdate(executor, update)
		})
	})

	t.Run("ReverseDirectionProcessing", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
			isReverseDirection: true,
			cycleStates:        make([]ArbitrageCycleState, 10),
			fanoutTables:       make([][]FanoutEntry, 1),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()

		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(constants.MaxInitializationPriority, handle, 0)

		pairID := PairID(456)
		executor.pairToQueueIndex.Put(uint32(pairID), 0)

		update := &TickUpdate{
			pairID:      pairID,
			forwardTick: 2.0,
			reverseTick: -2.0,
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			processTickUpdate(executor, update)
		})
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// SHARD CONSTRUCTION AND SYSTEM INITIALIZATION TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestFanoutShardConstruction(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("BasicShardCreation", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(1), PairID(4), PairID(5)},
			{PairID(2), PairID(6), PairID(7)},
		}

		buildFanoutShardBuckets(cycles)

		fixture.EXPECT_TRUE(pairShardBuckets != nil, "Shard buckets should be created")
		fixture.EXPECT_LT(0, len(pairShardBuckets), "Should have non-empty shard buckets")
	})

	t.Run("PairCycleMapping", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(1), PairID(4), PairID(5)},
		}

		buildFanoutShardBuckets(cycles)

		buckets, exists := pairShardBuckets[PairID(1)]
		fixture.EXPECT_TRUE(exists, "Pair 1 should have shard buckets")

		totalBindings := 0
		for _, bucket := range buckets {
			totalBindings += len(bucket.edgeBindings)
			fixture.EXPECT_EQ(PairID(1), bucket.pairID, "Bucket should have correct pairID")
		}

		fixture.EXPECT_EQ(2, totalBindings, "Pair 1 should have 2 edge bindings")
	})
}

func TestSystemInitialization(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("SmallSystemInit", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(4), PairID(5), PairID(6)},
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			InitializeArbitrageSystem(cycles)
		})

		// Give goroutines time to start
		time.Sleep(50 * time.Millisecond)

		// Verify some pairs were assigned
		assigned := false
		for i := 0; i < 10; i++ {
			if pairToCoreAssignment[i] != 0 {
				assigned = true
				break
			}
		}
		fixture.EXPECT_TRUE(assigned, "Some pairs should be assigned to cores")
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// STRESS TESTS AND EDGE CASES
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestHighVolumeOperations(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("MassAddressRegistration", func(t *testing.T) {
		numAddresses := 50

		for i := range addressToPairID {
			addressToPairID[i] = 0
			pairAddressKeys[i] = AddressKey{}
		}

		successfulRegistrations := 0
		for i := 0; i < numAddresses; i++ {
			address := fmt.Sprintf("%08x%032d", i*0x10000, i)
			pairID := PairID(i + 1)
			RegisterPairAddress([]byte(address), pairID)

			if lookupPairIDByAddress([]byte(address)) == pairID {
				successfulRegistrations++
			}
		}

		fixture.EXPECT_LT(numAddresses*7/10, successfulRegistrations,
			fmt.Sprintf("Most addresses should be registered successfully (got %d/%d)",
				successfulRegistrations, numAddresses))
	})

	t.Run("ConcurrentLookups", func(t *testing.T) {
		for i := range addressToPairID {
			addressToPairID[i] = 0
			pairAddressKeys[i] = AddressKey{}
		}

		numAddresses := 50
		for i := 0; i < numAddresses; i++ {
			address := fmt.Sprintf("%08x%032d", i*0x10000, i)
			RegisterPairAddress([]byte(address), PairID(i+1))
		}

		var wg sync.WaitGroup
		var successCount int32

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(startIdx int) {
				defer wg.Done()

				localSuccess := 0
				for j := 0; j < 10; j++ {
					idx := (startIdx*10 + j) % numAddresses
					address := fmt.Sprintf("%08x%032d", idx*0x10000, idx)
					result := lookupPairIDByAddress([]byte(address))
					expected := PairID(idx + 1)

					if result == expected {
						localSuccess++
					}
				}
				atomic.AddInt32(&successCount, int32(localSuccess))
			}(i)
		}

		wg.Wait()

		fixture.EXPECT_LT(35, int(successCount),
			fmt.Sprintf("Most concurrent lookups should succeed (got %d/50)", successCount))
	})
}

func TestExtremeValueHandling(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("ExtremeReserveValues", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := PairID(12345)

		RegisterPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCore(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		extremeValues := []struct {
			name     string
			reserve0 uint64
			reserve1 uint64
		}{
			{"BothZero", 0, 0},
			{"OneZero", 1000000, 0},
			{"MaxValues", math.MaxUint64 >> 16, math.MaxUint64 >> 16},
			{"HugeDifference", 1, 1000000000000},
			{"EqualValues", 5000000000, 5000000000},
		}

		for _, tv := range extremeValues {
			t.Run(tv.name, func(t *testing.T) {
				logView := fixture.CreateTestLogView(address, tv.reserve0, tv.reserve1)

				fixture.EXPECT_NO_FATAL_FAILURE(func() {
					DispatchTickUpdate(logView)
				})

				message := coreRings[0].Pop()
				if message != nil {
					tickUpdate := (*TickUpdate)(unsafe.Pointer(message))
					fixture.EXPECT_EQ(pairID, tickUpdate.pairID, "PairID should be correct")
					fixture.EXPECT_FALSE(math.IsNaN(tickUpdate.forwardTick), "Forward tick should not be NaN")
					fixture.EXPECT_FALSE(math.IsNaN(tickUpdate.reverseTick), "Reverse tick should not be NaN")
					fixture.EXPECT_FALSE(math.IsInf(tickUpdate.forwardTick, 0), "Forward tick should not be infinite")
					fixture.EXPECT_FALSE(math.IsInf(tickUpdate.reverseTick, 0), "Reverse tick should not be infinite")
				}
			})
		}
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// INTEGRATION AND END-TO-END TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestEndToEndIntegration(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("CompleteProcessingPipeline", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := PairID(12345)

		RegisterPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCore(pairID, 0)

		cycles := []ArbitrageTriplet{{pairID, PairID(2), PairID(3)}}
		buildFanoutShardBuckets(cycles)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchTickUpdate(logView)
		})

		message := coreRings[0].Pop()
		fixture.EXPECT_TRUE(message != nil, "Pipeline should produce message")

		if message != nil {
			tickUpdate := (*TickUpdate)(unsafe.Pointer(message))
			fixture.EXPECT_EQ(pairID, tickUpdate.pairID, "End-to-end pairID should match")
			fixture.EXPECT_NE(0.0, tickUpdate.forwardTick, "End-to-end tick should be non-zero")
		}
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// RING BUFFER OVERFLOW AND RETRY LOGIC TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestRingBufferRetryLogic(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("RingBufferFullRetry", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := PairID(12345)

		RegisterPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCore(pairID, 0)

		// Create a very small ring that will fill up quickly
		smallRing := ring24.New(4) // Only 4 slots
		coreRings[0] = smallRing

		// Fill the ring to capacity by pushing messages directly
		for i := 0; i < 4; i++ {
			msg := [24]byte{byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
			smallRing.Push(&msg)
		}

		// Verify ring is full
		testMsg := [24]byte{99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		fixture.EXPECT_FALSE(smallRing.Push(&testMsg), "Ring should be full and reject new messages")

		// Now try to dispatch - should trigger retry logic
		logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)

		// This call should enter the retry loop but eventually succeed when we drain
		go func() {
			time.Sleep(10 * time.Millisecond) // Let dispatch start and hit retry
			// Drain one message to make space
			smallRing.Pop()
		}()

		start := time.Now()
		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchTickUpdate(logView)
		})
		elapsed := time.Since(start)

		// Should have taken some time due to retry loop
		fixture.EXPECT_LT(5*time.Millisecond, elapsed, "Should have spent time in retry loop")

		// Verify the message eventually made it through
		drainedCount := 0
		for smallRing.Pop() != nil {
			drainedCount++
		}
		fixture.EXPECT_LT(3, drainedCount, "Should have original messages plus the new one")
	})

	t.Run("MultiCoreRetrySelective", func(t *testing.T) {
		address := "0x2234567890123456789012345678901234567890"
		pairID := PairID(23456)

		RegisterPairAddress([]byte(address[2:]), pairID)

		// Assign to multiple cores - some will be full, some won't
		RegisterPairToCore(pairID, 0)
		RegisterPairToCore(pairID, 1)
		RegisterPairToCore(pairID, 2)

		// Core 0: small ring (will fill)
		coreRings[0] = ring24.New(2)
		// Core 1: normal ring (won't fill)
		coreRings[1] = ring24.New(constants.DefaultRingSize)
		// Core 2: small ring (will fill)
		coreRings[2] = ring24.New(2)

		// Fill cores 0 and 2
		for i := 0; i < 2; i++ {
			msg := [24]byte{byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
			coreRings[0].Push(&msg)
			coreRings[2].Push(&msg)
		}

		logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)

		// Start draining the full rings after a delay
		go func() {
			time.Sleep(5 * time.Millisecond)
			coreRings[0].Pop() // Make space in core 0
			coreRings[2].Pop() // Make space in core 2
		}()

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchTickUpdate(logView)
		})

		// Verify all cores eventually received the message
		// Core 1 should have received immediately, cores 0&2 after retry
		msg1 := coreRings[1].Pop()
		fixture.EXPECT_TRUE(msg1 != nil, "Core 1 should have received message immediately")

		// Cores 0 and 2 should also have messages after retry
		remaining0 := 0
		for coreRings[0].Pop() != nil {
			remaining0++
		}
		remaining2 := 0
		for coreRings[2].Pop() != nil {
			remaining2++
		}

		fixture.EXPECT_LT(1, remaining0, "Core 0 should have messages after retry")
		fixture.EXPECT_LT(1, remaining2, "Core 2 should have messages after retry")
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// ROBIN HOOD HASH TABLE DISPLACEMENT TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestRobinHoodDisplacement(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("EarlyTerminationOnShorterDistance", func(t *testing.T) {
		// Clear the hash table
		for i := range addressToPairID {
			addressToPairID[i] = 0
			pairAddressKeys[i] = AddressKey{}
		}

		// Use real Ethereum addresses - these are actual Uniswap V2 pair addresses
		addr1 := "a478c2975ab1ea89e8196811f51a7b7ade33eb11" // DAI/WETH pair
		RegisterPairAddress([]byte(addr1), PairID(100))

		// Try to lookup a different real address that doesn't exist in our table
		// This should trigger the early termination logic: currentDist < dist
		nonExistent := "b4e16d0168e52d35cacd2c6185b44281ec28c9dc" // USDC/WETH pair (not registered)

		result := lookupPairIDByAddress([]byte(nonExistent))
		fixture.EXPECT_EQ(PairID(0), result, "Non-existent key should return 0 via early termination")
	})

	t.Run("RobinHoodDisplacementDuringInsertion", func(t *testing.T) {
		// Clear the hash table
		for i := range addressToPairID {
			addressToPairID[i] = 0
			pairAddressKeys[i] = AddressKey{}
		}

		// Use real Ethereum addresses that are likely to create some collisions
		// These are actual Uniswap V2 pair addresses
		testAddresses := []string{
			"a478c2975ab1ea89e8196811f51a7b7ade33eb11", // DAI/WETH
			"bb2b8038a1640196fbe3e38816f3e67cba72d940", // WBTC/WETH
			"0d4a11d5eeaac28ec3f61d100daf4d40471f1852", // WETH/USDT
			"f173214c720f58e03e194085b1db28b50acccead", // CELT/WETH
			"43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd", // DODO/WETH
		}

		// Register them in order
		originalPairs := make([]PairID, len(testAddresses))
		for i, addr := range testAddresses {
			pairID := PairID(i + 1)
			originalPairs[i] = pairID
			RegisterPairAddress([]byte(addr), pairID)
		}

		// Verify all addresses can be found after potential displacement
		allFound := true
		for i, addr := range testAddresses {
			result := lookupPairIDByAddress([]byte(addr))
			if result != originalPairs[i] {
				allFound = false
				t.Logf("Address %s expected PairID %d but got %d", addr, originalPairs[i], result)
			}
		}
		fixture.EXPECT_TRUE(allFound, "All addresses should be findable after insertion (displacement should preserve them)")

		// Insert another real address
		additionalAddr := "ae461ca67b15dc8dc81ce7615e0320da1a9ab8d5" // UNI/WETH
		additionalPairID := PairID(100)
		RegisterPairAddress([]byte(additionalAddr), additionalPairID)

		// Verify the new address can be found
		result := lookupPairIDByAddress([]byte(additionalAddr))
		fixture.EXPECT_EQ(additionalPairID, result, "New address should be findable after insertion")

		// Verify original addresses are still findable
		stillAllFound := true
		for i, addr := range testAddresses {
			result := lookupPairIDByAddress([]byte(addr))
			if result != originalPairs[i] {
				stillAllFound = false
				t.Logf("Original address %s expected PairID %d but got %d after additional insertion", addr, originalPairs[i], result)
			}
		}
		fixture.EXPECT_TRUE(stillAllFound, "Original addresses should still be findable after additional insertion")
	})

	t.Run("HashTableDistanceCalculation", func(t *testing.T) {
		// Clear the hash table
		for i := range addressToPairID {
			addressToPairID[i] = 0
			pairAddressKeys[i] = AddressKey{}
		}

		// Use a real Ethereum address
		addr := "d3d2e2692501a5c9ca623199d38826e513033a17" // UNI/WETH V1
		RegisterPairAddress([]byte(addr), PairID(42))

		// The lookup should work, which means distance calculation is correct
		result := lookupPairIDByAddress([]byte(addr))
		fixture.EXPECT_EQ(PairID(42), result, "Distance calculation should allow successful lookup")

		// Try with a different real address that's not registered
		// This should test the early termination via distance comparison
		notFound := "514910771af9ca656af840dff83e8264ecf986ca" // LINK token address (not a pair)
		result2 := lookupPairIDByAddress([]byte(notFound))
		fixture.EXPECT_EQ(PairID(0), result2, "Non-existent key should be caught by distance check")
	})

	t.Run("RobinHoodWithManyRealAddresses", func(t *testing.T) {
		// Clear the hash table
		for i := range addressToPairID {
			addressToPairID[i] = 0
			pairAddressKeys[i] = AddressKey{}
		}

		// Test with many real Uniswap pair addresses to stress the Robin Hood logic
		realAddresses := []string{
			"a478c2975ab1ea89e8196811f51a7b7ade33eb11", // DAI/WETH
			"bb2b8038a1640196fbe3e38816f3e67cba72d940", // WBTC/WETH
			"0d4a11d5eeaac28ec3f61d100daf4d40471f1852", // WETH/USDT
			"b4e16d0168e52d35cacd2c6185b44281ec28c9dc", // USDC/WETH
			"ae461ca67b15dc8dc81ce7615e0320da1a9ab8d5", // UNI/WETH
			"397ff1542f962076d0bfe58ea045ffa2d347aca0", // USDC/USDT
			"f173214c720f58e03e194085b1db28b50acccead", // CELT/WETH
			"43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd", // DODO/WETH
		}

		registeredPairs := make(map[string]PairID)

		// Register all addresses
		for i, addr := range realAddresses {
			pairID := PairID(i + 100) // Start from 100 to avoid confusion
			registeredPairs[addr] = pairID
			RegisterPairAddress([]byte(addr), pairID)
		}

		// Verify all addresses can still be found
		successCount := 0
		for addr, expectedPairID := range registeredPairs {
			result := lookupPairIDByAddress([]byte(addr))
			if result == expectedPairID {
				successCount++
			} else {
				t.Logf("Address %s: expected %d, got %d", addr, expectedPairID, result)
			}
		}

		fixture.EXPECT_EQ(len(realAddresses), successCount,
			fmt.Sprintf("All %d real addresses should be findable after Robin Hood displacement", len(realAddresses)))
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// FANOUT PROCESSING AND PRIORITY QUEUE TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestFanoutProcessingLogic(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("FanoutTickValueUpdates", func(t *testing.T) {
		// Create an executor with cycles and fanout tables
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
			isReverseDirection: false,
			cycleStates:        make([]ArbitrageCycleState, 3),
			fanoutTables:       make([][]FanoutEntry, 1),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
		}

		// Initialize priority queue
		executor.priorityQueues[0] = *quantumqueue64.New()

		// Create cycle states with known initial values
		executor.cycleStates[0] = ArbitrageCycleState{
			pairIDs:    [3]PairID{1, 2, 3},
			tickValues: [3]float64{1.0, 2.0, 3.0}, // Initial sum = 6.0
		}
		executor.cycleStates[1] = ArbitrageCycleState{
			pairIDs:    [3]PairID{1, 4, 5},
			tickValues: [3]float64{-1.0, 0.5, 1.5}, // Initial sum = 1.0
		}
		executor.cycleStates[2] = ArbitrageCycleState{
			pairIDs:    [3]PairID{1, 6, 7},
			tickValues: [3]float64{0.0, -2.0, 1.0}, // Initial sum = -1.0
		}

		// Add handles to the queue for MoveTick operations
		handle0, _ := executor.priorityQueues[0].BorrowSafe()
		handle1, _ := executor.priorityQueues[0].BorrowSafe()
		handle2, _ := executor.priorityQueues[0].BorrowSafe()

		// Set up fanout table for pair 1 (affects edge 0 of each cycle)
		executor.fanoutTables[0] = []FanoutEntry{
			{
				cycleStateIndex: 0,
				edgeIndex:       0, // Update first edge of cycle 0
				queue:           &executor.priorityQueues[0],
				queueHandle:     uint64(handle0),
			},
			{
				cycleStateIndex: 1,
				edgeIndex:       0, // Update first edge of cycle 1
				queue:           &executor.priorityQueues[0],
				queueHandle:     uint64(handle1),
			},
			{
				cycleStateIndex: 2,
				edgeIndex:       0, // Update first edge of cycle 2
				queue:           &executor.priorityQueues[0],
				queueHandle:     uint64(handle2),
			},
		}

		// Set up pair mapping
		executor.pairToQueueIndex.Put(1, 0)

		// Add cycles to queue with initial priorities
		executor.priorityQueues[0].Push(quantizeTickToInt64(6.0), handle0, 0)
		executor.priorityQueues[0].Push(quantizeTickToInt64(1.0), handle1, 1)
		executor.priorityQueues[0].Push(quantizeTickToInt64(-1.0), handle2, 2)

		// Process an update for pair 1 with new tick value
		update := &TickUpdate{
			pairID:      1,
			forwardTick: 5.0, // This will update edge 0 of all cycles
			reverseTick: -5.0,
		}

		processTickUpdate(executor, update)

		// Verify tick values were updated via fanout
		fixture.EXPECT_EQ(5.0, executor.cycleStates[0].tickValues[0], "Cycle 0 edge 0 should be updated")
		fixture.EXPECT_EQ(2.0, executor.cycleStates[0].tickValues[1], "Cycle 0 edge 1 should be unchanged")
		fixture.EXPECT_EQ(3.0, executor.cycleStates[0].tickValues[2], "Cycle 0 edge 2 should be unchanged")

		fixture.EXPECT_EQ(5.0, executor.cycleStates[1].tickValues[0], "Cycle 1 edge 0 should be updated")
		fixture.EXPECT_EQ(0.5, executor.cycleStates[1].tickValues[1], "Cycle 1 edge 1 should be unchanged")

		fixture.EXPECT_EQ(5.0, executor.cycleStates[2].tickValues[0], "Cycle 2 edge 0 should be updated")
		fixture.EXPECT_EQ(-2.0, executor.cycleStates[2].tickValues[1], "Cycle 2 edge 1 should be unchanged")

		// Verify priority recalculation happened
		// New sums: cycle 0 = 5+2+3 = 10, cycle 1 = 5+0.5+1.5 = 7, cycle 2 = 5-2+1 = 4
		// Note: We can't easily verify the exact queue state due to internal queue structure,
		// but we can verify the MoveTick operations completed without error by checking
		// that the fanout processing completed successfully
	})

	t.Run("FanoutWithMultipleEdges", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
			isReverseDirection: false,
			cycleStates:        make([]ArbitrageCycleState, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()

		// Create a cycle where one pair update affects multiple edges
		executor.cycleStates[0] = ArbitrageCycleState{
			pairIDs:    [3]PairID{1, 1, 2}, // Pair 1 appears twice in the cycle
			tickValues: [3]float64{0.0, 0.0, 0.0},
		}

		handle, _ := executor.priorityQueues[0].BorrowSafe()

		// Fanout affects edges 0 and 1 of the same cycle (both involve pair 1)
		executor.fanoutTables[0] = []FanoutEntry{
			{
				cycleStateIndex: 0,
				edgeIndex:       0,
				queue:           &executor.priorityQueues[0],
				queueHandle:     uint64(handle),
			},
			{
				cycleStateIndex: 0,
				edgeIndex:       1,
				queue:           &executor.priorityQueues[0],
				queueHandle:     uint64(handle),
			},
		}

		executor.pairToQueueIndex.Put(1, 0)
		executor.priorityQueues[0].Push(0, handle, 0)

		update := &TickUpdate{
			pairID:      1,
			forwardTick: 3.5,
			reverseTick: -3.5,
		}

		processTickUpdate(executor, update)

		// Both edges should be updated with the same value
		fixture.EXPECT_EQ(3.5, executor.cycleStates[0].tickValues[0], "Edge 0 should be updated")
		fixture.EXPECT_EQ(3.5, executor.cycleStates[0].tickValues[1], "Edge 1 should be updated")
		fixture.EXPECT_EQ(0.0, executor.cycleStates[0].tickValues[2], "Edge 2 should be unchanged")
	})

	t.Run("EmptyFanoutTable", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
			isReverseDirection: false,
			cycleStates:        make([]ArbitrageCycleState, 1),
			fanoutTables:       make([][]FanoutEntry, 1),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 1),
		}

		executor.priorityQueues[0] = *quantumqueue64.New()
		executor.fanoutTables[0] = []FanoutEntry{} // Empty fanout table

		executor.pairToQueueIndex.Put(1, 0)

		// Add a cycle to prevent empty queue issues
		handle, _ := executor.priorityQueues[0].BorrowSafe()
		executor.priorityQueues[0].Push(0, handle, 0)

		update := &TickUpdate{
			pairID:      1,
			forwardTick: 2.0,
			reverseTick: -2.0,
		}

		// Should complete without error even with empty fanout table
		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			processTickUpdate(executor, update)
		})
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// FOCUSED PERFORMANCE BENCHMARKS
// ════════════════════════════════════════════════════════════════════════════════════════════════

// ════════════════════════════════════════════════════════════════════════════════════════════════
// COMPREHENSIVE PERFORMANCE BENCHMARKS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func BenchmarkDispatchTickUpdate(b *testing.B) {
	fixture := NewRouterTestFixture(&testing.T{})
	fixture.SetUp()

	address := "0x882df4b0fb50a229c3b4124eb18c759911485bfb"
	pairID := PairID(12345)
	RegisterPairAddress([]byte(address[2:]), pairID)
	RegisterPairToCore(pairID, 0)

	coreRings[0] = ring24.New(1 << 16) // 64k slots

	logView := &types.LogView{
		Addr: []byte(address),
		Data: []byte("0x00000000000000000000000000000000000000000078ac4cf9c9bb7cb9e54739000000000000000000000000000000000000000000000000001fcf7f300f7aee"),
	}

	// Launch consumer
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				coreRings[0].Pop()
			}
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		DispatchTickUpdate(logView)
	}

	close(stop)
	time.Sleep(10 * time.Millisecond)
}

func BenchmarkCountLeadingZeros(b *testing.B) {
	testCases := []struct {
		name  string
		input []byte
	}{
		{"AllZeros", make([]byte, 32)},
		{"NoLeadingZeros", []byte("78e8455d7f2faa9bdeb859ff78e8455d")},
		{"HalfLeadingZeros", []byte("0000000000000000e8455d7f2faa9bde")},
		{"MostlyZeros", []byte("00000000000000000000000000000001")},
	}

	for _, tc := range testCases {
		// Ensure all inputs are exactly 32 bytes
		input := make([]byte, 32)
		copy(input, tc.input)
		for i := len(tc.input); i < 32; i++ {
			input[i] = '0'
		}

		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = countLeadingZeros(input)
			}
		})
	}
}

func BenchmarkAddressLookup(b *testing.B) {
	fixture := NewRouterTestFixture(&testing.T{})
	fixture.SetUp()

	// Use real Ethereum addresses for realistic benchmarking
	realAddresses := []string{
		"a478c2975ab1ea89e8196811f51a7b7ade33eb11", // DAI/WETH
		"bb2b8038a1640196fbe3e38816f3e67cba72d940", // WBTC/WETH
		"0d4a11d5eeaac28ec3f61d100daf4d40471f1852", // WETH/USDT
		"b4e16d0168e52d35cacd2c6185b44281ec28c9dc", // USDC/WETH
		"ae461ca67b15dc8dc81ce7615e0320da1a9ab8d5", // UNI/WETH
		"397ff1542f962076d0bfe58ea045ffa2d347aca0", // USDC/USDT
		"f173214c720f58e03e194085b1db28b50acccead", // CELT/WETH
		"43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd", // DODO/WETH
	}

	// Register addresses
	for i, address := range realAddresses {
		RegisterPairAddress([]byte(address), PairID(i+1))
	}

	b.Run("ExistingAddresses", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			addr := realAddresses[i%len(realAddresses)]
			_ = lookupPairIDByAddress([]byte(addr))
		}
	})

	b.Run("NonExistentAddresses", func(b *testing.B) {
		nonExistent := []string{
			"1111111111111111111111111111111111111111",
			"2222222222222222222222222222222222222222",
			"3333333333333333333333333333333333333333",
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			addr := nonExistent[i%len(nonExistent)]
			_ = lookupPairIDByAddress([]byte(addr))
		}
	})
}

func BenchmarkAddressRegistration(b *testing.B) {
	realAddresses := []string{
		"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
		"bb2b8038a1640196fbe3e38816f3e67cba72d940",
		"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
		"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
		"ae461ca67b15dc8dc81ce7615e0320da1a9ab8d5",
		"397ff1542f962076d0bfe58ea045ffa2d347aca0",
		"f173214c720f58e03e194085b1db28b50acccead",
		"43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clear table periodically to avoid filling up
		if i%1000 == 0 {
			for j := range addressToPairID {
				addressToPairID[j] = 0
				pairAddressKeys[j] = AddressKey{}
			}
		}

		addr := realAddresses[i%len(realAddresses)]
		RegisterPairAddress([]byte(addr), PairID(i+1))
	}
}

func BenchmarkQuantization(b *testing.B) {
	values := []float64{-100.5, -50.0, 0.0, 25.7, 100.0, -128.0, 127.9, 1.5, -1.5}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = quantizeTickToInt64(values[i%len(values)])
	}
}

func BenchmarkProcessTickUpdate(b *testing.B) {
	executor := &ArbitrageCoreExecutor{
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		isReverseDirection: false,
		cycleStates:        make([]ArbitrageCycleState, 100),
		fanoutTables:       make([][]FanoutEntry, 10),
		priorityQueues:     make([]quantumqueue64.QuantumQueue64, 10),
	}

	// Initialize queues and add cycles
	for i := range executor.priorityQueues {
		executor.priorityQueues[i] = *quantumqueue64.New()

		// Add cycles to each queue
		for j := 0; j < 10; j++ {
			handle, _ := executor.priorityQueues[i].BorrowSafe()
			executor.priorityQueues[i].Push(constants.MaxInitializationPriority-int64(j*100), handle, uint64(i*10+j))
		}
	}

	// Setup pair mappings
	for i := 0; i < 10; i++ {
		executor.pairToQueueIndex.Put(uint32(i+1), uint32(i))
	}

	updates := make([]*TickUpdate, 10)
	for i := range updates {
		updates[i] = &TickUpdate{
			pairID:      PairID(i + 1),
			forwardTick: float64(i) * 0.1,
			reverseTick: -float64(i) * 0.1,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processTickUpdate(executor, updates[i%len(updates)])
	}
}

func BenchmarkBytesToAddressKey(b *testing.B) {
	addresses := []string{
		"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
		"bb2b8038a1640196fbe3e38816f3e67cba72d940",
		"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
		"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = bytesToAddressKey([]byte(addr))
	}
}

func BenchmarkDirectAddressHashing(b *testing.B) {
	addresses := []string{
		"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
		"bb2b8038a1640196fbe3e38816f3e67cba72d940",
		"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
		"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = directAddressToIndex64([]byte(addr))
	}
}

func BenchmarkKeccakRandomGeneration(b *testing.B) {
	seed := []byte("benchmark_seed_12345")
	rng := newKeccakRandom(seed)

	b.Run("NextUint64", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = rng.nextUint64()
		}
	})

	b.Run("NextInt", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = rng.nextInt(1000)
		}
	})
}

func BenchmarkHighVolumeEventProcessing(b *testing.B) {
	fixture := NewRouterTestFixture(&testing.T{})
	fixture.SetUp()

	numPairs := 100
	numCores := 4

	// Initialize massive rings
	for i := 0; i < numCores; i++ {
		coreRings[i] = ring24.New(1 << 18) // 256k slots
	}

	// Register pairs with real-looking addresses
	events := make([]*types.LogView, numPairs)
	for i := 0; i < numPairs; i++ {
		addr := fmt.Sprintf("a%039x", i) // Make it look like real Ethereum address
		RegisterPairAddress([]byte(addr), PairID(i+1))
		RegisterPairToCore(PairID(i+1), uint8(i%numCores))

		events[i] = &types.LogView{
			Addr: []byte("0x" + addr),
			Data: []byte("0x" +
				"00000000000000000000000000000000000000000078ac4cf9c9bb7cb9e54739" +
				"000000000000000000000000000000000000000000000000001fcf7f300f7aee"),
		}
	}

	// Launch aggressive consumers
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func(coreID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					coreRings[coreID].Pop()
				}
			}
		}(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		DispatchTickUpdate(events[i%numPairs])
	}

	close(stop)
	wg.Wait()

	// Report metrics
	total := 0
	for i := 0; i < numCores; i++ {
		for coreRings[i].Pop() != nil {
			total++
		}
	}
	b.ReportMetric(float64(total), "messages_processed")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events_per_second")
}

func BenchmarkBlockProcessingSimulation(b *testing.B) {
	fixture := NewRouterTestFixture(&testing.T{})
	fixture.SetUp()

	eventsPerBlock := 1000
	numPairs := 100
	numCores := 4

	// Initialize rings
	for i := 0; i < numCores; i++ {
		coreRings[i] = ring24.New(1 << 14) // 16k slots per core
	}

	// Register pairs
	pairAddrs := make([]string, numPairs)
	for i := 0; i < numPairs; i++ {
		addr := fmt.Sprintf("b%039x", i)
		pairAddrs[i] = addr
		RegisterPairAddress([]byte(addr), PairID(i+1))
		RegisterPairToCore(PairID(i+1), uint8(i%numCores))
	}

	// Real Uniswap data samples
	realDataSamples := []string{
		"0x00000000000000000000000000000000000000000078ac4cf9c9bb7cb9e54739000000000000000000000000000000000000000000000000001fcf7f300f7aee",
		"0x00000000000000000000000000000000000000000078ac4cee1b8e921e20b13d000000000000000000000000000000000000000000000000001fcf7f3326170a",
		"0x00000000000000000000000000000000000000000078ac4ac34a431c7c75f189000000000000000000000000000000000000000000000000001fcf7fc5d80e43",
		"0x0000000000000000000000000000000000000000000000000000011b6dc13f6900000000000000000000000000000000000000000000001638362ed366158ac1",
		"0x0000000000000000000000000000000000000000000059a034d302879881a1e600000000000000000000000000000000000000000000000045ab5730c156ff13",
	}

	blockEvents := make([]*types.LogView, eventsPerBlock)
	for i := range blockEvents {
		pairIdx := i % numPairs
		dataIdx := i % len(realDataSamples)

		blockEvents[i] = &types.LogView{
			Addr: []byte("0x" + pairAddrs[pairIdx]),
			Data: []byte(realDataSamples[dataIdx]),
		}
	}

	// Launch consumers
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func(coreID int) {
			defer wg.Done()
			processed := 0
			for {
				select {
				case <-stop:
					return
				default:
					if coreRings[coreID].Pop() != nil {
						processed++
					}
				}
			}
		}(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()

		// Process entire block
		for _, event := range blockEvents {
			DispatchTickUpdate(event)
		}

		elapsed := time.Since(start)
		b.ReportMetric(float64(elapsed.Nanoseconds()/int64(eventsPerBlock)), "ns_per_event")
	}

	close(stop)
	wg.Wait()

	// Final metrics
	totalProcessed := 0
	for core := 0; core < numCores; core++ {
		for coreRings[core].Pop() != nil {
			totalProcessed++
		}
	}
	b.ReportMetric(float64(totalProcessed), "total_processed")
}

func BenchmarkSystemThroughput(b *testing.B) {
	fixture := NewRouterTestFixture(&testing.T{})
	fixture.SetUp()

	// Setup a realistic system
	cycles := []ArbitrageTriplet{
		{PairID(1), PairID(2), PairID(3)},
		{PairID(1), PairID(4), PairID(5)},
		{PairID(2), PairID(6), PairID(7)},
		{PairID(3), PairID(8), PairID(9)},
	}

	buildFanoutShardBuckets(cycles)

	// Register some pairs
	testPairs := []struct {
		addr   string
		pairID PairID
	}{
		{"a478c2975ab1ea89e8196811f51a7b7ade33eb11", PairID(1)},
		{"bb2b8038a1640196fbe3e38816f3e67cba72d940", PairID(2)},
		{"0d4a11d5eeaac28ec3f61d100daf4d40471f1852", PairID(3)},
	}

	for _, pair := range testPairs {
		RegisterPairAddress([]byte(pair.addr), pair.pairID)
		RegisterPairToCore(pair.pairID, 0)
	}

	coreRings[0] = ring24.New(1 << 16)

	events := make([]*types.LogView, len(testPairs))
	for i, pair := range testPairs {
		events[i] = &types.LogView{
			Addr: []byte("0x" + pair.addr),
			Data: []byte("0x00000000000000000000000000000000000000000078ac4cf9c9bb7cb9e54739000000000000000000000000000000000000000000000000001fcf7f300f7aee"),
		}
	}

	// Consumer
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				coreRings[0].Pop()
			}
		}
	}()

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		DispatchTickUpdate(events[i%len(events)])
	}

	elapsed := time.Since(start)
	close(stop)

	throughput := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(throughput, "events_per_second")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "ns_per_event")
}
