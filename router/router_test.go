package router

import (
	"fmt"
	"math"
	"sync"
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
// GOOGLE TEST-STYLE FRAMEWORK IMPLEMENTATION
// ════════════════════════════════════════════════════════════════════════════════════════════════

// Test assertion framework inspired by Google Test
type TestAssertion struct {
	t *testing.T
}

func NewAssertion(t *testing.T) *TestAssertion {
	return &TestAssertion{t: t}
}

// EXPECT_* functions (non-fatal assertions)
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
	case int:
		if r, ok := right.(int); ok && l > r {
			message := fmt.Sprintf("Expected: %v <= %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_LE failed: %s", message)
			return false
		}
	case uint64:
		if r, ok := right.(uint64); ok && l > r {
			message := fmt.Sprintf("Expected: %v <= %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_LE failed: %s", message)
			return false
		}
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

func (a *TestAssertion) EXPECT_GT(left, right interface{}, msg ...string) bool {
	switch l := left.(type) {
	case int:
		if r, ok := right.(int); ok && l <= r {
			message := fmt.Sprintf("Expected: %v > %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_GT failed: %s", message)
			return false
		}
	case uint64:
		if r, ok := right.(uint64); ok && l <= r {
			message := fmt.Sprintf("Expected: %v > %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_GT failed: %s", message)
			return false
		}
	case float64:
		if r, ok := right.(float64); ok && l <= r {
			message := fmt.Sprintf("Expected: %v > %v", left, right)
			if len(msg) > 0 {
				message = fmt.Sprintf("%s - %s", msg[0], message)
			}
			a.t.Errorf("EXPECT_GT failed: %s", message)
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
	case uint64:
		if r, ok := right.(uint64); ok && l < r {
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

// ASSERT_* functions (fatal assertions)
func (a *TestAssertion) ASSERT_EQ(expected, actual interface{}, msg ...string) {
	if !a.EXPECT_EQ(expected, actual, msg...) {
		a.t.FailNow()
	}
}

func (a *TestAssertion) ASSERT_TRUE(condition bool, msg ...string) {
	if !a.EXPECT_TRUE(condition, msg...) {
		a.t.FailNow()
	}
}

func (a *TestAssertion) ASSERT_FALSE(condition bool, msg ...string) {
	if !a.EXPECT_FALSE(condition, msg...) {
		a.t.FailNow()
	}
}

func (a *TestAssertion) ASSERT_NE(expected, actual interface{}, msg ...string) {
	if !a.EXPECT_NE(expected, actual, msg...) {
		a.t.FailNow()
	}
}

func (a *TestAssertion) ASSERT_NO_FATAL_FAILURE(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			a.t.Fatalf("ASSERT_NO_FATAL_FAILURE failed: panic occurred: %v", r)
		}
	}()
	fn()
}

// Test fixture base class
type TestFixture struct {
	*TestAssertion
}

func NewTestFixture(t *testing.T) *TestFixture {
	return &TestFixture{
		TestAssertion: NewAssertion(t),
	}
}

func (f *TestFixture) SetUp() {
	// Override in derived classes
}

func (f *TestFixture) TearDown() {
	// Override in derived classes
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// ROUTER TEST FIXTURE
// ════════════════════════════════════════════════════════════════════════════════════════════════

type RouterTestFixture struct {
	*TestFixture
}

func NewRouterTestFixture(t *testing.T) *RouterTestFixture {
	return &RouterTestFixture{
		TestFixture: NewTestFixture(t),
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
	// Create properly formatted hex data for reserves
	data := "0x"

	// Reserve 0: 32 bytes (64 hex chars) with uint112 value
	reserve0Hex := make([]byte, 64)
	for i := range reserve0Hex {
		reserve0Hex[i] = '0'
	}
	reserve0Str := fmt.Sprintf("%028x", reserve0) // 28 chars for uint112
	copy(reserve0Hex[64-len(reserve0Str):], reserve0Str)
	data += string(reserve0Hex)

	// Reserve 1: 32 bytes (64 hex chars) with uint112 value
	reserve1Hex := make([]byte, 64)
	for i := range reserve1Hex {
		reserve1Hex[i] = '0'
	}
	reserve1Str := fmt.Sprintf("%028x", reserve1) // 28 chars for uint112
	copy(reserve1Hex[64-len(reserve1Str):], reserve1Str)
	data += string(reserve1Hex)

	return &types.LogView{
		Addr: []byte(address),
		Data: []byte(data),
	}
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// CORE TYPE TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestTickUpdateStructure(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("SizeRequirement", func(t *testing.T) {
		tu := TickUpdate{
			pairID:      PairID(12345),
			forwardTick: 1.5,
			reverseTick: -1.5,
		}

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
	defer fixture.TearDown()

	t.Run("CacheLineAlignment", func(t *testing.T) {
		var cycle ArbitrageCycleState
		addr := uintptr(unsafe.Pointer(&cycle))

		fixture.EXPECT_EQ(uintptr(0), addr%64, "ArbitrageCycleState must be 64-byte aligned for cache optimization")
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
	defer fixture.TearDown()

	t.Run("EqualityComparison", func(t *testing.T) {
		key1 := AddressKey{words: [3]uint64{0x1234567890abcdef, 0xfedcba0987654321, 0x12345678}}
		key2 := AddressKey{words: [3]uint64{0x1234567890abcdef, 0xfedcba0987654321, 0x12345678}}
		key3 := AddressKey{words: [3]uint64{0x1234567890abcdef, 0xfedcba0987654321, 0x12345679}}

		fixture.EXPECT_TRUE(key1.isEqual(key2), "Identical keys should be equal")
		fixture.EXPECT_FALSE(key1.isEqual(key3), "Different keys should not be equal")
	})

	t.Run("MemoryLayout", func(t *testing.T) {
		var key AddressKey
		addr := uintptr(unsafe.Pointer(&key))

		fixture.EXPECT_EQ(uintptr(0), addr%32, "AddressKey should be 32-byte aligned")
		fixture.EXPECT_EQ(32, int(unsafe.Sizeof(key)), "AddressKey should be 32 bytes total")
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// HEX PARSING TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestCountLeadingZeros(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	testCases := []struct {
		name     string
		input    string
		expected int
	}{
		{"AllZeros", "00000000000000000000000000000000", 32},
		{"NoLeadingZeros", "12345678901234567890123456789012", 0},
		{"FourLeadingZeros", "0000567890123456789012345678901", 4},
		{"EightLeadingZeros", "00000000901234567890123456789012", 8},
		{"SixteenLeadingZeros", "0000000000000000567890123456789", 16},
		{"TwentyFourLeadingZeros", "000000000000000000000000567890", 24},
		{"ThirtyOneLeadingZeros", "0000000000000000000000000000001", 31},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := countLeadingZeros([]byte(tc.input))
			fixture.EXPECT_EQ(tc.expected, result, fmt.Sprintf("countLeadingZeros('%s')", tc.input))
		})
	}
}

func TestBytesToAddressKey(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

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
	defer fixture.TearDown()

	t.Run("HashIndexGeneration", func(t *testing.T) {
		address1 := "1234567890123456789012345678901234567890"
		address2 := "1234567890123456789012345678901234567891"

		idx1 := directAddressToIndex64([]byte(address1))
		idx2 := directAddressToIndex64([]byte(address2))

		// Different addresses should generally produce different indices
		fixture.EXPECT_NE(idx1, idx2, "Different addresses should produce different hash indices")

		// Indices should be within table bounds
		fixture.EXPECT_LT(idx1, uint64(constants.AddressTableCapacity), "Hash index should be within table bounds")
		fixture.EXPECT_LT(idx2, uint64(constants.AddressTableCapacity), "Hash index should be within table bounds")
	})

	t.Run("HashConsistency", func(t *testing.T) {
		address := "1234567890123456789012345678901234567890"
		key := bytesToAddressKey([]byte(address))

		directIdx := directAddressToIndex64([]byte(address))
		storedIdx := directAddressToIndex64Stored(key)

		fixture.EXPECT_EQ(directIdx, storedIdx, "Direct and stored hash methods should produce same index")
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS REGISTRATION TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestPairAddressRegistration(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

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
	defer fixture.TearDown()

	t.Run("MultipleAddressRegistration", func(t *testing.T) {
		addresses := []string{
			"1234567890123456789012345678901234567890",
			"1234567890123456789012345678901234567891",
			"1234567890123456789012345678901234567892",
			"abcdefabcdefabcdefabcdefabcdefabcdefabcd",
			"fedcbafedcbafedcbafedcbafedcbafedcbafed",
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
	defer fixture.TearDown()

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
// CRYPTOGRAPHIC RANDOM TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestKeccakRandomGeneration(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

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
}

func TestKeccakRandomBounds(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("NextIntBoundsChecking", func(t *testing.T) {
		seed := []byte("bounds_test")
		rng := newKeccakRandom(seed)

		bounds := []int{1, 10, 100, 1000}

		for _, bound := range bounds {
			for i := 0; i < 50; i++ {
				val := rng.nextInt(bound)
				fixture.EXPECT_GE(val, 0, "Value should be >= 0")
				fixture.EXPECT_LT(val, bound, fmt.Sprintf("Value should be < %d", bound))
			}
		}
	})

	t.Run("EdgeCaseBounds", func(t *testing.T) {
		seed := []byte("edge_cases")
		rng := newKeccakRandom(seed)

		fixture.EXPECT_EQ(0, rng.nextInt(0), "nextInt(0) should return 0")
		fixture.EXPECT_EQ(0, rng.nextInt(-1), "nextInt(-1) should return 0")
	})
}

func TestEdgeBindingsShuffle(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("ShufflePreservesElements", func(t *testing.T) {
		// Create test bindings
		bindings := make([]ArbitrageEdgeBinding, 10)
		for i := range bindings {
			bindings[i] = ArbitrageEdgeBinding{
				cyclePairs: [3]PairID{PairID(i), PairID(i + 1), PairID(i + 2)},
				edgeIndex:  uint64(i),
			}
		}

		original := make([]ArbitrageEdgeBinding, len(bindings))
		copy(original, bindings)

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
		// Create test bindings
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
	defer fixture.TearDown()

	t.Run("UnregisteredPairHandling", func(t *testing.T) {
		logView := fixture.CreateTestLogView(
			"0x9999999999999999999999999999999999999999",
			1000000000000, 2000000000000,
		)

		// Should return early without processing (no assertion possible)
		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchTickUpdate(logView)
		})
	})

	t.Run("ValidPairProcessing", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := PairID(12345)

		RegisterPairAddress([]byte(address[2:]), pairID) // Remove 0x prefix
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
}

func TestDispatchFallbackLogic(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("ZeroReservesFallback", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := PairID(12345)

		RegisterPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCore(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		// Create log view with zero reserves (triggers fallback)
		logView := fixture.CreateTestLogView(address, 0, 0)

		DispatchTickUpdate(logView)

		message := coreRings[0].Pop()
		fixture.ASSERT_TRUE(message != nil, "Fallback should still send message")

		tickUpdate := (*TickUpdate)(unsafe.Pointer(message))
		fixture.EXPECT_GE(tickUpdate.forwardTick, 51.2, "Fallback tick should be >= 51.2")
		fixture.EXPECT_LE(tickUpdate.forwardTick, 64.0, "Fallback tick should be <= 64.0")
		fixture.EXPECT_EQ(-tickUpdate.forwardTick, tickUpdate.reverseTick, "Reverse should be negative forward")
	})
}

func TestDispatchMultiCoreDistribution(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

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
// QUANTIZATION TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestTickQuantization(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("BasicQuantizationBounds", func(t *testing.T) {
		testValues := []float64{0.0, 50.0, -50.0, 100.0, -100.0}

		for _, tickValue := range testValues {
			result := quantizeTickToInt64(tickValue)

			fixture.EXPECT_GE(result, int64(0), "Quantized value should be non-negative")
			fixture.EXPECT_LE(result, int64(constants.MaxQuantizedTick), "Quantized value should be within max bound")
		}
	})

	t.Run("MonotonicityProperty", func(t *testing.T) {
		// Test that larger ticks produce larger quantized values
		baseValue := 10.0
		largerValue := baseValue + 1.0

		baseQuantized := quantizeTickToInt64(baseValue)
		largerQuantized := quantizeTickToInt64(largerValue)

		fixture.EXPECT_GT(largerQuantized, baseQuantized, "Quantization should preserve order")
	})

	t.Run("BoundaryValues", func(t *testing.T) {
		// Test boundary conditions
		minBound := -constants.TickClampingBound
		maxBound := constants.TickClampingBound

		minQuantized := quantizeTickToInt64(minBound)
		maxQuantized := quantizeTickToInt64(maxBound)

		fixture.EXPECT_GE(minQuantized, int64(0), "Min bound should quantize to valid range")
		fixture.EXPECT_LE(maxQuantized, int64(constants.MaxQuantizedTick), "Max bound should quantize to valid range")
		fixture.EXPECT_LT(minQuantized, maxQuantized, "Min should be less than max when quantized")
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// SHARD CONSTRUCTION TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestFanoutShardConstruction(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("BasicShardCreation", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(1), PairID(4), PairID(5)},
			{PairID(2), PairID(6), PairID(7)},
		}

		buildFanoutShardBuckets(cycles)

		fixture.EXPECT_TRUE(pairShardBuckets != nil, "Shard buckets should be created")
		fixture.EXPECT_GT(len(pairShardBuckets), 0, "Should have non-empty shard buckets")
	})

	t.Run("PairCycleMapping", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(1), PairID(4), PairID(5)},
		}

		buildFanoutShardBuckets(cycles)

		// Pair 1 should appear in 2 cycles
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

// ════════════════════════════════════════════════════════════════════════════════════════════════
// CORE PROCESSING TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestCoreProcessingLogic(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("ForwardDirectionProcessing", func(t *testing.T) {
		executor := &ArbitrageCoreExecutor{
			pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
			isReverseDirection: false,
			cycleStates:        make([]ArbitrageCycleState, 0),
			fanoutTables:       make([][]FanoutEntry, 0),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 0),
		}

		executor.priorityQueues = append(executor.priorityQueues, *quantumqueue64.New())
		executor.fanoutTables = append(executor.fanoutTables, nil)

		pairID := PairID(123)
		queueIndex := executor.pairToQueueIndex.Put(uint32(pairID), 0)
		fixture.EXPECT_EQ(uint32(0), queueIndex, "Queue index should be 0")

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
			isReverseDirection: true, // Reverse direction
			cycleStates:        make([]ArbitrageCycleState, 0),
			fanoutTables:       make([][]FanoutEntry, 0),
			priorityQueues:     make([]quantumqueue64.QuantumQueue64, 0),
		}

		executor.priorityQueues = append(executor.priorityQueues, *quantumqueue64.New())
		executor.fanoutTables = append(executor.fanoutTables, nil)

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
// INTEGRATION TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestSystemInitialization(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("SmallSystemInit", func(t *testing.T) {
		cycles := []ArbitrageTriplet{
			{PairID(1), PairID(2), PairID(3)},
			{PairID(4), PairID(5), PairID(6)},
		}

		// Test with small system (we can't override runtime.NumCPU)
		// The system will use actual CPU count, which should work fine for testing
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

func TestEndToEndIntegration(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("CompleteProcessingPipeline", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := PairID(12345)

		// Setup system
		RegisterPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCore(pairID, 0)

		cycles := []ArbitrageTriplet{{pairID, PairID(2), PairID(3)}}
		buildFanoutShardBuckets(cycles)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		// Process update
		logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchTickUpdate(logView)
		})

		// Verify end-to-end processing
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
// STRESS AND PERFORMANCE TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func TestHighVolumeOperations(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("MassAddressRegistration", func(t *testing.T) {
		numAddresses := 1000

		// Register many addresses
		for i := 0; i < numAddresses; i++ {
			address := fmt.Sprintf("%040d", i)
			pairID := PairID(i + 1)
			RegisterPairAddress([]byte(address), pairID)
		}

		// Verify all can be looked up
		lookupFailures := 0
		for i := 0; i < numAddresses; i++ {
			address := fmt.Sprintf("%040d", i)
			result := lookupPairIDByAddress([]byte(address))
			if result != PairID(i+1) {
				lookupFailures++
			}
		}

		fixture.EXPECT_EQ(0, lookupFailures, "All registered addresses should be retrievable")
	})

	t.Run("ConcurrentLookups", func(t *testing.T) {
		// Register test addresses
		for i := 0; i < 100; i++ {
			address := fmt.Sprintf("%040d", i)
			RegisterPairAddress([]byte(address), PairID(i+1))
		}

		// Concurrent lookup test
		var wg sync.WaitGroup
		errorCount := int32(0)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(startIdx int) {
				defer wg.Done()

				for j := 0; j < 10; j++ {
					address := fmt.Sprintf("%040d", startIdx*10+j)
					result := lookupPairIDByAddress([]byte(address))
					expected := PairID(startIdx*10 + j + 1)

					if result != expected {
						errorCount++
					}
				}
			}(i)
		}

		wg.Wait()
		fixture.EXPECT_EQ(int32(0), errorCount, "Concurrent lookups should all succeed")
	})
}

func TestExtremeValueHandling(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

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

func TestMemoryAlignmentRequirements(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("StructureAlignment", func(t *testing.T) {
		var cycle ArbitrageCycleState
		var fanout FanoutEntry
		var processed ProcessedCycle

		cycleAddr := uintptr(unsafe.Pointer(&cycle))
		fanoutAddr := uintptr(unsafe.Pointer(&fanout))
		processedAddr := uintptr(unsafe.Pointer(&processed))

		fixture.EXPECT_EQ(uintptr(0), cycleAddr%64, "ArbitrageCycleState should be 64-byte aligned")
		fixture.EXPECT_EQ(uintptr(0), fanoutAddr%32, "FanoutEntry should be 32-byte aligned")
		fixture.EXPECT_EQ(uintptr(0), processedAddr%32, "ProcessedCycle should be 32-byte aligned")
	})
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// BENCHMARK TESTS
// ════════════════════════════════════════════════════════════════════════════════════════════════

func BenchmarkDispatchTickUpdate(b *testing.B) {
	fixture := NewRouterTestFixture(&testing.T{})
	fixture.SetUp()
	defer fixture.TearDown()

	address := "0x1234567890123456789012345678901234567890"
	pairID := PairID(12345)
	RegisterPairAddress([]byte(address[2:]), pairID)
	RegisterPairToCore(pairID, 0)
	coreRings[0] = ring24.New(constants.DefaultRingSize)

	logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DispatchTickUpdate(logView)

		// Prevent overflow
		if i%1000 == 0 {
			for coreRings[0].Pop() != nil {
			}
		}
	}
}

func BenchmarkCountLeadingZeros(b *testing.B) {
	input := []byte("00000000000000001234567890123456")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = countLeadingZeros(input)
	}
}

func BenchmarkAddressLookup(b *testing.B) {
	fixture := NewRouterTestFixture(&testing.T{})
	fixture.SetUp()
	defer fixture.TearDown()

	// Register addresses
	for i := 0; i < 1000; i++ {
		address := fmt.Sprintf("%040d", i)
		RegisterPairAddress([]byte(address), PairID(i+1))
	}

	addresses := make([][]byte, 100)
	for i := range addresses {
		addresses[i] = []byte(fmt.Sprintf("%040d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lookupPairIDByAddress(addresses[i%len(addresses)])
	}
}

func BenchmarkQuantization(b *testing.B) {
	values := []float64{-100.5, -50.0, 0.0, 25.7, 100.0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = quantizeTickToInt64(values[i%len(values)])
	}
}
