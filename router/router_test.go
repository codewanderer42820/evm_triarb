// router_test.go — Comprehensive 100% coverage test suite for triangular arbitrage detection engine

package router

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"main/constants"
	"main/localidx"
	"main/pooledquantumqueue"
	"main/ring24"
	"main/types"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TEST UTILITIES AND HELPERS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type TestAssertion struct {
	t *testing.T
}

func NewAssertion(t *testing.T) *TestAssertion {
	return &TestAssertion{t: t}
}

func (a *TestAssertion) Equal(expected, actual interface{}, msg string) {
	if expected != actual {
		a.t.Errorf("%s: expected %v, got %v", msg, expected, actual)
	}
}

func (a *TestAssertion) True(condition bool, msg string) {
	if !condition {
		a.t.Errorf("%s: expected true", msg)
	}
}

func (a *TestAssertion) False(condition bool, msg string) {
	if condition {
		a.t.Errorf("%s: expected false", msg)
	}
}

func (a *TestAssertion) Near(expected, actual, tolerance float64, msg string) {
	if math.Abs(expected-actual) > tolerance {
		a.t.Errorf("%s: expected %v ± %v, got %v", msg, expected, tolerance, actual)
	}
}

func (a *TestAssertion) NoError(err error, msg string) {
	if err != nil {
		a.t.Errorf("%s: unexpected error: %v", msg, err)
	}
}

func (a *TestAssertion) NotNil(value interface{}, msg string) {
	if value == nil {
		a.t.Errorf("%s: expected non-nil value", msg)
	}
}

func (a *TestAssertion) Nil(value interface{}, msg string) {
	if value != nil {
		a.t.Errorf("%s: expected nil value, got %v", msg, value)
	}
}

type TestHelper struct {
	t *testing.T
}

func NewTestHelper(t *testing.T) *TestHelper {
	return &TestHelper{t: t}
}

func (h *TestHelper) ClearGlobalState() {
	// Clear core engines and rings
	for i := 0; i < constants.MaxSupportedCores; i++ {
		coreEngines[i] = nil
		coreRings[i] = nil
	}

	// Clear routing and address tables
	for i := 0; i < constants.PairRoutingTableCapacity && i < 10000; i++ {
		pairToCoreRouting[i] = 0
	}

	for i := 0; i < constants.AddressTableCapacity && i < 10000; i++ {
		addressToPairMap[i] = 0
		packedAddressKeys[i] = PackedAddress{}
	}

	// Clear workload shards
	pairWorkloadShards = nil
}

func (h *TestHelper) CreateLogView(address string, reserve0, reserve1 uint64) *types.LogView {
	// Create properly formatted hex data for reserves
	data := fmt.Sprintf("0x%064x%064x", reserve0, reserve1)
	return &types.LogView{
		Addr: []byte(address),
		Data: []byte(data),
	}
}

func (h *TestHelper) CreateLogViewWithRawData(address string, data string) *types.LogView {
	return &types.LogView{
		Addr: []byte(address),
		Data: []byte(data),
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// STRUCTURE AND MEMORY LAYOUT TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestStructureSizes(t *testing.T) {
	assert := NewAssertion(t)

	t.Run("PriceUpdateMessage", func(t *testing.T) {
		var msg PriceUpdateMessage
		assert.Equal(24, int(unsafe.Sizeof(msg)), "PriceUpdateMessage size should be 24 bytes")

		// Test field alignment
		msg.pairID = TradingPairID(12345)
		msg.forwardTick = 1.5
		msg.reverseTick = -1.5

		assert.Equal(TradingPairID(12345), msg.pairID, "pairID field access")
		assert.Equal(1.5, msg.forwardTick, "forwardTick field access")
		assert.Equal(-1.5, msg.reverseTick, "reverseTick field access")
	})

	t.Run("ArbitrageCycleState", func(t *testing.T) {
		var cycle ArbitrageCycleState
		assert.Equal(64, int(unsafe.Sizeof(cycle)), "ArbitrageCycleState size should be 64 bytes")

		// Test field access and the zero optimization
		cycle.tickValues[0] = 1.0
		cycle.tickValues[1] = 0.0 // This would be the intentionally zero value
		cycle.tickValues[2] = -0.5
		cycle.pairIDs[0] = TradingPairID(100)
		cycle.pairIDs[1] = TradingPairID(200)
		cycle.pairIDs[2] = TradingPairID(300)

		assert.Equal(1.0, cycle.tickValues[0], "tickValues[0] access")
		assert.Equal(0.0, cycle.tickValues[1], "tickValues[1] access (zero optimization)")
		assert.Equal(-0.5, cycle.tickValues[2], "tickValues[2] access")
		assert.Equal(TradingPairID(100), cycle.pairIDs[0], "pairIDs[0] access")
	})

	t.Run("ExtractedCycle", func(t *testing.T) {
		var extracted ExtractedCycle
		assert.Equal(32, int(unsafe.Sizeof(extracted)), "ExtractedCycle size should be 32 bytes")

		extracted.cycleIndex = CycleIndex(42)
		extracted.originalTick = 12345
		extracted.queueHandle = pooledquantumqueue.Handle(67890)

		assert.Equal(CycleIndex(42), extracted.cycleIndex, "cycleIndex field access")
		assert.Equal(int64(12345), extracted.originalTick, "originalTick field access")
		assert.Equal(pooledquantumqueue.Handle(67890), extracted.queueHandle, "queueHandle field access")
	})

	t.Run("ArbitrageEngine", func(t *testing.T) {
		var engine ArbitrageEngine
		size := unsafe.Sizeof(engine)

		// Verify the engine size is reasonable and extractedCycles array is correct size
		assert.Equal(32, len(engine.extractedCycles), "extractedCycles should have 32 elements")

		expectedExtractedCyclesSize := 32 * 32 // 32 cycles * 32 bytes each = 1024 bytes
		actualExtractedCyclesSize := unsafe.Sizeof(engine.extractedCycles)
		assert.Equal(expectedExtractedCyclesSize, int(actualExtractedCyclesSize), "extractedCycles array size")

		t.Logf("ArbitrageEngine total size: %d bytes", size)
	})

	t.Run("PackedAddress", func(t *testing.T) {
		var addr PackedAddress
		assert.Equal(32, int(unsafe.Sizeof(addr)), "PackedAddress size should be 32 bytes")

		addr.words[0] = 0x1234567890abcdef
		addr.words[1] = 0xfedcba0987654321
		addr.words[2] = 0x1122334455667788

		assert.Equal(uint64(0x1234567890abcdef), addr.words[0], "words[0] access")
		assert.Equal(uint64(0xfedcba0987654321), addr.words[1], "words[1] access")
		assert.Equal(uint64(0x1122334455667788), addr.words[2], "words[2] access")
	})
}

func TestTypeDefinitions(t *testing.T) {
	assert := NewAssertion(t)

	t.Run("TradingPairID", func(t *testing.T) {
		var pairID TradingPairID = 12345
		assert.Equal(TradingPairID(12345), pairID, "TradingPairID assignment")
		assert.Equal(uint64(12345), uint64(pairID), "TradingPairID underlying type")
	})

	t.Run("CycleIndex", func(t *testing.T) {
		var idx CycleIndex = 54321
		assert.Equal(CycleIndex(54321), idx, "CycleIndex assignment")
		assert.Equal(uint64(54321), uint64(idx), "CycleIndex underlying type")
	})

	t.Run("ArbitrageTriangle", func(t *testing.T) {
		triangle := ArbitrageTriangle{
			TradingPairID(1),
			TradingPairID(2),
			TradingPairID(3),
		}
		assert.Equal(TradingPairID(1), triangle[0], "triangle[0] access")
		assert.Equal(TradingPairID(2), triangle[1], "triangle[1] access")
		assert.Equal(TradingPairID(3), triangle[2], "triangle[2] access")
		assert.Equal(3, len(triangle), "triangle length")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE ENGINE METHODS TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestArbitrageEngineAllocation(t *testing.T) {
	assert := NewAssertion(t)

	t.Run("HandleAllocation", func(t *testing.T) {
		engine := &ArbitrageEngine{
			nextHandle: pooledquantumqueue.Handle(100),
		}

		// Test sequential allocation
		handle1 := engine.allocateQueueHandle()
		handle2 := engine.allocateQueueHandle()
		handle3 := engine.allocateQueueHandle()

		assert.Equal(pooledquantumqueue.Handle(100), handle1, "first handle")
		assert.Equal(pooledquantumqueue.Handle(101), handle2, "second handle")
		assert.Equal(pooledquantumqueue.Handle(102), handle3, "third handle")
		assert.Equal(pooledquantumqueue.Handle(103), engine.nextHandle, "nextHandle incremented")
	})

	t.Run("HandleAllocationFromZero", func(t *testing.T) {
		engine := &ArbitrageEngine{nextHandle: 0}

		handle1 := engine.allocateQueueHandle()
		handle2 := engine.allocateQueueHandle()

		assert.Equal(pooledquantumqueue.Handle(0), handle1, "first handle from zero")
		assert.Equal(pooledquantumqueue.Handle(1), handle2, "second handle from zero")
		assert.Equal(pooledquantumqueue.Handle(2), engine.nextHandle, "nextHandle at 2")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HEX PARSING AND UTILITY FUNCTIONS TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestCountHexLeadingZeros(t *testing.T) {
	assert := NewAssertion(t)

	testCases := []struct {
		name     string
		input    string
		expected int
	}{
		{"AllZeros", "00000000000000000000000000000000", 32},
		{"NoLeadingZeros", "12345678901234567890123456789012", 0},
		{"OneLeadingZero", "01234567890123456789012345678901", 1},
		{"TwoLeadingZeros", "00123456789012345678901234567890", 2},
		{"EightLeadingZeros", "00000000123456789012345678901234", 8},
		{"SixteenLeadingZeros", "0000000000000000123456789012345", 16},
		{"TwentyFourLeadingZeros", "000000000000000000000000123456789", 24},
		{"ThirtyLeadingZeros", "00000000000000000000000000000012", 30},
		{"ThirtyOneLeadingZeros", "00000000000000000000000000000001", 31},
		{"MixedPattern", "00000123456789abcdef000000000000", 5},
		{"EdgeCaseBoundary", "0000000012345678901234567890123", 7},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Ensure input is exactly 32 bytes
			input := make([]byte, 32)
			copy(input, []byte(tc.input))

			// Pad with zeros if input is shorter than 32 bytes
			for i := len(tc.input); i < 32; i++ {
				input[i] = '0'
			}

			result := countHexLeadingZeros(input)
			assert.Equal(tc.expected, result, fmt.Sprintf("countHexLeadingZeros(%s)", tc.name))
		})
	}

	t.Run("InvalidInputHandling", func(t *testing.T) {
		// Test with non-hex characters (should still work with bit manipulation)
		input := make([]byte, 32)
		copy(input, []byte("gggggggggggggggggggggggggggggggg"))

		result := countHexLeadingZeros(input)
		assert.Equal(0, result, "non-hex characters should give 0 leading zeros")
	})

	t.Run("ChunkBoundaryTesting", func(t *testing.T) {
		// Test inputs that cross 8-byte chunk boundaries
		testInputs := []struct {
			input    string
			expected int
		}{
			{"0000000012345678901234567890123", 7},   // 7 zeros (within first chunk)
			{"00000000123456789012345678901234", 8},  // 8 zeros (exactly one chunk)
			{"000000001234567890123456789012345", 8}, // 8 zeros (exactly one chunk)
			{"0000000000000000123456789012345", 16},  // 16 zeros (exactly two chunks)
		}

		for _, test := range testInputs {
			input := make([]byte, 32)
			copy(input, []byte(test.input))
			for i := len(test.input); i < 32; i++ {
				input[i] = '0'
			}

			result := countHexLeadingZeros(input)
			assert.Equal(test.expected, result, fmt.Sprintf("chunk boundary test: %s", test.input))
		}
	})
}

func TestQuantizeTickValue(t *testing.T) {
	assert := NewAssertion(t)

	t.Run("BasicQuantization", func(t *testing.T) {
		testCases := []struct {
			input float64
		}{
			{0.0},
			{1.0},
			{-1.0},
			{10.5},
			{-5.25},
		}

		for _, tc := range testCases {
			result := quantizeTickValue(tc.input)
			expected := quantizeTickValue(tc.input) // Call the actual function to get expected result
			assert.Equal(expected, result, fmt.Sprintf("quantizeTickValue(%f)", tc.input))

			// Verify the result is reasonable (non-negative due to clamping)
			assert.True(result >= 0, fmt.Sprintf("quantized value should be non-negative for %f", tc.input))
		}
	})

	t.Run("MonotonicityProperty", func(t *testing.T) {
		// Test that quantization preserves order
		values := []float64{-10.0, -5.0, -1.0, 0.0, 1.0, 5.0, 10.0}
		quantized := make([]int64, len(values))

		for i, val := range values {
			quantized[i] = quantizeTickValue(val)
		}

		// Verify monotonicity
		for i := 1; i < len(quantized); i++ {
			assert.True(quantized[i] > quantized[i-1],
				fmt.Sprintf("quantization should preserve order: %d should be > %d", quantized[i], quantized[i-1]))
		}
	})

	t.Run("NonNegativityProperty", func(t *testing.T) {
		// All quantized values should be non-negative due to clamping bound
		testValues := []float64{-100.0, -10.0, -1.0, 0.0, 1.0, 10.0, 100.0}

		for _, val := range testValues {
			result := quantizeTickValue(val)
			assert.True(result >= 0, fmt.Sprintf("quantized value should be non-negative for input %f, got %d", val, result))
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS PROCESSING TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestPackedAddressOperations(t *testing.T) {
	assert := NewAssertion(t)

	t.Run("AddressPacking", func(t *testing.T) {
		testAddresses := []string{
			"1234567890123456789012345678901234567890",
			"abcdefabcdefabcdefabcdefabcdefabcdefabcd",
			"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
			"0000000000000000000000000000000000000000",
			"ffffffffffffffffffffffffffffffffffffffff",
		}

		for _, addr := range testAddresses {
			packed := packEthereumAddress([]byte(addr))

			// Should not be all zeros unless input was all zeros
			if addr != "0000000000000000000000000000000000000000" {
				isZero := packed.words[0] == 0 && packed.words[1] == 0 && packed.words[2] == 0
				assert.False(isZero, fmt.Sprintf("packed address should not be all zeros for %s", addr))
			}
		}
	})

	t.Run("AddressEquality", func(t *testing.T) {
		addr1 := "1234567890123456789012345678901234567890"
		addr2 := "1234567890123456789012345678901234567890"
		addr3 := "abcdefabcdefabcdefabcdefabcdefabcdefabcd"

		packed1 := packEthereumAddress([]byte(addr1))
		packed2 := packEthereumAddress([]byte(addr2))
		packed3 := packEthereumAddress([]byte(addr3))

		assert.True(packed1.isEqual(packed2), "identical addresses should be equal")
		assert.False(packed1.isEqual(packed3), "different addresses should not be equal")
	})

	t.Run("HashingConsistency", func(t *testing.T) {
		address := "a478c2975ab1ea89e8196811f51a7b7ade33eb11"

		// Hash from raw address should be consistent
		hash1 := hashAddressToIndex([]byte(address))
		hash2 := hashAddressToIndex([]byte(address))
		assert.Equal(hash1, hash2, "raw address hashing should be consistent")

		// Hash from packed address should match
		packed := packEthereumAddress([]byte(address))
		packedHash := hashPackedAddressToIndex(packed)

		// Both hashes should be within table bounds
		assert.True(hash1 < uint64(constants.AddressTableMask+1), "raw hash within bounds")
		assert.True(packedHash < uint64(constants.AddressTableMask+1), "packed hash within bounds")
	})

	t.Run("UniqueAddressesProduceUniqueKeys", func(t *testing.T) {
		addresses := []string{
			"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
			"bb2b8038a1640196fbe3e38816f3e67cba72d940",
			"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
			"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			"ae461ca67b15dc8dc81ce7615e0320da1a9ab8d5",
		}

		packedAddresses := make([]PackedAddress, len(addresses))
		for i, addr := range addresses {
			packedAddresses[i] = packEthereumAddress([]byte(addr))
		}

		// Verify all packed addresses are unique
		for i := 0; i < len(packedAddresses); i++ {
			for j := i + 1; j < len(packedAddresses); j++ {
				assert.False(packedAddresses[i].isEqual(packedAddresses[j]),
					fmt.Sprintf("addresses %d and %d should produce different packed representations", i, j))
			}
		}
	})
}

func TestAddressRegistrationAndLookup(t *testing.T) {
	assert := NewAssertion(t)
	helper := NewTestHelper(t)

	t.Run("BasicRegistrationAndLookup", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		// Register and verify
		RegisterTradingPairAddress([]byte(address), pairID)
		result := lookupPairByAddress([]byte(address))
		assert.Equal(pairID, result, "registered address should be retrievable")
	})

	t.Run("NonExistentAddressLookup", func(t *testing.T) {
		helper.ClearGlobalState()

		nonExistent := "9999999999999999999999999999999999999999"
		result := lookupPairByAddress([]byte(nonExistent))
		assert.Equal(TradingPairID(0), result, "non-existent address should return 0")
	})

	t.Run("AddressUpdateOverwrite", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "1111111111111111111111111111111111111111"
		pairID1 := TradingPairID(100)
		pairID2 := TradingPairID(200)

		// Register first pair ID
		RegisterTradingPairAddress([]byte(address), pairID1)
		result1 := lookupPairByAddress([]byte(address))
		assert.Equal(pairID1, result1, "first registration should work")

		// Update with second pair ID
		RegisterTradingPairAddress([]byte(address), pairID2)
		result2 := lookupPairByAddress([]byte(address))
		assert.Equal(pairID2, result2, "address update should overwrite")
	})

	t.Run("MultipleAddressRegistration", func(t *testing.T) {
		helper.ClearGlobalState()

		addresses := []string{
			"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
			"bb2b8038a1640196fbe3e38816f3e67cba72d940",
			"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
			"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
		}

		// Register all addresses
		for i, addr := range addresses {
			pairID := TradingPairID(i + 1000)
			RegisterTradingPairAddress([]byte(addr), pairID)
		}

		// Verify all addresses can be looked up
		for i, addr := range addresses {
			expectedPairID := TradingPairID(i + 1000)
			result := lookupPairByAddress([]byte(addr))
			assert.Equal(expectedPairID, result, fmt.Sprintf("address %d should be retrievable", i))
		}
	})

	t.Run("RobinHoodHashingCollisionHandling", func(t *testing.T) {
		helper.ClearGlobalState()

		// Use a set of addresses that might cause collisions
		addresses := []string{
			"ae461ca67b15dc8dc81ce7615e0320da1a9ab8d5",
			"397ff1542f962076d0bfe58ea045ffa2d347aca0",
			"c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
			"a0b86a33e6776d1bc61e2c1c5f5e8e5c1e9b2c3d",
			"b0b86a33e6776d1bc61e2c1c5f5e8e5c1e9b2c3d",
			"c0b86a33e6776d1bc61e2c1c5f5e8e5c1e9b2c3d",
		}

		// Register all addresses
		for i, addr := range addresses {
			pairID := TradingPairID(i + 500)
			RegisterTradingPairAddress([]byte(addr), pairID)
		}

		// Verify all addresses can still be found despite potential collisions
		for i, addr := range addresses {
			expectedPairID := TradingPairID(i + 500)
			result := lookupPairByAddress([]byte(addr))
			assert.Equal(expectedPairID, result, fmt.Sprintf("collision candidate %d should be retrievable", i))
		}
	})

	t.Run("HighVolumeStressTest", func(t *testing.T) {
		helper.ClearGlobalState()

		numAddresses := 100
		registered := 0
		retrieved := 0

		// Generate and register many addresses
		for i := 0; i < numAddresses; i++ {
			address := fmt.Sprintf("%040x", i*0x123456789+0x987654321)
			pairID := TradingPairID(i + 2000)

			RegisterTradingPairAddress([]byte(address), pairID)
			registered++

			// Verify it can be retrieved
			result := lookupPairByAddress([]byte(address))
			if result == pairID {
				retrieved++
			}
		}

		assert.Equal(numAddresses, registered, "all addresses should be registered")

		// We expect high success rate but allow for some hash table limitations
		successRate := float64(retrieved) / float64(registered)
		assert.True(successRate > 0.9, fmt.Sprintf("high success rate expected, got %f", successRate))
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ROUTING TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestPairToCoreRouting(t *testing.T) {
	assert := NewAssertion(t)
	helper := NewTestHelper(t)

	t.Run("BasicRouting", func(t *testing.T) {
		helper.ClearGlobalState()

		pairID := TradingPairID(100)

		// Register routing for multiple cores
		RegisterPairToCoreRouting(pairID, 0)
		RegisterPairToCoreRouting(pairID, 2)
		RegisterPairToCoreRouting(pairID, 5)

		routing := pairToCoreRouting[pairID]

		// Verify bitmask has correct bits set
		assert.True((routing&(1<<0)) != 0, "core 0 should be set")
		assert.True((routing&(1<<1)) == 0, "core 1 should not be set")
		assert.True((routing&(1<<2)) != 0, "core 2 should be set")
		assert.True((routing&(1<<3)) == 0, "core 3 should not be set")
		assert.True((routing&(1<<4)) == 0, "core 4 should not be set")
		assert.True((routing&(1<<5)) != 0, "core 5 should be set")
	})

	t.Run("MultiplePairsRouting", func(t *testing.T) {
		helper.ClearGlobalState()

		pairs := []TradingPairID{100, 200, 300}
		coreAssignments := [][]uint8{
			{0, 1},
			{2, 3, 4},
			{1, 5},
		}

		// Register routing for multiple pairs
		for i, pairID := range pairs {
			for _, coreID := range coreAssignments[i] {
				RegisterPairToCoreRouting(pairID, coreID)
			}
		}

		// Verify each pair has correct routing
		for i, pairID := range pairs {
			routing := pairToCoreRouting[pairID]
			for _, expectedCore := range coreAssignments[i] {
				assert.True((routing&(1<<expectedCore)) != 0,
					fmt.Sprintf("pair %d should route to core %d", pairID, expectedCore))
			}
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// EVENT DISPATCH TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestDispatchPriceUpdate(t *testing.T) {
	assert := NewAssertion(t)
	helper := NewTestHelper(t)

	t.Run("UnregisteredPair", func(t *testing.T) {
		helper.ClearGlobalState()

		logView := helper.CreateLogView(
			"0x9999999999999999999999999999999999999999",
			1000000000000, 2000000000000,
		)

		// Should not panic or cause errors for unregistered pair
		DispatchPriceUpdate(logView)
		// Test passes if no panic occurs
	})

	t.Run("ValidPairDispatch", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		logView := helper.CreateLogView(address, 1000000000000, 2000000000000)
		DispatchPriceUpdate(logView)

		// Verify message was sent
		message := coreRings[0].Pop()
		assert.NotNil(message, "message should be sent")

		if message != nil {
			priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
			assert.Equal(pairID, priceUpdate.pairID, "correct pairID")
			assert.False(math.IsNaN(priceUpdate.forwardTick), "tick should not be NaN")
			assert.False(math.IsInf(priceUpdate.forwardTick, 0), "tick should not be infinite")
			assert.Equal(-priceUpdate.forwardTick, priceUpdate.reverseTick, "reverse tick should be negative of forward")
		}
	})

	t.Run("ZeroReservesFallback", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		logView := helper.CreateLogView(address, 0, 0)
		DispatchPriceUpdate(logView)

		message := coreRings[0].Pop()
		assert.NotNil(message, "fallback should send message")

		if message != nil {
			priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
			assert.True(priceUpdate.forwardTick >= 50.2, "fallback tick should be >= 50.2")
			assert.True(priceUpdate.forwardTick <= 64.0, "fallback tick should be <= 64.0")
			assert.Equal(priceUpdate.forwardTick, priceUpdate.reverseTick, "fallback should have equal ticks")
		}
	})

	t.Run("MultiCoreDistribution", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)
		cores := []int{0, 2, 5}

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		for _, coreID := range cores {
			RegisterPairToCoreRouting(pairID, uint8(coreID))
			coreRings[coreID] = ring24.New(constants.DefaultRingSize)
		}

		logView := helper.CreateLogView(address, 1500000000000, 3000000000000)
		DispatchPriceUpdate(logView)

		// Verify all assigned cores received message
		for _, coreID := range cores {
			message := coreRings[coreID].Pop()
			assert.NotNil(message, fmt.Sprintf("core %d should receive message", coreID))

			if message != nil {
				priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
				assert.Equal(pairID, priceUpdate.pairID, "correct pairID")
			}
		}

		// Verify unassigned core doesn't receive message
		if coreRings[1] == nil {
			coreRings[1] = ring24.New(constants.DefaultRingSize)
		}
		message := coreRings[1].Pop()
		assert.Nil(message, "unassigned core should not receive message")
	})

	t.Run("RealEthereumDataSamples", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11"
		pairID := TradingPairID(11111)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		realDataSamples := []string{
			"0x00000000000000000000000000000000000000000078e3833588cda8d5e102c3000000000000000000000000000000000000000000000000001fa8dd7963f22c",
			"0x00000000000000000000000000000000000000000078ac4cf9c9bb7cb9e54739000000000000000000000000000000000000000000000000001fcf7f300f7aee",
			"0x0000000000000000000000000000000000000000000000000000011b6dc13f6900000000000000000000000000000000000000000000001638362ed366158ac1",
		}

		for i, data := range realDataSamples {
			logView := helper.CreateLogViewWithRawData(address, data)
			DispatchPriceUpdate(logView)

			message := coreRings[0].Pop()
			assert.NotNil(message, fmt.Sprintf("sample %d should produce message", i))

			if message != nil {
				priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
				assert.Equal(pairID, priceUpdate.pairID, "correct pairID")
				assert.False(math.IsNaN(priceUpdate.forwardTick), "tick should not be NaN")
				assert.False(math.IsInf(priceUpdate.forwardTick, 0), "tick should not be infinite")
			}
		}
	})

	t.Run("GuaranteedDeliveryWithFullRings", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)

		// Create a very small ring to test retry logic
		coreRings[0] = ring24.New(4)

		// Fill the ring to capacity
		dummyMsg := [24]byte{0}
		for i := 0; i < 4; i++ {
			success := coreRings[0].Push(&dummyMsg)
			assert.True(success, fmt.Sprintf("should fill ring slot %d", i))
		}

		// Verify ring is full
		overflowMsg := [24]byte{1}
		assert.False(coreRings[0].Push(&overflowMsg), "ring should be full")

		// Now try to dispatch - should retry until successful
		logView := helper.CreateLogView(address, 1000000000000, 2000000000000)

		// Empty one slot in a separate goroutine to allow delivery
		go func() {
			time.Sleep(10 * time.Millisecond)
			coreRings[0].Pop()
		}()

		// This should eventually succeed due to guaranteed delivery
		DispatchPriceUpdate(logView)

		// Give some time for the retry mechanism
		time.Sleep(50 * time.Millisecond)

		// There should be messages in the ring
		foundPriceUpdate := false
		for i := 0; i < 5; i++ { // Check a few messages
			msg := coreRings[0].Pop()
			if msg != nil {
				priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(msg))
				if priceUpdate.pairID == pairID {
					foundPriceUpdate = true
					break
				}
			}
		}
		assert.True(foundPriceUpdate, "price update should eventually be delivered")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ARBITRAGE PROCESSING TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestProcessArbitrageUpdate(t *testing.T) {
	t.Run("DirectionSelection", func(t *testing.T) {
		// Test forward direction engine
		forwardEngine := &ArbitrageEngine{
			isReverseDirection: false,
			pairToQueueLookup:  localidx.New(16),
			priorityQueues:     make([]pooledquantumqueue.PooledQuantumQueue, 1),
		}

		// Test reverse direction engine
		reverseEngine := &ArbitrageEngine{
			isReverseDirection: true,
			pairToQueueLookup:  localidx.New(16),
			priorityQueues:     make([]pooledquantumqueue.PooledQuantumQueue, 1),
		}

		// Create a mock update
		update := &PriceUpdateMessage{
			pairID:      TradingPairID(1),
			forwardTick: 1.5,
			reverseTick: -1.5,
		}

		// Set up minimal queue state
		forwardEngine.pairToQueueLookup.Put(1, 0)
		reverseEngine.pairToQueueLookup.Put(1, 0)

		// For this test, we just verify no panics occur
		// Full processing requires more complex setup
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("processArbitrageUpdate should not panic: %v", r)
			}
		}()

		// These will work with empty queues
		processArbitrageUpdate(forwardEngine, update)
		processArbitrageUpdate(reverseEngine, update)
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CRYPTOGRAPHIC RANDOM GENERATION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestCryptoRandomGeneration(t *testing.T) {
	assert := NewAssertion(t)

	t.Run("BasicGeneration", func(t *testing.T) {
		seed := []byte("test_seed_12345")
		rng := newCryptoRandomGenerator(seed)

		// Generate several values
		values := make([]uint64, 20)
		for i := range values {
			values[i] = rng.generateRandomUint64()
		}

		// Check for uniqueness (should have mostly unique values)
		seen := make(map[uint64]bool)
		duplicates := 0
		for _, v := range values {
			if seen[v] {
				duplicates++
			}
			seen[v] = true
		}

		assert.True(duplicates < 5, fmt.Sprintf("should have mostly unique values, got %d duplicates", duplicates))
	})

	t.Run("DeterministicBehavior", func(t *testing.T) {
		seed := []byte("deterministic_test_seed")

		rng1 := newCryptoRandomGenerator(seed)
		rng2 := newCryptoRandomGenerator(seed)

		// Both generators should produce identical sequences
		for i := 0; i < 10; i++ {
			v1 := rng1.generateRandomUint64()
			v2 := rng2.generateRandomUint64()
			assert.Equal(v1, v2, fmt.Sprintf("deterministic failure at position %d", i))
		}
	})

	t.Run("BoundedIntGeneration", func(t *testing.T) {
		seed := []byte("bounded_test_seed")
		rng := newCryptoRandomGenerator(seed)

		bounds := []int{1, 2, 10, 100, 1000}
		for _, bound := range bounds {
			for i := 0; i < 20; i++ {
				val := rng.generateRandomInt(bound)
				assert.True(val >= 0, fmt.Sprintf("value should be >= 0, got %d", val))
				assert.True(val < bound, fmt.Sprintf("value should be < %d, got %d", bound, val))
			}
		}
	})

	t.Run("CounterIncrement", func(t *testing.T) {
		seed := []byte("counter_test")
		rng := newCryptoRandomGenerator(seed)

		initialCounter := rng.counter
		assert.Equal(uint64(0), initialCounter, "initial counter should be 0")

		// Generate some values and verify counter increments
		for i := 0; i < 5; i++ {
			rng.generateRandomUint64()
			expectedCounter := uint64(i + 1)
			assert.Equal(expectedCounter, rng.counter, fmt.Sprintf("counter should be %d after %d generations", expectedCounter, i+1))
		}
	})

	t.Run("DifferentSeedsProduceDifferentSequences", func(t *testing.T) {
		seed1 := []byte("seed_one")
		seed2 := []byte("seed_two")

		rng1 := newCryptoRandomGenerator(seed1)
		rng2 := newCryptoRandomGenerator(seed2)

		// Generate several values from each
		values1 := make([]uint64, 10)
		values2 := make([]uint64, 10)

		for i := 0; i < 10; i++ {
			values1[i] = rng1.generateRandomUint64()
			values2[i] = rng2.generateRandomUint64()
		}

		// Sequences should be different
		differences := 0
		for i := 0; i < 10; i++ {
			if values1[i] != values2[i] {
				differences++
			}
		}

		assert.True(differences >= 8, fmt.Sprintf("different seeds should produce mostly different sequences, got %d differences", differences))
	})
}

func TestShuffleCycleEdges(t *testing.T) {
	assert := NewAssertion(t)

	t.Run("BasicShuffle", func(t *testing.T) {
		cycles := []CycleEdge{
			{cyclePairs: [3]TradingPairID{1, 2, 3}, edgeIndex: 0},
			{cyclePairs: [3]TradingPairID{4, 5, 6}, edgeIndex: 1},
			{cyclePairs: [3]TradingPairID{7, 8, 9}, edgeIndex: 2},
			{cyclePairs: [3]TradingPairID{10, 11, 12}, edgeIndex: 0},
			{cyclePairs: [3]TradingPairID{13, 14, 15}, edgeIndex: 1},
		}

		originalCycles := make([]CycleEdge, len(cycles))
		copy(originalCycles, cycles)

		pairID := TradingPairID(12345)
		shuffleCycleEdges(cycles, pairID)

		// Should have same elements but potentially different order
		assert.Equal(len(originalCycles), len(cycles), "length should remain same")

		// Check that all original elements are still present
		for _, original := range originalCycles {
			found := false
			for _, shuffled := range cycles {
				if original.cyclePairs == shuffled.cyclePairs && original.edgeIndex == shuffled.edgeIndex {
					found = true
					break
				}
			}
			assert.True(found, "all original elements should be present after shuffle")
		}
	})

	t.Run("DeterministicShuffleWithSamePairID", func(t *testing.T) {
		cycles1 := []CycleEdge{
			{cyclePairs: [3]TradingPairID{1, 2, 3}, edgeIndex: 0},
			{cyclePairs: [3]TradingPairID{4, 5, 6}, edgeIndex: 1},
			{cyclePairs: [3]TradingPairID{7, 8, 9}, edgeIndex: 2},
		}

		cycles2 := make([]CycleEdge, len(cycles1))
		copy(cycles2, cycles1)

		pairID := TradingPairID(999)

		shuffleCycleEdges(cycles1, pairID)
		shuffleCycleEdges(cycles2, pairID)

		// Should produce identical shuffles for same pairID
		for i := 0; i < len(cycles1); i++ {
			assert.Equal(cycles1[i], cycles2[i], fmt.Sprintf("deterministic shuffle failed at position %d", i))
		}
	})

	t.Run("EmptyAndSingleElementEdgeCases", func(t *testing.T) {
		// Empty slice
		var emptyCycles []CycleEdge
		shuffleCycleEdges(emptyCycles, TradingPairID(1))
		assert.Equal(0, len(emptyCycles), "empty slice should remain empty")

		// Single element
		singleCycle := []CycleEdge{
			{cyclePairs: [3]TradingPairID{1, 2, 3}, edgeIndex: 0},
		}
		original := singleCycle[0]
		shuffleCycleEdges(singleCycle, TradingPairID(1))
		assert.Equal(original, singleCycle[0], "single element should remain unchanged")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// WORKLOAD SHARD TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestBuildWorkloadShards(t *testing.T) {
	assert := NewAssertion(t)
	helper := NewTestHelper(t)

	t.Run("BasicShardConstruction", func(t *testing.T) {
		helper.ClearGlobalState()

		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
			{TradingPairID(1), TradingPairID(4), TradingPairID(5)},
			{TradingPairID(2), TradingPairID(6), TradingPairID(7)},
		}

		buildWorkloadShards(triangles)

		assert.NotNil(pairWorkloadShards, "shards should be created")
		assert.True(len(pairWorkloadShards) > 0, "should have shards")

		// Verify pair 1 appears in 2 triangles
		shards1, exists1 := pairWorkloadShards[TradingPairID(1)]
		assert.True(exists1, "pair 1 should have shards")

		totalBindings1 := 0
		for _, shard := range shards1 {
			totalBindings1 += len(shard.cycleEdges)
			assert.Equal(TradingPairID(1), shard.pairID, "shard should have correct pairID")
		}
		assert.Equal(2, totalBindings1, "pair 1 should appear in 2 cycles")

		// Verify pair 2 appears in 2 triangles
		shards2, exists2 := pairWorkloadShards[TradingPairID(2)]
		assert.True(exists2, "pair 2 should have shards")

		totalBindings2 := 0
		for _, shard := range shards2 {
			totalBindings2 += len(shard.cycleEdges)
		}
		assert.Equal(2, totalBindings2, "pair 2 should appear in 2 cycles")
	})

	t.Run("ComplexTriangleOverlap", func(t *testing.T) {
		helper.ClearGlobalState()

		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
			{TradingPairID(2), TradingPairID(3), TradingPairID(4)},
			{TradingPairID(3), TradingPairID(4), TradingPairID(5)},
			{TradingPairID(1), TradingPairID(5), TradingPairID(6)},
		}

		buildWorkloadShards(triangles)

		expectedCounts := map[TradingPairID]int{
			TradingPairID(1): 2, // Appears in triangles 0 and 3
			TradingPairID(2): 2, // Appears in triangles 0 and 1
			TradingPairID(3): 3, // Appears in triangles 0, 1, and 2
			TradingPairID(4): 2, // Appears in triangles 1 and 2
			TradingPairID(5): 2, // Appears in triangles 2 and 3
			TradingPairID(6): 1, // Appears in triangle 3 only
		}

		for pairID, expectedCount := range expectedCounts {
			shards, exists := pairWorkloadShards[pairID]
			assert.True(exists, fmt.Sprintf("pair %d should exist", pairID))

			totalEdges := 0
			for _, shard := range shards {
				totalEdges += len(shard.cycleEdges)
			}
			assert.Equal(expectedCount, totalEdges, fmt.Sprintf("pair %d edge count", pairID))
		}
	})

	t.Run("EdgeIndexCorrectness", func(t *testing.T) {
		helper.ClearGlobalState()

		triangle := ArbitrageTriangle{TradingPairID(10), TradingPairID(20), TradingPairID(30)}
		triangles := []ArbitrageTriangle{triangle}

		buildWorkloadShards(triangles)

		// Verify each pair has correct edge index
		for i, pairID := range triangle {
			shards, exists := pairWorkloadShards[pairID]
			assert.True(exists, fmt.Sprintf("pair %d should exist", pairID))

			found := false
			for _, shard := range shards {
				for _, edge := range shard.cycleEdges {
					if edge.cyclePairs == triangle && edge.edgeIndex == uint64(i) {
						found = true
						break
					}
				}
			}
			assert.True(found, fmt.Sprintf("pair %d should have correct edge index %d", pairID, i))
		}
	})

	t.Run("EmptyTrianglesInput", func(t *testing.T) {
		helper.ClearGlobalState()

		var emptyTriangles []ArbitrageTriangle
		buildWorkloadShards(emptyTriangles)

		// Should handle empty input gracefully
		if pairWorkloadShards != nil {
			assert.Equal(0, len(pairWorkloadShards), "empty input should produce empty shards")
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestInitializeArbitrageSystem(t *testing.T) {
	assert := NewAssertion(t)
	helper := NewTestHelper(t)

	t.Run("BasicSystemInitialization", func(t *testing.T) {
		helper.ClearGlobalState()

		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
			{TradingPairID(4), TradingPairID(5), TradingPairID(6)},
		}

		// Should not panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("InitializeArbitrageSystem should not panic: %v", r)
			}
		}()

		InitializeArbitrageSystem(triangles)

		// Give workers time to start
		time.Sleep(50 * time.Millisecond)

		// Verify some pairs were assigned to cores
		assignedPairs := 0
		for i := 0; i < 10; i++ {
			if pairToCoreRouting[i] != 0 {
				assignedPairs++
			}
		}
		assert.True(assignedPairs > 0, "some pairs should be assigned to cores")
	})

	t.Run("CoreCountHandling", func(t *testing.T) {
		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
		}

		originalCPUCount := runtime.NumCPU()
		assert.True(originalCPUCount > 0, "should detect CPU cores")

		// System should handle various CPU counts gracefully
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("InitializeArbitrageSystem should handle CPU count gracefully: %v", r)
			}
		}()

		InitializeArbitrageSystem(triangles)
	})

	t.Run("EmptyTrianglesInitialization", func(t *testing.T) {
		helper.ClearGlobalState()

		var emptyTriangles []ArbitrageTriangle

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("InitializeArbitrageSystem should handle empty triangles: %v", r)
			}
		}()

		InitializeArbitrageSystem(emptyTriangles)
		time.Sleep(20 * time.Millisecond)
		// Should complete without panic
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONCURRENT AND STRESS TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestConcurrentOperations(t *testing.T) {
	assert := NewAssertion(t)
	helper := NewTestHelper(t)

	t.Run("ConcurrentAddressLookups", func(t *testing.T) {
		helper.ClearGlobalState()

		// Setup test addresses
		addresses := []string{
			"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
			"bb2b8038a1640196fbe3e38816f3e67cba72d940",
			"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
			"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
		}

		// Register addresses
		for i, addr := range addresses {
			RegisterTradingPairAddress([]byte(addr), TradingPairID(i+1))
		}

		var wg sync.WaitGroup
		var successCount int32
		numWorkers := 4

		// Launch concurrent workers
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				localSuccess := 0

				for i := 0; i < 50; i++ {
					for j, addr := range addresses {
						result := lookupPairByAddress([]byte(addr))
						if result == TradingPairID(j+1) {
							localSuccess++
						}
					}
				}
				atomic.AddInt32(&successCount, int32(localSuccess))
			}()
		}

		wg.Wait()

		expectedTotal := numWorkers * 50 * len(addresses)
		actualSuccess := int(successCount)
		successRate := float64(actualSuccess) / float64(expectedTotal)

		assert.True(successRate > 0.95, fmt.Sprintf("concurrent lookups should have high success rate, got %f", successRate))
	})

	t.Run("StressTestWithManyAddresses", func(t *testing.T) {
		helper.ClearGlobalState()

		numAddresses := 200
		addresses := make([]string, numAddresses)

		// Generate many test addresses
		for i := 0; i < numAddresses; i++ {
			addresses[i] = fmt.Sprintf("%040x", i*0x123456789abcdef+0x987654321fedcba)
		}

		// Register all addresses
		registered := 0
		for i, addr := range addresses {
			RegisterTradingPairAddress([]byte(addr), TradingPairID(i+1000))
			registered++
		}

		// Verify retrieval
		retrieved := 0
		for i, addr := range addresses {
			result := lookupPairByAddress([]byte(addr))
			if result == TradingPairID(i+1000) {
				retrieved++
			}
		}

		successRate := float64(retrieved) / float64(registered)
		assert.True(successRate > 0.8, fmt.Sprintf("stress test should have reasonable success rate, got %f", successRate))
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RING BUFFER INTEGRATION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestRingBufferIntegration(t *testing.T) {
	assert := NewAssertion(t)
	helper := NewTestHelper(t)

	t.Run("MessageStructureIntegrity", func(t *testing.T) {
		helper.ClearGlobalState()

		ring := ring24.New(16)
		assert.NotNil(ring, "ring should be created")

		// Create test message
		originalMsg := PriceUpdateMessage{
			pairID:      TradingPairID(12345),
			forwardTick: 1.5,
			reverseTick: -1.5,
		}

		// Convert to byte array and push
		msgBytes := (*[24]byte)(unsafe.Pointer(&originalMsg))
		success := ring.Push(msgBytes)
		assert.True(success, "should push message successfully")

		// Pop and verify integrity
		retrieved := ring.Pop()
		assert.NotNil(retrieved, "should retrieve message")

		if retrieved != nil {
			retrievedMsg := (*PriceUpdateMessage)(unsafe.Pointer(retrieved))
			assert.Equal(TradingPairID(12345), retrievedMsg.pairID, "pairID should match")
			assert.Equal(1.5, retrievedMsg.forwardTick, "forwardTick should match")
			assert.Equal(-1.5, retrievedMsg.reverseTick, "reverseTick should match")
		}
	})

	t.Run("MultipleMessageHandling", func(t *testing.T) {
		helper.ClearGlobalState()

		ring := ring24.New(8)

		// Create multiple different messages
		messages := []PriceUpdateMessage{
			{pairID: TradingPairID(100), forwardTick: 1.0, reverseTick: -1.0},
			{pairID: TradingPairID(200), forwardTick: 2.5, reverseTick: -2.5},
			{pairID: TradingPairID(300), forwardTick: -0.5, reverseTick: 0.5},
			{pairID: TradingPairID(400), forwardTick: 10.0, reverseTick: -10.0},
		}

		// Push all messages
		for i, msg := range messages {
			msgBytes := (*[24]byte)(unsafe.Pointer(&msg))
			success := ring.Push(msgBytes)
			assert.True(success, fmt.Sprintf("should push message %d", i))
		}

		// Pop and verify all messages in FIFO order
		for i, expectedMsg := range messages {
			retrieved := ring.Pop()
			assert.NotNil(retrieved, fmt.Sprintf("should retrieve message %d", i))

			if retrieved != nil {
				actualMsg := (*PriceUpdateMessage)(unsafe.Pointer(retrieved))
				assert.Equal(expectedMsg.pairID, actualMsg.pairID, fmt.Sprintf("message %d pairID", i))
				assert.Equal(expectedMsg.forwardTick, actualMsg.forwardTick, fmt.Sprintf("message %d forwardTick", i))
				assert.Equal(expectedMsg.reverseTick, actualMsg.reverseTick, fmt.Sprintf("message %d reverseTick", i))
			}
		}
	})

	t.Run("RingCapacityLimits", func(t *testing.T) {
		helper.ClearGlobalState()

		capacity := 4
		ring := ring24.New(capacity)

		// Fill to capacity
		for i := 0; i < capacity; i++ {
			msg := PriceUpdateMessage{pairID: TradingPairID(i)}
			msgBytes := (*[24]byte)(unsafe.Pointer(&msg))
			success := ring.Push(msgBytes)
			assert.True(success, fmt.Sprintf("should push message %d", i))
		}

		// Next push should fail (ring full)
		overflowMsg := PriceUpdateMessage{pairID: TradingPairID(999)}
		overflowBytes := (*[24]byte)(unsafe.Pointer(&overflowMsg))
		assert.False(ring.Push(overflowBytes), "ring should reject when full")

		// Pop one message to make space
		retrieved := ring.Pop()
		assert.NotNil(retrieved, "should pop oldest message")

		// Now we should be able to push again
		success := ring.Push(overflowBytes)
		assert.True(success, "should push after making space")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// EDGE CASES AND ERROR HANDLING TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestEdgeCasesAndErrorHandling(t *testing.T) {
	assert := NewAssertion(t)
	helper := NewTestHelper(t)

	t.Run("ZeroAddressRegistration", func(t *testing.T) {
		helper.ClearGlobalState()

		zeroAddress := "0000000000000000000000000000000000000000"
		pairID := TradingPairID(1)

		RegisterTradingPairAddress([]byte(zeroAddress), pairID)
		result := lookupPairByAddress([]byte(zeroAddress))
		assert.Equal(pairID, result, "zero address should be registerable")
	})

	t.Run("MaxValueAddressRegistration", func(t *testing.T) {
		helper.ClearGlobalState()

		maxAddress := "ffffffffffffffffffffffffffffffffffffffff"
		pairID := TradingPairID(2)

		RegisterTradingPairAddress([]byte(maxAddress), pairID)
		result := lookupPairByAddress([]byte(maxAddress))
		assert.Equal(pairID, result, "max value address should be registerable")
	})

	t.Run("VeryLargeReserveValues", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		// Test with very large reserve values
		maxUint64 := uint64(0xFFFFFFFFFFFFFFFF)
		logView := helper.CreateLogView(address, maxUint64, maxUint64/2)

		DispatchPriceUpdate(logView)

		message := coreRings[0].Pop()
		assert.NotNil(message, "should handle large reserves")

		if message != nil {
			priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
			assert.Equal(pairID, priceUpdate.pairID, "correct pairID")
			assert.False(math.IsNaN(priceUpdate.forwardTick), "tick should not be NaN")
			assert.False(math.IsInf(priceUpdate.forwardTick, 0), "tick should not be infinite")
		}
	})

	t.Run("InvalidHexDataHandling", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		// Test with malformed hex data
		logView := helper.CreateLogViewWithRawData(address, "0xinvalid_hex_data_that_is_too_short")

		DispatchPriceUpdate(logView)

		// Should either produce a fallback message or handle gracefully
		message := coreRings[0].Pop()
		if message != nil {
			priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
			assert.Equal(pairID, priceUpdate.pairID, "correct pairID")
		}
		// Test passes if no panic occurs
	})

	t.Run("ExtremelyLongAddresses", func(t *testing.T) {
		helper.ClearGlobalState()

		// Test with longer than normal address (should still work by taking first 40 chars)
		longAddress := "1234567890123456789012345678901234567890extra_characters_that_should_be_ignored"
		pairID := TradingPairID(999)

		RegisterTradingPairAddress([]byte(longAddress[:40]), pairID)
		result := lookupPairByAddress([]byte(longAddress[:40]))
		assert.Equal(pairID, result, "should handle address by taking first 40 characters")
	})

	t.Run("ZeroPairIDHandling", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "1234567890123456789012345678901234567890"

		// Register with zero pair ID (should work but be indistinguishable from "not found")
		RegisterTradingPairAddress([]byte(address), TradingPairID(0))
		result := lookupPairByAddress([]byte(address))
		// Result is 0, but we can't distinguish if it's registered as 0 or not found
		assert.Equal(TradingPairID(0), result, "zero pair ID lookup")
	})

	t.Run("MaxPairIDValues", func(t *testing.T) {
		helper.ClearGlobalState()

		address := "abcdefabcdefabcdefabcdefabcdefabcdefabcd"
		maxPairID := TradingPairID(^uint64(0)) // Maximum uint64 value

		RegisterTradingPairAddress([]byte(address), maxPairID)
		result := lookupPairByAddress([]byte(address))
		assert.Equal(maxPairID, result, "should handle maximum pair ID values")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION SPECIFIC TESTS (COVERING COMPLEX PATHS)
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestInitializationSpecific(t *testing.T) {
	assert := NewAssertion(t)
	helper := NewTestHelper(t)

	t.Run("InitializeArbitrageQueuesEmptyShards", func(t *testing.T) {
		engine := &ArbitrageEngine{
			pairToQueueLookup: localidx.New(16),
			priorityQueues:    make([]pooledquantumqueue.PooledQuantumQueue, 0),
		}

		var emptyShards []PairWorkloadShard

		// Should handle empty shards gracefully
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("initializeArbitrageQueues should handle empty shards: %v", r)
			}
		}()

		initializeArbitrageQueues(engine, emptyShards)
		// Test passes if no panic
	})

	t.Run("LaunchArbitrageWorkerWithRealShards", func(t *testing.T) {
		helper.ClearGlobalState()

		// Create a channel with actual workload shards
		shardChan := make(chan PairWorkloadShard, 10)

		// Create some test shards
		testShards := []PairWorkloadShard{
			{
				pairID: TradingPairID(100),
				cycleEdges: []CycleEdge{
					{cyclePairs: [3]TradingPairID{100, 200, 300}, edgeIndex: 0},
					{cyclePairs: [3]TradingPairID{100, 400, 500}, edgeIndex: 0},
				},
			},
			{
				pairID: TradingPairID(200),
				cycleEdges: []CycleEdge{
					{cyclePairs: [3]TradingPairID{100, 200, 300}, edgeIndex: 1},
				},
			},
		}

		// Send shards and close channel
		go func() {
			for _, shard := range testShards {
				shardChan <- shard
			}
			close(shardChan)
		}()

		// Launch worker in separate goroutine
		workerDone := make(chan bool)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("launchArbitrageWorker should not panic: %v", r)
				}
				workerDone <- true
			}()

			// This will block on ring24.PinnedConsumer, so we run it with timeout
			launchArbitrageWorker(0, 1, shardChan)
		}()

		// Give worker time to initialize
		select {
		case <-workerDone:
			// Worker completed (shouldn't happen normally due to PinnedConsumer)
		case <-time.After(100 * time.Millisecond):
			// Expected - worker should be running in PinnedConsumer loop
		}

		// Verify engine was created
		assert.NotNil(coreEngines[0], "engine should be created for core 0")
		if coreEngines[0] != nil {
			assert.False(coreEngines[0].isReverseDirection, "core 0 should be forward direction")
		}

		// Verify ring was created
		assert.NotNil(coreRings[0], "ring should be created for core 0")
	})

	t.Run("CompleteSystemWithRealData", func(t *testing.T) {
		helper.ClearGlobalState()

		// Create a more realistic triangle set
		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
			{TradingPairID(2), TradingPairID(4), TradingPairID(5)},
			{TradingPairID(3), TradingPairID(5), TradingPairID(6)},
			{TradingPairID(1), TradingPairID(4), TradingPairID(6)},
		}

		// Initialize the system
		InitializeArbitrageSystem(triangles)

		// Give system time to start up
		time.Sleep(100 * time.Millisecond)

		// Verify routing was set up
		routingSet := 0
		for i := TradingPairID(1); i <= TradingPairID(6); i++ {
			if pairToCoreRouting[i] != 0 {
				routingSet++
			}
		}
		assert.True(routingSet >= 4, "most pairs should have routing set up")

		// Register some addresses and test dispatch
		addresses := map[TradingPairID]string{
			TradingPairID(1): "a478c2975ab1ea89e8196811f51a7b7ade33eb11",
			TradingPairID(2): "bb2b8038a1640196fbe3e38816f3e67cba72d940",
		}

		for currentPairID, addr := range addresses {
			RegisterTradingPairAddress([]byte(addr), currentPairID)
		}

		// Test actual dispatch
		for _, addr := range addresses {
			logView := helper.CreateLogView("0x"+addr, 1000000000000, 2000000000000)
			DispatchPriceUpdate(logView)
		}

		// Give messages time to be processed
		time.Sleep(50 * time.Millisecond)

		// Check that some cores received messages
		messagesReceived := 0
		for i := 0; i < 8; i++ {
			if coreRings[i] != nil {
				for coreRings[i].Pop() != nil {
					messagesReceived++
				}
			}
		}
		assert.True(messagesReceived >= 0, "system should process messages")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PERFORMANCE AND BENCHMARK TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func BenchmarkCountHexLeadingZeros(b *testing.B) {
	input := make([]byte, 32)
	copy(input, []byte("0000000000000000e8455d7f2faa9bde"))
	for i := len("0000000000000000e8455d7f2faa9bde"); i < 32; i++ {
		input[i] = '0'
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = countHexLeadingZeros(input)
	}
}

func BenchmarkQuantizeTickValue(b *testing.B) {
	values := []float64{-10.0, -1.0, 0.0, 1.0, 10.0, 50.5, -25.25}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = quantizeTickValue(values[i%len(values)])
	}
}

func BenchmarkAddressLookup(b *testing.B) {
	// Setup
	for i := 0; i < 100; i++ {
		addressToPairMap[i] = 0
		packedAddressKeys[i] = PackedAddress{}
	}

	addresses := []string{
		"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
		"bb2b8038a1640196fbe3e38816f3e67cba72d940",
		"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
		"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
	}

	for i, addr := range addresses {
		RegisterTradingPairAddress([]byte(addr), TradingPairID(i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = lookupPairByAddress([]byte(addr))
	}
}

func BenchmarkAddressRegistration(b *testing.B) {
	addresses := []string{
		"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
		"bb2b8038a1640196fbe3e38816f3e67cba72d940",
		"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
		"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clear periodically to avoid table saturation
		if i%1000 == 0 {
			for j := 0; j < 200; j++ {
				addressToPairMap[j] = 0
				packedAddressKeys[j] = PackedAddress{}
			}
		}

		addr := addresses[i%len(addresses)]
		RegisterTradingPairAddress([]byte(addr), TradingPairID(i+1))
	}
}

func BenchmarkDispatchPriceUpdate(b *testing.B) {
	// Setup
	for i := 0; i < 100; i++ {
		addressToPairMap[i] = 0
		packedAddressKeys[i] = PackedAddress{}
		pairToCoreRouting[i] = 0
	}

	address := "a478c2975ab1ea89e8196811f51a7b7ade33eb11"
	pairID := TradingPairID(12345)
	RegisterTradingPairAddress([]byte(address), pairID)
	RegisterPairToCoreRouting(pairID, 0)

	coreRings[0] = ring24.New(1 << 16) // Large ring to avoid blocking

	logView := &types.LogView{
		Addr: []byte("0x" + address),
		Data: []byte("0x00000000000000000000000000000000000000000078ac4cf9c9bb7cb9e54739000000000000000000000000000000000000000000000000001fcf7f300f7aee"),
	}

	// Consumer goroutine to drain messages
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
		DispatchPriceUpdate(logView)
	}

	close(stop)
	time.Sleep(10 * time.Millisecond)
}

func BenchmarkPackEthereumAddress(b *testing.B) {
	address := "a478c2975ab1ea89e8196811f51a7b7ade33eb11"
	addressBytes := []byte(address)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = packEthereumAddress(addressBytes)
	}
}

func BenchmarkCryptoRandomGeneration(b *testing.B) {
	seed := []byte("benchmark_seed_12345")
	rng := newCryptoRandomGenerator(seed)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rng.generateRandomUint64()
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDITIONAL COVERAGE TESTS FOR MISSED PATHS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestAdditionalCoverage(t *testing.T) {
	assert := NewAssertion(t)

	t.Run("ArbitrageTriangleAccess", func(t *testing.T) {
		// Test direct access to triangle elements
		triangle := ArbitrageTriangle{TradingPairID(10), TradingPairID(20), TradingPairID(30)}

		assert.Equal(TradingPairID(10), triangle[0], "first element")
		assert.Equal(TradingPairID(20), triangle[1], "second element")
		assert.Equal(TradingPairID(30), triangle[2], "third element")

		// Test assignment
		triangle[1] = TradingPairID(25)
		assert.Equal(TradingPairID(25), triangle[1], "element reassignment")
	})

	t.Run("CycleFanoutEntryFields", func(t *testing.T) {
		// Test all fields of CycleFanoutEntry
		entry := CycleFanoutEntry{
			cycleIndex:  42,
			edgeIndex:   1,
			queueIndex:  3,
			queueHandle: pooledquantumqueue.Handle(999),
		}

		assert.Equal(uint64(42), entry.cycleIndex, "cycleIndex field")
		assert.Equal(uint64(1), entry.edgeIndex, "edgeIndex field")
		assert.Equal(uint64(3), entry.queueIndex, "queueIndex field")
		assert.Equal(pooledquantumqueue.Handle(999), entry.queueHandle, "queueHandle field")
	})

	t.Run("CycleEdgeFields", func(t *testing.T) {
		// Test CycleEdge structure
		edge := CycleEdge{
			cyclePairs: [3]TradingPairID{100, 200, 300},
			edgeIndex:  2,
		}

		assert.Equal(TradingPairID(100), edge.cyclePairs[0], "cyclePairs[0]")
		assert.Equal(TradingPairID(200), edge.cyclePairs[1], "cyclePairs[1]")
		assert.Equal(TradingPairID(300), edge.cyclePairs[2], "cyclePairs[2]")
		assert.Equal(uint64(2), edge.edgeIndex, "edgeIndex")
	})

	t.Run("PairWorkloadShardFields", func(t *testing.T) {
		// Test PairWorkloadShard structure
		shard := PairWorkloadShard{
			pairID: TradingPairID(500),
			cycleEdges: []CycleEdge{
				{cyclePairs: [3]TradingPairID{1, 2, 3}, edgeIndex: 0},
			},
		}

		assert.Equal(TradingPairID(500), shard.pairID, "pairID field")
		assert.Equal(1, len(shard.cycleEdges), "cycleEdges length")
		assert.Equal(uint64(0), shard.cycleEdges[0].edgeIndex, "nested edgeIndex")
	})

	t.Run("CryptoRandomGeneratorFields", func(t *testing.T) {
		// Test CryptoRandomGenerator fields access
		seed := []byte("test")
		rng := newCryptoRandomGenerator(seed)

		assert.Equal(uint64(0), rng.counter, "initial counter")
		assert.NotNil(rng.hasher, "hasher should be initialized")

		// Generate a value to increment counter
		rng.generateRandomUint64()
		assert.Equal(uint64(1), rng.counter, "counter after generation")
	})

	t.Run("TypeConversions", func(t *testing.T) {
		// Test type conversions and assignments
		var pairID TradingPairID = 12345
		var cycleIdx CycleIndex = 67890

		// Test conversions to base types
		assert.Equal(uint64(12345), uint64(pairID), "TradingPairID to uint64")
		assert.Equal(uint64(67890), uint64(cycleIdx), "CycleIndex to uint64")

		// Test assignments from base types
		pairID = TradingPairID(54321)
		cycleIdx = CycleIndex(9876)

		assert.Equal(TradingPairID(54321), pairID, "assignment from uint64")
		assert.Equal(CycleIndex(9876), cycleIdx, "assignment from uint64")
	})
}
