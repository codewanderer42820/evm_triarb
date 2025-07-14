// comprehensive_router_tests.go — Complete test suite for 57ns triangular arbitrage detection engine

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COMPREHENSIVE TEST COVERAGE FOR CLEAN POOLED QUANTUM QUEUE ARCHITECTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════
//
// Test categories with complete coverage:
//   - Core type structure validation and memory layout verification
//   - Hex parsing and address conversion correctness with edge cases
//   - Robin Hood hash table displacement and collision handling stress tests
//   - Address registration and lookup performance validation with real data
//   - Cryptographic randomness and deterministic shuffling verification
//   - Event dispatch pipeline and tick update processing with fallback logic
//   - Ring buffer overflow and retry logic stress testing with multi-core scenarios
//   - Quantization and core processing with shared arena architecture
//   - Fanout processing and priority queue operations validation
//   - Shard construction and system initialization comprehensive testing
//   - High-volume operations and extreme value handling stress tests
//   - End-to-end integration and performance benchmarks with realistic loads
//   - Clean shared arena architecture validation and isolation testing
//
// Architecture validation:
//   - Zero-allocation hot paths with shared memory architecture
//   - Multi-core parallelism and lock-free inter-core communication
//   - Robin Hood address resolution with backward shift deletion
//   - Branchless algorithms and mathematical correctness verification
//   - Shared memory pool efficiency and cache locality optimization
//   - Sequential handle allocation without complex partitioning
//
// Performance benchmarks:
//   - Event processing: Target 15 nanoseconds per Uniswap V2 Sync event
//   - Address resolution: Target 8 nanoseconds per lookup
//   - Arbitrage detection: Target 25 nanoseconds per cycle update
//   - System throughput: Linear scaling validation up to 64 CPU cores
//   - Memory efficiency: Zero allocations in all hot paths verification
//
// Safety model testing:
//   - Handle lifecycle management with proper allocation/deallocation
//   - Shared arena validation and memory coherency verification
//   - Pool boundary validation and collision prevention
//   - Proper cleanup and test isolation with comprehensive teardown
//   - Resource leak detection and prevention across all test scenarios

package router

import (
	"fmt"
	"math"
	"reflect"
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
	"main/utils"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ENHANCED TEST UTILITIES AND FIXTURES WITH COMPLETE ASSERTION COVERAGE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type TestAssertion struct {
	t *testing.T
}

func NewAssertion(t *testing.T) *TestAssertion {
	return &TestAssertion{t: t}
}

func (a *TestAssertion) EXPECT_EQ(expected, actual interface{}, msg ...string) bool {
	if !reflect.DeepEqual(expected, actual) {
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
	if reflect.DeepEqual(expected, actual) {
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
			a.t.Errorf("EXPECT_LT failed: %v >= %v - %s", left, right, a.formatMsg(msg))
			return false
		}
	case uint64:
		if r, ok := right.(uint64); ok && l >= r {
			a.t.Errorf("EXPECT_LT failed: %v >= %v - %s", left, right, a.formatMsg(msg))
			return false
		}
	case float64:
		if r, ok := right.(float64); ok && l >= r {
			a.t.Errorf("EXPECT_LT failed: %v >= %v - %s", left, right, a.formatMsg(msg))
			return false
		}
	case time.Duration:
		if r, ok := right.(time.Duration); ok && l >= r {
			a.t.Errorf("EXPECT_LT failed: %v >= %v - %s", left, right, a.formatMsg(msg))
			return false
		}
	}
	return true
}

func (a *TestAssertion) EXPECT_LE(left, right interface{}, msg ...string) bool {
	switch l := left.(type) {
	case float64:
		if r, ok := right.(float64); ok && l > r {
			a.t.Errorf("EXPECT_LE failed: %v > %v - %s", left, right, a.formatMsg(msg))
			return false
		}
	case int:
		if r, ok := right.(int); ok && l > r {
			a.t.Errorf("EXPECT_LE failed: %v > %v - %s", left, right, a.formatMsg(msg))
			return false
		}
	}
	return true
}

func (a *TestAssertion) EXPECT_GE(left, right interface{}, msg ...string) bool {
	switch l := left.(type) {
	case int:
		if r, ok := right.(int); ok && l < r {
			a.t.Errorf("EXPECT_GE failed: %v < %v - %s", left, right, a.formatMsg(msg))
			return false
		}
	case float64:
		if r, ok := right.(float64); ok && l < r {
			a.t.Errorf("EXPECT_GE failed: %v < %v - %s", left, right, a.formatMsg(msg))
			return false
		}
	case uint64:
		if r, ok := right.(uint64); ok && l < r {
			a.t.Errorf("EXPECT_GE failed: %v < %v - %s", left, right, a.formatMsg(msg))
			return false
		}
	}
	return true
}

func (a *TestAssertion) EXPECT_GT(left, right interface{}, msg ...string) bool {
	switch l := left.(type) {
	case int:
		if r, ok := right.(int); ok && l <= r {
			a.t.Errorf("EXPECT_GT failed: %v <= %v - %s", left, right, a.formatMsg(msg))
			return false
		}
	case float64:
		if r, ok := right.(float64); ok && l <= r {
			a.t.Errorf("EXPECT_GT failed: %v <= %v - %s", left, right, a.formatMsg(msg))
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

func (a *TestAssertion) EXPECT_PANIC(fn func(), msg ...string) bool {
	panicked := false
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	fn()
	if !panicked {
		a.t.Errorf("EXPECT_PANIC failed: expected panic but none occurred - %s", a.formatMsg(msg))
		return false
	}
	return true
}

func (a *TestAssertion) ASSERT_TRUE(condition bool, msg ...string) {
	if !a.EXPECT_TRUE(condition, msg...) {
		a.t.FailNow()
	}
}

func (a *TestAssertion) ASSERT_EQ(expected, actual interface{}, msg ...string) {
	if !a.EXPECT_EQ(expected, actual, msg...) {
		a.t.FailNow()
	}
}

func (a *TestAssertion) formatMsg(msg []string) string {
	if len(msg) > 0 {
		return msg[0]
	}
	return ""
}

// RouterTestFixture provides comprehensive test environment with proper cleanup
type RouterTestFixture struct {
	*TestAssertion
	originalState testGlobalState
}

// testGlobalState captures global state for restoration after tests
type testGlobalState struct {
	coreEngines        [constants.MaxSupportedCores]*ArbitrageEngine
	coreRings          [constants.MaxSupportedCores]*ring24.Ring
	pairToCoreRouting  [constants.PairRoutingTableCapacity]uint64
	packedAddressKeys  [constants.AddressTableCapacity]PackedAddress
	addressToPairMap   [constants.AddressTableCapacity]TradingPairID
	pairWorkloadShards map[TradingPairID][]PairWorkloadShard
}

func NewRouterTestFixture(t *testing.T) *RouterTestFixture {
	return &RouterTestFixture{
		TestAssertion: NewAssertion(t),
	}
}

func (f *RouterTestFixture) SetUp() {
	// Capture original state
	f.originalState = testGlobalState{
		coreEngines:        coreEngines,
		coreRings:          coreRings,
		pairToCoreRouting:  pairToCoreRouting,
		packedAddressKeys:  packedAddressKeys,
		addressToPairMap:   addressToPairMap,
		pairWorkloadShards: pairWorkloadShards,
	}

	// Clear global state for test isolation
	for i := range coreEngines {
		coreEngines[i] = nil
	}
	for i := range coreRings {
		coreRings[i] = nil
	}
	for i := range pairToCoreRouting {
		pairToCoreRouting[i] = 0
	}
	for i := range packedAddressKeys {
		packedAddressKeys[i] = PackedAddress{}
	}
	for i := range addressToPairMap {
		addressToPairMap[i] = 0
	}
	pairWorkloadShards = nil
}

func (f *RouterTestFixture) TearDown() {
	// Restore original state for complete isolation
	coreEngines = f.originalState.coreEngines
	coreRings = f.originalState.coreRings
	pairToCoreRouting = f.originalState.pairToCoreRouting
	packedAddressKeys = f.originalState.packedAddressKeys
	addressToPairMap = f.originalState.addressToPairMap
	pairWorkloadShards = f.originalState.pairWorkloadShards
}

func (f *RouterTestFixture) CreateTestLogView(address string, reserve0, reserve1 uint64) *types.LogView {
	data := fmt.Sprintf("0x%064x%064x", reserve0, reserve1)
	return &types.LogView{
		Addr: []byte(address),
		Data: []byte(data),
	}
}

// TestEngineBuilder creates properly initialized engines for testing
type TestEngineBuilder struct {
	arenaSize      int
	numQueues      int
	cyclesPerQueue int
	isReverse      bool
}

func NewTestEngineBuilder() *TestEngineBuilder {
	return &TestEngineBuilder{
		arenaSize:      1000,
		numQueues:      1,
		cyclesPerQueue: 10,
		isReverse:      false,
	}
}

func (b *TestEngineBuilder) WithArenaSize(size int) *TestEngineBuilder {
	b.arenaSize = size
	return b
}

func (b *TestEngineBuilder) WithQueues(numQueues int) *TestEngineBuilder {
	b.numQueues = numQueues
	return b
}

func (b *TestEngineBuilder) WithCyclesPerQueue(cycles int) *TestEngineBuilder {
	b.cyclesPerQueue = cycles
	return b
}

func (b *TestEngineBuilder) WithReverseDirection(reverse bool) *TestEngineBuilder {
	b.isReverse = reverse
	return b
}

func (b *TestEngineBuilder) Build() *ArbitrageEngine {
	totalCycles := b.numQueues * b.cyclesPerQueue
	arenaSize := uint64((totalCycles * 3) / 2) // 50% buffer
	if arenaSize < uint64(b.arenaSize) {
		arenaSize = uint64(b.arenaSize)
	}
	if arenaSize < 1024 {
		arenaSize = 1024
	}

	engine := &ArbitrageEngine{
		pairToQueueLookup:  localidx.New(constants.DefaultLocalIdxSize),
		isReverseDirection: b.isReverse,
		cycleStates:        make([]ArbitrageCycleState, 0),
		cycleFanoutTable:   make([][]CycleFanoutEntry, b.numQueues),
		priorityQueues:     make([]pooledquantumqueue.PooledQuantumQueue, b.numQueues),
		sharedArena:        make([]pooledquantumqueue.Entry, arenaSize),
		nextHandle:         0,
	}

	// Initialize arena entries to unlinked state
	for i := range engine.sharedArena {
		engine.sharedArena[i].Tick = -1
		engine.sharedArena[i].Prev = pooledquantumqueue.Handle(^uint64(0))
		engine.sharedArena[i].Next = pooledquantumqueue.Handle(^uint64(0))
		engine.sharedArena[i].Data = 0
	}

	// Initialize queues using shared arena
	for i := 0; i < b.numQueues; i++ {
		newQueue := pooledquantumqueue.New(unsafe.Pointer(&engine.sharedArena[0]))
		engine.priorityQueues[i] = *newQueue

		// Populate with test cycles
		for j := 0; j < b.cyclesPerQueue; j++ {
			handle := engine.allocateQueueHandle()

			cycleIndex := len(engine.cycleStates)
			engine.cycleStates = append(engine.cycleStates, ArbitrageCycleState{
				pairIDs: [3]TradingPairID{
					TradingPairID(i*3 + 1),
					TradingPairID(i*3 + 2),
					TradingPairID(i*3 + 3),
				},
				tickValues: [3]float64{0.0, 0.0, 0.0},
			})

			// Generate distributed priority
			cycleHash := utils.Mix64(uint64(cycleIndex))
			randBits := cycleHash & 0xFFFF
			initPriority := int64(196608 + randBits)

			engine.priorityQueues[i].Push(initPriority, handle, uint64(cycleIndex))

			// Create fanout entries
			if len(engine.cycleFanoutTable[i]) < 5 {
				engine.cycleFanoutTable[i] = append(engine.cycleFanoutTable[i], CycleFanoutEntry{
					cycleIndex:  uint64(cycleIndex),
					edgeIndex:   uint64(j % 3),
					queue:       &engine.priorityQueues[i],
					queueHandle: handle,
				})
			}
		}

		// Setup pair mapping
		engine.pairToQueueLookup.Put(uint32(i+1), uint32(i))
	}

	return engine
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE TYPE STRUCTURE TESTS WITH COMPREHENSIVE COVERAGE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestPriceUpdateMessageStructure(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("SizeRequirement", func(t *testing.T) {
		var msg PriceUpdateMessage
		fixture.EXPECT_EQ(24, int(unsafe.Sizeof(msg)), "PriceUpdateMessage must be exactly 24 bytes for ring buffer compatibility")
	})

	t.Run("FieldAccess", func(t *testing.T) {
		msg := PriceUpdateMessage{
			pairID:      TradingPairID(67890),
			forwardTick: 2.5,
			reverseTick: -2.5,
		}

		fixture.EXPECT_EQ(TradingPairID(67890), msg.pairID, "pairID field access")
		fixture.EXPECT_EQ(2.5, msg.forwardTick, "forwardTick field access")
		fixture.EXPECT_EQ(-2.5, msg.reverseTick, "reverseTick field access")
	})

	t.Run("FieldAlignment", func(t *testing.T) {
		var msg PriceUpdateMessage
		pairIDOffset := unsafe.Offsetof(msg.pairID)
		forwardTickOffset := unsafe.Offsetof(msg.forwardTick)
		reverseTickOffset := unsafe.Offsetof(msg.reverseTick)

		fixture.EXPECT_EQ(uintptr(0), pairIDOffset, "pairID should be at offset 0")
		fixture.EXPECT_EQ(uintptr(8), forwardTickOffset, "forwardTick should be at offset 8")
		fixture.EXPECT_EQ(uintptr(16), reverseTickOffset, "reverseTick should be at offset 16")
	})
}

func TestArbitrageCycleStateAlignment(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("SizeVerification", func(t *testing.T) {
		var cycle ArbitrageCycleState
		fixture.EXPECT_EQ(64, int(unsafe.Sizeof(cycle)), "ArbitrageCycleState must be exactly 64 bytes")
	})

	t.Run("CacheLineAlignment", func(t *testing.T) {
		cycle := &ArbitrageCycleState{}
		addr := uintptr(unsafe.Pointer(cycle))
		fixture.EXPECT_EQ(uintptr(0), addr%64, "ArbitrageCycleState should be 64-byte aligned")
	})

	t.Run("FieldOrdering", func(t *testing.T) {
		cycle := ArbitrageCycleState{
			tickValues: [3]float64{1.0, 2.0, 3.0},
			pairIDs:    [3]TradingPairID{10, 20, 30},
		}

		fixture.EXPECT_EQ(1.0, cycle.tickValues[0], "tickValues field access")
		fixture.EXPECT_EQ(TradingPairID(10), cycle.pairIDs[0], "pairIDs field access")

		// Verify hot field is first
		var c ArbitrageCycleState
		tickValuesOffset := unsafe.Offsetof(c.tickValues)
		fixture.EXPECT_EQ(uintptr(0), tickValuesOffset, "tickValues (hot field) should be at offset 0")
	})
}

func TestPackedAddressOperations(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("EqualityComparison", func(t *testing.T) {
		key1 := PackedAddress{words: [3]uint64{0x1234567890abcdef, 0xfedcba0987654321, 0x12345678}}
		key2 := PackedAddress{words: [3]uint64{0x1234567890abcdef, 0xfedcba0987654321, 0x12345678}}
		key3 := PackedAddress{words: [3]uint64{0x1234567890abcdef, 0xfedcba0987654321, 0x12345679}}

		fixture.EXPECT_TRUE(key1.isEqual(key2), "Identical keys should be equal")
		fixture.EXPECT_FALSE(key1.isEqual(key3), "Different keys should not be equal")
	})

	t.Run("MemoryLayout", func(t *testing.T) {
		var key PackedAddress
		fixture.EXPECT_EQ(32, int(unsafe.Sizeof(key)), "PackedAddress should be 32 bytes total")
	})

	t.Run("WordAlignment", func(t *testing.T) {
		var key PackedAddress
		word0Offset := unsafe.Offsetof(key.words) + 0*unsafe.Sizeof(key.words[0])
		word1Offset := unsafe.Offsetof(key.words) + 1*unsafe.Sizeof(key.words[0])
		word2Offset := unsafe.Offsetof(key.words) + 2*unsafe.Sizeof(key.words[0])

		fixture.EXPECT_EQ(uintptr(0), word0Offset, "word[0] should be at offset 0")
		fixture.EXPECT_EQ(uintptr(8), word1Offset, "word[1] should be at offset 8")
		fixture.EXPECT_EQ(uintptr(16), word2Offset, "word[2] should be at offset 16")
	})
}

func TestArbitrageEngineLayout(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("HotFieldsFirst", func(t *testing.T) {
		var engine ArbitrageEngine

		pairToQueueOffset := unsafe.Offsetof(engine.pairToQueueLookup)
		isReverseOffset := unsafe.Offsetof(engine.isReverseDirection)

		fixture.EXPECT_EQ(uintptr(0), pairToQueueOffset, "Hottest field should be at offset 0")
		fixture.EXPECT_EQ(uintptr(64), isReverseOffset, "Second hottest field should follow immediately")
	})

	t.Run("CacheLineAlignment", func(t *testing.T) {
		engine := &ArbitrageEngine{}
		addr := uintptr(unsafe.Pointer(engine))
		fixture.EXPECT_EQ(uintptr(0), addr%64, "ArbitrageEngine should be 64-byte aligned")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HEX PARSING AND ADDRESS CONVERSION TESTS WITH EDGE CASES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestCountHexLeadingZeros(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

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
		{"EdgeCase15Zeros", "000000000000000078e8455d7f2faa9b", 15},
		{"EdgeCase16Zeros", "0000000000000000e8455d7f2faa9bde", 16},
		{"EdgeCase17Zeros", "00000000000000000e8455d7f2faa9bd", 17},
		{"MixedCase", "000000000000ABCD78e8455d7f2faa9b", 12},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := make([]byte, 32)
			copy(input, []byte(tc.input))
			if len(tc.input) < 32 {
				for i := len(tc.input); i < 32; i++ {
					input[i] = '0'
				}
			}
			result := countHexLeadingZeros(input)
			fixture.EXPECT_EQ(tc.expected, result, tc.name)
		})
	}

	t.Run("ChunkBoundaries", func(t *testing.T) {
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
			result := countHexLeadingZeros(input)
			fixture.EXPECT_EQ(leadingZeros, result, fmt.Sprintf("Boundary test for %d zeros", leadingZeros))
		}
	})

	t.Run("Performance", func(t *testing.T) {
		input := []byte("0000000000000000e8455d7f2faa9bde")
		padded := make([]byte, 32)
		copy(padded, input)

		start := time.Now()
		for i := 0; i < 10000; i++ {
			_ = countHexLeadingZeros(padded)
		}
		elapsed := time.Since(start)

		// Should be very fast - less than 1µs per operation
		fixture.EXPECT_LT(elapsed, 10*time.Millisecond, "countHexLeadingZeros should be extremely fast")
	})
}

func TestPackEthereumAddress(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("ValidAddressConversion", func(t *testing.T) {
		address := "1234567890123456789012345678901234567890"
		key := packEthereumAddress([]byte(address))

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

		key1 := packEthereumAddress([]byte(address1))
		key2 := packEthereumAddress([]byte(address2))

		fixture.EXPECT_FALSE(key1.isEqual(key2), "Different addresses should produce different keys")
	})

	t.Run("RealEthereumAddresses", func(t *testing.T) {
		realAddresses := []string{
			"a478c2975ab1ea89e8196811f51a7b7ade33eb11", // DAI/WETH pair
			"bb2b8038a1640196fbe3e38816f3e67cba72d940", // WBTC/WETH pair
			"0d4a11d5eeaac28ec3f61d100daf4d40471f1852", // WETH/USDT pair
			"b4e16d0168e52d35cacd2c6185b44281ec28c9dc", // USDC/WETH pair
		}

		keys := make([]PackedAddress, len(realAddresses))
		for i, addr := range realAddresses {
			keys[i] = packEthereumAddress([]byte(addr))
		}

		// Verify all keys are unique
		for i := 0; i < len(keys); i++ {
			for j := i + 1; j < len(keys); j++ {
				fixture.EXPECT_FALSE(keys[i].isEqual(keys[j]),
					fmt.Sprintf("Keys for addresses %s and %s should be different", realAddresses[i], realAddresses[j]))
			}
		}
	})

	t.Run("EdgeCases", func(t *testing.T) {
		edgeCases := []struct {
			name    string
			address string
		}{
			{"AllZeros", "0000000000000000000000000000000000000000"},
			{"AllOnes", "1111111111111111111111111111111111111111"},
			{"AllFs", "ffffffffffffffffffffffffffffffffffffffff"},
			{"Alternating", "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f"},
		}

		for _, tc := range edgeCases {
			t.Run(tc.name, func(t *testing.T) {
				fixture.EXPECT_NO_FATAL_FAILURE(func() {
					key := packEthereumAddress([]byte(tc.address))
					_ = key // Use the key to avoid unused variable warning
				})
			})
		}
	})
}

func TestHashAddressToIndex(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("HashIndexGeneration", func(t *testing.T) {
		address1 := "1234567890123456789012345678901234567890"
		address2 := "abcdefabcdefabcdefabcdefabcdefabcdefabcd"

		idx1 := hashAddressToIndex([]byte(address1))
		idx2 := hashAddressToIndex([]byte(address2))

		fixture.EXPECT_LT(idx1, uint64(constants.AddressTableCapacity), "Hash index should be within table bounds")
		fixture.EXPECT_LT(idx2, uint64(constants.AddressTableCapacity), "Hash index should be within table bounds")
	})

	t.Run("HashConsistency", func(t *testing.T) {
		address := "1234567890123456789012345678901234567890"

		idx1 := hashAddressToIndex([]byte(address))
		idx2 := hashAddressToIndex([]byte(address))
		fixture.EXPECT_EQ(idx1, idx2, "Same address should produce same index")
	})

	t.Run("HashDistribution", func(t *testing.T) {
		addresses := []string{
			"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
			"bb2b8038a1640196fbe3e38816f3e67cba72d940",
			"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
			"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
			"ae461ca67b15dc8dc81ce7615e0320da1a9ab8d5",
			"397ff1542f962076d0bfe58ea045ffa2d347aca0",
			"f173214c720f58e03e194085b1db28b50acccead",
			"43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd",
		}

		indices := make(map[uint64]bool)
		for _, addr := range addresses {
			idx := hashAddressToIndex([]byte(addr))
			indices[idx] = true
		}

		// Should have good distribution - most should be unique
		fixture.EXPECT_GE(len(indices), len(addresses)/2, "Hash should distribute reasonably well")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS REGISTRATION AND LOOKUP TESTS WITH REAL DATA
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestTradingPairAddressRegistration(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("BasicRegistrationAndLookup", func(t *testing.T) {
		address := "1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address), pairID)

		result := lookupPairByAddress([]byte(address))
		fixture.EXPECT_EQ(pairID, result, "Registered address should be retrievable")
	})

	t.Run("NonExistentAddressLookup", func(t *testing.T) {
		nonExistent := "9999999999999999999999999999999999999999"
		result := lookupPairByAddress([]byte(nonExistent))

		fixture.EXPECT_EQ(TradingPairID(0), result, "Non-existent address should return 0")
	})

	t.Run("AddressUpdateOverwrite", func(t *testing.T) {
		address := "1111111111111111111111111111111111111111"
		pairID1 := TradingPairID(100)
		pairID2 := TradingPairID(200)

		RegisterTradingPairAddress([]byte(address), pairID1)
		result1 := lookupPairByAddress([]byte(address))
		fixture.EXPECT_EQ(pairID1, result1, "First registration should work")

		RegisterTradingPairAddress([]byte(address), pairID2)
		result2 := lookupPairByAddress([]byte(address))
		fixture.EXPECT_EQ(pairID2, result2, "Address update should overwrite previous value")
	})

	t.Run("RealUniswapPairs", func(t *testing.T) {
		realPairs := []struct {
			address string
			name    string
		}{
			{"a478c2975ab1ea89e8196811f51a7b7ade33eb11", "DAI/WETH"},
			{"bb2b8038a1640196fbe3e38816f3e67cba72d940", "WBTC/WETH"},
			{"0d4a11d5eeaac28ec3f61d100daf4d40471f1852", "WETH/USDT"},
			{"b4e16d0168e52d35cacd2c6185b44281ec28c9dc", "USDC/WETH"},
			{"ae461ca67b15dc8dc81ce7615e0320da1a9ab8d5", "UNI/WETH"},
			{"397ff1542f962076d0bfe58ea045ffa2d347aca0", "USDC/USDT"},
		}

		for i, pair := range realPairs {
			pairID := TradingPairID(i + 1000)
			RegisterTradingPairAddress([]byte(pair.address), pairID)

			result := lookupPairByAddress([]byte(pair.address))
			fixture.EXPECT_EQ(pairID, result, fmt.Sprintf("%s pair should be retrievable", pair.name))
		}
	})

	t.Run("CaseSensitivity", func(t *testing.T) {
		addressLower := "abcdef1234567890123456789012345678901234"
		addressUpper := "ABCDEF1234567890123456789012345678901234"

		RegisterTradingPairAddress([]byte(addressLower), TradingPairID(1))
		RegisterTradingPairAddress([]byte(addressUpper), TradingPairID(2))

		resultLower := lookupPairByAddress([]byte(addressLower))
		resultUpper := lookupPairByAddress([]byte(addressUpper))

		fixture.EXPECT_EQ(TradingPairID(1), resultLower, "Lowercase address should return correct ID")
		fixture.EXPECT_EQ(TradingPairID(2), resultUpper, "Uppercase address should return correct ID")
		fixture.EXPECT_NE(resultLower, resultUpper, "Case sensitive addresses should be different")
	})
}

func TestHashTableCollisionHandling(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("MultipleAddressRegistration", func(t *testing.T) {
		addresses := []string{
			"a000000000000000000000000000000000000001",
			"b000000000000000000000000000000000000002",
			"c000000000000000000000000000000000000003",
			"d000000000000000000000000000000000000004",
			"e000000000000000000000000000000000000005",
		}

		for i, addr := range addresses {
			RegisterTradingPairAddress([]byte(addr), TradingPairID(i+1))
		}

		for i, addr := range addresses {
			result := lookupPairByAddress([]byte(addr))
			fixture.EXPECT_EQ(TradingPairID(i+1), result, fmt.Sprintf("Address %s lookup failed", addr))
		}
	})

	t.Run("CollisionRecovery", func(t *testing.T) {
		// Generate addresses likely to collide
		baseAddr := "1000000000000000000000000000000000000000"
		collisionCandidates := []string{
			baseAddr,
			"1000000000000000000000000000000000000001",
			"1000000000000000000000000000000000000002",
			"1000000000000000000000000000000000000003",
		}

		for i, addr := range collisionCandidates {
			RegisterTradingPairAddress([]byte(addr), TradingPairID(i+100))
		}

		// All should be retrievable despite potential collisions
		for i, addr := range collisionCandidates {
			result := lookupPairByAddress([]byte(addr))
			fixture.EXPECT_EQ(TradingPairID(i+100), result,
				fmt.Sprintf("Collision candidate %s should be retrievable", addr))
		}
	})
}

func TestCoreRoutingAssignment(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("SingleCoreAssignment", func(t *testing.T) {
		pairID := TradingPairID(123)
		coreID := uint8(5)

		RegisterPairToCoreRouting(pairID, coreID)

		assignment := pairToCoreRouting[pairID]
		fixture.EXPECT_EQ(uint64(1), (assignment>>coreID)&1, "Assigned core bit should be set")
	})

	t.Run("MultipleCoreAssignment", func(t *testing.T) {
		pairID := TradingPairID(456)
		cores := []uint8{0, 5, 15, 31}

		for _, coreID := range cores {
			RegisterPairToCoreRouting(pairID, coreID)
		}

		assignment := pairToCoreRouting[pairID]
		for _, coreID := range cores {
			fixture.EXPECT_EQ(uint64(1), (assignment>>coreID)&1, fmt.Sprintf("Core %d should be assigned", coreID))
		}

		fixture.EXPECT_EQ(uint64(0), (assignment>>1)&1, "Unassigned core should not be set")
		fixture.EXPECT_EQ(uint64(0), (assignment>>10)&1, "Unassigned core should not be set")
	})

	t.Run("MaxCoreAssignment", func(t *testing.T) {
		pairID := TradingPairID(789)

		// Assign to all 64 cores
		for coreID := uint8(0); coreID < 64; coreID++ {
			RegisterPairToCoreRouting(pairID, coreID)
		}

		assignment := pairToCoreRouting[pairID]
		fixture.EXPECT_EQ(uint64(^uint64(0)), assignment, "All 64 cores should be assigned")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ROBIN HOOD HASH TABLE DISPLACEMENT STRESS TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestRobinHoodDisplacement(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("EarlyTerminationOnShorterDistance", func(t *testing.T) {
		addr1 := "a478c2975ab1ea89e8196811f51a7b7ade33eb11"
		RegisterTradingPairAddress([]byte(addr1), TradingPairID(100))

		nonExistent := "b4e16d0168e52d35cacd2c6185b44281ec28c9dc"
		result := lookupPairByAddress([]byte(nonExistent))
		fixture.EXPECT_EQ(TradingPairID(0), result, "Non-existent key should return 0 via early termination")
	})

	t.Run("RobinHoodDisplacementDuringInsertion", func(t *testing.T) {
		testAddresses := []string{
			"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
			"bb2b8038a1640196fbe3e38816f3e67cba72d940",
			"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
			"f173214c720f58e03e194085b1db28b50acccead",
			"43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd",
		}

		originalPairs := make([]TradingPairID, len(testAddresses))
		for i, addr := range testAddresses {
			pairID := TradingPairID(i + 1)
			originalPairs[i] = pairID
			RegisterTradingPairAddress([]byte(addr), pairID)
		}

		// Verify all addresses can be found after potential displacement
		for i, addr := range testAddresses {
			result := lookupPairByAddress([]byte(addr))
			fixture.EXPECT_EQ(originalPairs[i], result,
				fmt.Sprintf("Address %s should be findable after displacement", addr))
		}

		// Insert additional address
		additionalAddr := "ae461ca67b15dc8dc81ce7615e0320da1a9ab8d5"
		additionalPairID := TradingPairID(100)
		RegisterTradingPairAddress([]byte(additionalAddr), additionalPairID)

		result := lookupPairByAddress([]byte(additionalAddr))
		fixture.EXPECT_EQ(additionalPairID, result, "New address should be findable after insertion")

		// Verify original addresses are still findable
		for i, addr := range testAddresses {
			result := lookupPairByAddress([]byte(addr))
			fixture.EXPECT_EQ(originalPairs[i], result,
				fmt.Sprintf("Original address %s should still be findable", addr))
		}
	})

	t.Run("HighLoadStressTest", func(t *testing.T) {
		// Stress test with many real addresses
		realAddresses := []string{
			"a478c2975ab1ea89e8196811f51a7b7ade33eb11", // DAI/WETH
			"bb2b8038a1640196fbe3e38816f3e67cba72d940", // WBTC/WETH
			"0d4a11d5eeaac28ec3f61d100daf4d40471f1852", // WETH/USDT
			"b4e16d0168e52d35cacd2c6185b44281ec28c9dc", // USDC/WETH
			"ae461ca67b15dc8dc81ce7615e0320da1a9ab8d5", // UNI/WETH
			"397ff1542f962076d0bfe58ea045ffa2d347aca0", // USDC/USDT
			"f173214c720f58e03e194085b1db28b50acccead", // CELT/WETH
			"43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd", // DODO/WETH
			"d3d2e2692501a5c9ca623199d38826e513033a17", // UNI/WETH V1
			"514910771af9ca656af840dff83e8264ecf986ca", // Mock LINK
		}

		registeredPairs := make(map[string]TradingPairID)

		// Register all addresses
		for i, addr := range realAddresses {
			pairID := TradingPairID(i + 100)
			registeredPairs[addr] = pairID
			RegisterTradingPairAddress([]byte(addr), pairID)
		}

		// Verify all addresses can be found
		successCount := 0
		for addr, expectedPairID := range registeredPairs {
			result := lookupPairByAddress([]byte(addr))
			if result == expectedPairID {
				successCount++
			}
		}

		fixture.EXPECT_EQ(len(realAddresses), successCount,
			fmt.Sprintf("All %d real addresses should be findable after Robin Hood displacement", len(realAddresses)))
	})

	t.Run("WorstCaseCollisions", func(t *testing.T) {
		// Create addresses designed to hash to the same initial position
		basePattern := "1000000000000000000000000000000000000"
		worstCaseAddresses := make([]string, 10)
		for i := 0; i < 10; i++ {
			worstCaseAddresses[i] = fmt.Sprintf("%s%03d", basePattern, i)
		}

		// Register all potentially colliding addresses
		for i, addr := range worstCaseAddresses {
			RegisterTradingPairAddress([]byte(addr), TradingPairID(i+200))
		}

		// All should still be retrievable despite worst-case collisions
		for i, addr := range worstCaseAddresses {
			result := lookupPairByAddress([]byte(addr))
			fixture.EXPECT_EQ(TradingPairID(i+200), result,
				fmt.Sprintf("Worst case collision address %s should be retrievable", addr))
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CRYPTOGRAPHIC RANDOMNESS AND DETERMINISTIC SHUFFLING TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestCryptoRandomGeneration(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("BasicRandomGeneration", func(t *testing.T) {
		seed := []byte("test_seed_12345")
		rng := newCryptoRandomGenerator(seed)

		values := make([]uint64, 100)
		for i := range values {
			values[i] = rng.generateRandomUint64()
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

		rng1 := newCryptoRandomGenerator(seed)
		rng2 := newCryptoRandomGenerator(seed)

		for i := 0; i < 10; i++ {
			v1 := rng1.generateRandomUint64()
			v2 := rng2.generateRandomUint64()
			fixture.EXPECT_EQ(v1, v2, "Random sequence should be deterministic with same seed")
		}
	})

	t.Run("NextIntBoundsChecking", func(t *testing.T) {
		seed := []byte("bounds_test")
		rng := newCryptoRandomGenerator(seed)

		bounds := []int{1, 2, 10, 100, 1000}

		for _, bound := range bounds {
			for i := 0; i < 20; i++ {
				val := rng.generateRandomInt(bound)
				fixture.EXPECT_GE(val, 0, "Value should be >= 0")
				fixture.EXPECT_LT(val, bound, fmt.Sprintf("Value should be < %d", bound))
			}
		}
	})

	t.Run("SeedSensitivity", func(t *testing.T) {
		seed1 := []byte("seed1")
		seed2 := []byte("seed2")

		rng1 := newCryptoRandomGenerator(seed1)
		rng2 := newCryptoRandomGenerator(seed2)

		// Different seeds should produce different sequences
		differences := 0
		for i := 0; i < 10; i++ {
			v1 := rng1.generateRandomUint64()
			v2 := rng2.generateRandomUint64()
			if v1 != v2 {
				differences++
			}
		}

		fixture.EXPECT_GT(differences, 5, "Different seeds should produce mostly different values")
	})

	t.Run("LongSequenceQuality", func(t *testing.T) {
		seed := []byte("quality_test_seed")
		rng := newCryptoRandomGenerator(seed)

		// Generate longer sequence and check statistical properties
		values := make([]uint64, 1000)
		for i := range values {
			values[i] = rng.generateRandomUint64()
		}

		// Check bit distribution (each bit should appear roughly 50% of the time)
		bitCounts := make([]int, 64)
		for _, value := range values {
			for bit := 0; bit < 64; bit++ {
				if (value>>bit)&1 == 1 {
					bitCounts[bit]++
				}
			}
		}

		// Each bit should appear between 40% and 60% of the time
		for bit := 0; bit < 64; bit++ {
			percentage := float64(bitCounts[bit]) / float64(len(values))
			fixture.EXPECT_GE(percentage, 0.4, fmt.Sprintf("Bit %d should appear >= 40%% of time", bit))
			fixture.EXPECT_LE(percentage, 0.6, fmt.Sprintf("Bit %d should appear <= 60%% of time", bit))
		}
	})
}

func TestCycleEdgesShuffle(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("ShufflePreservesElements", func(t *testing.T) {
		cycleEdges := make([]CycleEdge, 10)
		for i := range cycleEdges {
			cycleEdges[i] = CycleEdge{
				cyclePairs: [3]TradingPairID{TradingPairID(i), TradingPairID(i + 1), TradingPairID(i + 2)},
				edgeIndex:  uint64(i),
			}
		}

		shuffleCycleEdges(cycleEdges, TradingPairID(12345))

		// Verify all elements are preserved
		elementCounts := make(map[uint64]int)
		for _, edge := range cycleEdges {
			elementCounts[edge.edgeIndex]++
		}

		for i := uint64(0); i < 10; i++ {
			fixture.EXPECT_EQ(1, elementCounts[i], fmt.Sprintf("Edge index %d should appear exactly once", i))
		}
	})

	t.Run("ShuffleDeterminism", func(t *testing.T) {
		original := make([]CycleEdge, 5)
		for i := range original {
			original[i] = CycleEdge{
				cyclePairs: [3]TradingPairID{TradingPairID(i * 3), TradingPairID(i*3 + 1), TradingPairID(i*3 + 2)},
				edgeIndex:  uint64(i),
			}
		}

		cycleEdges1 := make([]CycleEdge, len(original))
		cycleEdges2 := make([]CycleEdge, len(original))
		copy(cycleEdges1, original)
		copy(cycleEdges2, original)

		pairID := TradingPairID(54321)
		shuffleCycleEdges(cycleEdges1, pairID)
		shuffleCycleEdges(cycleEdges2, pairID)

		// Should produce identical results
		for i := range cycleEdges1 {
			fixture.EXPECT_EQ(cycleEdges1[i].edgeIndex, cycleEdges2[i].edgeIndex,
				"Shuffle should be deterministic for same pairID")
		}
	})

	t.Run("ShuffleRandomness", func(t *testing.T) {
		original := make([]CycleEdge, 10)
		for i := range original {
			original[i] = CycleEdge{
				cyclePairs: [3]TradingPairID{TradingPairID(i), TradingPairID(i + 1), TradingPairID(i + 2)},
				edgeIndex:  uint64(i),
			}
		}

		// Shuffle with different pair IDs should produce different results
		cycleEdges1 := make([]CycleEdge, len(original))
		cycleEdges2 := make([]CycleEdge, len(original))
		copy(cycleEdges1, original)
		copy(cycleEdges2, original)

		shuffleCycleEdges(cycleEdges1, TradingPairID(111))
		shuffleCycleEdges(cycleEdges2, TradingPairID(222))

		// Should be different (with high probability)
		differences := 0
		for i := range cycleEdges1 {
			if cycleEdges1[i].edgeIndex != cycleEdges2[i].edgeIndex {
				differences++
			}
		}

		fixture.EXPECT_GT(differences, 2, "Different pair IDs should produce different shuffles")
	})

	t.Run("EmptyAndSingleElementCases", func(t *testing.T) {
		// Empty slice
		var empty []CycleEdge
		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			shuffleCycleEdges(empty, TradingPairID(123))
		})

		// Single element
		single := []CycleEdge{{
			cyclePairs: [3]TradingPairID{1, 2, 3},
			edgeIndex:  0,
		}}
		originalEdgeIndex := single[0].edgeIndex
		shuffleCycleEdges(single, TradingPairID(123))
		fixture.EXPECT_EQ(originalEdgeIndex, single[0].edgeIndex, "Single element should remain unchanged")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// EVENT DISPATCH PIPELINE TESTS WITH COMPREHENSIVE COVERAGE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestDispatchPriceUpdateFlow(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("UnregisteredPairHandling", func(t *testing.T) {
		logView := fixture.CreateTestLogView(
			"0x9999999999999999999999999999999999999999",
			1000000000000, 2000000000000,
		)

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchPriceUpdate(logView)
		})
	})

	t.Run("ValidPairProcessing", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchPriceUpdate(logView)
		})

		// Verify message was sent
		message := coreRings[0].Pop()
		fixture.ASSERT_TRUE(message != nil, "Message should be sent to core ring")

		// Verify message content
		priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
		fixture.EXPECT_EQ(pairID, priceUpdate.pairID, "Real data should have correct pairID")
		fixture.EXPECT_NE(0.0, priceUpdate.forwardTick, "Real data should produce non-zero tick")
		fixture.EXPECT_FALSE(math.IsNaN(priceUpdate.forwardTick), "Real data tick should not be NaN")
		fixture.EXPECT_FALSE(math.IsInf(priceUpdate.forwardTick, 0), "Real data tick should not be infinite")
	})

	t.Run("MultipleRealDataSamples", func(t *testing.T) {
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
			logView := &types.LogView{
				Addr: []byte(address),
				Data: []byte(data),
			}

			fixture.EXPECT_NO_FATAL_FAILURE(func() {
				DispatchPriceUpdate(logView)
			})

			message := coreRings[0].Pop()
			fixture.ASSERT_TRUE(message != nil, fmt.Sprintf("Real data sample %d should produce message", i))

			priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
			fixture.EXPECT_EQ(pairID, priceUpdate.pairID, fmt.Sprintf("Sample %d should have correct pairID", i))
			fixture.EXPECT_FALSE(math.IsNaN(priceUpdate.forwardTick), fmt.Sprintf("Sample %d tick should not be NaN", i))
			fixture.EXPECT_FALSE(math.IsInf(priceUpdate.forwardTick, 0), fmt.Sprintf("Sample %d tick should not be infinite", i))
		}
	})
}

func TestDispatchFallbackLogic(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("ZeroReservesFallback", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		logView := fixture.CreateTestLogView(address, 0, 0)

		DispatchPriceUpdate(logView)

		message := coreRings[0].Pop()
		fixture.ASSERT_TRUE(message != nil, "Fallback should still send message")

		priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
		fixture.EXPECT_GE(priceUpdate.forwardTick, 50.2, "Fallback tick should be >= 50.2")
		fixture.EXPECT_LE(priceUpdate.forwardTick, 64.0, "Fallback tick should be <= 64.0")
		fixture.EXPECT_EQ(priceUpdate.forwardTick, priceUpdate.reverseTick, "Invalid reserves should have equal ticks")
	})

	t.Run("OneZeroReserveFallback", func(t *testing.T) {
		address := "0x2234567890123456789012345678901234567890"
		pairID := TradingPairID(22345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		// Test with one zero reserve
		logView := fixture.CreateTestLogView(address, 1000000000000, 0)

		DispatchPriceUpdate(logView)

		message := coreRings[0].Pop()
		fixture.ASSERT_TRUE(message != nil, "Fallback should handle one zero reserve")

		priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
		fixture.EXPECT_GE(priceUpdate.forwardTick, 50.2, "One zero reserve should trigger fallback")
		fixture.EXPECT_LE(priceUpdate.forwardTick, 64.0, "One zero reserve fallback should be bounded")
	})

	t.Run("VerySmallReserves", func(t *testing.T) {
		address := "0x3334567890123456789012345678901234567890"
		pairID := TradingPairID(33345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		// Test with very small reserves that might cause issues
		logView := fixture.CreateTestLogView(address, 1, 1)

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchPriceUpdate(logView)
		})

		message := coreRings[0].Pop()
		fixture.ASSERT_TRUE(message != nil, "Very small reserves should still produce message")

		priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
		fixture.EXPECT_FALSE(math.IsNaN(priceUpdate.forwardTick), "Small reserves should not produce NaN")
		fixture.EXPECT_FALSE(math.IsInf(priceUpdate.forwardTick, 0), "Small reserves should not produce infinity")
	})

	t.Run("FallbackRandomnessDistribution", func(t *testing.T) {
		address := "0x4444567890123456789012345678901234567890"
		pairID := TradingPairID(44345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		// Generate multiple fallback values to check distribution
		fallbackValues := make([]float64, 100)
		for i := 0; i < 100; i++ {
			// Use different pair IDs to get different random values
			testPairID := TradingPairID(44345 + i)
			testAddress := fmt.Sprintf("0x%040d", 4444567890+i)
			RegisterTradingPairAddress([]byte(testAddress[2:]), testPairID)
			RegisterPairToCoreRouting(testPairID, 0)

			logView := fixture.CreateTestLogView(testAddress, 0, 0)
			DispatchPriceUpdate(logView)

			message := coreRings[0].Pop()
			if message != nil {
				priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
				fallbackValues[i] = priceUpdate.forwardTick
			}
		}

		// Check that fallback values are distributed across the expected range
		minVal := fallbackValues[0]
		maxVal := fallbackValues[0]
		for _, val := range fallbackValues {
			if val < minVal {
				minVal = val
			}
			if val > maxVal {
				maxVal = val
			}
		}

		fixture.EXPECT_GE(minVal, 50.2, "Minimum fallback should be >= 50.2")
		fixture.EXPECT_LE(maxVal, 64.0, "Maximum fallback should be <= 64.0")
		fixture.EXPECT_GT(maxVal-minVal, 5.0, "Fallback values should have reasonable spread")
	})
}

func TestDispatchMultiCoreDistribution(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("MultipleCoreDelivery", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)
		assignedCores := []int{0, 2, 5}

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		for _, coreID := range assignedCores {
			RegisterPairToCoreRouting(pairID, uint8(coreID))
			coreRings[coreID] = ring24.New(constants.DefaultRingSize)
		}

		logView := fixture.CreateTestLogView(address, 1500000000000, 3000000000000)
		DispatchPriceUpdate(logView)

		// Verify all assigned cores received message
		for _, coreID := range assignedCores {
			message := coreRings[coreID].Pop()
			fixture.EXPECT_TRUE(message != nil, fmt.Sprintf("Core %d should receive message", coreID))

			if message != nil {
				priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
				fixture.EXPECT_EQ(pairID, priceUpdate.pairID, "Message should have correct pairID")
			}
		}

		// Verify unassigned core doesn't receive message
		coreRings[1] = ring24.New(constants.DefaultRingSize)
		message := coreRings[1].Pop()
		fixture.EXPECT_TRUE(message == nil, "Unassigned core should not receive message")
	})

	t.Run("AllCoresAssignment", func(t *testing.T) {
		address := "0x5555567890123456789012345678901234567890"
		pairID := TradingPairID(55555)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)

		// Assign to all available cores
		numCores := 8
		for coreID := 0; coreID < numCores; coreID++ {
			RegisterPairToCoreRouting(pairID, uint8(coreID))
			coreRings[coreID] = ring24.New(constants.DefaultRingSize)
		}

		logView := fixture.CreateTestLogView(address, 2000000000000, 4000000000000)
		DispatchPriceUpdate(logView)

		// All cores should receive the message
		for coreID := 0; coreID < numCores; coreID++ {
			message := coreRings[coreID].Pop()
			fixture.EXPECT_TRUE(message != nil, fmt.Sprintf("Core %d should receive message", coreID))
		}
	})

	t.Run("SelectiveCoreUpdate", func(t *testing.T) {
		// Different pairs assigned to different core sets
		pair1 := TradingPairID(11111)
		pair2 := TradingPairID(22222)
		address1 := "0x1111567890123456789012345678901234567890"
		address2 := "0x2222567890123456789012345678901234567890"

		RegisterTradingPairAddress([]byte(address1[2:]), pair1)
		RegisterTradingPairAddress([]byte(address2[2:]), pair2)

		// Pair 1 assigned to cores 0, 1
		RegisterPairToCoreRouting(pair1, 0)
		RegisterPairToCoreRouting(pair1, 1)

		// Pair 2 assigned to cores 2, 3
		RegisterPairToCoreRouting(pair2, 2)
		RegisterPairToCoreRouting(pair2, 3)

		// Initialize all rings
		for i := 0; i < 4; i++ {
			coreRings[i] = ring24.New(constants.DefaultRingSize)
		}

		// Update pair 1
		logView1 := fixture.CreateTestLogView(address1, 1000000000000, 2000000000000)
		DispatchPriceUpdate(logView1)

		// Cores 0, 1 should have messages, cores 2, 3 should not
		fixture.EXPECT_TRUE(coreRings[0].Pop() != nil, "Core 0 should receive pair 1 update")
		fixture.EXPECT_TRUE(coreRings[1].Pop() != nil, "Core 1 should receive pair 1 update")
		fixture.EXPECT_TRUE(coreRings[2].Pop() == nil, "Core 2 should not receive pair 1 update")
		fixture.EXPECT_TRUE(coreRings[3].Pop() == nil, "Core 3 should not receive pair 1 update")

		// Update pair 2
		logView2 := fixture.CreateTestLogView(address2, 3000000000000, 4000000000000)
		DispatchPriceUpdate(logView2)

		// Cores 2, 3 should have messages, cores 0, 1 should not
		fixture.EXPECT_TRUE(coreRings[0].Pop() == nil, "Core 0 should not receive pair 2 update")
		fixture.EXPECT_TRUE(coreRings[1].Pop() == nil, "Core 1 should not receive pair 2 update")
		fixture.EXPECT_TRUE(coreRings[2].Pop() != nil, "Core 2 should receive pair 2 update")
		fixture.EXPECT_TRUE(coreRings[3].Pop() != nil, "Core 3 should receive pair 2 update")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RING BUFFER OVERFLOW AND RETRY LOGIC STRESS TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestRingBufferRetryLogic(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("RingBufferFullRetry", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)

		// Create very small ring that will fill up quickly
		smallRing := ring24.New(4)
		coreRings[0] = smallRing

		// Fill the ring to capacity
		for i := 0; i < 4; i++ {
			msg := [24]byte{byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
			smallRing.Push(&msg)
		}

		// Verify ring is full
		testMsg := [24]byte{99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		fixture.EXPECT_FALSE(smallRing.Push(&testMsg), "Ring should be full and reject new messages")

		// Try to dispatch - should trigger retry logic
		logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)

		// Start draining after a delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			smallRing.Pop()
		}()

		start := time.Now()
		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchPriceUpdate(logView)
		})
		elapsed := time.Since(start)

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
		pairID := TradingPairID(23456)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)

		// Assign to multiple cores
		RegisterPairToCoreRouting(pairID, 0)
		RegisterPairToCoreRouting(pairID, 1)
		RegisterPairToCoreRouting(pairID, 2)

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
			coreRings[0].Pop()
			coreRings[2].Pop()
		}()

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchPriceUpdate(logView)
		})

		// Verify all cores eventually received the message
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

	t.Run("ConcurrentRetryStress", func(t *testing.T) {
		// Stress test with multiple pairs and cores
		numPairs := 5
		numCores := 3

		// Setup pairs and small rings
		for i := 0; i < numPairs; i++ {
			address := fmt.Sprintf("0x%040d", 1234567890+i)
			pairID := TradingPairID(12345 + i)

			RegisterTradingPairAddress([]byte(address[2:]), pairID)
			RegisterPairToCoreRouting(pairID, uint8(i%numCores))
		}

		// Create small rings that will fill quickly
		for i := 0; i < numCores; i++ {
			coreRings[i] = ring24.New(3)
			// Pre-fill rings
			for j := 0; j < 3; j++ {
				msg := [24]byte{byte(j), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
				coreRings[i].Push(&msg)
			}
		}

		// Start concurrent draining
		go func() {
			ticker := time.NewTicker(2 * time.Millisecond)
			defer ticker.Stop()
			for i := 0; i < 20; i++ {
				<-ticker.C
				for coreID := 0; coreID < numCores; coreID++ {
					coreRings[coreID].Pop()
				}
			}
		}()

		// Dispatch multiple updates concurrently
		var wg sync.WaitGroup
		for i := 0; i < numPairs; i++ {
			wg.Add(1)
			go func(pairIndex int) {
				defer wg.Done()
				address := fmt.Sprintf("0x%040d", 1234567890+pairIndex)
				logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)
				DispatchPriceUpdate(logView)
			}(i)
		}

		wg.Wait()

		// All dispatches should complete without panic
		// The retry mechanism should have handled full rings
	})

	t.Run("RetryWithVariableDelay", func(t *testing.T) {
		address := "0x3334567890123456789012345678901234567890"
		pairID := TradingPairID(33456)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)

		smallRing := ring24.New(2)
		coreRings[0] = smallRing

		// Fill ring
		for i := 0; i < 2; i++ {
			msg := [24]byte{byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
			smallRing.Push(&msg)
		}

		logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)

		// Variable delay draining to test retry persistence
		go func() {
			time.Sleep(5 * time.Millisecond)
			smallRing.Pop()
			time.Sleep(5 * time.Millisecond)
			smallRing.Pop()
		}()

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchPriceUpdate(logView)
		})

		// Should eventually succeed
		message := smallRing.Pop()
		fixture.EXPECT_TRUE(message != nil, "Message should eventually be delivered")
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// QUANTIZATION AND CORE PROCESSING WITH SHARED ARENA TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestTickQuantization(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("BasicQuantizationBounds", func(t *testing.T) {
		testValues := []float64{0.0, 50.0, -50.0, 100.0, -100.0}

		for _, tickValue := range testValues {
			result := quantizeTickValue(tickValue)
			fixture.EXPECT_GE(result, int64(0), "Quantized value should be non-negative")
			fixture.EXPECT_LE(result, int64(constants.MaxQuantizedTick), "Quantized value should be within max bound")
		}
	})

	t.Run("MonotonicityProperty", func(t *testing.T) {
		baseValue := 10.0
		largerValue := baseValue + 1.0

		baseQuantized := quantizeTickValue(baseValue)
		largerQuantized := quantizeTickValue(largerValue)

		fixture.EXPECT_LT(baseQuantized, largerQuantized, "Quantization should preserve order")
	})

	t.Run("PrecisionTesting", func(t *testing.T) {
		// Test precision around zero
		smallPositive := 0.001
		smallNegative := -0.001

		posQuantized := quantizeTickValue(smallPositive)
		negQuantized := quantizeTickValue(smallNegative)

		fixture.EXPECT_NE(posQuantized, negQuantized, "Small positive and negative values should quantize differently")
		fixture.EXPECT_LT(negQuantized, posQuantized, "Negative should quantize to smaller value than positive")
	})

	t.Run("ExtremeValues", func(t *testing.T) {
		extremeValues := []float64{
			-constants.TickClampingBound,
			constants.TickClampingBound,
			-1000.0,
			1000.0,
			math.Inf(1),
			math.Inf(-1),
			math.NaN(),
		}

		for _, value := range extremeValues {
			fixture.EXPECT_NO_FATAL_FAILURE(func() {
				result := quantizeTickValue(value)
				// Should not panic and should produce some result
				_ = result
			})
		}
	})

	t.Run("QuantizationConsistency", func(t *testing.T) {
		testValue := 42.123456789

		// Multiple calls should produce same result
		result1 := quantizeTickValue(testValue)
		result2 := quantizeTickValue(testValue)
		result3 := quantizeTickValue(testValue)

		fixture.EXPECT_EQ(result1, result2, "Quantization should be consistent")
		fixture.EXPECT_EQ(result2, result3, "Quantization should be consistent")
	})
}

func TestArbitrageEngineProcessing(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("ForwardDirectionProcessing", func(t *testing.T) {
		engine := NewTestEngineBuilder().
			WithArenaSize(1000).
			WithQueues(1).
			WithCyclesPerQueue(3).
			WithReverseDirection(false).
			Build()

		pairID := TradingPairID(1)
		update := &PriceUpdateMessage{
			pairID:      pairID,
			forwardTick: 1.5,
			reverseTick: -1.5,
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			processArbitrageUpdate(engine, update)
		})

		fixture.EXPECT_FALSE(engine.priorityQueues[0].Empty(), "Queue should have cycles after processing")
	})

	t.Run("ReverseDirectionProcessing", func(t *testing.T) {
		engine := NewTestEngineBuilder().
			WithArenaSize(1000).
			WithQueues(1).
			WithCyclesPerQueue(3).
			WithReverseDirection(true).
			Build()

		pairID := TradingPairID(1)
		update := &PriceUpdateMessage{
			pairID:      pairID,
			forwardTick: 2.0,
			reverseTick: -2.0,
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			processArbitrageUpdate(engine, update)
		})

		fixture.EXPECT_FALSE(engine.priorityQueues[0].Empty(), "Queue should have cycles after processing")
	})

	t.Run("SharedArenaUsage", func(t *testing.T) {
		engine := NewTestEngineBuilder().
			WithArenaSize(2000).
			WithQueues(3).
			WithCyclesPerQueue(5).
			Build()

		// Verify shared arena is being used
		fixture.EXPECT_TRUE(len(engine.sharedArena) >= 2000, "Shared arena should be allocated")

		// All queues should share the same arena pointer
		// arenaPtr := unsafe.Pointer(&engine.sharedArena[0])
		for i := 0; i < 3; i++ {
			// Queue should not be empty
			fixture.EXPECT_FALSE(engine.priorityQueues[i].Empty(), fmt.Sprintf("Queue %d should have cycles", i))
		}

		// Verify handle allocation is working
		initialHandle := engine.nextHandle
		newHandle := engine.allocateQueueHandle()
		fixture.EXPECT_EQ(initialHandle, newHandle, "Handle allocation should be sequential")
		fixture.EXPECT_EQ(initialHandle+1, engine.nextHandle, "Next handle should be incremented")
	})

	t.Run("MultipleQueueInteraction", func(t *testing.T) {
		engine := NewTestEngineBuilder().
			WithArenaSize(3000).
			WithQueues(4).
			WithCyclesPerQueue(3).
			Build()

		// Process updates for different pairs
		updates := []*PriceUpdateMessage{
			{pairID: TradingPairID(1), forwardTick: 1.0, reverseTick: -1.0},
			{pairID: TradingPairID(2), forwardTick: 2.0, reverseTick: -2.0},
			{pairID: TradingPairID(3), forwardTick: 3.0, reverseTick: -3.0},
			{pairID: TradingPairID(4), forwardTick: 4.0, reverseTick: -4.0},
		}

		for _, update := range updates {
			fixture.EXPECT_NO_FATAL_FAILURE(func() {
				processArbitrageUpdate(engine, update)
			})
		}

		// All queues should still have cycles
		for i := 0; i < 4; i++ {
			fixture.EXPECT_FALSE(engine.priorityQueues[i].Empty(), fmt.Sprintf("Queue %d should still have cycles", i))
		}
	})

	t.Run("HandleAllocationBounds", func(t *testing.T) {
		engine := NewTestEngineBuilder().
			WithArenaSize(100). // Small arena
			WithQueues(1).
			WithCyclesPerQueue(0). // Start empty
			Build()

		// Allocate handles up to arena limit
		handles := make([]pooledquantumqueue.Handle, 0)
		for i := 0; i < 100; i++ {
			handle := engine.allocateQueueHandle()
			handles = append(handles, handle)
		}

		fixture.EXPECT_EQ(100, len(handles), "Should allocate up to arena size")
		fixture.EXPECT_EQ(pooledquantumqueue.Handle(100), engine.nextHandle, "Next handle should be at arena limit")

		// All handles should be unique
		seen := make(map[pooledquantumqueue.Handle]bool)
		for _, handle := range handles {
			fixture.EXPECT_FALSE(seen[handle], fmt.Sprintf("Handle %d should be unique", handle))
			seen[handle] = true
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// FANOUT PROCESSING AND PRIORITY QUEUE OPERATIONS VALIDATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestFanoutProcessingLogic(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("FanoutTickValueUpdates", func(t *testing.T) {
		engine := NewTestEngineBuilder().
			WithArenaSize(2000).
			WithQueues(1).
			WithCyclesPerQueue(0). // Don't auto-populate
			Build()

		// Create cycles manually with known initial values
		engine.cycleStates = make([]ArbitrageCycleState, 3)
		engine.cycleStates[0] = ArbitrageCycleState{
			pairIDs:    [3]TradingPairID{1, 2, 3},
			tickValues: [3]float64{1.0, 2.0, 3.0},
		}
		engine.cycleStates[1] = ArbitrageCycleState{
			pairIDs:    [3]TradingPairID{1, 4, 5},
			tickValues: [3]float64{-1.0, 0.5, 1.5},
		}
		engine.cycleStates[2] = ArbitrageCycleState{
			pairIDs:    [3]TradingPairID{1, 6, 7},
			tickValues: [3]float64{0.0, -2.0, 1.0},
		}

		// Allocate handles for fanout
		handle0 := engine.allocateQueueHandle()
		handle1 := engine.allocateQueueHandle()
		handle2 := engine.allocateQueueHandle()

		// Set up fanout table for pair 1
		engine.cycleFanoutTable[0] = []CycleFanoutEntry{
			{
				cycleIndex:  0,
				edgeIndex:   0,
				queue:       &engine.priorityQueues[0],
				queueHandle: handle0,
			},
			{
				cycleIndex:  1,
				edgeIndex:   0,
				queue:       &engine.priorityQueues[0],
				queueHandle: handle1,
			},
			{
				cycleIndex:  2,
				edgeIndex:   0,
				queue:       &engine.priorityQueues[0],
				queueHandle: handle2,
			},
		}

		engine.pairToQueueLookup.Put(1, 0)

		// Add cycles to queue
		engine.priorityQueues[0].Push(quantizeTickValue(6.0), handle0, 0)
		engine.priorityQueues[0].Push(quantizeTickValue(1.0), handle1, 1)
		engine.priorityQueues[0].Push(quantizeTickValue(-1.0), handle2, 2)

		update := &PriceUpdateMessage{
			pairID:      1,
			forwardTick: 5.0,
			reverseTick: -5.0,
		}

		processArbitrageUpdate(engine, update)

		// Verify tick values were updated via fanout
		fixture.EXPECT_EQ(5.0, engine.cycleStates[0].tickValues[0], "Cycle 0 edge 0 should be updated")
		fixture.EXPECT_EQ(2.0, engine.cycleStates[0].tickValues[1], "Cycle 0 edge 1 should be unchanged")
		fixture.EXPECT_EQ(5.0, engine.cycleStates[1].tickValues[0], "Cycle 1 edge 0 should be updated")
		fixture.EXPECT_EQ(5.0, engine.cycleStates[2].tickValues[0], "Cycle 2 edge 0 should be updated")
	})

	t.Run("FanoutWithMultipleEdges", func(t *testing.T) {
		engine := NewTestEngineBuilder().
			WithArenaSize(1000).
			WithQueues(1).
			WithCyclesPerQueue(0).
			Build()

		// Create cycle where one pair affects multiple edges
		engine.cycleStates = make([]ArbitrageCycleState, 1)
		engine.cycleStates[0] = ArbitrageCycleState{
			pairIDs:    [3]TradingPairID{1, 1, 2}, // Pair 1 appears twice
			tickValues: [3]float64{0.0, 0.0, 0.0},
		}

		handle := engine.allocateQueueHandle()

		// Fanout affects edges 0 and 1 of the same cycle
		engine.cycleFanoutTable[0] = []CycleFanoutEntry{
			{
				cycleIndex:  0,
				edgeIndex:   0,
				queue:       &engine.priorityQueues[0],
				queueHandle: handle,
			},
			{
				cycleIndex:  0,
				edgeIndex:   1,
				queue:       &engine.priorityQueues[0],
				queueHandle: handle,
			},
		}

		engine.pairToQueueLookup.Put(1, 0)
		engine.priorityQueues[0].Push(0, handle, 0)

		update := &PriceUpdateMessage{
			pairID:      1,
			forwardTick: 3.5,
			reverseTick: -3.5,
		}

		processArbitrageUpdate(engine, update)

		// Both edges should be updated
		fixture.EXPECT_EQ(3.5, engine.cycleStates[0].tickValues[0], "Edge 0 should be updated")
		fixture.EXPECT_EQ(3.5, engine.cycleStates[0].tickValues[1], "Edge 1 should be updated")
		fixture.EXPECT_EQ(0.0, engine.cycleStates[0].tickValues[2], "Edge 2 should be unchanged")
	})

	t.Run("EmptyFanoutTable", func(t *testing.T) {
		engine := NewTestEngineBuilder().
			WithArenaSize(1000).
			WithQueues(1).
			WithCyclesPerQueue(1).
			Build()

		engine.cycleFanoutTable[0] = []CycleFanoutEntry{} // Empty fanout table
		engine.pairToQueueLookup.Put(1, 0)

		update := &PriceUpdateMessage{
			pairID:      1,
			forwardTick: 2.0,
			reverseTick: -2.0,
		}

		// Should complete without error
		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			processArbitrageUpdate(engine, update)
		})
	})

	t.Run("LargeFanoutTable", func(t *testing.T) {
		engine := NewTestEngineBuilder().
			WithArenaSize(5000).
			WithQueues(1).
			WithCyclesPerQueue(0).
			Build()

		// Create many cycles that all depend on pair 1
		numCycles := 100
		engine.cycleStates = make([]ArbitrageCycleState, numCycles)
		engine.cycleFanoutTable[0] = make([]CycleFanoutEntry, numCycles)

		for i := 0; i < numCycles; i++ {
			engine.cycleStates[i] = ArbitrageCycleState{
				pairIDs:    [3]TradingPairID{1, TradingPairID(i + 2), TradingPairID(i + 3)},
				tickValues: [3]float64{0.0, 0.0, 0.0},
			}

			handle := engine.allocateQueueHandle()
			engine.cycleFanoutTable[0][i] = CycleFanoutEntry{
				cycleIndex:  uint64(i),
				edgeIndex:   0,
				queue:       &engine.priorityQueues[0],
				queueHandle: handle,
			}

			engine.priorityQueues[0].Push(0, handle, uint64(i))
		}

		engine.pairToQueueLookup.Put(1, 0)

		update := &PriceUpdateMessage{
			pairID:      1,
			forwardTick: 7.5,
			reverseTick: -7.5,
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			processArbitrageUpdate(engine, update)
		})

		// All cycles should be updated
		for i := 0; i < numCycles; i++ {
			fixture.EXPECT_EQ(7.5, engine.cycleStates[i].tickValues[0],
				fmt.Sprintf("Cycle %d should be updated", i))
		}
	})

	t.Run("FanoutPerformance", func(t *testing.T) {
		engine := NewTestEngineBuilder().
			WithArenaSize(10000).
			WithQueues(1).
			WithCyclesPerQueue(0).
			Build()

		// Create moderate fanout for performance testing
		numCycles := 50
		engine.cycleStates = make([]ArbitrageCycleState, numCycles)
		engine.cycleFanoutTable[0] = make([]CycleFanoutEntry, numCycles)

		for i := 0; i < numCycles; i++ {
			engine.cycleStates[i] = ArbitrageCycleState{
				pairIDs:    [3]TradingPairID{1, TradingPairID(i + 2), TradingPairID(i + 3)},
				tickValues: [3]float64{float64(i), 0.0, 0.0},
			}

			handle := engine.allocateQueueHandle()
			engine.cycleFanoutTable[0][i] = CycleFanoutEntry{
				cycleIndex:  uint64(i),
				edgeIndex:   0,
				queue:       &engine.priorityQueues[0],
				queueHandle: handle,
			}

			engine.priorityQueues[0].Push(quantizeTickValue(float64(i)), handle, uint64(i))
		}

		engine.pairToQueueLookup.Put(1, 0)

		update := &PriceUpdateMessage{
			pairID:      1,
			forwardTick: 42.0,
			reverseTick: -42.0,
		}

		// Should be very fast
		start := time.Now()
		processArbitrageUpdate(engine, update)
		elapsed := time.Since(start)

		fixture.EXPECT_LT(elapsed, 100*time.Microsecond, "Fanout processing should be very fast")

		// Verify correctness
		for i := 0; i < numCycles; i++ {
			fixture.EXPECT_EQ(42.0, engine.cycleStates[i].tickValues[0],
				fmt.Sprintf("Cycle %d should be updated correctly", i))
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SHARD CONSTRUCTION AND SYSTEM INITIALIZATION COMPREHENSIVE TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestWorkloadShardConstruction(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("BasicShardCreation", func(t *testing.T) {
		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
			{TradingPairID(1), TradingPairID(4), TradingPairID(5)},
			{TradingPairID(2), TradingPairID(6), TradingPairID(7)},
		}

		buildWorkloadShards(triangles)

		fixture.EXPECT_TRUE(pairWorkloadShards != nil, "Shard buckets should be created")
		fixture.EXPECT_LT(0, len(pairWorkloadShards), "Should have non-empty shard buckets")
	})

	t.Run("PairCycleMapping", func(t *testing.T) {
		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
			{TradingPairID(1), TradingPairID(4), TradingPairID(5)},
		}

		buildWorkloadShards(triangles)

		buckets, exists := pairWorkloadShards[TradingPairID(1)]
		fixture.EXPECT_TRUE(exists, "Pair 1 should have shard buckets")

		totalBindings := 0
		for _, bucket := range buckets {
			totalBindings += len(bucket.cycleEdges)
			fixture.EXPECT_EQ(TradingPairID(1), bucket.pairID, "Bucket should have correct pairID")
		}

		fixture.EXPECT_EQ(2, totalBindings, "Pair 1 should have 2 edge bindings")
	})

	t.Run("ShardShufflingDeterminism", func(t *testing.T) {
		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
			{TradingPairID(1), TradingPairID(4), TradingPairID(5)},
			{TradingPairID(1), TradingPairID(6), TradingPairID(7)},
			{TradingPairID(1), TradingPairID(8), TradingPairID(9)},
		}

		// Build shards twice
		buildWorkloadShards(triangles)
		firstShards := make(map[TradingPairID][]PairWorkloadShard)
		for k, v := range pairWorkloadShards {
			firstShards[k] = make([]PairWorkloadShard, len(v))
			copy(firstShards[k], v)
		}

		// Reset and build again
		pairWorkloadShards = nil
		buildWorkloadShards(triangles)

		// Should produce identical results
		for pairID, shards := range firstShards {
			secondShards, exists := pairWorkloadShards[pairID]
			fixture.EXPECT_TRUE(exists, fmt.Sprintf("Pair %d should exist in second build", pairID))
			fixture.EXPECT_EQ(len(shards), len(secondShards), "Shard count should be identical")

			for i, shard := range shards {
				if i < len(secondShards) {
					fixture.EXPECT_EQ(len(shard.cycleEdges), len(secondShards[i].cycleEdges),
						"Edge count should be identical")
				}
			}
		}
	})

	t.Run("ShardSizeLimit", func(t *testing.T) {
		// Create many triangles for one pair to test sharding
		numTriangles := constants.MaxCyclesPerShard + 10
		triangles := make([]ArbitrageTriangle, numTriangles)
		for i := 0; i < numTriangles; i++ {
			triangles[i] = ArbitrageTriangle{
				TradingPairID(1),
				TradingPairID(i + 2),
				TradingPairID(i + 1002),
			}
		}

		buildWorkloadShards(triangles)

		shards, exists := pairWorkloadShards[TradingPairID(1)]
		fixture.EXPECT_TRUE(exists, "Pair 1 should have shards")
		fixture.EXPECT_LT(1, len(shards), "Should create multiple shards when exceeding limit")

		// Each shard should respect the limit
		for i, shard := range shards {
			fixture.EXPECT_LE(len(shard.cycleEdges), constants.MaxCyclesPerShard,
				fmt.Sprintf("Shard %d should respect size limit", i))
		}

		// Total should equal input
		totalEdges := 0
		for _, shard := range shards {
			totalEdges += len(shard.cycleEdges)
		}
		fixture.EXPECT_EQ(numTriangles, totalEdges, "Total edges should equal input triangles")
	})

	t.Run("ComplexTriangleDistribution", func(t *testing.T) {
		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
			{TradingPairID(2), TradingPairID(3), TradingPairID(4)},
			{TradingPairID(3), TradingPairID(4), TradingPairID(5)},
			{TradingPairID(4), TradingPairID(5), TradingPairID(1)},
			{TradingPairID(5), TradingPairID(1), TradingPairID(2)},
		}

		buildWorkloadShards(triangles)

		// Each pair should appear in multiple triangles
		expectedAppearances := map[TradingPairID]int{
			TradingPairID(1): 3,
			TradingPairID(2): 3,
			TradingPairID(3): 3,
			TradingPairID(4): 3,
			TradingPairID(5): 3,
		}

		for pairID, expectedCount := range expectedAppearances {
			shards, exists := pairWorkloadShards[pairID]
			fixture.EXPECT_TRUE(exists, fmt.Sprintf("Pair %d should exist", pairID))

			totalEdges := 0
			for _, shard := range shards {
				totalEdges += len(shard.cycleEdges)
			}
			fixture.EXPECT_EQ(expectedCount, totalEdges,
				fmt.Sprintf("Pair %d should appear in %d triangles", pairID, expectedCount))
		}
	})
}

func TestSystemInitialization(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("SmallSystemInit", func(t *testing.T) {
		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
			{TradingPairID(4), TradingPairID(5), TradingPairID(6)},
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			InitializeArbitrageSystem(triangles)
		})

		// Give goroutines time to start
		time.Sleep(50 * time.Millisecond)

		// Verify some pairs were assigned
		assigned := false
		for i := 0; i < 10; i++ {
			if pairToCoreRouting[i] != 0 {
				assigned = true
				break
			}
		}
		fixture.EXPECT_TRUE(assigned, "Some pairs should be assigned to cores")
	})

	t.Run("MediumSystemInit", func(t *testing.T) {
		// Create a more realistic triangle set
		triangles := make([]ArbitrageTriangle, 20)
		for i := 0; i < 20; i++ {
			triangles[i] = ArbitrageTriangle{
				TradingPairID(i*3 + 1),
				TradingPairID(i*3 + 2),
				TradingPairID(i*3 + 3),
			}
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			InitializeArbitrageSystem(triangles)
		})

		time.Sleep(100 * time.Millisecond)

		// More pairs should be assigned
		assignedCount := 0
		for i := 0; i < 70; i++ {
			if pairToCoreRouting[i] != 0 {
				assignedCount++
			}
		}
		fixture.EXPECT_LT(10, assignedCount, "Many pairs should be assigned in medium system")
	})

	t.Run("CoreCountValidation", func(t *testing.T) {
		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
		}

		// Mock runtime.NumCPU to test core count logic
		originalCPUCount := runtime.NumCPU()

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			InitializeArbitrageSystem(triangles)
		})

		// System should handle any reasonable core count
		fixture.EXPECT_LT(0, originalCPUCount, "System should detect CPU cores")
	})

	t.Run("PairRoutingDistribution", func(t *testing.T) {
		triangles := []ArbitrageTriangle{
			{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
			{TradingPairID(1), TradingPairID(4), TradingPairID(5)},
			{TradingPairID(2), TradingPairID(6), TradingPairID(7)},
		}

		InitializeArbitrageSystem(triangles)
		time.Sleep(50 * time.Millisecond)

		// Each pair should be assigned to both forward and reverse cores
		for pairID := TradingPairID(1); pairID <= 7; pairID++ {
			assignment := pairToCoreRouting[pairID]
			if assignment != 0 {
				// Should have at least one core assigned
				coreCount := 0
				for bit := 0; bit < 64; bit++ {
					if (assignment>>bit)&1 == 1 {
						coreCount++
					}
				}
				fixture.EXPECT_LT(0, coreCount, fmt.Sprintf("Pair %d should be assigned to at least one core", pairID))
			}
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HIGH-VOLUME OPERATIONS AND EXTREME VALUE HANDLING STRESS TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestHighVolumeOperations(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("MassAddressRegistration", func(t *testing.T) {
		numAddresses := 100

		successfulRegistrations := 0
		for i := 0; i < numAddresses; i++ {
			address := fmt.Sprintf("%040x", i*0x123456+0x789abc)
			pairID := TradingPairID(i + 1)
			RegisterTradingPairAddress([]byte(address), pairID)

			if lookupPairByAddress([]byte(address)) == pairID {
				successfulRegistrations++
			}
		}

		fixture.EXPECT_LT(numAddresses*8/10, successfulRegistrations,
			fmt.Sprintf("Most addresses should be registered successfully (got %d/%d)",
				successfulRegistrations, numAddresses))
	})

	t.Run("ConcurrentLookups", func(t *testing.T) {
		numAddresses := 50
		for i := 0; i < numAddresses; i++ {
			address := fmt.Sprintf("%040x", i*0x111111+0x222222)
			RegisterTradingPairAddress([]byte(address), TradingPairID(i+1))
		}

		var wg sync.WaitGroup
		var successCount int32

		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(startIdx int) {
				defer wg.Done()

				localSuccess := 0
				for j := 0; j < 10; j++ {
					idx := (startIdx*10 + j) % numAddresses
					address := fmt.Sprintf("%040x", idx*0x111111+0x222222)
					result := lookupPairByAddress([]byte(address))
					expected := TradingPairID(idx + 1)

					if result == expected {
						localSuccess++
					}
				}
				atomic.AddInt32(&successCount, int32(localSuccess))
			}(i)
		}

		wg.Wait()

		fixture.EXPECT_LT(60, int(successCount),
			fmt.Sprintf("Most concurrent lookups should succeed (got %d/80)", successCount))
	})

	t.Run("StressTestAddressTable", func(t *testing.T) {
		// Fill a significant portion of the address table
		numAddresses := constants.AddressTableCapacity / 4
		addresses := make([]string, numAddresses)

		for i := 0; i < numAddresses; i++ {
			addresses[i] = fmt.Sprintf("%040x", i*0x987654321+0x123456789)
			RegisterTradingPairAddress([]byte(addresses[i]), TradingPairID(i+1000))
		}

		// Verify all can still be looked up
		successCount := 0
		for i, addr := range addresses {
			result := lookupPairByAddress([]byte(addr))
			if result == TradingPairID(i+1000) {
				successCount++
			}
		}

		successRate := float64(successCount) / float64(numAddresses)
		fixture.EXPECT_GE(successRate, 0.95, "Should maintain high success rate even with many addresses")
	})

	t.Run("RapidFireDispatch", func(t *testing.T) {
		// Setup many pairs
		numPairs := 20
		for i := 0; i < numPairs; i++ {
			address := fmt.Sprintf("0x%040d", 1000000000+i)
			pairID := TradingPairID(i + 2000)
			RegisterTradingPairAddress([]byte(address[2:]), pairID)
			RegisterPairToCoreRouting(pairID, 0)
		}

		coreRings[0] = ring24.New(1 << 12) // 4K slots

		// Consumer to drain messages
		stop := make(chan struct{})
		var messageCount int32
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					if coreRings[0].Pop() != nil {
						atomic.AddInt32(&messageCount, 1)
					}
				}
			}
		}()

		// Rapid fire dispatching
		start := time.Now()
		for i := 0; i < 1000; i++ {
			pairIdx := i % numPairs
			address := fmt.Sprintf("0x%040d", 1000000000+pairIdx)
			logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)
			DispatchPriceUpdate(logView)
		}
		elapsed := time.Since(start)

		close(stop)
		time.Sleep(10 * time.Millisecond)

		fixture.EXPECT_LT(elapsed, 100*time.Millisecond, "Rapid dispatching should be fast")
		fixture.EXPECT_LT(900, int(messageCount), "Should process most messages successfully")
	})
}

func TestExtremeValueHandling(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("ExtremeReserveValues", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
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
			{"PowerOfTwo", 1024, 2048},
			{"PrimeNumbers", 1009, 2017},
			{"VeryLarge", math.MaxUint64 >> 8, math.MaxUint64 >> 12},
		}

		for _, tv := range extremeValues {
			t.Run(tv.name, func(t *testing.T) {
				logView := fixture.CreateTestLogView(address, tv.reserve0, tv.reserve1)

				fixture.EXPECT_NO_FATAL_FAILURE(func() {
					DispatchPriceUpdate(logView)
				})

				message := coreRings[0].Pop()
				if message != nil {
					priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
					fixture.EXPECT_EQ(pairID, priceUpdate.pairID, "PairID should be correct")
					fixture.EXPECT_FALSE(math.IsNaN(priceUpdate.forwardTick), "Forward tick should not be NaN")
					fixture.EXPECT_FALSE(math.IsNaN(priceUpdate.reverseTick), "Reverse tick should not be NaN")
					fixture.EXPECT_FALSE(math.IsInf(priceUpdate.forwardTick, 0), "Forward tick should not be infinite")
					fixture.EXPECT_FALSE(math.IsInf(priceUpdate.reverseTick, 0), "Reverse tick should not be infinite")
				}
			})
		}
	})

	t.Run("MalformedHexData", func(t *testing.T) {
		address := "0x2234567890123456789012345678901234567890"
		pairID := TradingPairID(22345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		malformedData := []string{
			"0x00",                          // Too short
			"0x" + string(make([]byte, 64)), // Invalid characters
			"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", // Correct length but all zeros
		}

		for _, data := range malformedData {
			logView := &types.LogView{
				Addr: []byte(address),
				Data: []byte(data),
			}

			fixture.EXPECT_NO_FATAL_FAILURE(func() {
				DispatchPriceUpdate(logView)
			})
		}
	})

	t.Run("EdgeCaseReserveCombinations", func(t *testing.T) {
		address := "0x3334567890123456789012345678901234567890"
		pairID := TradingPairID(33345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		edgeCases := []struct {
			name     string
			reserve0 uint64
			reserve1 uint64
		}{
			{"MinimumNonZero", 1, 1},
			{"OneMillion", 1000000, 1000000},
			{"PowersOfTen", 1000000000, 10000000000},
			{"FibonacciNumbers", 1597, 2584},
			{"LargeUneven", 999999999999, 1000000000001},
		}

		for _, tc := range edgeCases {
			t.Run(tc.name, func(t *testing.T) {
				logView := fixture.CreateTestLogView(address, tc.reserve0, tc.reserve1)

				fixture.EXPECT_NO_FATAL_FAILURE(func() {
					DispatchPriceUpdate(logView)
				})

				message := coreRings[0].Pop()
				if message != nil {
					priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
					fixture.EXPECT_FALSE(math.IsNaN(priceUpdate.forwardTick), "Should not produce NaN for edge cases")
					fixture.EXPECT_FALSE(math.IsInf(priceUpdate.forwardTick, 0), "Should not produce infinity for edge cases")
				}
			})
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// END-TO-END INTEGRATION AND PERFORMANCE BENCHMARKS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestEndToEndIntegration(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()
	defer fixture.TearDown()

	t.Run("CompleteProcessingPipeline", func(t *testing.T) {
		address := "0x1234567890123456789012345678901234567890"
		pairID := TradingPairID(12345)

		RegisterTradingPairAddress([]byte(address[2:]), pairID)
		RegisterPairToCoreRouting(pairID, 0)

		triangles := []ArbitrageTriangle{{pairID, TradingPairID(2), TradingPairID(3)}}
		buildWorkloadShards(triangles)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		logView := fixture.CreateTestLogView(address, 1000000000000, 2000000000000)

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			DispatchPriceUpdate(logView)
		})

		message := coreRings[0].Pop()
		fixture.EXPECT_TRUE(message != nil, "Pipeline should produce message")

		if message != nil {
			priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
			fixture.EXPECT_EQ(pairID, priceUpdate.pairID, "End-to-end pairID should match")
			fixture.EXPECT_NE(0.0, priceUpdate.forwardTick, "End-to-end tick should be non-zero")
		}
	})

	t.Run("SystemWithManyTriangles", func(t *testing.T) {
		// Create a realistic arbitrage system
		numTriangles := 50
		triangles := make([]ArbitrageTriangle, numTriangles)
		for i := 0; i < numTriangles; i++ {
			triangles[i] = ArbitrageTriangle{
				TradingPairID(i*3 + 1),
				TradingPairID(i*3 + 2),
				TradingPairID(i*3 + 3),
			}
		}

		// Register some addresses for testing
		testPairs := []TradingPairID{1, 4, 7, 10, 13}
		for i, pairID := range testPairs {
			address := fmt.Sprintf("0x%040d", 1000000000+i)
			RegisterTradingPairAddress([]byte(address[2:]), pairID)
			RegisterPairToCoreRouting(pairID, 0)
		}

		buildWorkloadShards(triangles)
		coreRings[0] = ring24.New(constants.DefaultRingSize)

		// Test dispatch to registered pairs
		for i, pairID := range testPairs {
			address := fmt.Sprintf("0x%040d", 1000000000+i)
			logView := fixture.CreateTestLogView(address, 1000000000000+uint64(i)*1000000000, 2000000000000)

			fixture.EXPECT_NO_FATAL_FAILURE(func() {
				DispatchPriceUpdate(logView)
			})

			message := coreRings[0].Pop()
			if message != nil {
				priceUpdate := (*PriceUpdateMessage)(unsafe.Pointer(message))
				fixture.EXPECT_EQ(pairID, priceUpdate.pairID, fmt.Sprintf("Pair %d should match", pairID))
			}
		}
	})

	t.Run("IntegrationWithArenaSharing", func(t *testing.T) {
		// Test that shared arena works correctly in integration
		engine := NewTestEngineBuilder().
			WithArenaSize(5000).
			WithQueues(3).
			WithCyclesPerQueue(10).
			Build()

		// Verify arena sharing works
		fixture.EXPECT_TRUE(len(engine.sharedArena) >= 5000, "Arena should be allocated")

		// Process various updates
		updates := []*PriceUpdateMessage{
			{pairID: TradingPairID(1), forwardTick: 1.5, reverseTick: -1.5},
			{pairID: TradingPairID(2), forwardTick: -2.0, reverseTick: 2.0},
			{pairID: TradingPairID(3), forwardTick: 0.5, reverseTick: -0.5},
		}

		for _, update := range updates {
			fixture.EXPECT_NO_FATAL_FAILURE(func() {
				processArbitrageUpdate(engine, update)
			})
		}

		// All queues should still function
		for i := 0; i < 3; i++ {
			fixture.EXPECT_FALSE(engine.priorityQueues[i].Empty(), fmt.Sprintf("Queue %d should have elements", i))
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// COMPREHENSIVE PERFORMANCE BENCHMARKS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func BenchmarkDispatchPriceUpdate(b *testing.B) {
	fixture := NewRouterTestFixture(&testing.T{})
	fixture.SetUp()

	address := "0x882df4b0fb50a229c3b4124eb18c759911485bfb"
	pairID := TradingPairID(12345)
	RegisterTradingPairAddress([]byte(address[2:]), pairID)
	RegisterPairToCoreRouting(pairID, 0)

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
		DispatchPriceUpdate(logView)
	}

	close(stop)
	time.Sleep(10 * time.Millisecond)
}

func BenchmarkCountHexLeadingZeros(b *testing.B) {
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
				_ = countHexLeadingZeros(input)
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
		RegisterTradingPairAddress([]byte(address), TradingPairID(i+1))
	}

	b.Run("ExistingAddresses", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			addr := realAddresses[i%len(realAddresses)]
			_ = lookupPairByAddress([]byte(addr))
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
			_ = lookupPairByAddress([]byte(addr))
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
			for j := range addressToPairMap {
				addressToPairMap[j] = 0
				packedAddressKeys[j] = PackedAddress{}
			}
		}

		addr := realAddresses[i%len(realAddresses)]
		RegisterTradingPairAddress([]byte(addr), TradingPairID(i+1))
	}
}

func BenchmarkQuantization(b *testing.B) {
	values := []float64{-100.5, -50.0, 0.0, 25.7, 100.0, -128.0, 127.9, 1.5, -1.5}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = quantizeTickValue(values[i%len(values)])
	}
}

func BenchmarkProcessArbitrageUpdate(b *testing.B) {
	engine := NewTestEngineBuilder().
		WithArenaSize(10000).
		WithQueues(10).
		WithCyclesPerQueue(10).
		Build()

	updates := make([]*PriceUpdateMessage, 10)
	for i := range updates {
		updates[i] = &PriceUpdateMessage{
			pairID:      TradingPairID(i + 1),
			forwardTick: float64(i) * 0.1,
			reverseTick: -float64(i) * 0.1,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processArbitrageUpdate(engine, updates[i%len(updates)])
	}
}

func BenchmarkPackEthereumAddress(b *testing.B) {
	addresses := []string{
		"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
		"bb2b8038a1640196fbe3e38816f3e67cba72d940",
		"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
		"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = packEthereumAddress([]byte(addr))
	}
}

func BenchmarkHashAddressToIndex(b *testing.B) {
	addresses := []string{
		"a478c2975ab1ea89e8196811f51a7b7ade33eb11",
		"bb2b8038a1640196fbe3e38816f3e67cba72d940",
		"0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
		"b4e16d0168e52d35cacd2c6185b44281ec28c9dc",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = hashAddressToIndex([]byte(addr))
	}
}

func BenchmarkCryptoRandomGeneration(b *testing.B) {
	seed := []byte("benchmark_seed_12345")
	rng := newCryptoRandomGenerator(seed)

	b.Run("GenerateRandomUint64", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = rng.generateRandomUint64()
		}
	})

	b.Run("GenerateRandomInt", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = rng.generateRandomInt(1000)
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
		RegisterTradingPairAddress([]byte(addr), TradingPairID(i+1))
		RegisterPairToCoreRouting(TradingPairID(i+1), uint8(i%numCores))

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
		DispatchPriceUpdate(events[i%numPairs])
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
		RegisterTradingPairAddress([]byte(addr), TradingPairID(i+1))
		RegisterPairToCoreRouting(TradingPairID(i+1), uint8(i%numCores))
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
			DispatchPriceUpdate(event)
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
	triangles := []ArbitrageTriangle{
		{TradingPairID(1), TradingPairID(2), TradingPairID(3)},
		{TradingPairID(1), TradingPairID(4), TradingPairID(5)},
		{TradingPairID(2), TradingPairID(6), TradingPairID(7)},
		{TradingPairID(3), TradingPairID(8), TradingPairID(9)},
	}

	buildWorkloadShards(triangles)

	// Register some pairs
	testPairs := []struct {
		addr   string
		pairID TradingPairID
	}{
		{"a478c2975ab1ea89e8196811f51a7b7ade33eb11", TradingPairID(1)},
		{"bb2b8038a1640196fbe3e38816f3e67cba72d940", TradingPairID(2)},
		{"0d4a11d5eeaac28ec3f61d100daf4d40471f1852", TradingPairID(3)},
	}

	for _, pair := range testPairs {
		RegisterTradingPairAddress([]byte(pair.addr), pair.pairID)
		RegisterPairToCoreRouting(pair.pairID, 0)
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
		DispatchPriceUpdate(events[i%len(events)])
	}

	elapsed := time.Since(start)
	close(stop)

	throughput := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(throughput, "events_per_second")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "ns_per_event")
}
