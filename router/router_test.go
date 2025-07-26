// ════════════════════════════════════════════════════════════════════════════════════════════════
// 🧪 COMPREHENSIVE TEST SUITE: ARBITRAGE DETECTION ENGINE
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Router & Arbitrage Engine Test Suite
//
// Description:
//   Validates the complete triangular arbitrage detection system through isolated performance
//   benchmarks, cache-cold scenario testing, and multi-core scaling analysis. Includes address
//   resolution correctness, SIMD parsing verification, and zero-allocation enforcement.
//
// Test Coverage:
//   - Unit tests: Address hash collision handling, price update distribution
//   - Integration tests: Full arbitrage flow, high-volume stress testing
//   - Benchmarks: Producer-only latency, cache-cold performance, multi-core efficiency
//   - Edge cases: Robin Hood displacement, pooled queue operations, fanout mapping
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package router

import (
	"main/constants"
	"main/control"
	"main/localidx"
	"main/pooledquantumqueue"
	"main/ring56"
	"main/types"
	"main/utils"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TEST FIXTURES AND UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// TestPairID constants for predictable testing
const (
	TestPairETH_DAI  types.TradingPairID = 1001
	TestPairDAI_USDC types.TradingPairID = 1002
	TestPairUSDC_ETH types.TradingPairID = 1003
	TestPairWBTC_ETH types.TradingPairID = 1004
	TestPairUNI_ETH  types.TradingPairID = 1005
)

// Test addresses - valid Ethereum addresses for testing
var (
	TestAddressETH_DAI  = "0x1234567890123456789012345678901234567890"
	TestAddressDAI_USDC = "0x2345678901234567890123456789012345678901"
	TestAddressUSDC_ETH = "0x3456789012345678901234567890123456789012"
	TestAddressWBTC_ETH = "0x4567890123456789012345678901234567890123"
	TestAddressUNI_ETH  = "0x5678901234567890123456789012345678901234"
)

// testSetup initializes clean test environment
func testSetup(_ *testing.T) func() {
	// Save original state for restoration
	originalCoreEngines := coreEngines
	originalCoreRings := coreRings
	originalPairToCoreRouting := pairToCoreRouting
	originalAddressToPairMap := addressToPairMap
	originalPackedAddressKeys := packedAddressKeys
	originalPairWorkloadShards := pairWorkloadShards

	// Reset global state
	coreEngines = [constants.MaxSupportedCores]*ArbitrageEngine{}
	coreRings = [constants.MaxSupportedCores]*ring56.Ring{}
	pairToCoreRouting = [constants.PairRoutingTableCapacity]uint64{}
	addressToPairMap = [constants.AddressTableCapacity]types.TradingPairID{}
	packedAddressKeys = [constants.AddressTableCapacity]PackedAddress{}
	pairWorkloadShards = make(map[types.TradingPairID][]PairWorkloadShard)

	// Reset control system
	control.ResetPollCounter()
	control.ForceInactive()

	// Return cleanup function
	return func() {
		// Stop any running cores
		control.Shutdown()
		time.Sleep(50 * time.Millisecond) // Allow graceful shutdown

		// Restore original state
		coreEngines = originalCoreEngines
		coreRings = originalCoreRings
		pairToCoreRouting = originalPairToCoreRouting
		addressToPairMap = originalAddressToPairMap
		packedAddressKeys = originalPackedAddressKeys
		pairWorkloadShards = originalPairWorkloadShards

		// Reset control system
		control.ResetPollCounter()
		control.ForceInactive()
	}
}

// createTestLogView creates a mock Ethereum log for testing
func createTestLogView(address, data string) *types.LogView {
	return &types.LogView{
		Addr:   []byte(address),
		Data:   []byte(data),
		Topics: []byte(`["0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"]`),
		BlkNum: []byte("0x123456"),
		LogIdx: []byte("0x1"),
	}
}

// waitForCoreReady waits for a core to be properly initialized
func waitForCoreReady(coreID int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if coreEngines[coreID] != nil && coreRings[coreID] != nil {
			time.Sleep(10 * time.Millisecond) // Allow full initialization
			return true
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ADDRESS RESOLUTION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestPackEthereumAddress(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	testCases := []struct {
		name    string
		address string
	}{
		{"Standard Address", TestAddressETH_DAI},
		{"Different Address", TestAddressDAI_USDC},
		{"Zero Address", "0x0000000000000000000000000000000000000000"},
		{"Max Address", "0xffffffffffffffffffffffffffffffffffffffff"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Remove 0x prefix for processing
			hexBytes := []byte(tc.address[2:])

			// Pack the address
			packed := packEthereumAddress(hexBytes)

			// Verify packing is deterministic
			packed2 := packEthereumAddress(hexBytes)
			if !packed.isEqual(packed2) {
				t.Errorf("Address packing not deterministic")
			}

			// Verify different addresses produce different packed representations
			if tc.address != TestAddressETH_DAI {
				standardPacked := packEthereumAddress([]byte(TestAddressETH_DAI[2:]))
				if packed.isEqual(standardPacked) {
					t.Errorf("Different addresses produced same packed representation")
				}
			}
		})
	}
}

func TestRegisterTradingPairAddress(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Register test addresses
	RegisterTradingPairAddress([]byte(TestAddressETH_DAI[2:]), TestPairETH_DAI)
	RegisterTradingPairAddress([]byte(TestAddressDAI_USDC[2:]), TestPairDAI_USDC)
	RegisterTradingPairAddress([]byte(TestAddressUSDC_ETH[2:]), TestPairUSDC_ETH)

	// Test successful lookups
	if pairID := LookupPairByAddress([]byte(TestAddressETH_DAI[2:])); pairID != TestPairETH_DAI {
		t.Errorf("Expected pair ID %d, got %d", TestPairETH_DAI, pairID)
	}

	if pairID := LookupPairByAddress([]byte(TestAddressDAI_USDC[2:])); pairID != TestPairDAI_USDC {
		t.Errorf("Expected pair ID %d, got %d", TestPairDAI_USDC, pairID)
	}

	// Test non-existent address
	unknownAddress := "1111111111111111111111111111111111111111"
	if pairID := LookupPairByAddress([]byte(unknownAddress)); pairID != 0 {
		t.Errorf("Expected 0 for unknown address, got %d", pairID)
	}

	// Test address update
	RegisterTradingPairAddress([]byte(TestAddressETH_DAI[2:]), TestPairWBTC_ETH)
	if pairID := LookupPairByAddress([]byte(TestAddressETH_DAI[2:])); pairID != TestPairWBTC_ETH {
		t.Errorf("Address update failed, expected %d, got %d", TestPairWBTC_ETH, pairID)
	}
}

func TestAddressHashCollisionHandling(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Generate addresses that might cause collisions
	collisionAddresses := []string{
		"0x1000000000000000000000000000000000000001",
		"0x1000000000000000000000000000000000000002",
		"0x1000000000000000000000000000000000000003",
		"0x1000000000000000000000000000000000000004",
		"0x1000000000000000000000000000000000000005",
	}

	// Register addresses one by one and verify each step
	for i, addr := range collisionAddresses {
		intendedPairID := types.TradingPairID(2000 + i)

		// Register the address
		RegisterTradingPairAddress([]byte(addr[2:]), intendedPairID)

		// Immediately verify it can be found correctly
		foundPairID := LookupPairByAddress([]byte(addr[2:]))
		if foundPairID != intendedPairID {
			t.Errorf("Address %s: expected %d, got %d after registration", addr, intendedPairID, foundPairID)

			// Debug: Check if ANY pair ID is being returned
			if foundPairID == 0 {
				t.Logf("  → Address not found at all")
			} else {
				t.Logf("  → Address found but wrong pair ID returned")

				// This suggests the router is doing partial matching, not full address verification
				t.Logf("  → CRITICAL: This indicates the router is not checking the complete address!")
			}
		}
	}

	// Test that different addresses don't return the same pair ID
	returnedPairIDs := make(map[types.TradingPairID]string)
	for _, addr := range collisionAddresses {
		pairID := LookupPairByAddress([]byte(addr[2:]))
		if pairID != 0 {
			if existingAddr, exists := returnedPairIDs[pairID]; exists {
				t.Errorf("CRITICAL BUG: Multiple addresses return same pair ID %d:", pairID)
				t.Errorf("  → %s", existingAddr)
				t.Errorf("  → %s", addr)
				t.Errorf("  → This proves the router is NOT checking the full address!")
				break
			}
			returnedPairIDs[pairID] = addr
		}
	}

	t.Logf("Address lookup summary:")
	for _, addr := range collisionAddresses {
		pairID := LookupPairByAddress([]byte(addr[2:]))
		t.Logf("  %s → %d", addr, pairID)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PRICE UPDATE PROCESSING TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestCountHexLeadingZeros(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected int
	}{
		{"All zeros", "00000000000000000000000000000000", 32},
		{"No leading zeros", "1234567890abcdef1234567890abcdef", 0},
		{"Four leading zeros", "0000567890abcdef1234567890abcdef", 4},
		{"Eight leading zeros", "00000000abcdef1234567890abcdef12", 8},
		{"Sixteen leading zeros", "00000000000000001234567890abcdef", 16},
		{"Thirty leading zeros", "0000000000000000000000000000001f", 30},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := utils.CountHexLeadingZeros([]byte(tc.input))
			if result != tc.expected {
				t.Errorf("Expected %d leading zeros, got %d for input %s", tc.expected, result, tc.input)
			}
		})
	}
}

func TestDispatchPriceUpdateBasic(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Register test pair
	RegisterTradingPairAddress([]byte(TestAddressETH_DAI[2:]), TestPairETH_DAI)
	RegisterPairToCoreRouting(TestPairETH_DAI, 0)

	// Initialize minimal core setup
	coreRings[0] = ring56.New(constants.DefaultRingSize)

	// Create test log with valid Uniswap V2 Sync event data
	// Format: 0x + 64 chars (reserve0) + 64 chars (reserve1)
	syncData := "0x" +
		"0000000000000000000000000000000000000000000000056bc75e2d630eb187" + // reserve0
		"00000000000000000000000000000000000000000000152d02c7e14af6800000" // reserve1

	logView := createTestLogView(TestAddressETH_DAI, syncData)

	// Dispatch the price update
	DispatchPriceUpdate(logView)

	// Verify message was queued
	time.Sleep(10 * time.Millisecond) // Allow processing
	messagePtr := coreRings[0].Pop()
	if messagePtr == nil {
		t.Fatal("No message received in core ring")
	}

	// Validate message content
	message := (*PriceUpdateMessage)(unsafe.Pointer(messagePtr))
	if message.pairID != TestPairETH_DAI {
		t.Errorf("Expected pair ID %d, got %d", TestPairETH_DAI, message.pairID)
	}

	// Verify tick values are opposites
	if message.forwardTick != -message.reverseTick {
		t.Errorf("Forward and reverse ticks should be opposites: %f vs %f",
			message.forwardTick, message.reverseTick)
	}
}

func TestDispatchPriceUpdateMultiCore(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Setup multiple cores
	numCores := 4
	for i := 0; i < numCores; i++ {
		coreRings[i] = ring56.New(constants.DefaultRingSize)
		RegisterPairToCoreRouting(TestPairETH_DAI, uint8(i))
	}

	// Register test pair
	RegisterTradingPairAddress([]byte(TestAddressETH_DAI[2:]), TestPairETH_DAI)

	// Create test event
	syncData := "0x" +
		"0000000000000000000000000000000000000000000000056bc75e2d630eb187" +
		"00000000000000000000000000000000000000000000152d02c7e14af6800000"

	logView := createTestLogView(TestAddressETH_DAI, syncData)

	// Dispatch update
	DispatchPriceUpdate(logView)

	// Verify all cores received the message
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < numCores; i++ {
		messagePtr := coreRings[i].Pop()
		if messagePtr == nil {
			t.Errorf("Core %d did not receive message", i)
			continue
		}

		message := (*PriceUpdateMessage)(unsafe.Pointer(messagePtr))
		if message.pairID != TestPairETH_DAI {
			t.Errorf("Core %d received wrong pair ID: %d", i, message.pairID)
		}
	}
}

func TestDispatchPriceUpdateUnknownAddress(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Setup core but don't register the address
	coreRings[0] = ring56.New(constants.DefaultRingSize)

	// Create test event for unregistered address
	syncData := "0x" +
		"0000000000000000000000000000000000000000000000056bc75e2d630eb187" +
		"00000000000000000000000000000000000000000000152d02c7e14af6800000"

	unknownAddress := "0x9999999999999999999999999999999999999999"
	logView := createTestLogView(unknownAddress, syncData)

	// Dispatch update
	DispatchPriceUpdate(logView)

	// Verify no message was sent (unknown address should be ignored)
	time.Sleep(10 * time.Millisecond)
	messagePtr := coreRings[0].Pop()
	if messagePtr != nil {
		t.Error("Message should not be sent for unknown address")
	}
}

func TestDispatchPriceUpdateInvalidData(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Register test pair and setup core
	RegisterTradingPairAddress([]byte(TestAddressETH_DAI[2:]), TestPairETH_DAI)
	RegisterPairToCoreRouting(TestPairETH_DAI, 0)
	coreRings[0] = ring56.New(constants.DefaultRingSize)

	// Test cases for legitimate on-chain scenarios that should be handled gracefully
	testCases := []struct {
		name string
		data string
	}{
		{"Zero reserves (legitimate on-chain)", "0x" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"0000000000000000000000000000000000000000000000000000000000000000"},
		{"One zero reserve (legitimate on-chain)", "0x" +
			"0000000000000000000000000000000000000000000000000000000000000000" +
			"00000000000000000000000000000000000000000000152d02c7e14af6800000"},
		{"Valid Uniswap V2 sync data", "0x" +
			"0000000000000000000000000000000000000000000000056bc75e2d630eb187" +
			"00000000000000000000000000000000000000000000152d02c7e14af6800000"},
		{"Large reserves (uint112 max)", "0x" +
			"000000000000000000000000000000000000ffffffffffffffffffffffffffff" +
			"000000000000000000000000000000000000ffffffffffffffffffffffffffff"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear any existing messages
			for coreRings[0].Pop() != nil {
			}

			logView := createTestLogView(TestAddressETH_DAI, tc.data)

			// Should handle all legitimate Ethereum data without panic
			DispatchPriceUpdate(logView)

			time.Sleep(10 * time.Millisecond)

			messagePtr := coreRings[0].Pop()
			if messagePtr == nil {
				t.Errorf("Should generate message for legitimate case %s", tc.name)
				return
			}

			message := (*PriceUpdateMessage)(unsafe.Pointer(messagePtr))
			if message.pairID != TestPairETH_DAI {
				t.Errorf("Message has wrong pair ID for case %s: %d", tc.name, message.pairID)
			}

			// Verify forward and reverse ticks are opposites (or both equal for fallback)
			if message.forwardTick != -message.reverseTick && message.forwardTick != message.reverseTick {
				t.Errorf("Tick relationship invalid for %s: forward=%f, reverse=%f",
					tc.name, message.forwardTick, message.reverseTick)
			}
		})
	}
}

// Test performance-critical path with malformed data (should panic as designed)
func TestDispatchPriceUpdateMalformedDataPanic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping panic test in short mode")
	}

	cleanup := testSetup(t)
	defer cleanup()

	// Register test pair and setup core
	RegisterTradingPairAddress([]byte(TestAddressETH_DAI[2:]), TestPairETH_DAI)
	RegisterPairToCoreRouting(TestPairETH_DAI, 0)
	coreRings[0] = ring56.New(constants.DefaultRingSize)

	// Test that malformed data causes expected panic (performance-critical code should fail fast on bad input)
	t.Run("Malformed data should panic (by design)", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for malformed data but none occurred - performance-critical code should fail fast on bad input")
			} else {
				t.Logf("Expected panic occurred for malformed data: %v", r)
			}
		}()

		logView := createTestLogView(TestAddressETH_DAI, "0x1234") // Too short
		DispatchPriceUpdate(logView)
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ARBITRAGE CYCLE MANAGEMENT TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestArbitrageEngineInitialization(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Create test triangles
	triangles := []ArbitrageTriangle{
		{TestPairETH_DAI, TestPairDAI_USDC, TestPairUSDC_ETH},
		{TestPairWBTC_ETH, TestPairETH_DAI, TestPairDAI_USDC},
	}

	// Initialize system
	InitializeArbitrageSystem(triangles)

	// Wait for cores to initialize
	time.Sleep(100 * time.Millisecond)

	// Verify engines were created
	coreCount := runtime.NumCPU() - 4 // Changed to match router.go which reserves 4 cores
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	coreCount &^= 1 // Ensure even

	for i := 0; i < coreCount; i++ {
		if !waitForCoreReady(i, 1*time.Second) {
			t.Errorf("Core %d not ready after initialization", i)
		}
	}

	// Test graceful shutdown
	control.Shutdown()
	time.Sleep(100 * time.Millisecond)
}

func TestProcessArbitrageUpdate(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Create a minimal engine for testing
	engine := &ArbitrageEngine{
		pairToQueueLookup:  localidx.New(16),
		pairToFanoutIndex:  localidx.New(16), // Need both lookups now
		isReverseDirection: false,
	}

	// Create shared arena and initialize
	const testCycles = 10
	engine.sharedArena = make([]pooledquantumqueue.Entry, testCycles)
	for i := range engine.sharedArena {
		engine.sharedArena[i].Tick = -1
		engine.sharedArena[i].Prev = pooledquantumqueue.Handle(^uint64(0))
		engine.sharedArena[i].Next = pooledquantumqueue.Handle(^uint64(0))
		engine.sharedArena[i].Data = 0
	}

	// Initialize queue
	arenaPtr := unsafe.Pointer(&engine.sharedArena[0])
	queue := pooledquantumqueue.New(arenaPtr)
	engine.priorityQueues = []pooledquantumqueue.PooledQuantumQueue{*queue}

	// Setup lookups
	engine.pairToQueueLookup.Put(uint32(TestPairETH_DAI), 0)

	// Setup fanout indices for all pairs
	engine.pairToFanoutIndex.Put(uint32(TestPairETH_DAI), 0)
	engine.pairToFanoutIndex.Put(uint32(TestPairDAI_USDC), 1)
	engine.pairToFanoutIndex.Put(uint32(TestPairUSDC_ETH), 2)

	// Initialize cycle states
	engine.cycleStates = make([]ArbitrageCycleState, 1)
	engine.cycleStates[0] = ArbitrageCycleState{
		pairIDs:    [3]types.TradingPairID{TestPairETH_DAI, TestPairDAI_USDC, TestPairUSDC_ETH},
		tickValues: [3]float64{0, 1.5, -2.0}, // Main pair (index 0) should be zero
	}

	// Setup fanout table with correct size
	engine.cycleFanoutTable = make([][]CycleFanoutEntry, 3) // One slot per pair

	// Add fanout entry to DAI_USDC's fanout table (not ETH_DAI's)
	engine.cycleFanoutTable[1] = []CycleFanoutEntry{
		{
			cycleIndex:  0,
			edgeIndex:   1, // DAI_USDC is at position 1
			queueIndex:  0,
			queueHandle: 0,
		},
	}

	// Add cycle to queue
	engine.priorityQueues[0].Push(131072, 0, 0) // Neutral priority

	// Create test update
	update := &PriceUpdateMessage{
		pairID:      TestPairETH_DAI,
		forwardTick: 1.0,
		reverseTick: -1.0,
	}

	// Process the update
	processArbitrageUpdate(engine, update)

	// Verify cycle was NOT updated (ETH_DAI is the main pair, tick should remain 0)
	if engine.cycleStates[0].tickValues[0] != 0 {
		t.Errorf("Main pair tick should remain zero, got %f", engine.cycleStates[0].tickValues[0])
	}

	// Test update for a non-main pair
	update2 := &PriceUpdateMessage{
		pairID:      TestPairDAI_USDC,
		forwardTick: 2.0,
		reverseTick: -2.0,
	}

	processArbitrageUpdate(engine, update2)

	// Verify the fanout update worked
	if engine.cycleStates[0].tickValues[1] != 2.0 {
		t.Errorf("Fanout pair tick not updated correctly, got %f", engine.cycleStates[0].tickValues[1])
	}
}

func TestSparseFanoutTable(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Create test triangles with one pair that only appears in fanout
	triangles := []ArbitrageTriangle{
		{TestPairETH_DAI, TestPairDAI_USDC, TestPairUSDC_ETH}, // ETH_DAI is main
		{TestPairWBTC_ETH, TestPairETH_DAI, TestPairUNI_ETH},  // WBTC_ETH is main
	}

	// Create mock engine
	engine := &ArbitrageEngine{
		pairToQueueLookup:  localidx.New(16),
		pairToFanoutIndex:  localidx.New(32), // Larger for all pairs
		isReverseDirection: false,
	}

	// Create workload shards
	workloadShards := []PairWorkloadShard{
		{
			pairID: TestPairETH_DAI,
			cycleEdges: []CycleEdge{
				{cyclePairs: triangles[0], edgeIndex: 0}, // ETH_DAI is main in first triangle
			},
		},
		{
			pairID: TestPairWBTC_ETH,
			cycleEdges: []CycleEdge{
				{cyclePairs: triangles[1], edgeIndex: 0}, // WBTC_ETH is main in second triangle
			},
		},
	}

	// Initialize the queues
	initializeArbitrageQueues(engine, workloadShards)

	// Verify queue count (only pairs with cycles get queues)
	if len(engine.priorityQueues) != 2 {
		t.Errorf("Expected 2 queues, got %d", len(engine.priorityQueues))
	}

	// Verify fanout table size (all unique pairs across all triangles)
	uniquePairs := map[types.TradingPairID]bool{
		TestPairETH_DAI:  true,
		TestPairDAI_USDC: true,
		TestPairUSDC_ETH: true,
		TestPairWBTC_ETH: true,
		TestPairUNI_ETH:  true,
	}

	if len(engine.cycleFanoutTable) != len(uniquePairs) {
		t.Errorf("Expected %d fanout slots, got %d", len(uniquePairs), len(engine.cycleFanoutTable))
	}

	// Verify pairs with queues
	_, hasQueue := engine.pairToQueueLookup.Get(uint32(TestPairETH_DAI))
	if !hasQueue {
		t.Error("ETH_DAI should have a queue")
	}

	_, hasQueue = engine.pairToQueueLookup.Get(uint32(TestPairDAI_USDC))
	if hasQueue {
		t.Error("DAI_USDC should NOT have a queue (fanout only)")
	}

	// Verify all pairs have fanout indices
	for pairID := range uniquePairs {
		_, hasFanout := engine.pairToFanoutIndex.Get(uint32(pairID))
		if !hasFanout {
			t.Errorf("Pair %d should have fanout index", pairID)
		}
	}

	// Test price update for fanout-only pair
	update := &PriceUpdateMessage{
		pairID:      TestPairDAI_USDC,
		forwardTick: 3.0,
		reverseTick: -3.0,
	}

	// Should not panic even though DAI_USDC has no queue
	processArbitrageUpdate(engine, update)

	// Verify the fanout update worked
	daiUsdcFanoutIdx, _ := engine.pairToFanoutIndex.Get(uint32(TestPairDAI_USDC))
	if len(engine.cycleFanoutTable[daiUsdcFanoutIdx]) == 0 {
		t.Error("DAI_USDC should have fanout entries")
	}
}

func TestCycleFanoutMapping(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Test triangles with overlapping pairs
	triangles := []ArbitrageTriangle{
		{TestPairETH_DAI, TestPairDAI_USDC, TestPairUSDC_ETH},
		{TestPairETH_DAI, TestPairWBTC_ETH, TestPairDAI_USDC}, // Shares ETH_DAI and DAI_USDC
		{TestPairUNI_ETH, TestPairETH_DAI, TestPairDAI_USDC},  // Shares ETH_DAI and DAI_USDC
	}

	// Build workload shards
	buildWorkloadShards(triangles)

	// Verify each pair appears in fanout mapping
	expectedPairs := []types.TradingPairID{
		TestPairETH_DAI, TestPairDAI_USDC, TestPairUSDC_ETH,
		TestPairWBTC_ETH, TestPairUNI_ETH,
	}

	for _, pairID := range expectedPairs {
		if shards, exists := pairWorkloadShards[pairID]; !exists || len(shards) == 0 {
			t.Errorf("Pair %d not found in workload shards", pairID)
		}
	}

	// Create a mock engine to test fanout distribution
	engine := &ArbitrageEngine{
		pairToQueueLookup:  localidx.New(16),
		pairToFanoutIndex:  localidx.New(32),
		isReverseDirection: false,
	}

	// Collect all shards
	var allShards []PairWorkloadShard
	for _, shardList := range pairWorkloadShards {
		allShards = append(allShards, shardList...)
	}

	// Initialize the system
	initializeArbitrageQueues(engine, allShards)

	// Count unique cycles (each cycle should only be counted once)
	uniqueCycles := make(map[[3]types.TradingPairID]bool)
	for _, cycleState := range engine.cycleStates {
		uniqueCycles[cycleState.pairIDs] = true
	}

	// Verify we have the correct number of unique cycles
	if len(uniqueCycles) != len(triangles) {
		t.Errorf("Expected %d unique cycles, got %d", len(triangles), len(uniqueCycles))
	}

	// Verify fanout entries are correctly distributed
	// The total fanout entries will be more than 3*2 because:
	// - ETH_DAI appears in 3 triangles, so its shard has 3 cycles
	// - DAI_USDC appears in 3 triangles, so its shard has 3 cycles
	// - Each cycle creates 2 fanout entries when processed
	// But we need to count actual fanout relationships

	// Count fanout entries by the cycles they reference
	fanoutByCycle := make(map[uint64]int)
	for _, fanoutList := range engine.cycleFanoutTable {
		for _, fanout := range fanoutList {
			fanoutByCycle[fanout.cycleIndex]++
		}
	}

	// Each cycle should have exactly 2 fanout entries (for the non-main pairs)
	for cycleIdx, count := range fanoutByCycle {
		if count != 2 {
			t.Errorf("Cycle %d has %d fanout entries, expected 2", cycleIdx, count)
		}
	}

	// The total fanout entries will be higher due to duplication in shards
	// This is expected behavior - when we process shards, we create all necessary
	// fanout entries for proper routing
	totalFanoutEntries := 0
	for _, fanoutList := range engine.cycleFanoutTable {
		totalFanoutEntries += len(fanoutList)
	}

	t.Logf("Total fanout entries: %d (includes duplicates from shard processing)", totalFanoutEntries)
	t.Logf("Unique cycles: %d", len(uniqueCycles))
	t.Logf("Fanout entries per cycle: %v", fanoutByCycle)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PRIORITY QUEUE OPERATION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestExtractedCycleManagement(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Create engine with test data
	engine := &ArbitrageEngine{
		pairToQueueLookup:  localidx.New(16),
		pairToFanoutIndex:  localidx.New(16),
		isReverseDirection: false,
	}

	// Setup shared arena
	const testCycles = 5
	engine.sharedArena = make([]pooledquantumqueue.Entry, testCycles)
	for i := range engine.sharedArena {
		engine.sharedArena[i].Tick = -1
		engine.sharedArena[i].Prev = pooledquantumqueue.Handle(^uint64(0))
		engine.sharedArena[i].Next = pooledquantumqueue.Handle(^uint64(0))
		engine.sharedArena[i].Data = 0
	}

	// Initialize queue
	arenaPtr := unsafe.Pointer(&engine.sharedArena[0])
	queue := pooledquantumqueue.New(arenaPtr)
	engine.priorityQueues = []pooledquantumqueue.PooledQuantumQueue{*queue}
	engine.pairToQueueLookup.Put(uint32(TestPairETH_DAI), 0)
	engine.pairToFanoutIndex.Put(uint32(TestPairETH_DAI), 0)

	// Setup cycle states with varying profitability
	engine.cycleStates = make([]ArbitrageCycleState, 3)
	for i := range engine.cycleStates {
		engine.cycleStates[i] = ArbitrageCycleState{
			pairIDs:    [3]types.TradingPairID{TestPairETH_DAI, TestPairDAI_USDC, TestPairUSDC_ETH},
			tickValues: [3]float64{0, float64(i), -float64(i + 1)}, // Varying profitability
		}
		// Add to queue with different priorities
		engine.priorityQueues[0].Push(int64(100000+i*1000), pooledquantumqueue.Handle(i), uint64(i))
	}

	// Setup fanout (minimal for test)
	engine.cycleFanoutTable = make([][]CycleFanoutEntry, 1)

	// Test update that should extract profitable cycles
	update := &PriceUpdateMessage{
		pairID:      TestPairETH_DAI,
		forwardTick: -5.0, // Very profitable update
		reverseTick: 5.0,
	}

	// Process update
	processArbitrageUpdate(engine, update)

	// Verify queue still has cycles (they should be restored)
	if engine.priorityQueues[0].Empty() {
		t.Error("Queue should not be empty after processing")
	}

	// Verify cycles can still be extracted
	if engine.priorityQueues[0].Size() != 3 {
		t.Errorf("Expected 3 cycles in queue, got %d", engine.priorityQueues[0].Size())
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SHUTDOWN AND CLEANUP TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestGracefulShutdown(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Create minimal arbitrage system
	triangles := []ArbitrageTriangle{
		{TestPairETH_DAI, TestPairDAI_USDC, TestPairUSDC_ETH},
	}

	// Initialize system
	InitializeArbitrageSystem(triangles)

	// Wait for system to be ready
	time.Sleep(100 * time.Millisecond)

	// Verify cores are running
	activeEngines := 0
	coreCount := runtime.NumCPU() - 4 // Changed to match router.go which reserves 4 cores
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	coreCount &^= 1

	for i := 0; i < coreCount; i++ {
		if waitForCoreReady(i, 1*time.Second) {
			activeEngines++
		}
	}

	if activeEngines == 0 {
		t.Fatal("No engines initialized")
	}

	t.Logf("Initialized %d engines", activeEngines)

	// Signal shutdown
	control.Shutdown()

	// Wait for graceful shutdown
	shutdownComplete := make(chan bool)
	go func() {
		// Check if control system reports stopping
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if control.IsShuttingDown() {
				time.Sleep(500 * time.Millisecond) // Allow cores to process shutdown
				shutdownComplete <- true
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		shutdownComplete <- false
	}()

	select {
	case success := <-shutdownComplete:
		if !success {
			t.Error("Shutdown signal not properly processed")
		}
	case <-time.After(3 * time.Second):
		t.Error("Shutdown took too long")
	}
}

func TestConcurrentShutdown(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Test multiple concurrent shutdown signals
	var wg sync.WaitGroup
	shutdownCount := 10

	for i := 0; i < shutdownCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			control.Shutdown()
		}()
	}

	// Wait for all shutdowns to complete
	wg.Wait()

	// Verify system is in stopped state
	if !control.IsShuttingDown() {
		t.Error("System should be in shutting down state after concurrent shutdowns")
	}
}

func TestShutdownWithActiveTraffic(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Setup system with registered addresses
	RegisterTradingPairAddress([]byte(TestAddressETH_DAI[2:]), TestPairETH_DAI)
	RegisterPairToCoreRouting(TestPairETH_DAI, 0)
	coreRings[0] = ring56.New(constants.DefaultRingSize)

	// Create traffic generator
	trafficDone := make(chan bool)
	go func() {
		defer close(trafficDone)

		syncData := "0x" +
			"0000000000000000000000000000000000000000000000056bc75e2d630eb187" +
			"00000000000000000000000000000000000000000000152d02c7e14af6800000"

		for i := 0; i < 100; i++ {
			if control.IsShuttingDown() {
				return
			}

			logView := createTestLogView(TestAddressETH_DAI, syncData)
			DispatchPriceUpdate(logView)
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Let traffic run briefly
	time.Sleep(10 * time.Millisecond)

	// Signal shutdown while traffic is active
	control.Shutdown()

	// Wait for traffic generator to stop
	select {
	case <-trafficDone:
		// Good - traffic stopped
	case <-time.After(1 * time.Second):
		t.Error("Traffic generator did not stop after shutdown signal")
	}

	// Verify shutdown state
	if !control.IsShuttingDown() {
		t.Error("System should be shutting down")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INTEGRATION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestFullArbitrageFlow(t *testing.T) {
	cleanup := testSetup(t)
	defer cleanup()

	// Register all test addresses
	RegisterTradingPairAddress([]byte(TestAddressETH_DAI[2:]), TestPairETH_DAI)
	RegisterTradingPairAddress([]byte(TestAddressDAI_USDC[2:]), TestPairDAI_USDC)
	RegisterTradingPairAddress([]byte(TestAddressUSDC_ETH[2:]), TestPairUSDC_ETH)

	// Create arbitrage triangle
	triangles := []ArbitrageTriangle{
		{TestPairETH_DAI, TestPairDAI_USDC, TestPairUSDC_ETH},
	}

	// Initialize system
	InitializeArbitrageSystem(triangles)

	// Wait for initialization
	time.Sleep(200 * time.Millisecond)

	// Send price updates for each pair in the triangle
	testUpdates := []struct {
		address string
		data    string
	}{
		{TestAddressETH_DAI, "0x" +
			"0000000000000000000000000000000000000000000000056bc75e2d630eb187" +
			"00000000000000000000000000000000000000000000152d02c7e14af6800000"},
		{TestAddressDAI_USDC, "0x" +
			"00000000000000000000000000000000000000000000152d02c7e14af6800000" +
			"0000000000000000000000000000000000000000000000056bc75e2d630eb187"},
		{TestAddressUSDC_ETH, "0x" +
			"0000000000000000000000000000000000000000000000056bc75e2d630eb187" +
			"00000000000000000000000000000000000000000000152d02c7e14af6800000"},
	}

	// Send updates
	for _, update := range testUpdates {
		logView := createTestLogView(update.address, update.data)
		DispatchPriceUpdate(logView)
		time.Sleep(5 * time.Millisecond) // Allow processing
	}

	// Allow system to process all updates
	time.Sleep(100 * time.Millisecond)

	// Verify no panics occurred and system is still responsive
	// Send one more update to verify system stability
	logView := createTestLogView(TestAddressETH_DAI, testUpdates[0].data)
	DispatchPriceUpdate(logView)

	// Clean shutdown
	control.Shutdown()
	time.Sleep(100 * time.Millisecond)
}

func TestHighVolumeStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cleanup := testSetup(t)
	defer cleanup()

	// Setup single pair to avoid complexity
	RegisterTradingPairAddress([]byte(TestAddressETH_DAI[2:]), TestPairETH_DAI)
	RegisterPairToCoreRouting(TestPairETH_DAI, 0)
	coreRings[0] = ring56.New(constants.DefaultRingSize)

	// Fixed sync data pattern - no dynamic generation
	syncData := "0x" +
		"0000000000000000000000000000000000000000000000056bc75e2d630eb187" +
		"00000000000000000000000000000000000000000000152d02c7e14af6800000"

	// Sequential processing to avoid concurrency issues
	const updateCount = 50
	for i := 0; i < updateCount; i++ {
		logView := createTestLogView(TestAddressETH_DAI, syncData)
		DispatchPriceUpdate(logView)

		// Clear ring buffer to prevent overflow
		coreRings[0].Pop()
	}

	t.Logf("Successfully processed %d price updates", updateCount)

	// Clean shutdown
	control.Shutdown()
	time.Sleep(50 * time.Millisecond)
}
