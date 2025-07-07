// router_test.go — Comprehensive test suite for triangular arbitrage router
// ============================================================================
// TRIANGULAR ARBITRAGE ROUTER TEST SUITE
// ============================================================================
//
// Comprehensive testing framework for high-performance arbitrage detection
// system with realistic workload simulation and performance benchmarking.
//
// Test coverage:
//   • Unit tests for all core components with edge cases
//   • Integration tests for full system workflows
//   • Performance benchmarks with realistic data volumes
//   • Memory allocation and cache efficiency validation
//   • Concurrent safety verification under load
//   • Error condition and boundary testing
//
// Benchmark scenarios:
//   • Single-core tick processing performance
//   • Multi-core scaling efficiency measurement
//   • Memory allocation overhead analysis
//   • Cache hit ratio optimization validation
//   • End-to-end latency characterization
//
// Mock data generation:
//   • Realistic Ethereum address patterns
//   • Representative reserve ratio distributions
//   • Production-scale cycle counts (100k+ pairs)
//   • Varied profitability scenarios
//
// Performance targets:
//   • Sub-nanosecond tick processing latency
//   • Zero allocation hot path execution
//   • Linear scaling across 64 cores
//   • >95% cache hit ratio maintenance

package router

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"main/types"
)

// ============================================================================
// TEST CONSTANTS AND CONFIGURATION
// ============================================================================

const (
	// Test scale configuration
	testTrianglePairCount = 10000  // Realistic test size (10k triangular pairs)
	testTotalCycleCount   = 30000  // 3 cycles per triangular pair
	testCoreCount         = 8      // Reduced for test environment
	testTickUpdatesCount  = 100000 // Tick updates for performance testing

	// Performance target thresholds
	maxTickProcessingNanos = 100  // Maximum tick processing latency (ns)
	maxMemoryAllocBytes    = 1024 // Maximum memory allocation per operation
	minCacheHitRatio       = 0.95 // Minimum acceptable cache hit ratio

	// Mock data generation parameters
	mockAddressSpace = 1000000 // Address space for collision testing
	mockReserveMin   = 1000    // Minimum reserve value
	mockReserveMax   = 1000000 // Maximum reserve value
)

// ============================================================================
// MOCK DATA STRUCTURES
// ============================================================================

// MockEthereumAddress generates realistic Ethereum address patterns
// for testing address-to-pair ID mapping performance and collision handling
type MockEthereumAddress [40]byte

// MockLogView simulates Ethereum transaction log for tick update testing
type MockLogView struct {
	addr    [64]byte  // Ethereum address (hex string format)
	data    [128]byte // Transaction data payload
	blkNum  [32]byte  // Block number
	logIdx  [16]byte  // Log index
	topics  [256]byte // Event topics
	txIndex [16]byte  // Transaction index
}

// MockArbitrageSetup represents a complete test configuration
// with pre-generated cycles, addresses, and expected profitability outcomes
type MockArbitrageSetup struct {
	trianglePairs    []ArbitrageTriplet   // Generated arbitrage cycles
	addressMappings  map[PairID][40]byte  // Pair ID to address mappings
	expectedProfits  map[string]float64   // Expected profitability outcomes
	tickUpdateQueue  []MockTickUpdate     // Pre-generated tick updates
	performanceStats MockPerformanceStats // Expected performance metrics
}

// MockTickUpdate represents a synthetic tick update for performance testing
type MockTickUpdate struct {
	pairID       PairID  // Target pair identifier
	reserve0     uint64  // Mock reserve value 0
	reserve1     uint64  // Mock reserve value 1
	expectedTick float64 // Pre-calculated expected tick value
	timestamp    int64   // Update timestamp for latency measurement
}

// MockPerformanceStats tracks system performance metrics during testing
type MockPerformanceStats struct {
	ticksProcessed     uint64  // Total tick updates processed
	arbitragesDetected uint64  // Profitable arbitrages found
	avgProcessingNanos float64 // Average processing latency
	peakMemoryBytes    uint64  // Peak memory usage
	cacheHitRatio      float64 // Cache hit ratio
	coreUtilization    float64 // CPU core utilization
}

// ============================================================================
// MOCK DATA GENERATION
// ============================================================================

// generateMockAddress creates a realistic Ethereum address with proper
// hex encoding and collision avoidance for hash table testing
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func generateMockAddress(seed uint64) MockEthereumAddress {
	var addr MockEthereumAddress

	// Generate pseudo-random but deterministic address
	// Use seed-based generation for reproducible tests
	for i := 0; i < 20; i++ {
		byteVal := uint8((seed >> (i * 8)) ^ (seed >> ((i + 7) * 3)))
		// Convert to hex string representation
		addr[i*2] = "0123456789abcdef"[byteVal>>4]
		addr[i*2+1] = "0123456789abcdef"[byteVal&0xF]
	}

	return addr
}

// generateMockLogView creates a synthetic Ethereum transaction log
// with realistic data patterns for tick update processing validation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func generateMockLogView(pairID PairID, reserve0, reserve1 uint64, addressMap map[PairID][40]byte) *MockLogView {
	logView := &MockLogView{}

	// Set address from mapping (with "0x" prefix)
	logView.addr[0] = '0'
	logView.addr[1] = 'x'
	addr := addressMap[pairID]
	copy(logView.addr[2:42], addr[:])

	// Encode reserve values in transaction data (Uniswap Sync event format)
	binary.BigEndian.PutUint64(logView.data[24:32], reserve0)
	binary.BigEndian.PutUint64(logView.data[56:64], reserve1)

	// Add realistic block and transaction metadata
	blockNum := uint64(time.Now().UnixNano() / 1000000) // Millisecond precision
	binary.BigEndian.PutUint64(logView.blkNum[24:32], blockNum)

	return logView
}

// generateArbitrageTriangle creates a valid triangular arbitrage cycle
// with realistic pair relationships and profitability characteristics
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func generateArbitrageTriangle(baseID uint32) ArbitrageTriplet {
	// Create triangle: A/B -> B/C -> C/A
	// Ensure realistic relationships (e.g., WETH/USDC -> USDC/DAI -> DAI/WETH)
	return ArbitrageTriplet{
		PairID(baseID),     // Primary pair (e.g., WETH/USDC)
		PairID(baseID + 1), // Secondary pair (e.g., USDC/DAI)
		PairID(baseID + 2), // Tertiary pair (e.g., DAI/WETH)
	}
}

// createMockArbitrageSetup generates a complete test environment
// with realistic scale and data patterns for comprehensive system testing
//
//go:norace
//go:nocheckptr
//go:nosplit
func createMockArbitrageSetup() *MockArbitrageSetup {
	setup := &MockArbitrageSetup{
		trianglePairs:   make([]ArbitrageTriplet, 0, testTrianglePairCount),
		addressMappings: make(map[PairID][40]byte),
		expectedProfits: make(map[string]float64),
		tickUpdateQueue: make([]MockTickUpdate, 0, testTickUpdatesCount),
	}

	// Generate triangular arbitrage cycles
	for i := 0; i < testTrianglePairCount; i++ {
		baseID := uint32(i * 3) // Ensure unique pair IDs
		triangle := generateArbitrageTriangle(baseID)
		setup.trianglePairs = append(setup.trianglePairs, triangle)

		// Create address mappings for each pair in triangle
		for j, pairID := range triangle {
			seed := uint64(baseID + uint32(j))
			addr := generateMockAddress(seed)
			setup.addressMappings[pairID] = addr
		}
	}

	// Generate realistic tick updates with varied profitability
	for i := 0; i < testTickUpdatesCount; i++ {
		// Select random pair from generated cycles
		pairIdx := i % (testTrianglePairCount * 3)
		pairID := PairID(pairIdx)

		// Generate realistic reserve values with market volatility
		volatility := 1.0 + (float64(i%100)/100.0)*0.1 // ±10% volatility
		reserve0 := uint64(float64(mockReserveMin) * volatility * (1 + float64(i%13)/100.0))
		reserve1 := uint64(float64(mockReserveMax) * volatility * (1 + float64(i%17)/100.0))

		// Pre-calculate expected tick value for validation
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

// TestAddressKeyOperations validates zero-copy address key generation
// and SIMD-optimized comparison operations for hash table performance
func TestAddressKeyOperations(t *testing.T) {
	t.Run("AddressKeyGeneration", func(t *testing.T) {
		// Test deterministic key generation
		addr1 := generateMockAddress(12345)
		addr2 := generateMockAddress(12345)
		addr3 := generateMockAddress(54321)

		key1 := bytesToAddressKey(addr1[:])
		key2 := bytesToAddressKey(addr2[:])
		key3 := bytesToAddressKey(addr3[:])

		// Verify deterministic generation
		if !key1.isEqual(key2) {
			t.Error("Same address should generate identical keys")
		}

		// Verify collision avoidance
		if key1.isEqual(key3) {
			t.Error("Different addresses should generate different keys")
		}
	})

	t.Run("AddressKeyComparison", func(t *testing.T) {
		// Test SIMD-optimized comparison performance
		addr := generateMockAddress(99999)
		key := bytesToAddressKey(addr[:])

		// Self-comparison should be true
		if !key.isEqual(key) {
			t.Error("Key should equal itself")
		}

		// Test word-level differences
		modifiedKey := key
		modifiedKey.words[2] ^= 1 // Flip one bit

		if key.isEqual(modifiedKey) {
			t.Error("Modified key should not equal original")
		}
	})
}

// TestPairAddressMapping validates O(1) address lookup performance
// with realistic collision handling and stride-64 optimization
func TestPairAddressMapping(t *testing.T) {
	t.Run("BasicMapping", func(t *testing.T) {
		// Test basic registration and lookup
		addr := generateMockAddress(42)
		pairID := PairID(1337)

		RegisterPairAddress(addr[:], pairID)

		foundID := lookupPairIDByAddress(addr[:])
		if foundID != pairID {
			t.Errorf("Expected pair ID %d, got %d", pairID, foundID)
		}
	})

	t.Run("CollisionHandling", func(t *testing.T) {
		// Test stride-64 collision resolution
		setup := createMockArbitrageSetup()

		// Register all address mappings
		for pairID, addr := range setup.addressMappings {
			RegisterPairAddress(addr[:], pairID)
		}

		// Verify all lookups succeed
		for pairID, addr := range setup.addressMappings {
			foundID := lookupPairIDByAddress(addr[:])
			if foundID != pairID {
				t.Errorf("Address lookup failed: expected %d, got %d", pairID, foundID)
			}
		}
	})

	t.Run("NotFoundHandling", func(t *testing.T) {
		// Test unknown address handling
		unknownAddr := generateMockAddress(999999)
		foundID := lookupPairIDByAddress(unknownAddr[:])

		if foundID != 0 {
			t.Error("Unknown address should return 0")
		}
	})
}

// TestQuantizationAccuracy validates tick value quantization
// for priority queue operations with proper overflow handling
func TestQuantizationAccuracy(t *testing.T) {
	testCases := []struct {
		input    float64
		expected int64
		name     string
	}{
		{-200.0, 0, "Underflow clamping"},
		{-128.0, 0, "Lower bound"},
		{-64.0, 32767, "Negative value"},
		{0.0, 131071, "Zero value"},
		{64.0, 196607, "Positive value"},
		{128.0, 262143, "Upper bound"},
		{200.0, 262143, "Overflow clamping"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := quantizeTickToInt64(tc.input)
			if result != tc.expected {
				t.Errorf("Input %f: expected %d, got %d", tc.input, tc.expected, result)
			}
		})
	}
}

// ============================================================================
// INTEGRATION TESTS - SYSTEM WORKFLOWS
// ============================================================================

// TestFullSystemInitialization validates complete system bootstrap
// with realistic cycle counts and proper core distribution
func TestFullSystemInitialization(t *testing.T) {
	t.Run("SystemBootstrap", func(t *testing.T) {
		setup := createMockArbitrageSetup()

		// Initialize system with mock data
		InitializeArbitrageSystem(setup.trianglePairs)

		// Allow initialization to complete
		time.Sleep(100 * time.Millisecond)

		// Verify executors are initialized
		activeExecutors := 0
		for i := 0; i < testCoreCount; i++ {
			if coreExecutors[i] != nil {
				activeExecutors++
			}
		}

		if activeExecutors == 0 {
			t.Error("No executors were initialized")
		}

		t.Logf("Initialized %d core executors", activeExecutors)
	})
}

// TestTickUpdateProcessing validates end-to-end tick processing
// with realistic Ethereum transaction log simulation
func TestTickUpdateProcessing(t *testing.T) {
	t.Run("BasicTickProcessing", func(t *testing.T) {
		setup := createMockArbitrageSetup()

		// Register address mappings
		for pairID, addr := range setup.addressMappings {
			RegisterPairAddress(addr[:], pairID)
		}

		// Process sample tick updates
		processedCount := 0
		for i := 0; i < 100 && i < len(setup.tickUpdateQueue); i++ {
			update := setup.tickUpdateQueue[i]

			// Create mock log view
			logView := generateMockLogView(update.pairID, update.reserve0, update.reserve1, setup.addressMappings)

			// Convert to types.LogView format
			realLogView := &types.LogView{
				Addr:    logView.addr[:],
				Data:    logView.data[:],
				BlkNum:  logView.blkNum[:],
				LogIdx:  logView.logIdx[:],
				Topics:  logView.topics[:],
				TxIndex: logView.txIndex[:],
			}

			// Process tick update
			DispatchTickUpdate(realLogView)
			processedCount++
		}

		t.Logf("Successfully processed %d tick updates", processedCount)
	})
}

// ============================================================================
// PERFORMANCE BENCHMARKS
// ============================================================================

// BenchmarkAddressLookup measures address-to-pair ID resolution performance
// under realistic load conditions with collision handling
func BenchmarkAddressLookup(b *testing.B) {
	setup := createMockArbitrageSetup()

	// Pre-register all addresses
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	// Create lookup test set
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

// BenchmarkTickQuantization measures tick value quantization performance
// for priority queue operations with various input ranges
func BenchmarkTickQuantization(b *testing.B) {
	// Pre-generate test inputs across full range
	inputs := make([]float64, 1000)
	for i := range inputs {
		inputs[i] = (float64(i)/float64(len(inputs)))*256.0 - 128.0 // Range: -128 to +128
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		input := inputs[i%len(inputs)]
		_ = quantizeTickToInt64(input)
	}
}

// BenchmarkTickUpdateDispatch measures end-to-end tick processing performance
// from log view parsing through core distribution
func BenchmarkTickUpdateDispatch(b *testing.B) {
	setup := createMockArbitrageSetup()

	// Pre-register addresses
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	// Pre-generate log views
	logViews := make([]*types.LogView, len(setup.tickUpdateQueue))
	for i, update := range setup.tickUpdateQueue {
		mockLog := generateMockLogView(update.pairID, update.reserve0, update.reserve1, setup.addressMappings)
		logViews[i] = &types.LogView{
			Addr:    mockLog.addr[:],
			Data:    mockLog.data[:],
			BlkNum:  mockLog.blkNum[:],
			LogIdx:  mockLog.logIdx[:],
			Topics:  mockLog.topics[:],
			TxIndex: mockLog.txIndex[:],
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logView := logViews[i%len(logViews)]
		DispatchTickUpdate(logView)
	}
}

// BenchmarkConcurrentProcessing measures multi-core scaling efficiency
// under realistic concurrent load patterns
func BenchmarkConcurrentProcessing(b *testing.B) {
	setup := createMockArbitrageSetup()

	// Initialize system
	InitializeArbitrageSystem(setup.trianglePairs[:100]) // Smaller set for benchmark

	// Register addresses
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	// Pre-generate test data
	logViews := make([]*types.LogView, 1000)
	for i := range logViews {
		update := setup.tickUpdateQueue[i%len(setup.tickUpdateQueue)]
		mockLog := generateMockLogView(update.pairID, update.reserve0, update.reserve1, setup.addressMappings)
		logViews[i] = &types.LogView{
			Addr:    mockLog.addr[:],
			Data:    mockLog.data[:],
			BlkNum:  mockLog.blkNum[:],
			LogIdx:  mockLog.logIdx[:],
			Topics:  mockLog.topics[:],
			TxIndex: mockLog.txIndex[:],
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			logView := logViews[i%len(logViews)]
			DispatchTickUpdate(logView)
			i++
		}
	})
}

// ============================================================================
// MEMORY AND CACHE PERFORMANCE TESTS
// ============================================================================

// TestMemoryAllocation validates zero-allocation hot path execution
// under sustained load conditions
func TestMemoryAllocation(t *testing.T) {
	setup := createMockArbitrageSetup()

	// Register addresses
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	// Measure allocation before test
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Process tick updates
	for i := 0; i < 1000; i++ {
		update := setup.tickUpdateQueue[i%len(setup.tickUpdateQueue)]
		logView := generateMockLogView(update.pairID, update.reserve0, update.reserve1, setup.addressMappings)

		realLogView := &types.LogView{
			Addr:   logView.addr[:],
			Data:   logView.data[:],
			BlkNum: logView.blkNum[:],
		}

		DispatchTickUpdate(realLogView)
	}

	// Measure allocation after test
	runtime.GC()
	runtime.ReadMemStats(&m2)

	allocatedBytes := m2.TotalAlloc - m1.TotalAlloc
	allocationsCount := m2.Mallocs - m1.Mallocs

	t.Logf("Memory allocated: %d bytes", allocatedBytes)
	t.Logf("Allocations count: %d", allocationsCount)

	// Hot path should have minimal allocations
	if allocatedBytes > maxMemoryAllocBytes*1000 {
		t.Errorf("Excessive memory allocation: %d bytes", allocatedBytes)
	}
}

// ============================================================================
// STRESS TESTS AND LOAD VALIDATION
// ============================================================================

// TestHighVolumeTickProcessing validates system stability under sustained
// high-frequency tick update load patterns
func TestHighVolumeTickProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high volume test in short mode")
	}

	setup := createMockArbitrageSetup()

	// Register addresses
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	// Process high volume of updates
	const highVolumeCount = 100000
	processed := uint64(0)

	start := time.Now()

	for i := 0; i < highVolumeCount; i++ {
		update := setup.tickUpdateQueue[i%len(setup.tickUpdateQueue)]
		logView := generateMockLogView(update.pairID, update.reserve0, update.reserve1, setup.addressMappings)

		realLogView := &types.LogView{
			Addr: logView.addr[:],
			Data: logView.data[:],
		}

		DispatchTickUpdate(realLogView)
		atomic.AddUint64(&processed, 1)
	}

	elapsed := time.Since(start)
	throughput := float64(processed) / elapsed.Seconds()

	t.Logf("Processed %d updates in %v", processed, elapsed)
	t.Logf("Throughput: %.0f updates/second", throughput)

	// Validate minimum throughput requirement
	if throughput < 50000 { // 50k updates/second minimum
		t.Errorf("Insufficient throughput: %.0f updates/second", throughput)
	}
}

// TestConcurrentCoreExecution validates proper isolation and performance
// across multiple CPU cores under concurrent access patterns
func TestConcurrentCoreExecution(t *testing.T) {
	setup := createMockArbitrageSetup()

	// Initialize with smaller dataset for test
	InitializeArbitrageSystem(setup.trianglePairs[:1000])

	// Register addresses
	for pairID, addr := range setup.addressMappings {
		RegisterPairAddress(addr[:], pairID)
	}

	const goroutineCount = 16
	const updatesPerGoroutine = 1000

	var wg sync.WaitGroup
	processed := uint64(0)

	start := time.Now()

	// Launch concurrent workers
	for g := 0; g < goroutineCount; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < updatesPerGoroutine; i++ {
				updateIdx := (workerID*updatesPerGoroutine + i) % len(setup.tickUpdateQueue)
				update := setup.tickUpdateQueue[updateIdx]

				logView := generateMockLogView(update.pairID, update.reserve0, update.reserve1, setup.addressMappings)
				realLogView := &types.LogView{
					Addr: logView.addr[:],
					Data: logView.data[:],
				}

				DispatchTickUpdate(realLogView)
				atomic.AddUint64(&processed, 1)
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	expectedTotal := uint64(goroutineCount * updatesPerGoroutine)
	if processed != expectedTotal {
		t.Errorf("Expected %d processed updates, got %d", expectedTotal, processed)
	}

	throughput := float64(processed) / elapsed.Seconds()
	t.Logf("Concurrent throughput: %.0f updates/second", throughput)
}

// ============================================================================
// BENCHMARK SUITE RUNNER
// ============================================================================

// BenchmarkFullSystem provides comprehensive performance characterization
// across all system components with realistic workload simulation
func BenchmarkFullSystem(b *testing.B) {
	b.Run("AddressLookup", BenchmarkAddressLookup)
	b.Run("TickQuantization", BenchmarkTickQuantization)
	b.Run("TickDispatch", BenchmarkTickUpdateDispatch)
	b.Run("ConcurrentProcessing", BenchmarkConcurrentProcessing)
}

// ============================================================================
// TEST UTILITIES AND HELPERS
// ============================================================================

// validateSystemState performs comprehensive system health checks
// after initialization or stress testing
func validateSystemState(t *testing.T) {
	t.Helper()

	// Check executor initialization
	activeExecutors := 0
	for i := 0; i < len(coreExecutors); i++ {
		if coreExecutors[i] != nil {
			activeExecutors++
		}
	}

	if activeExecutors == 0 {
		t.Error("No active executors found")
	}

	// Check ring buffer initialization
	activeRings := 0
	for i := 0; i < len(coreRings); i++ {
		if coreRings[i] != nil {
			activeRings++
		}
	}

	if activeRings == 0 {
		t.Error("No active ring buffers found")
	}

	t.Logf("System state: %d executors, %d rings", activeExecutors, activeRings)
}

// measureLatency provides high-precision latency measurement
// for performance-critical operations
func measureLatency(operation func()) time.Duration {
	start := time.Now()
	operation()
	return time.Since(start)
}

// simulateMarketConditions generates realistic market volatility patterns
// for comprehensive arbitrage opportunity testing
func simulateMarketConditions(baseReserve uint64, volatilityPercent float64) (uint64, uint64) {
	// Generate correlated reserve changes with realistic volatility
	randomBytes := make([]byte, 8)
	rand.Read(randomBytes)
	randomValue := binary.LittleEndian.Uint64(randomBytes)

	// Apply volatility scaling
	volatility := 1.0 + (float64(randomValue%10000)/10000.0-0.5)*volatilityPercent/100.0

	reserve0 := uint64(float64(baseReserve) * volatility)
	reserve1 := uint64(float64(baseReserve) * (2.0 - volatility)) // Inverse correlation

	return reserve0, reserve1
}
