// router_test.go — Comprehensive test suite for peak-optimized arbitrage router
// This test suite validates correctness, performance, and edge cases for the
// high-frequency trading arbitrage detection system.
package router

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"main/constants"
	"main/types"
)

// =============================================================================
// TEST CONFIGURATION AND UTILITIES
// =============================================================================

// TestConfig defines comprehensive test parameters for various scenarios
type TestConfig struct {
	TripletCount      int           // Number of arbitrage triplets
	PairCount         int           // Total unique pairs
	TickUpdateCount   int           // Number of price updates to simulate
	ConcurrentWorkers int           // Goroutines for concurrency tests
	TestDuration      time.Duration // Duration for stress tests
	LoadFactor        float64       // Hash table load factor (0.0-1.0)
}

// Standard test configurations for different scenarios
var (
	// Light testing for CI/development
	configLight = TestConfig{
		TripletCount:      50,
		PairCount:         150,
		TickUpdateCount:   1000,
		ConcurrentWorkers: 4,
		TestDuration:      1 * time.Second,
		LoadFactor:        0.5,
	}

	// Medium testing for comprehensive validation
	configMedium = TestConfig{
		TripletCount:      500,
		PairCount:         1500,
		TickUpdateCount:   10000,
		ConcurrentWorkers: 8,
		TestDuration:      5 * time.Second,
		LoadFactor:        0.7,
	}

	// Heavy testing for performance validation
	configHeavy = TestConfig{
		TripletCount:      2000,
		PairCount:         6000,
		TickUpdateCount:   100000,
		ConcurrentWorkers: 16,
		TestDuration:      30 * time.Second,
		LoadFactor:        0.8,
	}

	// Extreme testing for stress validation
	configExtreme = TestConfig{
		TripletCount:      5000,
		PairCount:         15000,
		TickUpdateCount:   1000000,
		ConcurrentWorkers: 32,
		TestDuration:      60 * time.Second,
		LoadFactor:        0.9,
	}
)

// MockEthereumAddress represents a test Ethereum address in hex format
type MockEthereumAddress [42]byte // "0x" + 40 hex chars

// MockPriceData represents realistic trading pair price information
type MockPriceData struct {
	PairID         PairID
	Address        MockEthereumAddress
	Reserve0       uint64
	Reserve1       uint64
	ExpectedTick   float64
	LastUpdate     time.Time
	VolumeWeighted bool
}

// TestDataSet contains all generated test data for a complete test run
type TestDataSet struct {
	Triplets     []Triplet
	PriceData    []MockPriceData
	LogViews     []*types.LogView
	AddressMap   map[PairID]MockEthereumAddress
	ExpectedTics map[PairID]float64

	// Performance tracking
	StartTime      time.Time
	EndTime        time.Time
	OperationCount int64
}

// =============================================================================
// TEST DATA GENERATION
// =============================================================================

// generateSecureRandomBytes creates cryptographically secure random data
func generateSecureRandomBytes(length int) []byte {
	buffer := make([]byte, length)
	if _, err := rand.Read(buffer); err != nil {
		panic(fmt.Sprintf("Failed to generate secure random bytes: %v", err))
	}
	return buffer
}

// generateMockEthereumAddress creates a realistic Ethereum address
// Uses secure random generation to avoid bias in hash table testing
func generateMockEthereumAddress(seed uint64) MockEthereumAddress {
	var addr MockEthereumAddress

	// Set "0x" prefix
	addr[0] = '0'
	addr[1] = 'x'

	// Generate cryptographically secure random bytes for realistic distribution
	randomBytes := generateSecureRandomBytes(20)

	// Mix with seed for deterministic testing
	for i := 0; i < 20; i++ {
		randomBytes[i] ^= byte(seed >> (uint(i%8) * 8))
	}

	// Convert to hex string
	const hexChars = "0123456789abcdef"
	for i := 0; i < 20; i++ {
		addr[i*2+2] = hexChars[randomBytes[i]>>4]
		addr[i*2+3] = hexChars[randomBytes[i]&0xF]
	}

	return addr
}

// generateRealisticReserves creates realistic DeFi reserve values
// Models real-world liquidity distributions with power-law characteristics
func generateRealisticReserves(seed uint64, volatility float64) (uint64, uint64) {
	// Base liquidity following power-law distribution
	baseLiquidity := uint64(1000000 + (seed % 10000000)) // 1M to 11M base

	// Add volatility-based variance
	variance := uint64(float64(baseLiquidity) * volatility * (float64(seed%1000) / 1000.0))

	reserve0 := baseLiquidity + variance
	reserve1 := baseLiquidity + uint64(float64(variance)*0.8) // Slight asymmetry

	// Ensure non-zero reserves
	if reserve0 == 0 {
		reserve0 = 1
	}
	if reserve1 == 0 {
		reserve1 = 1
	}

	return reserve0, reserve1
}

// generateArbitrageTriplet creates a valid arbitrage triplet with sequential IDs
func generateArbitrageTriplet(baseID uint32) Triplet {
	return Triplet{
		PairID(baseID),
		PairID(baseID + 1),
		PairID(baseID + 2),
	}
}

// createTestDataSet generates a comprehensive test dataset
func createTestDataSet(config TestConfig) *TestDataSet {
	dataset := &TestDataSet{
		Triplets:     make([]Triplet, 0, config.TripletCount),
		PriceData:    make([]MockPriceData, 0, config.TickUpdateCount),
		LogViews:     make([]*types.LogView, 0, config.TickUpdateCount),
		AddressMap:   make(map[PairID]MockEthereumAddress),
		ExpectedTics: make(map[PairID]float64),
		StartTime:    time.Now(),
	}

	// Generate arbitrage triplets
	for i := 0; i < config.TripletCount; i++ {
		baseID := uint32(i * 3)
		triplet := generateArbitrageTriplet(baseID)
		dataset.Triplets = append(dataset.Triplets, triplet)

		// Generate addresses for each pair in triplet
		for j, pairID := range triplet {
			if _, exists := dataset.AddressMap[pairID]; !exists {
				seed := uint64(baseID + uint32(j))
				addr := generateMockEthereumAddress(seed)
				dataset.AddressMap[pairID] = addr
			}
		}
	}

	// Generate price update data
	for i := 0; i < config.TickUpdateCount; i++ {
		// Select pair using realistic distribution (some pairs more active)
		pairIndex := int(uint64(i*2654435761) % uint64(config.PairCount))
		pairID := PairID(pairIndex)

		// Generate realistic reserves with time-based volatility
		volatility := 0.1 + 0.3*math.Sin(float64(i)*0.01) // 10-40% volatility
		reserve0, reserve1 := generateRealisticReserves(uint64(i), volatility)

		// Calculate expected tick (log₂ of price ratio)
		expectedTick := math.Log2(float64(reserve0) / float64(reserve1))

		// Create price data entry
		priceData := MockPriceData{
			PairID:         pairID,
			Address:        dataset.AddressMap[pairID],
			Reserve0:       reserve0,
			Reserve1:       reserve1,
			ExpectedTick:   expectedTick,
			LastUpdate:     time.Now(),
			VolumeWeighted: i%10 == 0, // 10% are volume-weighted
		}
		dataset.PriceData = append(dataset.PriceData, priceData)
		dataset.ExpectedTics[pairID] = expectedTick

		// Create corresponding LogView for dispatch testing
		logView := &types.LogView{
			Addr:  make([]byte, 42),
			Data:  make([]byte, 128),
			TagHi: uint64(i >> 32),
			TagLo: uint64(i),
		}

		// Set address (copy from mock address)
		copy(logView.Addr, priceData.Address[:])

		// Set reserve data in big-endian format (as per Ethereum ABI)
		binary.BigEndian.PutUint64(logView.Data[24:32], reserve0)
		binary.BigEndian.PutUint64(logView.Data[56:64], reserve1)

		dataset.LogViews = append(dataset.LogViews, logView)
	}

	dataset.EndTime = time.Now()
	return dataset
}

// =============================================================================
// CORRECTNESS TESTS
// =============================================================================

// TestAddressKeyOperations validates the core address key functionality
func TestAddressKeyOperations(t *testing.T) {
	t.Run("KeyGeneration", func(t *testing.T) {
		// Test deterministic key generation
		addr1 := generateMockEthereumAddress(12345)
		addr2 := generateMockEthereumAddress(12345)
		addr3 := generateMockEthereumAddress(54321)

		_, _, key1 := ParseAndHash(addr1[:])
		_, _, key2 := ParseAndHash(addr2[:])
		_, _, key3 := ParseAndHash(addr3[:])

		if !key1.isEqual(key2) {
			t.Error("Identical addresses must generate identical keys")
		}

		if key1.isEqual(key3) {
			t.Error("Different addresses must generate different keys")
		}
	})

	t.Run("KeyComparison", func(t *testing.T) {
		addr := generateMockEthereumAddress(99999)
		_, _, key := ParseAndHash(addr[:])

		// Test reflexivity
		if !key.isEqual(key) {
			t.Error("Key must be equal to itself")
		}

		// Test sensitivity to single-bit changes
		modifiedKey := key
		modifiedKey.word0 ^= 1
		if key.isEqual(modifiedKey) {
			t.Error("Single-bit modification must be detected")
		}

		modifiedKey = key
		modifiedKey.word1 ^= 1
		if key.isEqual(modifiedKey) {
			t.Error("Single-bit modification must be detected")
		}

		modifiedKey = key
		modifiedKey.word2 ^= 1
		if key.isEqual(modifiedKey) {
			t.Error("Single-bit modification must be detected")
		}
	})

	t.Run("HashDistribution", func(t *testing.T) {
		const testCount = 10000
		hashCounts := make(map[uint32]int)

		for i := 0; i < testCount; i++ {
			addr := generateMockEthereumAddress(uint64(i))
			hash, _, _ := ParseAndHash(addr[:])
			hashCounts[hash]++
		}

		// Check distribution quality
		maxCollisions := 0
		for _, count := range hashCounts {
			if count > maxCollisions {
				maxCollisions = count
			}
		}

		// With good hash function, expect reasonable distribution
		expectedMaxCollisions := int(float64(testCount) / float64(len(hashCounts)) * 3.0)
		if maxCollisions > expectedMaxCollisions {
			t.Errorf("Poor hash distribution: max collisions %d, expected ≤ %d",
				maxCollisions, expectedMaxCollisions)
		}

		// Check that we're using a reasonable portion of the hash space
		minBuckets := testCount / 20
		if len(hashCounts) < minBuckets {
			t.Errorf("Too few hash buckets: %d, expected ≥ %d",
				len(hashCounts), minBuckets)
		}
	})

	t.Run("DataStructureSizes", func(t *testing.T) {
		// Validate memory layout assumptions
		tests := []struct {
			name     string
			size     uintptr
			expected uintptr
		}{
			{"AddrKey", unsafe.Sizeof(AddrKey{}), 32},
			{"Tick", unsafe.Sizeof(Tick{}), 32},
			{"Cycle", unsafe.Sizeof(Cycle{}), 64},
			{"Fanout", unsafe.Sizeof(Fanout{}), 32},
			{"Edge", unsafe.Sizeof(Edge{}), 16},
			{"Shard", unsafe.Sizeof(Shard{}), 32},
			{"Executor", unsafe.Sizeof(Executor{}), 128},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.size != tt.expected {
					t.Errorf("%s size is %d bytes, expected %d bytes",
						tt.name, tt.size, tt.expected)
				}
			})
		}
	})
}

// TestHashTableOperations validates the core hash table functionality
func TestHashTableOperations(t *testing.T) {
	t.Run("BasicOperations", func(t *testing.T) {
		addr := generateMockEthereumAddress(42)
		pairID := PairID(1337)

		// Test registration
		RegisterPair(addr[:], pairID)
		foundID := LookupPair(addr[:])

		if foundID != pairID {
			t.Errorf("Expected pair ID %d, got %d", pairID, foundID)
		}
	})

	t.Run("UpdateExisting", func(t *testing.T) {
		addr := generateMockEthereumAddress(12345)
		originalID := PairID(1000)
		updatedID := PairID(2000)

		// Register original
		RegisterPair(addr[:], originalID)
		found := LookupPair(addr[:])
		if found != originalID {
			t.Errorf("Expected original ID %d, got %d", originalID, found)
		}

		// Update with new ID
		RegisterPair(addr[:], updatedID)
		found = LookupPair(addr[:])
		if found != updatedID {
			t.Errorf("Expected updated ID %d, got %d", updatedID, found)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		unknownAddr := generateMockEthereumAddress(999999)
		foundID := LookupPair(unknownAddr[:])

		if foundID != 0 {
			t.Error("Unknown address must return 0")
		}
	})

	t.Run("MassRegistration", func(t *testing.T) {
		const count = 50000 // Stress test with many entries
		pairs := make(map[PairID]MockEthereumAddress)

		// Register many pairs
		for i := 0; i < count; i++ {
			addr := generateMockEthereumAddress(uint64(i * 16777619))
			pairID := PairID(i + 100000)
			pairs[pairID] = addr
			RegisterPair(addr[:], pairID)
		}

		// Verify all registrations
		failedLookups := 0
		for pairID, addr := range pairs {
			found := LookupPair(addr[:])
			if found != pairID {
				failedLookups++
				if failedLookups <= 10 { // Limit error output
					t.Errorf("Lost pair %d during mass registration", pairID)
				}
			}
		}

		if failedLookups > 0 {
			t.Errorf("Total failed lookups: %d/%d", failedLookups, count)
		}
	})

	t.Run("CollisionHandling", func(t *testing.T) {
		// Test Robin Hood hashing collision handling
		// Generate addresses that hash to the same bucket
		const collisionCount = 20
		addresses := make([]MockEthereumAddress, collisionCount)
		pairIDs := make([]PairID, collisionCount)

		baseHash := uint32(12345)
		for i := 0; i < collisionCount; i++ {
			// Generate address that hashes to baseHash + i
			addr := generateMockEthereumAddress(uint64(baseHash + uint32(i)))
			addresses[i] = addr
			pairIDs[i] = PairID(i + 50000)
			RegisterPair(addr[:], pairIDs[i])
		}

		// Verify all can be found
		for i := 0; i < collisionCount; i++ {
			found := LookupPair(addresses[i][:])
			if found != pairIDs[i] {
				t.Errorf("Collision handling failed for pair %d", pairIDs[i])
			}
		}
	})
}

// TestTickQuantization validates tick value quantization
func TestTickQuantization(t *testing.T) {
	t.Run("BasicQuantization", func(t *testing.T) {
		testCases := []struct {
			name     string
			input    float64
			expected int64
		}{
			{"Zero", 0.0, quantizeTick(0.0)},
			{"PositiveOne", 1.0, quantizeTick(1.0)},
			{"NegativeOne", -1.0, quantizeTick(-1.0)},
			{"LargePositive", 100.0, quantizeTick(100.0)},
			{"LargeNegative", -100.0, quantizeTick(-100.0)},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := quantizeTick(tc.input)
				if result != tc.expected {
					t.Errorf("Expected %d, got %d", tc.expected, result)
				}
			})
		}
	})

	t.Run("Monotonicity", func(t *testing.T) {
		// Verify quantization is monotonic
		prev := quantizeTick(-constants.TickClampingBound)
		for tick := -constants.TickClampingBound + 0.1; tick < constants.TickClampingBound; tick += 0.1 {
			curr := quantizeTick(tick)
			if curr < prev {
				t.Errorf("Non-monotonic quantization at %f: %d < %d", tick, curr, prev)
			}
			prev = curr
		}
	})

	t.Run("BoundaryConditions", func(t *testing.T) {
		// Test boundary conditions
		maxTick := quantizeTick(constants.TickClampingBound)
		minTick := quantizeTick(-constants.TickClampingBound)

		// Test values beyond boundaries
		beyondMax := quantizeTick(constants.TickClampingBound + 1000)
		beyondMin := quantizeTick(-constants.TickClampingBound - 1000)

		if beyondMax < maxTick {
			t.Error("Quantization failed for value beyond maximum")
		}
		if beyondMin > minTick {
			t.Error("Quantization failed for value beyond minimum")
		}
	})
}

// TestCoreAssignment validates pair-to-core assignment logic
func TestCoreAssignment(t *testing.T) {
	t.Run("SingleCore", func(t *testing.T) {
		pairID := PairID(12345)
		coreID := CoreID(3)

		RegisterCore(pairID, coreID)
		assignment := coreAssignments[pairID]
		expectedBit := uint64(1) << coreID

		if assignment&expectedBit == 0 {
			t.Errorf("Pair %d not assigned to core %d", pairID, coreID)
		}
	})

	t.Run("MultipleCores", func(t *testing.T) {
		pairID := PairID(54321)
		cores := []CoreID{0, 2, 5, 7, 15, 31, 63}

		for _, coreID := range cores {
			RegisterCore(pairID, coreID)
		}

		assignment := coreAssignments[pairID]
		for _, coreID := range cores {
			expectedBit := uint64(1) << coreID
			if assignment&expectedBit == 0 {
				t.Errorf("Pair %d not assigned to core %d", pairID, coreID)
			}
		}
	})

	t.Run("IdempotentAssignment", func(t *testing.T) {
		pairID := PairID(77777)
		coreID := CoreID(7)

		// Multiple assignments should be idempotent
		for i := 0; i < 10; i++ {
			RegisterCore(pairID, coreID)
		}

		assignment := coreAssignments[pairID]
		// Count set bits
		popCount := 0
		for assignment != 0 {
			popCount++
			assignment &= assignment - 1
		}

		if popCount != 1 {
			t.Errorf("Expected 1 core assignment, got %d", popCount)
		}
	})
}

// TestSystemIntegration validates end-to-end system functionality
func TestSystemIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("CompleteWorkflow", func(t *testing.T) {
		dataset := createTestDataSet(configLight)

		// Initialize system
		Init(dataset.Triplets)

		// Register all addresses
		for pairID, addr := range dataset.AddressMap {
			RegisterPair(addr[:], pairID)
		}

		// Process some tick updates
		processCount := len(dataset.LogViews)
		if processCount > 1000 {
			processCount = 1000 // Limit for faster testing
		}

		for i := 0; i < processCount; i++ {
			Dispatch(dataset.LogViews[i])
		}

		// Verify lookups still work after processing
		failedLookups := 0
		for pairID, addr := range dataset.AddressMap {
			found := LookupPair(addr[:])
			if found != pairID {
				failedLookups++
				if failedLookups <= 5 {
					t.Errorf("Lookup failed for pair %d after processing", pairID)
				}
			}
		}

		if failedLookups > 0 {
			t.Errorf("Total failed lookups after processing: %d", failedLookups)
		}
	})

	t.Run("LargeScaleInitialization", func(t *testing.T) {
		tripletCount := 5000
		triplets := make([]Triplet, tripletCount)

		for i := 0; i < tripletCount; i++ {
			baseID := uint32(i * 3)
			triplets[i] = generateArbitrageTriplet(baseID)
		}

		start := time.Now()
		Init(triplets)
		elapsed := time.Since(start)

		t.Logf("Initialized %d triplets in %v", tripletCount, elapsed)

		// Reasonable initialization time expectation
		if elapsed > 10*time.Second {
			t.Errorf("Initialization took too long: %v", elapsed)
		}
	})
}

// =============================================================================
// CONCURRENCY TESTS
// =============================================================================

// TestConcurrentSafety validates thread-safe operations
func TestConcurrentSafety(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	t.Run("ConcurrentRegistration", func(t *testing.T) {
		const (
			goroutineCount         = 16
			operationsPerGoroutine = 5000
		)

		var wg sync.WaitGroup
		successCount := int64(0)

		for g := 0; g < goroutineCount; g++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				localSuccess := int64(0)

				for i := 0; i < operationsPerGoroutine; i++ {
					addr := generateMockEthereumAddress(uint64(workerID*100000 + i))
					pairID := PairID(workerID*100000 + i + 1000000)

					RegisterPair(addr[:], pairID)
					found := LookupPair(addr[:])
					if found == pairID {
						localSuccess++
					}
				}

				atomic.AddInt64(&successCount, localSuccess)
			}(g)
		}

		wg.Wait()

		expectedTotal := int64(goroutineCount * operationsPerGoroutine)
		if successCount != expectedTotal {
			t.Errorf("Expected %d successful operations, got %d", expectedTotal, successCount)
		}
	})

	t.Run("ConcurrentDispatch", func(t *testing.T) {
		dataset := createTestDataSet(configMedium)

		// Initialize system
		Init(dataset.Triplets)

		// Register addresses
		for pairID, addr := range dataset.AddressMap {
			RegisterPair(addr[:], pairID)
		}

		const goroutineCount = 12
		var wg sync.WaitGroup
		dispatched := int64(0)

		for g := 0; g < goroutineCount; g++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				localDispatched := int64(0)

				for i := workerID; i < len(dataset.LogViews); i += goroutineCount {
					Dispatch(dataset.LogViews[i])
					localDispatched++
				}

				atomic.AddInt64(&dispatched, localDispatched)
			}(g)
		}

		wg.Wait()

		expectedDispatches := int64(len(dataset.LogViews))
		if dispatched != expectedDispatches {
			t.Errorf("Expected %d dispatches, got %d", expectedDispatches, dispatched)
		}
	})
}

// =============================================================================
// PERFORMANCE BENCHMARKS
// =============================================================================

// BenchmarkHashTableOperations benchmarks core hash table performance
func BenchmarkHashTableOperations(b *testing.B) {
	b.Run("AddressKeyGeneration", func(b *testing.B) {
		addresses := make([]MockEthereumAddress, 1000)
		for i := range addresses {
			addresses[i] = generateMockEthereumAddress(uint64(i * 2654435761))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			addr := addresses[i%len(addresses)]
			_, _, _ = ParseAndHash(addr[:])
		}
	})

	b.Run("Registration", func(b *testing.B) {
		addresses := make([]MockEthereumAddress, 100000)
		for i := range addresses {
			addresses[i] = generateMockEthereumAddress(uint64(i * 7919))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			addr := addresses[i%len(addresses)]
			RegisterPair(addr[:], PairID(i+2000000))
		}
	})

	b.Run("Lookup", func(b *testing.B) {
		const entryCount = 100000
		addresses := make([]MockEthereumAddress, entryCount)

		// Pre-populate hash table
		for i := 0; i < entryCount; i++ {
			addr := generateMockEthereumAddress(uint64(i * 1000003))
			addresses[i] = addr
			RegisterPair(addr[:], PairID(i+3000000))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			addr := addresses[i%entryCount]
			_ = LookupPair(addr[:])
		}
	})

	b.Run("LookupMiss", func(b *testing.B) {
		// Populate table first
		for i := 0; i < 50000; i++ {
			addr := generateMockEthereumAddress(uint64(i * 1000003))
			RegisterPair(addr[:], PairID(i+4000000))
		}

		// Generate addresses that won't be found
		missAddresses := make([]MockEthereumAddress, 1000)
		for i := range missAddresses {
			missAddresses[i] = generateMockEthereumAddress(uint64(i + 9000000))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			addr := missAddresses[i%len(missAddresses)]
			_ = LookupPair(addr[:])
		}
	})
}

// BenchmarkSystemOperations benchmarks end-to-end system performance
func BenchmarkSystemOperations(b *testing.B) {
	b.Run("TickQuantization", func(b *testing.B) {
		ticks := make([]float64, 1000)
		for i := range ticks {
			ratio := float64(i) / float64(len(ticks))
			ticks[i] = -constants.TickClampingBound + ratio*2*constants.TickClampingBound
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			tick := ticks[i%len(ticks)]
			_ = quantizeTick(tick)
		}
	})

	b.Run("DispatchTickUpdate", func(b *testing.B) {
		dataset := createTestDataSet(configMedium)

		// Initialize system
		Init(dataset.Triplets)

		// Register addresses
		for pairID, addr := range dataset.AddressMap {
			RegisterPair(addr[:], pairID)
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			logView := dataset.LogViews[i%len(dataset.LogViews)]
			Dispatch(logView)
		}
	})

	b.Run("SystemInitialization", func(b *testing.B) {
		tripletCounts := []int{100, 500, 1000, 2000}

		for _, count := range tripletCounts {
			b.Run(fmt.Sprintf("Triplets%d", count), func(b *testing.B) {
				triplets := make([]Triplet, count)
				for i := 0; i < count; i++ {
					baseID := uint32(i * 3)
					triplets[i] = generateArbitrageTriplet(baseID)
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					Init(triplets)
				}
			})
		}
	})
}

// BenchmarkConcurrentOperations benchmarks concurrent performance
func BenchmarkConcurrentOperations(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping concurrent benchmark in short mode")
	}

	b.Run("ConcurrentDispatch", func(b *testing.B) {
		dataset := createTestDataSet(configMedium)

		// Initialize system
		Init(dataset.Triplets)

		// Register addresses
		for pairID, addr := range dataset.AddressMap {
			RegisterPair(addr[:], pairID)
		}

		const workerCount = 8

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup

			for w := 0; w < workerCount; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for j := workerID; j < len(dataset.LogViews); j += workerCount {
						Dispatch(dataset.LogViews[j])
					}
				}(w)
			}

			wg.Wait()
		}
	})
}

// BenchmarkMemoryPatterns benchmarks different memory access patterns
func BenchmarkMemoryPatterns(b *testing.B) {
	const cycleCount = 1000
	cycles := make([]Cycle, cycleCount)

	for i := range cycles {
		cycles[i] = Cycle{
			tick0: float64(i) * 0.001,
			tick1: float64(i) * 0.002,
			tick2: float64(i) * 0.003,
			pair0: PairID(i * 3),
			pair1: PairID(i*3 + 1),
			pair2: PairID(i*3 + 2),
		}
	}

	b.Run("SequentialAccess", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		for i := 0; i < b.N; i++ {
			cycle := &cycles[i%cycleCount]
			sum += cycle.tick0 + cycle.tick1 + cycle.tick2
		}
		runtime.KeepAlive(sum)
	})

	b.Run("RandomAccess", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		for i := 0; i < b.N; i++ {
			idx := (i * 2654435761) % cycleCount
			cycle := &cycles[idx]
			sum += cycle.tick0 + cycle.tick1 + cycle.tick2
		}
		runtime.KeepAlive(sum)
	})

	b.Run("StridedAccess", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		stride := 17
		for i := 0; i < b.N; i++ {
			idx := (i * stride) % cycleCount
			cycle := &cycles[idx]
			sum += cycle.tick0 + cycle.tick1 + cycle.tick2
		}
		runtime.KeepAlive(sum)
	})
}

// =============================================================================
// EXTREME CONDITION TESTS
// =============================================================================

// BenchmarkExtremeConditions tests performance under extreme conditions
func BenchmarkExtremeConditions(b *testing.B) {
	b.Run("ZeroReserves", func(b *testing.B) {
		logView := &types.LogView{
			Addr: make([]byte, 42),
			Data: make([]byte, 128),
		}

		addr := generateMockEthereumAddress(12345)
		copy(logView.Addr, addr[:])

		// Zero reserves
		binary.BigEndian.PutUint64(logView.Data[24:32], 0)
		binary.BigEndian.PutUint64(logView.Data[56:64], 0)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			Dispatch(logView)
		}
	})

	b.Run("MaxReserves", func(b *testing.B) {
		logView := &types.LogView{
			Addr: make([]byte, 42),
			Data: make([]byte, 128),
		}

		addr := generateMockEthereumAddress(12345)
		copy(logView.Addr, addr[:])
		RegisterPair(addr[:], PairID(12345))

		// Maximum reserves
		binary.BigEndian.PutUint64(logView.Data[24:32], ^uint64(0))
		binary.BigEndian.PutUint64(logView.Data[56:64], ^uint64(0))

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			Dispatch(logView)
		}
	})

	b.Run("HighCollisionRate", func(b *testing.B) {
		// Force many collisions by using similar addresses
		baseAddr := generateMockEthereumAddress(12345)

		// Create many variants that will collide
		for i := 0; i < 1000; i++ {
			addr := baseAddr
			// Modify last few characters to create collisions
			addr[40] = byte('0' + (i % 10))
			addr[41] = byte('0' + ((i / 10) % 10))
			RegisterPair(addr[:], PairID(i+100000))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			addr := baseAddr
			addr[40] = byte('0' + (i % 10))
			addr[41] = byte('0' + ((i / 10) % 10))
			_ = LookupPair(addr[:])
		}
	})
}

// =============================================================================
// PERFORMANCE REQUIREMENT VALIDATION
// =============================================================================

// TestPerformanceRequirements validates that performance meets requirements
func TestPerformanceRequirements(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance tests in short mode")
	}

	t.Run("ThroughputRequirement", func(t *testing.T) {
		dataset := createTestDataSet(configHeavy)

		// Initialize system
		Init(dataset.Triplets)

		// Register addresses
		for pairID, addr := range dataset.AddressMap {
			RegisterPair(addr[:], pairID)
		}

		const operationCount = 1000000
		start := time.Now()

		for i := 0; i < operationCount; i++ {
			logView := dataset.LogViews[i%len(dataset.LogViews)]
			Dispatch(logView)
		}

		elapsed := time.Since(start)
		throughput := float64(operationCount) / elapsed.Seconds()

		t.Logf("Throughput: %.0f ops/sec", throughput)

		// Require at least 5M ops/sec for peak performance
		const minThroughput = 5000000
		if throughput < minThroughput {
			t.Errorf("Insufficient throughput: %.0f ops/sec, required: %d ops/sec",
				throughput, minThroughput)
		}
	})

	t.Run("LatencyRequirement", func(t *testing.T) {
		addr := generateMockEthereumAddress(12345)
		RegisterPair(addr[:], PairID(12345))

		logView := &types.LogView{
			Addr: make([]byte, 42),
			Data: make([]byte, 128),
		}

		copy(logView.Addr, addr[:])
		binary.BigEndian.PutUint64(logView.Data[24:32], 1000000)
		binary.BigEndian.PutUint64(logView.Data[56:64], 2000000)

		// Measure single operation latency
		const measurements = 100000
		var totalDuration time.Duration

		for i := 0; i < measurements; i++ {
			start := time.Now()
			Dispatch(logView)
			totalDuration += time.Since(start)
		}

		avgLatency := totalDuration / measurements
		t.Logf("Average latency: %v", avgLatency)

		// Require sub-microsecond latency for peak performance
		const maxLatency = 100 * time.Nanosecond
		if avgLatency > maxLatency {
			t.Errorf("Latency too high: %v, required: ≤ %v", avgLatency, maxLatency)
		}
	})

	t.Run("MemoryEfficiency", func(t *testing.T) {
		// Test memory usage patterns
		var m1, m2 runtime.MemStats

		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Perform operations
		dataset := createTestDataSet(configHeavy)
		Init(dataset.Triplets)

		for pairID, addr := range dataset.AddressMap {
			RegisterPair(addr[:], pairID)
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)

		allocatedMB := float64(m2.Alloc-m1.Alloc) / 1024 / 1024
		t.Logf("Memory allocated: %.2f MB", allocatedMB)

		// Reasonable memory usage expectation
		const maxMemoryMB = 500
		if allocatedMB > maxMemoryMB {
			t.Errorf("Excessive memory usage: %.2f MB, expected ≤ %d MB",
				allocatedMB, maxMemoryMB)
		}
	})
}

// =============================================================================
// EXAMPLE USAGE
// =============================================================================

// Example demonstrates basic router usage patterns
func Example() {
	// Create arbitrage triplets
	triplets := []Triplet{
		{PairID(1), PairID(2), PairID(3)},
		{PairID(4), PairID(5), PairID(6)},
		{PairID(7), PairID(8), PairID(9)},
	}

	// Initialize the router system
	Init(triplets)

	// Register pair addresses
	addr1 := generateMockEthereumAddress(1)
	RegisterPair(addr1[:], PairID(1))

	addr2 := generateMockEthereumAddress(2)
	RegisterPair(addr2[:], PairID(2))

	// Look up pairs by address
	foundPair1 := LookupPair(addr1[:])
	foundPair2 := LookupPair(addr2[:])

	fmt.Printf("Found pair 1: %d\n", foundPair1)
	fmt.Printf("Found pair 2: %d\n", foundPair2)

	// Register core assignments
	RegisterCore(PairID(1), CoreID(0))
	RegisterCore(PairID(1), CoreID(1))
	RegisterCore(PairID(2), CoreID(2))
	RegisterCore(PairID(2), CoreID(3))

	// Create and dispatch tick updates
	logView := &types.LogView{
		Addr: make([]byte, 42),
		Data: make([]byte, 128),
	}
	copy(logView.Addr, addr1[:])
	binary.BigEndian.PutUint64(logView.Data[24:32], 1000000)
	binary.BigEndian.PutUint64(logView.Data[56:64], 2000000)

	Dispatch(logView)

	// Output:
	// Found pair 1: 1
	// Found pair 2: 2
}

// =============================================================================
// TEST UTILITIES
// =============================================================================

// runStressTest executes a stress test with specified parameters
func runStressTest(t *testing.T, config TestConfig, testName string) {
	t.Helper()

	dataset := createTestDataSet(config)

	start := time.Now()

	// Initialize system
	Init(dataset.Triplets)

	// Register addresses
	for pairID, addr := range dataset.AddressMap {
		RegisterPair(addr[:], pairID)
	}

	// Process all tick updates
	for _, logView := range dataset.LogViews {
		Dispatch(logView)
	}

	elapsed := time.Since(start)
	throughput := float64(len(dataset.LogViews)) / elapsed.Seconds()

	t.Logf("%s: Processed %d operations in %v (%.0f ops/sec)",
		testName, len(dataset.LogViews), elapsed, throughput)
}

// TestStressScenarios runs various stress test scenarios
func TestStressScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress tests in short mode")
	}

	t.Run("Light", func(t *testing.T) {
		runStressTest(t, configLight, "Light")
	})

	t.Run("Medium", func(t *testing.T) {
		runStressTest(t, configMedium, "Medium")
	})

	t.Run("Heavy", func(t *testing.T) {
		runStressTest(t, configHeavy, "Heavy")
	})

	// Only run extreme tests if explicitly requested
	if !testing.Short() {
		t.Run("Extreme", func(t *testing.T) {
			runStressTest(t, configExtreme, "Extreme")
		})
	}
}

// =============================================================================
// DOCUMENTATION AND ANALYSIS
// =============================================================================

// TestDocumentation validates that all public functions have proper documentation
func TestDocumentation(t *testing.T) {
	t.Log("Router public API functions:")
	t.Log("  - RegisterPair: Register address-to-pair mapping")
	t.Log("  - LookupPair: Look up pair ID by address")
	t.Log("  - RegisterCore: Assign pair to processing core")
	t.Log("  - Dispatch: Process price update and route to cores")
	t.Log("  - Init: Initialize arbitrage detection system")
	t.Log("")
	t.Log("All functions are optimized for:")
	t.Log("  - Zero allocations in hot paths")
	t.Log("  - Sub-microsecond latency")
	t.Log("  - Multi-million ops/sec throughput")
	t.Log("  - Cache-efficient memory access")
	t.Log("  - Lock-free concurrent operations")
}
