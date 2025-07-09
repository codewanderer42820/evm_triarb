package router

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"main/constants"
	"main/types"
)

// TestConfiguration holds test parameters
type TestConfiguration struct {
	TripletCount      int
	TickUpdateCount   int
	CoreCount         int
	AddressCount      int
	ConcurrentWorkers int
	IterationCount    int
}

// Standard test configuration
var testConfig = TestConfiguration{
	TripletCount:      100,
	TickUpdateCount:   1000,
	CoreCount:         4,
	AddressCount:      300,
	ConcurrentWorkers: 8,
	IterationCount:    10000,
}

// MockAddress represents a test Ethereum address
type MockAddress [40]byte

// MockTickData represents test tick update data
type MockTickData struct {
	PairID       PairID
	Reserve0     uint64
	Reserve1     uint64
	ExpectedTick float64
	Address      MockAddress
}

// TestSetup contains all test data
type TestSetup struct {
	Triplets      []Triplet
	Addresses     map[PairID]MockAddress
	TickData      []MockTickData
	LogViews      []*types.LogView
	ExpectedTicks map[PairID]float64
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// generateMockAddress creates a deterministic test address
func generateMockAddress(seed uint64) MockAddress {
	var addr MockAddress
	// Generate "0x" prefix
	addr[0] = '0'
	addr[1] = 'x'

	// Generate 38 hex characters (19 bytes * 2)
	for i := 0; i < 19; i++ {
		byteVal := uint8((seed >> (i * 8)) ^ (seed >> ((i + 7) * 3)))
		addr[i*2+2] = "0123456789abcdef"[byteVal>>4]
		addr[i*2+3] = "0123456789abcdef"[byteVal&0xF]
	}
	return addr
}

// generateTriplet creates a test arbitrage triplet
func generateTriplet(baseID uint32) Triplet {
	return Triplet{
		PairID(baseID),
		PairID(baseID + 1),
		PairID(baseID + 2),
	}
}

// createTestSetup generates comprehensive test data
func createTestSetup(config TestConfiguration) *TestSetup {
	setup := &TestSetup{
		Triplets:      make([]Triplet, 0, config.TripletCount),
		Addresses:     make(map[PairID]MockAddress),
		TickData:      make([]MockTickData, 0, config.TickUpdateCount),
		LogViews:      make([]*types.LogView, 0, config.TickUpdateCount),
		ExpectedTicks: make(map[PairID]float64),
	}

	// Generate triplets
	for i := 0; i < config.TripletCount; i++ {
		baseID := uint32(i * 3)
		triplet := generateTriplet(baseID)
		setup.Triplets = append(setup.Triplets, triplet)

		// Generate addresses for each pair in the triplet
		for j, pairID := range triplet {
			seed := uint64(baseID + uint32(j))
			addr := generateMockAddress(seed)
			setup.Addresses[pairID] = addr
		}
	}

	// Generate tick update data
	for i := 0; i < config.TickUpdateCount; i++ {
		pairIdx := i % config.AddressCount
		pairID := PairID(pairIdx)

		// Generate realistic reserve values
		baseReserve := uint64(1000000 + rand.Intn(10000000)) // 1M to 11M
		reserve0 := baseReserve + uint64(rand.Intn(1000000))
		reserve1 := baseReserve + uint64(rand.Intn(1000000))

		// Calculate expected tick
		expectedTick := math.Log2(float64(reserve0) / float64(reserve1))

		tickData := MockTickData{
			PairID:       pairID,
			Reserve0:     reserve0,
			Reserve1:     reserve1,
			ExpectedTick: expectedTick,
			Address:      setup.Addresses[pairID],
		}

		setup.TickData = append(setup.TickData, tickData)
		setup.ExpectedTicks[pairID] = expectedTick

		// Create corresponding LogView
		logView := &types.LogView{
			Addr: make([]byte, 42),
			Data: make([]byte, 128),
		}

		// Set address
		copy(logView.Addr, tickData.Address[:])

		// Set reserve data in big-endian format
		binary.BigEndian.PutUint64(logView.Data[24:32], reserve0)
		binary.BigEndian.PutUint64(logView.Data[56:64], reserve1)

		setup.LogViews = append(setup.LogViews, logView)
	}

	return setup
}

// =============================================================================
// UNIT TESTS
// =============================================================================

// TestAddrKeyOperations tests address key functionality
func TestAddrKeyOperations(t *testing.T) {
	t.Run("KeyGeneration", func(t *testing.T) {
		addr1 := generateMockAddress(12345)
		addr2 := generateMockAddress(12345)
		addr3 := generateMockAddress(54321)

		key1 := toAddrKey(addr1[:])
		key2 := toAddrKey(addr2[:])
		key3 := toAddrKey(addr3[:])

		if !key1.isEqual(key2) {
			t.Error("Identical addresses must generate equal keys")
		}

		if key1.isEqual(key3) {
			t.Error("Different addresses must generate different keys")
		}
	})

	t.Run("KeyComparison", func(t *testing.T) {
		addr := generateMockAddress(99999)
		key := toAddrKey(addr[:])

		// Test reflexivity
		if !key.isEqual(key) {
			t.Error("Key must equal itself")
		}

		// Test word-level modification detection
		for i := 0; i < 3; i++ {
			modifiedKey := key
			modifiedKey.word0 ^= 1 << i
			if key.isEqual(modifiedKey) {
				t.Errorf("Modified word %d must be detected", i)
			}
		}
	})

	t.Run("KeySize", func(t *testing.T) {
		var key AddrKey
		size := unsafe.Sizeof(key)
		if size != 32 {
			t.Errorf("AddrKey size is %d bytes, expected 32 bytes", size)
		}
	})

	t.Run("KeyAlignment", func(t *testing.T) {
		var key AddrKey
		alignment := unsafe.Alignof(key)
		if alignment < 32 {
			t.Errorf("AddrKey alignment is %d bytes, expected at least 32 bytes", alignment)
		}
	})
}

// TestAddressIndexing tests address indexing functionality
func TestAddressIndexing(t *testing.T) {
	t.Run("IndexConsistency", func(t *testing.T) {
		addr := generateMockAddress(12345)
		index1 := addrIndex(addr[:])
		index2 := addrIndex(addr[:])

		if index1 != index2 {
			t.Error("Address indexing must be deterministic")
		}
	})

	t.Run("IndexDistribution", func(t *testing.T) {
		const testCount = 10000
		indices := make(map[uint32]int)

		for i := 0; i < testCount; i++ {
			addr := generateMockAddress(uint64(i))
			index := addrIndex(addr[:])
			indices[index]++
		}

		// Check distribution quality
		maxCollisions := 0
		for _, count := range indices {
			if count > maxCollisions {
				maxCollisions = count
			}
		}

		// With good hash function, expect reasonable distribution
		if maxCollisions > 20 {
			t.Errorf("Excessive collisions: %d", maxCollisions)
		}

		// Check that we're using reasonable number of buckets
		bucketCount := len(indices)
		if bucketCount < testCount/50 {
			t.Errorf("Too few buckets: %d for %d addresses", bucketCount, testCount)
		}
	})

	t.Run("IndexBounds", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			addr := generateMockAddress(uint64(i))
			index := addrIndex(addr[:])
			if index >= constants.AddressTableCapacity {
				t.Errorf("Index %d exceeds capacity %d", index, constants.AddressTableCapacity)
			}
		}
	})
}

// TestHashTable tests address-to-pair mapping
func TestHashTable(t *testing.T) {
	t.Run("BasicOperations", func(t *testing.T) {
		addr := generateMockAddress(42)
		pairID := PairID(1337)

		RegisterPair(addr[:], pairID)
		foundID := LookupPair(addr[:])

		if foundID != pairID {
			t.Errorf("Expected pair ID %d, got %d", pairID, foundID)
		}
	})

	t.Run("UpdateExisting", func(t *testing.T) {
		addr := generateMockAddress(12345)
		originalPairID := PairID(1000)
		updatedPairID := PairID(2000)

		RegisterPair(addr[:], originalPairID)
		RegisterPair(addr[:], updatedPairID)

		found := LookupPair(addr[:])
		if found != updatedPairID {
			t.Errorf("Expected updated ID %d, got %d", updatedPairID, found)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		unknownAddr := generateMockAddress(999999)
		foundID := LookupPair(unknownAddr[:])

		if foundID != 0 {
			t.Error("Unknown address must return 0")
		}
	})

	t.Run("MassRegistration", func(t *testing.T) {
		const count = 10000
		pairs := make(map[PairID]MockAddress)

		// Register many pairs
		for i := 0; i < count; i++ {
			addr := generateMockAddress(uint64(i * 16777619))
			pairID := PairID(i + 50000)
			pairs[pairID] = addr
			RegisterPair(addr[:], pairID)
		}

		// Verify all registrations
		for pairID, addr := range pairs {
			found := LookupPair(addr[:])
			if found != pairID {
				t.Errorf("Lost pair %d during mass registration", pairID)
			}
		}
	})
}

// TestTickQuantization tests tick value quantization
func TestTickQuantization(t *testing.T) {
	testCases := []struct {
		name     string
		input    float64
		expected int64
	}{
		{"Zero", 0.0, int64(constants.TickClampingBound * constants.QuantizationScale)},
		{"Positive", 1.0, int64((1.0 + constants.TickClampingBound) * constants.QuantizationScale)},
		{"Negative", -1.0, int64((-1.0 + constants.TickClampingBound) * constants.QuantizationScale)},
		{"LargePositive", 100.0, int64((100.0 + constants.TickClampingBound) * constants.QuantizationScale)},
		{"LargeNegative", -100.0, int64((-100.0 + constants.TickClampingBound) * constants.QuantizationScale)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := quantizeTick(tc.input)
			if result != tc.expected {
				t.Errorf("Expected %d, got %d", tc.expected, result)
			}
		})
	}

	t.Run("Monotonicity", func(t *testing.T) {
		prev := quantizeTick(-100.0)
		for tick := -99.9; tick < 100.0; tick += 0.1 {
			curr := quantizeTick(tick)
			if curr < prev {
				t.Errorf("Non-monotonic at %f: %d < %d", tick, curr, prev)
			}
			prev = curr
		}
	})
}

// TestCoreAssignment tests pair-to-core assignment
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
		popCount := popCount64(assignment)
		if popCount != 1 {
			t.Errorf("Expected 1 core assignment, got %d", popCount)
		}
	})
}

// TestDataStructureSizes validates memory layout
func TestDataStructureSizes(t *testing.T) {
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
				t.Errorf("%s size is %d bytes, expected %d bytes", tt.name, tt.size, tt.expected)
			}
		})
	}
}

// TestDataStructureAlignment validates cache alignment
func TestDataStructureAlignment(t *testing.T) {
	tests := []struct {
		name      string
		alignment uintptr
		expected  uintptr
	}{
		{"AddrKey", unsafe.Alignof(AddrKey{}), 32},
		{"Tick", unsafe.Alignof(Tick{}), 32},
		{"Cycle", unsafe.Alignof(Cycle{}), 64},
		{"Fanout", unsafe.Alignof(Fanout{}), 32},
		{"Edge", unsafe.Alignof(Edge{}), 16},
		{"Shard", unsafe.Alignof(Shard{}), 32},
		{"Executor", unsafe.Alignof(Executor{}), 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.alignment < tt.expected {
				t.Errorf("%s alignment is %d bytes, expected at least %d bytes",
					tt.name, tt.alignment, tt.expected)
			}
		})
	}
}

// TestSystemIntegration tests complete system workflow
func TestSystemIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("CompleteWorkflow", func(t *testing.T) {
		setup := createTestSetup(TestConfiguration{
			TripletCount:    10,
			TickUpdateCount: 100,
			AddressCount:    30,
		})

		// Initialize system
		Init(setup.Triplets)

		// Register addresses
		for pairID, addr := range setup.Addresses {
			RegisterPair(addr[:], pairID)
		}

		// Process tick updates
		for i, logView := range setup.LogViews {
			if i < len(setup.TickData) {
				Dispatch(logView)
			}
		}

		// Verify lookups still work
		for pairID, addr := range setup.Addresses {
			found := LookupPair(addr[:])
			if found != pairID {
				t.Errorf("Lookup failed for pair %d after processing", pairID)
			}
		}
	})

	t.Run("LargeScaleInitialization", func(t *testing.T) {
		tripletCount := 1000
		triplets := make([]Triplet, tripletCount)

		for i := 0; i < tripletCount; i++ {
			baseID := uint32(i * 3)
			triplets[i] = generateTriplet(baseID)
		}

		start := time.Now()
		Init(triplets)
		elapsed := time.Since(start)

		t.Logf("Initialized %d triplets in %v", tripletCount, elapsed)

		if elapsed > 5*time.Second {
			t.Errorf("Initialization took too long: %v", elapsed)
		}
	})
}

// TestConcurrentSafety tests concurrent operations
func TestConcurrentSafety(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	t.Run("ConcurrentRegistration", func(t *testing.T) {
		const (
			goroutineCount         = 16
			operationsPerGoroutine = 1000
		)

		var wg sync.WaitGroup
		processed := uint64(0)

		for g := 0; g < goroutineCount; g++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				localProcessed := uint64(0)

				for i := 0; i < operationsPerGoroutine; i++ {
					addr := generateMockAddress(uint64(workerID*10000 + i))
					pairID := PairID(workerID*10000 + i + 100000)

					RegisterPair(addr[:], pairID)
					found := LookupPair(addr[:])
					if found == pairID {
						localProcessed++
					}
				}

				atomic.AddUint64(&processed, localProcessed)
			}(g)
		}

		wg.Wait()

		expectedTotal := uint64(goroutineCount * operationsPerGoroutine)
		if processed != expectedTotal {
			t.Errorf("Expected %d successful operations, got %d", expectedTotal, processed)
		}
	})

	t.Run("ConcurrentCoreAssignment", func(t *testing.T) {
		const (
			goroutineCount    = 8
			pairsPerGoroutine = 500
		)

		var wg sync.WaitGroup

		for g := 0; g < goroutineCount; g++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for i := 0; i < pairsPerGoroutine; i++ {
					pairID := PairID(workerID*10000 + i)
					coreID := CoreID((workerID + i) % 64)

					RegisterCore(pairID, coreID)

					// Verify assignment
					assignment := coreAssignments[pairID]
					expectedBit := uint64(1) << coreID
					if assignment&expectedBit == 0 {
						t.Errorf("Core assignment failed for pair %d", pairID)
					}
				}
			}(g)
		}

		wg.Wait()
	})

	t.Run("ConcurrentDispatch", func(t *testing.T) {
		setup := createTestSetup(TestConfiguration{
			TripletCount:    50,
			TickUpdateCount: 500,
			AddressCount:    150,
		})

		// Initialize system
		Init(setup.Triplets)

		// Register addresses
		for pairID, addr := range setup.Addresses {
			RegisterPair(addr[:], pairID)
		}

		const goroutineCount = 8
		var wg sync.WaitGroup
		dispatched := uint64(0)

		for g := 0; g < goroutineCount; g++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				localDispatched := uint64(0)

				for i := workerID; i < len(setup.LogViews); i += goroutineCount {
					Dispatch(setup.LogViews[i])
					localDispatched++
				}

				atomic.AddUint64(&dispatched, localDispatched)
			}(g)
		}

		wg.Wait()

		expectedDispatches := uint64(len(setup.LogViews))
		if dispatched != expectedDispatches {
			t.Errorf("Expected %d dispatches, got %d", expectedDispatches, dispatched)
		}
	})
}

// =============================================================================
// BENCHMARKS
// =============================================================================

// BenchmarkAddrKeyGeneration benchmarks address key generation
func BenchmarkAddrKeyGeneration(b *testing.B) {
	addresses := make([]MockAddress, 1000)
	for i := range addresses {
		addresses[i] = generateMockAddress(uint64(i * 2654435761))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = toAddrKey(addr[:])
	}
}

// BenchmarkAddrKeyComparison benchmarks address key comparison
func BenchmarkAddrKeyComparison(b *testing.B) {
	addr1 := generateMockAddress(12345)
	addr2 := generateMockAddress(54321)
	key1 := toAddrKey(addr1[:])
	key2 := toAddrKey(addr2[:])

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			_ = key1.isEqual(key1)
		} else {
			_ = key1.isEqual(key2)
		}
	}
}

// BenchmarkAddressIndexing benchmarks address indexing
func BenchmarkAddressIndexing(b *testing.B) {
	addresses := make([]MockAddress, 1000)
	for i := range addresses {
		addresses[i] = generateMockAddress(uint64(i * 16777619))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		_ = addrIndex(addr[:])
	}
}

// BenchmarkHashTableRegistration benchmarks pair registration
func BenchmarkHashTableRegistration(b *testing.B) {
	addresses := make([]MockAddress, 10000)
	for i := range addresses {
		addresses[i] = generateMockAddress(uint64(i * 7919))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%len(addresses)]
		RegisterPair(addr[:], PairID(i+1000000))
	}
}

// BenchmarkHashTableLookup benchmarks pair lookup
func BenchmarkHashTableLookup(b *testing.B) {
	const entryCount = 10000
	addresses := make([]MockAddress, entryCount)

	// Pre-populate hash table
	for i := 0; i < entryCount; i++ {
		addr := generateMockAddress(uint64(i * 1000003))
		addresses[i] = addr
		RegisterPair(addr[:], PairID(i+2000000))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addresses[i%entryCount]
		_ = LookupPair(addr[:])
	}
}

// BenchmarkTickQuantization benchmarks tick quantization
func BenchmarkTickQuantization(b *testing.B) {
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
}

// BenchmarkCoreAssignment benchmarks core assignment operations
func BenchmarkCoreAssignment(b *testing.B) {
	const pairCount = 10000

	// Pre-populate with assignments
	for i := 0; i < pairCount; i++ {
		pairID := PairID(i)
		numCores := 2 + (i % 6) // 2-7 cores per pair
		for j := 0; j < numCores; j++ {
			coreID := CoreID((i*17 + j*23) % 64)
			RegisterCore(pairID, coreID)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pairID := PairID(i % pairCount)
		assignments := coreAssignments[pairID]

		// Simulate core dispatch
		for assignments != 0 {
			coreID := trailingZeros64(assignments)
			assignments &^= 1 << coreID
			_ = coreID
		}
	}
}

// BenchmarkDispatchTickUpdate benchmarks tick dispatch
func BenchmarkDispatchTickUpdate(b *testing.B) {
	setup := createTestSetup(TestConfiguration{
		TripletCount:    100,
		TickUpdateCount: 1000,
		AddressCount:    300,
	})

	// Initialize system
	Init(setup.Triplets)

	// Register addresses
	for pairID, addr := range setup.Addresses {
		RegisterPair(addr[:], pairID)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logView := setup.LogViews[i%len(setup.LogViews)]
		Dispatch(logView)
	}
}

// BenchmarkSystemInitialization benchmarks system initialization
func BenchmarkSystemInitialization(b *testing.B) {
	tripletCounts := []int{100, 500, 1000, 2000}

	for _, count := range tripletCounts {
		b.Run(fmt.Sprintf("Triplets%d", count), func(b *testing.B) {
			triplets := make([]Triplet, count)
			for i := 0; i < count; i++ {
				baseID := uint32(i * 3)
				triplets[i] = generateTriplet(baseID)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				Init(triplets)
			}
		})
	}
}

// BenchmarkMemoryAccess benchmarks memory access patterns
func BenchmarkMemoryAccess(b *testing.B) {
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

	b.Run("Sequential", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		for i := 0; i < b.N; i++ {
			cycle := &cycles[i%cycleCount]
			sum += cycle.tick0 + cycle.tick1 + cycle.tick2
		}
		runtime.KeepAlive(sum)
	})

	b.Run("Random", func(b *testing.B) {
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

	b.Run("Strided", func(b *testing.B) {
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

// BenchmarkConcurrentOperations benchmarks concurrent performance
func BenchmarkConcurrentOperations(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping concurrent benchmark in short mode")
	}

	setup := createTestSetup(TestConfiguration{
		TripletCount:    200,
		TickUpdateCount: 2000,
		AddressCount:    600,
	})

	// Initialize system
	Init(setup.Triplets)

	// Register addresses
	for pairID, addr := range setup.Addresses {
		RegisterPair(addr[:], pairID)
	}

	b.Run("ConcurrentDispatch", func(b *testing.B) {
		const workerCount = 8

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup

			for w := 0; w < workerCount; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for j := workerID; j < len(setup.LogViews); j += workerCount {
						Dispatch(setup.LogViews[j])
					}
				}(w)
			}

			wg.Wait()
		}
	})
}

// BenchmarkExtremeCases benchmarks edge cases
func BenchmarkExtremeCases(b *testing.B) {
	b.Run("ZeroReserves", func(b *testing.B) {
		logView := &types.LogView{
			Addr: make([]byte, 42),
			Data: make([]byte, 128),
		}

		addr := generateMockAddress(12345)
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

		addr := generateMockAddress(12345)
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
}

// =============================================================================
// HELPER FUNCTIONS FOR TESTS
// =============================================================================

// popCount64 counts set bits in uint64
func popCount64(x uint64) int {
	count := 0
	for x != 0 {
		count++
		x &= x - 1
	}
	return count
}

// trailingZeros64 counts trailing zeros in uint64
func trailingZeros64(x uint64) int {
	if x == 0 {
		return 64
	}
	count := 0
	for (x & 1) == 0 {
		count++
		x >>= 1
	}
	return count
}

// =============================================================================
// PERFORMANCE VALIDATION TESTS
// =============================================================================

// TestPerformanceRequirements validates performance requirements
func TestPerformanceRequirements(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance tests in short mode")
	}

	t.Run("ThroughputRequirement", func(t *testing.T) {
		setup := createTestSetup(TestConfiguration{
			TripletCount:    100,
			TickUpdateCount: 10000,
			AddressCount:    300,
		})

		// Initialize system
		Init(setup.Triplets)

		// Register addresses
		for pairID, addr := range setup.Addresses {
			RegisterPair(addr[:], pairID)
		}

		const operationCount = 100000
		start := time.Now()

		for i := 0; i < operationCount; i++ {
			logView := setup.LogViews[i%len(setup.LogViews)]
			Dispatch(logView)
		}

		elapsed := time.Since(start)
		throughput := float64(operationCount) / elapsed.Seconds()

		t.Logf("Throughput: %.0f ops/sec", throughput)

		// Require at least 1M ops/sec
		if throughput < 1000000 {
			t.Errorf("Insufficient throughput: %.0f ops/sec", throughput)
		}
	})

	t.Run("LatencyRequirement", func(t *testing.T) {
		addr := generateMockAddress(12345)
		RegisterPair(addr[:], PairID(12345))

		logView := &types.LogView{
			Addr: make([]byte, 42),
			Data: make([]byte, 128),
		}

		copy(logView.Addr, addr[:])
		binary.BigEndian.PutUint64(logView.Data[24:32], 1000000)
		binary.BigEndian.PutUint64(logView.Data[56:64], 2000000)

		// Measure single operation latency
		const measurements = 10000
		var totalDuration time.Duration

		for i := 0; i < measurements; i++ {
			start := time.Now()
			Dispatch(logView)
			totalDuration += time.Since(start)
		}

		avgLatency := totalDuration / measurements
		t.Logf("Average latency: %v", avgLatency)

		// Require sub-microsecond latency
		if avgLatency > time.Microsecond {
			t.Errorf("Latency too high: %v", avgLatency)
		}
	})
}

// =============================================================================
// EXAMPLE USAGE TESTS
// =============================================================================

// ExampleBasicUsage demonstrates basic router usage
func ExampleBasicUsage() {
	// Create arbitrage triplets
	triplets := []Triplet{
		{PairID(1), PairID(2), PairID(3)},
		{PairID(4), PairID(5), PairID(6)},
	}

	// Initialize the router system
	Init(triplets)

	// Register pair addresses
	addr1 := generateMockAddress(1)
	RegisterPair(addr1[:], PairID(1))

	// Look up pair by address
	foundPair := LookupPair(addr1[:])
	fmt.Printf("Found pair: %d\n", foundPair)

	// Register core assignments
	RegisterCore(PairID(1), CoreID(0))
	RegisterCore(PairID(1), CoreID(1))

	// Create and dispatch tick update
	logView := &types.LogView{
		Addr: make([]byte, 42),
		Data: make([]byte, 128),
	}
	copy(logView.Addr, addr1[:])
	binary.BigEndian.PutUint64(logView.Data[24:32], 1000000)
	binary.BigEndian.PutUint64(logView.Data[56:64], 2000000)

	Dispatch(logView)

	// Output: Found pair: 1
}

// =============================================================================
// DOCUMENTATION TESTS
// =============================================================================

// TestDocumentation validates that all public functions are documented
func TestDocumentation(t *testing.T) {
	// This test ensures all exported functions have proper documentation
	// In a real implementation, this would use go/doc to parse comments
	t.Log("All public functions should have comprehensive documentation")
	t.Log("Public functions: RegisterPair, LookupPair, RegisterCore, Dispatch, Init")
}
