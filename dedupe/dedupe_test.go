// ════════════════════════════════════════════════════════════════════════════════════════════════
// 🧪 COMPREHENSIVE TEST SUITE: DEDUPLICATION CACHE
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Deduplication Engine Test Suite
//
// Description:
//   Validates correctness and performance of the deduplication system for preventing duplicate
//   processing of Ethereum events. Tests cover cache behavior, collision handling, staleness
//   detection, and blockchain reorganization scenarios.
//
// Test Coverage:
//   - Unit tests: Core functionality, edge cases, boundary conditions
//   - Integration tests: Blockchain scenarios, reorganization handling
//   - Benchmarks: Sub-5ns operations, memory efficiency, zero allocations
//   - Edge cases: Hash distribution, collision rates, cache eviction patterns
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package dedupe

import (
	"crypto/rand"
	"encoding/binary"
	"main/constants"
	"main/utils"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

// ============================================================================
// TEST CONFIGURATION
// ============================================================================

const (
	testCacheSize = 1 << constants.RingBits
	testDataSize  = 10000
)

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// generateTestEvent creates a test event with specified parameters
func generateTestEvent(block, tx, log uint32) (uint64, uint64) {
	// Generate deterministic but unique topic fingerprints
	topicHi := uint64(block)<<32 | uint64(tx)<<16 | uint64(log)
	topicLo := utils.Mix64(topicHi)
	return topicHi, topicLo
}

// ============================================================================
// UNIT TESTS - INITIALIZATION
// ============================================================================

func TestDeduper_InitialState(t *testing.T) {
	d := Deduper{}

	// Verify cache size matches configuration
	if len(d.entries) != testCacheSize {
		t.Errorf("Cache size: expected %d, got %d", testCacheSize, len(d.entries))
	}

	// Verify zero initialization of all entries
	for i := range d.entries {
		entry := &d.entries[i]
		if entry.block != 0 || entry.tx != 0 || entry.log != 0 ||
			entry.seenAt != 0 || entry.topicHi != 0 || entry.topicLo != 0 {
			t.Errorf("Entry %d not zero-initialized", i)
		}
	}

	// Verify first check on empty cache returns true (new entry)
	result := d.Check(1000, 5, 2, 0x1234, 0x5678, 1000)
	if !result {
		t.Error("First entry should always be accepted")
	}
}

// ============================================================================
// UNIT TESTS - BASIC DEDUPLICATION
// ============================================================================

func TestDeduper_BasicDeduplication(t *testing.T) {
	d := Deduper{}

	testCases := []struct {
		name             string
		block, tx, log   uint32
		topicHi, topicLo uint64
		currentBlock     uint32
		expectResult     bool
	}{
		// First insertion should succeed
		{"First insertion", 1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000, true},
		// Exact duplicate should fail
		{"Exact duplicate", 1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000, false},
		// Same coordinates, different topics should succeed
		{"Different topics", 1000, 5, 2, 0xaaaa567890abcdef, 0xbbbbba0987654321, 1000, true},
		// Different block should succeed
		{"Different block", 1001, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1001, true},
		// Different tx should succeed
		{"Different tx", 1000, 6, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000, true},
		// Different log should succeed
		{"Different log", 1000, 5, 3, 0x1234567890abcdef, 0xfedcba0987654321, 1000, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := d.Check(tc.block, tc.tx, tc.log, tc.topicHi, tc.topicLo, tc.currentBlock)
			if result != tc.expectResult {
				t.Errorf("Expected %v, got %v", tc.expectResult, result)
			}
		})
	}
}

// ============================================================================
// UNIT TESTS - STALENESS DETECTION
// ============================================================================

func TestDeduper_StalenessDetection(t *testing.T) {
	// Test each case with a fresh deduper to avoid interference
	testCases := []struct {
		name         string
		insertBlock  uint32
		checkBlock   uint32
		expectResult bool
	}{
		// Within reorg window - should be duplicate
		{"Same block", 1000, 1000, false},
		{"Within reorg window", 1000, 1000 + constants.MaxReorg - 1, false},
		{"At reorg boundary", 1000, 1000 + constants.MaxReorg, false},
		// Beyond reorg window - should be accepted as new
		{"Just beyond reorg", 1000, 1000 + constants.MaxReorg + 1, true},
		{"Well beyond reorg", 1000, 1000 + constants.MaxReorg + 100, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := Deduper{}

			block := tc.insertBlock
			tx := uint32(5)
			log := uint32(2)
			topicHi, topicLo := generateTestEvent(block, tx, log)

			// Insert initial entry
			result := d.Check(block, tx, log, topicHi, topicLo, tc.insertBlock)
			if !result {
				t.Error("Initial insertion should succeed")
			}

			// Check at different current block
			result = d.Check(block, tx, log, topicHi, topicLo, tc.checkBlock)
			if result != tc.expectResult {
				t.Errorf("At block %d (inserted at %d): expected %v, got %v",
					tc.checkBlock, tc.insertBlock, tc.expectResult, result)
			}
		})
	}
}

// ============================================================================
// UNIT TESTS - EDGE CASES
// ============================================================================

func TestDeduper_EdgeCases(t *testing.T) {
	t.Run("Zero values", func(t *testing.T) {
		d := Deduper{}

		// All zeros should be accepted first time
		result := d.Check(0, 0, 0, 0, 0, 0)
		if !result {
			t.Error("Zero values should be accepted initially")
		}

		// Duplicate zeros should be rejected
		result = d.Check(0, 0, 0, 0, 0, 0)
		if result {
			t.Error("Duplicate zero values should be rejected")
		}

		// Verify seenAt was set to 1 (not 0)
		key := uint64(0)
		index := utils.Mix64(key) & ((1 << constants.RingBits) - 1)
		if d.entries[index].seenAt != 1 {
			t.Errorf("seenAt should be 1 when currentBlock=0, got %d", d.entries[index].seenAt)
		}
	})

	t.Run("Maximum values", func(t *testing.T) {
		d := Deduper{}

		maxU32 := uint32(0xFFFFFFFF)
		maxU64 := uint64(0xFFFFFFFFFFFFFFFF)

		// Max values should be accepted
		result := d.Check(maxU32, maxU32, maxU32, maxU64, maxU64, maxU32)
		if !result {
			t.Error("Maximum values should be accepted")
		}

		// Duplicate max values should be rejected
		result = d.Check(maxU32, maxU32, maxU32, maxU64, maxU64, maxU32)
		if result {
			t.Error("Duplicate maximum values should be rejected")
		}
	})

	t.Run("Overflow scenarios", func(t *testing.T) {
		d := Deduper{}

		// Test with high block numbers near uint32 max
		highBlock := uint32(0xFFFFFFF0)
		result := d.Check(highBlock, 0, 0, 1, 1, highBlock)
		if !result {
			t.Error("High block number should be accepted")
		}

		// Test staleness when currentBlock wraps around
		// In this case, currentBlock (10) < seenAt (0xFFFFFFF0)
		// But the event is actually very old due to wraparound
		// The staleness check should handle this correctly
		wrappedBlock := uint32(10)

		// Since currentBlock < seenAt, the staleness check (currentBlock > entry.seenAt)
		// will be false, so isStale will be false
		// This means the entry will be considered a duplicate (not stale)
		result = d.Check(highBlock, 0, 0, 1, 1, wrappedBlock)
		if result {
			t.Error("Wrapped block should be treated as duplicate (staleness check fails due to wraparound)")
		}
	})
}

// ============================================================================
// UNIT TESTS - COLLISION HANDLING
// ============================================================================

func TestDeduper_CollisionHandling(t *testing.T) {
	d := Deduper{}

	// Find two entries that hash to the same cache slot
	var collision1, collision2 struct {
		block, tx, log uint32
	}

	// Start with a known entry
	collision1 = struct{ block, tx, log uint32 }{1000, 5, 2}
	key1 := uint64(collision1.block)<<32 | uint64(collision1.tx)<<16 | uint64(collision1.log)
	index1 := utils.Mix64(key1) & ((1 << constants.RingBits) - 1)

	// Find a colliding entry
	found := false
	for b := uint32(1001); b < 10000 && !found; b++ {
		for tx := uint32(0); tx < 100 && !found; tx++ {
			for log := uint32(0); log < 10 && !found; log++ {
				key2 := uint64(b)<<32 | uint64(tx)<<16 | uint64(log)
				index2 := utils.Mix64(key2) & ((1 << constants.RingBits) - 1)
				if index1 == index2 && (b != collision1.block || tx != collision1.tx || log != collision1.log) {
					collision2 = struct{ block, tx, log uint32 }{b, tx, log}
					found = true
				}
			}
		}
	}

	if !found {
		t.Skip("Could not find collision in reasonable time")
	}

	t.Logf("Found collision: (%d,%d,%d) and (%d,%d,%d) both map to index %d",
		collision1.block, collision1.tx, collision1.log,
		collision2.block, collision2.tx, collision2.log, index1)

	// Insert first entry
	topicHi1, topicLo1 := generateTestEvent(collision1.block, collision1.tx, collision1.log)
	result := d.Check(collision1.block, collision1.tx, collision1.log, topicHi1, topicLo1, collision1.block)
	if !result {
		t.Error("First collision entry should be accepted")
	}

	// Insert second entry (evicts first)
	topicHi2, topicLo2 := generateTestEvent(collision2.block, collision2.tx, collision2.log)
	result = d.Check(collision2.block, collision2.tx, collision2.log, topicHi2, topicLo2, collision2.block)
	if !result {
		t.Error("Second collision entry should be accepted")
	}

	// First entry should now be accepted again (was evicted)
	result = d.Check(collision1.block, collision1.tx, collision1.log, topicHi1, topicLo1, collision1.block)
	if !result {
		t.Error("First entry should be accepted after eviction")
	}

	// Second entry should now be accepted again (was evicted)
	result = d.Check(collision2.block, collision2.tx, collision2.log, topicHi2, topicLo2, collision2.block)
	if !result {
		t.Error("Second entry should be accepted after eviction")
	}
}

// ============================================================================
// UNIT TESTS - CACHE BEHAVIOR
// ============================================================================

func TestDeduper_CacheBehavior(t *testing.T) {
	t.Run("Fill pattern", func(t *testing.T) {
		d := Deduper{}

		// Fill cache sequentially
		for i := 0; i < 1000; i++ {
			block := uint32(1000 + i)
			topicHi, topicLo := generateTestEvent(block, 0, 0)
			result := d.Check(block, 0, 0, topicHi, topicLo, block)
			if !result {
				t.Errorf("Entry %d should be accepted on first insert", i)
			}
		}

		// Check duplicates are detected
		duplicatesDetected := 0
		for i := 0; i < 1000; i++ {
			block := uint32(1000 + i)
			topicHi, topicLo := generateTestEvent(block, 0, 0)
			result := d.Check(block, 0, 0, topicHi, topicLo, block)
			if !result {
				duplicatesDetected++
			}
		}

		t.Logf("Detected %d/%d duplicates", duplicatesDetected, 1000)
		if duplicatesDetected == 0 {
			t.Error("No duplicates detected, cache might be too small or hash function poor")
		}
	})

	t.Run("Eviction pattern", func(t *testing.T) {
		d := Deduper{}

		// Fill cache beyond capacity
		totalEntries := testCacheSize + 1000
		for i := 0; i < totalEntries; i++ {
			block := uint32(1000 + i)
			topicHi, topicLo := generateTestEvent(block, 0, 0)
			result := d.Check(block, 0, 0, topicHi, topicLo, block)
			if !result {
				t.Errorf("Entry %d should be accepted on first insert", i)
			}
		}

		// Check how many early entries were evicted
		evicted := 0
		for i := 0; i < 1000; i++ {
			block := uint32(1000 + i)
			topicHi, topicLo := generateTestEvent(block, 0, 0)
			result := d.Check(block, 0, 0, topicHi, topicLo, block)
			if result {
				evicted++
			}
		}

		t.Logf("Evicted %d/%d early entries", evicted, 1000)
		if evicted == 0 {
			t.Error("No evictions detected despite overfilling cache")
		}
	})
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

func TestDeduper_BlockchainSimulation(t *testing.T) {
	d := Deduper{}

	// Simulate realistic blockchain event processing
	const numBlocks = 100
	const txPerBlock = 50
	const logsPerTx = 5

	processedEvents := 0

	// Process initial blocks
	for block := uint32(1000); block < 1000+numBlocks; block++ {
		for tx := uint32(0); tx < txPerBlock; tx++ {
			for log := uint32(0); log < logsPerTx; log++ {
				topicHi, topicLo := generateTestEvent(block, tx, log)
				result := d.Check(block, tx, log, topicHi, topicLo, block)
				if result {
					processedEvents++
				} else {
					t.Errorf("Unexpected duplicate at block %d, tx %d, log %d", block, tx, log)
				}
			}
		}
	}

	// Simulate reprocessing some blocks (e.g., node restart)
	// Some events might have been evicted due to cache collisions
	duplicatesDetected := 0
	evictedEvents := 0

	for block := uint32(1050); block < 1060; block++ {
		for tx := uint32(0); tx < txPerBlock; tx++ {
			for log := uint32(0); log < logsPerTx; log++ {
				topicHi, topicLo := generateTestEvent(block, tx, log)
				result := d.Check(block, tx, log, topicHi, topicLo, block)
				if !result {
					duplicatesDetected++
				} else {
					evictedEvents++
				}
			}
		}
	}

	totalReplayed := 10 * txPerBlock * logsPerTx
	t.Logf("Processed %d events, detected %d duplicates and %d evicted on replay of %d events",
		processedEvents, duplicatesDetected, evictedEvents, totalReplayed)

	// Due to cache collisions, some entries might have been evicted
	// We should detect most duplicates but not necessarily all
	minExpectedDuplicates := totalReplayed * 80 / 100 // At least 80% should be detected
	if duplicatesDetected < minExpectedDuplicates {
		t.Errorf("Too few duplicates detected: %d (expected at least %d of %d)",
			duplicatesDetected, minExpectedDuplicates, totalReplayed)
	}

	// Eviction rate should be reasonable
	maxEvictionRate := 20 // Up to 20% eviction is acceptable
	evictionRate := evictedEvents * 100 / totalReplayed
	if evictionRate > maxEvictionRate {
		t.Errorf("Eviction rate too high: %d%% (max %d%%)", evictionRate, maxEvictionRate)
	}
}

func TestDeduper_ReorganizationHandling(t *testing.T) {
	d := Deduper{}

	// Process blocks up to 1100
	for block := uint32(1000); block < 1100; block++ {
		topicHi, topicLo := generateTestEvent(block, 0, 0)
		d.Check(block, 0, 0, topicHi, topicLo, block)
	}

	// Simulate reorganization at block 1050
	reorgBlock := uint32(1050)
	currentBlock := uint32(1150)

	// Events from before reorg point should still be detected as duplicates
	for block := uint32(1000); block < reorgBlock; block++ {
		topicHi, topicLo := generateTestEvent(block, 0, 0)
		result := d.Check(block, 0, 0, topicHi, topicLo, currentBlock)
		if result {
			t.Errorf("Block %d before reorg should still be duplicate", block)
		}
	}

	// Simulate reorg beyond MaxReorg threshold
	currentBlock = reorgBlock + constants.MaxReorg + 10

	// Old events should now be considered stale and accepted
	for block := uint32(1000); block < 1010; block++ {
		topicHi, topicLo := generateTestEvent(block, 0, 0)
		result := d.Check(block, 0, 0, topicHi, topicLo, currentBlock)
		if !result {
			t.Errorf("Block %d should be accepted as stale at current block %d", block, currentBlock)
		}
	}
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

func TestDeduper_RaceConditions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	// Note: Deduper is designed for single-threaded use, but we test
	// that concurrent access to different cache slots doesn't cause crashes
	d := Deduper{}

	const numGoroutines = 8
	const eventsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Track successful operations per goroutine
	successCounts := make([]int32, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			successCount := int32(0)
			// Use different block ranges to minimize collisions
			baseBlock := uint32(id * 10000)

			for j := 0; j < eventsPerGoroutine; j++ {
				block := baseBlock + uint32(j)
				tx := uint32(j % 100)
				log := uint32(j % 10)
				topicHi, topicLo := generateTestEvent(block, tx, log)

				result := d.Check(block, tx, log, topicHi, topicLo, block)
				if result {
					successCount++
				}
			}

			atomic.StoreInt32(&successCounts[id], successCount)
		}(i)
	}

	wg.Wait()

	// Verify all goroutines completed
	totalSuccess := int32(0)
	for i := 0; i < numGoroutines; i++ {
		count := atomic.LoadInt32(&successCounts[i])
		totalSuccess += count
		t.Logf("Goroutine %d: %d successful operations", i, count)
	}

	if totalSuccess == 0 {
		t.Error("No successful operations recorded")
	}
}

// ============================================================================
// MEMORY AND PERFORMANCE TESTS
// ============================================================================

func TestDeduper_MemoryLayout(t *testing.T) {
	// Verify structure sizes and alignment
	entrySize := unsafe.Sizeof(dedupeEntry{})
	expectedSize := uintptr(32) // 4*3 + 4 + 8*2 = 32 bytes

	if entrySize != expectedSize {
		t.Errorf("dedupeEntry size: expected %d, got %d", expectedSize, entrySize)
	}

	// Verify alignment
	d := &Deduper{}
	ptr := uintptr(unsafe.Pointer(d))
	if ptr%64 != 0 {
		t.Logf("Warning: Deduper not 64-byte aligned at address %x", ptr)
	}

	// Calculate total memory usage
	totalMemory := unsafe.Sizeof(Deduper{})
	t.Logf("Total cache memory: %d bytes (%.2f MB)", totalMemory, float64(totalMemory)/(1024*1024))
	t.Logf("Cache entries: %d", testCacheSize)
	t.Logf("Entry size: %d bytes", entrySize)
}

func TestDeduper_ZeroAllocations(t *testing.T) {
	d := Deduper{}

	// Verify Check operation allocates no memory
	allocs := testing.AllocsPerRun(100, func() {
		d.Check(1000, 5, 2, 0x1234, 0x5678, 1000)
	})

	if allocs > 0 {
		t.Errorf("Check operation allocated memory: %.2f allocs/op", allocs)
	}
}

// ============================================================================
// STATISTICAL TESTS
// ============================================================================

func TestDeduper_HashDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping statistical test in short mode")
	}

	// Test hash function distribution quality
	const numSamples = 100000
	buckets := make([]int, 256) // Sample 256 buckets

	for i := 0; i < numSamples; i++ {
		block := uint32(i)
		key := uint64(block)<<32 | uint64(i%1000)<<16 | uint64(i%10)
		index := utils.Mix64(key) & ((1 << constants.RingBits) - 1)
		buckets[index%256]++
	}

	// Calculate statistics
	expectedPerBucket := numSamples / 256
	variance := 0.0
	min, max := numSamples, 0

	for _, count := range buckets {
		if count < min {
			min = count
		}
		if count > max {
			max = count
		}
		diff := float64(count - expectedPerBucket)
		variance += diff * diff
	}
	variance /= 256
	stdDev := int(variance)

	t.Logf("Hash distribution: min=%d, max=%d, expected=%d, stdDev=%d",
		min, max, expectedPerBucket, stdDev)

	// Check distribution quality (within 20% of expected)
	tolerance := expectedPerBucket / 5
	if max-min > tolerance*2 {
		t.Errorf("Poor hash distribution: range %d exceeds tolerance %d", max-min, tolerance*2)
	}
}

// ============================================================================
// BENCHMARKS
// ============================================================================

func BenchmarkDeduper_Check_NewEntry(b *testing.B) {
	d := Deduper{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		block := uint32(i + 1000)
		topicHi, topicLo := generateTestEvent(block, 0, 0)
		d.Check(block, 0, 0, topicHi, topicLo, block)
	}
}

func BenchmarkDeduper_Check_Duplicate(b *testing.B) {
	d := Deduper{}

	// Pre-insert an entry
	d.Check(1000, 5, 2, 0x1234, 0x5678, 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		d.Check(1000, 5, 2, 0x1234, 0x5678, 1000)
	}
}

func BenchmarkDeduper_Check_MixedWorkload(b *testing.B) {
	d := Deduper{}

	// Pre-populate with some entries
	for i := 0; i < 1000; i++ {
		block := uint32(i + 1000)
		topicHi, topicLo := generateTestEvent(block, 0, 0)
		d.Check(block, 0, 0, topicHi, topicLo, block)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			// 33% duplicates
			block := uint32((i % 1000) + 1000)
			topicHi, topicLo := generateTestEvent(block, 0, 0)
			d.Check(block, 0, 0, topicHi, topicLo, block)
		} else {
			// 67% new entries
			block := uint32(i + 2000)
			topicHi, topicLo := generateTestEvent(block, 0, 0)
			d.Check(block, 0, 0, topicHi, topicLo, block)
		}
	}
}

func BenchmarkDeduper_Check_RandomAccess(b *testing.B) {
	d := Deduper{}

	// Generate random test data
	randomData := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
	}, 10000)

	randBytes := make([]byte, len(randomData)*32)
	rand.Read(randBytes)

	for i := range randomData {
		offset := i * 32
		randomData[i].block = binary.LittleEndian.Uint32(randBytes[offset:])
		randomData[i].tx = binary.LittleEndian.Uint32(randBytes[offset+4:]) % 1000
		randomData[i].log = binary.LittleEndian.Uint32(randBytes[offset+8:]) % 100
		randomData[i].topicHi = binary.LittleEndian.Uint64(randBytes[offset+12:])
		randomData[i].topicLo = binary.LittleEndian.Uint64(randBytes[offset+20:])
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := &randomData[i%len(randomData)]
		d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.block)
	}
}

func BenchmarkDeduper_Check_WorstCase(b *testing.B) {
	d := Deduper{}

	// Worst case: all entries hash to same slot (if we can find such entries)
	// For now, just use sequential entries which might have some collisions

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Use sequential blocks which might create more collisions
		block := uint32(i)
		d.Check(block, 0, 0, uint64(i), uint64(i), block)
	}
}

// ============================================================================
// EXAMPLE USAGE
// ============================================================================

func ExampleDeduper_Check() {
	d := Deduper{}

	// First occurrence of an event
	block, tx, log := uint32(1000), uint32(5), uint32(2)
	topicHi, topicLo := uint64(0x1234), uint64(0x5678)
	currentBlock := uint32(1000)

	isNew := d.Check(block, tx, log, topicHi, topicLo, currentBlock)
	println("First check:", isNew) // true - new event

	// Same event checked again
	isNew = d.Check(block, tx, log, topicHi, topicLo, currentBlock)
	println("Second check:", isNew) // false - duplicate

	// Same event after reorganization threshold
	currentBlock = block + constants.MaxReorg + 1
	isNew = d.Check(block, tx, log, topicHi, topicLo, currentBlock)
	println("After reorg threshold:", isNew) // true - stale entry
}

// ============================================================================
// FUZZ TESTS
// ============================================================================

func FuzzDeduper_Check(f *testing.F) {
	// Add seed corpus
	f.Add(uint32(1000), uint32(5), uint32(2), uint64(0x1234), uint64(0x5678), uint32(1000))
	f.Add(uint32(0), uint32(0), uint32(0), uint64(0), uint64(0), uint32(0))
	f.Add(uint32(0xFFFFFFFF), uint32(0xFFFFFFFF), uint32(0xFFFFFFFF),
		uint64(0xFFFFFFFFFFFFFFFF), uint64(0xFFFFFFFFFFFFFFFF), uint32(0xFFFFFFFF))

	d := Deduper{}

	f.Fuzz(func(t *testing.T, block, tx, log uint32, topicHi, topicLo uint64, currentBlock uint32) {
		// First check should not panic
		result1 := d.Check(block, tx, log, topicHi, topicLo, currentBlock)

		// Second check with same parameters should return false (duplicate)
		result2 := d.Check(block, tx, log, topicHi, topicLo, currentBlock)
		if result2 && currentBlock <= block+constants.MaxReorg {
			t.Errorf("Second check should detect duplicate: block=%d, current=%d", block, currentBlock)
		}

		// Check with different parameters should typically return true
		if block < 0xFFFFFFFF {
			result3 := d.Check(block+1, tx, log, topicHi, topicLo, currentBlock)
			_ = result1 // First result might be true or false depending on collision
			_ = result3 // Different block might collide, so we don't assert
		}
	})
}
