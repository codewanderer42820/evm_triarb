// dedupe_test.go — Comprehensive test suite for log deduplication system
package dedupe

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"main/constants"
	"main/utils"
	"runtime"
	"testing"
	"time"
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
// UNIT TESTS
// ============================================================================

// TestDeduper_InitialState validates clean initialization
func TestDeduper_InitialState(t *testing.T) {
	d := Deduper{}

	// Verify cache size
	if len(d.entries) != testCacheSize {
		t.Errorf("Cache size: expected %d, got %d", testCacheSize, len(d.entries))
	}

	// Verify zero initialization
	for i := range d.entries {
		entry := &d.entries[i]
		if entry.block != 0 || entry.tx != 0 || entry.log != 0 ||
			entry.seenAt != 0 || entry.topicHi != 0 || entry.topicLo != 0 {
			t.Errorf("Entry %d not zero-initialized", i)
		}
	}
}

// TestDeduper_BasicDeduplication validates core functionality
func TestDeduper_BasicDeduplication(t *testing.T) {
	d := Deduper{}

	// First insertion should be accepted
	result := d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)
	if !result {
		t.Error("First insertion should return true")
	}

	// Exact duplicate should be rejected
	result = d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)
	if result {
		t.Error("Duplicate should return false")
	}
}

// TestDeduper_CoordinateDistinction validates coordinate field sensitivity
func TestDeduper_CoordinateDistinction(t *testing.T) {
	base := struct {
		blk, tx, log uint32
		tagHi, tagLo uint64
	}{
		1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321,
	}

	testCases := []struct {
		name   string
		modify func() (uint32, uint32, uint32, uint64, uint64)
	}{
		{"DifferentBlock", func() (uint32, uint32, uint32, uint64, uint64) {
			return base.blk + 1, base.tx, base.log, base.tagHi, base.tagLo
		}},
		{"DifferentTx", func() (uint32, uint32, uint32, uint64, uint64) {
			return base.blk, base.tx + 1, base.log, base.tagHi, base.tagLo
		}},
		{"DifferentLog", func() (uint32, uint32, uint32, uint64, uint64) {
			return base.blk, base.tx, base.log + 1, base.tagHi, base.tagLo
		}},
		{"DifferentTopicHi", func() (uint32, uint32, uint32, uint64, uint64) {
			return base.blk, base.tx, base.log, base.tagHi + 1, base.tagLo
		}},
		{"DifferentTopicLo", func() (uint32, uint32, uint32, uint64, uint64) {
			return base.blk, base.tx, base.log, base.tagHi, base.tagLo + 1
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := Deduper{}

			// Insert base entry
			d.Check(base.blk, base.tx, base.log, base.tagHi, base.tagLo, base.blk)

			// Test variation
			blk, tx, log, tagHi, tagLo := tc.modify()
			result := d.Check(blk, tx, log, tagHi, tagLo, base.blk)
			if !result {
				t.Errorf("%s should be accepted as distinct", tc.name)
			}
		})
	}
}

// TestDeduper_StalenessHandling validates reorganization logic
func TestDeduper_StalenessHandling(t *testing.T) {
	d := Deduper{}

	// Insert at block 1000
	d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)

	// Within reorg threshold - should be duplicate
	result := d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000+constants.MaxReorg)
	if result {
		t.Error("Entry within reorg threshold should be duplicate")
	}

	// Beyond reorg threshold - should be accepted
	result = d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000+constants.MaxReorg+1)
	if !result {
		t.Error("Entry beyond reorg threshold should be accepted")
	}
}

// TestDeduper_EdgeCases validates boundary conditions
func TestDeduper_EdgeCases(t *testing.T) {
	t.Run("ZeroValues", func(t *testing.T) {
		d := Deduper{}

		result := d.Check(0, 0, 0, 0, 0, 0)
		if !result {
			t.Error("Zero values should be accepted")
		}

		result = d.Check(0, 0, 0, 0, 0, 0)
		if result {
			t.Error("Duplicate zero values should be rejected")
		}
	})

	t.Run("MaxValues", func(t *testing.T) {
		d := Deduper{}

		maxBlk := uint32(0xFFFFFFFF)
		maxTx := uint32(0xFFFFFFFF)
		maxLog := uint32(0xFFFFFFFF)
		maxTagHi := uint64(0xFFFFFFFFFFFFFFFF)
		maxTagLo := uint64(0xFFFFFFFFFFFFFFFF)

		result := d.Check(maxBlk, maxTx, maxLog, maxTagHi, maxTagLo, maxBlk)
		if !result {
			t.Error("Max values should be accepted")
		}

		result = d.Check(maxBlk, maxTx, maxLog, maxTagHi, maxTagLo, maxBlk)
		if result {
			t.Error("Duplicate max values should be rejected")
		}
	})

	t.Run("SeenAtOverflow", func(t *testing.T) {
		d := Deduper{}

		// Test currentBlock = 0 which would make seenAt = 0
		result := d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 0)
		if !result {
			t.Error("Entry with currentBlock=0 should be accepted")
		}

		// Verify seenAt was set to 1 (not 0)
		key := uint64(1000)<<32 | uint64(5)<<16 | uint64(2)
		index := utils.Mix64(key) & ((1 << constants.RingBits) - 1)
		if d.entries[index].seenAt != 1 {
			t.Error("seenAt should be 1 when currentBlock=0")
		}
	})
}

// TestDeduper_BitPatterns validates specific bit patterns
func TestDeduper_BitPatterns(t *testing.T) {
	patterns := []struct {
		name         string
		blk, tx, log uint32
		tagHi, tagLo uint64
	}{
		{"Alternating", 0xAAAAAAAA, 0xAAAAAAAA, 0xAAAAAAAA, 0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA},
		{"Checkerboard", 0x55555555, 0x55555555, 0x55555555, 0x5555555555555555, 0x5555555555555555},
		{"PowersOf2", 0x80000000, 0x80000000, 0x80000000, 0x8000000000000000, 0x8000000000000000},
		{"Sequential", 0x01234567, 0x89abcdef, 0x01234567, 0x0123456789abcdef, 0xfedcba9876543210},
	}

	for _, p := range patterns {
		t.Run(p.name, func(t *testing.T) {
			d := Deduper{}

			result := d.Check(p.blk, p.tx, p.log, p.tagHi, p.tagLo, p.blk)
			if !result {
				t.Errorf("Pattern %s should be accepted", p.name)
			}

			result = d.Check(p.blk, p.tx, p.log, p.tagHi, p.tagLo, p.blk)
			if result {
				t.Errorf("Duplicate pattern %s should be rejected", p.name)
			}
		})
	}
}

// ============================================================================
// COLLISION AND HASH TESTS
// ============================================================================

// TestDeduper_HashCollisions validates collision handling
func TestDeduper_HashCollisions(t *testing.T) {
	d := Deduper{}

	// Find two entries that hash to the same slot
	baseKey := uint64(1000)<<32 | uint64(5)<<16 | uint64(2)
	mask := uint64((1 << constants.RingBits) - 1)
	targetSlot := utils.Mix64(baseKey) & mask

	// Insert first entry
	d.Check(1000, 5, 2, 0x1111111111111111, 0x2222222222222222, 1000)

	// Search for colliding key
	for i := uint64(1); i < 1000; i++ {
		testKey := baseKey + (i << 32)
		if utils.Mix64(testKey)&mask == targetSlot {
			blk := uint32(testKey >> 32)

			// Colliding entry should evict original
			result := d.Check(blk, 5, 2, 0x3333333333333333, 0x4444444444444444, blk)
			if !result {
				t.Error("Colliding entry should be accepted")
			}

			// Original should now be accepted (was evicted)
			result = d.Check(1000, 5, 2, 0x1111111111111111, 0x2222222222222222, 1000)
			if !result {
				t.Error("Original entry should be accepted after eviction")
			}
			return
		}
	}

	t.Skip("Could not find hash collision within search limit")
}

// TestDeduper_CacheEviction validates direct-mapped cache behavior
func TestDeduper_CacheEviction(t *testing.T) {
	d := Deduper{}

	// Fill beyond cache capacity
	numEntries := testCacheSize + 1000

	for i := 0; i < numEntries; i++ {
		blk := uint32(i + 1000)
		result := d.Check(blk, 0, 0, uint64(i), uint64(i), blk)
		if !result {
			t.Errorf("Entry %d should be accepted during fill", i)
		}
	}

	// Some early entries should have been evicted
	evicted := 0
	for i := 0; i < 100; i++ {
		blk := uint32(i + 1000)
		result := d.Check(blk, 0, 0, uint64(i), uint64(i), blk)
		if result {
			evicted++
		}
	}

	if evicted == 0 {
		t.Error("Expected some early entries to be evicted")
	}

	t.Logf("Cache eviction working: %d entries evicted", evicted)
}

// ============================================================================
// LARGE DATASET TESTS
// ============================================================================

// TestDeduper_LargeDataset validates realistic workload performance
func TestDeduper_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	d := Deduper{}

	// Generate realistic EVM-like data
	entries := make([]struct {
		blk, tx, log uint32
		tagHi, tagLo uint64
	}, testDataSize)

	for i := 0; i < testDataSize; i++ {
		seed := make([]byte, 8)
		binary.LittleEndian.PutUint64(seed, uint64(i))
		hash := sha256.Sum256(seed)

		entries[i] = struct {
			blk, tx, log uint32
			tagHi, tagLo uint64
		}{
			blk:   (binary.LittleEndian.Uint32(hash[0:4]) % 1000) + 1000,
			tx:    binary.LittleEndian.Uint32(hash[4:8]) % 5000,
			log:   binary.LittleEndian.Uint32(hash[8:12]) % 32,
			tagHi: binary.LittleEndian.Uint64(hash[12:20]),
			tagLo: binary.LittleEndian.Uint64(hash[20:28]),
		}
	}

	// Phase 1: Insert all entries
	for _, entry := range entries {
		result := d.Check(entry.blk, entry.tx, entry.log, entry.tagHi, entry.tagLo, entry.blk)
		if !result {
			t.Error("New entry should be accepted")
		}
	}

	// Phase 2: Test duplicates (some will fail due to collisions)
	duplicatesFailed := 0
	for _, entry := range entries {
		result := d.Check(entry.blk, entry.tx, entry.log, entry.tagHi, entry.tagLo, entry.blk)
		if result {
			duplicatesFailed++
		}
	}

	// Validate collision rate is reasonable for direct-mapped cache
	failureRate := float64(duplicatesFailed) / float64(testDataSize) * 100
	expectedRate := float64(testDataSize) / float64(testCacheSize) * 100

	t.Logf("Cache utilization: %.1f%%", float64(testDataSize)/float64(testCacheSize)*100)
	t.Logf("Collision rate: %.1f%% (expected ~%.1f%%)", failureRate, expectedRate)

	if failureRate > expectedRate*3 {
		t.Errorf("Collision rate too high: %.1f%%", failureRate)
	}
}

// TestDeduper_RealWorkflow validates complete operational cycle
func TestDeduper_RealWorkflow(t *testing.T) {
	d := Deduper{}

	// Simulate blockchain processing
	for block := uint32(1000); block < 1100; block++ {
		for tx := uint32(0); tx < 10; tx++ {
			for log := uint32(0); log < 3; log++ {
				tagHi := uint64(block)<<32 | uint64(tx)
				tagLo := uint64(log)<<32 | uint64(block)

				result := d.Check(block, tx, log, tagHi, tagLo, block)
				if !result {
					t.Errorf("New entry should be accepted: block=%d tx=%d log=%d", block, tx, log)
				}
			}
		}
	}

	// Test reorganization scenario
	reorgBlock := uint32(1050)
	for tx := uint32(0); tx < 5; tx++ {
		for log := uint32(0); log < 2; log++ {
			// Same coordinates, different content (different topics)
			tagHi := uint64(reorgBlock+1000)<<32 | uint64(tx)
			tagLo := uint64(log)<<32 | uint64(reorgBlock+1000)
			currentBlock := reorgBlock + constants.MaxReorg + 1

			result := d.Check(reorgBlock, tx, log, tagHi, tagLo, currentBlock)
			if !result {
				t.Errorf("Reorg entry should be accepted: block=%d tx=%d log=%d", reorgBlock, tx, log)
			}
		}
	}
}

// ============================================================================
// MEMORY LAYOUT TESTS
// ============================================================================

// TestDeduper_MemoryLayout validates structure alignment
func TestDeduper_MemoryLayout(t *testing.T) {
	// Verify entry size
	expectedEntrySize := uintptr(32) // 4+4+4+4+8+8 = 32 bytes
	if unsafe.Sizeof(dedupeEntry{}) != expectedEntrySize {
		t.Errorf("Entry size: expected %d, got %d", expectedEntrySize, unsafe.Sizeof(dedupeEntry{}))
	}

	// Verify deduper alignment
	d := &Deduper{}
	addr := uintptr(unsafe.Pointer(d))
	if addr%64 != 0 {
		t.Errorf("Deduper not 64-byte aligned: %d", addr%64)
	}

	t.Logf("Entry size: %d bytes", unsafe.Sizeof(dedupeEntry{}))
	t.Logf("Cache size: %d entries (%d KB)", len(d.entries), len(d.entries)*32/1024)
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

// TestDeduper_ConcurrentAccess validates thread safety assumptions
func TestDeduper_ConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	d := Deduper{}
	const numGoroutines = 8
	const opsPerGoroutine = 1000

	// Test concurrent access to different cache slots
	// Note: Deduper is not thread-safe by design, but this tests data races
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < opsPerGoroutine; j++ {
				// Use different base blocks to reduce collision probability
				blk := uint32(id*10000 + j + 1000)
				d.Check(blk, uint32(j), 0, uint64(id), uint64(j), blk)
			}
		}(i)
	}

	// Let goroutines complete
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
}

// ============================================================================
// BENCHMARKS
// ============================================================================

// BenchmarkDeduper_Check measures core operation performance
func BenchmarkDeduper_Check(b *testing.B) {
	d := Deduper{}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		blk := uint32(i + 1000)
		tx := uint32(i % 65536)
		log := uint32(i % 32)
		tagHi := uint64(i) << 32
		tagLo := uint64(i + 1000000)

		d.Check(blk, tx, log, tagHi, tagLo, blk)
	}
}

// BenchmarkDeduper_NewEntries measures cache miss performance
func BenchmarkDeduper_NewEntries(b *testing.B) {
	d := Deduper{}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		blk := uint32(i + 1000)
		result := d.Check(blk, 0, 0, uint64(i), uint64(i), blk)
		if !result {
			b.Fatal("New entry should be accepted")
		}
	}
}

// BenchmarkDeduper_Duplicates measures cache hit performance
func BenchmarkDeduper_Duplicates(b *testing.B) {
	d := Deduper{}

	// Insert one entry
	d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result := d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)
		if result {
			b.Fatal("Duplicate should be rejected")
		}
	}
}

// BenchmarkDeduper_MixedWorkload measures realistic performance
func BenchmarkDeduper_MixedWorkload(b *testing.B) {
	d := Deduper{}

	// Pre-populate cache
	for i := 0; i < 1000; i++ {
		d.Check(uint32(i+1000), 0, 0, uint64(i), uint64(i), uint32(i+1000))
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if i%10 < 7 {
			// 70% new entries
			blk := uint32(i + 2000)
			d.Check(blk, 0, 0, uint64(i), uint64(i), blk)
		} else {
			// 30% duplicates
			idx := i % 1000
			d.Check(uint32(idx+1000), 0, 0, uint64(idx), uint64(idx), uint32(idx+1000))
		}
	}
}

// BenchmarkDeduper_HighFrequency simulates high-frequency blockchain
func BenchmarkDeduper_HighFrequency(b *testing.B) {
	d := Deduper{}

	b.ReportAllocs()
	blockCounter := 0
	logCounter := 0

	for i := 0; i < b.N; i++ {
		blk := uint32(1000 + blockCounter)
		tx := uint32(logCounter % 1000)
		log := uint32(logCounter % 50)
		tagHi := uint64(blk)<<32 | uint64(tx)
		tagLo := uint64(log)<<32 | uint64(blockCounter)

		d.Check(blk, tx, log, tagHi, tagLo, blk)

		logCounter++
		if logCounter >= 5000 {
			logCounter = 0
			blockCounter++
		}
	}
}

// BenchmarkDeduper_RandomData measures performance with high entropy
func BenchmarkDeduper_RandomData(b *testing.B) {
	d := Deduper{}

	// Pre-generate random data
	randomData := make([]struct {
		blk, tx, log uint32
		tagHi, tagLo uint64
	}, 10000)

	for i := range randomData {
		var buf [32]byte
		rand.Read(buf[:])

		randomData[i] = struct {
			blk, tx, log uint32
			tagHi, tagLo uint64
		}{
			blk:   binary.BigEndian.Uint32(buf[0:4]),
			tx:    binary.BigEndian.Uint32(buf[4:8]) & 0xFFFF,
			log:   binary.BigEndian.Uint32(buf[8:12]) & 0xFFFF,
			tagHi: binary.BigEndian.Uint64(buf[12:20]),
			tagLo: binary.BigEndian.Uint64(buf[20:28]),
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data := &randomData[i%len(randomData)]
		d.Check(data.blk, data.tx, data.log, data.tagHi, data.tagLo, data.blk)
	}
}

// BenchmarkDeduper_CacheUtilization measures performance at different fill levels
func BenchmarkDeduper_CacheUtilization(b *testing.B) {
	levels := []struct {
		name string
		fill float64
	}{
		{"10pct", 0.10},
		{"50pct", 0.50},
		{"90pct", 0.90},
	}

	for _, level := range levels {
		b.Run(level.name, func(b *testing.B) {
			d := Deduper{}
			fillSize := int(float64(testCacheSize) * level.fill)

			// Pre-fill cache
			for i := 0; i < fillSize; i++ {
				d.Check(uint32(i+1000), 0, 0, uint64(i), uint64(i), uint32(i+1000))
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if i%2 == 0 {
					// New entry
					blk := uint32(i + fillSize + 1000)
					d.Check(blk, 0, 0, uint64(i+fillSize), uint64(i+fillSize), blk)
				} else {
					// Existing entry lookup
					idx := i % fillSize
					d.Check(uint32(idx+1000), 0, 0, uint64(idx), uint64(idx), uint32(idx+1000))
				}
			}
		})
	}
}

// BenchmarkDeduper_MemoryEfficiency validates zero allocations
func BenchmarkDeduper_MemoryEfficiency(b *testing.B) {
	d := Deduper{}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d.Check(uint32(i+1000), 0, 0, uint64(i), uint64(i), uint32(i+1000))
	}

	// Verify zero allocations
	allocsPerOp := testing.AllocsPerRun(100, func() {
		d.Check(1000, 0, 0, 0, 0, 1000)
	})

	if allocsPerOp > 0 {
		b.Errorf("Expected zero allocations, got %.2f allocs/op", allocsPerOp)
	}
}

// ============================================================================
// PERFORMANCE VALIDATION
// ============================================================================

// TestDeduper_PerformanceRequirements validates timing requirements
func TestDeduper_PerformanceRequirements(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance validation in short mode")
	}

	d := Deduper{}
	const iterations = 100000

	// Measure average operation latency
	start := time.Now()
	for i := 0; i < iterations; i++ {
		d.Check(uint32(i+1000), 0, 0, uint64(i), uint64(i), uint32(i+1000))
	}
	elapsed := time.Since(start)

	nsPerOp := elapsed.Nanoseconds() / iterations
	t.Logf("Average latency: %d ns/op", nsPerOp)

	// Validate excellent performance (based on benchmark results showing 3-5ns typical)
	if nsPerOp > 10 {
		t.Errorf("Performance requirement not met: %d ns/op (expected <10ns)", nsPerOp)
	}

	// Measure throughput
	opsPerSec := float64(iterations) / elapsed.Seconds()
	t.Logf("Throughput: %.0f ops/sec", opsPerSec)

	// Based on benchmark results showing 200M-400M+ ops/sec capability
	if opsPerSec < 100_000_000 {
		t.Errorf("Throughput too low: %.0f ops/sec (expected >100M)", opsPerSec)
	}

	// Performance classification based on benchmark results
	if nsPerOp < 3 {
		t.Logf("Performance class: EXCEPTIONAL (sub-3ns - matches duplicate detection)")
	} else if nsPerOp < 5 {
		t.Logf("Performance class: EXCELLENT (sub-5ns - matches typical operations)")
	} else if nsPerOp < 10 {
		t.Logf("Performance class: GOOD (sub-10ns)")
	} else {
		t.Logf("Performance class: NEEDS_OPTIMIZATION (>10ns)")
	}
}
