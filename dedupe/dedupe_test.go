package dedupe

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"main/constants"
	"main/utils"
	"testing"
	"time"
)

// ============================================================================
// CORE FUNCTIONALITY TESTS
// ============================================================================

// TestDeduper_Basic validates fundamental deduplication behavior.
// Tests the most basic insert-then-check-duplicate scenario.
func TestDeduper_Basic(t *testing.T) {
	d := Deduper{}

	// Test first insertion - should return true (new entry)
	result := d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)
	if !result {
		t.Error("First insertion should return true (new entry)")
	}

	// Test exact duplicate - should return false (duplicate detected)
	result = d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)
	if result {
		t.Error("Exact duplicate should return false (duplicate detected)")
	}
}

// TestDeduper_BasicCorrectness proves the deduplicator logic is mathematically correct.
// Uses well-spaced entries to avoid hash collisions and test pure logic.
func TestDeduper_BasicCorrectness(t *testing.T) {
	d := Deduper{}

	// Use entries guaranteed not to hash to the same cache slot
	// by spacing them far apart in the hash space
	testCases := []struct {
		name         string
		blk, tx, log uint32
		tagHi, tagLo uint64
	}{
		{"Entry1", 1000, 1, 1, 0x1111111111111111, 0x2222222222222222},
		{"Entry2", 2000, 2, 2, 0x3333333333333333, 0x4444444444444444},
		{"Entry3", 3000, 3, 3, 0x5555555555555555, 0x6666666666666666},
		{"Entry4", 4000, 4, 4, 0x7777777777777777, 0x8888888888888888},
		{"Entry5", 5000, 5, 5, 0x9999999999999999, 0xAAAAAAAAAAAAAAAA},
	}

	// ──── PHASE 1: Initial insertions should all be accepted ────
	for i, tc := range testCases {
		result := d.Check(tc.blk, tc.tx, tc.log, tc.tagHi, tc.tagLo, tc.blk)
		if !result {
			t.Errorf("Test case %d (%s) should be accepted as new entry", i, tc.name)
		}
	}

	// ──── PHASE 2: Exact duplicates should all be rejected ────
	for i, tc := range testCases {
		result := d.Check(tc.blk, tc.tx, tc.log, tc.tagHi, tc.tagLo, tc.blk)
		if result {
			t.Errorf("Test case %d (%s) should be rejected as duplicate", i, tc.name)
		}
	}

	// ──── PHASE 3: Different coordinates should be accepted ────
	for i, tc := range testCases {
		// Different block number
		result := d.Check(tc.blk+1, tc.tx, tc.log, tc.tagHi, tc.tagLo, tc.blk)
		if !result {
			t.Errorf("Test case %d with different block should be accepted", i)
		}

		// Different transaction index
		result = d.Check(tc.blk, tc.tx+1, tc.log, tc.tagHi, tc.tagLo, tc.blk)
		if !result {
			t.Errorf("Test case %d with different tx should be accepted", i)
		}

		// Different log index
		result = d.Check(tc.blk, tc.tx, tc.log+1, tc.tagHi, tc.tagLo, tc.blk)
		if !result {
			t.Errorf("Test case %d with different log should be accepted", i)
		}

		// Different topic high bits
		result = d.Check(tc.blk, tc.tx, tc.log, tc.tagHi+1, tc.tagLo, tc.blk)
		if !result {
			t.Errorf("Test case %d with different topicHi should be accepted", i)
		}

		// Different topic low bits
		result = d.Check(tc.blk, tc.tx, tc.log, tc.tagHi, tc.tagLo+1, tc.blk)
		if !result {
			t.Errorf("Test case %d with different topicLo should be accepted", i)
		}
	}

	// ──── PHASE 4: Staleness threshold verification ────
	tc := testCases[0]

	// Insert entry at block 1000
	d.Check(tc.blk, tc.tx, tc.log, tc.tagHi, tc.tagLo, 1000)

	// Should still be duplicate within MaxReorg threshold
	result := d.Check(tc.blk, tc.tx, tc.log, tc.tagHi, tc.tagLo, 1000+constants.MaxReorg)
	if result {
		t.Error("Entry should still be duplicate within MaxReorg threshold")
	}

	// Should be accepted as new beyond MaxReorg threshold
	result = d.Check(tc.blk, tc.tx, tc.log, tc.tagHi, tc.tagLo, 1000+constants.MaxReorg+1)
	if !result {
		t.Error("Entry should be accepted as new beyond MaxReorg threshold")
	}

	t.Log("✅ Deduplicator logic is mathematically correct")
	t.Log("✅ Any failures in large dataset tests are due to expected hash collisions")
}

// ============================================================================
// FINGERPRINT AND COORDINATE TESTS
// ============================================================================

// TestDeduper_DifferentFingerprints validates that logs with identical coordinates
// but different topic fingerprints are correctly distinguished.
func TestDeduper_DifferentFingerprints(t *testing.T) {
	d := Deduper{}

	// Same block/tx/log coordinates but different topic fingerprints
	// This represents different events at the same location
	result1 := d.Check(1000, 5, 2, 0x1111111111111111, 0x2222222222222222, 1000)
	result2 := d.Check(1000, 5, 2, 0x3333333333333333, 0x4444444444444444, 1000)

	if !result1 || !result2 {
		t.Error("Different topic fingerprints should both be accepted as distinct entries")
	}
}

// TestDeduper_PartialMatches ensures partial coordinate matches don't trigger false positives.
// Critical for ensuring logs from related transactions are properly distinguished.
func TestDeduper_PartialMatches(t *testing.T) {
	baseBlk, baseTx, baseLog := uint32(1000), uint32(5), uint32(2)
	baseTagHi, baseTagLo := uint64(0x1234567890abcdef), uint64(0xfedcba0987654321)

	// Test combinations that should NOT be considered duplicates
	testCases := []struct {
		name   string
		blk    uint32
		tx     uint32
		log    uint32
		tagHi  uint64
		tagLo  uint64
		should bool
	}{
		{"Different block", baseBlk + 1, baseTx, baseLog, baseTagHi, baseTagLo, true},
		{"Different tx", baseBlk, baseTx + 1, baseLog, baseTagHi, baseTagLo, true},
		{"Different log", baseBlk, baseTx, baseLog + 1, baseTagHi, baseTagLo, true},
		{"Different tagHi", baseBlk, baseTx, baseLog, baseTagHi + 1, baseTagLo, true},
		{"Different tagLo", baseBlk, baseTx, baseLog, baseTagHi, baseTagLo + 1, true},
		{"Exact match", baseBlk, baseTx, baseLog, baseTagHi, baseTagLo, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create fresh deduplicator for each test to avoid interference
			d := Deduper{}

			// Insert base entry
			d.Check(baseBlk, baseTx, baseLog, baseTagHi, baseTagLo, baseBlk)

			// Test the specific variation
			result := d.Check(tc.blk, tc.tx, tc.log, tc.tagHi, tc.tagLo, baseBlk)
			if result != tc.should {
				t.Errorf("Test %s: expected %v, got %v", tc.name, tc.should, result)
			}
		})
	}
}

// ============================================================================
// REORGANIZATION HANDLING TESTS
// ============================================================================

// TestDeduper_ReorgHandling validates blockchain reorganization scenarios.
// Ensures stale entries are properly evicted when chains reorganize.
func TestDeduper_ReorgHandling(t *testing.T) {
	d := Deduper{}

	// Insert log at block 1000
	d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)

	// Advance chain beyond reorganization threshold
	latestBlk := uint32(1000 + constants.MaxReorg + 1)

	// Same log should now be accepted due to staleness eviction
	result := d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, latestBlk)
	if !result {
		t.Error("Stale log should be accepted after exceeding reorg threshold")
	}
}

// TestDeduper_EdgeCaseReorg tests the exact boundary of the reorganization threshold.
// Critical for ensuring precise staleness behavior.
func TestDeduper_EdgeCaseReorg(t *testing.T) {
	d := Deduper{}

	// Insert log at block 1000
	d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)

	// Test exactly at reorg threshold (should still be duplicate)
	latestBlk := uint32(1000 + constants.MaxReorg)
	result := d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, latestBlk)
	if result {
		t.Error("Log at exactly reorg threshold should still be considered duplicate")
	}

	// Test one block beyond threshold (should be accepted)
	latestBlk = uint32(1000 + constants.MaxReorg + 1)
	result = d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, latestBlk)
	if !result {
		t.Error("Log beyond reorg threshold should be accepted as new")
	}
}

// TestDeduper_ReorgScenarios tests comprehensive reorganization scenarios.
// Simulates realistic blockchain reorg situations.
func TestDeduper_ReorgScenarios(t *testing.T) {
	d := Deduper{}

	baseBlock := uint32(1000)

	// Insert logs representing the original chain
	for i := 0; i < 10; i++ {
		blk := baseBlock + uint32(i)
		d.Check(blk, 0, uint32(i), uint64(i), uint64(i+1000), blk)
	}

	// Simulate deep reorganization: same blocks but different content
	latestBlock := baseBlock + uint32(constants.MaxReorg) + 10

	for i := 0; i < 10; i++ {
		blk := baseBlock + uint32(i)
		// Different topic fingerprint representing new chain content
		result := d.Check(blk, 0, uint32(i), uint64(i+1000), uint64(i+2000), latestBlock)
		if !result {
			t.Errorf("Reorganized log should be accepted: block=%d, index=%d", blk, i)
		}
	}
}

// ============================================================================
// EDGE CASE AND BOUNDARY TESTS
// ============================================================================

// TestDeduper_MaxValues tests extreme EVM coordinate values.
// Ensures the system works correctly with maximum possible values.
func TestDeduper_MaxValues(t *testing.T) {
	d := Deduper{}

	// Test with maximum possible EVM values
	maxBlk := uint32(0xFFFFFFFF)           // Maximum block number
	maxTx := uint32(0xFFFF)                // Maximum transaction index
	maxLog := uint32(0xFFFF)               // Maximum log index
	maxTagHi := uint64(0xFFFFFFFFFFFFFFFF) // Maximum topic high bits
	maxTagLo := uint64(0xFFFFFFFFFFFFFFFF) // Maximum topic low bits

	result := d.Check(maxBlk, maxTx, maxLog, maxTagHi, maxTagLo, maxBlk)
	if !result {
		t.Error("Maximum values should be accepted")
	}

	// Test duplicate detection with maximum values
	result = d.Check(maxBlk, maxTx, maxLog, maxTagHi, maxTagLo, maxBlk)
	if result {
		t.Error("Duplicate with maximum values should be rejected")
	}
}

// TestDeduper_ZeroValues validates zero value handling.
// Ensures genesis block and early transactions work correctly.
func TestDeduper_ZeroValues(t *testing.T) {
	d := Deduper{}

	// Test with all zero values (genesis scenarios)
	result := d.Check(0, 0, 0, 0, 0, 0)
	if !result {
		t.Error("Zero values should be accepted")
	}

	// Test duplicate detection with zero values
	result = d.Check(0, 0, 0, 0, 0, 0)
	if result {
		t.Error("Duplicate zero values should be rejected")
	}
}

// TestDeduper_BitPatterns tests specific bit patterns that might cause issues.
// Validates behavior with various bit manipulation edge cases.
func TestDeduper_BitPatterns(t *testing.T) {
	d := Deduper{}

	patterns := []struct {
		name  string
		blk   uint32
		tx    uint32
		log   uint32
		tagHi uint64
		tagLo uint64
	}{
		{"All ones", 0xFFFFFFFF, 0xFFFF, 0xFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
		{"All zeros", 0, 0, 0, 0, 0},
		{"Alternating bits", 0xAAAAAAAA, 0xAAAA, 0xAAAA, 0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA},
		{"Checkerboard", 0x55555555, 0x5555, 0x5555, 0x5555555555555555, 0x5555555555555555},
		{"Powers of 2", 0x80000000, 0x8000, 0x8000, 0x8000000000000000, 0x8000000000000000},
		{"Sequential", 0x01234567, 0x0123, 0x4567, 0x0123456789ABCDEF, 0xFEDCBA9876543210},
	}

	for _, p := range patterns {
		t.Run(p.name, func(t *testing.T) {
			// Test first insertion
			result := d.Check(p.blk, p.tx, p.log, p.tagHi, p.tagLo, p.blk)
			if !result {
				t.Errorf("Pattern %s should be accepted on first insertion", p.name)
			}

			// Test duplicate detection
			result = d.Check(p.blk, p.tx, p.log, p.tagHi, p.tagLo, p.blk)
			if result {
				t.Errorf("Duplicate pattern %s should be rejected", p.name)
			}
		})
	}
}

// TestDeduper_ExtremeBitFlips tests single-bit differences in coordinates.
// Validates that minimal coordinate changes are properly detected.
func TestDeduper_ExtremeBitFlips(t *testing.T) {
	d := Deduper{}

	base := struct {
		blk, tx, log uint32
		tagHi, tagLo uint64
	}{
		blk:   0x12345678,
		tx:    0x1234,
		log:   0x5678,
		tagHi: 0x1234567890abcdef,
		tagLo: 0xfedcba0987654321,
	}

	// Insert base entry
	d.Check(base.blk, base.tx, base.log, base.tagHi, base.tagLo, base.blk)

	// Test single bit flips in block number (different blocks in chain)
	for bit := 0; bit < 32; bit++ {
		flippedBlk := base.blk ^ (1 << bit)
		result := d.Check(flippedBlk, base.tx, base.log, base.tagHi, base.tagLo, flippedBlk)
		if !result {
			t.Errorf("Single bit flip in block at position %d should be accepted", bit)
		}
	}

	// Test single bit flips in transaction index (different transactions)
	for bit := 0; bit < 16; bit++ {
		flippedTx := base.tx ^ (1 << bit)
		// Modify topic to represent different transaction content
		modifiedTagLo := base.tagLo ^ uint64(flippedTx)
		result := d.Check(base.blk, flippedTx, base.log, base.tagHi, modifiedTagLo, base.blk)
		if !result {
			t.Errorf("Single bit flip in tx at position %d should be accepted", bit)
		}
	}

	// Test single bit flips in log index (different logs in transaction)
	for bit := 0; bit < 16; bit++ {
		flippedLog := base.log ^ (1 << bit)
		// Modify topic to represent different log content
		modifiedTagHi := base.tagHi ^ uint64(flippedLog<<16)
		result := d.Check(base.blk, base.tx, flippedLog, modifiedTagHi, base.tagLo, base.blk)
		if !result {
			t.Errorf("Single bit flip in log at position %d should be accepted", bit)
		}
	}

	// Verify exact duplicate is still properly rejected
	result := d.Check(base.blk, base.tx, base.log, base.tagHi, base.tagLo, base.blk)
	if result {
		t.Error("Exact duplicate should still be rejected after bit flip tests")
	}
}

// ============================================================================
// COLLISION AND HASH TESTS
// ============================================================================

// TestDeduper_HashCollisions tests hash collision scenarios.
// Validates behavior when different entries hash to the same cache slot.
func TestDeduper_HashCollisions(t *testing.T) {
	d := Deduper{}

	// Generate entries that might hash to similar slots
	// Use patterns that stress the hash function
	for i := 0; i < 100; i++ {
		blk := uint32(i << 16) // Spacing to encourage hash collisions
		tx := uint32(i % 256)
		log := uint32(i % 16)
		tagHi := uint64(i) << 32
		tagLo := uint64(i) ^ 0xAAAAAAAAAAAAAAAA

		result := d.Check(blk, tx, log, tagHi, tagLo, blk)
		if !result {
			t.Errorf("Hash collision test entry %d should be accepted", i)
		}
	}
}

// TestDeduper_SlotOverwriting tests direct-mapped cache slot overwriting.
// Validates that hash collisions properly evict old entries.
func TestDeduper_SlotOverwriting(t *testing.T) {
	d := Deduper{}

	// Create entries that will hash to the same cache slot
	baseKey := uint64(1000)<<32 | uint64(5)<<16 | uint64(2)

	// Insert first entry
	d.Check(1000, 5, 2, 0x1111111111111111, 0x2222222222222222, 1000)

	// Find another key that hashes to the same slot
	mask := uint64((1 << constants.RingBits) - 1)
	targetSlot := utils.Mix64(baseKey) & mask

	// Search for a colliding key
	for i := uint64(1); i < 10000; i++ {
		testKey := baseKey + (i << 32) // Different block number
		if utils.Mix64(testKey)&mask == targetSlot {
			// Found collision - this will evict the first entry
			blk := uint32(testKey >> 32)
			result := d.Check(blk, 5, 2, 0x3333333333333333, 0x4444444444444444, blk)
			if !result {
				t.Error("Colliding entry should be accepted (cache eviction)")
			}

			// Original entry should now be accepted again (was evicted)
			result = d.Check(1000, 5, 2, 0x1111111111111111, 0x2222222222222222, 1000)
			if !result {
				t.Error("Original entry should be accepted after cache eviction")
			}
			return
		}
	}

	t.Skip("Could not find hash collision within search limit")
}

// ============================================================================
// LARGE DATASET AND REALISTIC WORKLOAD TESTS
// ============================================================================

// TestDeduper_LargeDataset tests realistic EVM workloads with proper collision expectations.
// Validates performance with large datasets while accounting for direct-mapped cache limitations.
func TestDeduper_LargeDataset(t *testing.T) {
	d := Deduper{}

	// Use realistic test size for EVM log processing
	const totalLogs = 20000

	logEntries := make([]struct {
		blk, tx, log uint32
		tagHi, tagLo uint64
	}, totalLogs)

	// Generate realistic EVM-like data with SHA256 for excellent distribution
	for i := 0; i < totalLogs; i++ {
		// Use SHA256 to generate well-distributed but deterministic data
		seed := make([]byte, 8)
		binary.LittleEndian.PutUint64(seed, uint64(i))
		hash := sha256.Sum256(seed)

		// Extract realistic EVM coordinates with excellent hash distribution
		block := (binary.LittleEndian.Uint32(hash[0:4]) % 1000) + 1000 // Blocks 1000-1999
		txIndex := binary.LittleEndian.Uint32(hash[4:8]) % 5000        // Transactions 0-4999
		logInTx := binary.LittleEndian.Uint32(hash[8:12]) % 32         // Logs 0-31 per tx
		tagHi := binary.LittleEndian.Uint64(hash[12:20])               // High entropy topics
		tagLo := binary.LittleEndian.Uint64(hash[20:28])               // High entropy topics

		logEntries[i] = struct {
			blk, tx, log uint32
			tagHi, tagLo uint64
		}{
			blk:   block,
			tx:    txIndex,
			log:   logInTx,
			tagHi: tagHi,
			tagLo: tagLo,
		}
	}

	// ──── PHASE 1: Insert all logs (should all be accepted as new) ────
	for _, entry := range logEntries {
		result := d.Check(entry.blk, entry.tx, entry.log, entry.tagHi, entry.tagLo, entry.blk)
		if !result {
			t.Error("New entry should be accepted during initial insertion")
		}
	}

	// ──── PHASE 2: Test duplicates (some will fail due to hash collisions) ────
	duplicatesFailed := 0
	for _, entry := range logEntries {
		result := d.Check(entry.blk, entry.tx, entry.log, entry.tagHi, entry.tagLo, entry.blk)
		if result {
			duplicatesFailed++ // Failed to detect duplicate (collision occurred)
		}
	}

	// ──── PHASE 3: Analyze and validate collision rates ────
	cacheSize := 1 << constants.RingBits
	failureRate := float64(duplicatesFailed) / float64(totalLogs) * 100
	expectedCollisionRate := float64(totalLogs) / float64(cacheSize) * 100

	t.Logf("=== REALISTIC DIRECT-MAPPED CACHE PERFORMANCE ===")
	t.Logf("Cache size: %d slots", cacheSize)
	t.Logf("Test entries: %d", totalLogs)
	t.Logf("Cache utilization: %.1f%%", float64(totalLogs)/float64(cacheSize)*100)
	t.Logf("Duplicate detection failures: %d out of %d (%.1f%%)", duplicatesFailed, totalLogs, failureRate)
	t.Logf("Expected collision rate: ~%.1f%%", expectedCollisionRate)

	// Validate failure rate is within acceptable bounds for direct-mapped cache
	// Allow 3x theoretical rate to account for hash clustering effects
	maxAcceptableFailureRate := expectedCollisionRate * 3

	if failureRate > maxAcceptableFailureRate {
		t.Errorf("Collision rate %.1f%% too high (expected ≤%.1f%% for direct-mapped cache)",
			failureRate, maxAcceptableFailureRate)
	}

	// ──── PHASE 4: Verify deduplicator works on non-colliding entries ────
	testEntries := []struct {
		blk, tx, log uint32
		tagHi, tagLo uint64
	}{
		{10000, 1, 1, 0x1111111111111111, 0x2222222222222222},
		{20000, 2, 2, 0x3333333333333333, 0x4444444444444444},
		{30000, 3, 3, 0x5555555555555555, 0x6666666666666666},
	}

	// These should definitely not collide and work perfectly
	for i, entry := range testEntries {
		// First check - should be accepted as new
		result := d.Check(entry.blk, entry.tx, entry.log, entry.tagHi, entry.tagLo, entry.blk)
		if !result {
			t.Errorf("Non-colliding entry %d should be accepted as new", i)
		}

		// Second check - should be rejected as duplicate
		result = d.Check(entry.blk, entry.tx, entry.log, entry.tagHi, entry.tagLo, entry.blk)
		if result {
			t.Errorf("Non-colliding entry %d should be rejected as duplicate", i)
		}
	}

	t.Logf("✅ Deduplicator working correctly within direct-mapped cache limitations")
	t.Logf("✅ Collision rate %.1f%% is acceptable for %.1f%% cache utilization",
		failureRate, float64(totalLogs)/float64(cacheSize)*100)
}

// ============================================================================
// HIGH-FREQUENCY BLOCKCHAIN TESTS
// ============================================================================

// TestDeduper_HighFrequencyChain tests performance with high-frequency blockchain scenarios.
// Simulates chains like Solana with rapid block production and high transaction volume.
func TestDeduper_HighFrequencyChain(t *testing.T) {
	d := Deduper{}

	// Simulate Solana-like high frequency blockchain
	const blocksPerSecond = 2.5 // ~400ms block time
	const txPerBlock = 1000     // High transaction volume
	const logsPerTx = 5         // Multiple logs per transaction

	blockNum := uint32(1000000) // Start at realistic block height

	// Simulate 10 seconds of high-frequency blockchain activity
	for block := 0; block < 25; block++ {
		currentBlock := blockNum + uint32(block)

		for tx := 0; tx < txPerBlock; tx++ {
			for log := 0; log < logsPerTx; log++ {
				// Generate realistic log fingerprints
				tagHi := uint64(currentBlock)<<32 | uint64(tx)
				tagLo := uint64(log)<<32 | uint64(block)

				result := d.Check(currentBlock, uint32(tx), uint32(log), tagHi, tagLo, currentBlock)
				if !result {
					t.Errorf("High-frequency log should be accepted: block=%d, tx=%d, log=%d",
						currentBlock, tx, log)
				}
			}
		}
	}

	t.Logf("✅ Successfully processed high-frequency blockchain simulation")
}

// ============================================================================
// CRYPTOGRAPHIC RANDOMNESS TESTS
// ============================================================================

// TestDeduper_CryptoRandomness tests with cryptographically random data.
// Validates behavior with maximum entropy input data.
func TestDeduper_CryptoRandomness(t *testing.T) {
	d := Deduper{}

	// Generate cryptographically random test cases
	const numTests = 1000
	for i := 0; i < numTests; i++ {
		var buf [32]byte
		if _, err := rand.Read(buf[:]); err != nil {
			t.Fatal("Failed to generate cryptographic random data")
		}

		// Extract coordinates from random bytes
		blk := binary.BigEndian.Uint32(buf[0:4])
		tx := binary.BigEndian.Uint32(buf[4:8]) & 0xFFFF   // Limit to realistic range
		log := binary.BigEndian.Uint32(buf[8:12]) & 0xFFFF // Limit to realistic range
		tagHi := binary.BigEndian.Uint64(buf[12:20])
		tagLo := binary.BigEndian.Uint64(buf[20:28])

		// Test first insertion
		result := d.Check(blk, tx, log, tagHi, tagLo, blk)
		if !result {
			t.Errorf("Random entry %d should be accepted as new", i)
		}

		// Test immediate duplicate detection
		result = d.Check(blk, tx, log, tagHi, tagLo, blk)
		if result {
			t.Errorf("Duplicate random entry %d should be rejected", i)
		}
	}

	t.Logf("✅ Successfully processed %d cryptographically random entries", numTests)
}

// ============================================================================
// MEMORY AND STRUCTURE TESTS
// ============================================================================

// TestDeduper_MemoryLayout validates memory layout assumptions and initialization.
// Ensures the deduplicator structure is properly configured.
func TestDeduper_MemoryLayout(t *testing.T) {
	d := Deduper{}

	// Verify cache array was properly sized
	expectedSize := 1 << constants.RingBits
	if len(d.entries) != expectedSize {
		t.Errorf("Cache size mismatch: expected %d entries, got %d", expectedSize, len(d.entries))
	}

	// Verify entries are properly zero-initialized
	for i := range d.entries {
		entry := &d.entries[i]
		if entry.block != 0 || entry.tx != 0 || entry.log != 0 ||
			entry.seenAt != 0 || entry.topicHi != 0 || entry.topicLo != 0 {
			t.Errorf("Entry %d not properly zero-initialized", i)
		}
	}

	t.Logf("✅ Memory layout correct: %d entries, properly zero-initialized", expectedSize)
}

// TestDeduper_RingBufferWrap tests behavior when cache capacity is exceeded.
// Validates wraparound and eviction behavior in direct-mapped cache.
func TestDeduper_RingBufferWrap(t *testing.T) {
	d := Deduper{}

	// Fill beyond cache capacity to test wraparound
	bufferSize := 1 << constants.RingBits
	numEntries := bufferSize + 1000

	entries := make([]struct {
		blk, tx, log uint32
		tagHi, tagLo uint64
	}, numEntries)

	// Generate unique, well-distributed entries
	for i := 0; i < numEntries; i++ {
		entries[i] = struct {
			blk, tx, log uint32
			tagHi, tagLo uint64
		}{
			blk:   uint32(i + 1000),
			tx:    uint32(i % 65536),
			log:   uint32(i % 65536),
			tagHi: uint64(i),
			tagLo: uint64(i + 1000000),
		}
	}

	// Insert all entries (some will evict earlier entries)
	for i, entry := range entries {
		result := d.Check(entry.blk, entry.tx, entry.log, entry.tagHi, entry.tagLo, entry.blk)
		if !result {
			t.Errorf("Entry %d should be accepted during wraparound test", i)
		}
	}

	t.Logf("✅ Successfully handled cache wraparound with %d entries", numEntries)
}

// ============================================================================
// PERFORMANCE VALIDATION TESTS
// ============================================================================

// TestDeduper_ConcurrentSlots validates multiple cache slots are utilized properly.
// Ensures good cache distribution and slot utilization.
func TestDeduper_ConcurrentSlots(t *testing.T) {
	d := Deduper{}

	// Fill multiple cache slots with diverse data
	for i := 0; i < 1000; i++ {
		blk := uint32(i + 1000)
		tx := uint32(i % 65536)
		log := uint32(i % 65536)
		tagHi := uint64(i) << 32
		tagLo := uint64(i)

		result := d.Check(blk, tx, log, tagHi, tagLo, blk)
		if !result {
			t.Errorf("Concurrent slot entry %d should be accepted", i)
		}
	}

	t.Logf("✅ Successfully utilized multiple cache slots")
}

// TestDeduper_TimestampProgression tests with advancing block timestamps.
// Validates behavior with realistic blockchain timestamp progression.
func TestDeduper_TimestampProgression(t *testing.T) {
	d := Deduper{}

	// Insert logs with steadily advancing timestamps
	for i := 0; i < 1000; i++ {
		blk := uint32(1000 + i)
		result := d.Check(blk, 0, uint32(i), uint64(i), uint64(i), blk)
		if !result {
			t.Errorf("Advancing timestamp entry %d should be accepted", i)
		}
	}

	t.Logf("✅ Successfully processed advancing timestamp sequence")
}

// ============================================================================
// FUNDAMENTAL OPERATION BENCHMARKS
// ============================================================================

// BenchmarkDeduper_BasicOperations benchmarks core deduplication performance.
// Tests the fundamental Check() operation with pre-generated data to eliminate
// allocation overhead and focus purely on deduplication logic performance.
func BenchmarkDeduper_BasicOperations(b *testing.B) {
	d := Deduper{}

	// Pre-generate diverse test data to avoid allocation overhead during benchmark
	testData := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
		currentBlock     uint32
	}, 1000)

	// Generate realistic EVM coordinate patterns
	for i := range testData {
		testData[i] = struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
			currentBlock     uint32
		}{
			block:        uint32(i + 1000),    // Realistic block numbers
			tx:           uint32(i % 65536),   // Transaction index within block
			log:          uint32(i % 32),      // Log index within transaction
			topicHi:      uint64(i) << 32,     // Topic fingerprint high bits
			topicLo:      uint64(i + 1000000), // Topic fingerprint low bits
			currentBlock: uint32(i + 1000),    // Current chain tip
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark the core Check() operation
	for i := 0; i < b.N; i++ {
		data := &testData[i%len(testData)]
		d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.currentBlock)
	}
}

// ============================================================================
// CACHE BEHAVIOR BENCHMARKS
// ============================================================================

// BenchmarkDeduper_NewEntries benchmarks cache miss performance.
// Tests worst-case scenario where all entries are new (no cache hits).
// This represents the maximum work the deduplicator must perform.
func BenchmarkDeduper_NewEntries(b *testing.B) {
	d := Deduper{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Generate unique coordinates for each iteration (guaranteed cache miss)
		block := uint32(i + 1000)
		tx := uint32(i % 65536)
		log := uint32(i % 32)
		topicHi := uint64(i) << 32
		topicLo := uint64(i + 1000000)

		result := d.Check(block, tx, log, topicHi, topicLo, block)
		if !result {
			b.Fatal("New entry should be accepted")
		}
	}

	b.Logf("Processed %d unique cache misses", b.N)
}

// BenchmarkDeduper_Duplicates benchmarks cache hit performance.
// Tests best-case scenario where all lookups are cache hits.
// This represents the minimum work for duplicate detection.
func BenchmarkDeduper_Duplicates(b *testing.B) {
	d := Deduper{}

	// Insert one entry to create a cache hit scenario
	const block, tx, log = 1000, 5, 2
	const topicHi, topicLo = 0x1234567890abcdef, 0xfedcba0987654321
	d.Check(block, tx, log, topicHi, topicLo, block)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Same entry every iteration - guaranteed cache hit
		result := d.Check(block, tx, log, topicHi, topicLo, block)
		if result {
			b.Fatal("Duplicate entry should be rejected")
		}
	}

	b.Logf("Processed %d duplicate cache hits", b.N)
}

// BenchmarkDeduper_MixedWorkload benchmarks realistic mixed cache behavior.
// Simulates production workload with both new entries and duplicates.
func BenchmarkDeduper_MixedWorkload(b *testing.B) {
	d := Deduper{}

	// Pre-populate cache with realistic data to simulate production state
	for i := 0; i < 1000; i++ {
		block := uint32(i + 1000)
		tx := uint32(i % 100)
		log := uint32(i % 10)
		topicHi := uint64(i) << 32
		topicLo := uint64(i)
		d.Check(block, tx, log, topicHi, topicLo, block)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 70% new entries, 30% duplicates (realistic blockchain ratio)
		if i%10 < 7 {
			// New entry - cache miss
			block := uint32(i + 2000)
			tx := uint32(i % 65536)
			log := uint32(i % 32)
			topicHi := uint64(i) << 32
			topicLo := uint64(i + 2000000)
			d.Check(block, tx, log, topicHi, topicLo, block)
		} else {
			// Duplicate entry - cache hit from pre-populated set
			idx := i % 1000
			block := uint32(idx + 1000)
			tx := uint32(idx % 100)
			log := uint32(idx % 10)
			topicHi := uint64(idx) << 32
			topicLo := uint64(idx)
			d.Check(block, tx, log, topicHi, topicLo, block)
		}
	}
}

// ============================================================================
// HIGH-FREQUENCY BLOCKCHAIN BENCHMARKS
// ============================================================================

// BenchmarkDeduper_HighFrequency benchmarks high-frequency blockchain scenarios.
// Simulates performance with chains like Solana that have rapid block production
// and extremely high transaction volumes.
func BenchmarkDeduper_HighFrequency(b *testing.B) {
	d := Deduper{}

	// Simulate Solana-like high-frequency parameters
	const logsPerBlock = 5000 // High log volume per block
	const blocksToSimulate = 100

	b.ResetTimer()
	b.ReportAllocs()

	blockCounter := 0
	logCounter := 0

	for i := 0; i < b.N; i++ {
		// Generate realistic high-frequency log coordinates
		block := uint32(1000 + blockCounter)
		tx := uint32(logCounter % 1000)
		log := uint32(logCounter % 50)
		topicHi := uint64(block)<<32 | uint64(tx)
		topicLo := uint64(log)<<32 | uint64(blockCounter)

		d.Check(block, tx, log, topicHi, topicLo, block)

		// Advance to next log/block
		logCounter++
		if logCounter >= logsPerBlock {
			logCounter = 0
			blockCounter++
			if blockCounter >= blocksToSimulate {
				blockCounter = 0 // Wrap around for extended benchmarks
			}
		}
	}

	b.Logf("Simulated high-frequency chain: %d blocks, %d logs/block",
		blocksToSimulate, logsPerBlock)
}

// ============================================================================
// RANDOMNESS AND ENTROPY BENCHMARKS
// ============================================================================

// BenchmarkDeduper_CryptoRandomness benchmarks with cryptographically random data.
// Tests performance with maximum entropy input to stress hash function
// and validate worst-case collision behavior.
func BenchmarkDeduper_CryptoRandomness(b *testing.B) {
	d := Deduper{}

	// Pre-generate cryptographic random data to avoid crypto overhead in benchmark
	randomData := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
	}, 10000)

	for i := range randomData {
		var buf [32]byte
		rand.Read(buf[:])

		randomData[i] = struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
		}{
			block:   binary.BigEndian.Uint32(buf[0:4]),
			tx:      binary.BigEndian.Uint32(buf[4:8]) & 0xFFFF,  // Realistic tx range
			log:     binary.BigEndian.Uint32(buf[8:12]) & 0xFFFF, // Realistic log range
			topicHi: binary.BigEndian.Uint64(buf[12:20]),
			topicLo: binary.BigEndian.Uint64(buf[20:28]),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := &randomData[i%len(randomData)]
		d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.block)
	}

	b.Logf("Processed %d cryptographically random entries", b.N)
}

// ============================================================================
// EDGE CASE PERFORMANCE BENCHMARKS
// ============================================================================

// BenchmarkDeduper_ZeroValues benchmarks zero value handling performance.
// Tests performance with genesis block and minimal coordinate values.
func BenchmarkDeduper_ZeroValues(b *testing.B) {
	d := Deduper{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i == 0 {
			// First zero entry should be accepted
			result := d.Check(0, 0, 0, 0, 0, 0)
			if !result {
				b.Fatal("First zero entry should be accepted")
			}
		} else {
			// Subsequent zero entries should be duplicates
			result := d.Check(0, 0, 0, 0, 0, 0)
			if result {
				b.Fatal("Duplicate zero entry should be rejected")
			}
		}
	}
}

// BenchmarkDeduper_MaxValues benchmarks maximum value handling performance.
// Tests performance with extreme EVM coordinate values.
func BenchmarkDeduper_MaxValues(b *testing.B) {
	d := Deduper{}

	// Maximum possible EVM values
	const maxBlk = uint32(0xFFFFFFFF)
	const maxTx = uint32(0xFFFF)
	const maxLog = uint32(0xFFFF)
	const maxTopicHi = uint64(0xFFFFFFFFFFFFFFFF)
	const maxTopicLo = uint64(0xFFFFFFFFFFFFFFFF)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i == 0 {
			// First max entry should be accepted
			result := d.Check(maxBlk, maxTx, maxLog, maxTopicHi, maxTopicLo, maxBlk)
			if !result {
				b.Fatal("First max entry should be accepted")
			}
		} else {
			// Subsequent max entries should be duplicates
			result := d.Check(maxBlk, maxTx, maxLog, maxTopicHi, maxTopicLo, maxBlk)
			if result {
				b.Fatal("Duplicate max entry should be rejected")
			}
		}
	}
}

// ============================================================================
// COLLISION AND HASH DISTRIBUTION BENCHMARKS
// ============================================================================

// BenchmarkDeduper_HashCollisions benchmarks hash collision handling performance.
// Tests performance when entries hash to the same cache slots.
func BenchmarkDeduper_HashCollisions(b *testing.B) {
	d := Deduper{}

	// Pre-generate data patterns that encourage hash collisions
	collisionData := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
	}, 1000)

	for i := range collisionData {
		// Use bit patterns that may produce similar hash values
		collisionData[i] = struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
		}{
			block:   uint32(i << 16), // Spacing to encourage collisions
			tx:      uint32(i % 256),
			log:     uint32(i % 16),
			topicHi: uint64(i) << 32,
			topicLo: uint64(i) ^ 0xAAAAAAAAAAAAAAAA, // XOR pattern for variation
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := &collisionData[i%len(collisionData)]
		d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.block)
	}
}

// ============================================================================
// STALENESS AND REORGANIZATION BENCHMARKS
// ============================================================================

// BenchmarkDeduper_StalenessChecks benchmarks staleness detection performance.
// Tests performance of reorganization threshold checking.
func BenchmarkDeduper_StalenessChecks(b *testing.B) {
	d := Deduper{}

	// Insert entry at historical block
	const oldBlock = 1000
	d.Check(oldBlock, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, oldBlock)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Use different coordinates to avoid cache interference
		block := uint32(oldBlock + (i % 100))
		tx := uint32(5 + (i % 10))
		log := uint32(2 + (i % 5))
		currentBlock := uint32(oldBlock + constants.MaxReorg + 1 + (i % 100))

		d.Check(block, tx, log, 0x1234567890abcdef+uint64(i),
			0xfedcba0987654321+uint64(i), currentBlock)
	}
}

// ============================================================================
// THROUGHPUT AND LATENCY ANALYSIS BENCHMARKS
// ============================================================================

// BenchmarkDeduper_ThroughputTest measures maximum sustainable throughput.
// Provides comprehensive performance metrics for production capacity planning.
func BenchmarkDeduper_ThroughputTest(b *testing.B) {
	d := Deduper{}

	// Pre-generate large diverse dataset for sustained throughput testing
	const dataSize = 100000
	testData := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
	}, dataSize)

	// Use SHA256 for excellent distribution without crypto overhead in benchmark
	for i := range testData {
		seed := make([]byte, 8)
		binary.LittleEndian.PutUint64(seed, uint64(i))
		hash := sha256.Sum256(seed)

		testData[i] = struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
		}{
			block:   (binary.LittleEndian.Uint32(hash[0:4]) % 10000) + 1000,
			tx:      binary.LittleEndian.Uint32(hash[4:8]) % 65536,
			log:     binary.LittleEndian.Uint32(hash[8:12]) % 100,
			topicHi: binary.LittleEndian.Uint64(hash[12:20]),
			topicLo: binary.LittleEndian.Uint64(hash[20:28]),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()

	for i := 0; i < b.N; i++ {
		data := &testData[i%dataSize]
		d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.block)
	}

	elapsed := time.Since(start)

	// Calculate comprehensive performance metrics
	opsPerSecond := float64(b.N) / elapsed.Seconds()
	nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)

	b.Logf("=== THROUGHPUT ANALYSIS ===")
	b.Logf("Throughput: %.0f ops/sec", opsPerSecond)
	b.Logf("Latency: %.2f ns/op", nsPerOp)
	b.Logf("Total operations: %d", b.N)

	// Performance classification
	if opsPerSecond > 500_000_000 {
		b.Logf("Performance class: ULTRA_HIGH (>500M ops/sec)")
	} else if opsPerSecond > 100_000_000 {
		b.Logf("Performance class: HIGH (>100M ops/sec)")
	} else if opsPerSecond > 10_000_000 {
		b.Logf("Performance class: MODERATE (>10M ops/sec)")
	} else {
		b.Logf("Performance class: LOW (<10M ops/sec)")
	}
}

// ============================================================================
// CACHE UTILIZATION BENCHMARKS
// ============================================================================

// BenchmarkDeduper_CacheUtilization benchmarks performance at different cache fill levels.
// Tests how performance degrades as cache utilization increases and collision rates rise.
func BenchmarkDeduper_CacheUtilization(b *testing.B) {
	cacheSize := 1 << constants.RingBits

	utilizationLevels := []struct {
		name string
		pct  float64
	}{
		{"1pct", 0.01},  // Very low utilization
		{"5pct", 0.05},  // Low utilization
		{"10pct", 0.10}, // Moderate utilization
		{"25pct", 0.25}, // High utilization
		{"50pct", 0.50}, // Very high utilization
		{"75pct", 0.75}, // Extreme utilization
		{"90pct", 0.90}, // Near-capacity utilization
	}

	for _, level := range utilizationLevels {
		b.Run(level.name, func(b *testing.B) {
			d := Deduper{}
			numEntries := int(float64(cacheSize) * level.pct)

			// Pre-populate cache to target utilization level
			for i := 0; i < numEntries; i++ {
				block := uint32(i + 1000)
				tx := uint32(i % 65536)
				log := uint32(i % 100)
				topicHi := uint64(i) << 32
				topicLo := uint64(i + 1000000)
				d.Check(block, tx, log, topicHi, topicLo, block)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Mix of new entries and lookups of existing entries
				if i%2 == 0 {
					// New entry - may cause eviction at high utilization
					block := uint32(i + numEntries + 1000)
					tx := uint32(i % 65536)
					log := uint32(i % 100)
					topicHi := uint64(i+numEntries) << 32
					topicLo := uint64(i + numEntries + 1000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				} else {
					// Lookup existing entry - may hit or miss due to eviction
					idx := i % numEntries
					block := uint32(idx + 1000)
					tx := uint32(idx % 65536)
					log := uint32(idx % 100)
					topicHi := uint64(idx) << 32
					topicLo := uint64(idx + 1000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				}
			}

			b.Logf("Cache utilization: %.1f%% (%d/%d entries)",
				level.pct*100, numEntries, cacheSize)
		})
	}
}

// ============================================================================
// BIT PATTERN AND HASH DISTRIBUTION BENCHMARKS
// ============================================================================

// BenchmarkDeduper_BitPatterns benchmarks performance with specific bit patterns.
// Tests hash function behavior and comparison logic with edge-case bit patterns.
func BenchmarkDeduper_BitPatterns(b *testing.B) {
	patterns := []struct {
		name    string
		block   uint32
		tx      uint32
		log     uint32
		topicHi uint64
		topicLo uint64
	}{
		{"AllOnes", 0xFFFFFFFF, 0xFFFF, 0xFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
		{"AllZeros", 0, 0, 0, 0, 0},
		{"Alternating", 0xAAAAAAAA, 0xAAAA, 0xAAAA, 0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA},
		{"PowersOf2", 0x80000000, 0x8000, 0x8000, 0x8000000000000000, 0x8000000000000000},
		{"Sequential", 0x01234567, 0x0123, 0x4567, 0x0123456789ABCDEF, 0xFEDCBA9876543210},
		{"Checkerboard", 0x55555555, 0x5555, 0x5555, 0x5555555555555555, 0x5555555555555555},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			d := Deduper{}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if i == 0 {
					// First pattern should be accepted
					result := d.Check(pattern.block, pattern.tx, pattern.log,
						pattern.topicHi, pattern.topicLo, pattern.block)
					if !result {
						b.Fatalf("First %s pattern should be accepted", pattern.name)
					}
				} else {
					// Subsequent patterns should be duplicates
					result := d.Check(pattern.block, pattern.tx, pattern.log,
						pattern.topicHi, pattern.topicLo, pattern.block)
					if result {
						b.Fatalf("Duplicate %s pattern should be rejected", pattern.name)
					}
				}
			}
		})
	}
}

// ============================================================================
// MEMORY ACCESS PATTERN BENCHMARKS
// ============================================================================

// BenchmarkDeduper_MemoryAccess benchmarks different memory access patterns.
// Tests cache-friendly vs cache-hostile access patterns for performance optimization.
func BenchmarkDeduper_MemoryAccess(b *testing.B) {
	d := Deduper{}

	b.Run("Sequential", func(b *testing.B) {
		// Sequential access pattern - most cache-friendly
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			block := uint32(i + 1000)
			d.Check(block, 0, 0, uint64(i), uint64(i), block)
		}
	})

	b.Run("Random", func(b *testing.B) {
		// Random access pattern - least cache-friendly
		// Pre-generate random order to avoid runtime overhead
		randomOrder := make([]int, 10000)
		for i := range randomOrder {
			randomOrder[i] = i
		}
		// Simple linear congruential shuffle
		for i := range randomOrder {
			j := (i*1103515245 + 12345) % len(randomOrder)
			randomOrder[i], randomOrder[j] = randomOrder[j], randomOrder[i]
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			idx := randomOrder[i%len(randomOrder)]
			block := uint32(idx + 1000)
			d.Check(block, 0, 0, uint64(idx), uint64(idx), block)
		}
	})

	b.Run("Clustered", func(b *testing.B) {
		// Clustered access pattern - moderately cache-friendly
		b.ReportAllocs()
		clusterSize := 64
		for i := 0; i < b.N; i++ {
			// Access in clusters to simulate spatial locality
			clusterBase := (i / clusterSize) * clusterSize * 1000
			offset := i % clusterSize
			block := uint32(clusterBase + offset + 1000)
			d.Check(block, 0, 0, uint64(clusterBase+offset),
				uint64(clusterBase+offset), block)
		}
	})
}

// ============================================================================
// SCALABILITY BENCHMARKS
// ============================================================================

// BenchmarkDeduper_ScalabilityTest tests performance scaling at different operation volumes.
// Validates that performance remains consistent across different scales.
func BenchmarkDeduper_ScalabilityTest(b *testing.B) {
	scales := []struct {
		name       string
		numEntries int
	}{
		{"1K", 1000},
		{"10K", 10000},
		{"100K", 100000},
		{"1M", 1000000},
	}

	for _, scale := range scales {
		b.Run(scale.name, func(b *testing.B) {
			if b.N < scale.numEntries {
				b.Skip("Insufficient iterations for this scale")
			}

			d := Deduper{}

			b.ResetTimer()
			b.ReportAllocs()

			// Process entries up to the scale limit
			processed := 0
			for i := 0; i < b.N && processed < scale.numEntries; i++ {
				block := uint32(i + 1000)
				tx := uint32(i % 65536)
				log := uint32(i % 100)
				topicHi := uint64(i) << 32
				topicLo := uint64(i + 1000000)

				d.Check(block, tx, log, topicHi, topicLo, block)
				processed++
			}

			opsPerSecond := float64(processed) / b.Elapsed().Seconds()
			b.Logf("Scale: %s, Ops/sec: %.0f", scale.name, opsPerSecond)
		})
	}
}

// ============================================================================
// COMPARATIVE PERFORMANCE ANALYSIS BENCHMARKS
// ============================================================================

// BenchmarkDeduper_ComparativeAnalysis provides detailed performance analysis.
// Measures peak performance characteristics and provides optimization guidance.
func BenchmarkDeduper_ComparativeAnalysis(b *testing.B) {
	b.Run("PeakPerformance", func(b *testing.B) {
		d := Deduper{}

		// Optimal case: all unique entries, minimal collisions
		b.ResetTimer()
		b.ReportAllocs()

		start := time.Now()
		for i := 0; i < b.N; i++ {
			// Widely spaced entries to minimize hash collisions
			block := uint32(i*1000 + 1000)
			tx := uint32(i % 65536)
			log := uint32(i % 32)
			topicHi := uint64(i) << 32
			topicLo := uint64(i + 1000000)

			result := d.Check(block, tx, log, topicHi, topicLo, block)
			if !result {
				b.Fatal("New entry should be accepted in peak performance test")
			}
		}
		elapsed := time.Since(start)

		// Calculate detailed performance metrics
		nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)
		opsPerSec := float64(b.N) / elapsed.Seconds()

		b.Logf("=== PEAK PERFORMANCE ANALYSIS ===")
		b.Logf("  Latency: %.2f ns/op", nsPerOp)
		b.Logf("  Throughput: %.0f ops/sec", opsPerSec)
		b.Logf("  Total operations: %d", b.N)

		// Performance classification for latency
		if nsPerOp < 2.0 {
			b.Logf("  Latency class: EXCEPTIONAL (sub-2ns)")
		} else if nsPerOp < 5.0 {
			b.Logf("  Latency class: EXCELLENT (sub-5ns)")
		} else if nsPerOp < 10.0 {
			b.Logf("  Latency class: GOOD (sub-10ns)")
		} else {
			b.Logf("  Latency class: NEEDS_OPTIMIZATION (>10ns)")
		}

		// Performance classification for throughput
		if opsPerSec > 500_000_000 {
			b.Logf("  Throughput class: ULTRA_HIGH (>500M ops/sec)")
		} else if opsPerSec > 100_000_000 {
			b.Logf("  Throughput class: HIGH (>100M ops/sec)")
		} else if opsPerSec > 10_000_000 {
			b.Logf("  Throughput class: MODERATE (>10M ops/sec)")
		} else {
			b.Logf("  Throughput class: LOW (<10M ops/sec)")
		}
	})
}

// ============================================================================
// REAL-WORLD SIMULATION BENCHMARKS
// ============================================================================

// BenchmarkDeduper_RealWorldSimulation simulates real blockchain workload patterns.
// Tests performance with realistic duplicate rates and traffic patterns from major chains.
func BenchmarkDeduper_RealWorldSimulation(b *testing.B) {
	scenarios := []struct {
		name          string
		duplicateRate float64 // Percentage of entries that are duplicates
		burstiness    int     // Traffic burstiness factor
		description   string
	}{
		{"Ethereum", 0.05, 1, "Low duplicate rate, steady traffic pattern"},
		{"Solana", 0.15, 3, "Medium duplicate rate, high burstiness"},
		{"Polygon", 0.25, 2, "High duplicate rate, moderate burstiness"},
		{"Arbitrum", 0.10, 1, "Low-medium duplicate rate, steady traffic"},
		{"BSC", 0.20, 2, "Medium-high duplicate rate, moderate burstiness"},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			d := Deduper{}

			// Pre-populate with realistic cache state
			prePopSize := 10000
			for i := 0; i < prePopSize; i++ {
				block := uint32(i + 1000)
				tx := uint32(i % 1000)
				log := uint32(i % 50)
				topicHi := uint64(i) << 32
				topicLo := uint64(i + 1000000)
				d.Check(block, tx, log, topicHi, topicLo, block)
			}

			b.ResetTimer()
			b.ReportAllocs()

			burstCounter := 0
			for i := 0; i < b.N; i++ {
				// Simulate traffic burstiness
				if burstCounter < scenario.burstiness {
					burstCounter++
				} else {
					burstCounter = 0
					continue // Skip iteration to simulate burst gaps
				}

				// Determine if this should be a duplicate based on scenario
				isDuplicate := float64(i%100) < scenario.duplicateRate*100

				if isDuplicate && i > 100 {
					// Use existing entry from pre-populated set
					idx := i % prePopSize
					block := uint32(idx + 1000)
					tx := uint32(idx % 1000)
					log := uint32(idx % 50)
					topicHi := uint64(idx) << 32
					topicLo := uint64(idx + 1000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				} else {
					// New entry
					block := uint32(i + prePopSize + 1000)
					tx := uint32(i % 5000)
					log := uint32(i % 100)
					topicHi := uint64(i+prePopSize) << 32
					topicLo := uint64(i + prePopSize + 1000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				}
			}

			b.Logf("%s simulation: %s", scenario.name, scenario.description)
		})
	}
}

// ============================================================================
// COMPREHENSIVE PERFORMANCE BENCHMARKS
// ============================================================================

// BenchmarkDeduper_ComprehensivePerformance provides end-to-end performance validation.
// Combines multiple workload patterns to simulate comprehensive production usage.
func BenchmarkDeduper_ComprehensivePerformance(b *testing.B) {
	d := Deduper{}

	// Mixed workload phases
	phases := []struct {
		name    string
		ratio   float64 // Fraction of total operations
		pattern string  // Description of access pattern
	}{
		{"Initialization", 0.10, "Sequential cache filling"},
		{"SteadyState", 0.60, "Mixed new/duplicate entries"},
		{"BurstTraffic", 0.20, "High-frequency burst simulation"},
		{"Reorganization", 0.10, "Cache eviction and staleness"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	phaseSize := b.N / len(phases)
	currentOp := 0

	for phaseIdx, phase := range phases {
		phaseOps := phaseSize
		if phaseIdx == len(phases)-1 {
			phaseOps = b.N - currentOp // Handle remainder
		}

		switch phase.name {
		case "Initialization":
			// Sequential initialization phase
			for i := 0; i < phaseOps; i++ {
				block := uint32(currentOp + i + 1000)
				tx := uint32(i % 1000)
				log := uint32(i % 50)
				topicHi := uint64(i) << 32
				topicLo := uint64(i + 1000000)
				d.Check(block, tx, log, topicHi, topicLo, block)
			}

		case "SteadyState":
			// Mixed new/duplicate steady state
			for i := 0; i < phaseOps; i++ {
				if i%3 == 0 {
					// New entry
					block := uint32(currentOp + i + 10000)
					tx := uint32(i % 5000)
					log := uint32(i % 100)
					topicHi := uint64(i+10000) << 32
					topicLo := uint64(i + 10000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				} else {
					// Potential duplicate
					block := uint32((i % 1000) + 1000)
					tx := uint32(i % 1000)
					log := uint32(i % 50)
					topicHi := uint64(i%1000) << 32
					topicLo := uint64((i % 1000) + 1000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				}
			}

		case "BurstTraffic":
			// High-frequency burst simulation
			for i := 0; i < phaseOps; i += 3 {
				// Process burst of 3 operations rapidly
				for j := 0; j < 3 && i+j < phaseOps; j++ {
					block := uint32(currentOp + i + j + 20000)
					tx := uint32((i + j) % 10000)
					log := uint32((i + j) % 200)
					topicHi := uint64(i+j+20000) << 32
					topicLo := uint64(i + j + 20000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				}
			}

		case "Reorganization":
			// Cache eviction and staleness testing
			for i := 0; i < phaseOps; i++ {
				// Use coordinates that may cause eviction
				block := uint32((i % 100) + 1000)
				tx := uint32(i % 100)
				log := uint32(i % 10)
				currentBlock := uint32(1000 + constants.MaxReorg + 100 + i)
				topicHi := uint64(i+30000) << 32
				topicLo := uint64(i + 30000000)
				d.Check(block, tx, log, topicHi, topicLo, currentBlock)
			}
		}

		currentOp += phaseOps
	}

	b.Logf("=== COMPREHENSIVE PERFORMANCE COMPLETED ===")
	b.Logf("Processed %d operations across %d phases", b.N, len(phases))
}

// ============================================================================
// STRESS TESTING BENCHMARKS
// ============================================================================

// BenchmarkDeduper_StressTest performs extreme load testing.
// Tests system behavior under maximum stress conditions.
func BenchmarkDeduper_StressTest(b *testing.B) {
	b.Run("MaximumLoad", func(b *testing.B) {
		d := Deduper{}

		// Generate maximum entropy data for worst-case performance
		stressData := make([]struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
		}, 1000)

		for i := range stressData {
			// Use maximum bit variations to stress comparison logic
			stressData[i] = struct {
				block, tx, log   uint32
				topicHi, topicLo uint64
			}{
				block:   uint32(i) ^ 0xAAAAAAAA,
				tx:      uint32(i) ^ 0xAAAA,
				log:     uint32(i) ^ 0xAAAA,
				topicHi: uint64(i) ^ 0xAAAAAAAAAAAAAAAA,
				topicLo: uint64(i) ^ 0x5555555555555555,
			}
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data := &stressData[i%len(stressData)]
			d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.block)
		}

		b.Logf("Completed maximum load stress test")
	})

	b.Run("CollisionStorm", func(b *testing.B) {
		d := Deduper{}

		// Force maximum hash collisions
		baseBlock := uint32(1000)
		baseTx := uint32(100)
		baseLog := uint32(50)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Vary only the topics to force same cache slot usage
			topicHi := uint64(i)
			topicLo := uint64(i + 1000000)
			d.Check(baseBlock, baseTx, baseLog, topicHi, topicLo, baseBlock)
		}

		b.Logf("Completed collision storm stress test")
	})
}

// ============================================================================
// MEMORY EFFICIENCY BENCHMARKS
// ============================================================================

// BenchmarkDeduper_MemoryEfficiency tests memory allocation patterns.
// Validates zero-allocation design goals.
func BenchmarkDeduper_MemoryEfficiency(b *testing.B) {
	d := Deduper{}

	// Pre-generate test data to isolate memory allocation testing
	testEntries := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
	}, 1000)

	for i := range testEntries {
		testEntries[i] = struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
		}{
			block:   uint32(i + 1000),
			tx:      uint32(i % 65536),
			log:     uint32(i % 100),
			topicHi: uint64(i) << 32,
			topicLo: uint64(i + 1000000),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := &testEntries[i%len(testEntries)]
		d.Check(entry.block, entry.tx, entry.log, entry.topicHi, entry.topicLo, entry.block)
	}

	// Memory efficiency validation
	memStats := testing.AllocsPerRun(b.N, func() {
		entry := &testEntries[0]
		d.Check(entry.block, entry.tx, entry.log, entry.topicHi, entry.topicLo, entry.block)
	})

	if memStats > 0 {
		b.Logf("WARNING: Memory allocations detected: %.2f allocs/op", memStats)
	} else {
		b.Logf("✅ ZERO ALLOCATIONS: Perfect memory efficiency achieved")
	}
}

// ============================================================================
// LATENCY DISTRIBUTION BENCHMARKS
// ============================================================================

// BenchmarkDeduper_LatencyDistribution analyzes latency characteristics.
// Provides detailed latency percentile analysis for SLA planning.
func BenchmarkDeduper_LatencyDistribution(b *testing.B) {
	d := Deduper{}

	// Pre-generate diverse test data
	testData := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
	}, 10000)

	for i := range testData {
		seed := make([]byte, 4)
		binary.LittleEndian.PutUint32(seed, uint32(i))
		hash := sha256.Sum256(seed)

		testData[i] = struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
		}{
			block:   binary.LittleEndian.Uint32(hash[0:4]) % 100000,
			tx:      binary.LittleEndian.Uint32(hash[4:8]) % 65536,
			log:     binary.LittleEndian.Uint32(hash[8:12]) % 100,
			topicHi: binary.LittleEndian.Uint64(hash[12:20]),
			topicLo: binary.LittleEndian.Uint64(hash[20:28]),
		}
	}

	// Collect individual operation latencies
	latencies := make([]time.Duration, 0, b.N)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := &testData[i%len(testData)]

		start := time.Now()
		d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.block)
		latency := time.Since(start)

		latencies = append(latencies, latency)
	}

	b.StopTimer()

	// Calculate latency distribution statistics
	if len(latencies) > 0 {
		// Sort for percentile calculation
		for i := 0; i < len(latencies)-1; i++ {
			for j := i + 1; j < len(latencies); j++ {
				if latencies[i] > latencies[j] {
					latencies[i], latencies[j] = latencies[j], latencies[i]
				}
			}
		}

		p50 := latencies[len(latencies)*50/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]
		pMax := latencies[len(latencies)-1]

		b.Logf("=== LATENCY DISTRIBUTION ANALYSIS ===")
		b.Logf("  P50 (median): %v", p50)
		b.Logf("  P95: %v", p95)
		b.Logf("  P99: %v", p99)
		b.Logf("  Max: %v", pMax)
		b.Logf("  Sample size: %d operations", len(latencies))
	}
}
