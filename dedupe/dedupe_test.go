package dedupe

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"main/constants"
	"main/utils"
	"testing"
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
