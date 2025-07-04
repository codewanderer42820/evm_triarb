package dedupe

import (
	"crypto/rand"
	"encoding/binary"
	"main/constants"
	"main/utils"
	"math/big"
	"testing"
)

// randUint32 returns a cryptographically secure random uint32 between 0 and max (exclusive).
func randUint32(max uint32) (uint32, error) {
	// Generate a random big.Int between 0 and max (exclusive)
	randBig, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0, err
	}
	return uint32(randBig.Int64()), nil
}

// randUint64 returns a cryptographically secure random uint64.
func randUint64() (uint64, error) {
	// Generate 8 random bytes and convert to uint64
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

// TestDeduper_Basic tests basic deduplication functionality
func TestDeduper_Basic(t *testing.T) {
	d := NewDeduper()

	// Test first insertion - should return true (new)
	result := d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)
	if !result {
		t.Error("First insertion should return true")
	}

	// Test duplicate - should return false
	result = d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)
	if result {
		t.Errorf("Duplicate should return false, but got true")
	}
}

// TestDeduper_DifferentFingerprints tests logs with same coordinates but different fingerprints
func TestDeduper_DifferentFingerprints(t *testing.T) {
	d := NewDeduper()

	// Same block/tx/log but different fingerprint - should both be accepted
	result1 := d.Check(1000, 5, 2, 0x1111111111111111, 0x2222222222222222, 1000)
	result2 := d.Check(1000, 5, 2, 0x3333333333333333, 0x4444444444444444, 1000)

	if !result1 || !result2 {
		t.Error("Different fingerprints should both be accepted")
	}
}

// TestDeduper_ReorgHandling tests reorganization scenarios
func TestDeduper_ReorgHandling(t *testing.T) {
	d := NewDeduper()

	// Insert log at block 1000
	d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)

	// Advance beyond reorg threshold
	latestBlk := uint32(1000 + constants.MaxReorg + 1)

	// Same log should now be accepted due to staleness
	result := d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, latestBlk)
	if !result {
		t.Error("Stale log should be accepted after reorg threshold")
	}
}

// TestDeduper_EdgeCaseReorg tests edge case where we're exactly at reorg threshold
func TestDeduper_EdgeCaseReorg(t *testing.T) {
	d := NewDeduper()

	// Insert log at block 1000
	d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, 1000)

	// Advance to exactly reorg threshold (should still be duplicate)
	latestBlk := uint32(1000 + constants.MaxReorg)
	result := d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, latestBlk)
	if result {
		t.Error("Log at exactly reorg threshold should still be duplicate")
	}

	// One block beyond threshold (should be accepted)
	latestBlk = uint32(1000 + constants.MaxReorg + 1)
	result = d.Check(1000, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, latestBlk)
	if !result {
		t.Error("Log beyond reorg threshold should be accepted")
	}
}

// TestDeduper_MaxValues tests extreme EVM values
func TestDeduper_MaxValues(t *testing.T) {
	d := NewDeduper()

	// Test with maximum possible values
	maxBlk := uint32(0xFFFFFFFF) // 2^32 - 1
	maxTx := uint32(0xFFFF)      // 2^16 - 1
	maxLog := uint32(0xFFFF)     // 2^16 - 1
	maxTagHi := uint64(0xFFFFFFFFFFFFFFFF)
	maxTagLo := uint64(0xFFFFFFFFFFFFFFFF)

	result := d.Check(maxBlk, maxTx, maxLog, maxTagHi, maxTagLo, maxBlk)
	if !result {
		t.Error("Max values should be accepted")
	}

	// Test duplicate with max values
	result = d.Check(maxBlk, maxTx, maxLog, maxTagHi, maxTagLo, maxBlk)
	if result {
		t.Error("Duplicate with max values should be rejected")
	}
}

// TestDeduper_ZeroValues tests zero values
func TestDeduper_ZeroValues(t *testing.T) {
	d := NewDeduper()

	result := d.Check(0, 0, 0, 0, 0, 0)
	if !result {
		t.Error("Zero values should be accepted")
	}

	result = d.Check(0, 0, 0, 0, 0, 0)
	if result {
		t.Error("Duplicate zero values should be rejected")
	}
}

// TestDeduper_HashCollisions tests potential hash collisions
func TestDeduper_HashCollisions(t *testing.T) {
	d := NewDeduper()

	// Generate keys that might hash to same slot
	keys := make([]uint64, 100)
	for i := range keys {
		keys[i] = uint64(i) << 32 // Different blocks, same tx/log
	}

	// Insert all keys
	for _, key := range keys {
		blk := uint32(key >> 32)
		result := d.Check(blk, 0, 0, uint64(blk), uint64(blk), blk)
		if !result {
			t.Errorf("Key %d should be accepted", key)
		}
	}
}

// TestDeduper_ConcurrentSlots tests multiple slots usage
func TestDeduper_ConcurrentSlots(t *testing.T) {
	d := NewDeduper()

	// Fill multiple slots with different data
	for i := 0; i < 1000; i++ {
		blk := uint32(i + 1000)
		tx := uint32(i % 65536)
		log := uint32(i % 65536)
		tagHi := uint64(i) << 32
		tagLo := uint64(i)

		result := d.Check(blk, tx, log, tagHi, tagLo, blk)
		if !result {
			t.Errorf("Slot %d should be accepted", i)
		}
	}
}

// TestDeduper_PartialMatches tests partial matching scenarios
func TestDeduper_PartialMatches(t *testing.T) {
	baseBlk, baseTx, baseLog := uint32(1000), uint32(5), uint32(2)
	baseTagHi, baseTagLo := uint64(0x1234567890abcdef), uint64(0xfedcba0987654321)

	// Test different combinations that should NOT match
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
			// Create fresh deduplicator for each test case to avoid interference
			d := NewDeduper()

			// Insert base log
			d.Check(baseBlk, baseTx, baseLog, baseTagHi, baseTagLo, baseBlk)

			// Test the specific case
			result := d.Check(tc.blk, tc.tx, tc.log, tc.tagHi, tc.tagLo, baseBlk)
			if result != tc.should {
				t.Errorf("Test %s: expected %v, got %v", tc.name, tc.should, result)
			}
		})
	}
}

// TestDeduper_BitPatterns tests specific bit patterns that might cause issues
func TestDeduper_BitPatterns(t *testing.T) {
	d := NewDeduper()

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
		{"Alternating", 0xAAAAAAAA, 0xAAAA, 0xAAAA, 0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA},
		{"Checkerboard", 0x55555555, 0x5555, 0x5555, 0x5555555555555555, 0x5555555555555555},
		{"Powers of 2", 0x80000000, 0x8000, 0x8000, 0x8000000000000000, 0x8000000000000000},
	}

	for _, p := range patterns {
		t.Run(p.name, func(t *testing.T) {
			result := d.Check(p.blk, p.tx, p.log, p.tagHi, p.tagLo, p.blk)
			if !result {
				t.Errorf("Pattern %s should be accepted", p.name)
			}

			// Test duplicate
			result = d.Check(p.blk, p.tx, p.log, p.tagHi, p.tagLo, p.blk)
			if result {
				t.Errorf("Duplicate pattern %s should be rejected", p.name)
			}
		})
	}
}

// TestDeduper_LargeDataset tests with large amounts of data
func TestDeduper_LargeDataset(t *testing.T) {
	d := NewDeduper()

	// Test immediate duplicates (these should always be caught)
	const numTests = 1000
	for i := 0; i < numTests; i++ {
		blk, _ := randUint32(1000000)
		tx, _ := randUint32(65536)
		log, _ := randUint32(65536)
		tagHi, _ := randUint64()
		tagLo, _ := randUint64()

		// First insertion should succeed
		result1 := d.Check(blk, tx, log, tagHi, tagLo, blk)
		if !result1 {
			t.Errorf("First insertion %d should succeed", i)
		}

		// Immediate duplicate should be rejected
		result2 := d.Check(blk, tx, log, tagHi, tagLo, blk)
		if result2 {
			t.Errorf("Immediate duplicate %d should be rejected", i)
		}
	}
}

// TestDeduper_RingBufferWrap tests ring buffer wraparound behavior
func TestDeduper_RingBufferWrap(t *testing.T) {
	d := NewDeduper()

	// Fill more than ring buffer capacity
	bufferSize := 1 << constants.RingBits
	numEntries := bufferSize + 1000

	entries := make([]struct {
		blk, tx, log uint32
		tagHi, tagLo uint64
	}, numEntries)

	// Generate unique entries
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

	// Insert all entries
	for i, entry := range entries {
		result := d.Check(entry.blk, entry.tx, entry.log, entry.tagHi, entry.tagLo, entry.blk)
		if !result {
			t.Errorf("Entry %d should be accepted", i)
		}
	}
}

// TestDeduper_TimestampProgression tests with advancing timestamps
func TestDeduper_TimestampProgression(t *testing.T) {
	d := NewDeduper()

	// Insert logs with advancing timestamps
	for i := 0; i < 1000; i++ {
		blk := uint32(1000 + i)
		result := d.Check(blk, 0, uint32(i), uint64(i), uint64(i), blk)
		if !result {
			t.Errorf("Advancing timestamp %d should be accepted", i)
		}
	}
}

// TestDeduper_ExtremeBitFlips tests single bit differences
func TestDeduper_ExtremeBitFlips(t *testing.T) {
	d := NewDeduper()

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

	// Insert base
	d.Check(base.blk, base.tx, base.log, base.tagHi, base.tagLo, base.blk)

	// Test single bit flips in each field
	for bit := 0; bit < 32; bit++ {
		// Flip bit in blk
		flippedBlk := base.blk ^ (1 << bit)
		result := d.Check(flippedBlk, base.tx, base.log, base.tagHi, base.tagLo, flippedBlk)
		if !result {
			t.Errorf("Single bit flip in blk at position %d should be accepted", bit)
		}
	}

	for bit := 0; bit < 16; bit++ {
		// Flip bit in tx
		flippedTx := base.tx ^ (1 << bit)
		result := d.Check(base.blk, flippedTx, base.log, base.tagHi, base.tagLo, base.blk)
		if !result {
			t.Errorf("Single bit flip in tx at position %d should be accepted", bit)
		}

		// Flip bit in log
		flippedLog := base.log ^ (1 << bit)
		result = d.Check(base.blk, base.tx, flippedLog, base.tagHi, base.tagLo, base.blk)
		if !result {
			t.Errorf("Single bit flip in log at position %d should be accepted", bit)
		}
	}

	for bit := 0; bit < 64; bit++ {
		// Flip bit in tagHi
		flippedTagHi := base.tagHi ^ (1 << bit)
		result := d.Check(base.blk, base.tx, base.log, flippedTagHi, base.tagLo, base.blk)
		if !result {
			t.Errorf("Single bit flip in tagHi at position %d should be accepted", bit)
		}

		// Flip bit in tagLo
		flippedTagLo := base.tagLo ^ (1 << bit)
		result = d.Check(base.blk, base.tx, base.log, base.tagHi, flippedTagLo, base.blk)
		if !result {
			t.Errorf("Single bit flip in tagLo at position %d should be accepted", bit)
		}
	}
}

// TestDeduper_MemoryLayout tests memory layout assumptions
func TestDeduper_MemoryLayout(t *testing.T) {
	d := NewDeduper()

	// Verify the deduper was properly initialized
	if len(d.entries) != (1 << constants.RingBits) {
		t.Errorf("Buffer size mismatch: expected %d, got %d", 1<<constants.RingBits, len(d.entries))
	}

	// Test that slots are properly initialized with dummy values
	for i := range d.entries {
		entry := &d.entries[i]
		if entry.block != 0xFFFFFFFE || entry.seenAt != 0 {
			t.Errorf("Entry %d not properly initialized with dummy values", i)
		}
	}
}

// TestDeduper_CryptoRandomness tests with cryptographically random data
func TestDeduper_CryptoRandomness(t *testing.T) {
	d := NewDeduper()

	// Generate cryptographically random test data
	const numTests = 1000
	for i := 0; i < numTests; i++ {
		var buf [32]byte
		if _, err := rand.Read(buf[:]); err != nil {
			t.Fatal("Failed to generate random data")
		}

		blk := binary.BigEndian.Uint32(buf[0:4])
		tx := binary.BigEndian.Uint32(buf[4:8]) & 0xFFFF
		log := binary.BigEndian.Uint32(buf[8:12]) & 0xFFFF
		tagHi := binary.BigEndian.Uint64(buf[12:20])
		tagLo := binary.BigEndian.Uint64(buf[20:28])

		result := d.Check(blk, tx, log, tagHi, tagLo, blk)
		if !result {
			t.Errorf("Random entry %d should be accepted", i)
		}

		// Test immediate duplicate
		result = d.Check(blk, tx, log, tagHi, tagLo, blk)
		if result {
			t.Errorf("Duplicate random entry %d should be rejected", i)
		}
	}
}

// TestDeduper_HighFrequencyChain tests high-frequency chain scenarios
func TestDeduper_HighFrequencyChain(t *testing.T) {
	d := NewDeduper()

	// Simulate Solana-like high frequency (400ms blocks with many transactions)
	const blocksPerSecond = 2.5 // ~400ms blocks
	const txPerBlock = 1000
	const logsPerTx = 5

	blockNum := uint32(1000000) // Start at high block number

	// Simulate 10 seconds of high-frequency activity
	for block := 0; block < 25; block++ {
		currentBlock := blockNum + uint32(block)

		for tx := 0; tx < txPerBlock; tx++ {
			for log := 0; log < logsPerTx; log++ {
				// Generate realistic log data
				tagHi := uint64(currentBlock)<<32 | uint64(tx)
				tagLo := uint64(log)<<32 | uint64(block)

				result := d.Check(currentBlock, uint32(tx), uint32(log), tagHi, tagLo, currentBlock)
				if !result {
					t.Errorf("High-frequency log should be accepted: block=%d, tx=%d, log=%d", currentBlock, tx, log)
				}
			}
		}
	}
}

// TestDeduper_ReorgScenarios tests various reorganization scenarios
func TestDeduper_ReorgScenarios(t *testing.T) {
	d := NewDeduper()

	// Test deep reorg scenario
	baseBlock := uint32(1000)

	// Insert logs in the original chain
	for i := 0; i < 10; i++ {
		blk := baseBlock + uint32(i)
		d.Check(blk, 0, uint32(i), uint64(i), uint64(i+1000), blk)
	}

	// Simulate reorg: same blocks but different content
	latestBlock := baseBlock + uint32(constants.MaxReorg) + 10

	for i := 0; i < 10; i++ {
		blk := baseBlock + uint32(i)
		// Different fingerprint for same block/tx/log
		result := d.Check(blk, 0, uint32(i), uint64(i+1000), uint64(i+2000), latestBlock)
		if !result {
			t.Errorf("Reorg'd log should be accepted: block=%d, index=%d", blk, i)
		}
	}
}

// TestDeduper_SlotOverwriting tests slot overwriting behavior
func TestDeduper_SlotOverwriting(t *testing.T) {
	d := NewDeduper()

	// Create entries that will hash to the same slot
	// This tests the hash collision handling
	baseKey := uint64(1000)<<32 | uint64(5)<<16 | uint64(2)

	// Insert first entry
	d.Check(1000, 5, 2, 0x1111111111111111, 0x2222222222222222, 1000)

	// Find a key that hashes to the same slot
	mask := uint64((1 << constants.RingBits) - 1)
	targetSlot := utils.Mix64(baseKey) & mask

	// Try to find another key that hashes to the same slot
	for i := uint64(1); i < 10000; i++ {
		testKey := baseKey + (i << 32) // Different block
		if utils.Mix64(testKey)&mask == targetSlot {
			// Found collision - this will overwrite the first entry
			blk := uint32(testKey >> 32)
			result := d.Check(blk, 5, 2, 0x3333333333333333, 0x4444444444444444, blk)
			if !result {
				t.Error("Colliding entry should be accepted")
			}

			// Original entry should now be accepted again (was overwritten)
			result = d.Check(1000, 5, 2, 0x1111111111111111, 0x2222222222222222, 1000)
			if !result {
				t.Error("Original entry should be accepted after collision")
			}
			break
		}
	}
}
