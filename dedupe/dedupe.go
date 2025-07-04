package dedupe

import (
	"main/constants"
	"main/utils"
)

// dedupeEntry represents a single cache line optimized deduplication entry
// Exactly 32 bytes for optimal cache alignment and minimal memory waste
//
//go:notinheap
//go:align 32
type dedupeEntry struct {
	block   uint32 // Block number of the log
	tx      uint32 // Transaction index within block
	log     uint32 // Log index within transaction
	seenAt  uint32 // Block number when this log was first seen
	topicHi uint64 // Upper 64 bits of topic0 (for collision resistance)
	topicLo uint64 // Lower 64 bits of topic0 (for collision resistance)
}

// Deduper is a direct-mapped cache for log deduplication
// Uses power-of-2 sizing for efficient bit masking
//
//go:notinheap
//go:align 64
type Deduper struct {
	entries [1 << constants.RingBits]dedupeEntry // Direct-mapped cache array
	_       [8]uint64                            // Padding to 64-byte boundary
}

// NewDeduper creates a properly initialized deduplicator
// Pre-fills with dummy values that will never match real logs
func NewDeduper() *Deduper {
	var d Deduper

	// Fill with dummy values that ensure no false matches
	// Use unique dummy values per slot to prevent any chance of collision
	for i := range d.entries {
		d.entries[i] = dedupeEntry{
			block:   0xFFFFFFFE,                     // Impossible block number
			tx:      uint32(i) | 0x80000000,         // Unique per slot, high bit set
			log:     uint32(i) | 0x80000000,         // Unique per slot, high bit set
			seenAt:  0,                              // Very old (will always be stale)
			topicHi: uint64(i) | 0x8000000000000000, // Unique per slot
			topicLo: uint64(i) | 0x8000000000000000, // Unique per slot
		}
	}

	return &d
}

// Check returns true if this log should be processed (not a recent duplicate)
// Uses direct array indexing for O(1) cache-friendly lookup
//
//go:nosplit
//go:inline
//go:registerparams
func (d *Deduper) Check(
	block, tx, log uint32, // EVM log coordinates
	topicHi, topicLo uint64, // Topic0 hash for collision resistance
	currentBlock uint32, // Current chain tip for staleness detection
) bool {
	// ──── 1. Compute direct array index using fast bit operations ────
	key := uint64(block)<<32 | uint64(tx)<<16 | uint64(log)
	index := utils.Mix64(key) & ((1 << constants.RingBits) - 1)
	entry := &d.entries[index]

	// ──── 2. Fast exact match check using XOR operations ────
	blockMatch := entry.block ^ block
	txMatch := entry.tx ^ tx
	logMatch := entry.log ^ log
	topicHiMatch := entry.topicHi ^ topicHi
	topicLoMatch := entry.topicLo ^ topicLo

	exactMatch := (blockMatch|txMatch|logMatch) == 0 && (topicHiMatch|topicLoMatch) == 0

	// ──── 3. Staleness check for reorg handling ────
	isStale := currentBlock > entry.seenAt && (currentBlock-entry.seenAt) > constants.MaxReorg

	// ──── 4. Duplicate detection logic ────
	if exactMatch && !isStale {
		return false // Recent duplicate found
	}

	// ──── 5. Update cache entry with new log ────
	*entry = dedupeEntry{
		block:   block,
		tx:      tx,
		log:     log,
		seenAt:  currentBlock,
		topicHi: topicHi,
		topicLo: topicLo,
	}

	return true
}
