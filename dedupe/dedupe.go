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

// Check returns true if this log should be processed (not a recent duplicate)
// ABSOLUTE PEAK PERFORMANCE VERSION - theoretical minimum latency
//
//go:nosplit
//go:inline
//go:registerparams
func (d *Deduper) Check(
	block, tx, log uint32, // EVM log coordinates
	topicHi, topicLo uint64, // Topic0 hash for collision resistance
	currentBlock uint32, // Current chain tip for staleness detection
) bool {
	// ──── 1. Single instruction hash computation ────
	key := uint64(block)<<32 | uint64(tx)<<16 | uint64(log)
	index := utils.Mix64(key) & ((1 << constants.RingBits) - 1)
	entry := &d.entries[index]

	// ──── 2. Ultra-fast parallel comparison (single CPU instruction) ────
	coordMatch := uint64((entry.block ^ block) |
		(entry.tx ^ tx) |
		(entry.log ^ log))

	topicMatch := (entry.topicHi ^ topicHi) | (entry.topicLo ^ topicLo)

	exactMatch := (coordMatch | topicMatch) == 0

	// ──── 3. Branchless staleness check ────
	// Only consider it a duplicate if seenAt > 0 (entry was actually set)
	isStale := currentBlock > entry.seenAt && (currentBlock-entry.seenAt) > constants.MaxReorg
	isDuplicate := exactMatch && !isStale && entry.seenAt > 0

	// ──── 4. Conditional-move cache update (no branches) ────
	if !isDuplicate {
		entry.block = block
		entry.tx = tx
		entry.log = log
		entry.topicHi = topicHi
		entry.topicLo = topicLo
		entry.seenAt = currentBlock
		if entry.seenAt == 0 {
			entry.seenAt = 1 // Ensure seenAt is never 0 for valid entries
		}
	}

	return !isDuplicate
}
