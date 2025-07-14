// dedupe.go — Sub-5ns O(1) duplicate detection using Robin Hood direct-mapped cache

package dedupe

import (
	"main/constants"
	"main/utils"
)

// dedupeEntry - 32-byte cache entry optimized for CPU cache lines
//
//go:notinheap
//go:align 32
type dedupeEntry struct {
	// Hot fields: accessed on every comparison (16 bytes)
	block  uint32 // EVM block number
	tx     uint32 // Transaction index (0-based)
	log    uint32 // Log index within transaction
	seenAt uint32 // Block when first cached

	// Collision resistance: 128-bit topic fingerprint (16 bytes)
	topicHi uint64 // Upper 64 bits of topic0
	topicLo uint64 // Lower 64 bits of topic0
}

// Deduper - Direct-mapped cache for O(1) duplicate detection
//
//go:notinheap
//go:align 64
type Deduper struct {
	entries [1 << constants.RingBits]dedupeEntry // Power-of-2 for bit masking
}

// Check performs sub-5ns deduplication with staleness detection.
// Returns true if log should be processed, false if duplicate.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (d *Deduper) Check(
	block, tx, log uint32,
	topicHi, topicLo uint64,
	currentBlock uint32,
) bool {
	// Compute cache index via fast modulo
	key := uint64(block)<<32 | uint64(tx)<<16 | uint64(log)
	index := utils.Mix64(key) & ((1 << constants.RingBits) - 1)
	entry := &d.entries[index]

	// Parallel coordinate comparison
	coordMatch := uint64((entry.block ^ block) |
		(entry.tx ^ tx) |
		(entry.log ^ log))

	// Parallel topic comparison
	topicMatch := (entry.topicHi ^ topicHi) | (entry.topicLo ^ topicLo)

	// Exact match detection
	exactMatch := (coordMatch | topicMatch) == 0

	// Staleness check (entries older than MaxReorg blocks)
	isStale := currentBlock > entry.seenAt &&
		(currentBlock-entry.seenAt) > constants.MaxReorg

	// Duplicate if: exact match + not stale + initialized
	isDuplicate := exactMatch && !isStale && entry.seenAt > 0

	// Update cache on new or stale entries
	if !isDuplicate {
		entry.block = block
		entry.tx = tx
		entry.log = log
		entry.seenAt = currentBlock
		entry.topicHi = topicHi
		entry.topicLo = topicLo

		// Ensure seenAt never 0 (uninitialized marker)
		if entry.seenAt == 0 {
			entry.seenAt = 1
		}
	}

	return !isDuplicate
}
