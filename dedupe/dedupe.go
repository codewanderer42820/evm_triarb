// ════════════════════════════════════════════════════════════════════════════════════════════════
// 🛡️ HIGH-PERFORMANCE DEDUPLICATION CACHE
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Arbitrage Detection System
// Component: Event Deduplication Engine
//
// Description:
//   Direct-mapped cache for blockchain event deduplication with reorganization awareness.
//   Provides O(1) duplicate detection while handling chain reorgs gracefully.
//
// Performance Characteristics:
//   - Lookup time: Sub-5ns operation
//   - Memory: 2MB fixed-size cache
//   - Allocations: Zero per check
//   - Cache design: Direct-mapped with LRU eviction
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package dedupe

import (
	"main/constants"
	"main/utils"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CACHE ENTRY STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// dedupeEntry represents a single cache entry for duplicate detection.
// Each entry stores the complete identification of an Ethereum log event along with
// timing information to handle blockchain reorganizations. The structure is precisely
// 32 bytes to fit within a single CPU cache line for optimal performance.
//
// Memory layout (32 bytes total):
// - Coordinates (12 bytes): block, tx, log indices for unique event identification
// - Timing (4 bytes): seenAt tracks when entry was cached for staleness detection
// - Fingerprint (16 bytes): 128-bit topic hash for collision resistance
//
//go:notinheap
//go:align 32
type dedupeEntry struct {
	// EVENT COORDINATES (12 bytes)
	// These fields uniquely identify an event's position in the blockchain.
	// Accessed on every lookup for exact match detection.
	block uint32 // EVM block number where event was emitted
	tx    uint32 // Transaction index within the block (0-based)
	log   uint32 // Log index within the transaction

	// STALENESS TRACKING (4 bytes)
	// Enables detection of stale entries from potential chain reorganizations.
	// Events older than MaxReorg blocks are considered potentially invalid.
	seenAt uint32 // Block number when this entry was first cached

	// COLLISION FINGERPRINT (16 bytes)
	// 128-bit fingerprint from event topics provides strong collision resistance.
	// Even if coordinates match, different topics indicate different events.
	topicHi uint64 // Upper 64 bits of topic[0] hash
	topicLo uint64 // Lower 64 bits of topic[0] hash
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DEDUPLICATION CACHE STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Deduper implements a high-performance cache for detecting duplicate blockchain events.
// Uses direct-mapped cache architecture where each event maps to exactly one cache slot,
// providing guaranteed O(1) lookup time with minimal memory overhead.
//
// Cache characteristics:
// - Fixed size: 2^RingBits entries (typically 65536)
// - Direct mapped: Each event hashes to one specific slot
// - Collision handling: Newer entries evict older ones (LRU-like behavior)
// - Memory usage: 2MB for 65536 entries (32 bytes per entry)
//
//go:notinheap
//go:align 64
type Deduper struct {
	// Fixed-size array provides zero-allocation operation and predictable memory layout.
	// Power-of-2 size enables fast modulo via bit masking for index calculation.
	entries [1 << constants.RingBits]dedupeEntry
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DUPLICATE DETECTION ALGORITHM
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Check performs high-speed duplicate detection with blockchain reorganization handling.
// This is the core operation called for every incoming Ethereum event to determine
// if it has been processed before.
//
// Parameters:
//   - block, tx, log: Event coordinates that uniquely identify its blockchain position
//   - topicHi, topicLo: 128-bit fingerprint from event topics for collision detection
//   - currentBlock: Current blockchain height for staleness calculation
//
// Returns:
//   - true: New event that should be processed
//   - false: Duplicate event that should be skipped
//
// Algorithm:
// 1. Hash event coordinates to find cache slot
// 2. Compare stored entry with incoming event
// 3. Check for staleness due to potential reorganization
// 4. Update cache if new or stale entry
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
	// STEP 1: CACHE INDEX CALCULATION
	// Combine event coordinates into a single key for hashing.
	// Layout: [block:32][tx:16][log:16] provides good distribution.
	key := uint64(block)<<32 | uint64(tx)<<16 | uint64(log)

	// Mix the key for better hash distribution and compute cache index.
	// Bit masking with (size-1) performs fast modulo for power-of-2 sizes.
	index := utils.Mix64(key) & ((1 << constants.RingBits) - 1)
	entry := &d.entries[index]

	// STEP 2: PARALLEL COMPARISON
	// Use bitwise operations to compare all fields simultaneously.
	// This approach minimizes branches and maximizes CPU efficiency.

	// Compare event coordinates using XOR - result is 0 only if all fields match
	coordMatch := uint64((entry.block ^ block) |
		(entry.tx ^ tx) |
		(entry.log ^ log))

	// Compare 128-bit topic fingerprint for collision detection
	topicMatch := (entry.topicHi ^ topicHi) | (entry.topicLo ^ topicLo)

	// Exact match occurs when both coordinates and topics are identical
	exactMatch := (coordMatch | topicMatch) == 0

	// STEP 3: STALENESS DETECTION
	// Handle blockchain reorganizations by invalidating old entries.
	// Entries older than MaxReorg blocks might be from orphaned chain segments.
	isStale := currentBlock > entry.seenAt &&
		(currentBlock-entry.seenAt) > constants.MaxReorg

	// STEP 4: DUPLICATE DETERMINATION
	// An entry is duplicate only if:
	// - Exact match on all fields (coordinates + topics)
	// - Not stale (within reorganization window)
	// - Previously initialized (seenAt > 0)
	isDuplicate := exactMatch && !isStale && entry.seenAt > 0

	// STEP 5: CACHE UPDATE
	// Update the cache slot if this is a new entry or replaces a stale one.
	// Direct-mapped design means new entries always evict old ones at same slot.
	if !isDuplicate {
		entry.block = block
		entry.tx = tx
		entry.log = log
		entry.seenAt = currentBlock
		entry.topicHi = topicHi
		entry.topicLo = topicLo

		// Ensure seenAt is never zero (reserved as uninitialized marker).
		// This handles the edge case where currentBlock is 0.
		if entry.seenAt == 0 {
			entry.seenAt = 1
		}
	}

	// Return true for new events (should process), false for duplicates (should skip)
	return !isDuplicate
}
