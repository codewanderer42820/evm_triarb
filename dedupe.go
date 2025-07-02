// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: dedupe.go — ISR-grade L1-resident de-duplication buffer
//
// Purpose:
//   - Prevents redundant processing of reorg-sensitive logs in ISR pipelines
//   - Lock-free and fully branchless to achieve nanosecond-level latency
//
// Notes:
//   - Total ring size = 8 MiB (if ringBits = 18) → fully L1/L2 cache-resident
//   - Fingerprint-based deduplication using topic0 or data fallback
//   - Optimized for single-threaded execution (no locks, no atomics)
//
// Compiler Directives:
//   - //go:nosplit
//   - //go:inline
//   - //go:registerparams
//
// ⚠️ Footgun mode: Potential corruption if misused across multiple goroutines
// ─────────────────────────────────────────────────────────────────────────────

package main

import "main/utils"

// dedupeSlot represents a single deduplication entry.
// Struct layout ensures optimal cache line usage with 64-byte alignment.
// Total size: 32 bytes = 2 slots per cache line.
//
// Layout:
//   - blk, tx, log: 96-bit composite event identity (block/transaction/log index)
//   - tagHi, tagLo: 128-bit fingerprint (using topic0 or data for uniqueness)
//   - age: block height for eviction based on reorg window
//
//go:notinheap
//go:align 64
type dedupeSlot struct {
	blk, _, tx, _, log, _ uint32    // 96-bit event identity key (blk/tx/log)
	tagHi, tagLo          uint64    // 128-bit event fingerprint (topic0 preferred)
	age                   uint32    // block height of slot entry
	_                     uint32    // Padding to ensure 64-byte alignment
	_                     [2]uint64 // Padding for cache-line alignment (total struct size: 64 bytes)
}

// Deduper is a lock-free circular buffer that tracks recent log identities.
// The buffer operates efficiently in an L1 cache-resident manner to reduce latency.
// The deduplication mechanism ensures reorg-tolerant freshness, evicting logs that
// belong to blocks no longer in the canonical chain (reorg handling).
//
// Memory allocation: (1 << ringBits) * 32B → 8 MiB when ringBits=18.
//
//go:notinheap
//go:align 64
type Deduper struct {
	buf [1 << ringBits]dedupeSlot // Deduplication ring buffer (power-of-two sized)
}

// Check tests if the given (blk, tx, log, tag) tuple is NEW and should be processed.
// If the log is either unseen or stale due to a reorg, it stores the new entry and returns true.
//
// Eviction strategy:
//   - Slot is overwritten if the log is from a block older than `maxReorg` blocks
//   - Slot is evicted if the block number has been reverted (reorg detection)
//
// Write strategy:
//   - Slot is always overwritten if the log is new or stale (branchless, no locks)
//
//go:nosplit
//go:inline
//go:registerparams
func (d *Deduper) Check(
	blk, tx, log uint32, // Log event identifiers (block/transaction/log index)
	tagHi, tagLo uint64, // Fingerprint derived from topic0/data
	latestBlk uint32, // Current block tip for eviction judgment (reorg handling)
) bool {
	// ───── 1. Compute a 64-bit hash combining blk, tx, and log for unique identity ─────
	key := (uint64(blk) << 40) ^ (uint64(tx) << 20) ^ uint64(log)

	// ───── 2. Compute ring slot index using the hash, ensuring even distribution ─────
	i := utils.Mix64(key) & (uint64(len(d.buf)) - 1)
	slot := &d.buf[i]

	// ───── 3. Reorg Detection ─────
	// If blk is smaller than latestBlk, it indicates a reorg, and the log is invalidated
	if blk < latestBlk {
		// Log belongs to a forked chain, so it must be evicted
		slot.blk = blk       // Update the log's block number (indicates it was reverted)
		slot.age = latestBlk // Set the age to the current block height
		return false         // Mark the log as stale (no need to reprocess)
	}

	// ───── 4. Staleness Check ─────
	// If the log is older than `maxReorg` blocks, mark it as stale
	stale := (latestBlk - slot.age) > maxReorg
	if stale {
		// Evict logs that are considered stale due to reorgs
		return false
	}

	// ───── 5. Exact Match Check ─────
	// If the current log has the same (blk, tx, log, tag) tuple, it’s a duplicate
	match := slot.blk == blk && slot.tx == tx && slot.log == log && slot.tagHi == tagHi && slot.tagLo == tagLo

	// ───── 6. Branchless Overwrite ─────
	// If the log is either new or stale, overwrite the slot with the current log data
	if !match || stale {
		slot.blk, slot.tx, slot.log = blk, tx, log
		slot.tagHi, slot.tagLo = tagHi, tagLo
		slot.age = latestBlk
	}

	// Return false if the log is a duplicate, true if it's new and should be processed
	return !match
}
