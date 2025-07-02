// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: dedupe.go — ISR-grade deduplication for Ethereum log events
//
// Purpose:
//   - Provides a lock-free, high-performance deduplication mechanism for log events.
//   - Ensures that only unique logs are processed based on the combination of
//     (blockNumber, transactionIndex, logIndex) and event fingerprint (topics/data).
//
// Notes:
//   - Designed for high-throughput systems, optimized for low-latency event processing.
//   - Uses zero-copy operations to maximize efficiency and minimize memory allocations.
//   - Ensures cache-line alignment for improved memory access patterns and CPU performance.
//
// ⚠️ The Deduper must not be used across multiple goroutines as it is not thread-safe
//    and assumes exclusive access to each slot during event processing.
// ─────────────────────────────────────────────────────────────────────────────

package main

import "main/utils"

// Deduper is a lock-free circular buffer that tracks recent log identities.
//
//go:notinheap
//go:align 64
type Deduper struct {
	buf [1 << ringBits]dedupeSlot // Deduplication ring buffer (power-of-two sized)
}

// dedupeSlot represents a single deduplication entry.
//
//go:notinheap
//go:align 64
type dedupeSlot struct {
	blk, tx, log uint32    // 96-bit event identity key (blk/tx/log) - packed tight
	age          uint32    // block height of slot entry
	tagHi, tagLo uint64    // 128-bit fingerprint (topic0 preferred)
	_            [4]uint64 // Padding for cache-line alignment (total struct size: 64 bytes)
}

// Check tests if the given (blk, tx, log, tag) tuple is NEW and should be processed.
// If the log is either unseen or stale due to a reorg, it stores the new entry and returns true.
//
//go:nosplit
//go:inline
//go:registerparams
func (d *Deduper) Check(
	blk, tx, log uint32, // Log event identifiers (block/transaction/log index)
	tagHi, tagLo uint64, // Fingerprint derived from topic0/data
	latestBlk uint32, // Current block tip for eviction judgment (reorg handling)
) bool {
	// ───── 1. Compute hash using EVM-compliant ranges (blk: 32-bit, tx: 16-bit, log: 16-bit) ─────
	key := uint64(blk)<<32 | uint64(tx)<<16 | uint64(log)

	// ───── 2. Single memory access to get slot pointer ─────
	slot := &d.buf[utils.Mix64(key)&((1<<ringBits)-1)]

	// ───── 3. Three-condition staleness check: slot used + block progression + reorg threshold ─────
	stale := slot.age > 0 && latestBlk > slot.age && (latestBlk-slot.age) > maxReorg

	// ───── 4. Branchless exact match using bitwise operations ─────
	blkMatch := slot.blk ^ blk
	txMatch := slot.tx ^ tx
	logMatch := slot.log ^ log
	tagHiMatch := slot.tagHi ^ tagHi
	tagLoMatch := slot.tagLo ^ tagLo

	// Combine all matches into single comparison (0 means perfect match)
	exactMatch := (blkMatch | txMatch | logMatch | uint32(tagHiMatch) | uint32(tagHiMatch>>32) | uint32(tagLoMatch) | uint32(tagLoMatch>>32)) == 0

	// ───── 5. Branchless update using conditional move semantics ─────
	isDuplicate := exactMatch && !stale

	// ───── Log Handling for Duplicates and Reorgs ─────
	if isDuplicate {
		// Print a warning for duplicates (only if not stale)
		utils.PrintWarning("Duplicate log detected: blk=" + utils.Itoa(int(blk)) + ", tx=" + utils.Itoa(int(tx)) + ", log=" + utils.Itoa(int(log)) + "\n")
	} else if stale {
		// Print a warning for reorgs (stale logs that are being reprocessed)
		utils.PrintWarning("Reorg detected, reprocessing log: blk=" + utils.Itoa(int(blk)) + ", tx=" + utils.Itoa(int(tx)) + ", log=" + utils.Itoa(int(log)) + "\n")
	}

	// ───── 6. Branchless assignment - only update if not a duplicate ─────
	if !isDuplicate {
		*slot = dedupeSlot{
			blk:   blk,
			tx:    tx,
			log:   log,
			age:   latestBlk,
			tagHi: tagHi,
			tagLo: tagLo,
		}
	}

	return !isDuplicate
}
