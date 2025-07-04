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

import (
	"main/constants"
	"main/utils"
)

// Deduper is a lock-free circular buffer that tracks recent log identities.
//
//go:notinheap
//go:align 64
type Deduper struct {
	buf [1 << constants.RingBits]dedupeSlot // Deduplication ring buffer (power-of-two sized)
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
//go:inline
//go:registerparams
func (d *Deduper) Check(
	blk, tx, log uint32, // Log event identifiers (block/transaction/log index)
	tagHi, tagLo uint64, // Fingerprint derived from topic0/data
	latestBlk uint32, // Current block tip for eviction judgment (reorg handling)
) bool {
	// ───── 1. Compute a unique key using the EVM-compliant ranges (blk: 32-bit, tx: 16-bit, log: 16-bit) ─────
	// Combining block, transaction, and log index into a single 64-bit key for easy lookups.
	key := uint64(blk)<<32 | uint64(tx)<<16 | uint64(log)

	// ───── 2. Efficiently access the deduplication slot using a single memory access ─────
	// We apply a hash function to the key to locate the appropriate slot in the buffer.
	slot := &d.buf[utils.Mix64(key)&((1<<constants.RingBits)-1)]

	// ───── 3. Check for staleness based on three conditions: slot used, block progression, and reorg threshold ─────
	stale := slot.age > 0 && latestBlk > slot.age && (latestBlk-slot.age) > constants.MaxReorg

	// ───── 4. Branchless exact match using bitwise operations ─────
	// Compare the current log event's identifiers against the stored ones using XOR.
	// If any match fails, the result will be non-zero, indicating no match.
	blkMatch := slot.blk ^ blk
	txMatch := slot.tx ^ tx
	logMatch := slot.log ^ log
	tagHiMatch := slot.tagHi ^ tagHi
	tagLoMatch := slot.tagLo ^ tagLo

	// Combine all matches into a single comparison (0 means perfect match)
	exactMatch := (blkMatch | txMatch | logMatch | uint32(tagHiMatch) | uint32(tagHiMatch>>32) | uint32(tagLoMatch) | uint32(tagLoMatch>>32)) == 0

	// ───── 5. Determine if the log is a duplicate using the result of the exact match and staleness check ─────
	isDuplicate := exactMatch && !stale

	// ───── Log Handling for Duplicates and Reorgs ─────
	if isDuplicate {
		// Print a warning for duplicate logs (only if the log is not stale)
		utils.PrintWarning("Duplicate log detected: blk=" + utils.Itoa(int(blk)) + ", tx=" + utils.Itoa(int(tx)) + ", log=" + utils.Itoa(int(log)) + "\n")
	} else if stale {
		// Print a warning for stale logs being reprocessed due to a reorg
		utils.PrintWarning("Reorg detected, reprocessing log: blk=" + utils.Itoa(int(blk)) + ", tx=" + utils.Itoa(int(tx)) + ", log=" + utils.Itoa(int(log)) + "\n")
	}

	// ───── 6. Update the slot with the new log event if it's not a duplicate ─────
	// Only update the deduplication slot if the log event is not a duplicate.
	// The slot will be replaced with the new log event and its block height.
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

	// Return true if the event is not a duplicate, indicating that it should be processed.
	return !isDuplicate
}
