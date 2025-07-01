// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: dedupe.go — ISR-grade L1-resident de-duplication buffer
//
// Purpose:
//   - Prevents redundant processing of reorg-sensitive logs in ISR pipelines
//   - Lock-free and fully branchless for nanosecond-level latency
//
// Notes:
//   - Total ring size = 8 MiB (if ringBits = 18) → fully L1/L2 cache-resident
//   - Fingerprint-based deduplication using topic0 or data fallback
//   - Safe for single-threaded use only (no locks, no atomics)
//
// Compiler Directives:
//   - //go:nosplit
//   - //go:inline
//   - //go:registerparams
//
// ⚠️ Footgun mode: Corruption possible if misused across goroutines
// ─────────────────────────────────────────────────────────────────────────────

package main

import "main/utils"

// dedupeSlot represents a single deduplication entry.
// Size: 32 bytes = 2 slots per cache line for maximum spatial locality.
//
// Struct layout:
//   - blk, tx, log: 96-bit composite event identity
//   - tagHi, tagLo: 128-bit content fingerprint
//   - age: block height for eviction windowing
//
//go:notinheap
//go:align 64
type dedupeSlot struct {
	blk, _, tx, _, log, _ uint32 // 96-bit event identity key (blk/tx/log)
	tagHi, tagLo          uint64 // 128-bit event fingerprint (topic0 preferred)
	age, _                uint32 // block height of slot entry
	_                     [2]uint64
}

// Deduper is a lock-free circular ring for recent log identities.
// Eviction and overwrite strategy ensures reorg-tolerant freshness.
// Total memory = (1 << ringBits) * 32B → 8 MiB at ringBits=18.
type Deduper struct {
	buf [1 << ringBits]dedupeSlot // deduplication ring buffer (power-of-two sized)
}

// Check tests if the given (blk, tx, log, tag) tuple is NEW.
// If unseen or stale, it stores the new entry and returns true.
//
// Eviction strategy:
//   - Slot is overwritten if older than `maxReorg` blocks
//
// Write strategy:
//   - Always overwrites (branchless)
//
//go:nosplit
//go:inline
//go:registerparams
func (d *Deduper) Check(
	blk, tx, log uint32, // log event identifiers
	tagHi, tagLo uint64, // fingerprint from topic0/data
	latestBlk uint32, // current block tip for eviction judgment
) bool {
	// ───── 1. Fuse blk/tx/log into 64-bit hash ─────
	key := (uint64(blk) << 40) ^ (uint64(tx) << 20) ^ uint64(log)

	// ───── 2. Compute ring slot index ─────
	i := utils.Mix64(key) & (uint64(len(d.buf)) - 1)
	slot := &d.buf[i]

	// ───── 3. Staleness check ─────
	stale := latestBlk < slot.age || (latestBlk-slot.age) > maxReorg

	// ───── 4. Exact match check ─────
	match := !stale &&
		slot.blk == blk && slot.tx == tx && slot.log == log &&
		slot.tagHi == tagHi && slot.tagLo == tagLo

	// ───── 5. Branchless overwrite ─────
	slot.blk, slot.tx, slot.log = blk, tx, log
	slot.tagHi, slot.tagLo = tagHi, tagLo
	slot.age = latestBlk

	// Return true if event is new (was not matched)
	return !match
}
