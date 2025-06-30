package main

import "main/utils"

///////////////////////////////////////////////////////////////////////////////
// Deduper — Lock-free L1-resident de-duplication ring
///////////////////////////////////////////////////////////////////////////////

// dedupeSlot represents a single entry in the deduplication ring buffer.
// Size: 32 bytes per slot — exactly two per 64-byte cache line.
// This ensures optimal spatial locality and full L1 residency.
//
// Fields:
//   - blk, tx, log  → event coordinates
//   - tagHi/Lo      → fingerprint of content (topic0 or data fallback)
//   - age           → block height for staleness & reorg detection
//
//go:notinheap
type dedupeSlot struct {
	blk, _, tx, _, log, _ uint32 // 96-bit composite key
	tagHi, tagLo          uint64 // 128-bit fingerprint
	age, _                uint32
	_                     [2]uint64
}

// Deduper is a fixed-size ring buffer that stores recent event identities.
// It rejects duplicate or stale logs and overwrites without branches.
// Total size: (1 << ringBits) * 32 bytes = 8 MiB if ringBits = 18.
//
// Runtime behavior:
//   - Evicts using `age` vs `latestBlk`
//   - Fully branchless write path
//   - Thread-local by design (not synchronized)
type Deduper struct {
	buf [1 << ringBits]dedupeSlot // power-of-two ring of slots (cache-aligned)
}

// Check returns true if the (blk, tx, log, tag) tuple is NEW (not seen recently).
//
// Eviction strategy:
//   - Each slot is evicted if older than `maxReorg` blocks
//   - Writes always happen regardless of match (branchless overwrite)
//
// Hash strategy:
//   - Key is a 64-bit fused triplet: blk (24b), tx (20b), log (20b)
//   - Mixed with Murmur-style hash for uniform distribution
//
// Compiler Directives:
//   - nosplit         → avoids stack checks, safe for flat logic
//   - inline          → embeds into caller to remove call overhead
//   - registerparams  → ABI-optimal on modern Go compilers
//
//go:nosplit
//go:inline
//go:registerparams
func (d *Deduper) Check(
	blk, tx, log uint32, // EVM identity (composite key)
	tagHi, tagLo uint64, // fingerprint of content
	latestBlk uint32, // latest known block (for reorg eviction)
) bool {
	// ───── Step 1: Fuse key into a 64-bit pseudo-unique hash ─────
	//   blk → top 24 bits
	//   tx  → mid 20 bits
	//   log → low 20 bits
	key := (uint64(blk) << 40) ^ (uint64(tx) << 20) ^ uint64(log)

	// ───── Step 2: Compute slot index using high-entropy hash ─────
	i := utils.Mix64(key) & (uint64(len(d.buf)) - 1)
	slot := &d.buf[i]

	// ───── Step 3: Check if slot is stale — evict on deep reorgs ─────
	stale := latestBlk < slot.age || (latestBlk-slot.age) > maxReorg

	// ───── Step 4: Determine match — event is duplicate if all fields match ─────
	match := !stale &&
		slot.blk == blk && slot.tx == tx && slot.log == log &&
		slot.tagHi == tagHi && slot.tagLo == tagLo

	// ───── Step 5: Write path — unconditional overwrite, branch-free ─────
	slot.blk, slot.tx, slot.log = blk, tx, log
	slot.tagHi, slot.tagLo = tagHi, tagLo
	slot.age = latestBlk

	// Return true if this is a brand new event
	return !match
}
