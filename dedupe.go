package main

import "main/utils"

///////////////////////////////////////////////////////////////////////////////
// Deduper — Lock-free L1-resident de-duplication ring
///////////////////////////////////////////////////////////////////////////////

// dedupeSlot represents one entry in the deduplication ring buffer.
// Each slot is exactly 32 bytes — fits two per 64-byte cache line.
// Fields are tightly packed to avoid wasted space and false sharing.
type dedupeSlot struct {
	blk, tx, log uint32 // 96-bit composite key (block, tx, log index)
	tagHi, tagLo uint64 // 128-bit fingerprint of content (topic0, data, etc.)
	age          uint32 // block number of last-seen entry, used for eviction

	// padding to enforce 32-byte alignment and cache line pairing
	//lint:ignore U1000 unused but required to avoid misalignment
	_ uint32
}

// Deduper maintains a fixed-size ring for high-speed, branch-free deduplication.
// Used to reject replays or stale logs. Entire structure fits in L1 on modern CPUs.
type Deduper struct {
	buf [1 << ringBits]dedupeSlot // ringBits = 18 ⇒ 256 Ki slots = 8 MiB
}

// Check determines whether an incoming event is new under the current tip block.
// It returns true if the (blk, tx, log, tag) tuple has NOT been seen recently.
//
// Implements eviction via `age`, and always writes unconditionally (branch-free).
//
//go:nosplit
//go:inline
func (d *Deduper) Check(
	blk, tx, log uint32,
	tagHi, tagLo uint64,
	latestBlk uint32,
) bool {
	// Combine blk, tx, log into a compact 64-bit key:
	// blk: top 24 bits, tx: mid 20 bits, log: low 20 bits.
	key := (uint64(blk) << 40) ^ (uint64(tx) << 20) ^ uint64(log)

	// Use high-quality mixer and mask to locate ring index.
	i := utils.Mix64(key) & (uint64(len(d.buf)) - 1)
	slot := &d.buf[i]

	// Check staleness (re-org too deep or age regression).
	stale := latestBlk < slot.age || (latestBlk-slot.age) > maxReorg

	// Match if not stale and key+tag fields match exactly.
	match := !stale &&
		slot.blk == blk && slot.tx == tx && slot.log == log &&
		slot.tagHi == tagHi && slot.tagLo == tagLo

	// Overwrite slot unconditionally — removes need for branches.
	slot.blk, slot.tx, slot.log = blk, tx, log
	slot.tagHi, slot.tagLo = tagHi, tagLo
	slot.age = latestBlk

	return !match // true if this event is NEW
}
