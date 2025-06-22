package main

// dedupe.go — branch-free ring that lives entirely in L1 cache.
// Each slot is exactly 32 bytes => half a cache-line (avoids false sharing).

type dedupeSlot struct {
	blk, tx, log uint32 // 96-bit location key
	tagHi, tagLo uint64 // 128-bit payload fingerprint
	age          uint32 // last-seen block (for re-org eviction)
	//lint:ignore U1000 keep struct 32 B-aligned
	_pad uint32
}

type Deduper struct {
	buf [1 << ringBits]dedupeSlot
}

// Check returns true ⇢ event (blk,tx,log,tag) is **new** under current tip.
func (d *Deduper) Check(
	blk, tx, log uint32,
	tagHi, tagLo uint64,
	latestBlk uint32,
) bool {
	key := (uint64(blk) << 40) ^ (uint64(tx) << 20) ^ uint64(log)
	i := mix64(key) & (uint64(len(d.buf)) - 1)
	slot := &d.buf[i]

	stale := (latestBlk - slot.age) > maxReorg
	match := !stale &&
		slot.blk == blk && slot.tx == tx && slot.log == log &&
		slot.tagHi == tagHi && slot.tagLo == tagLo

	// unconditional overwrite keeps inner loop branch-free
	slot.blk, slot.tx, slot.log = blk, tx, log
	slot.tagHi, slot.tagLo = tagHi, tagLo
	slot.age = latestBlk

	return !match
}
