// router_lookup_zeroalloc.go — ZERO‑alloc, ZERO‑copy, 64‑bit equality version
// -----------------------------------------------------------------------------
// High‑performance pool‑address → PairID map used by the router’s hot path.
// Each 40‑byte ASCII‑hex address is stored as **five native‑endian uint64s**
// (5 × 8 = 40 B).  Equality checks therefore boil down to five register
// comparisons — no heap allocations, no `bytes.Equal`, and no slice copying.
//
//  • Table size:  131 072 slots  ⇒  5 MiB of keys  + 0.5 MiB of PairIDs.
//  • Probing   :  stride‑64 linear probing (spreads colliding keys across
//                 cache sets; identical to the previous implementation).
//  • Safety    :  `binary.LittleEndian.Uint64` issues unaligned loads, so we
//                 can read directly from the incoming `[]byte` without copy.
// -----------------------------------------------------------------------------

package router

import (
	"encoding/binary"
	"main/utils"
)

// wordKey holds one 40‑byte address broken into 5× uint64 words.
// Using a fixed value type lets the compiler generate plain register
// comparisons instead of a function call to bytes.Equal.
//
//   w[0] – addr40[ 0: 8]
//   w[1] – addr40[ 8:16]
//   w[2] – addr40[16:24]
//   w[3] – addr40[24:32]
//   w[4] – addr40[32:40]
// -----------------------------------------------------------------------------

type wordKey struct{ w [5]uint64 }

// Global tables:  5 MiB of keys + 0.5 MiB for PairIDs.
var (
	pairKey  [1 << 17]wordKey // full key words (0 ⇒ unused when addr2pid[idx]==0)
	addr2pid [1 << 17]PairID  // 0 ⇒ empty slot
)

// sliceToWordKey converts a 40‑byte slice into a stack‑allocated wordKey.
// Cost: five unaligned 8‑byte loads → effectively free.
// -----------------------------------------------------------------------------

func sliceToWordKey(addr40 []byte) wordKey {
	return wordKey{w: [5]uint64{
		binary.LittleEndian.Uint64(addr40[0:8]),
		binary.LittleEndian.Uint64(addr40[8:16]),
		binary.LittleEndian.Uint64(addr40[16:24]),
		binary.LittleEndian.Uint64(addr40[24:32]),
		binary.LittleEndian.Uint64(addr40[32:40]),
	}}
}

// equal performs a 5‑word equality test — the compiler usually unrolls this.
func (k wordKey) equal(o wordKey) bool {
	return k.w[0] == o.w[0] &&
		k.w[1] == o.w[1] &&
		k.w[2] == o.w[2] &&
		k.w[3] == o.w[3] &&
		k.w[4] == o.w[4]
}

// RegisterPair inserts or overwrites a mapping from 40‑byte address → PairID.
// Zero‑allocation, zero‑copy.  Uses stride‑64 linear probing to resolve
// clashes; behaviour is unchanged from earlier byte‑slice version.
// -----------------------------------------------------------------------------

func RegisterPair(addr40 []byte, pid PairID) {
	k := sliceToWordKey(addr40) // stack‑only, no heap
	idx := utils.Hash17(addr40) // 17‑bit home bucket
	mask := uint32((1 << 17) - 1)
	for {
		if addr2pid[idx] == 0 { // empty slot ⇒ claim
			pairKey[idx] = k
			addr2pid[idx] = pid
			return
		}
		if pairKey[idx].equal(k) { // identical address ⇒ overwrite
			addr2pid[idx] = pid
			return
		}
		idx = (idx + 64) & mask // continue probe (stride‑64)
	}
}

// lookupPairID resolves a 40‑byte address to its PairID (0 if unknown).
// Also zero‑alloc / zero‑copy.
// -----------------------------------------------------------------------------

func lookupPairID(addr40 []byte) PairID {
	k := sliceToWordKey(addr40)
	idx := utils.Hash17(addr40)
	mask := uint32((1 << 17) - 1)
	for {
		pid := addr2pid[idx]
		if pid == 0 {
			return 0 // empty slot ⇒ key not present
		}
		if pairKey[idx].equal(k) {
			return pid // exact match
		}
		idx = (idx + 64) & mask // continue probe
	}
}
