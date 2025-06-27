// router_lookup_zeroalloc.go — ZERO‑alloc, ZERO‑copy, 64‑bit equality version
// Replaces RegisterPair & lookupPairID with probe‑compatible variants that
// compare *five* native‑endian uint64 words (5 × 8 = 40 B) instead of a []byte.
// This removes all heap allocations, avoids bytes.Equal, and lets the
// compiler use plain registers for fast equality checks.
//
// • Key storage:  131 072 × 5×8 = 5.0 MiB  (unchanged, but now strongly typed)
// • Probing:      stride‑64 open‑addressing as before (cache‑set spreading)
// • Safety:       binary.LittleEndian.Uint64 performs an unaligned 8‑byte load
//                 on all Go architectures, so we can read straight from the
//                 ascii‑hex slice without copying.
//
// NOTE:  We keep *identical* behaviour for collisions & overwrites, only faster.
// -----------------------------------------------------------------------------

package router

import (
	"encoding/binary"
	"main/utils"
	"unsafe"
)

// wordKey represents one 40‑byte ascii‑hex pool address as five uint64 words.
// (We store native‑endian; equality tests compare the raw words.)
// Using a value type instead of a byte slice eliminates allocations and lets
// the compiler emit straight‑line 64‑bit comparisons.
//
//   words[0] – addr40[ 0: 8]
//   words[1] – addr40[ 8:16]
//   words[2] – addr40[16:24]
//   words[3] – addr40[24:32]
//   words[4] – addr40[32:40]
//
// Alignment:  The table below is \u003c1‑cacheline stride and will be 8‑byte
//             aligned in globals section, so mis‑alignment can only arise when
//             converting the incoming []byte.  We bypass that with the safe
//             binary.LittleEndian helpers (they generate unaligned loads).
// ----------------------------------------------------------------------------

type wordKey struct{ w [5]uint64 }

// Global tables:  5.0 MiB of keys + 0.5 MiB of PairIDs.
var (
	pairKey  [1 << 17]wordKey // full key words
	addr2pid [1 << 17]PairID  // 0 ⇒ empty slot
)

// sliceToWordKey converts a 40‑byte ascii‑hex slice into a stack‑allocated
// wordKey WITHOUT allocating or copying onto the heap.
// Cost:  5×  unaligned 8‑byte loads + registers → negligible.
func sliceToWordKey(addr40 []byte) wordKey {
	return wordKey{[5]uint64{
		binary.LittleEndian.Uint64(addr40[0:8]),
		binary.LittleEndian.Uint64(addr40[8:16]),
		binary.LittleEndian.Uint64(addr40[16:24]),
		binary.LittleEndian.Uint64(addr40[24:32]),
		binary.LittleEndian.Uint64(addr40[32:40]),
	}}
}

// equal reports whether two wordKeys are identical (compiler unrolls this).
func (k wordKey) equal(other wordKey) bool {
	return k.w[0] == other.w[0] &&
		k.w[1] == other.w[1] &&
		k.w[2] == other.w[2] &&
		k.w[3] == other.w[3] &&
		k.w[4] == other.w[4]
}

// RegisterPair stores (address → PairID).  Overwrites if same address exists.
func RegisterPair(addr40 []byte, pid PairID) {
	k := sliceToWordKey(addr40) // stack‑only, zero‑alloc
	idx := utils.Hash17(addr40) // 17‑bit home bucket
	mask := uint32((1 << 17) - 1)
	for {
		if addr2pid[idx] == 0 { // empty slot ⇒ claim
			pairKey[idx] = k
			addr2pid[idx] = pid
			return
		}
		if pairKey[idx].equal(k) { // same address ⇒ overwrite PairID
			addr2pid[idx] = pid
			return
		}
		idx = (idx + 64) & mask // stride‑64 linear probing
	}
}

// lookupPairID resolves addr40 → PairID (0 if absent). Zero‑alloc, zero‑copy.
func lookupPairID(addr40 []byte) PairID {
	k := sliceToWordKey(addr40)
	idx := utils.Hash17(addr40)
	mask := uint32((1 << 17) - 1)
	for {
		pid := addr2pid[idx]
		if pid == 0 {
			return 0 // empty slot ⇒ not present
		}
		if pairKey[idx].equal(k) {
			return pid // exact match
		}
		idx = (idx + 64) & mask
	}
}

// Compile‑time safeguard: ensure wordKey is exactly 40 bytes (5×8) without padd.
var _ [1]struct{} = [1]struct{}{
	func() struct{} {
		if unsafe.Sizeof(wordKey{}) != 40 {
			panic("wordKey size != 40 bytes")
		}
		return struct{}{}
	}(),
}
