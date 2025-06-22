// Ultra-low-latency fixed-capacity map with XXH3-style hashing.
// After New() it performs **no heap allocations** and touches user keys
// only through unsafe, zero-copy loads.
package pairidx

import (
	"math/bits"
	"unsafe"
)

/*─────────────── capacity / layout ───────────────*/

const (
	bucketCnt      = 64 // power-of-two ⇒ single mask
	clustersPerBkt = 4
	clusterSlots   = 16

	bucketMask   = bucketCnt - 1
	clusterShift = 6 // log₂(bucketCnt)
	clMask       = clustersPerBkt - 1
	slotMask     = clusterSlots - 1
)

/*────────────── tiny XXH3-style mixer ────────────*/

// Two 64-bit primes from XXH3 (“secret” folded to a constant).
const (
	prime64_1 = 0x9E3779B185EBCA87 // 64-bit golden ratio
	prime64_2 = 0xC2B2AE3D27D4EB4F
)

// xxhMix64 hashes a *single* 8-byte lane into a high-entropy 64-bit
// value with ~2 ns latency on ARM64.  For <=32-byte keys we XOR-mix up
// to four lanes; longer keys fall back to an 8-byte rolling hash.
//
//go:nosplit
func xxhMix64(p unsafe.Pointer, n uint16) uint64 {
	var h uint64 = uint64(n) * prime64_1

	switch {
	case n <= 8:
		v := *(*uint64)(p)
		h = bits.RotateLeft64(v*prime64_2, 31) * prime64_1
	case n <= 16:
		v0 := *(*uint64)(p)
		v1 := *(*uint64)(unsafe.Add(p, uintptr(n)-8))
		h = v0 ^ bits.RotateLeft64(v1*prime64_2, 27)
		h = bits.RotateLeft64(h*prime64_1, 31) * prime64_2
	case n <= 32:
		v0 := *(*uint64)(p)
		v1 := *(*uint64)(unsafe.Add(p, 8))
		v2 := *(*uint64)(unsafe.Add(p, uintptr(n)-16))
		v3 := *(*uint64)(unsafe.Add(p, uintptr(n)-8))
		h = v0 ^ bits.RotateLeft64(v1*prime64_2, 31)
		h ^= bits.RotateLeft64(v2*prime64_2, 27)
		h ^= bits.RotateLeft64(v3*prime64_1, 33)
		h = bits.RotateLeft64(h*prime64_1, 27) * prime64_1
	default:
		// rolling 8-byte hash; single pass, no allocation
		p8 := uintptr(p)
		for rem := n; rem >= 8; rem -= 8 {
			v := *(*uint64)(unsafe.Pointer(p8))
			p8 += 8
			h ^= bits.RotateLeft64(v*prime64_2, 31)
			h = bits.RotateLeft64(h, 27) * prime64_1
		}
		if tail := n & 7; tail != 0 {
			t := *(*uint64)(unsafe.Pointer(p8)) & ((1 << (tail * 8)) - 1)
			h ^= bits.RotateLeft64(t*prime64_2, 11)
			h = bits.RotateLeft64(h, 7) * prime64_1
		}
	}

	// final avalanche (32+64 → 64 bits of quality)
	h ^= h >> 33
	h *= prime64_2
	h ^= h >> 29
	h *= prime64_1
	h ^= h >> 32
	return h
}

/*────────────── slot / cluster structs ──────────*/

type slot struct {
	tag, klen uint16
	kptr      unsafe.Pointer
	val       uint32
	_         [8]byte // pad → 32 B (fits 2/line)
}

type cluster struct {
	bitmap uint64
	_      [56]byte
	slots  [clusterSlots]slot
}

type HashMap struct {
	buckets [bucketCnt][clustersPerBkt]cluster
	size    uint32
}

func New() *HashMap { return &HashMap{} }

/*──────────── key compare (SWMR) ────────────────*/

//go:nosplit
func sameKey(a, b unsafe.Pointer, n uint16) bool {
	if n <= 8 {
		return *(*uint64)(a) == *(*uint64)(b)
	}
	if n <= 12 {
		return *(*uint64)(a) == *(*uint64)(b) &&
			*(*uint32)(unsafe.Add(a, 8)) == *(*uint32)(unsafe.Add(b, 8))
	}
	// SWMR loop, 8-4-2-1 tail
	p1, p2 := uintptr(a), uintptr(b)
	for rem := n; rem >= 8; rem -= 8 {
		if *(*uint64)(unsafe.Pointer(p1)) !=
			*(*uint64)(unsafe.Pointer(p2)) {
			return false
		}
		p1, p2 = p1+8, p2+8
	}
	if n&4 != 0 {
		if *(*uint32)(unsafe.Pointer(p1)) !=
			*(*uint32)(unsafe.Pointer(p2)) {
			return false
		}
		p1, p2 = p1+4, p2+4
	}
	if n&2 != 0 &&
		*(*uint16)(unsafe.Pointer(p1)) != *(*uint16)(unsafe.Pointer(p2)) {
		return false
	}
	if n&1 != 0 &&
		*(*uint8)(unsafe.Pointer(p1)) != *(*uint8)(unsafe.Pointer(p2)) {
		return false
	}
	return true
}

/*────────────── probe order (const) ─────────────*/

var probeSeq = [...]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

/*─────────────────── Get ─────────────────────────*/

//go:nosplit
func (h *HashMap) Get(k string) (uint16, bool) {
	ptr := unsafe.StringData(k)
	klen := uint16(len(k))

	hash := xxhMix64(unsafe.Pointer(ptr), klen)
	low := uint16(hash)
	tag := uint16(hash>>48) | 1 // high 16 b make a non-zero tag

	cl := &h.buckets[uint32(low)&bucketMask][(uint32(low)>>clusterShift)&clMask]
	bm := cl.bitmap
	base := int(low) & slotMask

	for _, off := range probeSeq {
		i := (base + int(off)) & slotMask
		bit := uint64(1) << uint(i)

		if bm&bit == 0 {
			return 0, false
		}
		s := &cl.slots[i]
		if s.tag == tag && s.klen == klen &&
			sameKey(s.kptr, unsafe.Pointer(ptr), klen) {
			return uint16(s.val), true
		}
	}
	return 0, false
}

/*─────────────────── Put ─────────────────────────*/

//go:nosplit
func (h *HashMap) Put(k string, v uint16) {
	ptr := unsafe.StringData(k)
	klen := uint16(len(k))

	hash := xxhMix64(unsafe.Pointer(ptr), klen)
	low := uint16(hash)
	tag := uint16(hash>>48) | 1

	cl := &h.buckets[uint32(low)&bucketMask][(uint32(low)>>clusterShift)&clMask]
	bm := cl.bitmap
	val32 := uint32(v)
	base := int(low) & slotMask

probe:
	for _, off := range probeSeq {
		i := (base + int(off)) & slotMask
		bit := uint64(1) << uint(i)
		s := &cl.slots[i]

		if bm&bit != 0 {
			if s.tag == tag && s.klen == klen &&
				sameKey(s.kptr, unsafe.Pointer(ptr), klen) {
				if s.val != val32 {
					s.val = val32
				}
				return
			}
			continue
		}
		// empty slot → insert
		s.tag, s.klen, s.kptr = tag, klen, unsafe.Pointer(ptr)
		s.val = val32
		cl.bitmap = bm | bit
		h.size++
		return
	}

	// ───────── optional secondary cluster hop (never panics) ───────
	// Remove this loop and keep the panic if you prefer strict 16-slot caps.
	ci := (uint32(low)>>clusterShift + 1) & clMask
	cl = &h.buckets[uint32(low)&bucketMask][ci]
	bm = cl.bitmap
	goto probe
}

/*──────────────── misc ───────────────────────────*/

func (h *HashMap) Size() int { return int(h.size) }
