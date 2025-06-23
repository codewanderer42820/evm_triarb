package pairidx

import (
	"fmt"
	"math/bits"
	"unsafe"
)

// -----------------------------------------------------------------------------
// Constants & Layout (keep it ridiculously simple)
// -----------------------------------------------------------------------------
const (
	CacheLineSize = 64 // fixed 64‑byte value block

	bucketCnt      = 1 << 17 // 131 072 primary buckets
	clustersPerBkt = 4       // 4 clusters / bucket
	clusterSlots   = 16      // 16 slots / cluster (64‑bit bitmap)

	bucketMask   = bucketCnt - 1      // bucket index mask
	clusterShift = 17                 // bits for bucket
	clMask       = clustersPerBkt - 1 // cluster index mask inside bucket
	slotMask     = clusterSlots - 1   // slot index mask inside cluster
)

// -----------------------------------------------------------------------------
// Tiny xxHash‑style mix (branch‑cheap, alloc‑free)
// -----------------------------------------------------------------------------
const (
	prime64_1 = 0x9E3779B185EBCA87
	prime64_2 = 0xC2B2AE3D27D4EB4F
)

//go:nosplit
func xxhMix64(p unsafe.Pointer, n uint16) uint64 {
	h := uint64(n) * prime64_1
	switch {
	case n <= 8:
		v := *(*uint64)(p)
		h = bits.RotateLeft64(v*prime64_2, 31) * prime64_1
	case n <= 16:
		v0 := *(*uint64)(p)
		v1 := *(*uint64)(unsafe.Add(p, uintptr(n)-8))
		h = bits.RotateLeft64(v0^bits.RotateLeft64(v1*prime64_2, 27), 31) * prime64_1
	default:
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
	h ^= h >> 33
	h *= prime64_2
	h ^= h >> 29
	h *= prime64_1
	h ^= h >> 32
	return h
}

//go:nosplit
func sameKey(a, b unsafe.Pointer, n uint16) bool {
	p1, p2 := uintptr(a), uintptr(b)
	for rem := n; rem >= 8; rem -= 8 {
		if *(*uint64)(unsafe.Pointer(p1)) != *(*uint64)(unsafe.Pointer(p2)) {
			return false
		}
		p1 += 8
		p2 += 8
	}
	if n&4 != 0 {
		if *(*uint32)(unsafe.Pointer(p1)) != *(*uint32)(unsafe.Pointer(p2)) {
			return false
		}
		p1 += 4
		p2 += 4
	}
	if n&2 != 0 && *(*uint16)(unsafe.Pointer(p1)) != *(*uint16)(unsafe.Pointer(p2)) {
		return false
	}
	if n&1 != 0 && *(*uint8)(unsafe.Pointer(p1)) != *(*uint8)(unsafe.Pointer(p2)) {
		return false
	}
	return true
}

var probeSeq = [...]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

// -----------------------------------------------------------------------------
// Data layout (packed, alignment‑friendly)
// -----------------------------------------------------------------------------

type slot struct {
	tag  uint16         // upper 16 bits of hash | 1 (0 → empty)
	klen uint16         // key length (≤ 65 535)
	kptr unsafe.Pointer // key bytes (caller keeps alive)
	_    [4]byte        // align to 16B
	data [CacheLineSize]byte
}

type cluster struct {
	bitmap uint64
	_      [56]byte
	slots  [clusterSlots]slot
}

type HashMap struct {
	buckets [bucketCnt][clustersPerBkt]cluster
	size    uint32 // purely informational; caller decides load factor
}

// -----------------------------------------------------------------------------
// Simple public helpers
// -----------------------------------------------------------------------------

func New() *HashMap          { return &HashMap{} }
func (h *HashMap) Size() int { return int(h.size) }

func (h *HashMap) Put(k string, v []byte) {
	if len(v) > CacheLineSize {
		panic("value exceeds 64 bytes")
	}
	copy(h.PutView(k)[:], v)
}

func (h *HashMap) Get(k string) ([]byte, bool) {
	if p := h.GetView(k); p != nil {
		return p[:], true
	}
	return nil, false
}

// -----------------------------------------------------------------------------
// Hot‑path API (zero‑alloc, zero‑copy, single‑bucket logic)
// -----------------------------------------------------------------------------

func (h *HashMap) GetView(k string) *[CacheLineSize]byte {
	ptr := unsafe.StringData(k)
	klen := uint16(len(k))
	hash := xxhMix64(unsafe.Pointer(ptr), klen)

	bucketIdx := uint32(hash) & bucketMask
	ci := (uint32(hash) >> clusterShift) & clMask
	base := int(uint16(hash)) & slotMask
	tag := uint16(hash>>48) | 1

	cl := &h.buckets[bucketIdx][ci]
	bm := cl.bitmap

	for _, off := range probeSeq {
		i := (base + int(off)) & slotMask
		if bm&(uint64(1)<<uint(i)) == 0 {
			return nil
		}
		s := &cl.slots[i]
		if s.tag == tag && s.klen == klen && sameKey(s.kptr, unsafe.Pointer(ptr), klen) {
			return &s.data
		}
	}
	return nil
}

func (h *HashMap) PutView(k string) *[CacheLineSize]byte {
	ptr := unsafe.StringData(k)
	klen := uint16(len(k))
	hash := xxhMix64(unsafe.Pointer(ptr), klen)

	bucketIdx := uint32(hash) & bucketMask
	ci := (uint32(hash) >> clusterShift) & clMask
	base := int(uint16(hash)) & slotMask
	tag := uint16(hash>>48) | 1

	cl := &h.buckets[bucketIdx][ci]
	bm := cl.bitmap

	for _, off := range probeSeq {
		i := (base + int(off)) & slotMask
		bit := uint64(1) << uint(i)
		s := &cl.slots[i]

		if bm&bit == 0 {
			s.tag, s.klen, s.kptr = tag, klen, unsafe.Pointer(ptr)
			cl.bitmap = bm | bit
			h.size++
			return &s.data
		}
		if s.tag == tag && s.klen == klen && sameKey(s.kptr, unsafe.Pointer(ptr), klen) {
			return &s.data
		}
	}
	panic(fmt.Sprintf("cluster full for key %s (caller must keep load factor low)", k))
}
