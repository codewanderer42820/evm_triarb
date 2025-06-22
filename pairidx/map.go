package pairidx

import (
	"hash/crc32"
	"unsafe"
)

const (
	// --- shape -----------------------------------------------------------
	bucketCnt      = 64
	clustersPerBkt = 4
	clusterSlots   = 16
	bucketMask     = bucketCnt - 1
	clusterShift   = 6 // == bits.Len32(bucketCnt-1)
	clMask         = clustersPerBkt - 1
	slotMask       = clusterSlots - 1
)

// hardware CRC table (still used for long keys)
var crcTab = crc32.MakeTable(crc32.Castagnoli)

/*──────────────────── helpers ────────────────────*/

// hash16 returns the low-16 CRC bits and the 16-bit tag.
// Keys ≤ 12 B use a branch-free XOR mix instead of crc32.Update.
//
//go:inline
func hash16(ptr unsafe.Pointer, n uint16) (low, tag uint16) {
	if n <= 8 {
		v := *(*uint64)(ptr)
		low, tag = uint16(v), uint16(v>>16)|1
		return
	}
	if n <= 12 {
		v0 := *(*uint64)(ptr)
		v1 := *(*uint32)(unsafe.Add(ptr, 8))
		x := v0 ^ (uint64(v1) << 32)
		low, tag = uint16(x), uint16(x>>16)|1
		return
	}
	crc := crc32.Update(0, crcTab, unsafe.Slice((*byte)(ptr), int(n)))
	return uint16(crc), uint16(crc>>16) | 1
}

// sameKey – fast pointer compare for short keys (≤12B).
//
//go:inline
func sameKey(a, b unsafe.Pointer, n uint16) bool {
	if n <= 8 {
		return *(*uint64)(a) == *(*uint64)(b)
	}
	return *(*uint64)(a) == *(*uint64)(b) &&
		*(*uint32)(unsafe.Add(a, 8)) == *(*uint32)(unsafe.Add(b, 8))
}

/*──────────────────── layout ─────────────────────*/

type slotUnsafe struct {
	tag, klen uint16
	kptr      unsafe.Pointer
	val       uint32
	_         [8]byte // pad → 32 B
}

type cluster struct {
	bitmap uint64
	_      [56]byte
	slots  [clusterSlots]slotUnsafe
}

type HashMap struct {
	buckets [bucketCnt][clustersPerBkt]cluster
	size    uint32
}

func New() *HashMap { return &HashMap{} }

/*──────────────────── probe order (const) ────────*/

var probeSeq = [...]int{
	0, 1, 2, 3, 4, 5, 6, 7,
	8, 9, 10, 11, 12, 13, 14, 15,
}

/*──────────────────── Get ────────────────────────*/

//go:nosplit
func (h *HashMap) Get(k string) (uint16, bool) {
	ptr := unsafe.StringData(k)
	klen := uint16(len(k))

	low, tag := hash16(unsafe.Pointer(ptr), klen)

	bi := uint32(low) & bucketMask
	ci := (uint32(low) >> clusterShift) & clMask
	cl := &h.buckets[bi][ci]

	bm := cl.bitmap
	base := int(low) & slotMask

	for _, off := range probeSeq {
		i := (base + off) & slotMask
		bit := uint64(1) << uint(i)

		if bm&bit == 0 {
			return 0, false // empty
		}
		s := &cl.slots[i]
		if s.tag == tag && s.klen == klen &&
			sameKey(s.kptr, unsafe.Pointer(ptr), klen) {
			return uint16(s.val), true
		}
	}
	return 0, false
}

/*──────────────────── Put ────────────────────────*/

//go:nosplit
func (h *HashMap) Put(k string, v uint16) {
	ptr := unsafe.StringData(k)
	klen := uint16(len(k))

	low, tag := hash16(unsafe.Pointer(ptr), klen)

	bi := uint32(low) & bucketMask
	ci := (uint32(low) >> clusterShift) & clMask
	cl := &h.buckets[bi][ci]

	bm := cl.bitmap
	val32 := uint32(v)
	base := int(low) & slotMask

	for _, off := range probeSeq {
		i := (base + off) & slotMask
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
		s.tag, s.klen, s.kptr = tag, klen, unsafe.Pointer(ptr)
		s.val = val32
		cl.bitmap = bm | bit
		h.size++
		return
	}
	panic("pairidx: cluster full")
}

func (h *HashMap) Size() int { return int(h.size) }
