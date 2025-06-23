package pairidx

import (
	"math/bits"
	"unsafe"
)

// -----------------------------------------------------------------------------
// Constants & layout – cache‑friendly, power‑of‑two everywhere
// -----------------------------------------------------------------------------
const (
	bucketCnt      = 64 // primary buckets (must be 2^n)
	clustersPerBkt = 4  // secondary hash fan‑out
	clusterSlots   = 16 // slots per cluster – fits in a u64 bitmap

	slotMask   = clusterSlots - 1
	clMask     = clustersPerBkt - 1
	bucketMask = bucketCnt - 1

	bucketShift = 6 // log2(bucketCnt) – compile‑time constant (64 → 6)
)

// -----------------------------------------------------------------------------
// 64‑bit pointer‑length avalanche (xxh3‑style)
// -----------------------------------------------------------------------------
func hashPtrLen(ptr unsafe.Pointer, klen uint16) (h32 uint32, tag uint16) {
	x := uint64(uintptr(ptr)) ^ (uint64(klen) * 0x9e3779b97f4a7c15)
	x ^= x >> 33
	x *= 0xc2b2ae3d27d4eb4f
	x ^= x >> 29
	tag = uint16(x>>48) | 1 // never zero
	h32 = uint32(x)
	return
}

// -----------------------------------------------------------------------------
// Key compare – zero‑alloc, branch‑cheap
// -----------------------------------------------------------------------------
func sameKey(a, b unsafe.Pointer, n uint16) bool {
	if n <= 8 {
		return *(*uint64)(a) == *(*uint64)(b)
	}
	if *(*uint64)(a) != *(*uint64)(b) {
		return false
	}
	off := uintptr(8)
	rem := uintptr(n) - 8
	for rem >= 8 {
		if *(*uint64)(unsafe.Add(a, off)) != *(*uint64)(unsafe.Add(b, off)) {
			return false
		}
		off += 8
		rem -= 8
	}
	if rem >= 4 {
		if *(*uint32)(unsafe.Add(a, off)) != *(*uint32)(unsafe.Add(b, off)) {
			return false
		}
		off += 4
		rem -= 4
	}
	if rem >= 2 {
		if *(*uint16)(unsafe.Add(a, off)) != *(*uint16)(unsafe.Add(b, off)) {
			return false
		}
		off += 2
		rem -= 2
	}
	if rem == 1 {
		return *(*uint8)(unsafe.Add(a, off)) == *(*uint8)(unsafe.Add(b, off))
	}
	return true
}

// -----------------------------------------------------------------------------
// Slot / cluster layout
// -----------------------------------------------------------------------------

type slot struct {
	tag  uint16         // 16‑bit collision tag
	klen uint16         // key length
	kptr unsafe.Pointer // raw pointer to key bytes
	val  uint32         // user value
	_    [8]byte        // pad to 32 B
}

type cluster struct {
	bitmap uint64             // occupancy bitmap (LSB ⇒ slot0)
	_      [56]byte           // keep metadata away from slot array
	slots  [clusterSlots]slot // 512 B
}

// -----------------------------------------------------------------------------
// HashMap – SWMR, zero alloc
// -----------------------------------------------------------------------------

type HashMap struct {
	buckets [bucketCnt][clustersPerBkt]cluster
	size    uint32 // monotonically increasing – no deletes
}

func New() *HashMap { return &HashMap{} }

// -----------------------------------------------------------------------------
// Helpers – bitmap rotations
// -----------------------------------------------------------------------------

const fullMask16 = uint64(1<<clusterSlots) - 1

func rotateBits16(x uint64, k int) uint64 {
	// Rotate left by k (0‑15) in the low 16 bits.
	return ((x >> uint(k)) | (x << uint(clusterSlots-k))) & fullMask16
}

func derotateIdx(i, start int) int { return (i + start) & slotMask }

// -----------------------------------------------------------------------------
// Get – branchless bitmap scan
// -----------------------------------------------------------------------------

func (h *HashMap) Get(k string) unsafe.Pointer {
	kptr := unsafe.StringData(k)
	klen := uint16(len(k))

	h32, tag := hashPtrLen(unsafe.Pointer(kptr), klen)
	bi := h32 & bucketMask
	ci := (h32 >> bucketShift) & clMask
	cl := &h.buckets[bi][ci]

	bm := cl.bitmap
	if bm == 0 {
		return nil
	}

	start := int(h32) & slotMask
	rbm := rotateBits16(bm, start)

	for rbm != 0 {
		tz := bits.TrailingZeros64(rbm)
		idx := derotateIdx(tz, start)
		s := &cl.slots[idx]
		if s.tag == tag && s.klen == klen && sameKey(s.kptr, unsafe.Pointer(kptr), klen) {
			return unsafe.Pointer(&s.val)
		}
		rbm &= rbm - 1 // clear lowest set bit
	}
	return nil
}

// -----------------------------------------------------------------------------
// Put – scans all 16 probe positions once (branchless), inserts at first hole
// -----------------------------------------------------------------------------

func (h *HashMap) Put(k string, v uint32) unsafe.Pointer {
	kptr := unsafe.StringData(k)
	klen := uint16(len(k))

	h32, tag := hashPtrLen(unsafe.Pointer(kptr), klen)
	bi := h32 & bucketMask
	ci := (h32 >> bucketShift) & clMask
	cl := &h.buckets[bi][ci]

	bm := cl.bitmap
	start := int(h32) & slotMask

	// Pre‑rotated mask that walks every slot exactly once in probe order.
	scan := rotateBits16(fullMask16, start)

	for scan != 0 {
		tz := bits.TrailingZeros64(scan)
		idx := derotateIdx(tz, start)
		bit := uint64(1) << uint(idx)
		s := &cl.slots[idx]

		if bm&bit == 0 {
			// Empty – claim and return.
			s.tag, s.klen, s.kptr, s.val = tag, klen, unsafe.Pointer(kptr), v
			cl.bitmap = bm | bit
			h.size++
			return unsafe.Pointer(&s.val)
		}

		// Occupied – check for duplicate.
		if s.tag == tag && s.klen == klen && sameKey(s.kptr, unsafe.Pointer(kptr), klen) {
			s.val = v // overwrite
			return unsafe.Pointer(&s.val)
		}
		scan &= scan - 1 // clear bit, continue
	}

	panic("pairidx: cluster full – reduce load factor or re‑hash keys")
}

// Size returns the current entry count.
func (h *HashMap) Size() int { return int(h.size) }
