// SPDX-License-Identifier: MIT
// One-writer / many-reader hash-map with a single upfront allocation:
//   • All buckets/clusters/slots + an 8 MiB key arena are allocated in New().
//   • Writer copies key bytes into the arena on first insert; no other allocs.
//   • Synchronisation = one atomic OR (release) / atomic Load (acquire) on
//     the bitmap word; no epochs, retries, or spin loops.

package pairidx

import (
	"hash/crc32"
	"math/bits"
	"sync/atomic"
	"unsafe"
)

/* ------------------------------------------------------------------------- */
/* Geometry                                                                  */
/* ------------------------------------------------------------------------- */

const (
	bucketCnt      = 1024 // power-of-two primary buckets
	bucketShift    = 10   // log2(bucketCnt)
	bucketMask     = bucketCnt - 1
	clustersPerBkt = 4
	clMask         = clustersPerBkt - 1
	clusterSlots   = 64 // 64-slot bitmap fits one uint64
	slotMask       = clusterSlots - 1
	fullMask       = ^uint64(0) // 0xFFFF_FFFF_FFFF_FFFF

	keyArenaSize = 8 << 20 // 8 MiB upfront
	hashShift    = 16      // skip low entropy bits
)

/* ------------------------------------------------------------------------- */
/* Pre-computed CRC-32C table (built once at start-up)                        */
/* ------------------------------------------------------------------------- */

var crcTab = crc32.MakeTable(crc32.Castagnoli)

func crc32cMix(k string) (h32 uint32, tag uint16) {
	data := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.StringData(k))), len(k))
	x := crc32.Update(0, crcTab, data)
	x64 := uint64(x) * 0x9E3779B185EBCA87
	return uint32(x64), uint16(x64>>48) | 1
}

/* ------------------------------------------------------------------------- */
/* Key compare – 64-bit numeric words, zero-padded tail                      */
/* ------------------------------------------------------------------------- */

func sameKey64(a, b unsafe.Pointer, n uint16) bool {
	na := uintptr(a)
	nb := uintptr(b)
	words := int(n) / 8
	for i := 0; i < words; i++ {
		if *(*uint64)(unsafe.Pointer(na + uintptr(i*8))) !=
			*(*uint64)(unsafe.Pointer(nb + uintptr(i*8))) {
			return false
		}
	}
	rem := n & 7
	if rem == 0 {
		return true
	}
	var ta, tb uint64
	copy((*[8]byte)(unsafe.Pointer(&ta))[:], unsafe.Slice((*byte)(unsafe.Pointer(na+uintptr(words*8))), int(rem)))
	copy((*[8]byte)(unsafe.Pointer(&tb))[:], unsafe.Slice((*byte)(unsafe.Pointer(nb+uintptr(words*8))), int(rem)))
	return ta == tb
}

/* ------------------------------------------------------------------------- */
/* Layout                                                                    */
/* ------------------------------------------------------------------------- */

type slot struct {
	tag  uint16
	klen uint16
	kptr unsafe.Pointer
	val  uint32
	_    [8]byte // pad to 32 B
}

type cluster struct {
	bitmap uint64
	_      [56]byte
	slots  [clusterSlots]slot
}

type HashMap struct {
	buckets [bucketCnt][clustersPerBkt]cluster
	arena   []byte
	off     uintptr
	size    uint32
}

/* ------------------------------------------------------------------------- */
/* Constructor – single heap allocation                                      */
/* ------------------------------------------------------------------------- */

func New() *HashMap {
	h := &HashMap{
		arena: make([]byte, keyArenaSize),
	}
	return h
}

/* ------------------------------------------------------------------------- */
/* Private helper – copy key into arena                                      */
/* ------------------------------------------------------------------------- */

func (h *HashMap) allocKeyCopy(k string) unsafe.Pointer {
	n := len(k)
	if h.off+uintptr(n) > uintptr(len(h.arena)) {
		panic("pairidx: key arena full – increase keyArenaSize")
	}
	dst := h.arena[h.off : h.off+uintptr(n)]
	copy(dst, k)
	ptr := unsafe.Pointer(&dst[0])
	h.off += uintptr(n)
	return ptr
}

/* ------------------------------------------------------------------------- */
/* Get                                                                       */
/* ------------------------------------------------------------------------- */

func (h *HashMap) Get(k string) unsafe.Pointer {
	kptr := unsafe.StringData(k)
	klen := uint16(len(k))

	h32, tag := crc32cMix(k)
	bi := (h32 >> hashShift) & bucketMask
	ci := (h32 >> (hashShift + bucketShift)) & clMask
	cl := &h.buckets[bi][ci]

	bm := atomic.LoadUint64(&cl.bitmap) // acquire
	if bm == 0 {
		return nil
	}

	start := int(h32) & slotMask
	rbm := ((bm >> start) | (bm << (clusterSlots - start))) & fullMask

	for rbm != 0 {
		tz := bits.TrailingZeros64(rbm)
		idx := (tz + start) & slotMask
		s := &cl.slots[idx]

		if s.tag == tag && s.klen == klen &&
			sameKey64(unsafe.Pointer(kptr), s.kptr, klen) {
			return unsafe.Pointer(&s.val)
		}
		rbm &= rbm - 1
	}
	return nil
}

/* ------------------------------------------------------------------------- */
/* Put – writer only                                                         */
/* ------------------------------------------------------------------------- */

func (h *HashMap) Put(k string, v uint32) unsafe.Pointer {
	kptr := unsafe.StringData(k)
	klen := uint16(len(k))

	h32, tag := crc32cMix(k)
	bi := (h32 >> hashShift) & bucketMask
	ci := (h32 >> (hashShift + bucketShift)) & clMask
	cl := &h.buckets[bi][ci]

	bm := cl.bitmap
	start := int(h32) & slotMask
	scan := ((fullMask >> start) | (fullMask << (clusterSlots - start))) & fullMask

	for scan != 0 {
		tz := bits.TrailingZeros64(scan)
		idx := (tz + start) & slotMask
		bit := uint64(1) << uint(idx)
		s := &cl.slots[idx]

		if bm&bit == 0 { // empty slot → insert
			arenaPtr := h.allocKeyCopy(k)
			s.tag, s.klen, s.kptr, s.val = tag, klen, arenaPtr, v
			atomic.OrUint64(&cl.bitmap, bit) // release
			h.size++
			return unsafe.Pointer(&s.val)
		}
		if s.tag == tag && s.klen == klen &&
			sameKey64(unsafe.Pointer(kptr), s.kptr, klen) {
			s.val = v // overwrite (no arena alloc, no size++)
			return unsafe.Pointer(&s.val)
		}
		scan &= scan - 1
	}
	panic("pairidx: cluster full – re-hash keys or enlarge map")
}

/* Size returns the number of unique keys. */
func (h *HashMap) Size() int { return int(atomic.LoadUint32(&h.size)) }
