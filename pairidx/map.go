// SPDX‑License‑Identifier: MIT
// Ultra‑low‑latency SWMR hash map – bucketed + clustered + bitmap scan
// Target: < 8 ns per Get/Put (uncontended) with reader‑lock‑free safety.

package pairidx

import (
	"hash/crc32"
	"math/bits"
	"sync/atomic"
	"unsafe"
)

/*
─────────────────────────────────────────────────────────────────────────────*
| Geometry – everything power‑of‑two so masks beat mods                    |
*────────────────────────────────────────────────────────────────────────────
*/
const (
	bucketCnt   = 1024 // primary buckets (2^10)
	bucketShift = 10   // log2(bucketCnt)
	bucketMask  = bucketCnt - 1

	clustersPerBkt = 4 // fan‑out per bucket
	clMask         = clustersPerBkt - 1

	clusterSlots = 64                                // slots per cluster (fits u64 bitmap)
	slotMask     = clusterSlots - 1                  // == 63
	fullMask     = ^uint64(0) >> (64 - clusterSlots) // low N bits set

	hashShift = 16 // skip low entropy bits
)

/*
─────────────────────────────────────────────────────────────────────────────*

	| Fast CRC32C hash – good avalanche, ~2–4 ns on Apple M‑series             |
	*────────────────────────────────────────────────────────────────────────────
*/
var crcTab = crc32.MakeTable(crc32.Castagnoli)

func crc32cMix(k string) (h32 uint32, tag uint16) {
	// Zero‑copy view of the string bytes
	bs := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.StringData(k))), len(k))
	x := crc32.Update(0, crcTab, bs)

	// One 32×32→64 multiply to smear low bits further
	x64 := uint64(x) * 0x9E3779B185EBCA87
	h32 = uint32(x64)
	tag = uint16(x64>>48) | 1 // never zero
	return
}

/*
─────────────────────────────────────────────────────────────────────────────*

	| Key compare – zero‑alloc, early‑exit                                     |
	*────────────────────────────────────────────────────────────────────────────
*/
func sameKey(a, b unsafe.Pointer, n uint16) bool {
	if n == 0 {
		return true
	}
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

/*─────────────────────────────────────────────────────────────────────────────*
| Slot / cluster layout                                                    |
*────────────────────────────────────────────────────────────────────────────*/
// 32‑byte cold slot – padded for cache alignment.
// (tag,klen) share the first word so one 32‑bit load gets both.
// Epoch/version is handled at table level so we keep slot lean.
type slot struct {
	tag  uint16         // collision tag (upper 16 bits of hash)
	klen uint16         // key length (<= 65 535)
	kptr unsafe.Pointer // ptr to key bytes (string data)
	val  uint32         // user value (change as needed)
	_pad [12]byte       // pad to 32 B
}

type cluster struct {
	bitmap uint64             // 1‑bit per slot: 1 = used
	_pad   [56]byte           // keep meta separate (false‑sharing guard)
	slots  [clusterSlots]slot // 64 × 32 B = 2 KiB
}

/*
─────────────────────────────────────────────────────────────────────────────*

	| HashMap – single writer / multi reader                                   |
	*────────────────────────────────────────────────────────────────────────────
*/
type HashMap struct {
	epoch   uint64                             // even ⇒ quiescent, odd ⇒ writer busy
	buckets [bucketCnt][clustersPerBkt]cluster // main storage
	size    uint32                             // monotonic – no deletes here
}

func New() *HashMap { return &HashMap{} }

/*
─────────────────────────────────────────────────────────────────────────────*

	| Helpers                                                                   |
	*────────────────────────────────────────────────────────────────────────────
*/
func rotateBits(x uint64, k int) uint64 {
	return ((x >> uint(k)) | (x << uint(clusterSlots-k))) & fullMask
}
func derotateIdx(i, start int) int { return (i + start) & slotMask }

/*
─────────────────────────────────────────────────────────────────────────────*

	| SWMR Get – reader lock‑free, writer‑consistent                            |
	*────────────────────────────────────────────────────────────────────────────
*/
func (h *HashMap) Get(k string) unsafe.Pointer {
	for {
		snap := atomic.LoadUint64(&h.epoch)
		if snap&1 != 0 { // writer in flight – spin briefly
			continue
		}

		kptr := unsafe.StringData(k)
		klen := uint16(len(k))
		h32, tag := crc32cMix(k)

		bi := (h32 >> hashShift) & bucketMask
		ci := (h32 >> (hashShift + bucketShift)) & clMask
		cl := &h.buckets[bi][ci]

		bm := atomic.LoadUint64(&cl.bitmap) // relaxed ok – verified by epoch
		if bm == 0 {
			if atomic.LoadUint64(&h.epoch) == snap {
				return nil
			}
			continue // retry – writer touched something
		}

		start := int(h32) & slotMask
		rbm := rotateBits(bm, start)
		for rbm != 0 {
			tz := bits.TrailingZeros64(rbm)
			idx := derotateIdx(tz, start)
			s := &cl.slots[idx]
			if s.tag == tag && s.klen == klen && sameKey(s.kptr, unsafe.Pointer(kptr), klen) {
				if atomic.LoadUint64(&h.epoch) == snap {
					return unsafe.Pointer(&s.val)
				}
				break // epoch changed – redo lookup
			}
			rbm &= rbm - 1
		}
		if atomic.LoadUint64(&h.epoch) == snap {
			return nil // consistent miss
		}
		// else: writer raced – retry
	}
}

/*
─────────────────────────────────────────────────────────────────────────────*

	| Put – single writer only; readers stay lock‑free                           |
	*────────────────────────────────────────────────────────────────────────────
*/
func (h *HashMap) Put(k string, v uint32) unsafe.Pointer {
	// Block readers
	atomic.AddUint64(&h.epoch, 1) // odd

	kptr := unsafe.StringData(k)
	klen := uint16(len(k))
	h32, tag := crc32cMix(k)

	bi := (h32 >> hashShift) & bucketMask
	ci := (h32 >> (hashShift + bucketShift)) & clMask
	cl := &h.buckets[bi][ci]

	bm := cl.bitmap
	start := int(h32) & slotMask
	scan := rotateBits(fullMask, start) // 64 bits set

	for scan != 0 {
		tz := bits.TrailingZeros64(scan)
		idx := derotateIdx(tz, start)
		bit := uint64(1) << uint(idx)
		s := &cl.slots[idx]

		if bm&bit == 0 { // empty – insert
			s.tag, s.klen, s.kptr, s.val = tag, klen, unsafe.Pointer(kptr), v
			cl.bitmap = bm | bit
			h.size++
			atomic.AddUint64(&h.epoch, 1) // even – readers proceed
			return unsafe.Pointer(&s.val)
		}
		if s.tag == tag && s.klen == klen && sameKey(s.kptr, unsafe.Pointer(kptr), klen) {
			s.val = v // overwrite
			atomic.AddUint64(&h.epoch, 1)
			return unsafe.Pointer(&s.val)
		}
		scan &= scan - 1
	}

	// Fallback cluster (second hash) – extremely rare
	h32b := bits.Reverse32(h32) // cheap 1‑cycle permutation
	bi = (h32b >> hashShift) & bucketMask
	ci = (h32b >> (hashShift + bucketShift)) & clMask
	cl = &h.buckets[bi][ci]
	bm = cl.bitmap
	scan = fullMask
	for scan != 0 {
		tz := bits.TrailingZeros64(scan)
		idx := tz
		bit := uint64(1) << uint(idx)
		s := &cl.slots[idx]
		if bm&bit == 0 {
			s.tag, s.klen, s.kptr, s.val = tag, klen, unsafe.Pointer(kptr), v
			cl.bitmap = bm | bit
			h.size++
			atomic.AddUint64(&h.epoch, 1)
			return unsafe.Pointer(&s.val)
		}
		scan &= scan - 1
	}

	atomic.AddUint64(&h.epoch, 1) // unblock readers before panicking
	panic("pairidx: cluster full – reduce load factor or re‑hash keys")
}

// Size returns the monotonically increasing entry count.
func (h *HashMap) Size() int { return int(atomic.LoadUint32(&h.size)) }
