// SPDX‑License‑Identifier: MIT
// -----------------------------------------------------------------------------
// Ultra‑compact one‑writer / many‑reader hash map
// -----------------------------------------------------------------------------
// ❶  Memory strategy
//     • **Single allocation** in New(): buckets + clusters + 8 MiB arena.
//     • No further heap work; writer copies key bytes into the arena on first
//       insert (memmove only, no allocate).
//
// ❷  Concurrency model
//     • One writer goroutine, any number of readers.
//     • Writer publishes slot via `atomic.OrUint64` (release) on the 64‑bit
//       bitmap; readers acquire with one `atomic.LoadUint64`.
//     • No epochs, spin loops, or CAS retries.
//
// ❸  Hash + addressing
//     • CRC‑32C (hardware) + 64‑bit mix ⇒ h32.
//     • Primary bucket = bits 16‑25, cluster = bits 26‑27, slot = bits 0‑5.
//
// ❹  Layout tweaks in this revision
//     • **slot** reordered for minimal padding: kptr first (8 B align), then
//       val (4 B), tag+klen (4 B) → 16 B of data + 16 B reserved for future
//       per‑key metadata = **32 B** exactly.
//     • Comments regenerated for clarity and kept Go doc‑friendly.
// -----------------------------------------------------------------------------

package pairidx

import (
	"hash/crc32"
	"math/bits"
	"sync/atomic"
	"unsafe"
)

/*──────────────────────────────────────────────────────────────────────────────*/
/* Geometry                                                                    */
/*──────────────────────────────────────────────────────────────────────────────*/

const (
	bucketCnt   = 1024 // 2¹⁰ primary buckets
	bucketShift = 10   // log2(bucketCnt)
	bucketMask  = bucketCnt - 1

	clustersPerBkt = 4 // secondary fan‑out
	clMask         = clustersPerBkt - 1

	clusterSlots = 64 // slots per cluster (bitmap = uint64)
	slotMask     = clusterSlots - 1

	fullMask = ^uint64(0) // handy all‑ones mask

	keyArenaSize = 8 << 20 // 8 MiB upfront; panic if exceeded

	hashShift = 16 // skip noisy low bits of CRC
)

/*──────────────────────────────────────────────────────────────────────────────*/
/* Hash                                                                         */
/*──────────────────────────────────────────────────────────────────────────────*/

var crcTab = crc32.MakeTable(crc32.Castagnoli) // built once at init

func crc32cMix(k string) (h32 uint32, tag uint16) {
	data := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.StringData(k))), len(k))
	x := crc32.Update(0, crcTab, data)
	x64 := uint64(x) * 0x9E3779B185EBCA87   // single 64×64→128 mix step
	return uint32(x64), uint16(x64>>48) | 1 // tag never 0
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Key compare – 64‑bit numeric words (zero‑padded tail)                        */
/*──────────────────────────────────────────────────────────────────────────────*/

func sameKey64(a, b unsafe.Pointer, n uint16) bool {
	na, nb := uintptr(a), uintptr(b)
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

/*──────────────────────────────────────────────────────────────────────────────*/
/* Layout structs                                                               */
/*──────────────────────────────────────────────────────────────────────────────*/

// slot holds one key→value entry and is exactly 32 B.
// Field order chosen to minimise padding while keeping the 32‑B stride.
//
//   offset 0:  kptr (8 B)
//   offset 8:  val  (4 B)
//   offset 12: tag  (2 B)
//   offset 14: klen (2 B)
//   offset 16: reserved/padding (16 B)
//
// Pointer first avoids the 4‑B padding Go would otherwise insert before it.

type slot struct {
	kptr unsafe.Pointer // ↖ 8 B
	val  uint32         // ↖ 4 B
	tag  uint16         // ↖ 2 B (collision tag)
	klen uint16         // ↖ 2 B (key length)
	_    [16]byte       // future: per‑key LRU, timestamp, etc.
}

// cluster: 64‑slot array + bitmap header (uint64). 2048 B total; aligns nicely.

type cluster struct {
	bitmap uint64             // first cache‑line → cheap acquire
	_      [56]byte           // keep hot header away from slots
	slots  [clusterSlots]slot // 64 × 32 B = 2 kiB
}

// HashMap: buckets inline; arena pointer/offset; size counter.

type HashMap struct {
	buckets [bucketCnt][clustersPerBkt]cluster
	arena   []byte  // contiguous key storage
	off     uintptr // next‑free offset into arena
	size    uint32  // unique key count (atomically updated)
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Constructor                                                                  */
/*──────────────────────────────────────────────────────────────────────────────*/

func New() *HashMap {
	return &HashMap{arena: make([]byte, keyArenaSize)}
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Key‑copy helper                                                              */
/*──────────────────────────────────────────────────────────────────────────────*/

func (h *HashMap) allocKeyCopy(k string) unsafe.Pointer {
	n := len(k)
	if h.off+uintptr(n) > uintptr(len(h.arena)) {
		panic("pairidx: key arena full – increase keyArenaSize")
	}
	dst := h.arena[h.off : h.off+uintptr(n)]
	copy(dst, k)
	h.off += uintptr(n)
	return unsafe.Pointer(&dst[0])
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Get (reader)                                                                 */
/*──────────────────────────────────────────────────────────────────────────────*/

func (h *HashMap) Get(k string) unsafe.Pointer {
	kptr := unsafe.StringData(k)
	klen := uint16(len(k))

	h32, tag := crc32cMix(k)
	bi := (h32 >> hashShift) & bucketMask
	ci := (h32 >> (hashShift + bucketShift)) & clMask
	cl := &h.buckets[bi][ci]

	bm := atomic.LoadUint64(&cl.bitmap) // acquire fence
	if bm == 0 {
		return nil
	}

	start := int(h32) & slotMask
	rbm := ((bm >> start) | (bm << (clusterSlots - start)))

	for rbm != 0 {
		tz := bits.TrailingZeros64(rbm)
		idx := (tz + start) & slotMask
		s := &cl.slots[idx]

		if s.tag == tag && s.klen == klen &&
			sameKey64(unsafe.Pointer(kptr), s.kptr, klen) {
			return unsafe.Pointer(&s.val)
		}
		rbm &= rbm - 1 // clear LSB and continue scan
	}
	return nil
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Put (writer)                                                                 */
/*──────────────────────────────────────────────────────────────────────────────*/

func (h *HashMap) Put(k string, v uint32) unsafe.Pointer {
	kptr := unsafe.StringData(k)
	klen := uint16(len(k))

	h32, tag := crc32cMix(k)
	bi := (h32 >> hashShift) & bucketMask
	ci := (h32 >> (hashShift + bucketShift)) & clMask
	cl := &h.buckets[bi][ci]

	bm := cl.bitmap // relaxed; writer is sole mutator
	start := int(h32) & slotMask
	scan := ((fullMask >> start) | (fullMask << (clusterSlots - start)))

	for scan != 0 {
		tz := bits.TrailingZeros64(scan)
		idx := (tz + start) & slotMask
		bit := uint64(1) << uint(idx)
		s := &cl.slots[idx]

		if bm&bit == 0 { // empty slot → fresh insert
			s.kptr = h.allocKeyCopy(k)
			s.val = v
			s.tag = tag
			s.klen = klen
			atomic.OrUint64(&cl.bitmap, bit) // release publish
			atomic.AddUint32(&h.size, 1)
			return unsafe.Pointer(&s.val)
		}
		if s.tag == tag && s.klen == klen &&
			sameKey64(unsafe.Pointer(kptr), s.kptr, klen) {
			s.val = v // overwrite in place, no arena copy
			return unsafe.Pointer(&s.val)
		}
		scan &= scan - 1 // continue probing
	}
	panic("pairidx: cluster full – rehash or enlarge map")
}
