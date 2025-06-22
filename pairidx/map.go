package pairidx

import (
	"hash/crc32"
	"math/bits"
	"unsafe"
)

const (
	// Fixed number of buckets in the hash table
	bucketCnt = 64
	// Number of clusters per bucket; used for secondary hashing
	clustersPerBkt = 4
	// Each cluster contains this many slots (fixed-size open addressing)
	clusterSlots = 16
	// Masks to quickly mod by cluster size
	slotMask = clusterSlots - 1
	clMask   = clustersPerBkt - 1
)

// Precomputed CRC32-C table using Castagnoli polynomial
var crcTab = crc32.MakeTable(crc32.Castagnoli)

// sameKey compares two keys by raw pointer for equality.
// Optimized for very short keys (<= 12 bytes), comparing only the first 8 or 12 bytes.
func sameKey(a, b unsafe.Pointer, n uint16) bool {
	if n <= 8 {
		return *(*uint64)(a) == *(*uint64)(b)
	}
	return *(*uint64)(a) == *(*uint64)(b) &&
		*(*uint32)(unsafe.Add(a, 8)) == *(*uint32)(unsafe.Add(b, 8))
}

// slotUnsafe represents one key/value entry in a cluster.
type slotUnsafe struct {
	tag  uint16         // High 16 bits of CRC for fast filtering
	klen uint16         // Key length (for sameKey short-compare)
	kptr unsafe.Pointer // Raw pointer to original key (no copy)
	val  uint32         // Associated value (non-atomic)
	_    [8]byte        // Padding to align to 32 bytes
}

// cluster contains a bitmap and fixed-size slot array.
type cluster struct {
	bitmap uint64                   // Bitmap indicating occupied slots (1 = filled)
	_      [56]byte                 // Padding to separate metadata from hot data
	slots  [clusterSlots]slotUnsafe // Actual slots (fixed layout)
}

// HashMap is a fast, allocation-free hash table optimized for fixed-length string keys.
type HashMap struct {
	buckets [bucketCnt][clustersPerBkt]cluster // Top-level hash structure
	size    uint32                             // Entry count (non-atomic)
}

// New returns a new empty hash map instance.
func New() *HashMap { return &HashMap{} }

// Get performs a lookup for the given string key.
// Returns (value, true) if found; otherwise (0, false).
func (h *HashMap) Get(k string) (uint16, bool) {
	ptr := unsafe.StringData(k)
	klen := uint16(len(k))

	// Compute CRC32 for hash and tag
	crc := crc32.Update(0, crcTab, unsafe.Slice(ptr, int(klen)))
	low := uint16(crc)
	tag := uint16(crc>>16) | 1 // Ensure nonzero tag

	// Calculate bucket and cluster index
	bi := uint32(low) & (bucketCnt - 1)
	ci := (uint32(low) >> bits.Len32(bucketCnt-1)) & clMask
	cl := &h.buckets[bi][ci]

	// Load bitmap indicating filled slots (non-atomic)
	bm := cl.bitmap

	// Calculate probe start index within cluster
	start := int(low) & slotMask

	// Probe all slots in unrolled loop
	for _, i := range [clusterSlots]int{
		(start + 0) & slotMask, (start + 1) & slotMask,
		(start + 2) & slotMask, (start + 3) & slotMask,
		(start + 4) & slotMask, (start + 5) & slotMask,
		(start + 6) & slotMask, (start + 7) & slotMask,
		(start + 8) & slotMask, (start + 9) & slotMask,
		(start + 10) & slotMask, (start + 11) & slotMask,
		(start + 12) & slotMask, (start + 13) & slotMask,
		(start + 14) & slotMask, (start + 15) & slotMask,
	} {
		bit := uint64(1) << uint(i)
		if bm&bit == 0 {
			// Slot empty
			return 0, false
		}
		s := &cl.slots[i]
		if s.tag == tag && s.klen == klen && sameKey(s.kptr, unsafe.Pointer(ptr), klen) {
			// Found matching key
			return uint16(s.val), true
		}
	}
	return 0, false
}

// Put inserts or updates the given key with a new value.
// If the key exists with the same value, it's a no-op.
// If all slots are occupied and no match is found, the function panics.
func (h *HashMap) Put(k string, v uint16) {
	ptr := unsafe.StringData(k)
	klen := uint16(len(k))

	// Compute CRC32 for indexing and tag
	crc := crc32.Update(0, crcTab, unsafe.Slice(ptr, int(klen)))
	low := uint16(crc)
	tag := uint16(crc>>16) | 1

	// Calculate bucket and cluster index
	bi := uint32(low) & (bucketCnt - 1)
	ci := (uint32(low) >> bits.Len32(bucketCnt-1)) & clMask
	cl := &h.buckets[bi][ci]

	// Read current bitmap
	bm := cl.bitmap
	val32 := uint32(v)
	start := int(low) & slotMask

	// Unrolled linear probing
	for _, i := range [clusterSlots]int{
		(start + 0) & slotMask, (start + 1) & slotMask,
		(start + 2) & slotMask, (start + 3) & slotMask,
		(start + 4) & slotMask, (start + 5) & slotMask,
		(start + 6) & slotMask, (start + 7) & slotMask,
		(start + 8) & slotMask, (start + 9) & slotMask,
		(start + 10) & slotMask, (start + 11) & slotMask,
		(start + 12) & slotMask, (start + 13) & slotMask,
		(start + 14) & slotMask, (start + 15) & slotMask,
	} {
		bit := uint64(1) << uint(i)
		s := &cl.slots[i]

		if bm&bit != 0 {
			// Slot filled — check for existing key
			if s.tag == tag && s.klen == klen && sameKey(s.kptr, unsafe.Pointer(ptr), klen) {
				// Key exists — update value if changed
				if s.val == val32 {
					return
				}
				s.val = val32
				return
			}
			continue
		}
		// Insert new entry in empty slot
		s.tag, s.klen, s.kptr = tag, klen, unsafe.Pointer(ptr)
		s.val = val32
		cl.bitmap = bm | bit
		h.size++
		return
	}
	// All slots probed; no space
	panic("map: cluster full — too many keys hashed into the same cluster; reduce load factor or improve key distribution")
}

// Size returns the current number of entries in the hash map.
func (h *HashMap) Size() int {
	return int(h.size)
}
