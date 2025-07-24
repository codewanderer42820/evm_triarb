// ════════════════════════════════════════════════════════════════════════════════════════════════
// CompactQueue128 - TRULY MINIMAL with exact bitmap logic clone
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Strategy: Use IDENTICAL bit manipulation as original, but with truly minimal storage
// ════════════════════════════════════════════════════════════════════════════════════════════════

package compactqueue128

import (
	"math/bits"
	"unsafe"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONFIGURATION CONSTANTS - Keep originals for bit math compatibility
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	BucketCount = 128 // Only 128 priorities supported

	// Keep original constants for bit manipulation compatibility
	GroupCount = 64 // Original, but we only use group 0
	LaneCount  = 64 // Original, but we only use lanes 0-1
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type Handle uint64

const nilIdx Handle = ^Handle(0)

//go:notinheap
//go:align 32
type Entry struct {
	Tick int64  // 8B - Active tick or -1 if free
	Data uint64 // 8B - Compact payload
	Next Handle // 8B - Next in chain
	Prev Handle // 8B - Previous in chain
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TRULY MINIMAL STORAGE - Only what we actually need
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// groupBlock - minimal version that only stores what we need
//
//go:notinheap
//go:align 64
type groupBlock struct {
	l1Summary uint64    // 8B - Only bits 63,62 used (lanes 0,1)
	l2        [2]uint64 // 16B - Only l2[0] and l2[1] used
	_         [40]byte  // 40B - Padding to 64 bytes
}

//go:notinheap
//go:align 64
type CompactQueue128 struct {
	// Hot path metadata (24 bytes)
	summary uint64  // 8B - Only bit 63 used (group 0)
	size    int     // 8B - Current entry count
	arena   uintptr // 8B - Base pointer to shared pool

	// Padding to cache line boundary (40 bytes)
	_ [40]byte

	// Truly minimal storage - only what we actually need
	buckets [BucketCount]Handle // 128 buckets × 8B = 1024B
	groups  [1]groupBlock       // 1 group × 64B = 64B
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New(arena unsafe.Pointer) *CompactQueue128 {
	q := &CompactQueue128{arena: uintptr(arena)}

	for i := range q.buckets {
		q.buckets[i] = nilIdx
	}

	return q
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MEMORY ACCESS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) entry(h Handle) *Entry {
	return (*Entry)(unsafe.Pointer(q.arena + uintptr(h)<<5))
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// QUERY OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) Size() int {
	return q.size
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) Empty() bool {
	return q.size == 0
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INTERNAL OPERATIONS - IDENTICAL BIT LOGIC TO ORIGINAL
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) unlink(h Handle) {
	entry := q.entry(h)
	b := Handle(entry.Tick)

	// IDENTICAL doubly-linked list logic
	if entry.Prev != nilIdx {
		q.entry(entry.Prev).Next = entry.Next
	} else {
		q.buckets[b] = entry.Next
	}

	if entry.Next != nilIdx {
		q.entry(entry.Next).Prev = entry.Prev
	}

	// IDENTICAL bitmap logic to original
	if q.buckets[b] == nilIdx {
		// IDENTICAL bit decomposition
		g := uint64(entry.Tick) >> 12       // Group index (always 0 for 0-127)
		l := (uint64(entry.Tick) >> 6) & 63 // Lane index (0 or 1 for 0-127)
		bb := uint64(entry.Tick) & 63       // Bucket index

		gb := &q.groups[g]          // Always groups[0]
		gb.l2[l] &^= 1 << (63 - bb) // Clear bucket bit

		if gb.l2[l] == 0 { // Lane became empty
			gb.l1Summary &^= 1 << (63 - l) // Clear lane bit
			if gb.l1Summary == 0 {         // Group became empty
				q.summary &^= 1 << (63 - g) // Clear group bit
			}
		}
	}

	// Mark as unlinked
	entry.Next = nilIdx
	entry.Prev = nilIdx
	entry.Tick = -1
	q.size--
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) linkAtHead(h Handle, tick int64) {
	entry := q.entry(h)
	b := Handle(uint64(tick))

	// IDENTICAL insertion logic
	entry.Tick = tick
	entry.Prev = nilIdx
	entry.Next = q.buckets[b]
	if entry.Next != nilIdx {
		q.entry(entry.Next).Prev = h
	}
	q.buckets[b] = h

	// IDENTICAL bitmap logic to original
	g := uint64(tick) >> 12       // Group index (always 0 for 0-127)
	l := (uint64(tick) >> 6) & 63 // Lane index (0 or 1 for 0-127)
	bb := uint64(tick) & 63       // Bucket index

	gb := &q.groups[g]            // Always groups[0]
	gb.l2[l] |= 1 << (63 - bb)    // Set bucket bit
	gb.l1Summary |= 1 << (63 - l) // Set lane bit
	q.summary |= 1 << (63 - g)    // Set group bit

	q.size++
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC OPERATIONS - IDENTICAL TO ORIGINAL
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) Push(tick int64, h Handle, val uint64) {
	entry := q.entry(h)

	// IDENTICAL logic to original
	if entry.Tick == tick {
		entry.Data = val
		return
	}

	if entry.Tick >= 0 {
		q.unlink(h)
	}
	q.linkAtHead(h, tick)
	entry.Data = val
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) PeepMin() (Handle, int64, uint64) {
	// IDENTICAL bitmap traversal to original
	g := bits.LeadingZeros64(q.summary)
	gb := &q.groups[g] // Always groups[0]
	l := bits.LeadingZeros64(gb.l1Summary)
	t := bits.LeadingZeros64(gb.l2[l])

	// IDENTICAL bucket reconstruction
	b := Handle((uint64(g) << 12) | (uint64(l) << 6) | uint64(t))
	h := q.buckets[b]

	entry := q.entry(h)
	return h, entry.Tick, entry.Data
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) MoveTick(h Handle, newTick int64) {
	entry := q.entry(h)

	if entry.Tick == newTick {
		return
	}

	q.unlink(h)
	q.linkAtHead(h, newTick)
}

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) UnlinkMin(h Handle) {
	q.unlink(h)
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MEMORY USAGE: Truly minimal while maintaining exact bitmap compatibility
// ═══════════════════════════════════════════════════════════════════════════════════════════════
// Core struct: 64B (hot path + padding)
// buckets[128]: 1024B (128 × 8B handles)
// groups[1]: 64B (1 × 64B groupBlock)
// groupBlock contains:
//   - l1Summary: 8B (only bits 63,62 used for lanes 0,1)
//   - l2[2]: 16B (l2[0] for ticks 0-63, l2[1] for ticks 64-127)
//   - padding: 40B
//
// Total: ~1.1KB vs 37KB+ for original = 97% memory reduction
// ═══════════════════════════════════════════════════════════════════════════════════════════════
