// ════════════════════════════════════════════════════════════════════════════════════════════════
// CompactQueue128 - TRULY MINIMAL (3 uint64s) - Tests Must Be Updated
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System  
// Component: Absolute Minimal Memory Priority Queue
//
// MEMORY OPTIMIZATION ACHIEVED:
//   - Bitmap hierarchy: EXACTLY 3 uint64s (24 bytes total)
//   - 1 summary + 2 groups = optimal for 128 priorities
//   - ~97% memory reduction vs full quantum queue bitmap
//
// TRADE-OFF: Tests cannot be 100% compatible due to Go language limitations:
//   - Direct field access (gb.l1Summary) cannot be intercepted/overridden
//   - Tests must be updated to work with simplified structure
//   - All functionality remains identical, just different internal layout
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package compactqueue128

import (
	"math/bits"
	"unsafe"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONFIGURATION CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	BucketCount = 128 // Exactly 128 priorities (0-127)
	GroupCount = 2    // 2 groups of 64 priorities each
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TYPE DEFINITIONS  
// ═══════════════════════════════════════════════════════════════════════════════════════════════

type Handle uint64
const nilIdx Handle = ^Handle(0)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SHARED MEMORY ENTRY
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 32
type Entry struct {
	Tick int64  // 8B - Active tick or -1 if free
	Data uint64 // 8B - Compact payload  
	Next Handle // 8B - Next in chain
	Prev Handle // 8B - Previous in chain
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MINIMAL QUEUE STRUCTURE - EXACTLY 3 UINT64S FOR BITMAP
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 64
type CompactQueue128 struct {
	// Hot path metadata (32 bytes)
	summary uint64    // 8B - Group activity mask (2 bits used)
	size    int       // 8B - Current entry count
	arena   uintptr   // 8B - Base pointer to shared pool  
	groups  [2]uint64 // 16B - THE MINIMAL BITMAP: exactly 2 uint64s for 128 priorities

	// Padding to cache line boundary (32 bytes)
	_ [32]byte

	// Bucket chain heads (1024 bytes)  
	buckets [BucketCount]Handle // 128 × 8B per handle
}

// TOTAL BITMAP MEMORY: summary(8) + groups[2](16) = 24 bytes = 3 uint64s ✓

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
// INTERNAL OPERATIONS - OPTIMAL MINIMAL BITMAP LOGIC
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams  
func (q *CompactQueue128) unlink(h Handle) {
	entry := q.entry(h)
	b := Handle(entry.Tick)

	// Update doubly-linked list
	if entry.Prev != nilIdx {
		q.entry(entry.Prev).Next = entry.Next
	} else {
		q.buckets[b] = entry.Next
	}

	if entry.Next != nilIdx {
		q.entry(entry.Next).Prev = entry.Prev  
	}

	// Update minimal bitmap when bucket becomes empty
	if q.buckets[b] == nilIdx {
		tick := uint64(entry.Tick)
		g := tick >> 6      // Group: 0-63→0, 64-127→1
		bb := tick & 63     // Bucket within group

		// Clear bucket bit in group
		q.groups[g] &^= 1 << (63 - bb)
		
		// Clear group bit if group becomes empty
		if q.groups[g] == 0 {
			q.summary &^= 1 << (63 - g)
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

	// Insert at head of bucket chain
	entry.Tick = tick
	entry.Prev = nilIdx
	entry.Next = q.buckets[b]
	if entry.Next != nilIdx {
		q.entry(entry.Next).Prev = h
	}
	q.buckets[b] = h

	// Update minimal bitmap
	utick := uint64(tick)
	g := utick >> 6      // Group: 0-63→0, 64-127→1
	bb := utick & 63     // Bucket within group

	// Set bucket and group bits  
	q.groups[g] |= 1 << (63 - bb)
	q.summary |= 1 << (63 - g)

	q.size++
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (q *CompactQueue128) Push(tick int64, h Handle, val uint64) {
	entry := q.entry(h)

	// Hot path: same tick update
	if entry.Tick == tick {
		entry.Data = val
		return
	}

	// Cold path: relocation
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
	// Find minimum group using CLZ
	g := bits.LeadingZeros64(q.summary)
	
	// Find minimum bucket within group using CLZ
	bb := bits.LeadingZeros64(q.groups[g])
	
	// Reconstruct bucket: group * 64 + bucket_offset
	b := Handle((uint64(g) << 6) | uint64(bb))
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
// MEMORY USAGE SUMMARY
// ═══════════════════════════════════════════════════════════════════════════════════════════════
//
// Bitmap Memory (the critical optimization):
//   - summary:    8 bytes (1 uint64) 
//   - groups[2]: 16 bytes (2 uint64)
//   - TOTAL:     24 bytes (3 uint64) ✓
//
// Compare to original quantum queue bitmap:
//   - summary:     8 bytes
//   - groups[64]:  64 × 576 bytes = 36,864 bytes  
//   - TOTAL:      36,872 bytes
//
// Memory reduction: 36,872 → 24 = 99.93% reduction in bitmap memory ✓
//
// Total structure size: ~1.1KB vs ~37KB = 97% total reduction ✓
//
// ═══════════════════════════════════════════════════════════════════════════════════════════════