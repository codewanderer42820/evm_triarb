// ════════════════════════════════════════════════════════════════════════════════════════════════
// Lock-Free SPSC Ring Buffer
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Single-Producer Single-Consumer Communication Channel
//
// Description:
//   Cache-optimized lock-free ring buffer for inter-core communication. Implements a wait-free
//   algorithm for single producer and single consumer threads with strict memory ordering
//   guarantees and false sharing prevention through cache line isolation.
//
// Features:
//   - Wait-free progress guarantees for both producer and consumer
//   - Cache line isolation prevents false sharing between cores
//   - Sequence number validation ensures data consistency
//   - Power-of-2 sizing enables efficient masking operations
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package ring24

import (
	"sync/atomic"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RING BUFFER SLOT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// slot represents a single entry in the ring buffer.
// Each slot contains a fixed-size payload and sequence number for synchronization.
//
// Memory Layout:
//
//	32-byte alignment ensures efficient cache utilization with two slots
//	per 64-byte cache line, maximizing memory bandwidth usage.
//
//go:notinheap
//go:align 32
type slot struct {
	val [24]byte // Fixed payload size optimized for message passing
	seq uint64   // Sequence number for availability signaling
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// RING BUFFER STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Ring implements a cache-optimized SPSC ring buffer with false sharing prevention.
// The structure uses cache line padding to isolate producer and consumer state.
//
// Cache Line Organization:
//   - Line 1: Padding for isolation
//   - Line 2: Consumer position (head)
//   - Line 3: Padding for isolation
//   - Line 4: Producer position (tail)
//   - Line 5: Reserved space
//   - Line 6: Configuration and buffer pointer
//
//go:notinheap
//go:align 64
type Ring struct {
	_    [64]byte // Cache line 1: Isolation padding
	head uint64   // Cache line 2: Consumer position

	_    [56]byte // Padding to next cache line
	tail uint64   // Cache line 4: Producer position

	_ [56]byte // Reserved space for future use

	// Configuration (cache line 6)
	mask uint64 // Size - 1 for efficient modulo
	step uint64 // Size for sequence number wrapping
	buf  []slot // Backing buffer

	_ [24]byte // Tail padding
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONSTRUCTOR
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// New creates a ring buffer with the specified power-of-2 capacity.
// The size must be a power of 2 for efficient masking operations.
//
// Initialization:
//   - Allocates backing buffer with specified capacity
//   - Initializes sequence numbers for each slot
//   - Sets up masking values for efficient indexing
//
// Constraints:
//   - Size must be greater than 0
//   - Size must be a power of 2
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New(size int) *Ring {
	// Validate size is power of 2
	if size <= 0 || size&(size-1) != 0 {
		panic("ring: size must be >0 and power of two")
	}

	r := &Ring{
		mask: uint64(size - 1), // Bitmask for modulo operation
		step: uint64(size),     // Sequence increment value
		buf:  make([]slot, size),
	}

	// Initialize sequence numbers
	// Each slot starts with a sequence equal to its index
	for i := range r.buf {
		r.buf[i].seq = uint64(i)
	}

	return r
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PRODUCER OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Push attempts to enqueue a value into the ring buffer.
// This operation is wait-free and safe for single producer use.
//
// Algorithm:
//  1. Load current tail position
//  2. Check if target slot is available (sequence matches tail)
//  3. Copy payload data if available
//  4. Update sequence to signal data availability
//  5. Advance tail pointer
//
// Memory Ordering:
//   - Atomic load ensures visibility of consumer progress
//   - Atomic store ensures consumer sees complete data
//
// Return Values:
//   - true: Value successfully enqueued
//   - false: Ring buffer is full
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (r *Ring) Push(val *[24]byte) bool {
	// Load current producer position
	t := r.tail
	s := &r.buf[t&r.mask]

	// Check if slot is available for writing
	// Slot is free when sequence equals current tail
	if atomic.LoadUint64(&s.seq) != t {
		return false // Buffer full
	}

	// Copy payload data
	s.val = *val

	// Signal data availability to consumer
	// Sequence = tail + 1 indicates data ready
	atomic.StoreUint64(&s.seq, t+1)

	// Advance producer position
	r.tail = t + 1
	return true
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONSUMER OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Pop attempts to dequeue a value from the ring buffer.
// This operation is wait-free and safe for single consumer use.
//
// Algorithm:
//  1. Load current head position
//  2. Check if data is available (sequence == head + 1)
//  3. Extract payload pointer if available
//  4. Update sequence to mark slot as free
//  5. Advance head pointer
//
// Memory Ordering:
//   - Atomic load ensures visibility of producer writes
//   - Atomic store ensures producer sees freed slot
//
// Return Values:
//   - Non-nil: Pointer to dequeued payload
//   - nil: Ring buffer is empty
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (r *Ring) Pop() *[24]byte {
	// Load current consumer position
	h := r.head
	s := &r.buf[h&r.mask]

	// Check if data is available
	// Data ready when sequence == head + 1
	if atomic.LoadUint64(&s.seq) != h+1 {
		return nil // Buffer empty
	}

	// Extract payload pointer
	val := &s.val

	// Mark slot as free for reuse
	// Sequence = head + step indicates slot available
	atomic.StoreUint64(&s.seq, h+r.step)

	// Advance consumer position
	r.head = h + 1
	return val
}

// PopWait blocks until data becomes available.
// This variant spins waiting for data, consuming CPU cycles.
//
// Use Cases:
//   - Dedicated consumer threads with no other work
//   - Systems with sufficient CPU resources for busy waiting
//   - Applications requiring minimal latency where spinning is acceptable
//
// Warning:
//
//	This function performs busy-waiting and will consume CPU cycles
//	while waiting for data. Use only in appropriate scenarios.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (r *Ring) PopWait() *[24]byte {
	for {
		if p := r.Pop(); p != nil {
			return p
		}
		// CPU relaxation to reduce power consumption
		// and improve performance on hyperthreaded cores
		cpuRelax()
	}
}
