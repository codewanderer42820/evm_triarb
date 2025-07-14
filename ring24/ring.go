// ring24 - Lock-free SPSC ring buffer for ISR-grade communication
package ring24

import (
	"sync/atomic"
)

// slot - 64-byte ring entry (full cache line)
//
//go:notinheap
//go:align 64
type slot struct {
	val [24]byte // Fixed payload
	seq uint64   // Availability signal
	_   [32]byte // Cache line isolation
}

// Ring - Cache-optimized SPSC buffer with isolation padding
//
//go:notinheap
//go:align 64
type Ring struct {
	_    [64]byte // Cache line isolation
	head uint64   // Consumer position

	_    [56]byte // Cache line isolation
	tail uint64   // Producer position

	_ [56]byte // Reserved space

	// Configuration (cache line 4)
	mask uint64 // Size - 1 for modulo
	step uint64 // Size for sequence reset
	buf  []slot // Backing buffer

	_ [24]byte // Tail padding
}

// New creates ring with power-of-2 capacity
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func New(size int) *Ring {
	if size <= 0 || size&(size-1) != 0 {
		panic("ring: size must be >0 and power of two")
	}

	r := &Ring{
		mask: uint64(size - 1),
		step: uint64(size),
		buf:  make([]slot, size),
	}

	// Initialize sequence numbers
	for i := range r.buf {
		r.buf[i].seq = uint64(i)
	}

	return r
}

// Push attempts lock-free enqueue
// Returns: true on success, false if full
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (r *Ring) Push(val *[24]byte) bool {
	t := r.tail
	s := &r.buf[t&r.mask]

	// Check slot available
	if atomic.LoadUint64(&s.seq) != t {
		return false
	}

	// Copy payload
	s.val = *val

	// Signal availability
	atomic.StoreUint64(&s.seq, t+1)

	// Advance producer
	r.tail = t + 1
	return true
}

// Pop attempts lock-free dequeue
// Returns: payload pointer or nil if empty
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (r *Ring) Pop() *[24]byte {
	h := r.head
	s := &r.buf[h&r.mask]

	// Check data available
	if atomic.LoadUint64(&s.seq) != h+1 {
		return nil
	}

	// Extract payload
	val := &s.val

	// Reset for reuse
	atomic.StoreUint64(&s.seq, h+r.step)

	// Advance consumer
	r.head = h + 1
	return val
}

// PopWait blocks until data available
// ⚠️  WARNING: High CPU usage during wait
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
		cpuRelax()
	}
}
