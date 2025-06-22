// ring.go
//
// Lock-free single-producer/single-consumer ring buffer tuned for <10 ns
// hand-off latency on modern CPUs.  The structure deliberately separates
// producer and consumer fields with full cache-lines to eliminate
// false-sharing, and each slot carries a sequence number so Push/Pop can
// be wait-free without additional atomics.

package ring

import "unsafe"

// slot couples a payload pointer with its sequence stamp.
type slot struct {
	seq uint64         // position in the sequence space
	ptr unsafe.Pointer // user payload
}

// Ring is a fixed-capacity circular buffer dedicated to one producer and
// one consumer.  Accessors are nosplit so they stay callable from hot
// assembly loops if needed.
type Ring struct {
	_    [64]byte // producer head isolated on its own cache-line
	head uint64
	//lint:ignore U1000 padding to keep head & tail on different cache-lines
	_pad1 [64]byte
	tail  uint64
	//lint:ignore U1000 padding to keep hot fields from colliding with metadata
	_pad2 [64]byte
	mask  uint64
	buf   []slot
}

// New allocates a ring whose size must be a power-of-two; otherwise it
// panics so that the bit-masking arithmetic stays valid.
func New(size int) *Ring {
	if size <= 0 || size&(size-1) != 0 {
		panic("ring: size must be >0 and a power of two")
	}
	r := &Ring{
		mask: uint64(size - 1),
		buf:  make([]slot, size),
	}
	for i := range r.buf {
		r.buf[i].seq = uint64(i)
	}
	return r
}

// Push enqueues p, returning false if the buffer is full.
//
//go:nosplit
func (r *Ring) Push(p unsafe.Pointer) bool {
	t := r.tail
	s := &r.buf[t&r.mask]
	if loadAcquireUint64(&s.seq) != t {
		return false // consumer has not yet reclaimed the slot
	}
	s.ptr = p
	storeReleaseUint64(&s.seq, t+1)
	r.tail = t + 1
	return true
}

// Pop dequeues one pointer or nil if the buffer is empty.
//
//go:nosplit
func (r *Ring) Pop() unsafe.Pointer {
	h := r.head
	s := &r.buf[h&r.mask]
	if loadAcquireUint64(&s.seq) != h+1 {
		return nil // producer has not yet published to the slot
	}
	p := s.ptr
	storeReleaseUint64(&s.seq, h+uint64(len(r.buf)))
	r.head = h + 1
	return p
}

// PopWait busy-spins until an item becomes available.
//
//go:nosplit
func (r *Ring) PopWait() unsafe.Pointer {
	for {
		if p := r.Pop(); p != nil {
			return p
		}
		cpuRelax()
	}
}
