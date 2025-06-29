// ring.go — Lock‑free single‑producer/single‑consumer (SPSC) ring queue
package ring32

import (
	"sync/atomic"
)

type slot struct {
	val [32]byte // fixed-size payload
	seq uint64   // slot ticket number
	_   [24]byte // pad to 64 bytes total, prevents false sharing
}

type Ring struct {
	_    [64]byte // cache-line isolation (consumer head)
	head uint64   // consumer cursor
	_    [64]byte // cache-line isolation (producer tail)
	tail uint64   // producer cursor
	_    [64]byte // further isolation

	mask uint64 // == len(buf) - 1
	step uint64 // == len(buf)
	buf  []slot // backing array of slots
}

//go:nosplit
//go:inline
func New(size int) *Ring {
	if size <= 0 || size&(size-1) != 0 {
		panic("ring: size must be >0 and power of two")
	}
	r := &Ring{
		mask: uint64(size - 1),
		step: uint64(size),
		buf:  make([]slot, size),
	}
	for i := range r.buf {
		r.buf[i].seq = uint64(i)
	}
	return r
}

//go:nosplit
//go:inline
func (r *Ring) Push(val *[32]byte) bool {
	t := r.tail
	s := &r.buf[t&r.mask]
	if atomic.LoadUint64(&s.seq) != t {
		return false
	}
	s.val = *val
	atomic.StoreUint64(&s.seq, t+1)
	r.tail = t + 1
	return true
}

//go:nosplit
//go:inline
func (r *Ring) Pop() *[32]byte {
	h := r.head
	s := &r.buf[h&r.mask]
	if atomic.LoadUint64(&s.seq) != h+1 {
		return nil
	}
	val := &s.val
	atomic.StoreUint64(&s.seq, h+r.step)
	r.head = h + 1
	return val
}

//go:nosplit
func (r *Ring) PopWait() *[32]byte {
	for {
		if p := r.Pop(); p != nil {
			return p
		}
		cpuRelax()
	}
}
