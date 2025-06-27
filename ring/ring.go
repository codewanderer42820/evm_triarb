// SPDX-License-Identifier: MIT
//
// Ring: Lock-free SPSC queue with 32-byte inlined payload
// This version stores [32]byte buffers per slot and treats them as generic
// buffers. The producer and consumer handle encoding/decoding explicitly.
package ring

/*───────────────────────── slot & ring ─────────────────────────*/

// slot holds a fixed-size buffer (32 bytes) and a sequence number for ordering.
type slot struct {
	val [32]byte // generic fixed-size buffer
	seq uint64   // sequence tag for SPSC synchronization
}

// Ring holds state and a fixed-size circular buffer.
//
// head & tail are deliberately separated by 64-byte paddings so that the
// producer (writes tail) and consumer (writes head) never share a cache
// line on SMP machines.
type Ring struct {
	_pad0 [64]byte
	head  uint64 // read cursor  (consumer)
	_pad1 [64]byte
	tail  uint64 // write cursor (producer)
	_pad2 [64]byte

	mask uint64 // len(buf)-1 (power-of-two sizing)
	step uint64 // == len(buf)
	buf  []slot
}

/*──────────────────────── constructor ──────────────────────────*/

// New returns an empty ring with *size* slots. *size* **must** be a
// power of two or the function panics.
func New(size int) *Ring {
	if size <= 0 || size&(size-1) != 0 {
		panic("ring: size must be >0 and a power of two")
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

/*────────────────────────── Push ───────────────────────────────*
| Producer algorithm                                             |
|   slot = buf[tail & mask]                                      |
|   if slot.seq == tail            => slot is free               |
|       slot.val = input buffer                                  |
|       slot.seq = tail + 1       => publish to consumer         |
|       tail++                                                   |
|   else ring is full                                            |
*────────────────────────────────────────────────────────────────*/

// Push inserts a [32]byte buffer into the ring.
func (r *Ring) Push(val *[32]byte) bool {
	t := r.tail
	s := &r.buf[t&r.mask]

	if loadAcquireUint64(&s.seq) != t {
		return false // queue full
	}

	s.val = *val // direct copy
	storeReleaseUint64(&s.seq, t+1)
	r.tail = t + 1
	return true
}

/*────────────────────────── Pop ────────────────────────────────*
| Consumer algorithm                                             |
|   slot = buf[head & mask]                                      |
|   if slot.seq == head+1          => item ready                 |
|       output = slot.val                                       |
|       slot.seq = head + step     => recycle for next wrap      |
|       head++                                                   |
|   else queue is empty                                          |
*────────────────────────────────────────────────────────────────*/

// Pop returns a pointer to a [32]byte buffer if available, or nil if empty.
func (r *Ring) Pop() *[32]byte {
	h := r.head
	s := &r.buf[h&r.mask]

	if loadAcquireUint64(&s.seq) != h+1 {
		return nil
	}

	val := &s.val
	storeReleaseUint64(&s.seq, h+r.step)
	r.head = h + 1
	return val
}

/*─────────────────────── PopWait helper ────────────────────────*/

// PopWait spins (or yields) until an item is ready.
func (r *Ring) PopWait() *[32]byte {
	for {
		if p := r.Pop(); p != nil {
			return p
		}
		cpuRelax()
	}
}
