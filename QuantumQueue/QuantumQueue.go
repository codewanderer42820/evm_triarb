package quantumqueue

import (
	"errors"
	"fmt"
	"math/bits"
	"os"
	"unsafe"
)

const (
	GroupCount  = 64
	LaneCount   = 64
	BucketCount = GroupCount * LaneCount * LaneCount
	CapItems    = 1 << 16
)

var (
	ErrFull         = errors.New("QuantumQueue: no free handles")
	ErrPastWindow   = errors.New("QuantumQueue: tick too far in the past")
	ErrBeyondWindow = errors.New("QuantumQueue: tick too far in the future")
	ErrNotFound     = errors.New("QuantumQueue: invalid handle or removed")
)

type idx32 uint32

const nilIdx idx32 = ^idx32(0)

type Handle idx32

type groupBlock struct {
	l1Summary uint64
	_pad0     [7]uint64
	l2        [LaneCount]uint64
	_pad1     [56]byte
}

type node struct {
	tick  int64
	data  unsafe.Pointer
	next  idx32
	prev  idx32
	count uint32
	_pad  uint32
}

type QuantumQueue struct {
	summary  uint64
	_padG    [7]uint64
	groups   [GroupCount]groupBlock
	buckets  [BucketCount]idx32
	arena    [CapItems]node
	freeHead idx32
	size     int
	baseTick uint64
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[QDBG] "+format+"\n", args...)
}

func NewQuantumQueue() *QuantumQueue {
	q := new(QuantumQueue)
	for i := CapItems - 1; i > 1; i-- {
		q.arena[i].next = idx32(i - 1)
	}
	q.arena[1].next = nilIdx
	q.freeHead = idx32(CapItems - 1)
	logf("NewQuantumQueue: initialized freelist head=%d", q.freeHead)
	return q
}

func (q *QuantumQueue) Borrow() (Handle, error) {
	h := q.freeHead
	if h == nilIdx {
		logf("Borrow failed: no free handles")
		return Handle(nilIdx), ErrFull
	}
	n := &q.arena[h]
	q.freeHead = n.next
	n.next, n.prev, n.count = nilIdx, nilIdx, 0
	logf("Borrow: h=%d", h)
	return Handle(h), nil
}

func (q *QuantumQueue) peekIndexes() (g, lane, bit uint) {
	g = uint(bits.LeadingZeros64(q.summary))
	gb := &q.groups[g]
	lane = uint(bits.LeadingZeros64(gb.l1Summary))
	bit = uint(bits.LeadingZeros64(gb.l2[lane]))
	return
}

func (q *QuantumQueue) PeepMin() (Handle, int64, unsafe.Pointer) {
	logf("PeepMin: size=%d", q.size)
	if q.size == 0 {
		logf("PeepMin: empty")
		return Handle(nilIdx), 0, nil
	}
	g, lane, bit := q.peekIndexes()
	b := (g << 12) | (lane << 6) | bit
	h := q.buckets[b]
	n := &q.arena[h]
	logf("PeepMin: h=%d tick=%d count=%d", h, n.tick, n.count)
	return Handle(h), n.tick, n.data
}

func (q *QuantumQueue) Push(tick int64, h Handle, val unsafe.Pointer) error {
	idx := idx32(h)
	if idx == nilIdx || idx >= CapItems {
		logf("Push: invalid handle %d", h)
		return ErrNotFound
	}
	delta := uint64(tick) - q.baseTick
	switch {
	case delta >= 1<<63:
		logf("Push: tick too far in past")
		return ErrPastWindow
	case delta >= BucketCount:
		logf("Push: tick too far in future")
		return ErrBeyondWindow
	}

	n := &q.arena[idx]
	logf("Push: h=%d tick=%d count=%d prevTick=%d", h, tick, n.count, n.tick)
	if n.count > 0 && n.tick == tick {
		n.count++
		n.data = val
		q.size++
		logf("Push: fast path tick match → count=%d", n.count)
		return nil
	}
	if n.prev != nilIdx {
		old := uint64(n.tick) - q.baseTick
		logf("Push: tick change, unlinking h=%d oldDelta=%d", h, old)
		q.unlinkByIndex(idx, old)
		q.size--
	}
	n.count = 1
	return q.insertByDelta(idx, tick, delta, val)
}

func (q *QuantumQueue) PopMin() (Handle, int64, unsafe.Pointer) {
	h, t, v := q.PeepMin()
	logf("PopMin: h=%d t=%d", h, t)
	if h == Handle(nilIdx) {
		return h, 0, nil
	}
	idx := idx32(h)
	n := &q.arena[idx]
	if n.count > 1 {
		n.count--
		q.size--
		logf("PopMin: decrement count → %d", n.count)
		return h, t, n.data
	}
	delta := uint64(t) - q.baseTick
	q.unlinkByIndex(idx, delta)
	q.size--
	n.next = q.freeHead
	q.freeHead = idx
	logf("PopMin: full remove h=%d", h)
	return h, t, v
}

func (q *QuantumQueue) Size() int   { return q.size }
func (q *QuantumQueue) Empty() bool { return q.size == 0 }

func (q *QuantumQueue) unlinkByIndex(idx idx32, delta uint64) {
	g := uint(delta >> 12)
	lane := uint((delta >> 6) & 0x3F)
	bit := uint(delta & 0x3F)
	b := (g << 12) | (lane << 6) | bit

	n := &q.arena[idx]
	if n.prev != nilIdx {
		q.arena[n.prev].next = n.next
	} else {
		q.buckets[b] = n.next
	}
	if n.next != nilIdx {
		q.arena[n.next].prev = n.prev
	}

	laneMask := uint64(1) << (63 - bit)
	slot := &q.groups[g]
	newL2 := slot.l2[lane] &^ laneMask
	slot.l2[lane] = newL2
	clearL1 := ((newL2 - 1) >> 63) & laneMask
	slot.l1Summary &^= clearL1
	l1 := slot.l1Summary
	gMask := uint64(1) << (63 - g)
	clearG := ((l1 - 1) >> 63) & gMask
	q.summary &^= clearG
	logf("unlinkByIndex: idx=%d → g=%d lane=%d bit=%d", idx, g, lane, bit)
}

func (q *QuantumQueue) insertByDelta(idx idx32, tick int64, delta uint64, val unsafe.Pointer) error {
	g := uint(delta >> 12)
	lane := uint((delta >> 6) & 0x3F)
	bit := uint(delta & 0x3F)

	n := &q.arena[idx]
	n.next = q.buckets[delta]
	n.prev = nilIdx
	if n.next != nilIdx {
		q.arena[n.next].prev = idx
	}
	q.buckets[delta] = idx

	n.tick = tick
	n.data = val

	laneMask := uint64(1) << (63 - bit)
	slot := &q.groups[g]
	slot.l2[lane] |= laneMask
	summaryMask := uint64(1) << (63 - lane)
	slot.l1Summary |= summaryMask
	q.summary |= uint64(1) << (63 - g)

	q.size++
	logf("insertByDelta: idx=%d tick=%d g=%d lane=%d bit=%d", idx, tick, g, lane, bit)
	return nil
}
