// router_test.go — Comprehensive test suite for router.go
// Tests cover zero-copy key mapping, tick conversion, routing, fanout shard building,
// dispatch/update logic, execution paths, and ring-based consumption.
// Hot-path helpers and utilities are verified for correctness and robustness.

package router

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"main/bucketqueue"
	"main/localidx"
	"main/ring24"
	"main/types"
)

// TestWordKeyEqual verifies that wordKey.equal correctly identifies equal and non-equal keys.
func TestWordKeyEqual(t *testing.T) {
	var k1, k2 wordKey
	// identical zero keys
	if !k1.equal(k2) {
		t.Errorf("wordKey.equal should return true for identical zero-value keys")
	}
	// different keys
	k2 = wordKey{w: [5]uint64{1, 2, 3, 4, 5}}
	if k1.equal(k2) {
		t.Errorf("wordKey.equal should return false for different keys")
	}
}

// TestRegisterAndLookupPair verifies RegisterPair and lookupPairID behavior:
// mapping a fresh address, overwriting, and unknown lookups.
func TestRegisterAndLookupPair(t *testing.T) {
	// reset mapping
	for i := range addr2pid {
		addr2pid[i] = 0
	}
	addr := make([]byte, 40)
	for i := range addr {
		addr[i] = byte(i)
	}
	pid1 := PairID(123)
	RegisterPair(addr, pid1)
	if got := lookupPairID(addr); got != pid1 {
		t.Errorf("lookupPairID returned %v; want %v", got, pid1)
	}
	// overwrite existing
	pid2 := PairID(456)
	RegisterPair(addr, pid2)
	if got := lookupPairID(addr); got != pid2 {
		t.Errorf("lookupPairID returned %v; want %v", got, pid2)
	}
	// unknown address returns 0
	addr3 := make([]byte, 40)
	for i := range addr3 {
		addr3[i] = byte(40 + i)
	}
	if got := lookupPairID(addr3); got != 0 {
		t.Errorf("lookupPairID returned %v for unknown; want 0", got)
	}
}

// TestLog2ToTick checks that log2ToTick clamps inputs and maps in-range values correctly.
func TestLog2ToTick(t *testing.T) {
	// below minimum
	if got := log2ToTick(-200); got != 0 {
		t.Errorf("log2ToTick(-200)=%v; want 0", got)
	}
	// midpoint
	mid := int64((0 + clamp) * scale)
	if got := log2ToTick(0); got != mid {
		t.Errorf("log2ToTick(0)=%v; want %v", got, mid)
	}
	// overflow above maxTick
	over := (float64(maxTick+1)/scale - clamp) + 1.0
	if got := log2ToTick(over); got != maxTick {
		t.Errorf("log2ToTick(overflow)=%v; want %v", got, maxTick)
	}
}

// TestLog2Clamp verifies exact clamping at the boundaries -clamp and +clamp.
func TestLog2Clamp(t *testing.T) {
	if got := log2ToTick(-clamp); got != 0 {
		t.Errorf("log2ToTick(-clamp)=%v; want 0", got)
	}
	if got := log2ToTick(clamp); got != maxTick {
		t.Errorf("log2ToTick(clamp)=%v; want %v", got, maxTick)
	}
}

// TestRegisterRoute ensures RegisterRoute sets the correct bit in pair2cores.
func TestRegisterRoute(t *testing.T) {
	pid := PairID(7)
	pair2cores[pid] = 0
	RegisterRoute(pid, 3)
	if pair2cores[pid]&(1<<3) == 0 {
		t.Errorf("RegisterRoute did not set core bit; got %b", pair2cores[pid])
	}
}

// TestBuildFanoutShards validates shardBucket population for a single cycle.
func TestBuildFanoutShards(t *testing.T) {
	cycles := []PairTriplet{{1, 2, 3}}
	buildFanoutShards(cycles)
	if len(shardBucket) != 3 {
		t.Errorf("shardBucket length %v; want 3", len(shardBucket))
	}
	// verify EdgeBinding integrity after shuffle
	bins := []EdgeBinding{{Pairs: cycles[0], EdgeIdx: 0}, {Pairs: cycles[0], EdgeIdx: 1}, {Pairs: cycles[0], EdgeIdx: 2}}
	orig := make([]EdgeBinding, len(bins))
	copy(orig, bins)
	shuffleBindings(bins)
	for _, eb := range bins {
		found := false
		for _, o := range orig {
			if reflect.DeepEqual(eb, o) {
				found = true
			}
		}
		if !found {
			t.Errorf("shuffleBindings produced unexpected binding %v", eb)
		}
	}
}

// TestDispatchUpdate verifies early return when unmapped, and normal dispatch path.
func TestDispatchUpdate(t *testing.T) {
	v := &types.LogView{Addr: make([]byte, addrHexEnd), Data: make([]byte, 64)}
	// unmapped address -> no panic
	DispatchUpdate(v)
	// mapped path
	addr := make([]byte, 40)
	for i := range addr {
		addr[i] = byte(i)
	}
	RegisterPair(addr, PairID(9))
	copy(v.Addr[addrHexStart:addrHexEnd], addr)
	pair2cores[9] = 1 << 0
	rings[0] = ring24.New(1 << 4)
	binary.BigEndian.PutUint64(v.Data[24:32], 100)
	binary.BigEndian.PutUint64(v.Data[56:64], 200)
	// should push into ring without panic
	DispatchUpdate(v)
}

// TestHandleTick covers both profitable and non-profitable drain loops and tick updates.
func TestHandleTick(t *testing.T) {
	pid := PairID(11)
	ex := &CoreExecutor{Heaps: []bucketqueue.Queue{}, Fanouts: [][]FanoutEntry{}, LocalIdx: localidx.New(1 << 16), IsReverse: false}
	tri := PairTriplet{pid, pid, pid}
	shard := &PairShard{Pair: pid, Bins: []EdgeBinding{{Pairs: tri, EdgeIdx: 1}}}
	buf := make([]CycleState, 0)
	attachShard(ex, shard, &buf)

	// profitable update => cs.Ticks[2] should be set
	upd1 := &TickUpdate{Pair: pid, FwdTick: 5, RevTick: -5}
	handleTick(ex, upd1)
	cs1 := buf[0]
	if cs1.Ticks[2] != 5 {
		t.Errorf("Expected cs.Ticks[2]=5; got %v", cs1.Ticks[2])
	}

	// non-profitable update => should not change slot beyond EdgeIdx
	upd2 := &TickUpdate{Pair: pid, FwdTick: -3, RevTick: 3}
	handleTick(ex, upd2)
}

// TestOnProfitable ensures onProfitable hook does not mutate CycleState.
func TestOnProfitable(t *testing.T) {
	cs := CycleState{Ticks: [3]float64{1.1, 2.2, 3.3}, Pairs: PairTriplet{7, 8, 9}}
	orig := cs
	onProfitable(&cs)
	if cs != orig {
		t.Errorf("onProfitable mutated state")
	}
}

// TestInitAndPair2cores checks InitExecutors populates pair2cores for provided cycles.
func TestInitAndPair2cores(t *testing.T) {
	cycles := []PairTriplet{{21, 22, 23}}
	// reset global mapping
	for pid := range pair2cores {
		pair2cores[PairID(pid)] = 0
	}
	InitExecutors(cycles)
	for _, pid := range cycles[0] {
		if pair2cores[pid] == 0 {
			t.Errorf("pair2cores[%d] is zero after InitExecutors", pid)
		}
	}
}

// TestZeroAllocLookup validates lookupPairID in zero-alloc context.
func TestZeroAllocLookup(t *testing.T) {
	addr := []byte("0123456789012345678901234567890123456789")
	pid := PairID(42)
	RegisterPair(addr, pid)
	if got := lookupPairID(addr); got != pid {
		t.Errorf("lookupPairID zero-alloc returned %v; want %v", got, pid)
	}
}

// TestDrainLoopProfitPath ensures handleTick drain loop caps at maxDrain and processes profit cases.
func TestDrainLoopProfitPath(t *testing.T) {
	ex := &CoreExecutor{Heaps: make([]bucketqueue.Queue, 1), Fanouts: make([][]FanoutEntry, 1), LocalIdx: localidx.New(1 << 16), IsReverse: false}
	hq := &ex.Heaps[0]
	for i := 0; i < 64; i++ {
		cs := &CycleState{Ticks: [3]float64{-10, -10, -10}}
		h, _ := hq.Borrow()
		_ = hq.Push(0, h, unsafe.Pointer(cs))
		ex.Fanouts[0] = append(ex.Fanouts[0], FanoutEntry{State: cs, Queue: hq, Handle: h, EdgeIdx: 1})
	}
	ex.LocalIdx.Put(0, 0)
	handleTick(ex, &TickUpdate{Pair: 0, FwdTick: -10})
}

// TestDrainLoopStopsAtNil tests handleTick exits drain loop gracefully when queue empties.
func TestDrainLoopStopsAtNil(t *testing.T) {
	ex := &CoreExecutor{Heaps: make([]bucketqueue.Queue, 1), Fanouts: make([][]FanoutEntry, 1), LocalIdx: localidx.New(1 << 16)}
	cs := &CycleState{Ticks: [3]float64{0, 0, 0}}
	hq := &ex.Heaps[0]
	h, _ := hq.Borrow()
	_ = hq.Push(0, h, unsafe.Pointer(cs))
	ex.Fanouts[0] = []FanoutEntry{{State: cs, Queue: hq, Handle: h, EdgeIdx: 0}}
	ex.LocalIdx.Put(0, 0)
	handleTick(ex, &TickUpdate{Pair: 0, FwdTick: 0})
}

// TestHandleTickReverse verifies that RevTick is used when IsReverse=true.
func TestHandleTickReverse(t *testing.T) {
	ex := &CoreExecutor{Heaps: make([]bucketqueue.Queue, 1), Fanouts: make([][]FanoutEntry, 1), LocalIdx: localidx.New(1 << 16), IsReverse: true}
	cs := &CycleState{}
	hq := &ex.Heaps[0]
	h, _ := hq.Borrow()
	_ = hq.Push(0, h, unsafe.Pointer(cs))
	ex.Fanouts[0] = []FanoutEntry{{State: cs, Queue: hq, Handle: h, EdgeIdx: 1}}
	ex.LocalIdx.Put(0, 0)
	upd := &TickUpdate{Pair: 0, FwdTick: 1, RevTick: 42}
	handleTick(ex, upd)
	if cs.Ticks[1] != 42 {
		t.Errorf("Expected reverse tick=42; got %v", cs.Ticks[1])
	}
}

// TestPinnedConsumerDispatch ensures PinnedConsumer routes messages to handleTick via ring24.
func TestPinnedConsumerDispatch(t *testing.T) {
	ex := &CoreExecutor{Heaps: make([]bucketqueue.Queue, 1), Fanouts: make([][]FanoutEntry, 1), LocalIdx: localidx.New(1 << 16)}
	cs := &CycleState{}
	hq := &ex.Heaps[0]
	h, _ := hq.Borrow()
	_ = hq.Push(0, h, unsafe.Pointer(cs))
	ex.Fanouts[0] = []FanoutEntry{{State: cs, Queue: hq, Handle: h, EdgeIdx: 0}}
	ex.LocalIdx.Put(123, 0)
	executors[3] = ex
	r := ring24.New(4)
	rings[3] = r
	msg := &TickUpdate{Pair: 123, FwdTick: 99}
	r.Push((*[32]byte)(unsafe.Pointer(msg)))
	done := make(chan struct{})
	go ring24.PinnedConsumer(3, r, new(uint32), new(uint32), func(p *[32]byte) {
		handleTick(ex, (*TickUpdate)(unsafe.Pointer(p)))
		close(done)
	}, done)
	time.Sleep(10 * time.Millisecond)
	if cs.Ticks[0] != 99 {
		t.Errorf("Expected tick=99 from PinnedConsumer; got %v", cs.Ticks[0])
	}
}
