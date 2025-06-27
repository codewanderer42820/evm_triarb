// router_coverage_test.go
// Tests for router.go and router_lookup_zeroalloc.go fileciteturn3file0 fileciteturn3file10
package router

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"main/bucketqueue"
	"main/localidx"
	"main/ring32"
	"main/types"
)

// TestSliceToWordKeyEqual tests sliceToWordKey and wordKey.equal
func TestSliceToWordKeyEqual(t *testing.T) {
	addr := make([]byte, 40)
	for i := range addr {
		addr[i] = byte(i)
	}
	k1 := sliceToWordKey(addr)
	if !k1.equal(k1) {
		t.Errorf("wordKey.equal should return true for identical keys")
	}
	addr2 := make([]byte, 40)
	copy(addr2, addr)
	addr2[0] ^= 0xFF
	k2 := sliceToWordKey(addr2)
	if k1.equal(k2) {
		t.Errorf("wordKey.equal should return false for different keys")
	}
}

// TestRegisterAndLookupPair tests RegisterPair and lookupPairID
func TestRegisterAndLookupPair(t *testing.T) {
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
	pid2 := PairID(456)
	RegisterPair(addr, pid2)
	if got := lookupPairID(addr); got != pid2 {
		t.Errorf("lookupPairID returned %v; want %v", got, pid2)
	}
	addr3 := make([]byte, 40)
	for i := range addr3 {
		addr3[i] = byte(40 + i)
	}
	if got := lookupPairID(addr3); got != 0 {
		t.Errorf("lookupPairID returned %v for unknown; want 0", got)
	}
}

// TestLog2ToTick tests log2ToTick clamp behavior
func TestLog2ToTick(t *testing.T) {
	if got := log2ToTick(-200); got != 0 {
		t.Errorf("log2ToTick(-200)=%v; want 0", got)
	}
	if got := log2ToTick(0); got != int64((0+clamp)*scale) {
		t.Errorf("log2ToTick(0)=%v; want %v", got, int64((0+clamp)*scale))
	}
	over := (float64(maxTick+1) / scale) - clamp + 1.0
	if got := log2ToTick(over); got != maxTick {
		t.Errorf("log2ToTick(overflow)=%v; want %v", got, maxTick)
	}
}

// TestLog2Clamp covers exact clamp boundaries
func TestLog2Clamp(t *testing.T) {
	if got := log2ToTick(-clamp); got != 0 {
		t.Errorf("log2ToTick(-clamp)=%v; want 0", got)
	}
	if got := log2ToTick(clamp); got != maxTick {
		t.Errorf("log2ToTick(clamp)=%v; want %v", got, maxTick)
	}
}

// TestRegisterRoute tests RegisterRoute function
func TestRegisterRoute(t *testing.T) {
	pid := PairID(7)
	pair2cores[pid] = 0
	RegisterRoute(pid, 3)
	if pair2cores[pid]&(1<<3) == 0 {
		t.Errorf("RegisterRoute did not set bit for core")
	}
}

// TestBuildFanoutShards tests buildFanoutShards and shuffleBindings
func TestBuildFanoutShards(t *testing.T) {
	cycles := []PairTriplet{{1, 2, 3}}
	buildFanoutShards(cycles)
	if len(shardBucket) != 3 {
		t.Errorf("shardBucket length %v; want 3", len(shardBucket))
	}
	bins := []EdgeBinding{
		{Pairs: cycles[0], EdgeIdx: 0},
		{Pairs: cycles[0], EdgeIdx: 1},
		{Pairs: cycles[0], EdgeIdx: 2},
	}
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
			t.Errorf("shuffleBindings unexpected %v", eb)
		}
	}
}

// TestDispatchUpdate tests DispatchUpdate early return and non-zero path
func TestDispatchUpdate(t *testing.T) {
	v := &types.LogView{Addr: make([]byte, addrHexEnd), Data: make([]byte, 64)}
	DispatchUpdate(v)
	addr := make([]byte, 40)
	for i := range addr {
		addr[i] = byte(i + 10)
	}
	RegisterPair(addr, PairID(9))
	copy(v.Addr[addrHexStart:addrHexEnd], addr)
	pair2cores[9] = 1 << 0
	rings[0] = ring32.New(1 << 4)
	binary.BigEndian.PutUint64(v.Data[24:32], 100)
	binary.BigEndian.PutUint64(v.Data[56:64], 200)
	DispatchUpdate(v)
}

// TestHandleTick covers both profitable and non-profitable paths
func TestHandleTick(t *testing.T) {
	pid := PairID(11)
	ex := &CoreExecutor{Heaps: []bucketqueue.Queue{}, Fanouts: [][]FanoutEntry{}, LocalIdx: localidx.New(1 << 16), IsReverse: false}
	tri := PairTriplet{pid, pid, pid}
	shard := &PairShard{Pair: pid, Bins: []EdgeBinding{{Pairs: tri, EdgeIdx: 1}}}
	buf := make([]CycleState, 0)
	attachShard(ex, shard, &buf)

	// profitable update
	upd1 := &TickUpdate{Pair: pid, FwdTick: 5, RevTick: -5}
	handleTick(ex, upd1)
	cs1 := buf[0]
	if cs1.Ticks[2] != 5 {
		t.Errorf("Expected cs.Ticks[2]=5; got %v", cs1.Ticks[2])
	}

	// non-profitable update
	upd2 := &TickUpdate{Pair: pid, FwdTick: -3, RevTick: 3}
	handleTick(ex, upd2)
	cs2 := buf[0]
	if cs2.Ticks[0] != -3 {
		t.Errorf("Expected cs.Ticks[0]=-3; got %v", cs2.Ticks[0])
	}
}

// TestOnProfitable ensures onProfitable does not mutate
func TestOnProfitable(t *testing.T) {
	cs := CycleState{Ticks: [3]float64{1.1, 2.2, 3.3}, Pairs: PairTriplet{7, 8, 9}}
	orig := cs
	onProfitable(&cs)
	if cs != orig {
		t.Errorf("onProfitable mutated state")
	}
}

// TestInitAndPair2cores tests InitExecutors updates pair2cores mapping
func TestInitAndPair2cores(t *testing.T) {
	cycles := []PairTriplet{{21, 22, 23}}
	// reset
	for pid := range pair2cores {
		pair2cores[PairID(pid)] = 0
	}
	InitExecutors(cycles)
	// bits should be set for these PIDs
	for _, pid := range cycles[0] {
		if pair2cores[pid] == 0 {
			t.Errorf("pair2cores[%d] is zero", pid)
		}
	}
}

// TestZeroAllocLookup tests lookup under zero-copy implementation
func TestZeroAllocLookup(t *testing.T) {
	addr := []byte("0123456789012345678901234567890123456789")
	pid := PairID(42)
	RegisterPair(addr, pid)
	if got := lookupPairID(addr); got != pid {
		t.Errorf("zeroalloc lookup returned %v; want %v", got, pid)
	}
}

// TestDrainLoopProfitPath tests handleTick with wasProfit=true and maxDrain cap
func TestDrainLoopProfitPath(t *testing.T) {
	ex := &CoreExecutor{
		Heaps:     make([]bucketqueue.Queue, 1),
		Fanouts:   make([][]FanoutEntry, 1),
		LocalIdx:  localidx.New(1 << 16),
		IsReverse: false,
	}
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

// TestDrainLoopStopsAtNil tests handleTick exit on nil ptr
func TestDrainLoopStopsAtNil(t *testing.T) {
	ex := &CoreExecutor{
		Heaps:    make([]bucketqueue.Queue, 1),
		Fanouts:  make([][]FanoutEntry, 1),
		LocalIdx: localidx.New(1 << 16),
	}
	cs := &CycleState{Ticks: [3]float64{0, 0, 0}}
	hq := &ex.Heaps[0]
	h, _ := hq.Borrow()
	_ = hq.Push(0, h, unsafe.Pointer(cs))
	ex.Fanouts[0] = nil
	ex.LocalIdx.Put(0, 0)
	handleTick(ex, &TickUpdate{Pair: 0, FwdTick: 0})
}

// TestHandleTickReverse tests that RevTick is used on reverse
func TestHandleTickReverse(t *testing.T) {
	ex := &CoreExecutor{
		Heaps:     make([]bucketqueue.Queue, 1),
		Fanouts:   make([][]FanoutEntry, 1),
		LocalIdx:  localidx.New(1 << 16),
		IsReverse: true,
	}
	cs := &CycleState{}
	hq := &ex.Heaps[0]
	h, _ := hq.Borrow()
	_ = hq.Push(0, h, unsafe.Pointer(cs))
	ex.Fanouts[0] = []FanoutEntry{{State: cs, Queue: hq, Handle: h, EdgeIdx: 1}}
	ex.LocalIdx.Put(0, 0)
	upd := &TickUpdate{Pair: 0, FwdTick: 1, RevTick: 42}
	handleTick(ex, upd)
	if cs.Ticks[1] != 42 {
		t.Errorf("Expected tick=42 in reverse, got %v", cs.Ticks[1])
	}
}

// TestPinnedConsumerDispatch ensures the ring dispatch path runs handleTick
func TestPinnedConsumerDispatch(t *testing.T) {
	ex := &CoreExecutor{
		Heaps:    make([]bucketqueue.Queue, 1),
		Fanouts:  make([][]FanoutEntry, 1),
		LocalIdx: localidx.New(1 << 16),
	}
	cs := &CycleState{}
	hq := &ex.Heaps[0]
	h, _ := hq.Borrow()
	_ = hq.Push(0, h, unsafe.Pointer(cs))
	ex.Fanouts[0] = []FanoutEntry{{State: cs, Queue: hq, Handle: h, EdgeIdx: 0}}
	ex.LocalIdx.Put(123, 0)
	executors[3] = ex
	r := ring32.New(4)
	rings[3] = r
	msg := &TickUpdate{Pair: 123, FwdTick: 99}
	r.Push((*[32]byte)(unsafe.Pointer(msg)))
	done := make(chan struct{})
	go ring32.PinnedConsumer(3, r, new(uint32), new(uint32), func(p *[32]byte) {
		handleTick(ex, (*TickUpdate)(unsafe.Pointer(p)))
		close(done)
	}, done)
	time.Sleep(10 * time.Millisecond)
	if cs.Ticks[0] != 99 {
		t.Errorf("Expected tick=99 from PinnedConsumer, got %v", cs.Ticks[0])
	}
}
