// router_coverage_test.go
// Tests for router.go and router_lookup_zeroalloc.go fileciteturn0file0 fileciteturn0file1
package router

import (
	"encoding/binary"
	"reflect"
	"testing"

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
	addr2[0] = addr2[0] ^ 0xFF
	k2 := sliceToWordKey(addr2)
	if k1.equal(k2) {
		t.Errorf("wordKey.equal should return false for different keys")
	}
}

// TestRegisterAndLookupPair tests RegisterPair and lookupPairID
func TestRegisterAndLookupPair(t *testing.T) {
	// clear global tables
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
	// overwrite
	pid2 := PairID(456)
	RegisterPair(addr, pid2)
	if got := lookupPairID(addr); got != pid2 {
		t.Errorf("lookupPairID returned %v; want %v", got, pid2)
	}
	// unknown address
	addr3 := make([]byte, 40)
	for i := range addr3 {
		addr3[i] = byte(40 + i)
	}
	if got := lookupPairID(addr3); got != 0 {
		t.Errorf("lookupPairID returned %v for unknown; want 0", got)
	}
}

// TestLog2ToTick tests log2ToTick boundary conditions
func TestLog2ToTick(t *testing.T) {
	if got := log2ToTick(-200); got != 0 {
		t.Errorf("log2ToTick(-200)=%v; want 0", got)
	}
	if got := log2ToTick(200); got != maxTick {
		t.Errorf("log2ToTick(200)=%v; want %v", got, maxTick)
	}
	mid := log2ToTick(0)
	wantMid := int64((0 + clamp) * scale)
	if mid != wantMid {
		t.Errorf("log2ToTick(0)=%v; want %v", mid, wantMid)
	}
	// overflow trimming
	overflow := (float64(maxTick+1) / scale) - clamp + 1
	if got := log2ToTick(overflow); got != maxTick {
		t.Errorf("log2ToTick(overflow)=%v; want %v", got, maxTick)
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
		t.Errorf("buildFanoutShards: shardBucket length %v; want 3", len(shardBucket))
	}
	for _, pid := range cycles[0] {
		shards := shardBucket[pid]
		if len(shards) != 1 {
			t.Errorf("shardBucket[%v] length %v; want 1", pid, len(shards))
		}
		if shards[0].Pair != pid {
			t.Errorf("shards[0].Pair = %v; want %v", shards[0].Pair, pid)
		}
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
				break
			}
		}
		if !found {
			t.Errorf("shuffleBindings produced unexpected element: %v", eb)
		}
	}
}

// TestDispatchUpdate tests DispatchUpdate early return and non-zero path
func TestDispatchUpdate(t *testing.T) {
	v := &types.LogView{
		Addr: make([]byte, addrHexEnd),
		Data: make([]byte, 64),
	}
	// early return: no pair address, should not panic
	DispatchUpdate(v)

	// prepare non-zero path
	addr := make([]byte, 40)
	for i := range addr {
		addr[i] = byte(i + 10)
	}
	RegisterPair(addr, PairID(9))
	copy(v.Addr[addrHexStart:addrHexEnd], addr)
	pair2cores[9] = 1 << 0
	rings[0] = ring32.New(1 << 4)
	// set non-zero reserves to avoid fastuni panic
	binary.BigEndian.PutUint64(v.Data[24:32], 100)
	binary.BigEndian.PutUint64(v.Data[56:64], 200)
	DispatchUpdate(v)
}

// TestHandleTick covers handleTick path execution
func TestHandleTick(t *testing.T) {
	ex := &CoreExecutor{
		Heaps:     []bucketqueue.Queue{*bucketqueue.New()},
		Fanouts:   make([][]FanoutEntry, 1),
		LocalIdx:  localidx.New(1 << 16),
		IsReverse: false,
	}
	tri := PairTriplet{4, 5, 6}
	shard := &PairShard{Pair: 4, Bins: []EdgeBinding{{Pairs: tri, EdgeIdx: 0}}}
	buf := make([]CycleState, 0)
	attachShard(ex, shard, &buf)
	upd := &TickUpdate{Pair: 4, FwdTick: 10, RevTick: -10}
	handleTick(ex, upd)
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
