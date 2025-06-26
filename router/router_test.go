package router

import (
	"testing"

	"main/bucketqueue"
	"main/localidx"
	"main/types"
)

// TestL2Bucket covers boundary and mid-range conversion of log2 sums to bucket IDs.
func TestL2Bucket(t *testing.T) {
	// Below lower clamp
	t0 := l2Bucket(-200)
	if t0 != 0 {
		t.Errorf("l2Bucket(-200) = %d; want 0", t0)
	}

	// Above upper clamp
	t1 := l2Bucket(200)
	// maximum bucket index is (2*clamp*scale) = 4096
	if t1 != 4096 {
		t.Errorf("l2Bucket(200) = %d; want 4096", t1)
	}

	// Mid-range (zero sum)
	exp := int64((0 + 128) * 16)
	if m := l2Bucket(0); m != exp {
		t.Errorf("l2Bucket(0) = %d; want %d", m, exp)
	}
}

// TestBuildFanouts ensures BuildFanouts partitions refs by PairID correctly.
func TestBuildFanouts(t *testing.T) {
	ResetFanouts()
	cycles := []TriCycle{
		{1, 2, 3},
		{2, 3, 4},
	}
	BuildFanouts(cycles)

	// Expect at least one shard for each pair
	for _, pid := range []PairID{1, 2, 3, 4} {
		if shards, ok := rawShards[pid]; !ok || len(shards) == 0 {
			t.Errorf("rawShards[%d] = %v; want non-empty slice", pid, shards)
		}
	}
}

// TestShuffleRefs confirms shuffleRefs preserves elements and length.
func TestShuffleRefs(t *testing.T) {
	refs := []Ref{
		{Pairs: TriCycle{1, 2, 3}, Edge: 0},
		{Pairs: TriCycle{4, 5, 6}, Edge: 1},
		{Pairs: TriCycle{7, 8, 9}, Edge: 2},
	}
	rev := make([]Ref, len(refs))
	copy(rev, refs)
	shuffleRefs(rev)

	// Length must remain same
	if len(rev) != len(refs) {
		t.Fatalf("shuffleRefs changed length: got %d, want %d", len(rev), len(refs))
	}

	// Each original element must appear exactly once in rev
	counts := make(map[Ref]int)
	for _, r := range refs {
		counts[r]++
	}
	for _, r := range rev {
		if counts[r] == 0 {
			t.Errorf("shuffleRefs result contains unexpected element %v", r)
		} else {
			counts[r]--
		}
	}
	for r, c := range counts {
		if c != 0 {
			t.Errorf("shuffleRefs missing element %v, count %d", r, c)
		}
	}
}

// TestRegisterPairAndLookup covers RegisterPair and lookupPairID.
func TestRegisterPairAndLookup(t *testing.T) {
	addr := make([]byte, 40)
	pid := PairID(42)
	RegisterPair(addr, pid)

	found := lookupPairID(addr)
	if found != pid {
		t.Errorf("lookupPairID returned %d; want %d", found, pid)
	}
}

// TestRegisterRoute sets and reads routingBitmap bits.
func TestRegisterRoute(t *testing.T) {
	pid := PairID(7)
	core := uint8(3)
	RegisterRoute(pid, core)
	mask := routingBitmap[pid]
	if mask&(1<<core) == 0 {
		t.Errorf("routingBitmap[%d] missing bit %d", pid, core)
	}
}

// TestInstallShard covers basic bucket and fanout population.
func TestInstallShard(t *testing.T) {
	rt := &CoreRouter{
		Buckets: make([]bucketqueue.Queue, 0),
		Fanouts: make([][]Fanout, 0),
		Local:   localidx.New(1 << 10),
	}
	paths := []ArbPath{}
	sh := Shard{
		Pair: 5,
		Refs: []Ref{{Pairs: TriCycle{5, 6, 7}, Edge: 2}},
	}
	installShard(rt, &sh, &paths)

	if len(rt.Buckets) != 1 {
		t.Fatalf("Buckets length = %d; want 1", len(rt.Buckets))
	}
	if len(rt.Fanouts) != 1 || len(rt.Fanouts[0]) != 1 {
		t.Fatalf("Fanouts = %v; want [[...]]", rt.Fanouts)
	}
	f := rt.Fanouts[0][0]
	if f.Edge != 2 {
		t.Errorf("Fanout.Edge = %d; want 2", f.Edge)
	}
	if f.Path.Pairs != (TriCycle{5, 6, 7}) {
		t.Errorf("Fanout.Path.Pairs = %v; want {5,6,7}", f.Path.Pairs)
	}
	if len(paths) != 1 {
		t.Errorf("paths length = %d; want 1", len(paths))
	}
}

// TestOnPrice_NoPanic ensures onPrice executes without panic on minimal input.
func TestOnPrice_NoPanic(t *testing.T) {
	// Prepare CoreRouter with valid mapping for pair 0
	rt := &CoreRouter{
		Buckets: make([]bucketqueue.Queue, 1),
		Fanouts: make([][]Fanout, 1),
		Local:   localidx.New(1 << 10),
	}
	// Map global PairID 0 to local index 0
	rt.Local.Put(0, 0)

	upd := &PriceUpdate{Pair: 0, FwdTick: 1.23, RevTick: -1.23}
	// Should not panic
	onPrice(rt, upd)
}

// TestRouteUpdate_NoPanic ensures RouteUpdate does not panic with realistic reserves.
func TestRouteUpdate_NoPanic(t *testing.T) {
	v := &types.LogView{
		Addr: make([]byte, 43),
		Data: make([]byte, 64),
	}
	// Populate Data so both reserve values are non-zero
	for i := 24; i < 32; i++ {
		v.Data[i] = 1
	}
	for i := 56; i < 64; i++ {
		v.Data[i] = 2
	}
	// Should not panic
	RouteUpdate(v)
}
