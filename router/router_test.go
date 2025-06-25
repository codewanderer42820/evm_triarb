// Full-coverage tests for the router package.
// This file validates core router behaviors: mapping reserves to buckets, pair registration,
// route initialization, cycle registration, price-update routing, fanout logic, and error conditions.
package router

import (
	"encoding/binary"
	"math/rand"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"main/bucketqueue"
	"main/ring"
	"main/types"
	"main/utils"
)

// resetGlobals clears all global state so each test starts from a clean slate.
func resetGlobals() {
	for i := range routingBitmap {
		routingBitmap[i] = 0
	}
	for i := range addrToPairId {
		addrToPairId[i] = 0
	}
	coreRouters = nil
	for i := range coreRings {
		coreRings[i] = nil
	}
}

// TestMapL2ToBucket validates that tick values are clamped and mapped correctly into bucket range.
func TestMapL2ToBucket(t *testing.T) {
	cases := []float64{-9999, -64, -32, 0, 32, 64, 9999}
	for _, in := range cases {
		out := mapL2ToBucket(in)
		if out < 0 || out >= buckets {
			t.Fatalf("bucket out of range for %.2f: %d", in, out)
		}
	}
}

// TestRegisterPairLookup ensures the RegisterPair and lookupPairID path works without collision.
func TestRegisterPairLookup(t *testing.T) {
	resetGlobals()
	addr := make([]byte, 40)
	rand.Seed(time.Now().UnixNano())
	rand.Read(addr)
	const pairID = 12345
	RegisterPair(addr, pairID)
	if got := lookupPairID(addr); got != pairID {
		t.Fatalf("lookupPairID=%d, want %d", got, pairID)
	}
}

// TestRegisterRoute confirms that a bitmask is correctly written to the routing table.
func TestRegisterRoute(t *testing.T) {
	resetGlobals()
	RegisterRoute(42, 0xdeadbeef)
	if routingBitmap[42] != 0xdeadbeef {
		t.Fatalf("routingBitmap not recorded: got %x", routingBitmap[42])
	}
}

// TestInitAndRegisterCycles ensures RegisterCycles assigns each pair to exactly 2 cores.
func TestInitAndRegisterCycles(t *testing.T) {
	resetGlobals()
	runtime.GOMAXPROCS(8)
	InitCPURings()

	cyc := Cycle{Pairs: [3]uint16{10, 20, 30}}
	RegisterCycles([]Cycle{cyc})

	// Expect exactly 6 unique cores used — 2 per pair, no overlap
	coreUsed := make([]bool, len(coreRouters))
	for coreIdx, rt := range coreRouters {
		for _, pid := range []uint16{10, 20, 30} {
			if rt.PairIndex[pid] != 0 {
				coreUsed[coreIdx] = true
				break
			}
		}
	}

	var used int
	for _, active := range coreUsed {
		if active {
			used++
		}
	}

	if used != 6 {
		t.Fatalf("expected exactly 6 unique cores to be used, got %d", used)
	}
}

// TestRouteUpdateAndOnPriceUpdate drives a price update into a single-core setup.
func TestRouteUpdateAndOnPriceUpdate(t *testing.T) {
	resetGlobals()
	coreRouters = []*CoreRouter{{
		Routes:    []*DeltaBucket{{Queue: bucketqueue.New()}},
		Fanouts:   [][]fanRef{{}},
		PairIndex: make([]uint32, 1<<17),
	}}
	coreRings[0] = ring.New(16)
	addr := make([]byte, 40)
	rand.Read(addr)
	const pairID = 77
	RegisterPair(addr, pairID)
	RegisterRoute(pairID, 1)
	coreRouters[0].PairIndex[pairID] = 0
	path := &ArbPath{}
	h, _ := coreRouters[0].Routes[0].Queue.Borrow()
	_ = coreRouters[0].Routes[0].Queue.Push(0, h, unsafe.Pointer(path))
	var lv types.LogView
	lv.Addr = append([]byte{0, 0, 0}, addr...)
	lv.Data = make([]byte, 64)
	const (
		r0 uint64 = 123
		r1 uint64 = 456
	)
	binary.BigEndian.PutUint64(lv.Data[24:32], r0)
	binary.BigEndian.PutUint64(lv.Data[56:64], r1)
	RouteUpdate(&lv)
	ptr := coreRings[0].Pop()
	if ptr == nil {
		t.Fatalf("ring was empty after RouteUpdate push")
	}
	onPriceUpdate(coreRouters[0], (*PriceUpdate)(ptr))
	idx := int(coreRouters[0].PairIndex[pairID])
	bkt := coreRouters[0].Routes[idx]
	if bkt.CurLog == 0 {
		t.Fatalf("CurLog not updated by onPriceUpdate")
	}
	_, _, gotPtr := bkt.Queue.PeepMin()
	if gotPtr != unsafe.Pointer(path) {
		t.Fatalf("PeepMin pointer mismatch: got %p, want %p", gotPtr, unsafe.Pointer(path))
	}
}

// TestRegisterCyclesMultiPair confirms all 3 pairs in a cycle appear on at least one core.
func TestRegisterCyclesMultiPair(t *testing.T) {
	resetGlobals()
	runtime.GOMAXPROCS(4)
	InitCPURings()
	cyc := Cycle{Pairs: [3]uint16{1, 2, 3}}
	RegisterCycles([]Cycle{cyc})

	mapped := map[uint16]bool{}
	for _, rt := range coreRouters {
		for _, pid := range []uint16{1, 2, 3} {
			if rt.PairIndex[pid] != 0 {
				mapped[pid] = true
			}
		}
	}
	for _, pid := range []uint16{1, 2, 3} {
		if !mapped[pid] {
			t.Fatalf("pair %d not mapped to any core", pid)
		}
	}
}

// TestInitCPURings_SpawnsRoutersAndConsumers confirms InitCPURings allocates coreRouters and coreRings.
func TestInitCPURings_SpawnsRoutersAndConsumers(t *testing.T) {
	InitCPURings()
	if len(coreRouters) == 0 {
		t.Fatal("expected at least one coreRouter")
	}
	if coreRings[0] == nil {
		t.Fatal("expected coreRings[0] to be non-nil")
	}
}

// TestOnPriceUpdate_UsesRevTickWhenIsReverse ensures tick selection logic works with reverse flag.
func TestOnPriceUpdate_UsesRevTickWhenIsReverse(t *testing.T) {
	q := bucketqueue.New()
	h, _ := q.Borrow()
	dummyPath := &ArbPath{}
	_ = q.Push(0, h, unsafe.Pointer(dummyPath))
	rt := &CoreRouter{
		Routes:    []*DeltaBucket{{Queue: q}},
		Fanouts:   make([][]fanRef, 1),
		PairIndex: []uint32{0},
		IsReverse: true,
	}
	pu := &PriceUpdate{PairId: 0, FwdTick: 1.23, RevTick: 4.56}
	onPriceUpdate(rt, pu)
	if rt.Routes[0].CurLog != 4.56 {
		t.Fatalf("got CurLog=%v; want RevTick=4.56", rt.Routes[0].CurLog)
	}
}

// TestOnPriceUpdate_FanoutPathUpdate ensures the fanout pointer's tick value is updated correctly.
func TestOnPriceUpdate_FanoutPathUpdate(t *testing.T) {
	mainQ := bucketqueue.New()
	mh, _ := mainQ.Borrow()
	mainDummy := &ArbPath{}
	_ = mainQ.Push(0, mh, unsafe.Pointer(mainDummy))
	q := bucketqueue.New()
	path := &ArbPath{}
	h, _ := q.Borrow()
	_ = q.Push(0, h, unsafe.Pointer(path))
	rt := &CoreRouter{
		Routes:    []*DeltaBucket{{Queue: mainQ}},
		Fanouts:   [][]fanRef{{{P: path, Q: q, H: h, SharedLeg: 1}}},
		PairIndex: []uint32{0},
		IsReverse: false,
	}
	pu := &PriceUpdate{PairId: 0, FwdTick: 2.0, RevTick: -2.0}
	onPriceUpdate(rt, pu)
	if path.Ticks[1] != 2.0 {
		t.Fatalf("fanout did not update path.Ticks[1]: got %v, want 2.0", path.Ticks[1])
	}
	_, _, got := q.PeepMin()
	if got != unsafe.Pointer(path) {
		t.Fatal("queue.Update moved or dropped the element unexpectedly")
	}
}

// TestRegisterPair_PanicsWhenTableFull forces a RegisterPair collision exhaustion case.
func TestRegisterPair_PanicsWhenTableFull(t *testing.T) {
	addr := make([]byte, 40)
	start := utils.Hash17(addr)
	mask := (1 << 17) - 1
	for i := 0; i < 2048; i++ {
		idx := (start + uint32(i*64)) & uint32(mask)
		addrToPairId[idx] = 0xFFFF
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from full addrToPairId in RegisterPair")
		}
	}()
	RegisterPair(addr, 42)
}

// TestLookupPairID_PanicsWhenUnregistered confirms panic occurs when lookup fails.
func TestLookupPairID_PanicsWhenUnregistered(t *testing.T) {
	for i := range addrToPairId {
		addrToPairId[i] = 0
	}
	addr := make([]byte, 40)
	start := utils.Hash17(addr)
	mask := (1 << 17) - 1
	for i := 0; i < 2048; i++ {
		idx := (start + uint32(i*64)) & uint32(mask)
		addrToPairId[idx] = 0
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from exhausted addrToPairId in lookupPairID")
		}
	}()
	_ = lookupPairID(addr)
}
