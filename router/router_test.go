// Full‑coverage tests for the router package.
// Run: go test -cover ./...
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

// resetGlobals zeroes global state mutated across tests so each test can run in isolation.
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

func TestMapL2ToBucket(t *testing.T) {
	cases := []float64{-9999, -64, -32, 0, 32, 64, 9999}
	for _, in := range cases {
		out := mapL2ToBucket(in)
		if out < 0 || out >= buckets {
			t.Fatalf("bucket out of range for %.2f: %d", in, out)
		}
	}
}

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

func TestRegisterRoute(t *testing.T) {
	resetGlobals()
	RegisterRoute(42, 0xdead)
	if routingBitmap[42] != 0xdead {
		t.Fatalf("routingBitmap not recorded")
	}
}

func TestInitAndRegisterCycles(t *testing.T) {
	resetGlobals()
	runtime.GOMAXPROCS(2)
	InitCPURings()

	if len(coreRouters) == 0 {
		t.Fatalf("InitCPURings created zero routers")
	}

	cyc := Cycle{Pairs: [3]uint16{10, 20, 30}}
	RegisterCycles([]Cycle{cyc})

	mask := uint16((1 << len(coreRouters)) - 1)
	if routingBitmap[10] != mask {
		t.Fatalf("pair bitmap not set by RegisterCycles")
	}

	for _, rt := range coreRouters {
		idx := int(rt.PairIndex[10])
		if idx >= len(rt.Routes) {
			t.Fatalf("pair index %d out of range (routes=%d)", idx, len(rt.Routes))
		}
		if rt.Routes[idx] == nil || rt.Routes[idx].Queue == nil {
			t.Fatalf("bucket queue missing after RegisterCycles")
		}
	}
}

// TestRouteUpdateAndOnPriceUpdate drives tick ingestion and fan‑out logic on a single‑core router.
func TestRouteUpdateAndOnPriceUpdate(t *testing.T) {
	resetGlobals()

	// single‑core router with one bucket queue
	coreRouters = []*CoreRouter{{
		Routes:    []*DeltaBucket{{Queue: bucketqueue.New()}},
		Fanouts:   [][]fanRef{{}},
		PairIndex: make([]uint32, 1<<17),
	}}
	coreRings[0] = ring.New(16)

	// Random address ↔ pair registration.
	addr := make([]byte, 40)
	rand.Read(addr)
	const pairID = 77
	RegisterPair(addr, pairID)
	RegisterRoute(pairID, 1)             // core 0 only
	coreRouters[0].PairIndex[pairID] = 0 // bucket 0

	// Seed queue so PeepMin() has a value to work with.
	path := &ArbPath{}
	h, _ := coreRouters[0].Routes[0].Queue.Borrow()
	_ = coreRouters[0].Routes[0].Queue.Push(0, h, unsafe.Pointer(path))

	// Craft a mock Uniswap "Sync" log view with non‑zero reserves.
	var lv types.LogView
	lv.Addr = append([]byte{0, 0, 0}, addr...)
	lv.Data = make([]byte, 64)
	const (
		r0 uint64 = 123
		r1 uint64 = 456
	)
	// place 8‑byte big‑endian at offsets 24..31 and 56..63
	binary.BigEndian.PutUint64(lv.Data[24:32], r0)
	binary.BigEndian.PutUint64(lv.Data[56:64], r1)

	// Producer path: convert log → price update → ring push
	RouteUpdate(&lv)

	// Drain ring deterministically (simulate consumer goroutine)
	ptr := coreRings[0].Pop()
	if ptr == nil {
		t.Fatalf("ring was empty after RouteUpdate push")
	}
	onPriceUpdate(coreRouters[0], (*PriceUpdate)(ptr))

	// Validate side‑effects: CurLog updated & queue head still valid.
	idx := int(coreRouters[0].PairIndex[pairID])
	bkt := coreRouters[0].Routes[idx]
	if bkt.CurLog == 0 {
		t.Fatalf("CurLog not updated by onPriceUpdate")
	}
	_, _, gotPtr := bkt.Queue.PeepMin()
	if gotPtr != unsafe.Pointer(path) {
		t.Fatalf("PeepMin pointer mismatch after onPriceUpdate")
	}
}

// Verifies that each pair in a cycle gets its own bucket index.
func TestRegisterCyclesMultiPair(t *testing.T) {
	resetGlobals()
	runtime.GOMAXPROCS(4)
	InitCPURings()

	cyc := Cycle{Pairs: [3]uint16{1, 2, 3}}
	RegisterCycles([]Cycle{cyc})

	idx1 := coreRouters[0].PairIndex[1]
	idx2 := coreRouters[0].PairIndex[2]
	idx3 := coreRouters[0].PairIndex[3]

	if idx1 == 0 || idx2 == 0 || idx3 == 0 {
		t.Fatalf("one or more pairs not mapped (idx=0)")
	}
	if idx1 == idx2 || idx1 == idx3 || idx2 == idx3 {
		t.Fatalf("pairs mapped to the same bucket: %d,%d,%d", idx1, idx2, idx3)
	}
}

func TestInitCPURings_SpawnsRoutersAndConsumers(t *testing.T) {
	// Make sure InitCPURings doesn’t panic, allocates at least one coreRouter
	InitCPURings()
	if len(coreRouters) == 0 {
		t.Fatal("expected at least one coreRouter")
	}
	if coreRings[0] == nil {
		t.Fatal("expected coreRings[0] to be non-nil")
	}
}

func TestOnPriceUpdate_UsesRevTickWhenIsReverse(t *testing.T) {
	// set up a single-route router with a seeded queue & fanouts
	q := bucketqueue.New()
	h, _ := q.Borrow()
	dummyPath := &ArbPath{}
	_ = q.Push(0, h, unsafe.Pointer(dummyPath))

	rt := &CoreRouter{
		Routes:    []*DeltaBucket{&DeltaBucket{Queue: q}}, // one bucket
		Fanouts:   make([][]fanRef, 1),                    // one empty fanout list
		PairIndex: []uint32{0},                            // map pair 0 → bucket 0
		IsReverse: true,                                   // force use of RevTick
	}
	pu := &PriceUpdate{PairId: 0, FwdTick: 1.23, RevTick: 4.56}
	onPriceUpdate(rt, pu)
	// since IsReverse=true, CurLog must equal RevTick
	if rt.Routes[0].CurLog != 4.56 {
		t.Fatalf("got CurLog=%v; want RevTick=4.56", rt.Routes[0].CurLog)
	}
}

func TestOnPriceUpdate_FanoutPathUpdate(t *testing.T) {
	// seed the route’s primary queue so PeepMin() never returns nil
	mainQ := bucketqueue.New()
	mh, _ := mainQ.Borrow()
	mainDummy := &ArbPath{}
	_ = mainQ.Push(0, mh, unsafe.Pointer(mainDummy))

	// single-route, single-fanRef
	q := bucketqueue.New()
	path := &ArbPath{}
	h, _ := q.Borrow()
	// initial push so the handle is in the queue
	_ = q.Push(0, h, unsafe.Pointer(path))

	rt := &CoreRouter{
		Routes:    []*DeltaBucket{{Queue: mainQ}},
		Fanouts:   [][]fanRef{{{P: path, Q: q, H: h, SharedLeg: 1}}},
		PairIndex: []uint32{0},
		IsReverse: false,
	}
	pu := &PriceUpdate{PairId: 0, FwdTick: 2.0, RevTick: -2.0}

	// drive the fanout branch
	onPriceUpdate(rt, pu)

	// fanRef.P.Ticks[1] should now be set to FwdTick
	if path.Ticks[1] != 2.0 {
		t.Fatalf("fanout did not update path.Ticks[1]: got %v, want 2.0", path.Ticks[1])
	}

	// and queue should have been “updated” — PeepMin should still return the same pointer
	_, _, got := q.PeepMin()
	if got != unsafe.Pointer(path) {
		t.Fatal("queue.Update moved or dropped the element unexpectedly")
	}
}

func TestRegisterPair_PanicsWhenTableFull(t *testing.T) {
	// pick a test address and fill all 2048 slots spaced by 64
	addr := make([]byte, 40)
	start := utils.Hash17(addr)
	mask := (1 << 17) - 1
	for i := 0; i < 2048; i++ {
		idx := (start + uint32(i*64)) & uint32(mask)
		addrToPairId[idx] = 0xFFFF // dummy non-zero
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from full addrToPairId in RegisterPair")
		}
	}()
	RegisterPair(addr, 42)
}

func TestLookupPairID_PanicsWhenUnregistered(t *testing.T) {
	// clear table and then force exhaustion
	for i := range addrToPairId {
		addrToPairId[i] = 0
	}
	addr := make([]byte, 40)
	start := utils.Hash17(addr)
	// fill exactly the same 2048 slots
	mask := (1 << 17) - 1
	for i := 0; i < 2048; i++ {
		idx := (start + uint32(i*64)) & uint32(mask)
		addrToPairId[idx] = 0 // keep zero so lookup never finds a pair
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from exhausted addrToPairId in lookupPairID")
		}
	}()
	_ = lookupPairID(addr)
}
