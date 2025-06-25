// Full-coverage tests for the router package.
// This file validates core router behaviors: mapping reserves to buckets, pair registration,
// route initialization, cycle registration, price-update routing, fanout logic, and error conditions.
package router

import (
	"encoding/binary"
	"math/bits"
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
// It zeroes routing bitmaps, address mappings, core router slices and rings.
func resetGlobals() {
	// Clear routing bitmap entries used by RegisterRoute and RegisterCycles
	for i := range routingBitmap {
		routingBitmap[i] = 0
	}

	// Clear address-to-pair ID lookup table used by RegisterPair and lookupPairID
	for i := range addrToPairId {
		addrToPairId[i] = 0
	}

	// Reset coreRouters slice (each element is a *CoreRouter)
	coreRouters = nil

	// Reset coreRings ring buffers to nil
	for i := range coreRings {
		coreRings[i] = nil
	}
}

// TestMapL2ToBucket ensures that mapL2ToBucket maps any log2 ratio to a valid bucket index.
func TestMapL2ToBucket(t *testing.T) {
	// Test a range of input values, including negative, zero, and large positive floats
	cases := []float64{-9999, -64, -32, 0, 32, 64, 9999}
	for _, in := range cases {
		out := mapL2ToBucket(in)
		// The bucket index must lie within [0, buckets)
		if out < 0 || out >= buckets {
			t.Fatalf("bucket out of range for %.2f: %d", in, out)
		}
	}
}

// TestRegisterPairLookup verifies that RegisterPair stores the given pairID and lookupPairID retrieves it.
func TestRegisterPairLookup(t *testing.T) {
	resetGlobals() // Ensure no prior mappings exist

	// Create a random 40-byte address
	addr := make([]byte, 40)
	rand.Seed(time.Now().UnixNano())
	rand.Read(addr)
	const pairID = 12345

	// Register the address -> pairID mapping
	RegisterPair(addr, pairID)

	// lookupPairID should return the same pairID
	if got := lookupPairID(addr); got != pairID {
		t.Fatalf("lookupPairID=%d, want %d", got, pairID)
	}
}

// TestRegisterRoute ensures RegisterRoute writes into the routingBitmap array.
func TestRegisterRoute(t *testing.T) {
	resetGlobals() // Clear previous routes

	// Register a route for pairID 42 to use mask 0xdead
	RegisterRoute(42, 0xdead)
	// Verify the bitmap entry was updated
	if routingBitmap[42] != 0xdead {
		t.Fatalf("routingBitmap not recorded: got %x", routingBitmap[42])
	}
}

// TestInitAndRegisterCycles verifies InitCPURings and that RegisterCycles
// picks exactly one forward-core and one reverse-core, wiring their queues.
func TestInitAndRegisterCycles(t *testing.T) {
	resetGlobals()
	runtime.GOMAXPROCS(4)
	InitCPURings()

	// Register a single 3-pair cycle
	cyc := Cycle{Pairs: [3]uint16{10, 20, 30}}
	RegisterCycles([]Cycle{cyc})

	// Mask for pair 10
	mask := routingBitmap[10]

	// 1) Exactly two bits set
	if bits.OnesCount16(mask) != 2 {
		t.Fatalf("expected exactly 2 cores enabled for pair 10, got mask %x", mask)
	}

	// 2) One in forward half, one in reverse half
	half := len(coreRouters) / 2
	var sawFwd, sawRev bool
	for i := range coreRouters {
		if mask&(1<<i) != 0 {
			if i < half {
				sawFwd = true
			} else {
				sawRev = true
			}
		}
	}
	if !sawFwd || !sawRev {
		t.Fatalf("expected one forward and one reverse core, got mask %x", mask)
	}

	// 3) Only those two cores must have routes queued
	for i, rt := range coreRouters {
		if mask&(1<<i) == 0 {
			// this core wasn’t selected → no routes for pair 10
			if rt.PairIndex[10] != 0 {
				t.Errorf("core %d was not selected but has PairIndex=%d", i, rt.PairIndex[10])
			}
			continue
		}
		// for selected cores, PairIndex should be 1-based and within Routes
		idx := rt.PairIndex[10]
		if idx == 0 || int(idx) > len(rt.Routes) {
			t.Fatalf("core %d: invalid PairIndex %d (routes=%d)", i, idx, len(rt.Routes))
		}
		b := rt.Routes[idx-1]
		if b == nil || b.Queue == nil {
			t.Fatalf("core %d: missing bucket queue at index %d", i, idx-1)
		}
	}
}

// TestRouteUpdateAndOnPriceUpdate drives a price update through a single-core router.
func TestRouteUpdateAndOnPriceUpdate(t *testing.T) {
	resetGlobals()

	// Manually set up a single-core router without using InitCPURings
	coreRouters = []*CoreRouter{{
		Routes:    []*DeltaBucket{{Queue: bucketqueue.New()}},
		Fanouts:   [][]fanRef{{}},
		PairIndex: make([]uint32, 1<<17),
	}}
	// Create a small ring buffer for core 0
	coreRings[0] = ring.New(16)

	// Simulate registering an address->pair and route on core 0
	addr := make([]byte, 40)
	rand.Read(addr)
	const pairID = 77
	RegisterPair(addr, pairID)
	RegisterRoute(pairID, 1)             // mask=1 -> core 0 only
	coreRouters[0].PairIndex[pairID] = 0 // map to bucket 0

	// Seed the bucket queue so PeepMin never returns nil
	path := &ArbPath{}
	h, _ := coreRouters[0].Routes[0].Queue.Borrow()
	_ = coreRouters[0].Routes[0].Queue.Push(0, h, unsafe.Pointer(path))

	// Create a fake Uniswap Sync log with r0,r1 reserves at offsets 24 and 56
	var lv types.LogView
	lv.Addr = append([]byte{0, 0, 0}, addr...)
	lv.Data = make([]byte, 64)
	const (
		r0 uint64 = 123
		r1 uint64 = 456
	)
	binary.BigEndian.PutUint64(lv.Data[24:32], r0)
	binary.BigEndian.PutUint64(lv.Data[56:64], r1)

	// Execute RouteUpdate: push PriceUpdate onto ring
	RouteUpdate(&lv)

	// Pop the update off the ring to simulate the consumer goroutine
	ptr := coreRings[0].Pop()
	if ptr == nil {
		t.Fatalf("ring was empty after RouteUpdate push")
	}
	// Invoke onPriceUpdate to apply the update to the router state
	onPriceUpdate(coreRouters[0], (*PriceUpdate)(ptr))

	// Check that CurLog was updated from 0 to a non-zero value
	idx := int(coreRouters[0].PairIndex[pairID])
	bkt := coreRouters[0].Routes[idx]
	if bkt.CurLog == 0 {
		t.Fatalf("CurLog not updated by onPriceUpdate")
	}

	// Ensure PeepMin still returns the original path pointer
	_, _, gotPtr := bkt.Queue.PeepMin()
	if gotPtr != unsafe.Pointer(path) {
		t.Fatalf("PeepMin pointer mismatch: got %p, want %p", gotPtr, unsafe.Pointer(path))
	}
}

// TestRegisterCyclesMultiPair verifies that multi-pair cycles assign distinct buckets
func TestRegisterCyclesMultiPair(t *testing.T) {
	resetGlobals()
	runtime.GOMAXPROCS(4)
	InitCPURings()

	// Register a cycle of 3 distinct pairs
	cyc := Cycle{Pairs: [3]uint16{1, 2, 3}}
	RegisterCycles([]Cycle{cyc})

	// Each pair should have a non-zero bucket index
	idx1 := coreRouters[0].PairIndex[1]
	idx2 := coreRouters[0].PairIndex[2]
	idx3 := coreRouters[0].PairIndex[3]

	if idx1 == 0 || idx2 == 0 || idx3 == 0 {
		t.Fatalf("one or more pairs not mapped: %d, %d, %d", idx1, idx2, idx3)
	}
	// Indices must all be unique
	if idx1 == idx2 || idx1 == idx3 || idx2 == idx3 {
		t.Fatalf("pairs mapped to same bucket: %d, %d, %d", idx1, idx2, idx3)
	}
}

// TestInitCPURings_SpawnsRoutersAndConsumers ensures InitCPURings produces at least one router
func TestInitCPURings_SpawnsRoutersAndConsumers(t *testing.T) {
	InitCPURings()
	if len(coreRouters) == 0 {
		t.Fatal("expected at least one coreRouter")
	}
	if coreRings[0] == nil {
		t.Fatal("expected coreRings[0] to be non-nil")
	}
}

// TestOnPriceUpdate_UsesRevTickWhenIsReverse verifies reverse-tick behavior
func TestOnPriceUpdate_UsesRevTickWhenIsReverse(t *testing.T) {
	// Set up a bucketqueue with a dummy path
	q := bucketqueue.New()
	h, _ := q.Borrow()
	dummyPath := &ArbPath{}
	_ = q.Push(0, h, unsafe.Pointer(dummyPath))

	// Create a CoreRouter marked as reverse
	rt := &CoreRouter{
		Routes:    []*DeltaBucket{{Queue: q}},
		Fanouts:   make([][]fanRef, 1),
		PairIndex: []uint32{0},
		IsReverse: true,
	}
	pu := &PriceUpdate{PairId: 0, FwdTick: 1.23, RevTick: 4.56}
	onPriceUpdate(rt, pu)

	// CurLog should reflect RevTick when IsReverse=true
	if rt.Routes[0].CurLog != 4.56 {
		t.Fatalf("got CurLog=%v; want RevTick=4.56", rt.Routes[0].CurLog)
	}
}

// TestOnPriceUpdate_FanoutPathUpdate validates fanout logic updates shared paths
func TestOnPriceUpdate_FanoutPathUpdate(t *testing.T) {
	// Seed main queue so its PeepMin never returns nil
	mainQ := bucketqueue.New()
	mh, _ := mainQ.Borrow()
	mainDummy := &ArbPath{}
	_ = mainQ.Push(0, mh, unsafe.Pointer(mainDummy))

	// Create secondary queue for fanout
	q := bucketqueue.New()
	path := &ArbPath{}
	h, _ := q.Borrow()
	_ = q.Push(0, h, unsafe.Pointer(path))

	// Single route with one fanRef: path shares element in secondary queue
	rt := &CoreRouter{
		Routes:    []*DeltaBucket{{Queue: mainQ}},
		Fanouts:   [][]fanRef{{{P: path, Q: q, H: h, SharedLeg: 1}}},
		PairIndex: []uint32{0},
		IsReverse: false,
	}
	pu := &PriceUpdate{PairId: 0, FwdTick: 2.0, RevTick: -2.0}

	// Trigger onPriceUpdate which should update path.Ticks[1]
	onPriceUpdate(rt, pu)

	// Confirm tick update on shared path leg
	if path.Ticks[1] != 2.0 {
		t.Fatalf("fanout did not update path.Ticks[1]: got %v, want 2.0", path.Ticks[1])
	}

	// Ensure secondary queue still holds the path
	_, _, got := q.PeepMin()
	if got != unsafe.Pointer(path) {
		t.Fatal("queue.Update moved or dropped the element unexpectedly")
	}
}

// TestRegisterPair_PanicsWhenTableFull verifies panic on full addrToPairId table
func TestRegisterPair_PanicsWhenTableFull(t *testing.T) {
	// Pre-fill all 2048 slots so RegisterPair cannot find a free slot
	addr := make([]byte, 40)
	start := utils.Hash17(addr)
	mask := (1 << 17) - 1
	for i := 0; i < 2048; i++ {
		idx := (start + uint32(i*64)) & uint32(mask)
		addrToPairId[idx] = 0xFFFF // dummy non-zero means occupied
	}

	// Expect panic due to exhausted table
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from full addrToPairId in RegisterPair")
		}
	}()
	RegisterPair(addr, 42)
}

// TestLookupPairID_PanicsWhenUnregistered verifies panic when lookupPairID finds no entry
func TestLookupPairID_PanicsWhenUnregistered(t *testing.T) {
	// Clear the table
	for i := range addrToPairId {
		addrToPairId[i] = 0
	}
	addr := make([]byte, 40)
	start := utils.Hash17(addr)
	mask := (1 << 17) - 1
	// Fill same slots with zero (unregistered) to exhaust lookup
	for i := 0; i < 2048; i++ {
		idx := (start + uint32(i*64)) & uint32(mask)
		addrToPairId[idx] = 0 // still zero, forces lookup exhaustion
	}

	// Expect panic due to missing entry
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from exhausted addrToPairId in lookupPairID")
		}
	}()
	_ = lookupPairID(addr)
}
