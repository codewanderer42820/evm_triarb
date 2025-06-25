// Full‑coverage tests for the router package.
// Run: go test -cover ./...
package router

import (
	"math/rand"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"main/bucketqueue"
	"main/ring"
	"main/types"
)

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// unit‑tests
// -----------------------------------------------------------------------------

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
		idx := int(rt.PairIndex[10]) // direct bucket index stored
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
	lv.Addr = append([]byte{0, 0, 0}, addr...) // pad to 43‑byte layout
	lv.Data = []byte("0000000000000000000000000000007b" +
		"000000000000000000000000000001c8") // reserve0=123, reserve1=456

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
