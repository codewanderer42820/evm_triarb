// router/update_test.go — end-to-end tests for router API and update logic
package router

import (
	"encoding/hex"
	"runtime"
	"testing"
	"unsafe"

	"main/bucketqueue"
	"main/ring"
	"main/types"

	"golang.org/x/crypto/sha3"
)

// makeAddr40 returns a deterministic 40-char hex address from Keccak256(seed).
func makeAddr40(seed byte) []byte {
	h := sha3.Sum256([]byte{seed})
	dst := make([]byte, 40)
	hex.Encode(dst, h[:20]) // 20 bytes → 40 hex chars
	return dst
}

// drain pops everything in a ring and returns the count.
func drain(r *ring.Ring) (n int) {
	for {
		if p := r.Pop(); p == nil {
			return
		}
		n++
	}
}

func TestRegisterCyclesAndRouteUpdate(t *testing.T) {
	InitCPURings()

	// Register mock token/pair mapping
	a := makeAddr40(1)
	b := makeAddr40(2)
	c := makeAddr40(3)

	RegisterPair(a, 100)
	RegisterPair(b, 200)
	RegisterPair(c, 300)

	// Register one cycle across all cores
	RegisterCycles([]Cycle{{
		Tokens: [3]uint32{1, 2, 3},
		Pairs:  [3]uint16{100, 200, 300},
	}})

	// Inject fake Sync log: r0=1, r1=2 (just non-zero values)
	hex32 := "00000000000000000000000000000001"
	res := hex32 + hex32 // 32chars+32chars = 64 bytes

	lv := &types.LogView{
		Addr: append([]byte{'"', '0', 'x'}, append(a, '"')...),
		Data: []byte(res),
	}

	RouteUpdate(lv)
	runtime.Gosched()

	// Verify that one update was dispatched per core
	total := 0
	for i := range coreRings {
		if coreRings[i] == nil {
			t.Fatalf("missing ring on core %d", i)
		}
		n := drain(coreRings[i])
		if n != 1 {
			t.Fatalf("core %d received %d updates, want 1", i, n)
		}
		total += n
	}
	if total != len(coreRings) {
		t.Fatalf("total updates = %d, want %d", total, len(coreRings))
	}
}

func TestFanoutTickPropagation(t *testing.T) {
	q := bucketqueue.New()
	path := &ArbPath{Ticks: [3]float64{0, -1, 0}} // Sum = -1
	h, _ := q.Borrow()
	_ = q.Push(0, h, unsafe.Pointer(path))

	rt := &CoreRouter{
		Routes:    []*DeltaBucket{{CurLog: 0, Queue: q}},
		Fanouts:   [][]fanRef{{{P: path, Q: q, H: h, SharedLeg: 1}}},
		PairIndex: make([]uint32, 2),
		IsReverse: true,
	}

	onPriceUpdate(rt, &PriceUpdate{
		PairId:  0,
		FwdTick: 9.9,
		RevTick: -2.5,
	})

	if rt.Routes[0].CurLog != -2.5 {
		t.Fatalf("reverse tick not applied: got %.2f", rt.Routes[0].CurLog)
	}
}

func TestMapL2ToBucket(t *testing.T) {
	cases := []struct {
		in   float64
		want int64
	}{
		{in: 100.0, want: buckets - 1}, // above clamp
		{in: -100.0, want: 0},          // below negative clamp
		{in: 0.0, want: zeroOff},       // middle
		{in: clampL2, want: int64(zeroOff + scaleMul*clampL2/scaleDiv)},
	}
	for _, c := range cases {
		if got := mapL2ToBucket(c.in); got != c.want {
			t.Errorf("mapL2ToBucket(%v) = %v; want %v", c.in, got, c.want)
		}
	}
}

func TestRegisterPairAndLookupPanics(t *testing.T) {
	// backup
	var backup [1 << 17]uint16
	copy(backup[:], addrToPairId[:])
	defer copy(addrToPairId[:], backup[:])

	// fill all slots
	for i := range addrToPairId {
		addrToPairId[i] = 1
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on full table")
		}
	}()
	RegisterPair(makeAddr40(42), 123)
}

func TestLookupMissingPanics(t *testing.T) {
	// backup
	var backup [1 << 17]uint16
	copy(backup[:], addrToPairId[:])
	for i := range addrToPairId {
		addrToPairId[i] = 0
	}
	defer copy(addrToPairId[:], backup[:])

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on missing address")
		}
	}()
	_ = lookupPairID(makeAddr40(99))
}

func TestRegisterRoute(t *testing.T) {
	const id = 4242
	const mask = 0xF0F0
	RegisterRoute(id, uint16(mask))
	if routingBitmap[id] != uint16(mask) {
		t.Fatalf("routingBitmap[%d] = %04x; want %04x", id, routingBitmap[id], mask)
	}
}
