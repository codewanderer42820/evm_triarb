// router/update_test.go — comprehensive test suite for router fast‑path helpers.
// Run with `go test ./router`.
package router

import (
	"encoding/hex"
	"runtime"
	"testing"
	"unsafe"

	"main/bucketqueue"
	"main/ring"
	"main/types"
	"main/utils"

	"golang.org/x/crypto/sha3"
)

// makeAddr40 returns a deterministic 40‑char hex address from Keccak256(seed).
func makeAddr40(seed byte) []byte {
	h := sha3.Sum256([]byte{seed})
	dst := make([]byte, 40)
	hex.Encode(dst, h[:20]) // 20 bytes → 40 hex
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

// ───────────────────────── 1. RegisterRoute stores mask ─────────────────────
func TestRegisterRouteStoresMask(t *testing.T) {
	const id = 4242
	const mask = uint16(0x0F0F)
	prev := routingBitmap[id]
	RegisterRoute(id, mask)
	if routingBitmap[id] != mask {
		t.Fatalf("routingBitmap[%d] = %04x, want %04x", id, routingBitmap[id], mask)
	}
	routingBitmap[id] = prev // restore for other tests
}

// ───────────────────────── 2. Bulk register + lookup ─────────────────────────
func TestBulkRegisterLookup(t *testing.T) {
	for i := 0; i < 200; i++ {
		RegisterPair(makeAddr40(byte(i)), uint16(i+1))
	}
	for i := 0; i < 200; i++ {
		if id := lookupPairID(makeAddr40(byte(i))); id != uint16(i+1) {
			t.Fatalf("idx %d → id %d want %d", i, id, i+1)
		}
	}
}

// ───────────── 3. Duplicate insert keeps first mapping intact ───────────────
func TestDuplicateInsertKeepsFirst(t *testing.T) {
	a := makeAddr40(201)
	RegisterPair(a, 55)
	RegisterPair(a, 99) // probes to a different slot
	if id := lookupPairID(a); id != 55 {
		t.Fatalf("duplicate insert overwrote: %d", id)
	}
}

// ───────────── 4. mapL2ToBucket clamp edge‑cases ────────────────────────────
func TestMapL2Clamp(t *testing.T) {
	cases := []struct {
		in   float64
		want int64
	}{{-100, 1}, {-64, 1}, {0, 2048}, {63.99, 4095}, {150, 4095}}
	for _, c := range cases {
		if got := mapL2ToBucket(c.in); got != c.want {
			t.Fatalf("mapL2(%f) => %d want %d", c.in, got, c.want)
		}
	}
}

// ───────────── 5. Fan‑out pushes one element to every marked core ───────────
func TestFanOutPushesAll(t *testing.T) {
	for i := 0; i < 16; i++ {
		cpuRingsGlobal[i] = ring.New(8)
	}
	const pairID = 300
	routingBitmap[pairID] = 0xFFFF // all 16 cores
	addr := makeAddr40(202)
	RegisterPair(addr, pairID)

	res := "00000000000000010000000000000002" +
		"00000000000000010000000000000002" // 64‑hex reserves

	lv := &types.LogView{
		Addr: append([]byte{'"', '0', 'x'}, append(addr, '"')...),
		Data: []byte(res),
	}

	RouteUpdate(lv)
	runtime.Gosched()

	for i := 0; i < 16; i++ {
		if n := drain(cpuRingsGlobal[i]); n != 1 {
			t.Fatalf("core %d push count %d want 1", i, n)
		}
	}
}

// ───────────── 6. onPriceUpdate mutates shared leg & reverse tick ─────────────
func TestOnPriceUpdateLogic(t *testing.T) {
	q := bucketqueue.New()
	path := &ArbPath{LegVal: [3]float64{0, -1.0, 0}} // Sum() = -1
	h, _ := q.Borrow()
	_ = q.Push(0, h, unsafe.Pointer(path))

	rt := &CoreRouter{
		buckets:     []*DeltaBucket{{CurLog: 0, Queue: q}},
		fanOut:      [][]fanRef{{{P: path, Q: q, H: h, SharedLeg: 2}}},
		pairToLocal: []uint32{0, 0},
		isReverse:   true,
	}

	onPriceUpdate(rt, &PriceUpdate{PairId: 1, FwdTick: 9.9, RevTick: -2.5})

	if rt.buckets[0].CurLog != -2.5 { // reverse branch must pick RevTick
		t.Fatalf("reverse tick not applied: got %.2f", rt.buckets[0].CurLog)
	}

}

// ───────────── 7. RegisterPair panics when table full ───────────────────────
func TestRegisterPairFullTablePanics(t *testing.T) {
	// Backup table
	var backup [1 << 17]uint16
	copy(backup[:], addrToPairId[:])
	defer copy(addrToPairId[:], backup[:])

	// Fill every slot with 1
	for i := range addrToPairId {
		addrToPairId[i] = 1
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("RegisterPair did not panic on full table")
		}
	}()
	RegisterPair(makeAddr40(251), 123)
}

// ───────────── 8. lookupPairID panics on full‑wrap miss ───────────────────── lookupPairID panics on full‑wrap miss ─────────────────────
func TestLookupMissingPanics(t *testing.T) {
	var backup [1 << 17]uint16
	copy(backup[:], addrToPairId[:])
	for i := range addrToPairId {
		addrToPairId[i] = 0
	}
	defer copy(addrToPairId[:], backup[:])

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("lookupPairID did not panic on missing address")
		}
	}()
	_ = lookupPairID(makeAddr40(250))
}

// ───────────── 9. Hash17 distribution sanity (fold 17→8 bits) ───────────────
func TestHash17Uniformity8(t *testing.T) {
	buckets := make([]int, 256)
	for i := 0; i < 1000; i++ {
		b := utils.Hash17(makeAddr40(byte(i))) >> 9 // 17→8 bits
		buckets[b]++
	}
	for i, v := range buckets {
		if v > 22 { // allow heavier tail up to 22
			t.Fatalf("bucket %d heavy tail: %d hits (>20)", i, v)
		}
	}
}
