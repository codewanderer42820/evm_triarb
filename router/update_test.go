// router/update_test.go — exhaustive unit tests for hot‑path helpers.
// Note: lives in `package router` to access unexported symbols.
package router

import (
	"runtime"
	"testing"

	"main/ring"
	"main/types"
	"main/utils"
)

// ----------------------------------------------------------------------------
// Stub for parser output struct expected by RouteUpdate.
// ----------------------------------------------------------------------------
// drain pops until ring empty; returns number of popped elements.
func drain(r *ring.Ring) (n int) {
	for {
		if p := r.Pop(); p == nil {
			return
		}
		n++
	}
}

// ----------------------------------------------------------------------------
// 1. RegisterPair + lookupPairID basic round‑trip.
// ----------------------------------------------------------------------------
func TestRegisterAndLookupPair(t *testing.T) {
	addr := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	RegisterPair(addr, 42)
	if got := lookupPairID(addr); got != 42 {
		t.Fatalf("lookupPairID → %d, want 42", got)
	}
}

// Duplicate insert should not panic; first mapping must stay.
func TestRegisterDuplicateInsert(t *testing.T) {
	addr := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	RegisterPair(addr, 1)
	RegisterPair(addr, 2) // probes to free slot
	if got := lookupPairID(addr); got != 1 {
		t.Fatalf("duplicate insert overwrote id: got %d, want 1", got)
	}
}

// ----------------------------------------------------------------------------
// 2. Fan‑out bitmap → pushes.
// ----------------------------------------------------------------------------
func TestRouteUpdateFansOutToAllMarkedCores(t *testing.T) {
	// Setup rings for first 4 cores
	for i := 0; i < 4; i++ {
		cpuRingsGlobal[i] = ring.New(8)
	}

	const pairID = 7
	routingBitmap[pairID] = 0x000F // cores 0‑3

	addr40 := []byte("cccccccccccccccccccccccccccccccccccccccc")
	RegisterPair(addr40, pairID)

	// reserves: r0=1, r1=2 (non‑zero hex, 32 bytes each → 64 total)
	reserves := []byte("00000000000000010000000000000002")
	reserves = append(reserves, reserves...)

	v := &types.LogView{
		Addr: append([]byte{'"', '0', 'x'}, append(addr40, '"')...),
		Data: reserves,
	}

	RouteUpdate(v)
	runtime.Gosched()

	for i := 0; i < 4; i++ {
		if n := drain(cpuRingsGlobal[i]); n != 1 {
			t.Fatalf("core %d push count = %d, want 1", i, n)
		}
	}
}

// ----------------------------------------------------------------------------
// 3. lookupPairID panics on full wrap (missing address).
// ----------------------------------------------------------------------------
func TestLookupPairIDPanicsOnMissing(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on full‑wrap miss, got nil")
		}
	}()
	_ = lookupPairID([]byte("dddddddddddddddddddddddddddddddddddddddd"))
}

// ----------------------------------------------------------------------------
// 4. Hash17 distribution sanity on small sample.
// ----------------------------------------------------------------------------
func TestHash17UniformitySmallSample(t *testing.T) {
	buckets := make([]int, 1024)
	sample := [][]byte{
		[]byte("0000000000000000000000000000000000000000"),
		[]byte("1111111111111111111111111111111111111111"),
		[]byte("2222222222222222222222222222222222222222"),
		[]byte("3333333333333333333333333333333333333333"),
		[]byte("4444444444444444444444444444444444444444"),
		[]byte("5555555555555555555555555555555555555555"),
	}
	for _, a := range sample {
		buckets[utils.Hash17(a)>>7]++ // fold 17 bits → 10 bits
	}
	for i, v := range buckets {
		if v > 2 { // any bucket should hold ≤2 hits with 6 samples
			t.Fatalf("bucket %d has %d hits; hash looks biased", i, v)
		}
	}
}
