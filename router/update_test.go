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

func makeAddr40(seed byte) []byte {
	h := sha3.Sum256([]byte{seed})
	dst := make([]byte, 40)
	hex.Encode(dst, h[:20])
	return dst
}

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

	// Inject fake Sync log
	addr := append([]byte{'"', '0', 'x'}, append(a, '"')...)
	res := "00000000000000010000000000000002" + "00000000000000010000000000000002"

	v := &types.LogView{
		Addr: addr,
		Data: []byte(res),
	}

	RouteUpdate(v)
	runtime.Gosched()

	// Verify that one update was dispatched per core
	count := 0
	for i := 0; i < len(routers); i++ {
		if cpuRingsGlobal[i] == nil {
			t.Fatalf("missing ring on core %d", i)
		}
		n := drain(cpuRingsGlobal[i])
		if n != 1 {
			t.Fatalf("core %d received %d updates, want 1", i, n)
		}
		count += n
	}

	if count != len(routers) {
		t.Fatalf("total updates pushed = %d, want %d", count, len(routers))
	}
}

func TestFanoutTickPropagation(t *testing.T) {
	q := bucketqueue.New()
	path := &ArbPath{LegVal: [3]float64{0, -1, 0}} // Sum = -1
	h, _ := q.Borrow()
	_ = q.Push(0, h, unsafe.Pointer(path))

	rt := &CoreRouter{
		Buckets:     []*DeltaBucket{{CurLog: 0, Queue: q}},
		FanOut:      [][]fanRef{{{P: path, Q: q, H: h, SharedLeg: 1}}},
		PairToLocal: []uint32{0},
		isReverse:   true,
	}

	onPriceUpdate(rt, &PriceUpdate{
		PairId:  0,
		FwdTick: 9.9,
		RevTick: -2.5,
	})

	if rt.Buckets[0].CurLog != -2.5 {
		t.Fatalf("reverse tick not applied: got %.2f", rt.Buckets[0].CurLog)
	}
}
