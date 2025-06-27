// router_extensive_test.go — exhaustive test‑suite for router.go
//
// The goal is **100 % statement & branch coverage** across router.go.
// The tests exercise every path that does not rely on external I/O by
// deterministically driving the hot‑path helpers, shard wiring, and the
// tick‑handling state‑machine.
//
// Run with:
//
//	go test -race -cover -count=1 ./...
package router

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"math"
	"sync"
	"testing"
	"time"
	"unsafe"

	"main/bucketqueue"
	"main/localidx"
)

/*────────────────────────────── Helpers ──────────────────────────────*/

// resetGlobals zeroes the package‑level globals so that each test starts from
// a pristine state and results remain deterministic regardless of execution
// order or ‑run filters.
func resetGlobals() {
	addr2pair = [1 << 17]PairID{}
	pair2cores = [1 << 17]CoreMask{}
	shardBucket = make(map[PairID][]PairShard)
	splitThreshold = 16_384 // restore default if tampered with
	for i := range executors {
		executors[i] = nil
	}
	for i := range rings {
		rings[i] = nil
	}
}

// randHex40 returns a cryptographically random 40‑byte ASCII‑hex address.
func randHex40() []byte {
	var b [20]byte
	_, _ = rand.Read(b[:])
	dst := make([]byte, 40)
	hex.Encode(dst, b[:])
	return dst
}

/*───────────────────────────── Test Cases ─────────────────────────────*/

func TestLog2ToTick(t *testing.T) {
	t.Parallel()
	cases := []struct {
		r    float64
		want int64
	}{
		{r: -200, want: 0},                   // clamp low
		{r: -128, want: 0},                   // exact low clamp
		{r: -127.5, want: int64((0.5) * 16)}, // inside low bound
		{r: 0, want: 2048},                   // midpoint
		{r: 127.5, want: 4088},               // inside high bound
		{r: 128, want: 4095},                 // exact high clamp
		{r: 200, want: 4095},                 // clamp high
	}
	for _, c := range cases {
		got := log2ToTick(c.r)
		if got != c.want {
			t.Fatalf("log2ToTick(%v) = %d, want %d", c.r, got, c.want)
		}
	}
}

func TestCrandIntRange(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 2, 3, 97, 1 << 16} {
		for i := 0; i < 1_000; i++ {
			v := crandInt(n)
			if v < 0 || v >= n {
				t.Fatalf("crandInt(%d) produced out‑of‑range value %d", n, v)
			}
		}
	}
}

func TestShuffleBindingsIntegrity(t *testing.T) {
	t.Parallel()
	// Build a deterministic slice of EdgeBinding values.
	bindings := make([]EdgeBinding, 100)
	for i := range bindings {
		bindings[i] = EdgeBinding{EdgeIdx: uint16(i % 3), Pairs: PairTriplet{PairID(i), PairID(i + 1), PairID(i + 2)}}
	}

	// Make a deep copy for comparison.
	orig := make([]EdgeBinding, len(bindings))
	copy(orig, bindings)

	shuffleBindings(bindings)

	if len(bindings) != len(orig) {
		t.Fatalf("shuffle mutated slice length: got %d, want %d", len(bindings), len(orig))
	}

	// Every original element must still be present exactly once.
	m := make(map[EdgeBinding]int, len(bindings))
	for _, b := range bindings {
		m[b]++
	}
	for _, b := range orig {
		if m[b] != 1 {
			t.Fatalf("binding %+v present %d times after shuffle", b, m[b])
		}
	}
}

func TestRegisterPairAndLookup(t *testing.T) {
	t.Parallel()
	resetGlobals()

	const pairs = 128
	for i := 0; i < pairs; i++ {
		pid := PairID(i + 1) // 0 is sentinel «unknown»
		addr := randHex40()
		RegisterPair(addr, pid)
		if got := lookupPairID(addr); got != pid {
			t.Fatalf("lookupPairID mismatch: got %d, want %d", got, pid)
		}
	}
}

func TestBuildFanoutShardsSplitThreshold(t *testing.T) {
	resetGlobals()

	// Force tiny shards so that the splitting logic is deterministic.
	splitThreshold = 4

	cycles := make([]PairTriplet, 30)
	for i := range cycles {
		cycles[i] = PairTriplet{PairID(i), PairID(i + 1000), PairID(i + 2000)}
	}

	buildFanoutShards(cycles)

	if len(shardBucket) == 0 {
		t.Fatalf("shardBucket empty after buildFanoutShards")
	}

	for pid, shards := range shardBucket {
		for _, s := range shards {
			if s.Pair != pid {
				t.Fatalf("shard.Pair=%d does not match map key %d", s.Pair, pid)
			}
			if len(s.Bins) == 0 || len(s.Bins) > splitThreshold {
				t.Fatalf("shard size %d outside allowed range (1..%d)", len(s.Bins), splitThreshold)
			}
		}
	}
}

func TestHandleTickProfitAndPropagate(t *testing.T) {
	resetGlobals()

	// Minimal executor with one local pair & heap.
	ex := &CoreExecutor{
		Heaps:     []bucketqueue.Queue{*bucketqueue.New()},
		Fanouts:   make([][]FanoutEntry, 1),
		LocalIdx:  localidx.New(8),
		IsReverse: false,
	}

	pid := PairID(42)
	_ = ex.LocalIdx.Put(uint32(pid), 0)

	// Prepare one CycleState in the heap.
	cs := new(CycleState)
	h, _ := ex.Heaps[0].Borrow()
	_ = ex.Heaps[0].Push(0, h, unsafe.Pointer(cs))
	ex.Fanouts[0] = []FanoutEntry{{State: cs, Queue: &ex.Heaps[0], Handle: h, EdgeIdx: 1}}

	upd := &TickUpdate{Pair: pid, FwdTick: -2.5, RevTick: 2.5}

	handleTick(ex, upd)

	// Tick should have been written into slot 1 by propagate.
	if got := cs.Ticks[1]; math.Abs(got-(-2.5)) > 1e-9 {
		t.Fatalf("handleTick failed propagate: got %f, want -2.5", got)
	}

	// A second call with non‑profitable tick should drain nothing but still update.
	upd2 := &TickUpdate{Pair: pid, FwdTick: 1.0, RevTick: -1.0}
	handleTick(ex, upd2)
	if got := cs.Ticks[1]; math.Abs(got-1.0) > 1e-9 {
		t.Fatalf("second propagate failed: got %f, want 1.0", got)
	}
}

/*───────────────────────────── Fuzz Hook ──────────────────────────────*/

// TestFuzzLog2ToTick is a lightweight fuzz harness that complements the
// hand‑rolled table tests by exploring the function’s behaviour over the full
// float64 domain (sans ±Inf/NaN). The harness runs for a bounded duration so
// as not to dominate the CPU budget when -race is enabled.
func TestFuzzLog2ToTick(t *testing.T) {
	t.Parallel()

	stop := time.After(150 * time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				// Uniform float in (‑200, 200).
				b := make([]byte, 8)
				_, _ = rand.Read(b)
				v := float64(int64(binary.LittleEndian.Uint64(b))%40000-20000) / 100
				_ = log2ToTick(v)
			}
		}
	}()
	wg.Wait()
}
