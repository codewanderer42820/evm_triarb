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
	"runtime"
	"sync"
	"sync/atomic"
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

/*───────────────────────────── RegisterRoute ─────────────────────────────*/

// TestRegisterRoute verifies that RegisterRoute correctly sets the CPU‑bit
// inside the package‑level pair2cores bitmap.
func TestRegisterRoute(t *testing.T) {
	t.Parallel()
	resetGlobals()

	const (
		pid  = PairID(777)
		core = 9
	)

	RegisterRoute(pid, core)

	if pair2cores[pid]&(1<<core) == 0 {
		t.Fatalf("RegisterRoute did not set bit %d for pair %d", core, pid)
	}
}

/*────────────────────────── lookupPairID collision ───────────────────────*/

// TestLookupPairIDCollision crafts two addresses that hash to the same bucket
// (identical low‑order bits) so that open‑addressing must probe forward. Both
// must still be retrievable.
func TestLookupPairIDCollision(t *testing.T) {
	t.Parallel()
	resetGlobals()

	// Generate a deterministic address and create a second that collides in
	// the lowest 6 bits used by utils.Hash17’s stride walk.
	a1 := randHex40()
	a2 := make([]byte, len(a1))
	copy(a2, a1)
	a2[len(a2)-1] ^= 0b0100_0000 // flip a high bit to force collision

	const (
		pid1 = PairID(11)
		pid2 = PairID(22)
	)

	RegisterPair(a1, pid1)
	RegisterPair(a2, pid2)

	if got := lookupPairID(a1); got != pid1 {
		t.Fatalf("lookupPairID collision mismatch for addr1: got %d, want %d", got, pid1)
	}
	if got := lookupPairID(a2); got != pid2 {
		t.Fatalf("lookupPairID collision mismatch for addr2: got %d, want %d", got, pid2)
	}
}

/*──────────────────────── helpers & scaffolding ───────────────────────*/

// newExec returns a minimally‑wired *CoreExecutor that owns exactly ONE local
// pair (pid = 1).  The single CycleState lives in an arena‑style slice so its
// address does not move.  The executor’s heap contains a single handle for
// that CycleState and Fanouts[0] contains one entry that overwrites Ticks[1].
func newExec() (*CoreExecutor, *CycleState) {
	ex := &CoreExecutor{
		Heaps:    make([]bucketqueue.Queue, 1),
		Fanouts:  make([][]FanoutEntry, 1),
		LocalIdx: localidx.New(16),
	}

	// One min‑heap per local pair.
	hq := &ex.Heaps[0]

	// One CycleState backed by a grow‑only slice so the pointer stays stable.
	buf := make([]CycleState, 1)
	cs := &buf[0]

	// Borrow a handle and enqueue the CycleState with an arbitrary key.
	h, _ := hq.Borrow()
	_ = hq.Push(0, h, unsafe.Pointer(cs))

	// Wire the local‑pair index.
	ex.LocalIdx.Put(uint32(1), 0)

	// Fanout entry updates edge #1 (index 1) of the CycleState.
	ex.Fanouts[0] = append(ex.Fanouts[0], FanoutEntry{
		State:   cs,
		Queue:   hq,
		Handle:  h,
		EdgeIdx: 1,
	})
	return ex, cs
}

/*────────────────────────────  Unit tests  ────────────────────────────*/

// TestAttachShard exercises attachShard directly, verifying that it correctly
// grows the executor’s heap/fanout slices and that every CycleState ends up in
// the heap with a placeholder key.
func TestAttachShard(t *testing.T) {
	t.Parallel()
	resetGlobals()

	ex := &CoreExecutor{
		Heaps:    make([]bucketqueue.Queue, 0, 4),
		Fanouts:  make([][]FanoutEntry, 0, 4),
		LocalIdx: localidx.New(32),
	}
	cycleBuf := make([]CycleState, 0, 4)

	// Craft a single‑bin PairShard for pair = 2.
	shard := PairShard{
		Pair: 2,
		Bins: []EdgeBinding{{Pairs: PairTriplet{2, 3, 4}, EdgeIdx: 0}},
	}

	t.Logf("[INIT] Attaching shard for PairID %d with %d bin(s)", shard.Pair, len(shard.Bins))
	attachShard(ex, &shard, &cycleBuf)
	t.Logf("[POST] Total heaps: %d", len(ex.Heaps))
	t.Logf("[POST] Total fanouts: %d", len(ex.Fanouts))
	t.Logf("[POST] CycleState buffer len: %d", len(cycleBuf))

	// Resolve correct heap index (local ID)
	lid32, ok := ex.LocalIdx.Get(uint32(shard.Pair))
	if !ok {
		t.Fatalf("LocalIdx missing mapping for pair %d", shard.Pair)
	}
	lid := uint32(lid32)
	t.Logf("[LOOKUP] LocalIdx returned lid=%d for PairID=%d", lid, shard.Pair)

	if int(lid) >= len(ex.Heaps) {
		t.Fatalf("LocalIdx returned lid=%d but Heaps only has %d entries", lid, len(ex.Heaps))
	}
	hq := &ex.Heaps[lid]

	if hq.Empty() {
		t.Fatalf("Heap at lid=%d is unexpectedly empty after attach", lid)
	}

	t.Logf("[PRE-POP] Heap size = %d", hq.Size())
	h, key, ptr := hq.PopMin()
	t.Logf("[POP] h=%v key=%d ptr=%v", h, key, ptr)

	if ptr == nil {
		t.Logf("[FAIL] ptr is nil — likely handle was invalid or Push failed silently.")
	} else {
		cs := (*CycleState)(ptr)
		t.Logf("[DATA] CycleState: ticks=%v pairs=%v", cs.Ticks, cs.Pairs)
	}

	if ptr == nil || key != 4095 {
		t.Fatalf("heap entry corrupt: key=%d ptr=%v h=%v", key, ptr, h)
	}
}

// TestHandleTickForwardReverse drives handleTick through both forward and
// reverse polarity branches and both profit / non‑profit paths.
func TestHandleTickForwardReverse(t *testing.T) {
	t.Parallel()
	resetGlobals()

	ex, cs := newExec() // fresh executor with PairID==1

	// 1️⃣ Forward core, profitable (tick negative ⇒ profit<0).
	upd := TickUpdate{Pair: 1, FwdTick: -10, RevTick: +10}
	handleTick(ex, &upd)
	if got := cs.Ticks[1]; got != -10 {
		t.Fatalf("forward update failed: got tick %.1f want -10", got)
	}

	// 2️⃣ Forward core, non‑profitable (tick positive ⇒ profit>=0).
	upd2 := TickUpdate{Pair: 1, FwdTick: +5, RevTick: -5}
	handleTick(ex, &upd2)
	if got := cs.Ticks[1]; got != +5 {
		t.Fatalf("forward non‑profit update failed: got tick %.1f want +5", got)
	}

	// 3️⃣ Reverse polarity — same executor reused.
	ex.IsReverse = true // flip polarity so handleTick uses RevTick
	upd3 := TickUpdate{Pair: 1, FwdTick: +99, RevTick: -99}
	handleTick(ex, &upd3)
	if got := cs.Ticks[1]; got != -99 {
		t.Fatalf("reverse update failed: got tick %.1f want -99", got)
	}
}

// TestRegisterRouteBitmask verifies that the per‑pair bitmap is updated with
// the correct core bit.
func TestRegisterRouteBitmask(t *testing.T) {
	t.Parallel()
	resetGlobals()

	const (
		pid  = PairID(7)
		core = uint8(3)
	)
	RegisterRoute(pid, core)
	if pair2cores[pid]&(1<<core) == 0 {
		t.Fatalf("RegisterRoute failed to set bit: bitmap=%064b", pair2cores[pid])
	}
}

// TestHandleTickConcurrent spawns many goroutines, each with its own executor,
// to ensure the hot path remains data‑race free.  Errors are aggregated via an
// atomic flag so that T.Fatalf executes on the parent goroutine only.
func TestHandleTickConcurrent(t *testing.T) {
	const goroutines = 32
	var wg sync.WaitGroup
	var fail uint32

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			ex, cs := newExec()
			ex.IsReverse = idx%2 == 1 // alternate polarity

			tick := float64(idx)
			upd := TickUpdate{Pair: 1, FwdTick: tick, RevTick: -tick}
			handleTick(ex, &upd)

			want := tick
			if ex.IsReverse {
				want = -tick
			}
			if cs.Ticks[1] != want {
				atomic.StoreUint32(&fail, 1)
			}
		}(i)
	}
	wg.Wait()
	if fail != 0 {
		t.Fatalf("concurrent handleTick produced inconsistent results")
	}

	// Sanity‑check that goroutines did not leak OS threads excessively.
	if got := runtime.NumGoroutine(); got > goroutines+16 { // generous headroom
		t.Fatalf("goroutine leak detected: %d live goroutines", got)
	}
}
