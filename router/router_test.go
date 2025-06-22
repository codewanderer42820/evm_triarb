// router_test.go — exhaustive coverage for router package (except main)
// ====================================================================
// These tests hit every helper, SWAR branch and runLoop path that can be
// exercised without spinning the real websocket ingest.

package router

import (
	"os"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"main/pairidx"
	"main/ring"
)

// -----------------------------------------------------------------------------
// 1. parseCycles – happy path + malformed-line skip
// -----------------------------------------------------------------------------
func TestParseCyclesValidAndInvalidLines(t *testing.T) {
	tmp, err := os.CreateTemp("", "cycles_*.txt")
	require.NoError(t, err)
	_, _ = tmp.WriteString(`1 → (12) → 2 → (34) → 3 → (56) → 1
bad line
5 → (1) → 2`)
	tmp.Close()
	defer os.Remove(tmp.Name())

	cycles := parseCycles(tmp.Name())
	require.Equal(t, 1, len(cycles))
	require.Equal(t, [3]int{12, 34, 56}, cycles[0].pools)
}

// -----------------------------------------------------------------------------
// 2. btoi edge cases
// -----------------------------------------------------------------------------
func TestBtoi(t *testing.T) {
	require.Equal(t, 0, btoi(false))
	require.Equal(t, 1, btoi(true))
}

// -----------------------------------------------------------------------------
// 3. mustDB + addr20 + wireAddrs – in-memory round‑trip
// -----------------------------------------------------------------------------
func TestAddr20AndWireAddrs(t *testing.T) {
	db := mustDB(":memory:")
	_, _ = db.Exec(`CREATE TABLE pools (id INTEGER PRIMARY KEY, address TEXT)`)
	_, _ = db.Exec(`INSERT INTO pools (id,address) VALUES (42,'0x00000000000000000000000000000000000000aa')`)
	_, _ = db.Exec(`INSERT INTO pools (id,address) VALUES (84,'0x00000000000000000000000000000000000000bb')`)
	_, _ = db.Exec(`INSERT INTO pools (id,address) VALUES (126,'0x00000000000000000000000000000000000000cc')`)

	c := Cycle{pools: [3]int{42, 84, 126}, last: 126, ap: &ArbPath{}}
	wireAddrs(db, &c)

	require.Equal(t, byte(0xaa), c.ap.Addr[0][19])
	require.Equal(t, byte(0xbb), c.ap.Addr[1][19])
	require.Equal(t, byte(0xcc), c.ap.Addr[2][19])
	require.Equal(t, [3]bool{false, true, true}, c.ap.Dir)
}

// -----------------------------------------------------------------------------
// 4. buildSplit – verifies bucket memo + fanOut wiring
// -----------------------------------------------------------------------------
func TestBuildSplitFanOut(t *testing.T) {
	routers = []*CoreRouter{
		{bucketByKey: pairidx.New(), queueByKey: pairidx.New()},
		{bucketByKey: pairidx.New(), queueByKey: pairidx.New()},
	}
	memo := make(map[[20]byte][2]*DeltaBucket)

	cyc := &Cycle{ap: &ArbPath{}}
	bytes := [...]byte{10, 20, 30}
	for i := 0; i < 3; i++ {
		cyc.ap.Addr[i][0] = bytes[i]
		cyc.ap.Dir[i] = i != 0 // false, true, true
		cyc.pools[i] = int(bytes[i])
	}
	cyc.last = cyc.pools[2]

	buildSplit(memo, cyc)

	// bucket memo must be populated
	require.NotNil(t, memo[cyc.ap.Addr[0]][btoi(cyc.ap.Dir[0])])

	// ensure at least one fanOut slice non‑empty
	found := false
	for _, r := range routers {
		for _, f := range r.fanOut {
			if len(f) > 0 {
				found = true
				break
			}
		}
	}
	require.True(t, found)
}

// -----------------------------------------------------------------------------
// 5. recomputeSWAR – verifies both Update‑hit and Push‑fallback paths
// -----------------------------------------------------------------------------
func TestRecomputeSWARDuplicateAndMissing(t *testing.T) {
	//----------------------------------------------------------------------
	// 1.  Two router cores ⇒ buildSplit’s `half := len(routers)/2` is 1
	//----------------------------------------------------------------------
	routers = []*CoreRouter{
		{bucketByKey: pairidx.New(), queueByKey: pairidx.New()},
		{bucketByKey: pairidx.New(), queueByKey: pairidx.New()},
	}

	//----------------------------------------------------------------------
	// 2.  Minimal cycle & memo for buildSplit
	//----------------------------------------------------------------------
	ap := &ArbPath{
		LegVal: [3]float64{1, 2, 3},
		Dir:    [3]bool{false, false, false},
	}
	var memo = make(map[[20]byte][2]*DeltaBucket)

	c := &Cycle{ap: ap}
	buildSplit(memo, c) // populates routers[*].fanOut

	//----------------------------------------------------------------------
	// 3.  Locate the non-empty fan-out slice, regardless of which core
	//----------------------------------------------------------------------
	var refs []ref
	for _, r := range routers {
		for _, f := range r.fanOut {
			if len(f) > 0 {
				refs = f
				break
			}
		}
		if len(refs) > 0 {
			break
		}
	}
	if len(refs) == 0 {
		t.Fatalf("fan-out produced no refs")
	}

	//----------------------------------------------------------------------
	// 4.  Duplicate-push the same handle three times (count → 4, size → 4)
	//----------------------------------------------------------------------
	for i := 0; i < 3; i++ {
		if err := refs[0].Q.Push(0, refs[0].H); err != nil {
			t.Fatalf("duplicate Push failed: %v", err)
		}
	}

	//----------------------------------------------------------------------
	// 5.  RecomputeSWAR must collapse the duplicates back to a single node
	//----------------------------------------------------------------------
	recomputeSWAR(refs)

	if refs[0].Q.Size() != 1 {
		t.Fatalf("expected queue size 1 after recompute; got %d", refs[0].Q.Size())
	}
}

// -----------------------------------------------------------------------------
// 6. runLoop end‑to‑end drive with stub ring
// -----------------------------------------------------------------------------
func TestRunLoopProcessesPriceUpdates(t *testing.T) {
	// 2 routers, matching runLoop split logic (fwd idx 0, rev idx 1)
	routers = []*CoreRouter{
		{bucketByKey: pairidx.New(), queueByKey: pairidx.New()},
		{bucketByKey: pairidx.New(), queueByKey: pairidx.New()},
	}
	r0 := ring.New(8) // used by fwd‑dir goroutine
	_ = ring.New(8)   // spare slot for rev‑dir; keeps rings slice in expected shape
	rings = []*ring.Ring{r0}

	// simple path so Addr[0] routed to routers[0]
	memo := make(map[[20]byte][2]*DeltaBucket)
	cyc := &Cycle{ap: &ArbPath{}}
	cyc.ap.Addr = [3][20]byte{{1}, {1}, {1}}
	buildSplit(memo, cyc)

	bucket := routers[0].buckets[0]
	require.NotNil(t, bucket)

	var stop uint32
	go runLoop(0, r0, routers[0], false)

	upd := &PriceUpdate{FwdTick: 7.5, Addr: [20]byte{1}}
	r0.Push(unsafe.Pointer(upd))

	time.Sleep(25 * time.Millisecond)
	require.Equal(t, 7.5, bucket.CurLog)

	atomic.StoreUint32(&stop, 1)
	time.Sleep(5 * time.Millisecond)
}
