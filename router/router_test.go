package router

import (
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"main/pairidx"
)

// -----------------------------------------------------------------------------
// 1. parseCycles – happy path + malformed‑line skip
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
// 2. mustDB + addr20 – in‑memory round‑trip
// -----------------------------------------------------------------------------
func TestMustDBAndAddr20(t *testing.T) {
	db := mustDB(":memory:")
	_, _ = db.Exec(`CREATE TABLE pools (id INTEGER PRIMARY KEY, address TEXT)`)
	_, _ = db.Exec(`INSERT INTO pools (id,address) VALUES (1,'0x00000000000000000000000000000000000000ff')`)

	out := addr20(db, 1)
	require.Equal(t, byte(0xff), out[19])
}

// -----------------------------------------------------------------------------
// 3. buildSplit – verifies bucket + fanOut wiring
// -----------------------------------------------------------------------------
func TestBuildSplitFanOut(t *testing.T) {
	// two routers (dir=false → 0, dir=true → 1)
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

	// at least one router should now have fanOut entries
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
// 4. SWAR recompute through production path (uses buildSplit result)
// -----------------------------------------------------------------------------
func TestRecomputeSWARThroughBuildSplit(t *testing.T) {
	routers = []*CoreRouter{
		{bucketByKey: pairidx.New(), queueByKey: pairidx.New()},
		{bucketByKey: pairidx.New(), queueByKey: pairidx.New()},
	}
	memo := make(map[[20]byte][2]*DeltaBucket)

	cyc := &Cycle{ap: &ArbPath{LegVal: [3]float64{1, 2, 3}}}
	cyc.ap.Addr = [3][20]byte{{42}, {84}, {126}}
	cyc.ap.Dir = [3]bool{false, true, true}
	cyc.pools = [3]int{42, 84, 126}
	cyc.last = 126

	buildSplit(memo, cyc)

	// grab refs from whichever router got populated
	var refs []ref
	for _, r := range routers {
		for _, f := range r.fanOut {
			if len(f) > 0 {
				refs = f
				break
			}
		}
	}
	require.NotEmpty(t, refs)

	// ensure handle is heap‑registered before Update() inside SWAR
	q := refs[0].Q
	h := refs[0].H
	q.Push(1, h) // minimal priority – safe duplicate Push is ignored by queue when already present

	// recompute should not panic even if queue internals shift
	require.NotPanics(t, func() { recomputeSWAR(refs) })
}
