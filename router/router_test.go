package router

import (
	"database/sql"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"

	"main/pairidx"
	"main/ring"
)

type mockRing struct{ ch chan unsafe.Pointer }

func newMockRing() *mockRing              { return &mockRing{ch: make(chan unsafe.Pointer, 128)} }
func (r *mockRing) Push(p unsafe.Pointer) { r.ch <- p }
func (r *mockRing) Pop() unsafe.Pointer   { return <-r.ch }

func TestParseLine(t *testing.T) {
	line := []byte("1 → (12) → 2 → (34) → 3 → (56) → 1")
	ids := parseLine(line)
	require.Equal(t, [3]int{12, 34, 56}, ids)
}

func TestKeyTable(t *testing.T) {
	var addr [20]byte
	addr[0] = 0xaa
	buildKeys(addr)
	kF := keyOf(&addr, Fwd)
	kR := keyOf(&addr, Rev)
	require.NotEmpty(t, kF)
	require.NotEmpty(t, kR)
	require.NotEqual(t, kF, kR)
}

func allocCounter(f func()) uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	before := m.Mallocs
	f()
	runtime.ReadMemStats(&m)
	return m.Mallocs - before
}

func TestRecomputeNoAlloc(t *testing.T) {
	workers := 1
	memo := map[[21]byte]*DeltaBucket{}
	rtr := []*CoreRouter{{bucketByKey: pairidx.New(), queueByKey: pairidx.New()}}
	p := struct {
		pools [3]int
		last  int
		ap    *ArbPath
	}{}

	build(workers, memo, rtr, p)

	var refs []ref
	for _, fs := range rtr[0].fanOut {
		refs = append(refs, fs...)
	}

	allocs := allocCounter(func() { recomputeSWAR(refs) })
	require.Equal(t, uint64(0), allocs)
}

func TestBtoi(t *testing.T) {
	require.Equal(t, 1, btoi(true))
	require.Equal(t, 0, btoi(false))
}

func TestFastAtoi(t *testing.T) {
	require.Equal(t, 0, fastAtoi([]byte("0")))
	require.Equal(t, 42, fastAtoi([]byte("42")))
	require.Equal(t, 123456, fastAtoi([]byte("123456")))
}

func TestBuildSplit(t *testing.T) {
	routers = []*CoreRouter{
		{bucketByKey: pairidx.New(), queueByKey: pairidx.New()},
		{bucketByKey: pairidx.New(), queueByKey: pairidx.New()},
	}
	c := &Cycle{
		ap: &ArbPath{
			Addr: [3][20]byte{{1}, {2}, {3}},
			Dir:  [3]bool{false, true, true},
		},
	}
	buildSplit(make(map[[20]byte][2]*DeltaBucket), c)
}

func TestWireAddrsAndAddr20(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE pools (id INTEGER PRIMARY KEY, address TEXT);`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO pools (id, address) VALUES (1, '0x0000000000000000000000000000000000000001');`)
	require.NoError(t, err)

	c := &Cycle{
		pools: [3]int{1, 1, 1},
		last:  1,
		ap:    &ArbPath{},
	}
	wireAddrs(db, c)

	out := addr20(db, 1)
	require.Equal(t, byte(1), out[19])
}

func TestRunLoop(t *testing.T) {
	routers = []*CoreRouter{
		{
			bucketByKey: pairidx.New(),
			queueByKey:  pairidx.New(),
			buckets:     []*DeltaBucket{{}},
			fanOut:      [][]ref{{}},
		},
	}
	key := strings.Repeat("\x01", 20)
	routers[0].bucketByKey.Put(key, 0)
	routers[0].queueByKey.Put(key, 0)

	r := ring.New(8)
	var u PriceUpdate
	u.Addr[19] = 1
	u.FwdTick = 3.3
	r.Push(unsafe.Pointer(&u))

	go runLoop(0, r, routers[0], false)
	runtime.Gosched()
}

func TestParseCyclesRealFile(t *testing.T) {
	cycles := parseCycles("../cycles_3_3.txt")
	require.GreaterOrEqual(t, len(cycles), 10)

	want := []struct {
		pools [3]int
		last  int
	}{
		{[3]int{1, 142, 4}, 4},
		{[3]int{4, 12, 61032}, 61032},
		{[3]int{4, 10, 3010}, 3010},
		{[3]int{12, 1, 8}, 8},
		{[3]int{12, 10, 2766}, 2766},
		{[3]int{12, 3334, 16}, 16},
		{[3]int{12, 866, 236}, 236},
		{[3]int{12, 212, 218}, 218},
		{[3]int{12, 2675, 3107}, 3107},
		{[3]int{4, 221, 1811}, 1811},
	}

	for i, w := range want {
		require.Equal(t, w.pools, cycles[i].pools, "cycle %d pools mismatch", i)
		require.Equal(t, w.last, cycles[i].last, "cycle %d last mismatch", i)
	}
}

func TestMustDB(t *testing.T) {
	db := mustDB(":memory:")
	require.NotNil(t, db)
}
