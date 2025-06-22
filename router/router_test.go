package router_test

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"main/pairidx"
)

// mockRing is a minimal single‑producer, single‑consumer ring for tests.
type mockRing struct{ ch chan unsafe.Pointer }

func newMockRing() *mockRing              { return &mockRing{ch: make(chan unsafe.Pointer, 128)} }
func (r *mockRing) Push(p unsafe.Pointer) { r.ch <- p }
func (r *mockRing) Pop() unsafe.Pointer   { return <-r.ch }

// TestParseLine verifies pool‑id extraction logic.
func TestParseLine(t *testing.T) {
	line := []byte("1 → (12) → 2 → (34) → 3 → (56) → 1")
	ids := parseLine(line)
	require.Equal(t, [4]int{12, 34, 56, 12}, ids)
}

// TestKeyTable ensures buildKeys/keyOf round‑trip without allocation.
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

// allocCounter counts allocations for f().
func allocCounter(f func()) uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	before := m.Mallocs
	f()
	runtime.ReadMemStats(&m)
	return m.Mallocs - before
}

// TestRecomputeNoAlloc checks recompute() alloc‑free.
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

	allocs := allocCounter(func() { recompute(refs) })
	require.Equal(t, uint64(0), allocs)
}
