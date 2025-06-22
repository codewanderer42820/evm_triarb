package pairidx

import (
	"fmt"
	"hash/crc32"
	"math/bits"
	"sync"
	"testing"
	"unsafe"
)

// hash16Inline replicates the map's internal hashing logic for testing purposes.
// Returns the key pointer, key length, low 16 bits of CRC32, and the 8-bit tag.
func hash16Inline(key string) (uintptr, uintptr, uint16, byte) {
	bytePtr := unsafe.StringData(key)
	ptr := uintptr(unsafe.Pointer(bytePtr))
	length := uintptr(len(key))

	// CRC32 hash using the same Castagnoli table as production code
	crc := crc32.Update(0, crcTab, unsafe.Slice(bytePtr, int(length)))
	low := uint16(crc)
	tag := byte((crc>>16)&0xFF) | 1
	return ptr, length, low, tag
}

// fillCluster tries to generate `clusterSlots + 1` keys that hash into the same cluster.
// This is used to test cluster overflow behavior.
func fillCluster(prefix string) []string {
	bc := bucketCnt
	cpb := clustersPerBkt
	cs := clusterSlots

	mask := uint32(bc - 1)
	clMask := uint32(cpb - 1)

	// Derive bucket and cluster index to target
	_, _, baseLow, _ := hash16Inline(prefix)
	wantBI := uint32(baseLow) & mask
	wantCI := (uint32(baseLow) >> bits.Len32(mask)) & clMask

	var keys []string
	for i := 0; len(keys) < cs+1; i++ {
		k := fmt.Sprintf("%s_%d", prefix, i)
		_, _, l, _ := hash16Inline(k)
		bi := uint32(l) & mask
		ci := (uint32(l) >> bits.Len32(mask)) & clMask
		if bi == wantBI && ci == wantCI {
			keys = append(keys, k)
		}
		if i > 10_000_000 {
			panic("unable to generate enough collisions")
		}
	}
	return keys
}

// mustPut wraps HashMap.Put and fails the test if a panic occurs.
// Used to assert successful insertions.
func mustPut(tb testing.TB, m *HashMap, k string, v uint16) {
	tb.Helper()
	defer func() {
		if r := recover(); r != nil {
			tb.Fatalf("unexpected panic on Put(%q): %v", k, r)
		}
	}()
	m.Put(k, v)
}

// getEq ensures that HashMap.Get returns (want, true).
func getEq(tb testing.TB, m *HashMap, k string, want uint16) {
	tb.Helper()
	v, ok := m.Get(k)
	if !ok || v != want {
		tb.Fatalf("Get(%q) = (%d,%v), want (%d,true)", k, v, ok, want)
	}
}

/* ---------- 1. Basic functionality ----------------------------------- */

// Test basic insert/update and lookup functionality.
func TestPutGet(t *testing.T) {
	m := New()
	mustPut(t, m, "BTC", 1)
	getEq(t, m, "BTC", 1)
	mustPut(t, m, "BTC", 9)
	getEq(t, m, "BTC", 9)
}

// Ensure that inserting same key with same value skips update.
func TestPutSameValueNoOverwrite(t *testing.T) {
	m := New()
	m.Put("foo", 42)
	m.Put("foo", 42) // Should avoid atomic.Store since value is unchanged
}

// Check correct size accounting after inserts.
func TestSizeAccounting(t *testing.T) {
	m := New()
	if m.Size() != 0 {
		t.Fatalf("expected size 0, got %d", m.Size())
	}
	m.Put("a", 1)
	m.Put("b", 2)
	if m.Size() != 2 {
		t.Fatalf("expected size 2, got %d", m.Size())
	}
}

/* ---------- 2. Tag collision sanity ----------------------------------- */

// Test that two different keys with same CRC low bits still work.
func TestTagCollision(t *testing.T) {
	base := "AAA"
	_, _, low1, _ := hash16Inline(base)
	var coll string
	for i := 0; ; i++ {
		c := fmt.Sprintf("AAA_%d", i)
		_, _, low2, _ := hash16Inline(c)
		if low2 == low1 {
			coll = c
			break
		}
	}
	m := New()
	mustPut(t, m, base, 1)
	mustPut(t, m, coll, 2)
	getEq(t, m, base, 1)
	getEq(t, m, coll, 2)
}

/* ---------- 3. Get fallback and misses ------------------------------- */

// Miss due to entirely different hash (bucket/cluster/slot mismatch)
func TestGetFallbackMiss(t *testing.T) {
	m := New()
	m.Put("foo", 123)
	if _, ok := m.Get("bar"); ok {
		t.Fatal("expected miss, got hit")
	}
}

// Same low bits, but different key → test full slot probe and miss
func TestGetProbeMiss(t *testing.T) {
	m := New()
	var baseLow uint16
	prefix := "same"
	for i := 0; ; i++ {
		k := fmt.Sprintf("%s_%d", prefix, i)
		_, _, low, _ := hash16Inline(k)
		if i == 0 {
			baseLow = low
			m.Put(k, 1)
			continue
		}
		if low == baseLow {
			if _, ok := m.Get(k); ok {
				t.Fatalf("unexpected hit with %q", k)
			}
			return
		}
	}
}

/* ---------- 4. Cluster-full behavior --------------------------------- */

// Ensures Get fails when cluster is full and key is not present.
func TestClusterFullMiss(t *testing.T) {
	m := New()
	keys := fillCluster("X")
	for _, k := range keys[:clusterSlots] {
		mustPut(t, m, k, 100)
	}
	if _, ok := m.Get(keys[clusterSlots]); ok {
		t.Fatalf("expected miss in full cluster, got hit")
	}
}

// Inserting more than 16 items in one cluster should panic.
func TestClusterFullPanic(t *testing.T) {
	m := New()
	keys := fillCluster("Y")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on cluster overflow")
		}
	}()
	for _, k := range keys {
		m.Put(k, 200)
	}
}

/* ---------- 5. Concurrency -------------------------------------------- */

// Validate that multiple concurrent readers don’t race or panic.
func TestConcurrentReaders(t *testing.T) {
	m := New()
	mustPut(t, m, "AAA", 7)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				m.Get("AAA")
			}
		}()
	}
	wg.Wait()
}
