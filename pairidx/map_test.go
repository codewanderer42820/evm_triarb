package pairidx

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"unsafe"
)

/* helper ------------------------------------------------------------------ */

func getEq(tb testing.TB, m *HashMap, k string, want uint32) {
	tb.Helper()
	ptr := m.Get(k)
	if ptr == nil || *(*uint32)(ptr) != want {
		tb.Fatalf("Get(%q) = %v, want %d", k, ptr, want)
	}
}

func mustPut(tb testing.TB, m *HashMap, k string, v uint32) {
	tb.Helper()
	defer func() {
		if r := recover(); r != nil {
			tb.Fatalf("unexpected panic on Put(%q): %v", k, r)
		}
	}()
	m.Put(k, v)
}

func p(b []byte) unsafe.Pointer { // single definition
	if len(b) == 0 {
		return nil
	}
	return unsafe.Pointer(&b[0])
}

/* ---------- 1. Basic functionality ------------------------------------ */

func TestPutGet(t *testing.T) {
	m := New()
	mustPut(t, m, "BTC", 1)
	getEq(t, m, "BTC", 1)
	mustPut(t, m, "BTC", 9)
	getEq(t, m, "BTC", 9)
}

func TestPutSameValueNoOverwrite(t *testing.T) {
	m := New()
	m.Put("foo", 42)
	m.Put("foo", 42) // identical value → should not change size
	if m.size != 1 {
		t.Fatalf("size changed after no-op overwrite")
	}
}

func TestSizeAccounting(t *testing.T) {
	m := New()
	m.Put("a", 1)
	m.Put("b", 2)
	if m.size != 2 {
		t.Fatalf("expected size 2, got %d", m.size)
	}
}

/* ---------- 2. Miss paths --------------------------------------------- */

func TestGetFallbackMiss(t *testing.T) {
	m := New()
	m.Put("foo", 123)
	if m.Get("bar") != nil {
		t.Fatal("expected miss, got hit")
	}
}

func TestGetProbeMiss(t *testing.T) {
	m := New()
	m.Put("alpha", 1)
	if m.Get("alpha_x") != nil {
		t.Fatal("unexpected hit")
	}
}

/* ---------- 3. Concurrency (read-only) -------------------------------- */

func TestConcurrentReaders(t *testing.T) {
	m := New()
	mustPut(t, m, "AAA", 7)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10_000; j++ {
				_ = m.Get("AAA")
			}
		}()
	}
	wg.Wait()
}

/* ---------- 4. Cluster-overflow safety -------------------------------- */

func TestClusterOverflowPanic(t *testing.T) {
	m := New()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on pathological overflow")
		}
	}()
	for i := 0; ; i++ { // write until it panics
		m.Put(fmt.Sprintf("%d", i), uint32(i))
	}
}

/* ---------- 5. Pointer stability -------------------------------------- */

func TestUnsafePointerStable(t *testing.T) {
	m := New()
	p1 := m.Put("x", 99)
	if *(*uint32)(p1) != 99 {
		t.Fatalf("wrong value via pointer")
	}
	if p2 := m.Get("x"); p2 == nil || p2 != p1 {
		t.Fatalf("pointer mismatch between Put and Get")
	}
}

/* ---------- 6. Key bytes immutability --------------------------------- */

func TestKeyBytesNotMoved(t *testing.T) {
	k := "hello"
	before := unsafe.StringData(k)
	_ = k + "world"
	if after := unsafe.StringData(k); before != after {
		t.Fatalf("key bytes relocated")
	}
}

/* ---------- 7. Bitmap scan return-nil path ---------------------------- */

func meta(k string) (bucket, cluster, slot uint32) {
	h32, _ := crc32cMix(k)
	return (h32 >> hashShift) & bucketMask,
		(h32 >> (hashShift + bucketShift)) & clMask,
		h32 & slotMask
}

func craftKeysSameCluster(seed string, n int) []string {
	b0, c0, _ := meta(seed)
	keys := []string{seed}
	for i := 0; len(keys) < n; i++ {
		k := fmt.Sprintf("%s_%d", seed, i)
		if b, c, _ := meta(k); b == b0 && c == c0 {
			keys = append(keys, k)
		}
	}
	return keys
}

func TestGetScanExhaustNil(t *testing.T) {
	keys := craftKeysSameCluster("scanSeed", 3)
	m := New()
	m.Put(keys[0], 0)
	m.Put(keys[1], 1)
	if m.Get(keys[2]) != nil {
		t.Fatalf("expected miss after full bitmap scan")
	}
}

/* ---------- 8. sameKey64 sanity & false branches ---------------------- */

func TestSameKeyWideLengths(t *testing.T) {
	for _, n := range []int{0, 1, 2, 7, 8, 9, 15, 16, 31, 64} {
		b1 := bytes.Repeat([]byte{'A'}, n)
		b2 := append([]byte(nil), b1...)
		if !sameKey64(p(b1), p(b2), uint16(n)) {
			t.Fatalf("sameKey64 failed at len=%d", n)
		}
	}
}

func TestSameKeyFalseBranches(t *testing.T) {
	cases := []struct{ a, b []byte }{
		{bytes.Repeat([]byte{'X'}, 12), append([]byte{'Y'}, bytes.Repeat([]byte{'X'}, 11)...)},
		{append(bytes.Repeat([]byte{'A'}, 8), bytes.Repeat([]byte{'B'}, 8)...),
			append(bytes.Repeat([]byte{'A'}, 8), bytes.Repeat([]byte{'C'}, 8)...)},
		{append(bytes.Repeat([]byte{'A'}, 8), []byte("1234")...),
			append(bytes.Repeat([]byte{'A'}, 8), []byte("zz34")...)},
		{append(bytes.Repeat([]byte{'A'}, 8), []byte("XY")...),
			append(bytes.Repeat([]byte{'A'}, 8), []byte("XZ")...)},
	}
	for i, tc := range cases {
		if sameKey64(p(tc.a), p(tc.b), uint16(len(tc.a))) {
			t.Fatalf("case %d: expected false", i)
		}
	}
}

/* ---------- 9. Arena full panic path --------------------------------- */

func TestArenaFullPanic(t *testing.T) {
	m := New()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when arena capacity exceeded, got none")
		}
	}()
	// Consume the 8 MiB arena in ~1 KiB chunks so the loop terminates fast.
	const chunk = 1024
	buf := bytes.Repeat([]byte{'X'}, chunk)
	for used := 0; ; used += chunk {
		k := fmt.Sprintf("%d-%s", used, string(buf)) // unique & >1 KiB key
		m.Put(k, 0)
	}
}
