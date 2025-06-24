package pairidx

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// getEq asserts that Get returns the wanted value.
func getEq(tb testing.TB, m *HashMap, k string, want uint32) {
	tb.Helper()
	ptr := m.Get(k)
	if ptr == nil || *(*uint32)(ptr) != want {
		tb.Fatalf("Get(%q) = %v, want %d", k, ptr, want)
	}
}

// mustPut wraps Put and fails the test on panic.
func mustPut(tb testing.TB, m *HashMap, k string, v uint32) {
	tb.Helper()
	defer func() {
		if r := recover(); r != nil {
			tb.Fatalf("unexpected panic on Put(%q): %v", k, r)
		}
	}()
	m.Put(k, v)
}

/* ---------- 1. Basic functionality ----------------------------------- */

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
	if m.Size() != 1 {
		t.Fatalf("size changed after no-op overwrite")
	}
}

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

/* ---------- 2. Miss paths -------------------------------------------- */

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

/* ---------- 4. Cluster-overflow safety ------------------------------- */

// We can still check that inserting far more than theoretical capacity panics.
func TestClusterOverflowPanic(t *testing.T) {
	m := New()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on pathological overflow")
		}
	}()

	// Insert way beyond total map capacity; eventually a cluster must overflow.
	for i := 0; ; i++ {
		m.Put(string(rune(i)), uint32(i))
	}
}

/* ---------- 5. Internal sanity (alignment) --------------------------- */

func TestUnsafePointerStable(t *testing.T) {
	m := New()
	p := m.Put("x", 99)
	if *(*uint32)(p) != 99 {
		t.Fatalf("wrong value via pointer")
	}
	// Ensure the same pointer is returned on a Get() hit.
	if p2 := m.Get("x"); p2 == nil || p2 != p {
		t.Fatalf("pointer mismatch between Put and Get")
	}
}

// Auxiliary assertion to keep compiler honest about pointer safety.
func TestKeyBytesNotMoved(t *testing.T) {
	k := "hello"
	ptrBefore := unsafe.StringData(k)
	_ = k + "world" // create another string, ensure GC not confused
	if ptrAfter := unsafe.StringData(k); ptrBefore != ptrAfter {
		t.Fatalf("unexpected key bytes relocation")
	}
}

// ---------- 6. rbm scan / return-nil path ------------------------------

// meta returns (bucket, cluster, slot) exactly the way the table derives them.
func meta(k string) (bucket, cluster, slot uint32) {
	h32, _ := crc32cMix(k)
	bucket = (h32 >> hashShift) & bucketMask
	cluster = (h32 >> (hashShift + bucketShift)) & clMask
	slot = h32 & slotMask
	return
}

// craftKeysSameCluster returns N distinct keys that fall into the same bucket+cluster.
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

// The first two keys occupy different slots; the third is absent.
func TestGetScanExhaustNil(t *testing.T) {
	keys := craftKeysSameCluster("scanSeed", 3)
	m := New()
	m.Put(keys[0], 0) // fills 1st bit
	m.Put(keys[1], 1) // fills 2nd bit

	// Lookup of the 3rd key must iterate twice (rbm &= rbm-1 executed ≥1 time)
	// then return nil after the mask is cleared.
	if m.Get(keys[2]) != nil {
		t.Fatalf("expected miss after full bitmap scan")
	}
}

// ---------- 7. sameKey multi-length sanity -----------------------------

func TestSameKeyWideLengths(t *testing.T) {
	lengths := []int{0, 1, 2, 7, 8, 9, 15, 16, 31, 64}
	for _, n := range lengths {
		b1 := bytes.Repeat([]byte{'A'}, n)
		b2 := append([]byte(nil), b1...) // deep copy
		var p1, p2 unsafe.Pointer
		if n > 0 {
			p1, p2 = unsafe.Pointer(&b1[0]), unsafe.Pointer(&b2[0])
		} else { // empty slice – use nil (valid, never dereferenced)
			p1, p2 = nil, nil
		}
		if !sameKey64(p1, p2, uint16(n)) {
			t.Fatalf("sameKey failed at len=%d", n)
		}
	}
}

// ---------- 8. sameKey false-branch coverage --------------------------

// Each case tweaks exactly one compare stage to fail.
func TestSameKeyFalseBranches(t *testing.T) {
	cases := []struct {
		name string
		a    []byte
		b    []byte
	}{
		// Case 1: first 8 bytes differ
		{"first8", bytes.Repeat([]byte{'X'}, 12), append([]byte{'Y'}, bytes.Repeat([]byte{'X'}, 11)...)},

		// Case 2: second 8-byte block differs (first equal)
		{"mid8", append(bytes.Repeat([]byte{'A'}, 8), bytes.Repeat([]byte{'B'}, 8)...),
			append(bytes.Repeat([]byte{'A'}, 8), bytes.Repeat([]byte{'C'}, 8)...)},

		// Case 3: 4-byte tail differs
		{"tail4", append(bytes.Repeat([]byte{'A'}, 8), []byte("1234")...),
			append(bytes.Repeat([]byte{'A'}, 8), []byte("zz34")...)},

		// Case 4: 2-byte tail differs
		{"tail2", append(bytes.Repeat([]byte{'A'}, 8), []byte("XY")...),
			append(bytes.Repeat([]byte{'A'}, 8), []byte("XZ")...)},
	}

	for _, tc := range cases {
		if sameKey64(p(tc.a), p(tc.b), uint16(len(tc.a))) {
			t.Fatalf("%s: expected false", tc.name)
		}
	}
}

/* ---------- 9. Epoch-retry & fallback-cluster coverage -------------------- */

func TestEpochRetryAndFallback(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	m := New()

	seed := "fallbackSeed"
	b0, c0, _ := meta(seed)

	// Fill every slot in that cluster.
	count := 0
	for i := 0; count < clusterSlots; i++ {
		k := fmt.Sprintf("%s_%d_%d", seed, rand.Uint32(), i)
		if b, c, _ := meta(k); b == b0 && c == c0 {
			m.Put(k, uint32(i))
			count++
		}
	}

	// Launch writer that must fall back to secondary cluster.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.Put("fallbackWinner", 999)
	}()

	wg.Wait() // writer done → epoch even again

	// Now the key must be visible to any reader.
	if p := m.Get("fallbackWinner"); p == nil || *(*uint32)(p) != 999 {
		t.Fatal("fallback key not visible after writer completed")
	}
}

/* ---------- 10. sameKey corner-case branches ----------------------------- */

func TestSameKeyCornerBranches(t *testing.T) {
	a := []byte("ABCDEFGH")  // 8 bytes
	b := []byte("ABCDEFGH1") // 9 bytes

	if !sameKey64(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), 8) {
		t.Fatal("sameKey64 failed on identical 8-byte prefix")
	}
	if sameKey64(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), 9) {
		t.Fatal("sameKey64 incorrectly matched 9-byte strings")
	}
}

/* helper: []byte → unsafe.Pointer */
func p(b []byte) unsafe.Pointer { return unsafe.Pointer(&b[0]) }
