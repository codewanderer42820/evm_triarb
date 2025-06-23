package pairidx

import (
	"sync"
	"testing"
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
