// Package localidx provides correctness tests for the fixed-capacity,
// Robin-Hood footgun hashmap. These tests validate behavior under high
// collision, wraparound, overwrite suppression, and load factor saturation.
package localidx

import (
	"math/rand"
	"testing"
)

// -----------------------------------------------------------------------------
// ░░ Constructor and Allocation Semantics ░░
// -----------------------------------------------------------------------------

func TestNewHash(t *testing.T) {
	h := New(8)
	if h.mask == 0 {
		t.Fatal("mask should be non-zero")
	}
	if len(h.keys) != 16 || len(h.vals) != 16 {
		t.Fatalf("expected 16-slot table, got keys=%d, vals=%d", len(h.keys), len(h.vals))
	}
}

// -----------------------------------------------------------------------------
// ░░ Basic Put / Get Semantics ░░
// -----------------------------------------------------------------------------

func TestPutAndGet(t *testing.T) {
	h := New(16)
	for i := 1; i <= 16; i++ {
		h.Put(uint32(i), uint32(i*10))
	}
	for i := 1; i <= 16; i++ {
		v, ok := h.Get(uint32(i))
		if !ok || v != uint32(i*10) {
			t.Fatalf("Get(%d) = %d,%v ; want %d,true", i, v, ok, i*10)
		}
	}
}

func TestGetMiss(t *testing.T) {
	h := New(4)
	h.Put(1, 123)
	if _, ok := h.Get(99); ok {
		t.Fatal("Get(99) should return false for missing key")
	}
}

// -----------------------------------------------------------------------------
// ░░ Overwrite Behavior ░░
// -----------------------------------------------------------------------------

func TestPutOverwrite(t *testing.T) {
	h := New(8)
	first := h.Put(42, 100)
	if first != 100 {
		t.Fatalf("first Put returned %d, want 100", first)
	}
	old := h.Put(42, 200)
	if old != 100 {
		t.Fatalf("overwrite returned %d, want 100", old)
	}
	if v, ok := h.Get(42); !ok || v != 100 {
		t.Fatalf("Get(42) = %d,%v ; want 100,true", v, ok)
	}
}

// -----------------------------------------------------------------------------
// ░░ Collision Handling & Wraparound ░░
// -----------------------------------------------------------------------------

func TestCollisionAndRobinHood(t *testing.T) {
	h := New(4)
	base := uint32(0xDEADBEEF)
	for i := 0; i < 4; i++ {
		h.Put(base+uint32(i), uint32(i))
	}
	for i := 0; i < 4; i++ {
		v, ok := h.Get(base + uint32(i))
		if !ok || v != uint32(i) {
			t.Fatalf("Get(%d) = %d,%v ; want %d,true", base+uint32(i), v, ok, i)
		}
	}
}

func TestWraparound(t *testing.T) {
	h := New(4)
	for i := 1; i <= 4; i++ {
		h.Put(uint32(i)<<29, uint32(i))
	}
	for i := 1; i <= 4; i++ {
		k := uint32(i) << 29
		v, ok := h.Get(k)
		if !ok || v != uint32(i) {
			t.Fatalf("Get(%d) = %d,%v ; want %d,true", k, v, ok, i)
		}
	}
}

// -----------------------------------------------------------------------------
// ░░ Randomized Stress and Bound Check ░░
// -----------------------------------------------------------------------------

func TestRandomStress(t *testing.T) {
	h := New(1 << 10)
	ref := make(map[uint32]uint32)
	r := rand.New(rand.NewSource(12345))
	for i := 0; i < 900; i++ {
		k := uint32(r.Intn(1_000_000)) + 1
		ref[k] = uint32(i)
		h.Put(k, uint32(i))
	}
	for k, want := range ref {
		if got, ok := h.Get(k); !ok || got != want {
			t.Fatalf("Get(%d) = %d,%v ; want %d,true", k, got, ok, want)
		}
	}
}

func TestGetRobinHoodBound(t *testing.T) {
	h := New(4)
	h.Put(1, 10)
	h.Put(9, 20)
	h.Put(17, 30)
	h.Put(4, 40)
	if v, ok := h.Get(33); ok {
		t.Fatalf("expected miss via bound-check, got %d,true", v)
	}
}
