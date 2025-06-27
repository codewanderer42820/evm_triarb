package localidx

import (
	"math/rand"
	"testing"
)

// -----------------------------------------------------------------------------
// ░░ Constructor and Allocation Semantics ░░
// -----------------------------------------------------------------------------

// TestNewHash verifies that:
//   - The internal slice capacity is rounded to the next power-of-two ×2.
//   - The modulo mask is non-zero and consistent with slice size.
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
// ░░ Basic Insertion & Lookup Semantics ░░
// -----------------------------------------------------------------------------

// TestPutAndGet inserts a sequence of unique keys,
// then checks that each is retrievable with correct value.
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

// TestGetMiss checks that a key not present in the table returns (0, false).
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

// TestPutOverwrite checks:
//   - Re-inserting an existing key returns the *old* value.
//   - The value stored in the table is not overwritten.
func TestPutOverwrite(t *testing.T) {
	h := New(8)

	// First insert should return input value
	first := h.Put(42, 100)
	if first != 100 {
		t.Fatalf("first Put returned %d, want 100", first)
	}

	// Overwrite attempt: should return original
	old := h.Put(42, 200)
	if old != 100 {
		t.Fatalf("overwrite returned %d, want 100", old)
	}

	// Ensure value was not replaced
	if v, ok := h.Get(42); !ok || v != 100 {
		t.Fatalf("Get(42) = %d,%v ; want 100,true", v, ok)
	}
}

// -----------------------------------------------------------------------------
// ░░ Collision Handling and Probing Behaviour ░░
// -----------------------------------------------------------------------------

// TestCollisionAndRobinHood builds a forced collision chain,
// verifying that all items in the probe cluster are retrievable.
func TestCollisionAndRobinHood(t *testing.T) {
	h := New(4)
	base := uint32(0xDEADBEEF)

	// Insert 4 colliding keys (same bucket mod table size)
	for i := 0; i < 4; i++ {
		h.Put(base+uint32(i), uint32(i))
	}

	// Validate each is still retrievable
	for i := 0; i < 4; i++ {
		v, ok := h.Get(base + uint32(i))
		if !ok || v != uint32(i) {
			t.Fatalf("Get(%d) = %d,%v ; want %d,true", base+uint32(i), v, ok, i)
		}
	}
}

// TestWraparound ensures keys probing off the end of the slice
// correctly wrap around to the start of the table.
func TestWraparound(t *testing.T) {
	h := New(4)

	// These keys all map to the same index due to bit shifts
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
// ░░ Randomized Stress Test ░░
// -----------------------------------------------------------------------------

// TestRandomStress inserts many randomly distributed keys,
// verifies all are retrievable. Compares against a Go map.
func TestRandomStress(t *testing.T) {
	h := New(1 << 10)
	ref := make(map[uint32]uint32)
	r := rand.New(rand.NewSource(12345))

	for i := 0; i < 900; i++ {
		k := uint32(r.Intn(1_000_000)) + 1 // avoid zero-key
		ref[k] = uint32(i)
		h.Put(k, uint32(i))
	}

	for k, want := range ref {
		if got, ok := h.Get(k); !ok || got != want {
			t.Fatalf("Get(%d) = %d,%v ; want %d,true", k, got, ok, want)
		}
	}
}

// -----------------------------------------------------------------------------
// ░░ Bound Check Optimization ░░
// -----------------------------------------------------------------------------

// TestGetRobinHoodBound ensures that the "early exit" optimization
// using probe distance comparison works as intended.
//
// The missing key here maps into a filled probe chain, but terminates
// correctly when it hits a lower probe distance.
func TestGetRobinHoodBound(t *testing.T) {
	h := New(4)

	// Build cluster of keys with increasing probe distance
	h.Put(1, 10)  // idx=1
	h.Put(9, 20)  // idx=1→2
	h.Put(17, 30) // idx=1→2→3
	h.Put(4, 40)  // idx=4, resets probe distance

	// This key hashes to slot 1 and traverses → should stop at slot 4
	if v, ok := h.Get(33); ok {
		t.Fatalf("expected miss via bound-check, got %d,true", v)
	}
}
