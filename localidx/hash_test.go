// -----------------------------------------------------------------------------
// localidx/hash_test.go — Unit-tests for the fixed-size Robin-Hood map
// -----------------------------------------------------------------------------
//
//  • Verifies constructor invariants, insertion & overwrite semantics
//  • Exercises collision clusters and wrap-around behaviour
//  • Includes a targeted test for the early-exit “bound check” branch
// -----------------------------------------------------------------------------

package localidx

import (
	"math/rand"
	"testing"
)

// -----------------------------------------------------------------------------
// Constructor & basic behaviour
// -----------------------------------------------------------------------------

// TestNewHash ensures New() rounds capacity up to the next power-of-two
// and initialises the mask & backing slices correctly.
func TestNewHash(t *testing.T) {
	h := New(8)
	if h.mask == 0 {
		t.Fatal("mask should be non-zero")
	}
	if len(h.keys) != 16 || len(h.vals) != 16 {
		t.Fatalf("expected 16-slot table, got %d/%d", len(h.keys), len(h.vals))
	}
}

// TestPutAndGet inserts unique keys then retrieves them.
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

// -----------------------------------------------------------------------------
// Overwrite semantics
// -----------------------------------------------------------------------------

// TestPutOverwrite confirms that Put returns the *old* value when overwriting
// an existing key, and that the stored value remains unchanged afterwards.
func TestPutOverwrite(t *testing.T) {
	h := New(8)

	first := h.Put(42, 100)
	if first != 100 {
		t.Fatalf("1st Put returned %d, want 100", first)
	}

	old := h.Put(42, 200)
	if old != 100 {
		t.Fatalf("overwrite returned %d, want 100", old)
	}

	if v, ok := h.Get(42); !ok || v != 100 {
		t.Fatalf("Get(42) = %d,%v ; want 100,true", v, ok)
	}
}

// TestGetMiss checks that a missing key yields (0,false).
func TestGetMiss(t *testing.T) {
	h := New(4)
	h.Put(1, 123)
	if _, ok := h.Get(99); ok {
		t.Fatal("Get(99) should miss")
	}
}

// -----------------------------------------------------------------------------
// Collision / clustering scenarios
// -----------------------------------------------------------------------------

// TestCollisionAndRobinHood builds a deliberate collision chain and confirms
// all keys remain reachable.
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

// TestWraparound ensures probes wrap past the table end correctly.
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
// Randomised stress test
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

// -----------------------------------------------------------------------------
// Bound-check miss branch
// -----------------------------------------------------------------------------

func TestGetRobinHoodBound(t *testing.T) {
	h := New(4)

	// Build cluster at slots 1-3 (probe distances 0,1,2) then a short 0-dist key
	h.Put(1, 10)  // idx=1
	h.Put(9, 20)  // idx=1→2 (dist 1)
	h.Put(17, 30) // idx=1→3 (dist 2)
	h.Put(4, 40)  // idx=4

	// Missing key=33 maps to idx=1; its dist grows to 3, but slot 4 has dist=0
	// → bound-check triggers early miss.
	if v, ok := h.Get(33); ok {
		t.Fatalf("expected miss via bound-check, got %d,true", v)
	}
}
