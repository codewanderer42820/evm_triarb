package localidx

import (
	"math/rand"
	"testing"
)

// TestNewHash checks that New allocates the correct size and mask.
func TestNewHash(t *testing.T) {
	h := New(8)
	if h.mask == 0 {
		t.Fatal("expected non-zero mask")
	}
	if len(h.keys) != 16 {
		t.Fatalf("expected keys length 16, got %d", len(h.keys))
	}
	if len(h.vals) != 16 {
		t.Fatalf("expected vals length 16, got %d", len(h.vals))
	}
}

// TestPutAndGet inserts unique keys and confirms they can be retrieved.
func TestPutAndGet(t *testing.T) {
	h := New(16)
	for i := 1; i <= 16; i++ {
		h.Put(uint32(i), uint32(i*10))
	}
	for i := 1; i <= 16; i++ {
		val, ok := h.Get(uint32(i))
		if !ok || val != uint32(i*10) {
			t.Errorf("Get(%d) = %d, %v; want %d, true", i, val, ok, i*10)
		}
	}
}

// TestPutOverwrite ensures an existing key returns the original value.
func TestPutOverwrite(t *testing.T) {
	h := New(8)
	h.Put(42, 100)
	v := h.Put(42, 200)
	if v != 100 {
		t.Errorf("Put overwrite returned %d, want 100", v)
	}
	val, ok := h.Get(42)
	if !ok || val != 100 {
		t.Errorf("Get(42) = %d, %v; want 100, true", val, ok)
	}
}

// TestGetMiss confirms that a missing key returns (0, false).
func TestGetMiss(t *testing.T) {
	h := New(4)
	h.Put(1, 100)
	if _, ok := h.Get(99); ok {
		t.Error("expected Get(99) to miss")
	}
}

// TestCollisionAndRobinHood inserts colliding keys to trigger Robin Hood swaps.
func TestCollisionAndRobinHood(t *testing.T) {
	h := New(4)
	base := uint32(0xDEADBEEF)
	for i := 0; i < 4; i++ {
		h.Put(base+uint32(i), uint32(i))
	}
	for i := 0; i < 4; i++ {
		val, ok := h.Get(base + uint32(i))
		if !ok || val != uint32(i) {
			t.Errorf("Get(%d) = %d, %v; want %d, true", base+uint32(i), val, ok, i)
		}
	}
}

// TestWraparound ensures probing wraps around the end of the slice correctly.
func TestWraparound(t *testing.T) {
	h := New(4)
	for i := 1; i <= 4; i++ {
		h.Put(uint32(i)<<29, uint32(i)) // max is 4 << 29 = 0x80000000, valid
	}
	for i := 1; i <= 4; i++ {
		key := uint32(i) << 29
		val, ok := h.Get(key)
		if !ok || val != uint32(i) {
			t.Errorf("Get(%d) = %d, %v; want %d, true", key, val, ok, i)
		}
	}
}

// TestRandomStress inserts many random keys and verifies retrieval.
func TestRandomStress(t *testing.T) {
	h := New(1 << 10)
	m := map[uint32]uint32{}
	rnd := rand.New(rand.NewSource(12345))

	for i := 0; i < 900; i++ {
		k := uint32(rnd.Intn(1000000)) + 1 // never zero
		v := uint32(i)
		m[k] = v
		h.Put(k, v)
	}

	for k, v := range m {
		got, ok := h.Get(k)
		if !ok || got != v {
			t.Errorf("Get(%d) = %d, %v; want %d, true", k, got, ok, v)
		}
	}
}
