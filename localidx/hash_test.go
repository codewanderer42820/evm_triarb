package localidx

import (
	"math/rand"
	"testing"
)

// TestNewHash checks that New allocates a power-of-two sized table
// and correctly sets the mask for indexing.
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

// TestPutOverwrite ensures that inserting a key returns the inserted value,
// and that attempting to insert the same key again returns the original (old) value.
func TestPutOverwrite(t *testing.T) {
	h := New(8)

	// First insertion should return the inserted val
	existing := h.Put(42, 100)
	if existing != 100 {
		t.Errorf("initial Put returned %d, want 100", existing)
	}

	// Second insertion (same key) should return the old value (100)
	old := h.Put(42, 200)
	if old != 100 {
		t.Errorf("Put overwrite returned %d, want 100", old)
	}

	// The stored value should remain the original
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
		t.Error("expected Get(99) to miss, got true")
	}
}

// TestCollisionAndRobinHood inserts colliding keys to trigger
// Robin Hood displacement and ensures all keys remain retrievable.
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

// TestWraparound ensures probing wraps around the end of the table correctly.
func TestWraparound(t *testing.T) {
	h := New(4)
	// Keys shifted into high bits to collide at table edges
	for i := 1; i <= 4; i++ {
		h.Put(uint32(i)<<29, uint32(i))
	}
	for i := 1; i <= 4; i++ {
		key := uint32(i) << 29
		val, ok := h.Get(key)
		if !ok || val != uint32(i) {
			t.Errorf("Get(%d) = %d, %v; want %d, true", key, val, ok, i)
		}
	}
}

// TestRandomStress inserts many random keys and verifies retrieval
// to stress-test table behavior under load.
func TestRandomStress(t *testing.T) {
	h := New(1 << 10)
	m := make(map[uint32]uint32)
	rnd := rand.New(rand.NewSource(12345))

	for i := 0; i < 900; i++ {
		k := uint32(rnd.Intn(1_000_000)) + 1 // never zero
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

// TestGetRobinHoodBound specifically exercises the Robin Hood
// "bound check" branch (kDist < dist) in Get, ensuring a miss
// when probe distance exceeds the stored key's distance.
func TestGetRobinHoodBound(t *testing.T) {
	// Small table (size=8, mask=7) to force a tight cluster.
	h := New(4)

	// Build a collision cluster at positions 1→4:
	//   key=1  maps to idx=1, dist=0
	//   key=9  maps to idx=1, displaced to idx=2, dist=1
	//   key=17 maps to idx=1, displaced to idx=3, dist=2
	//   key=4  maps to idx=4, dist=0
	h.Put(1, 10)
	h.Put(9, 20)
	h.Put(17, 30)
	h.Put(4, 40)

	// Lookup a missing key=33 (maps to idx=1), which will
	// probe through slots 1,2,3 and at slot 4 find a shorter
	// existing probe distance and trigger the bound check miss.
	if v, ok := h.Get(33); ok {
		t.Fatalf("expected Get(33) to miss via bound check, got %d, true", v)
	}
}
