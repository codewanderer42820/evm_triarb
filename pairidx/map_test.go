package pairidx

import (
	"bytes"
	"testing"
	"unsafe"
)

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func fillBytes(n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(i)
	}
	return b
}

// makeKey returns a repeated‑byte string of length n.
func makeKey(n int, ch byte) string {
	return string(bytes.Repeat([]byte{ch}, n))
}

// expose internal hashing for test‑driven cluster saturation
func bucketAndCluster(k string) (bucket uint32, cl *cluster) {
	ptr := unsafe.StringData(k)
	klen := uint16(len(k))
	h := xxhMix64(unsafe.Pointer(ptr), klen)
	bucketIdx := uint32(h) & bucketMask
	ci := (uint32(h) >> clusterShift) & clMask
	return bucketIdx, &New().buckets[bucketIdx][ci] // dummy map for pointer layout only
}

// -----------------------------------------------------------------------------
// Tests covering 100 % of logic paths
// -----------------------------------------------------------------------------

func TestPutGetVariants(t *testing.T) {
	m := New()

	// len≤8 → xxhMix64 small path
	keySmall := makeKey(7, 'a')
	m.Put(keySmall, []byte("x"))

	// 9≤len≤16 → middle path & sameKey 4‑byte branch
	keyMid := makeKey(12, 'b')
	m.Put(keyMid, []byte("y"))

	// len>16, odd len → default path with tail handling
	keyLarge := makeKey(25, 'c')
	m.Put(keyLarge, []byte("z"))

	tests := []struct {
		k    string
		want byte
	}{{keySmall, 'x'}, {keyMid, 'y'}, {keyLarge, 'z'}}

	for _, tt := range tests {
		got, ok := m.Get(tt.k)
		if !ok || got[0] != tt.want {
			t.Fatalf("Get(%q) = %q, ok=%v", tt.k, got[:1], ok)
		}
	}
}

func TestOverwriteAndGetView(t *testing.T) {
	m := New()
	k := "dup"

	// first put
	blk := m.PutView(k)
	copy(blk[:], []byte("first"))

	// overwrite via second put
	m.Put(k, []byte("second"))

	got := m.GetView(k)
	if string(got[:6]) != "second" {
		t.Fatalf("overwrite failed, got %q", got[:6])
	}
}

func TestSizeCountsUnique(t *testing.T) {
	m := New()
	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		m.Put(k, []byte{1})
	}
	// dup insert
	m.Put("a", []byte{2})

	if got := m.Size(); got != len(keys) {
		t.Fatalf("Size=%d, want %d", got, len(keys))
	}
}

func TestMissingKey(t *testing.T) {
	m := New()
	if _, ok := m.Get("noway"); ok {
		t.Fatalf("expected !ok for missing key")
	}
}

// TestClusterFullPanic artificially saturates a cluster by directly toggling its
// bitmap and tags, then asserts that PutView panics.
func TestClusterFullPanic(t *testing.T) {
	m := New()
	key := "sat" // any key → derive its cluster

	// Locate target cluster inside *m* for key
	ptr := unsafe.StringData(key)
	h := xxhMix64(unsafe.Pointer(ptr), uint16(len(key)))
	bucketIdx := uint32(h) & bucketMask
	ci := (uint32(h) >> clusterShift) & clMask
	cl := &m.buckets[bucketIdx][ci]

	// Saturate bitmap (16 bits set) and inject dummy tags ≠ target tag
	cl.bitmap = 0xFFFF
	for i := range cl.slots {
		cl.slots[i].tag = uint16(i + 1) // non‑zero, unlikely to match `key`
		cl.slots[i].klen = 1
		cl.slots[i].kptr = unsafe.Pointer(ptr) // any valid pointer
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected cluster full panic")
		}
	}()
	_ = m.PutView(key) // must panic
}
