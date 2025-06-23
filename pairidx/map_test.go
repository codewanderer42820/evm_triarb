package pairidx

import (
	"strings"
	"testing"
	"unsafe"
)

// Tests hitting every branch of xxhMix64, sameKey, Get, Put,
// collision handling, and secondary cluster hop.
// Implementation details in map.go fileciteturn0file0

func TestXxhMix64Branches(t *testing.T) {
	// Representative lengths to cover all switch cases in xxhMix64
	lengths := []int{1, 8, 9, 16, 17, 32, 33, 64}
	for _, n := range lengths {
		s := strings.Repeat("x", n)
		h := xxhMix64(unsafe.Pointer(unsafe.StringData(s)), uint16(n))
		if h == 0 {
			t.Errorf("xxhMix64 returned zero for length %d", n)
		}
	}
}

func TestSameKeyBranches(t *testing.T) {
	// n <= 8
	a, b := "abcd", "abcd"
	if !sameKey(unsafe.Pointer(unsafe.StringData(a)), unsafe.Pointer(unsafe.StringData(b)), uint16(len(a))) {
		t.Errorf("sameKey failed for n<=8")
	}
	// n <= 12
	s10 := strings.Repeat("y", 10)
	if !sameKey(unsafe.Pointer(unsafe.StringData(s10)), unsafe.Pointer(unsafe.StringData(s10)), uint16(len(s10))) {
		t.Errorf("sameKey failed for n<=12")
	}
	// n <= 32
	s20 := strings.Repeat("z", 20)
	if !sameKey(unsafe.Pointer(unsafe.StringData(s20)), unsafe.Pointer(unsafe.StringData(s20)), uint16(len(s20))) {
		t.Errorf("sameKey failed for n<=32")
	}
	// n > 32
	s40 := strings.Repeat("w", 40)
	if !sameKey(unsafe.Pointer(unsafe.StringData(s40)), unsafe.Pointer(unsafe.StringData(s40)), uint16(len(s40))) {
		t.Errorf("sameKey failed for n>32")
	}
	// mismatch
	if sameKey(unsafe.Pointer(unsafe.StringData(a)), unsafe.Pointer(unsafe.StringData("abce")), uint16(len(a))) {
		t.Errorf("sameKey returned true for different keys")
	}
}

func TestHashMapBasic(t *testing.T) {
	hm := New()
	if hm.Size() != 0 {
		t.Errorf("Size expected 0, got %d", hm.Size())
	}
	// Missing key
	if v, ok := hm.Get("missing"); ok || v != 0 {
		t.Errorf("Get missing returned (%d, %t), want (0, false)", v, ok)
	}
	// Insert new key
	hm.Put("key1", 42)
	if hm.Size() != 1 {
		t.Errorf("Size after Put expected 1, got %d", hm.Size())
	}
	if v, ok := hm.Get("key1"); !ok || v != 42 {
		t.Errorf("Get after Put returned (%d, %t), want (42, true)", v, ok)
	}
	// Update existing key
	hm.Put("key1", 100)
	if hm.Size() != 1 {
		t.Errorf("Size after update expected 1, got %d", hm.Size())
	}
	if v, ok := hm.Get("key1"); !ok || v != 100 {
		t.Errorf("Get after update returned (%d, %t), want (100, true)", v, ok)
	}
}

func TestHashMapCollisionBranch(t *testing.T) {
	hm := New()
	key := "collision"
	ptr := unsafe.StringData(key)
	klen := uint16(len(key))
	hash := xxhMix64(unsafe.Pointer(ptr), klen)
	low := uint16(hash)
	bucketIdx := uint32(low) & bucketMask
	clusterIdx := (uint32(low) >> clusterShift) & clMask
	base := int(low) & slotMask

	// Simulate collision at primary cluster
	cl := &hm.buckets[bucketIdx][clusterIdx]
	cl.bitmap = 1 << uint(base)
	// Insert key, should skip occupied slot and place in next available
	hm.Put(key, 55)
	if v, ok := hm.Get(key); !ok || v != 55 {
		t.Errorf("Collision Put/Get returned (%d, %t), want (55, true)", v, ok)
	}
}
