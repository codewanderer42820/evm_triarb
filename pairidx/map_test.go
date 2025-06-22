package pairidx

import (
	"math/bits"
	"strconv"
	"testing"
	"unsafe"
)

/*───────────────────────────────────────────────────────────────*
| Helpers that duplicate a tiny subset of map.go for hashing     |
*───────────────────────────────────────────────────────────────*/

// hashLow16 returns the same 16-bit “low” value map.go uses for
// bucket / cluster / slot selection.
func hashLow16(p unsafe.Pointer, n uint16) uint16 {
	var h uint64 = uint64(n) * prime64_1

	switch {
	case n <= 8:
		v := *(*uint64)(p)
		h = bits.RotateLeft64(v*prime64_2, 31) * prime64_1
	case n <= 16:
		v0 := *(*uint64)(p)
		v1 := *(*uint64)(unsafe.Add(p, uintptr(n)-8))
		h = v0 ^ bits.RotateLeft64(v1*prime64_2, 27)
		h = bits.RotateLeft64(h*prime64_1, 31) * prime64_2
	case n <= 32:
		v0 := *(*uint64)(p)
		v1 := *(*uint64)(unsafe.Add(p, 8))
		v2 := *(*uint64)(unsafe.Add(p, uintptr(n)-16))
		v3 := *(*uint64)(unsafe.Add(p, uintptr(n)-8))
		h = v0 ^ bits.RotateLeft64(v1*prime64_2, 31)
		h ^= bits.RotateLeft64(v2*prime64_2, 27)
		h ^= bits.RotateLeft64(v3*prime64_1, 33)
		h = bits.RotateLeft64(h*prime64_1, 27) * prime64_1
	default:
		p8 := uintptr(p)
		for rem := n; rem >= 8; rem -= 8 {
			v := *(*uint64)(unsafe.Pointer(p8))
			p8 += 8
			h ^= bits.RotateLeft64(v*prime64_2, 31)
			h = bits.RotateLeft64(h, 27) * prime64_1
		}
		if tail := n & 7; tail != 0 {
			t := *(*uint64)(unsafe.Pointer(p8)) & ((1 << (tail * 8)) - 1)
			h ^= bits.RotateLeft64(t*prime64_2, 11)
			h = bits.RotateLeft64(h, 7) * prime64_1
		}
	}
	h ^= h >> 33
	h *= prime64_2
	h ^= h >> 29
	h *= prime64_1
	h ^= h >> 32
	return uint16(h)
}

/*──────────────────────────── tests ───────────────────────────*/

// 1. Fast-path coverage (<8 B, 9-12 B, >12 B key lengths) plus overwrite.
func TestPutGetPaths(t *testing.T) {
	h := New()

	keys := []string{
		"abc",                        // ≤8  B
		"01234567890",                // 11 B (≤12 B path)
		"abcdefghijklmnopqrstuvwxyz", // 26 B (>12 B)
	}

	for i, k := range keys {
		h.Put(k, uint16(i+1))
		if v, ok := h.Get(k); !ok || v != uint16(i+1) {
			t.Fatalf("miss or wrong value for %q", k)
		}
	}

	// overwrite should keep size constant
	if sz := h.Size(); sz != len(keys) {
		t.Fatalf("size mismatch before overwrite: %d", sz)
	}
	h.Put(keys[1], 99)
	if v, _ := h.Get(keys[1]); v != 99 {
		t.Fatalf("overwrite failed, got %d", v)
	}
	if sz := h.Size(); sz != len(keys) {
		t.Fatalf("size changed after overwrite")
	}
}

// 2. Exercise secondary-cluster hop: fill 17 colliding keys.
func TestSecondaryClusterHop(t *testing.T) {
	const need = clusterSlots + 1 // 17 ⇒ forces hop
	h := New()

	// Pick a target low16 from the first random key we find.
	targetLow := uint16(0xffff)
	var keys []string
	for i := 0; len(keys) < need; i++ {
		k := "k_" + strconv.Itoa(i)
		low := hashLow16(unsafe.Pointer(unsafe.StringData(k)), uint16(len(k)))
		if targetLow == 0xffff {
			targetLow = low
		}
		if low == targetLow { // same bucket+cluster family
			keys = append(keys, k)
		}
	}

	for i, k := range keys {
		h.Put(k, uint16(i))
	}

	if h.Size() != need {
		t.Fatalf("expected %d keys after hop, got %d", need, h.Size())
	}

	// Ensure every key survives the hop.
	for i, k := range keys {
		if v, ok := h.Get(k); !ok || v != uint16(i) {
			t.Fatalf("lost key %q after hop (v=%d ok=%v)", k, v, ok)
		}
	}
}

// 3. Allocation guard: Put/Get must allocate 0 bytes in steady-state.
func TestZeroAllocs(t *testing.T) {
	h := New()
	h.Put("aaa", 1)

	allocs := testingAllocsPerRun(1000, func() {
		_ = h.Size() // read-only
		h.Get("aaa")
		h.Put("bbb", 2)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op, got %.2f", allocs)
	}
}

/*──────────────────────── helper for alloc check ─────────────*/

// testing.AllocsPerRun is an internal helper; re-export small shim.
func testingAllocsPerRun(runs int, f func()) float64 {
	return testing.AllocsPerRun(runs, f)
}
