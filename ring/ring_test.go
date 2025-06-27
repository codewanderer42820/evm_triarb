package ring

import (
	"testing"
	"time"
)

// TestNewPanicsOnBadSize verifies that the constructor rejects sizes that are
// either non‑power‑of‑two or ≤ 0.  We wrap the call in an inlined closure so we
// can recover() and inspect the panic without terminating the whole test run.
func TestNewPanicsOnBadSize(t *testing.T) {
	bad := []int{0, 3, 1000} // 3 and 1000 are not powers of two
	for _, sz := range bad {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("New(%d) should panic", sz)
				}
			}()
			_ = New(sz) // expect panic
		}()
	}
}

// TestPushPopRoundTrip performs a minimal sanity round‑trip on a size‑8 ring.
// It pushes one element, pops it, and confirms the ring is empty afterwards.
func TestPushPopRoundTrip(t *testing.T) {
	r := New(8)
	val := &[32]byte{1, 2, 3}

	if !r.Push(val) {
		t.Fatal("first push must succeed")
	}
	got := r.Pop()
	if got == nil || *got != *val {
		t.Fatalf("got %v, want %v", got, val)
	}
	if r.Pop() != nil {
		t.Fatal("ring should now be empty")
	}
}

// TestPushFailsWhenFull fills the ring to capacity and checks that a further
// Push returns false (non‑blocking back‑pressure).
func TestPushFailsWhenFull(t *testing.T) {
	r := New(4)
	val := &[32]byte{7}
	for i := 0; i < 4; i++ {
		if !r.Push(val) {
			t.Fatalf("push %d unexpectedly failed", i)
		}
	}
	if r.Push(val) {
		t.Fatal("push into full ring should return false")
	}
}

// TestPopWaitBlocksUntilItem launches a goroutine that will push after a tiny
// delay, then asserts PopWait blocks and eventually returns the value.
func TestPopWaitBlocksUntilItem(t *testing.T) {
	r := New(2)
	want := &[32]byte{42}

	go func() {
		time.Sleep(5 * time.Millisecond)
		r.Push(want)
	}()

	if got := r.PopWait(); got == nil || *got != *want {
		t.Fatalf("PopWait returned %v, want %v", got, want)
	}
}

// TestPopNil confirms that Pop on an empty ring returns nil.
func TestPopNil(t *testing.T) {
	r := New(4)
	if r.Pop() != nil {
		t.Fatal("Pop on empty ring returned non‑nil")
	}
}

// TestWrapAround exercises >mask iterations to ensure head/tail wrap correctly
// and masking math is sound.
func TestWrapAround(t *testing.T) {
	const size = 4
	r := New(size)
	for i := 0; i < 10; i++ {
		val := &[32]byte{byte(i)}
		if !r.Push(val) {
			t.Fatalf("push %d failed unexpectedly", i)
		}
		got := r.Pop()
		if got == nil || got[0] != byte(i) {
			t.Fatalf("iteration %d: got %v, want %v", i, got[0], val[0])
		}
	}
}
