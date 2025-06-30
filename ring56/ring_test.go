// ring_test.go — Functional verification of the lock-free SPSC ring with 56-byte payloads.
//
// This suite tests:
//   - Basic push/pop correctness
//   - Full ring capacity behavior
//   - Wraparound cursor logic
//   - Blocking PopWait behavior under delay
//
// All tests assume:
//   - Caller is single-threaded SPSC (no races).
//   - Memory is reused immediately upon pop.
//   - No GC interference; short-lived allocations only.

package ring56

import (
	"testing"
	"time"
)

// TestNewPanicsOnBadSize validates that the constructor panics
// on non-power-of-two or non-positive sizes.
func TestNewPanicsOnBadSize(t *testing.T) {
	bad := []int{0, 3, 1000}
	for _, sz := range bad {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("New(%d) should panic", sz)
				}
			}()
			_ = New(sz)
		}()
	}
}

// TestPushPopRoundTrip confirms single element round-trip integrity
// and checks that the ring is empty afterwards.
func TestPushPopRoundTrip(t *testing.T) {
	r := New(8)
	val := &[56]byte{1, 2, 3}

	if !r.Push(val) {
		t.Fatal("Push should succeed")
	}
	got := r.Pop()
	if got == nil || *got != *val {
		t.Fatalf("expected %v, got %v", val, got)
	}
	if r.Pop() != nil {
		t.Fatal("Ring should be empty after Pop")
	}
}

// TestPushFailsWhenFull fills the ring and verifies
// that a full queue blocks further pushes.
func TestPushFailsWhenFull(t *testing.T) {
	r := New(4)
	val := &[56]byte{7}
	for i := 0; i < 4; i++ {
		if !r.Push(val) {
			t.Fatalf("push %d unexpectedly failed", i)
		}
	}
	if r.Push(val) {
		t.Fatal("push into full ring should return false")
	}
}

// TestPopWaitBlocksUntilItem verifies PopWait blocks until data arrives
// and returns the correct value.
func TestPopWaitBlocksUntilItem(t *testing.T) {
	r := New(2)
	want := &[56]byte{42}
	go func() {
		time.Sleep(5 * time.Millisecond)
		r.Push(want)
	}()
	if got := r.PopWait(); got == nil || *got != *want {
		t.Fatalf("PopWait = %v, want %v", got, want)
	}
}

// TestPopNil confirms that Pop returns nil on an empty ring.
func TestPopNil(t *testing.T) {
	r := New(4)
	if r.Pop() != nil {
		t.Fatal("Pop on empty ring should return nil")
	}
}

// TestWrapAround ensures that cursor arithmetic works correctly
// when wrapping around the ring buffer multiple times.
func TestWrapAround(t *testing.T) {
	const size = 4
	r := New(size)
	for i := 0; i < 10; i++ {
		val := &[56]byte{byte(i)}
		if !r.Push(val) {
			t.Fatalf("push %d failed unexpectedly", i)
		}
		got := r.Pop()
		if got == nil || got[0] != byte(i) {
			t.Fatalf("iteration %d: got %v, want %v", i, got[0], val[0])
		}
	}
}
