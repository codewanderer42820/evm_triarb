package ring56

import (
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// ░░ Constructor Tests ░░
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// ░░ Core Ring Operation Tests ░░
// -----------------------------------------------------------------------------

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

// TestPopNil confirms that Pop returns nil on an empty ring.
func TestPopNil(t *testing.T) {
	r := New(4)
	if r.Pop() != nil {
		t.Fatal("Pop on empty ring should return nil")
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

// -----------------------------------------------------------------------------
// ░░ Interleaved & Idle Edge Case Tests ░░
// -----------------------------------------------------------------------------

// TestPushPopInterleaved checks cursor safety under interleaved ops.
func TestPushPopInterleaved(t *testing.T) {
	r := New(8)
	for i := 0; i < 64; i++ {
		val := &[56]byte{byte(i)}
		if !r.Push(val) {
			t.Fatalf("push %d failed", i)
		}
		got := r.Pop()
		if got == nil || got[0] != byte(i) {
			t.Fatalf("mismatch at %d", i)
		}
	}
}

// TestPopAfterLongIdle ensures ring resumes correctly after inactivity.
func TestPopAfterLongIdle(t *testing.T) {
	r := New(2)
	time.Sleep(50 * time.Millisecond)
	want := &[56]byte{88}
	if !r.Push(want) {
		t.Fatal("Push failed after idle")
	}
	got := r.Pop()
	if got == nil || *got != *want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestPushDropOnOverflow simulates back-to-back Push beyond capacity.
func TestPushDropOnOverflow(t *testing.T) {
	r := New(4)
	val := &[56]byte{99}
	drops := 0
	for i := 0; i < 8; i++ {
		if !r.Push(val) {
			drops++
		} else {
			_ = r.Pop()
		}
	}
	if drops == 0 {
		t.Fatal("expected at least one push to fail on overflow")
	}
}
