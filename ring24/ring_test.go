// -----------------------------------------------------------------------------
// ░░ Constructor & Edge Case Validation ░░
// -----------------------------------------------------------------------------
//
// Tests under this group verify ring buffer invariants:
//   - Constructor rejects invalid sizes
//   - Edge conditions (empty, full, wraparound)
//   - Data integrity under interleaved push/pop patterns

package ring24

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
	val := &[24]byte{1, 2, 3}

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
	val := &[24]byte{7}
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
	want := &[24]byte{42}
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
		val := &[24]byte{byte(i)}
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
		val := &[24]byte{byte(i)}
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
	want := &[24]byte{88}
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
	val := &[24]byte{99}
	drops := 0
	for i := 0; i < 8; i++ {
		if !r.Push(val) {
			drops++
		}
	}
	if drops == 0 {
		t.Fatal("expected at least one push to fail on overflow")
	}
}

// -----------------------------------------------------------------------------
// ░░ Additional Edge Case Tests ░░
// -----------------------------------------------------------------------------

// TestDoublePopWithoutPush confirms repeated pops on empty ring are safe.
func TestDoublePopWithoutPush(t *testing.T) {
	r := New(4)
	if r.Pop() != nil {
		t.Fatal("First Pop on empty ring should return nil")
	}
	if r.Pop() != nil {
		t.Fatal("Second Pop on empty ring should also return nil")
	}
}

// TestFullWrapPushPop verifies full ring wrap-around under repeated push/pop cycles.
func TestFullWrapPushPop(t *testing.T) {
	const size = 4
	r := New(size)
	for i := 0; i < size*3; i++ {
		val := &[24]byte{byte(i)}
		if !r.Push(val) {
			t.Fatalf("Push failed at iteration %d", i)
		}
		got := r.Pop()
		if got == nil || got[0] != byte(i) {
			t.Fatalf("iteration %d: got %v, want %v", i, got[0], val[0])
		}
	}
}

// TestPopImmediateReuse ensures Pop-returned pointers are valid before reuse.
func TestPopImmediateReuse(t *testing.T) {
	r := New(2)
	val1 := &[24]byte{42}
	val2 := &[24]byte{99}
	if !r.Push(val1) {
		t.Fatal("Push val1 failed")
	}
	ptr := r.Pop()
	if ptr == nil || *ptr != *val1 {
		t.Fatal("First Pop mismatch")
	}
	copy := *ptr // Copy before next Push
	if !r.Push(val2) {
		t.Fatal("Push val2 failed")
	}
	if copy != *val1 {
		t.Fatal("Copied value changed after ring reuse")
	}
}
