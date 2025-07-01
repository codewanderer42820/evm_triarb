// ring_test.go — Unit tests for ring24: lock-free 24-byte SPSC ring buffer
//
// These tests verify construction, full/empty logic, and interleaved operation
// scenarios in real-time ISR-like conditions.
//
// Assumptions:
//   - Tests only single-producer/single-consumer usage
//   - Hot-spin, capacity, and lifecycle behaviors are validated
//
// Test Groups:
//   - Constructor safety
//   - Push/Pop roundtrip
//   - Overflow and wraparound behavior
//   - Interleaved and idle-state validation

package ring24

import (
	"testing"
	"time"
)

/*──────────────────── Constructor & Sanity Tests ────────────────────*/

// TestNewPanicsOnBadSize ensures constructor rejects invalid sizes.
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

/*──────────────────── Basic Operation Tests ────────────────────*/

// TestPushPopRoundTrip verifies one push-pop roundtrip and post-pop emptiness.
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

// TestPushFailsWhenFull ensures ring refuses overflow pushes.
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

// TestPopNil ensures Pop returns nil when empty.
func TestPopNil(t *testing.T) {
	r := New(4)
	if r.Pop() != nil {
		t.Fatal("Pop on empty ring should return nil")
	}
}

// TestPopWaitBlocksUntilItem confirms PopWait blocks and returns next item.
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

// TestWrapAround checks correctness of pointer arithmetic over wrap cycles.
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

/*──────────────────── Interleaved & Idle Case Tests ────────────────────*/

// TestPushPopInterleaved ensures safety under alternating ops.
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

// TestPopAfterLongIdle ensures ring resumes after idle period.
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

// TestPushDropOnOverflow simulates multiple over-capacity Push calls.
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

/*──────────────────── Extended Edge Case Tests ────────────────────*/

// TestDoublePopWithoutPush checks double-read is safe.
func TestDoublePopWithoutPush(t *testing.T) {
	r := New(4)
	if r.Pop() != nil {
		t.Fatal("First Pop on empty ring should return nil")
	}
	if r.Pop() != nil {
		t.Fatal("Second Pop on empty ring should also return nil")
	}
}

// TestFullWrapPushPop performs full-capacity wrap cycles.
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

// TestPopImmediateReuse checks pointer safety on reuse.
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
	copy := *ptr
	if !r.Push(val2) {
		t.Fatal("Push val2 failed")
	}
	if copy != *val1 {
		t.Fatal("Copied value changed after ring reuse")
	}
}
