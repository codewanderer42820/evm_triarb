// ring_test.go — Unit-test suite for 32-byte ring queue
//
// This validates constructor behavior, push/pop edge cases,
// wraparound logic, and PopWait blocking.
// All paths now use *[32]byte directly.

package ring

import (
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// Constructor validation
// -----------------------------------------------------------------------------

func TestNewPanicsOnBadSize(t *testing.T) {
	bad := []int{0, 3, 1000}
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

// -----------------------------------------------------------------------------
// Push/Pop round trip: single item push-pop
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Push fails when full
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// PopWait blocks and receives value
// -----------------------------------------------------------------------------

func TestPopWaitBlocksUntilItem(t *testing.T) {
	r := New(2)
	want := &[32]byte{42}

	go func() {
		time.Sleep(5 * time.Millisecond) // allow PopWait to block
		r.Push(want)
	}()

	if got := r.PopWait(); got == nil || *got != *want {
		t.Fatalf("PopWait returned %v, want %v", got, want)
	}
}

// -----------------------------------------------------------------------------
// Pop returns nil on empty
// -----------------------------------------------------------------------------

func TestPopNil(t *testing.T) {
	r := New(4)
	if r.Pop() != nil {
		t.Fatal("Pop on empty ring returned non-nil")
	}
}

// -----------------------------------------------------------------------------
// Wrap-around test (masking correctness)
// -----------------------------------------------------------------------------

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
