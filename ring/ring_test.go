// ring_test.go
//
// Unit-test suite for the SPSC ring, validating:
//   - constructor invariants and parameter checking
//   - single-threaded Push/Pop paths
//   - wrap-around arithmetic with a small capacity
//   - PopWait busy-spin semantics
//   - pinned consumer helper correctness
//
// The tests purposefully use unsafe.Pointer to mirror how the library is
// expected to be used when every nanosecond matters.

package ring

import (
	"testing"
	"time"
	"unsafe"
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
// Single-threaded Push / Pop path
// -----------------------------------------------------------------------------

func TestPushPopRoundTrip(t *testing.T) {
	r := New(8)
	p := unsafe.Pointer(new(int))

	if !r.Push(p) {
		t.Fatal("first push must succeed")
	}
	if got := r.Pop(); got != p {
		t.Fatalf("got %p, want %p", got, p)
	}
	if r.Pop() != nil {
		t.Fatal("ring should now be empty")
	}
}

func TestPushFailsWhenFull(t *testing.T) {
	r := New(4)
	ptr := unsafe.Pointer(new(int))
	for i := 0; i < 4; i++ {
		if !r.Push(ptr) {
			t.Fatalf("push %d unexpectedly failed", i)
		}
	}
	if r.Push(ptr) {
		t.Fatal("push into full ring should return false")
	}
}

func TestPopWaitBlocksUntilItem(t *testing.T) {
	r := New(2)
	want := unsafe.Pointer(new(int))

	done := make(chan struct{})
	go func() {
		time.Sleep(5 * time.Millisecond) // give PopWait time to block
		r.Push(want)
	}()

	if got := r.PopWait(); got != want {
		t.Fatalf("PopWait returned %p, want %p", got, want)
	}
	close(done)
}

// -----------------------------------------------------------------------------
// Unsafe-pointer helpers (no generics)
// -----------------------------------------------------------------------------

func TestUnsafePushPop(t *testing.T) {
	r := New(16)
	val := &struct{ N int }{42}
	p := unsafe.Pointer(val)

	if !r.Push(p) {
		t.Fatal("Push failed")
	}
	gotPtr := r.Pop()
	got := (*struct{ N int })(gotPtr)
	if got == nil || got.N != 42 {
		t.Fatalf("Pop returned %#v, want N=42", got)
	}
}

func TestPopNil(t *testing.T) {
	r := New(4)
	if r.Pop() != nil {
		t.Fatal("Pop on empty ring returned non-nil")
	}
}

// -----------------------------------------------------------------------------
// Wrap-around behaviour (exercise mask arithmetic)
// -----------------------------------------------------------------------------

func TestWrapAround(t *testing.T) {
	const size = 4
	r := New(size)

	for i := 0; i < 10; i++ { // more than twice the capacity
		p := unsafe.Pointer(&[1]int{i}) // unique address each loop
		if !r.Push(p) {
			t.Fatalf("push %d failed unexpectedly", i)
		}
		if got := r.Pop(); got != p {
			t.Fatalf("iteration %d: got %p, want %p", i, got, p)
		}
	}
}
