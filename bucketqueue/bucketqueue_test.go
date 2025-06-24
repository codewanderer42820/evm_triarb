package bucketqueue

import (
	"math/rand"
	"testing"
	"unsafe"
)

// Shared Test Helpers
func expectError(t *testing.T, got, want error) {
	t.Helper()
	if got != want {
		t.Fatalf("want err %v, got %v", want, got)
	}
}

func borrowOrPanic(t *testing.T, q *Queue) Handle {
	t.Helper()
	h, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	return h
}

func pushOrFatal(t *testing.T, q *Queue, tick int64, h Handle) {
	t.Helper()
	if err := q.Push(tick, h, unsafe.Pointer(uintptr(h))); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
}

func expectSize(t *testing.T, q *Queue, want int) {
	t.Helper()
	if q.Size() != want {
		t.Fatalf("expected size=%d; got %d", want, q.Size())
	}
}

func expectEmpty(t *testing.T, q *Queue) {
	t.Helper()
	if !q.Empty() {
		t.Fatalf("expected empty; size=%d", q.Size())
	}
}

// Full Test Suite for 100% Coverage
func TestPushPastWindow(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	tick := int64(q.baseTick + numBuckets)
	err := q.Push(tick, h, nil)
	expectError(t, err, ErrBeyondWindow)
}

func TestUpdateTickOutOfBounds(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	pushOrFatal(t, q, 0, h)
	past := int64(q.baseTick - 1)
	err := q.Update(past, h, nil)
	expectError(t, err, ErrPastWindow)
}

func TestPushOutOfOrderStress(t *testing.T) {
	const N = 1000
	q := New()
	perm := rand.Perm(N)
	for i := 0; i < N; i++ {
		h := borrowOrPanic(t, q)
		pushOrFatal(t, q, int64(perm[i]), h)
	}
	expectSize(t, q, N)
	for !q.Empty() {
		q.PopMin()
	}
}

func TestPushNegativeTickStress(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	err := q.Push(-1, h, nil)
	expectError(t, err, ErrPastWindow)
}

func TestPushPopCycle(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	for i := 0; i < 100; i++ {
		pushOrFatal(t, q, int64(i), h)
		q.PopMin()
	}
	expectEmpty(t, q)
}

func TestReturnErrors(t *testing.T) {
	q := New()
	err := q.Return(Handle(nilIdx))
	expectError(t, err, ErrItemNotFound)
}

func TestPushThenUpdate(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	pushOrFatal(t, q, 0, h)
	err := q.Update(1, h, unsafe.Pointer(uintptr(123)))
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
}

func TestPeepMin(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	pushOrFatal(t, q, 42, h)
	ph, tick, _ := q.PeepMin()
	if ph != h || tick != 42 {
		t.Fatalf("PeepMin mismatch")
	}
}

func TestPopMin(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	pushOrFatal(t, q, 10, h)
	ph, tick, _ := q.PopMin()
	if ph != h || tick != 10 {
		t.Fatalf("PopMin mismatch")
	}
}

func TestDuplicatePush(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	_ = q.Push(10, h, nil)
	_ = q.Push(10, h, nil)
	_, _, _ = q.PopMin()
	_, _, _ = q.PopMin()
	expectEmpty(t, q)
}
