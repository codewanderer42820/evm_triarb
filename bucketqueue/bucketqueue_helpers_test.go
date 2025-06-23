package bucketqueue

import (
    "strings"
    "testing"
    "unsafe"
)

// -----------------------------------------------------------------------------
// tiny helpers shared by tests
func expectError(t *testing.T, got, want error) {
    t.Helper()
    if got != want {
        t.Fatalf("want err %v, got %v", want, got)
    }
}

func expectErrorContains(t *testing.T, err error, sub string) {
    t.Helper()
    if err == nil || !strings.Contains(err.Error(), sub) {
        t.Fatalf("expected error containing %q; got %v", sub, err)
    }
}

func expectTick(t *testing.T, tick, want int64) {
    t.Helper()
    if tick != want {
        t.Fatalf("tick want %d got %d", want, tick)
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
