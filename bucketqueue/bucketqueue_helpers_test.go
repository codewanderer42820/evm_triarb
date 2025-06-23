// bucketqueue_helpers_test.go — shared helpers for bucketqueue tests (uint64-edition)
package bucketqueue

import (
	"strings"
	"testing"
)

// borrowOrPanic grabs a new handle or aborts the test.
func borrowOrPanic(t *testing.T, q *Queue) Handle {
	h, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	return h
}

// pushOrPanic queues (tick,h) or aborts the test.
func pushOrPanic(t *testing.T, q *Queue, tick uint64, h Handle) {
	if err := q.Push(tick, h, nil); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
}

// ─── tiny assertion helpers ────────────────────────────────────────────────────

func expectTick(t *testing.T, got, want uint64) {
	if got != want {
		t.Fatalf("Unexpected tick: got %d, want %d", got, want)
	}
}

func expectHandle(t *testing.T, got, want Handle) {
	if got != want {
		t.Fatalf("Unexpected handle: got %v, want %v", got, want)
	}
}

func expectEmpty(t *testing.T, q *Queue) {
	if !q.Empty() || q.Size() != 0 {
		t.Fatalf("Expected empty queue; got size=%d, empty=%v", q.Size(), q.Empty())
	}
}

func expectSize(t *testing.T, q *Queue, want uint64) {
	if q.Size() != want {
		t.Fatalf("Expected size=%d; got %d", want, q.Size())
	}
}

func expectError(t *testing.T, err, want error) {
	if err != want {
		t.Fatalf("Expected error %v; got %v", want, err)
	}
}

func expectErrorContains(t *testing.T, err error, sub string) {
	if err == nil || !strings.Contains(err.Error(), sub) {
		t.Fatalf("Expected error containing %q; got %v", sub, err)
	}
}

func expectPeep(t *testing.T, q *Queue, want Handle, wantTick uint64) {
	got, tick, _ := q.PeepMin()
	if got != want || tick != wantTick {
		t.Fatalf("PeepMin mismatch: got %v@%d, want %v@%d", got, tick, want, wantTick)
	}
}
