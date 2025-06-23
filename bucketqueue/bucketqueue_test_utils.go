// bucketqueue_test_utils.go — shared helpers for bucketqueue tests
package bucketqueue

import (
	"strings"
	"testing"
)

// borrowOrPanic attempts q.Borrow() and fails the test on error.
func borrowOrPanic(t *testing.T, q *Queue) Handle {
	h, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	return h
}

// pushOrPanic attempts q.Push and fails the test on error.
func pushOrPanic(t *testing.T, q *Queue, tick int64, h Handle) {
	if err := q.Push(tick, h, nil); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
}

// expectTick asserts that the tick matches expectation.
func expectTick(t *testing.T, got, want int64) {
	if got != want {
		t.Fatalf("Unexpected tick: got %d, want %d", got, want)
	}
}

// expectHandle asserts that the handle matches expectation.
func expectHandle(t *testing.T, got, want Handle) {
	if got != want {
		t.Fatalf("Unexpected handle: got %v, want %v", got, want)
	}
}

// expectEmpty asserts that the queue is empty.
func expectEmpty(t *testing.T, q *Queue) {
	if !q.Empty() || q.Size() != 0 {
		t.Fatalf("Expected empty queue; got size=%d, empty=%v", q.Size(), q.Empty())
	}
}

// expectSize asserts that the queue size matches.
func expectSize(t *testing.T, q *Queue, want int) {
	if q.Size() != want {
		t.Fatalf("Expected size=%d; got %d", want, q.Size())
	}
}

// expectError asserts that the returned error matches the expected error.
func expectError(t *testing.T, err error, want error) {
	if err != want {
		t.Fatalf("Expected error %v; got %v", want, err)
	}
}

// expectErrorContains asserts that err's message includes wantSubstring.
func expectErrorContains(t *testing.T, err error, wantSubstring string) {
	if err == nil || !strings.Contains(err.Error(), wantSubstring) {
		t.Fatalf("Expected error containing %q; got %v", wantSubstring, err)
	}
}

// expectPeep asserts PeepMin returns the expected handle and tick.
func expectPeep(t *testing.T, q *Queue, want Handle, wantTick int64) {
	got, tick, _ := q.PeepMin()
	if got != want || tick != wantTick {
		t.Fatalf("PeepMin mismatch: got %v@%d, want %v@%d", got, tick, want, wantTick)
	}
}
