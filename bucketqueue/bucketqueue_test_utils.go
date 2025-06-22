// bucketqueue_test_utils.go — shared helpers for bucketqueue tests
// ===============================================================
// Centralizes common boilerplate for borrowing, pushing, and panicking
// on errors, to avoid duplication across test suites.

package bucketqueue

import (
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
	if err := q.Push(tick, h); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
}
