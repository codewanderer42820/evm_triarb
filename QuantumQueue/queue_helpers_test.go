package quantumqueue

import (
	"testing"
	"unsafe"
)

// payload returns a stable, non-nil pointer unique per call.
// Tests use pointer equality to verify Update() patched data.
func payload(i int) unsafe.Pointer { return unsafe.Pointer(&[]int{i}[0]) }

// mustBorrow borrows a handle from q or fails the test immediately.
func mustBorrow(t *testing.T, q *QuantumQueue) Handle {
	h, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	return h
}
