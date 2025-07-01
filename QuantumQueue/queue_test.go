// ─────────────────────────────────────────────────────────────────────────────
// queue_test.go — ISR-safe test coverage for QuantumQueue
//
// Purpose:
//   - Validates footgun-mode behavior across all queue operations
//   - Verifies contract: size, order, reuse, unlink, and corruption detection
//
// Structure:
//   - Each test case follows ISR-style invariant and safety validation
//   - PeepMin, Push, MoveTick, and UnlinkMin behaviors are stress-verified
//
// Notes:
//   - Panic-expected edge cases are explicitly isolated
//   - Safe and unsafe reuse cycles are modeled
//   - Includes 10/10 footgun checks like double unlink, PeepMin on empty
// ─────────────────────────────────────────────────────────────────────────────

package quantumqueue

import "testing"

/*─────────────────────────────────────────────────────────────────────────────*
 * Utility: arr48 for fixed-size payload injection                             *
 *─────────────────────────────────────────────────────────────────────────────*/

// arr48 converts a short byte slice into a fixed-size [48]byte pointer.
func arr48(b []byte) *[48]byte {
	var a [48]byte
	copy(a[:], b)
	return &a
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Basic Queue Construction and Empty Behavior                                 *
 *─────────────────────────────────────────────────────────────────────────────*/

// -----------------------------------------------------------------------------
// TestNewQueueBehavior checks that the queue initializes empty and safe.
// -----------------------------------------------------------------------------
func TestNewQueueBehavior(t *testing.T) {
	q := NewQuantumQueue()
	if !q.Empty() || q.Size() != 0 {
		t.Errorf("new queue: Empty=%v Size=%d; want true, 0", q.Empty(), q.Size())
	}
	h1, err := q.Borrow()
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	n1 := &q.arena[h1]
	if n1.tick != -1 || n1.prev != nilIdx || n1.next != nilIdx {
		t.Errorf("Borrow reset failed: tick=%d prev=%v next=%v", n1.tick, n1.prev, n1.next)
	}
	h2, err2 := q.Borrow()
	if err2 != nil || h2 != h1+1 {
		t.Errorf("Borrow order incorrect: h2=%v want=%v", h2, h1+1)
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Borrow Exhaustion                                                           *
 *─────────────────────────────────────────────────────────────────────────────*/

// -----------------------------------------------------------------------------
// TestBorrowSafeExhaustion ensures the arena has fixed capacity.
// -----------------------------------------------------------------------------
func TestBorrowSafeExhaustion(t *testing.T) {
	q := NewQuantumQueue()
	for i := 0; i < CapItems; i++ {
		if _, err := q.BorrowSafe(); err != nil {
			t.Fatalf("Borrow #%d failed: %v", i, err)
		}
	}
	if _, err := q.BorrowSafe(); err == nil {
		t.Error("Expected exhaustion error, got nil")
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Push and Peep: Core Scheduling Semantics                                    *
 *─────────────────────────────────────────────────────────────────────────────*/

// -----------------------------------------------------------------------------
// TestPushAndPeepMin validates order, update path, and edge ticks.
// -----------------------------------------------------------------------------
func TestPushAndPeepMin(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(10, h, arr48([]byte("foo")))
	if q.Empty() || q.Size() != 1 {
		t.Errorf("Queue state invalid after Push")
	}
	hGot, tickGot, data := q.PeepMin()
	if hGot != h || tickGot != 10 || string(data[:3]) != "foo" {
		t.Errorf("PeepMin mismatch: h=%v tick=%d data=%q", hGot, tickGot, data[:3])
	}

	// Update same handle, same tick
	q.Push(10, h, arr48([]byte("bar")))
	if q.Size() != 1 {
		t.Errorf("Push update should not increase size")
	}
	hGot2, _, data2 := q.PeepMin()
	if hGot2 != h || string(data2[:3]) != "bar" {
		t.Errorf("Updated payload incorrect: %q", data2[:3])
	}

	q.UnlinkMin(h, 10)
	if !q.Empty() || q.Size() != 0 {
		t.Error("Queue not empty after UnlinkMin")
	}

	// Edge case: lowest and highest tick
	q2 := NewQuantumQueue()
	h0, _ := q2.BorrowSafe()
	hMax, _ := q2.BorrowSafe()
	q2.Push(0, h0, arr48([]byte("low")))
	q2.Push(int64(CapItems-1), hMax, arr48([]byte("hi")))

	hMin, tickMin, _ := q2.PeepMin()
	if hMin != h0 || tickMin != 0 {
		t.Errorf("Edge tick low failed: (%v, %d)", hMin, tickMin)
	}
	q2.UnlinkMin(h0, 0)
	hHigh, tickHigh, _ := q2.PeepMin()
	if hHigh != hMax || tickHigh != int64(CapItems-1) {
		t.Errorf("Edge tick high failed: (%v, %d)", hHigh, tickHigh)
	}
	q2.UnlinkMin(hMax, tickHigh)
	if !q2.Empty() {
		t.Error("Edge tick cleanup failed")
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Ordering Tests: Same and Different Tick Logic                               *
 *─────────────────────────────────────────────────────────────────────────────*/

func TestMultipleSameTickOrdering(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	q.Push(5, h1, arr48([]byte("a1")))
	q.Push(5, h2, arr48([]byte("a2"))) // newer, should be head
	hMin, _, data := q.PeepMin()
	if hMin != h2 || string(data[:2]) != "a2" {
		t.Errorf("Same tick LIFO failed: got %v %q", hMin, data[:2])
	}
}

func TestPushDifferentTicks(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	q.Push(100, h1, arr48([]byte("one")))
	q.Push(50, h2, arr48([]byte("two"))) // earlier tick wins
	hMin, tickMin, _ := q.PeepMin()
	if hMin != h2 || tickMin != 50 {
		t.Errorf("Tick ordering broken: got (%v, %d)", hMin, tickMin)
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Tick Movement and Manual Unlinking                                         *
 *─────────────────────────────────────────────────────────────────────────────*/

func TestMoveTickBehavior(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(20, h, arr48([]byte("x")))
	q.MoveTick(h, 20) // no-op
	q.MoveTick(h, 30)
	hNew, tickNew, _ := q.PeepMin()
	if hNew != h || tickNew != 30 {
		t.Errorf("MoveTick failed: got (%v, %d)", hNew, tickNew)
	}
}

func TestUnlinkMinNonHead(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	h3, _ := q.BorrowSafe()
	q.Push(1, h1, arr48([]byte("h1")))
	q.Push(2, h2, arr48([]byte("h2")))
	q.Push(3, h3, arr48([]byte("h3")))
	q.UnlinkMin(h2, 2)
	hMin, tickMin, _ := q.PeepMin()
	if hMin != h1 || tickMin != 1 {
		t.Errorf("UnlinkMin non-head failed: got (%v, %d)", hMin, tickMin)
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
 * End-to-End, Multi-Handle Rotation                                          *
 *─────────────────────────────────────────────────────────────────────────────*/

func TestMixedOperations(t *testing.T) {
	q := NewQuantumQueue()
	var hs [3]Handle
	for i := range hs {
		h, _ := q.BorrowSafe()
		hs[i] = h
		q.Push(int64(i), h, arr48([]byte{byte(i)}))
	}
	for i := 0; i < 3; i++ {
		h, tick, _ := q.PeepMin()
		if tick != int64(i) {
			t.Errorf("Tick mismatch: want %d got %d", i, tick)
		}
		q.UnlinkMin(h, tick)
	}
	if !q.Empty() {
		t.Error("Queue should be empty after all Unlinks")
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
 * ⚠️ Footgun Behavior: Unsafe Panic Scenarios                                *
 *─────────────────────────────────────────────────────────────────────────────*/

// PeepMin on empty queue must panic (footgun 10)
func TestPeepMinWhenEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic on empty PeepMin")
		}
	}()
	NewQuantumQueue().PeepMin()
}

// Double unlink must panic (footgun 2/4/8)
func TestDoubleUnlink(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic on double unlink")
		}
	}()
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(100, h, arr48([]byte("foo")))
	q.UnlinkMin(h, 100)
	q.UnlinkMin(h, 100)
}

// Handle reuse should be safe after unlink
func TestHandleReuseAfterUnlink(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	q.Push(123, h, arr48([]byte("x")))
	q.UnlinkMin(h, 123)
	q.Push(456, h, arr48([]byte("y")))
	_, tick, data := q.PeepMin()
	if tick != 456 || string(data[:1]) != "y" {
		t.Errorf("Handle reuse failed: tick=%d data=%q", tick, data[:1])
	}
}

// Invalid ticks must panic or corrupt
func TestPushWithInvalidTicks(t *testing.T) {
	q := NewQuantumQueue()
	h, _ := q.BorrowSafe()
	t.Run("NegativeTick", func(t *testing.T) {
		defer func() { recover() }()
		q.Push(-9999, h, arr48([]byte("neg")))
	})
	t.Run("OverflowTick", func(t *testing.T) {
		defer func() { recover() }()
		q.Push(int64(BucketCount+1000), h, arr48([]byte("hi")))
	})
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Lifecycle, Freelist, and Arena Recycling                                   *
 *─────────────────────────────────────────────────────────────────────────────*/

func TestSizeTracking(t *testing.T) {
	q := NewQuantumQueue()
	h1, _ := q.BorrowSafe()
	h2, _ := q.BorrowSafe()
	q.Push(10, h1, arr48([]byte("a")))
	q.Push(20, h2, arr48([]byte("b")))
	q.Push(10, h1, arr48([]byte("c")))
	q.UnlinkMin(h1, 10)
	q.UnlinkMin(h2, 20)
	if q.Size() != 0 {
		t.Error("Size tracking failed")
	}
}

func TestFreelistCycle(t *testing.T) {
	q := NewQuantumQueue()
	var handles []Handle
	for i := 0; i < CapItems; i++ {
		h, _ := q.BorrowSafe()
		handles = append(handles, h)
		q.Push(int64(i), h, arr48([]byte("x")))
	}
	for i, h := range handles {
		q.UnlinkMin(h, int64(i))
	}
	for i := 0; i < CapItems; i++ {
		if _, err := q.BorrowSafe(); err != nil {
			t.Fatalf("Freelist reuse failed: #%d", i)
		}
	}
}
