// bucketqueue_full_test.go — comprehensive functional tests for bucketqueue
// =======================================================================
// Exercises all core operations and edge cases of the Queue:
// Borrow, Return, Push, PopMin, Update, Remove, error conditions,
// window sliding, arena exhaustion, and helper functions.

package bucketqueue

import (
	"testing"
)

// TestFullQueue_AllSituations groups sub-tests covering core behavior,
// boundary conditions, and error handling for Queue.
func TestFullQueue_AllSituations(t *testing.T) {

	// Basic single-element push/pop scenario
	t.Run("BasicPushPop", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 1, h)
		popped, tick := q.PopMin()
		if popped != h || tick != 1 {
			t.Fatalf("Expected PopMin to return handle %v at tick 1; got %v@%d", h, popped, tick)
		}
		if !q.Empty() || q.Size() != 0 {
			t.Fatalf("Expected empty queue after PopMin; got size=%d", q.Size())
		}
	})

	// Duplicate-push should bump ref-count, needing two pops to drain
	t.Run("DuplicatePushSameTick", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 5, h)
		pushOrPanic(t, q, 5, h)
		pop1, t1 := q.PopMin()
		if pop1 != h || t1 != 5 {
			t.Fatalf("First PopMin expected %v@5; got %v@%d", h, pop1, t1)
		}
		if q.Empty() {
			t.Fatalf("Queue unexpectedly empty after first pop")
		}
		pop2, t2 := q.PopMin()
		if pop2 != h || t2 != 5 {
			t.Fatalf("Second PopMin expected %v@5; got %v@%d", h, pop2, t2)
		}
		if !q.Empty() {
			t.Fatalf("Expected empty queue after second pop")
		}
	})

	// Update moves an existing handle from one tick to another
	t.Run("UpdateTick", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 10, h)
		if err := q.Update(20, h); err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		p, tk := q.PopMin()
		if p != h || tk != 20 {
			t.Fatalf("Expected updated handle %v@20; got %v@%d", h, p, tk)
		}
	})

	// Remove from middle of bucket list
	t.Run("RemoveMidList", func(t *testing.T) {
		q := New()
		h1 := borrowOrPanic(t, q)
		h2 := borrowOrPanic(t, q)
		pushOrPanic(t, q, 7, h1)
		pushOrPanic(t, q, 7, h2)
		if err := q.Remove(h1); err != nil {
			t.Fatalf("Remove failed: %v", err)
		}
		pop, _ := q.PopMin()
		if pop != h2 {
			t.Fatalf("Expected remaining handle %v; got %v", h2, pop)
		}
	})

	// Return should error on invalid handle values
	t.Run("ReturnErrors", func(t *testing.T) {
		q := New()
		if err := q.Return(-1); err != ErrItemNotFound {
			t.Fatalf("Expected ErrItemNotFound for invalid return")
		}
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 3, h)
		if err := q.Return(h); err == nil {
			t.Fatalf("Expected error when returning in-use handle")
		}
	})

	// Pushing a tick before current baseTick should error
	t.Run("PastTickError", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		// Negative tick is always before the window start
		if err := q.Push(-1, h); err != ErrPastTick {
			t.Fatalf("Expected ErrPastTick for tick -1; got %v", err)
		}
	})

	// Exhausting all handles should yield ErrFull
	t.Run("FullArena", func(t *testing.T) {
		q := New()
		handles := make([]Handle, capItems)
		for i := 0; i < capItems; i++ {
			h, err := q.Borrow()
			if err != nil {
				t.Fatalf("Unexpected borrow error at %d: %v", i, err)
			}
			handles[i] = h
		}
		if _, err := q.Borrow(); err != ErrFull {
			t.Fatalf("Expected ErrFull when arena exhausted; got %v", err)
		}
	})

	// Window slides and recycles handles
	t.Run("WindowSlideAndRecycle", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		for i := 0; i < capItems; i++ {
			pushOrPanic(t, q, int64(i), h)
			q.PopMin()
		}
		if _, err := q.Borrow(); err != nil {
			t.Fatalf("Expected recycled handle; got %v", err)
		}
	})

	// PopMin on empty queue should return nilIdx and zero tick
	t.Run("PopEmpty", func(t *testing.T) {
		q := New()
		pop, tick := q.PopMin()
		if pop != Handle(nilIdx) || tick != 0 {
			t.Fatalf("Expected nilIdx,0 on empty PopMin; got %v@%d", pop, tick)
		}
	})

	// Invalid push should error
	t.Run("InvalidPushHandle", func(t *testing.T) {
		q := New()
		if err := q.Push(1, Handle(-1)); err != ErrItemNotFound {
			t.Fatalf("Expected ErrItemNotFound for invalid push; got %v", err)
		}
	})

	// Invalid remove should error
	t.Run("InvalidRemoveHandle", func(t *testing.T) {
		q := New()
		if err := q.Remove(Handle(-1)); err != ErrItemNotFound {
			t.Fatalf("Expected ErrItemNotFound for negative remove; got %v", err)
		}
		if err := q.Remove(Handle(capItems)); err != ErrItemNotFound {
			t.Fatalf("Expected ErrItemNotFound for out-of-range remove; got %v", err)
		}
	})

	// Invalid update should error
	t.Run("UpdateInvalidHandle", func(t *testing.T) {
		q := New()
		if err := q.Update(0, Handle(-1)); err != ErrItemNotFound {
			t.Fatalf("Expected ErrItemNotFound for invalid update; got %v", err)
		}
	})

	// Helper functions should not panic normally
	t.Run("BorrowHelper", func(t *testing.T) {
		q := New()
		_ = borrowOrPanic(t, q)
	})

	t.Run("PushHelper", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 1, h)
	})

	// ───── Remove on an unused handle → ErrItemNotFound ────────────────
	t.Run("RemoveUnusedHandleError", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		// never pushed, so count==0 ⇒ Remove must return ErrItemNotFound
		if err := q.Remove(h); err != ErrItemNotFound {
			t.Fatalf("Expected ErrItemNotFound for unused handle; got %v", err)
		}
	}) // covers `if n.count==0 … return ErrItemNotFound`

	// ───── removeInternal head-unlink path ────────────────────────────
	t.Run("RemoveInternalHeadLink", func(t *testing.T) {
		q := New()
		h1 := borrowOrPanic(t, q)
		h2 := borrowOrPanic(t, q)
		// both at same tick ⇒ list: h2 → h1
		pushOrPanic(t, q, 42, h1)
		pushOrPanic(t, q, 42, h2)
		// removing the head (h2) should rewire h1.prev = nilIdx
		if err := q.Remove(h2); err != nil {
			t.Fatalf("Head removal failed: %v", err)
		}
		if p, _ := q.PopMin(); p != h1 {
			t.Fatalf("Expected h1 after head-unlink; got %v", p)
		}
	}) // exercises `if n.next != nilIdx { q.arena[n.next].prev = n.prev }`

	// ───── Return on an active handle hits error, then release path ────
	t.Run("ReturnActiveAndRelease", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 7, h)
		// still in queue ⇒ Return must error
		err := q.Return(h)
		if err == nil {
			t.Fatalf("Expected error when returning active handle; got nil")
		}
		if err.Error() != "bucketqueue: cannot return active handle" {
			t.Fatalf("Unexpected error message: %v", err)
		}
		// now pop it out and try Return again ⇒ hits release path
		_, _ = q.PopMin()
		if err := q.Return(h); err != nil {
			t.Fatalf("Expected successful Return after pop; got %v", err)
		}
		// finally, borrowing again should succeed
		h2, err := q.Borrow()
		if err != nil || h2 != h {
			t.Fatalf("Expected to re-borrow same handle; got %v, %v", h2, err)
		}
	}) // covers `q.release(idx); return nil` in Return

}
