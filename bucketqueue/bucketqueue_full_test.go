// bucketqueue_full_test.go — comprehensive functional tests for bucketqueue
// =======================================================================
// Exercises all core operations and edge cases of the Queue:
// Borrow, Return, Push, PopMin, PeepMin, Update, Remove, Size, Empty,
// tick sliding, generation rotation, and arena exhaustion.

package bucketqueue

import (
	"testing"
)

func TestFullQueue_AllSituations(t *testing.T) {

	t.Run("BasicPushPop", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 1, h)
		expectSize(t, q, 1)
		expectPeep(t, q, h, 1)

		popped, tick := q.PopMin()
		expectHandle(t, popped, h)
		expectTick(t, tick, 1)
		expectEmpty(t, q)
	})

	t.Run("PeepMinEmpty", func(t *testing.T) {
		q := New()
		expectPeep(t, q, Handle(nilIdx), 0)
	})

	t.Run("PeepMinBasic", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 5, h)
		expectPeep(t, q, h, 5)
	})

	t.Run("PeepMinWithDuplicatePushes", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		for i := 0; i < 3; i++ {
			pushOrPanic(t, q, 7, h)
		}
		expectSize(t, q, 3)
		expectPeep(t, q, h, 7)
	})

	t.Run("PeepMinAfterPop", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 9, h)
		expectPeep(t, q, h, 9)
		q.PopMin()
		expectPeep(t, q, Handle(nilIdx), 0)
	})

	t.Run("PeepMinUpdatesWithTickSlide", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 10, h)
		_ = q.Update(int64(q.baseTick)+numBuckets+5, h) // force tick slide
		expectSize(t, q, 1)
		ph, tick := q.PeepMin()
		expectHandle(t, ph, h)
		if tick < int64(q.baseTick) {
			t.Fatalf("PeepMin returned stale tick")
		}
	})

	t.Run("DuplicatePushSameTick", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 5, h)
		pushOrPanic(t, q, 5, h)

		expectSize(t, q, 2)
		expectPeep(t, q, h, 5)

		pop1, _ := q.PopMin()
		expectHandle(t, pop1, h)

		expectSize(t, q, 1)
		expectPeep(t, q, h, 5)

		pop2, _ := q.PopMin()
		expectHandle(t, pop2, h)
		expectEmpty(t, q)
	})

	t.Run("UpdateTick", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 10, h)
		_ = q.Update(20, h)
		p, tk := q.PopMin()
		expectHandle(t, p, h)
		expectTick(t, tk, 20)
	})

	t.Run("RemoveMidList", func(t *testing.T) {
		q := New()
		h1 := borrowOrPanic(t, q)
		h2 := borrowOrPanic(t, q)
		pushOrPanic(t, q, 7, h1)
		pushOrPanic(t, q, 7, h2)

		_ = q.Remove(h1)
		p, _ := q.PopMin()
		expectHandle(t, p, h2)
	})

	t.Run("ReturnErrors", func(t *testing.T) {
		q := New()
		expectError(t, q.Return(-1), ErrItemNotFound)

		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 3, h)
		expectErrorContains(t, q.Return(h), "cannot return active handle")
	})

	t.Run("PastTickError", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		expectError(t, q.Push(-1, h), ErrPastTick)
	})

	t.Run("FullArena", func(t *testing.T) {
		q := New()
		for i := 0; i < capItems; i++ {
			h, err := q.Borrow()
			if err != nil {
				t.Fatalf("Unexpected borrow error at %d: %v", i, err)
			}
			_ = h
		}
		_, err := q.Borrow()
		expectError(t, err, ErrFull)
	})

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

	t.Run("PopEmpty", func(t *testing.T) {
		q := New()
		h, tick := q.PopMin()
		expectHandle(t, h, Handle(nilIdx))
		expectTick(t, tick, 0)
	})

	t.Run("InvalidPushHandle", func(t *testing.T) {
		q := New()
		expectError(t, q.Push(1, Handle(-1)), ErrItemNotFound)
	})

	t.Run("InvalidRemoveHandle", func(t *testing.T) {
		q := New()
		expectError(t, q.Remove(Handle(-1)), ErrItemNotFound)
		expectError(t, q.Remove(Handle(capItems)), ErrItemNotFound)
	})

	t.Run("UpdateInvalidHandle", func(t *testing.T) {
		q := New()
		expectError(t, q.Update(0, Handle(-1)), ErrItemNotFound)
	})

	t.Run("RemoveUnusedHandleError", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		expectError(t, q.Remove(h), ErrItemNotFound)
	})

	t.Run("RemoveInternalHeadLink", func(t *testing.T) {
		q := New()
		h1 := borrowOrPanic(t, q)
		h2 := borrowOrPanic(t, q)
		pushOrPanic(t, q, 42, h1)
		pushOrPanic(t, q, 42, h2)
		expectSize(t, q, 2)

		_ = q.Remove(h2)
		p, _ := q.PopMin()
		expectHandle(t, p, h1)
	})

	t.Run("ReturnActiveAndRelease", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, 7, h)

		// Should error: handle is still active in queue
		expectErrorContains(t, q.Return(h), "cannot return active handle")

		// Pop to deactivate
		_, _ = q.PopMin()

		// Should succeed now
		if err := q.Return(h); err != nil {
			t.Fatalf("Expected successful Return after PopMin; got %v", err)
		}

		// Should be borrowable again
		h2, err := q.Borrow()
		if err != nil || h2 != h {
			t.Fatalf("Expected re-borrow of same handle; got %v, %v", h2, err)
		}
	})

	t.Run("UpdatePastTickError", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		q.baseTick += 10
		expectError(t, q.Update(int64(q.baseTick-1), h), ErrPastTick)
	})

	t.Run("UpdateInvalidHandleState", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)

		// Never pushed ⇒ count==0, bucketIdx==-1
		expectError(t, q.Update(10, h), ErrItemNotFound)

		// Stale generation
		pushOrPanic(t, q, int64(q.baseTick), h)
		q.baseTick += numBuckets + 5
		q.gen++
		q.recycleStaleBuckets()
		expectError(t, q.Update(int64(q.baseTick), h), ErrItemNotFound)
	})

	t.Run("UpdateNoOpSameTick", func(t *testing.T) {
		q := New()
		h := borrowOrPanic(t, q)
		tick := int64(q.baseTick + 3)
		pushOrPanic(t, q, tick, h)
		expectError(t, q.Update(tick, h), nil) // should return nil
	})
}
