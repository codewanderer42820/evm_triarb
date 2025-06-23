// bucketqueue_full_test.go — exhaustive functional tests for Queue
package bucketqueue

import "testing"

func TestBasicPushPop(t *testing.T) {
	q := New()
	h1, h2 := borrowOrPanic(t, q), borrowOrPanic(t, q)

	pushOrPanic(t, q, 5_000_000, h1)
	expectSize(t, q, 1)
	expectPeep(t, q, h1, 5_000_000)

	pushOrPanic(t, q, 5_000_002, h2)
	expectSize(t, q, 2)

	got, tick, _ := q.PopMin()
	expectTick(t, tick, 5_000_000)
	expectHandle(t, got, h1)

	got, tick, _ = q.PopMin()
	expectTick(t, tick, 5_000_002)
	expectHandle(t, got, h2)
	expectEmpty(t, q)
}

// ─── past-window guard ─────────────────────────────────────────────────────────
// We advance the window first, then try to insert behind it.
func TestPushPastWindow(t *testing.T) {
	q := New()
	adv := q.baseTick + numBuckets + 32
	h := borrowOrPanic(t, q)
	pushOrPanic(t, q, adv, h)

	bad := borrowOrPanic(t, q)
	err := q.Push(q.baseTick-1, bad, nil) // now <baseTick
	expectError(t, err, ErrPastTick)
}

func TestUpdateTick(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	pushOrPanic(t, q, 1, h)

	_ = q.Update(20, h, nil)
	got, tick, _ := q.PeepMin()
	expectTick(t, tick, 20)
	expectHandle(t, got, h)
}

func TestUpdateTickOutOfBounds(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)

	// 1.  Push far enough ahead to slide the window forward.
	far := q.baseTick + numBuckets + 16 // > window span → baseTick becomes `far`
	pushOrPanic(t, q, far, h)

	// 2.  Now try to update to a tick that is *before* the new baseTick.
	past := q.baseTick - 1
	expectError(t, q.Update(past, h, nil), ErrPastTick)
}

func TestRemoveEdgeCases(t *testing.T) {
	q := New()

	err := q.Remove(Handle(capItems + 8))
	expectError(t, err, ErrItemNotFound)

	h := borrowOrPanic(t, q)
	expectError(t, q.Remove(h), ErrItemNotFound) // not in queue

	pushOrPanic(t, q, 1, h)
	err = q.Remove(h) // now valid
	expectError(t, err, nil)
	expectEmpty(t, q)
}

func TestReturnErrors(t *testing.T) {
	q := New()

	err := q.Return(Handle(capItems + 99))
	expectError(t, err, ErrItemNotFound)

	h := borrowOrPanic(t, q)
	pushOrPanic(t, q, 1, h)
	err = q.Return(h)
	expectErrorContains(t, err, "cannot return active handle")

	_, _, _ = q.PopMin()
	err = q.Return(h) // now inactive
	expectError(t, err, nil)
}

func TestBorrowExhaustion(t *testing.T) {
	q := New()
	for i := uint64(0); i < capItems; i++ {
		if _, err := q.Borrow(); err != nil {
			t.Fatalf("unexpected borrow fail at i=%d: %v", i, err)
		}
	}
	_, err := q.Borrow()
	expectError(t, err, ErrFull)
}

func TestPopMinMultiCount(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)

	for i := 0; i < 4; i++ {
		pushOrPanic(t, q, 0, h)
	}
	expectSize(t, q, 4)

	for left := uint64(4); left > 0; left-- {
		got, tick, _ := q.PopMin()
		expectHandle(t, got, h)
		expectTick(t, tick, 0)
		expectSize(t, q, left-1)
	}
	expectEmpty(t, q)
}

// ─── generation-wrap test (bucketGen reset) ───────────────────────────────────

func TestGenWrapClearsBucketGen(t *testing.T) {
	q := New()
	for i := range q.bucketGen {
		q.bucketGen[i] = 123
	}
	q.gen = ^uint64(0) // force overflow next Push

	h := borrowOrPanic(t, q)
	pushOrPanic(t, q, q.baseTick+numBuckets, h) // triggers wrap

	for i, g := range q.bucketGen {
		if g != 0 {
			t.Fatalf("bucketGen[%d] not cleared (got %d)", i, g)
		}
	}
}
