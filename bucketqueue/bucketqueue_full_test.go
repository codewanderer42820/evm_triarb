// bucketqueue_full_test.go — fully patched and corrected
package bucketqueue

import (
	"testing"
)

func TestBasicPushPop(t *testing.T) {
	q := New()
	h1 := borrowOrPanic(t, q)
	h2 := borrowOrPanic(t, q)

	pushOrPanic(t, q, 5000000, h1)
	expectSize(t, q, 1)
	expectPeep(t, q, h1, 5000000)

	pushOrPanic(t, q, 5000002, h2)
	expectSize(t, q, 2)

	got, tick, _ := q.PopMin()
	expectTick(t, tick, 5000000)
	expectHandle(t, got, h1)
	expectSize(t, q, 1)

	got2, tick2, _ := q.PopMin()
	expectTick(t, tick2, 5000002)
	expectHandle(t, got2, h2)
	expectEmpty(t, q)
}

func TestPopPastWrapPoint(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)

	pushOrPanic(t, q, 9223372036854775800, h)
	_, _, _ = q.PopMin()

	h2 := borrowOrPanic(t, q)
	pushOrPanic(t, q, int64(q.baseTick+1), h2)
	_, _, _ = q.PopMin()
	expectEmpty(t, q)
}

func TestPushPastWraparound(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)

	tick := int64(9223372036854775800)
	pushOrPanic(t, q, tick, h)

	h2 := borrowOrPanic(t, q)
	pushOrPanic(t, q, tick+1, h2)

	_, _, _ = q.PopMin()
	_, _, _ = q.PopMin()
	expectEmpty(t, q)
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
	pushOrPanic(t, q, 0, h)
	expectError(t, q.Update(int64(q.baseTick-1), h, nil), ErrPastTick)
}

func TestUpdateInvalidHandleState(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	expectError(t, q.Update(10, h, nil), ErrItemNotFound)
}

func TestPushPopAlternating(t *testing.T) {
	q := New()
	const count = 100
	for i := 0; i < count; i++ {
		h := borrowOrPanic(t, q)
		pushOrPanic(t, q, int64(i), h)
		got, tick, _ := q.PopMin()
		expectTick(t, tick, int64(i))
		expectHandle(t, got, h)
	}
	expectEmpty(t, q)
}

func TestPushOutOfOrder(t *testing.T) {
	q := New()
	h1 := borrowOrPanic(t, q)
	h2 := borrowOrPanic(t, q)
	pushOrPanic(t, q, 10, h1)
	pushOrPanic(t, q, 5, h2)
	got, tick, _ := q.PopMin()
	expectTick(t, tick, 5)
	expectHandle(t, got, h2)
	got, tick, _ = q.PopMin()
	expectTick(t, tick, 10)
	expectHandle(t, got, h1)
	expectEmpty(t, q)
}

func TestPushNegativeTick(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	expectError(t, q.Push(-1, h, nil), ErrPastTick)
}

func TestUpdateNoOpSameTick(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	tick := int64(7)
	pushOrPanic(t, q, tick, h)
	expectError(t, q.Update(tick, h, nil), nil)
}

func TestReuseHandle(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	for i := int64(0); i < 50; i++ {
		_ = q.Push(i, h, nil)
		got, tick, _ := q.PopMin()
		expectTick(t, tick, i)
		expectHandle(t, got, h)
	}
}

func TestReturnErrors(t *testing.T) {
	q := New()

	err := q.Return(-1)
	expectError(t, err, ErrItemNotFound)

	h := borrowOrPanic(t, q)
	pushOrPanic(t, q, 1, h)
	err = q.Return(h)
	expectErrorContains(t, err, "cannot return active handle")

	_, _, _ = q.PopMin() // make it inactive
	err = q.Return(h)
	expectError(t, err, nil)
}

func TestRemoveEdgeCases(t *testing.T) {
	q := New()
	err := q.Remove(-1)
	expectError(t, err, ErrItemNotFound)

	h := borrowOrPanic(t, q)
	expectError(t, q.Remove(h), ErrItemNotFound)

	pushOrPanic(t, q, 1, h)
	err = q.Remove(h)
	expectError(t, err, nil)
	expectEmpty(t, q)

	// Make sure it's reusable
	pushOrPanic(t, q, 2, h)
	got, tick, _ := q.PopMin()
	expectTick(t, tick, 2)
	expectHandle(t, got, h)
}

func TestBorrowExhaustion(t *testing.T) {
	q := New()
	for i := 0; i < capItems; i++ {
		if _, err := q.Borrow(); err != nil {
			t.Fatalf("unexpected borrow fail at i=%d: %v", i, err)
		}
	}
	_, err := q.Borrow()
	expectError(t, err, ErrFull)
}

func TestPushBadHandle(t *testing.T) {
	q := New()
	err := q.Push(1, -1, nil)
	expectError(t, err, ErrItemNotFound)

	err = q.Push(1, capItems, nil)
	expectError(t, err, ErrItemNotFound)
}

func TestPushTriggersDetachAndLink(t *testing.T) {
	q := New()
	h1 := borrowOrPanic(t, q)
	h2 := borrowOrPanic(t, q)

	// h1 into tick 1
	pushOrPanic(t, q, 1, h1)
	// h2 into same bucket to ensure h1.prev gets updated
	pushOrPanic(t, q, 1, h2)

	// Move h1 to a different tick to trigger detach + prev patch
	pushOrPanic(t, q, 2, h1)
}

func TestPopMinWithNext(t *testing.T) {
	q := New()
	h1 := borrowOrPanic(t, q)
	h2 := borrowOrPanic(t, q)

	// Push two handles to same tick to build a list
	pushOrPanic(t, q, 5, h1)
	pushOrPanic(t, q, 5, h2)

	// One pop leaves the other in the list, hitting `next != nilIdx`
	got, tick, _ := q.PopMin()
	if got != h2 && got != h1 {
		t.Fatalf("Unexpected handle from PopMin with next")
	}
	expectTick(t, tick, 5)
}

func TestPeepMinEmpty(t *testing.T) {
	q := New()
	h, tick, data := q.PeepMin()
	expectHandle(t, h, Handle(nilIdx))
	expectTick(t, tick, 0)
	if data != nil {
		t.Fatalf("Expected nil data from empty PeepMin")
	}
}

func TestDetachBothPrevNext(t *testing.T) {
	q := New()
	h1 := borrowOrPanic(t, q)
	h2 := borrowOrPanic(t, q)
	h3 := borrowOrPanic(t, q)

	// Chain three handles in the same bucket
	pushOrPanic(t, q, 5, h1)
	pushOrPanic(t, q, 5, h2)
	pushOrPanic(t, q, 5, h3)

	// Remove h2, which is in the middle — triggers both prev/next logic
	expectError(t, q.Remove(h2), nil)
}

func TestRecycleStaleBuckets(t *testing.T) {
	q := New()
	h1 := borrowOrPanic(t, q)

	// Push at baseTick + 5 (within window)
	pushOrPanic(t, q, int64(q.baseTick+5), h1)

	// Trigger stale bucket cleanup (shift baseTick by numBuckets)
	h2 := borrowOrPanic(t, q)
	pushOrPanic(t, q, int64(q.baseTick)+numBuckets+1, h2)

	// Ensure q is still valid
	got, tick, _ := q.PopMin()
	expectHandle(t, got, h2)
	expectTick(t, tick, int64(q.baseTick))
}

func TestDirectRelease(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	pushOrPanic(t, q, 1, h)
	expectError(t, q.Remove(h), nil) // hits `release`
	// Make sure h reusable
	pushOrPanic(t, q, 2, h)
}

// TestPopMinMultiCount exercises the fast “count– –” path and the
// final-removal path of PopMin.
func TestPopMinMultiCount(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)

	const dup = 5 // push the same handle 5 times
	for i := 0; i < dup; i++ {
		pushOrPanic(t, q, 0, h)
	}
	expectSize(t, q, dup)

	// First dup-1 pops go through the n.count>1 fast path,
	// the last pop removes the node entirely.
	for i := 0; i < dup; i++ {
		got, tick, _ := q.PopMin()
		expectHandle(t, got, h)
		expectTick(t, tick, 0)
		expectSize(t, q, dup-i-1)
	}
	expectEmpty(t, q)
}

// TestGenWrapClearsBucketGen hits the for-range zeroing loop in Push.
// It pre-poisons bucketGen, forces q.gen to overflow to 0, then verifies
// that every entry was cleared back to zero.
func TestGenWrapClearsBucketGen(t *testing.T) {
	q := New()

	// Poison bucketGen so we can detect the reset.
	for i := range q.bucketGen {
		q.bucketGen[i] = 123
	}

	// Force the next Push() to wrap the generation counter.
	q.gen = ^uint32(0) // 0xFFFFFFFF

	h := borrowOrPanic(t, q)
	wrapTick := int64(q.baseTick) + int64(numBuckets) // d ≥ numBuckets → window shift
	if err := q.Push(wrapTick, h, nil); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	// After wrap every bucketGen entry must be zero.
	for i, g := range q.bucketGen {
		if g != 0 {
			t.Fatalf("bucketGen[%d] not cleared (got %d)", i, g)
		}
	}
}

// TestUpdateBadHandle exercises the out-of-range guard in Update.
func TestUpdateBadHandle(t *testing.T) {
	q := New()
	tick := int64(q.baseTick)

	err := q.Update(tick, -1, nil)
	expectError(t, err, ErrItemNotFound)

	err = q.Update(tick, capItems, nil)
	expectError(t, err, ErrItemNotFound)
}
