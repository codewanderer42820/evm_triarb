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
	handles := make([]Handle, 0, capItems)
	for i := 0; i < capItems; i++ {
		h, err := q.Borrow()
		if err != nil {
			t.Fatalf("unexpected borrow fail: %v", err)
		}
		handles = append(handles, h)
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
