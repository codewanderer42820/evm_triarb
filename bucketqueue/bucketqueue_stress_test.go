package bucketqueue

import (
	"math/rand"
	"testing"
)

func TestPushPopStress(t *testing.T) {
	const total = 1 << 12

	q := New()
	all := make([]Handle, 0, total)
	for i := 0; i < total; i++ {
		h, _ := q.Borrow()
		all = append(all, h)
		if err := q.Push(int64(i), h, nil); err != nil {
			t.Fatalf("Push failed at i=%d: %v", i, err)
		}
	}
	for i := 0; i < total; i++ {
		h, tick, _ := q.PopMin()
		expectTick(t, tick, int64(i))
		expectHandle(t, h, all[i])
	}
}

func TestPushOutOfOrderStress(t *testing.T) {
	const total = 1 << 12

	q := New()
	all := make([]Handle, total)
	for i := range all {
		all[i], _ = q.Borrow()
	}

	perm := rand.New(rand.NewSource(1)).Perm(total)
	for i := 0; i < total; i++ {
		h := all[i]
		tick := perm[i]
		if err := q.Push(int64(tick), h, nil); err != nil {
			t.Fatalf("Push failed: %v", err)
		}
	}
	for i := 0; i < total; i++ {
		h, tick, _ := q.PopMin()
		expectTick(t, tick, int64(i))
		expectHandle(t, h, all[tick])
	}
}

func TestPushNegativeTickStress(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)
	err := q.Push(int64(q.baseTick)-1, h, nil)
	expectError(t, err, ErrPastTick)
}

func TestPopEmptyStress(t *testing.T) {
	q := New()
	_, _, _ = q.PopMin() // should not panic
}

func TestRingWrapStress(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)

	const jump = int64(1 << 20)
	farTick := int64(q.baseTick) + jump

	if err := q.Push(farTick, h, nil); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	for i := 0; i < 10; i++ {
		_ = q.Push(farTick, h, nil)
	}
	_, _, _ = q.PopMin()
}

func TestReuseStress(t *testing.T) {
	const rounds = 64
	q := New()
	h := borrowOrPanic(t, q)

	for i := int64(0); i < rounds; i++ {
		if err := q.Push(i, h, nil); err != nil {
			t.Fatalf("Push failed at %d: %v", i, err)
		}
		got, tick, _ := q.PopMin()
		expectTick(t, tick, i)
		expectHandle(t, got, h)
	}
}
