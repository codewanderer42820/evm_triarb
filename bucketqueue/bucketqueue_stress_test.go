package bucketqueue

import (
	"math/rand"
	"testing"
)

func TestPushPopStress(t *testing.T) {
	const total = 1 << 12
	q := New()

	handles := make([]Handle, 0, total)
	for i := 0; i < total; i++ {
		h := borrowOrPanic(t, q)
		handles = append(handles, h)
		pushOrPanic(t, q, uint64(i), h)
	}
	for i := 0; i < total; i++ {
		h, tick, _ := q.PopMin()
		expectTick(t, tick, uint64(i))
		expectHandle(t, h, handles[i])
	}
}

func TestPushOutOfOrderStress(t *testing.T) {
	const total = 1 << 12
	q := New()

	all := make([]Handle, total)
	for i := range all {
		all[i] = borrowOrPanic(t, q)
	}

	rng := rand.New(rand.NewSource(1))
	perm := rng.Perm(total)

	inv := make([]int, total)
	for i, v := range perm {
		inv[v] = i
	}

	for i := 0; i < total; i++ {
		if err := q.Push(uint64(perm[i]), all[i], nil); err != nil {
			t.Fatalf("Push failed at i=%d: %v", i, err)
		}
	}

	for i := 0; i < total; i++ {
		h, tick, _ := q.PopMin()
		expectTick(t, tick, uint64(i))
		expectHandle(t, h, all[inv[i]])
	}
}

func TestPushPastWindowStress(t *testing.T) {
	q := New()
	h1 := borrowOrPanic(t, q)
	far := q.baseTick + numBuckets + 8
	pushOrPanic(t, q, far, h1)

	h2 := borrowOrPanic(t, q)
	err := q.Push(q.baseTick-1, h2, nil)
	expectError(t, err, ErrPastTick)
}

func TestPopEmptyStress(t *testing.T) {
	q := New()
	_, _, _ = q.PopMin() // should not panic
}

func TestRingWrapStress(t *testing.T) {
	q := New()
	h := borrowOrPanic(t, q)

	far := q.baseTick + (1 << 20)
	pushOrPanic(t, q, far, h)
	for i := 0; i < 10; i++ {
		_ = q.Push(far, h, nil)
	}
	_, _, _ = q.PopMin()
}

func TestReuseStress(t *testing.T) {
	const rounds = 64
	q := New()
	h := borrowOrPanic(t, q)

	for i := uint64(0); i < rounds; i++ {
		pushOrPanic(t, q, i, h)
		got, tick, _ := q.PopMin()
		expectTick(t, tick, i)
		expectHandle(t, got, h)
	}
}
