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
	const total = 1 << 12 // 4096
	q := New()

	// Allocate handles
	all := make([]Handle, total)
	for i := range all {
		h, _ := q.Borrow()
		all[i] = h
	}

	// Create random permutation of ticks
	rng := rand.New(rand.NewSource(1))
	perm := rng.Perm(total)

	// Build inverse map from tick → handle index
	inv := make([]int, total)
	for i, v := range perm {
		inv[v] = i
	}

	// Push handles at permuted tick values
	for i := 0; i < total; i++ {
		if err := q.Push(int64(perm[i]), all[i], nil); err != nil {
			t.Fatalf("Push failed at i=%d: %v", i, err)
		}
	}

	// Pop in order and verify tick and handle match
	for i := 0; i < total; i++ {
		h, tick, _ := q.PopMin()
		expectTick(t, tick, int64(i))
		expectHandle(t, h, all[inv[i]])
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
