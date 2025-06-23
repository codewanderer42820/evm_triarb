package bucketqueue

import "testing"

func TestPushPastWindow(t *testing.T) {
    q := New()
    h := borrowOrPanic(t, q)
    tick := int64(q.baseTick + numBuckets)
    err := q.Push(tick, h, nil)
    expectError(t, err, ErrBeyondWindow)
}

func TestUpdateTickOutOfBounds(t *testing.T) {
    q := New()
    h := borrowOrPanic(t, q)
    pushOrFatal(t, q, 0, h)
    past := int64(q.baseTick - 1)
    err := q.Update(past, h, nil)
    expectError(t, err, ErrPastWindow)
}

func TestRemove(t *testing.T) {
    q := New()
    h := borrowOrPanic(t, q)
    pushOrFatal(t, q, 5, h)
    if err := q.Remove(h); err != nil {
        t.Fatalf("remove: %v", err)
    }
    if !q.Empty() {
        t.Fatalf("queue should be empty; size=%d", q.Size())
    }
}

func TestGenWrapClearsBucketGen(t *testing.T) {
    q := New()
    for i := 0; i < numBuckets; i++ {
        h := borrowOrPanic(t, q)
        pushOrFatal(t, q, int64(i), h)
    }
    // force generation wrap
    for i := uint32(0); i < ^uint32(0); i++ {
        q.gen++
    }
    q.recycleStaleBuckets()
    for _, g := range q.bucketGen {
        if g != 0 {
            t.Fatalf("bucketGen not cleared")
        }
    }
}
