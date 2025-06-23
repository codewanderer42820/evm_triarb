package bucketqueue

import (
    "math/rand"
    "testing"
)

func TestPushOutOfOrderStress(t *testing.T) {
    const N = 1000
    q := New()
    perm := rand.Perm(N)
    for i := 0; i < N; i++ {
        h := borrowOrPanic(t, q)
        pushOrFatal(t, q, int64(perm[i]), h)
    }
    if q.Size() != N {
        t.Fatalf("size mismatch; want %d got %d", N, q.Size())
    }
    for !q.Empty() {
        q.PopMin()
    }
}
