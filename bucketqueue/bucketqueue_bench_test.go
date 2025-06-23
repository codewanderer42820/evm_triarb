package bucketqueue

import (
    "testing"
    "unsafe"
)

func BenchmarkPush(b *testing.B) {
    q := New()
    h, _ := q.Borrow()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = q.Push(int64(i), h, nil)
    }
}

func BenchmarkPopMin(b *testing.B) {
    q := New()
    h, _ := q.Borrow()
    for i := 0; i < b.N; i++ {
        _ = q.Push(int64(i), h, nil)
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        q.PopMin()
    }
}

func BenchmarkPushPopCycle(b *testing.B) {
    q := New()
    for i := 0; i < b.N; i++ {
        h, _ := q.Borrow()
        q.Push(int64(i), h, nil)
        q.PopMin()
    }
}

func BenchmarkMixedHeavy(b *testing.B) {
    q := New()
    handles := make([]Handle, 0, 1024)
    for i := 0; i < 1024; i++ {
        h, _ := q.Borrow()
        handles = append(handles, h)
    }
    b.ResetTimer()
    var tick int64
    for i := 0; i < b.N; i++ {
        h := handles[i&1023]
        _ = q.Push(tick, h, unsafe.Pointer(uintptr(i)))
        if i&1 == 1 {
            q.PopMin()
        }
        tick++
    }
}
