// ─────────────────────────────────────────────────────────────────────────────
// queue_bench_test.go — Microbenchmarks for QuantumQueue
//
// Purpose:
//   - Measure cost of core API: Push, PeepMin, MoveTick, UnlinkMin, etc.
//   - Model ISR usage patterns: bursty updates, tick reuse, summary collapses
//
// Benchmark Notes:
//   - All use pre-filled arenas (CapItems) unless noted
//   - Edge tick usage (0, max) included for performance consistency
//
// Modes:
//   - Hot path = same tick / repeated update
//   - Cold path = random / spread tick patterns
//
// ─────────────────────────────────────────────────────────────────────────────

package quantumqueue

import (
	"math/rand"
	"testing"
	"time"
)

/*─────────────────────────────────────────────────────────────────────────────*
 * Constants                                                                   *
 *─────────────────────────────────────────────────────────────────────────────*/

const benchSize = CapItems // use full arena for stress realism

/*─────────────────────────────────────────────────────────────────────────────*
 * Metadata Access: Empty(), Size()                                            *
 *─────────────────────────────────────────────────────────────────────────────*/

// -----------------------------------------------------------------------------
// BenchmarkEmpty measures cost of q.Empty(), a single field check
// -----------------------------------------------------------------------------
func BenchmarkEmpty(b *testing.B) {
	q := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Empty()
	}
}

// -----------------------------------------------------------------------------
// BenchmarkSize measures cost of q.Size(), also a field load
// -----------------------------------------------------------------------------
func BenchmarkSize(b *testing.B) {
	q := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Size()
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Push Benchmarks                                                             *
 *─────────────────────────────────────────────────────────────────────────────*/

// -----------------------------------------------------------------------------
// BenchmarkPushUnique — cold path: new tick each time
// -----------------------------------------------------------------------------
func BenchmarkPushUnique(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}
	val := new([48]byte)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, val)
	}
}

// -----------------------------------------------------------------------------
// BenchmarkPushUpdate — hot path: update same tick
// -----------------------------------------------------------------------------
func BenchmarkPushUpdate(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, new([48]byte))
	}
	val := new([48]byte)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(int64(i%benchSize), h, val)
	}
}

// -----------------------------------------------------------------------------
// BenchmarkPushSameTickZero — repeated writes to tick 0
// -----------------------------------------------------------------------------
func BenchmarkPushSameTickZero(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	val := new([48]byte)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(0, h, val)
	}
}

// -----------------------------------------------------------------------------
// BenchmarkPushSameTickMax — test upper boundary tick
// -----------------------------------------------------------------------------
func BenchmarkPushSameTickMax(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	val := new([48]byte)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(int64(benchSize-1), h, val)
	}
}

// -----------------------------------------------------------------------------
// BenchmarkPushRandom — fully random tick burst pattern
// -----------------------------------------------------------------------------
func BenchmarkPushRandom(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
	}
	rand.Seed(time.Now().UnixNano())
	ticks := make([]int64, benchSize)
	for i := range ticks {
		ticks[i] = rand.Int63n(benchSize)
	}
	val := new([48]byte)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.Push(ticks[i%benchSize], h, val)
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
 * PeepMin                                                                    *
 *─────────────────────────────────────────────────────────────────────────────*/

// -----------------------------------------------------------------------------
// BenchmarkPeepMin — measure minimum extraction cost
// -----------------------------------------------------------------------------
func BenchmarkPeepMin(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, new([48]byte))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PeepMin()
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
 * UnlinkMin (Hot + Collapse Paths)                                           *
 *─────────────────────────────────────────────────────────────────────────────*/

// -----------------------------------------------------------------------------
// BenchmarkUnlinkMin_StableBucket — no summary update, just unlink
// -----------------------------------------------------------------------------
func BenchmarkUnlinkMin_StableBucket(b *testing.B) {
	q := New()
	h, _ := q.BorrowSafe()
	val := new([48]byte)
	q.Push(2048, h, val)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.UnlinkMin(h, 2048)
		q.Push(2048, h, val)
	}
}

// -----------------------------------------------------------------------------
// BenchmarkUnlinkMin_DenseBucket — 3-handle shared bucket (no collapse)
// -----------------------------------------------------------------------------
func BenchmarkUnlinkMin_DenseBucket(b *testing.B) {
	q := New()
	var hs [3]Handle
	val := new([48]byte)
	for i := 0; i < 3; i++ {
		hs[i], _ = q.BorrowSafe()
		q.Push(1234, hs[i], val)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := hs[i%3]
		q.UnlinkMin(h, 1234)
		q.Push(1234, h, val)
	}
}

// -----------------------------------------------------------------------------
// BenchmarkUnlinkMin_BitmapCollapse — each unlink collapses entire summary
// -----------------------------------------------------------------------------
func BenchmarkUnlinkMin_BitmapCollapse(b *testing.B) {
	q := New()
	val := new([48]byte)
	h, _ := q.BorrowSafe()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick := int64(i % BucketCount)
		q.Push(tick, h, val)
		q.UnlinkMin(h, tick)
	}
}

// -----------------------------------------------------------------------------
// BenchmarkUnlinkMin_ScatterCollapse — fully random ticks with frequent collapse
// -----------------------------------------------------------------------------
func BenchmarkUnlinkMin_ScatterCollapse(b *testing.B) {
	q := New()
	val := new([48]byte)
	h, _ := q.BorrowSafe()
	ticks := make([]int64, b.N)
	for i := range ticks {
		ticks[i] = int64(rand.Intn(BucketCount))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick := ticks[i]
		q.Push(tick, h, val)
		q.UnlinkMin(h, tick)
	}
}

// -----------------------------------------------------------------------------
// BenchmarkUnlinkMin_ReinsertAfterCollapse — always collapse + refill
// -----------------------------------------------------------------------------
func BenchmarkUnlinkMin_ReinsertAfterCollapse(b *testing.B) {
	q := New()
	val := new([48]byte)
	h, _ := q.BorrowSafe()
	const tick = 4095
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(tick, h, val)
		q.UnlinkMin(h, tick)
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
 * MoveTick                                                                   *
 *─────────────────────────────────────────────────────────────────────────────*/

// -----------------------------------------------------------------------------
// BenchmarkMoveTick — frequent tick relocations (unlink + reinsert)
// -----------------------------------------------------------------------------
func BenchmarkMoveTick(b *testing.B) {
	q := New()
	handles := make([]Handle, benchSize)
	for i := range handles {
		h, _ := q.BorrowSafe()
		handles[i] = h
		q.Push(int64(i), h, new([48]byte))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := handles[i%benchSize]
		q.MoveTick(h, int64((i+1)%benchSize))
	}
}
