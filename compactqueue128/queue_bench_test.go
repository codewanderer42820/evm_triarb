// ============================================================================
// COMPACTQUEUE128 MICROBENCHMARK SUITE
// ============================================================================
//
// Comprehensive performance measurement suite for CompactQueue128 core operations.
// Validates sub-8ns operation latency under realistic shared pool workload patterns.

package compactqueue128

import (
	"math/rand"
	"testing"
	"unsafe"
)

// ============================================================================
// BENCHMARK CONFIGURATION
// ============================================================================

const (
	benchPoolSize = 100000 // Shared pool size for benchmarks
	benchHandles  = 10000  // Number of handles to use in tests
)

// ============================================================================
// METADATA ACCESS BENCHMARKS
// ============================================================================

// BenchmarkEmpty measures the cost of queue emptiness checking.
func BenchmarkEmpty(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	var result bool
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = q.Empty()
	}
	_ = result
}

// BenchmarkSize measures the cost of queue size retrieval.
func BenchmarkSize(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	var result int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = q.Size()
	}
	_ = result
}

// ============================================================================
// PUSH OPERATION BENCHMARKS
// ============================================================================

// BenchmarkPushUnique measures cold path performance for new tick insertions.
func BenchmarkPushUnique(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	const testValue = uint64(0x123456789ABCDEF0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		tick := int64(i % BucketCount) // Keep within 0-127 range
		q.Push(tick, h, testValue)
	}
}

// BenchmarkPushUpdate measures hot path performance for tick updates.
func BenchmarkPushUpdate(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	const initValue = uint64(0x1111111111111111)
	const updateValue = uint64(0x2222222222222222)

	// Pre-populate with initial values (use modulo to stay in range)
	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		tick := int64(i % BucketCount)
		q.Push(tick, h, initValue)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		tick := int64((i % benchHandles) % BucketCount)
		q.Push(tick, h, updateValue)
	}
}

// BenchmarkPushSameTickZero measures performance for edge case tick value (0).
func BenchmarkPushSameTickZero(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	const testValue = uint64(0x3333333333333333)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(0, h, testValue)
	}
}

// BenchmarkPushSameTickMax measures performance for maximum tick value.
func BenchmarkPushSameTickMax(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	maxTick := int64(BucketCount - 1)
	const testValue = uint64(0x4444444444444444)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(maxTick, h, testValue)
	}
}

// BenchmarkPushRandom measures worst-case performance under chaotic tick patterns.
func BenchmarkPushRandom(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	const testValue = uint64(0x5555555555555555)

	// Pre-generate deterministic random sequence for reproducibility
	rand.Seed(42)
	ticks := make([]int64, benchHandles)
	for i := range ticks {
		ticks[i] = rand.Int63n(BucketCount)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		q.Push(ticks[i%benchHandles], h, testValue)
	}
}

// BenchmarkPushBursty measures coalesced operation patterns.
func BenchmarkPushBursty(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	const testValue = uint64(0x6666666666666666)

	rand.Seed(42)
	baseTick := int64(50) // Start in middle of range

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		var tick int64

		if rand.Float32() < 0.8 {
			// 80% within burst window
			tick = baseTick + rand.Int63n(16)
		} else {
			// 20% random scatter
			tick = rand.Int63n(BucketCount)
		}

		// Ensure tick is within bounds
		if tick >= BucketCount {
			tick = BucketCount - 1
		}

		q.Push(tick, h, testValue)

		// Shift burst window occasionally
		if i%1000 == 0 {
			baseTick = (baseTick + 20) % (BucketCount - 20)
		}
	}
}

// ============================================================================
// MINIMUM EXTRACTION BENCHMARKS
// ============================================================================

// BenchmarkPeepMin measures minimum finding performance via bitmap hierarchy.
func BenchmarkPeepMin(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Pre-populate queue
	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		tick := int64(i % BucketCount)
		q.Push(tick, h, uint64(i))
	}

	var h Handle
	var tick int64
	var data uint64
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h, tick, data = q.PeepMin()
	}
	_, _, _ = h, tick, data
}

// BenchmarkPeepMinSparse measures minimum finding in sparse queues.
func BenchmarkPeepMinSparse(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	const sparseCount = 50 // Reduced for 128-bucket queue

	for i := 0; i < sparseCount; i++ {
		h := Handle(i)
		// Spread entries across tick space for sparsity
		tick := int64(i * (BucketCount / sparseCount))
		q.Push(tick, h, uint64(i))
	}

	var h Handle
	var tick int64
	var data uint64
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h, tick, data = q.PeepMin()
	}
	_, _, _ = h, tick, data
}

// BenchmarkPeepMinDense measures minimum finding in dense queues.
func BenchmarkPeepMinDense(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		// Pack entries densely (90% of buckets)
		tick := int64(i % (BucketCount * 9 / 10))
		q.Push(tick, h, uint64(i))
	}

	var h Handle
	var tick int64
	var data uint64
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h, tick, data = q.PeepMin()
	}
	_, _, _ = h, tick, data
}

// ============================================================================
// UNLINK OPERATION BENCHMARKS
// ============================================================================

// BenchmarkUnlinkMin_StableBucket measures unlink performance without summary updates.
func BenchmarkUnlinkMin_StableBucket(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	const testValue = uint64(0x1234567890ABCDEF)
	q.Push(64, h, testValue) // Middle of range
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.UnlinkMin(h)
		q.Push(64, h, testValue)
	}
}

// BenchmarkUnlinkMin_DenseBucket measures unlink performance in populated buckets.
func BenchmarkUnlinkMin_DenseBucket(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	hs := [3]Handle{0, 1, 2}
	const testValue = uint64(0x7777777777777777)

	for i := 0; i < 3; i++ {
		q.Push(32, hs[i], testValue) // Middle of range
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := hs[i%3]
		q.UnlinkMin(h)
		q.Push(32, h, testValue)
	}
}

// BenchmarkUnlinkMin_BitmapCollapse measures summary update overhead.
func BenchmarkUnlinkMin_BitmapCollapse(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	const testValue = uint64(0x8888888888888888)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tick := int64(i % BucketCount)
		q.Push(tick, h, testValue)
		q.UnlinkMin(h)
	}
}

// BenchmarkUnlinkMin_ScatterCollapse measures random collapse performance.
func BenchmarkUnlinkMin_ScatterCollapse(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	h := Handle(0)
	const testValue = uint64(0x9999999999999999)

	// Pre-generate random tick sequence for reproducibility
	rand.Seed(42)
	ticks := make([]int64, b.N)
	for i := range ticks {
		ticks[i] = int64(rand.Intn(BucketCount))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tick := ticks[i]
		q.Push(tick, h, testValue)
		q.UnlinkMin(h)
	}
}

// ============================================================================
// TICK MOVEMENT BENCHMARKS
// ============================================================================

// BenchmarkMoveTick measures tick relocation performance.
func BenchmarkMoveTick(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Pre-populate with handles
	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		tick := int64(i % BucketCount)
		q.Push(tick, h, uint64(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		newTick := int64((i + 1) % BucketCount)
		q.MoveTick(h, newTick)
	}
}

// BenchmarkMoveTickNoop measures no-op movement optimization.
func BenchmarkMoveTickNoop(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Pre-populate with handles
	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		tick := int64(i % BucketCount)
		q.Push(tick, h, uint64(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		currentTick := int64((i % benchHandles) % BucketCount)
		q.MoveTick(h, currentTick) // Same tick - should be no-op
	}
}

// BenchmarkMoveTickRandom measures worst-case movement performance.
func BenchmarkMoveTickRandom(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	// Pre-populate with handles
	for i := 0; i < benchHandles; i++ {
		h := Handle(i)
		tick := int64(i % BucketCount)
		q.Push(tick, h, uint64(i))
	}

	// Pre-generate random destinations
	rand.Seed(42)
	destinations := make([]int64, benchHandles)
	for i := range destinations {
		destinations[i] = rand.Int63n(BucketCount)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		q.MoveTick(h, destinations[i%benchHandles])
	}
}

// ============================================================================
// SHARED POOL BENCHMARKS
// ============================================================================

// BenchmarkSharedPoolMultiQueue measures performance with multiple queues sharing a pool.
func BenchmarkSharedPoolMultiQueue(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q1 := New(unsafe.Pointer(&pool[0]))
	q2 := New(unsafe.Pointer(&pool[0]))
	q3 := New(unsafe.Pointer(&pool[0]))

	const testValue = uint64(0xAAAAAAAAAAAAAAAA)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Round-robin across queues with different handle ranges
		switch i % 3 {
		case 0:
			h := Handle(i%1000 + 1000) // Range 1000-1999
			tick := int64(i % BucketCount)
			q1.Push(tick, h, testValue)
		case 1:
			h := Handle(i%1000 + 2000) // Range 2000-2999
			tick := int64(i % BucketCount)
			q2.Push(tick, h, testValue)
		case 2:
			h := Handle(i%1000 + 3000) // Range 3000-3999
			tick := int64(i % BucketCount)
			q3.Push(tick, h, testValue)
		}
	}
}

// BenchmarkPoolUtilization measures memory pool utilization efficiency.
func BenchmarkPoolUtilization(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))

	const testValue = uint64(0xBBBBBBBBBBBBBBBB)

	// Use widely distributed handles across pool
	handles := make([]Handle, 1000)
	for i := range handles {
		handles[i] = Handle(i * (benchPoolSize / 1000))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := handles[i%len(handles)]
		tick := int64(i % BucketCount)
		q.Push(tick, h, testValue)
	}
}

// ============================================================================
// MIXED WORKLOAD BENCHMARKS
// ============================================================================

// BenchmarkMixedOperations measures realistic workload performance.
func BenchmarkMixedOperations(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	const testValue = uint64(0xCCCCCCCCCCCCCCCC)

	// Initialize some entries
	for i := 0; i < benchHandles/2; i++ {
		h := Handle(i)
		tick := int64(i % BucketCount)
		q.Push(tick, h, uint64(i))
	}

	rand.Seed(42)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		op := rand.Float32()

		switch {
		case op < 0.4: // 40% Push
			h := Handle(i % benchHandles)
			tick := rand.Int63n(BucketCount)
			q.Push(tick, h, testValue)

		case op < 0.7: // 30% PeepMin
			if !q.Empty() {
				_, _, _ = q.PeepMin()
			}

		case op < 0.9: // 20% UnlinkMin
			if !q.Empty() {
				h, _, _ := q.PeepMin()
				q.UnlinkMin(h)
			}

		default: // 10% MoveTick
			if !q.Empty() {
				h, _, _ := q.PeepMin()
				newTick := rand.Int63n(BucketCount)
				q.MoveTick(h, newTick)
			}
		}
	}
}

// BenchmarkHighContention measures performance under high contention scenarios.
func BenchmarkHighContention(b *testing.B) {
	pool := make([]Entry, benchPoolSize)
	InitializePool(pool)
	q := New(unsafe.Pointer(&pool[0]))
	const testValue = uint64(0xDDDDDDDDDDDDDDDD)

	// All operations within narrow range for maximum contention
	const contentionRange = 32 // Smaller range for 128-bucket queue

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := Handle(i % benchHandles)
		tick := int64(i % contentionRange) // Force contention
		q.Push(tick, h, testValue)
	}
}