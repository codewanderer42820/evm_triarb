// ============================================================================
// SPSC RING BUFFER CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive unit testing framework for lock-free SPSC ring buffer
// with emphasis on ISR-grade reliability and edge case coverage.
//
// Test categories:
//   - Constructor validation: Power-of-2 sizing and initialization
//   - Basic operations: Push/Pop semantics and data integrity
//   - Capacity management: Full/empty state handling and overflow behavior
//   - Wraparound logic: Circular buffer pointer arithmetic validation
//   - Edge cases: Boundary conditions, reuse safety, and idle behavior
//   - Memory safety: Pointer validity and lifetime management
//   - Sequence integrity: Ordering guarantees and consistency checks
//
// Validation methodology:
//   - Single-threaded operation validation (SPSC discipline)
//   - Data integrity verification across operation cycles
//   - Capacity boundary testing with overflow scenarios
//   - Pointer lifetime and reuse safety validation
//   - Sequence number consistency verification
//   - Memory access pattern validation
//
// Performance assumptions:
//   - Sub-10ns operation latency for Push/Pop operations
//   - Zero allocation during steady-state operation
//   - Predictable behavior under varying load conditions
//   - Cache-friendly access patterns for sequential operations

package ring24

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// ============================================================================
// TEST UTILITIES AND HELPERS
// ============================================================================

// testData generates deterministic test payloads for validation
func testData(seed byte) *[24]byte {
	data := &[24]byte{}
	for i := range data {
		data[i] = seed + byte(i)
	}
	return data
}

// randomData generates cryptographically random test payloads
func randomData() *[24]byte {
	data := &[24]byte{}
	rand.Read(data[:])
	return data
}

// validateData ensures payload integrity across operations
func validateData(t *testing.T, got, want *[24]byte, context string) {
	t.Helper()
	if got == nil && want != nil {
		t.Fatalf("%s: got nil, want %v", context, want)
	}
	if got != nil && want == nil {
		t.Fatalf("%s: got %v, want nil", context, got)
	}
	if got != nil && want != nil && *got != *want {
		t.Fatalf("%s: got %v, want %v", context, got, want)
	}
}

// memoryBarrier ensures memory ordering for test synchronization
func memoryBarrier() {
	runtime.Gosched()
	atomic.LoadUint64(new(uint64))
}

// ============================================================================
// CONSTRUCTOR VALIDATION
// ============================================================================

// TestNewValidSizes validates constructor with valid power-of-2 sizes
func TestNewValidSizes(t *testing.T) {
	validSizes := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}

	for _, size := range validSizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			r := New(size)
			if r == nil {
				t.Fatalf("New(%d) returned nil", size)
			}

			// Verify internal structure
			if r.mask != uint64(size-1) {
				t.Errorf("mask = %d, want %d", r.mask, size-1)
			}
			if r.step != uint64(size) {
				t.Errorf("step = %d, want %d", r.step, size)
			}
			if len(r.buf) != size {
				t.Errorf("buffer length = %d, want %d", len(r.buf), size)
			}

			// Verify sequence initialization
			for i := 0; i < size; i++ {
				if r.buf[i].seq != uint64(i) {
					t.Errorf("buf[%d].seq = %d, want %d", i, r.buf[i].seq, i)
				}
			}
		})
	}
}

// TestNewPanicsOnInvalidSize validates constructor input validation
func TestNewPanicsOnInvalidSize(t *testing.T) {
	invalidSizes := []int{
		0,     // Zero size
		-1,    // Negative size
		3,     // Small non-power-of-2
		5,     // Another small non-power-of-2
		7,     // Another small non-power-of-2
		9,     // Just above power-of-2
		15,    // Just below power-of-2
		1000,  // Large non-power-of-2
		1023,  // Just below large power-of-2
		1025,  // Just above large power-of-2
		65535, // Large odd number
	}

	for _, size := range invalidSizes {
		t.Run(fmt.Sprintf("invalid_size_%d", size), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("New(%d) should panic on invalid size", size)
				}
			}()
			_ = New(size)
		})
	}
}

// ============================================================================
// BASIC OPERATION VALIDATION
// ============================================================================

// TestPushPopRoundTrip validates fundamental Push/Pop semantics
func TestPushPopRoundTrip(t *testing.T) {
	sizes := []int{2, 4, 8, 16, 32, 64, 128}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			r := New(size)
			want := testData(42)

			// Test successful push to empty ring
			if !r.Push(want) {
				t.Fatal("Push should succeed on empty ring")
			}

			// Test pop and data integrity
			got := r.Pop()
			validateData(t, got, want, "round trip")

			// Test empty state after pop
			if r.Pop() != nil {
				t.Fatal("ring should be empty after single push/pop cycle")
			}
		})
	}
}

// TestPushFailsWhenFull validates capacity enforcement
func TestPushFailsWhenFull(t *testing.T) {
	sizes := []int{2, 4, 8, 16}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			r := New(size)
			val := testData(7)

			// Fill to exact capacity
			for i := 0; i < size; i++ {
				if !r.Push(val) {
					t.Fatalf("push %d unexpectedly failed before capacity reached", i)
				}
			}

			// Test overflow rejection
			if r.Push(val) {
				t.Fatal("push into full ring should return false")
			}

			// Verify all pushes after full should fail
			for i := 0; i < 10; i++ {
				if r.Push(val) {
					t.Fatalf("push %d after full should fail", i)
				}
			}
		})
	}
}

// TestPopNilWhenEmpty validates empty ring behavior
func TestPopNilWhenEmpty(t *testing.T) {
	sizes := []int{2, 4, 8, 16}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			r := New(size)

			// Test pop on fresh empty ring
			if r.Pop() != nil {
				t.Fatal("Pop on empty ring should return nil")
			}

			// Test multiple pops on empty ring
			for i := 0; i < 10; i++ {
				if r.Pop() != nil {
					t.Fatalf("Pop %d on empty ring should return nil", i)
				}
			}
		})
	}
}

// TestPopWaitBlocksUntilItem validates blocking consumption behavior
func TestPopWaitBlocksUntilItem(t *testing.T) {
	r := New(4)
	want := testData(42)
	done := make(chan struct{})
	var got *[24]byte

	// Consumer goroutine
	go func() {
		got = r.PopWait()
		close(done)
	}()

	// Ensure consumer is waiting
	time.Sleep(10 * time.Millisecond)

	// Producer with delay
	if !r.Push(want) {
		t.Fatal("Push failed")
	}

	// Wait for consumer completion
	select {
	case <-done:
		validateData(t, got, want, "PopWait")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("PopWait did not complete within timeout")
	}
}

// ============================================================================
// CAPACITY AND STATE MANAGEMENT
// ============================================================================

// TestCapacityBoundaries validates exact capacity limits
func TestCapacityBoundaries(t *testing.T) {
	sizes := []int{2, 4, 8, 16, 32}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			r := New(size)
			val := testData(99)

			// Fill to exact capacity
			for i := 0; i < size; i++ {
				if !r.Push(val) {
					t.Fatalf("push %d failed before reaching capacity %d", i, size)
				}
			}

			// Verify ring is full
			if r.Push(val) {
				t.Fatal("push should fail when ring is full")
			}

			// Empty the ring
			for i := 0; i < size; i++ {
				got := r.Pop()
				if got == nil {
					t.Fatalf("pop %d failed when ring should have data", i)
				}
				validateData(t, got, val, fmt.Sprintf("pop %d", i))
			}

			// Verify ring is empty
			if r.Pop() != nil {
				t.Fatal("pop should return nil when ring is empty")
			}
		})
	}
}

// TestPartialFillOperations validates operations at various fill levels
func TestPartialFillOperations(t *testing.T) {
	r := New(8)

	// Test operations at different fill levels
	fillLevels := []int{1, 2, 3, 4, 5, 6, 7}

	for _, level := range fillLevels {
		t.Run(fmt.Sprintf("fill_level_%d", level), func(t *testing.T) {
			// Clear ring
			for r.Pop() != nil {
			}

			// Fill to specified level
			for i := 0; i < level; i++ {
				val := testData(byte(i))
				if !r.Push(val) {
					t.Fatalf("push %d failed at fill level %d", i, level)
				}
			}

			// Verify can still push until full
			for i := level; i < 8; i++ {
				val := testData(byte(i))
				if !r.Push(val) {
					t.Fatalf("push %d failed when ring should accept more", i)
				}
			}

			// Verify overflow rejection
			if r.Push(testData(255)) {
				t.Fatal("push should fail when ring is full")
			}
		})
	}
}

// ============================================================================
// WRAPAROUND AND CIRCULAR BUFFER VALIDATION
// ============================================================================

// TestWrapAroundOperations validates circular buffer mechanics
func TestWrapAroundOperations(t *testing.T) {
	sizes := []int{4, 8, 16}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			r := New(size)

			// Perform multiple complete cycles
			cycles := 5
			for cycle := 0; cycle < cycles; cycle++ {
				for i := 0; i < size*2; i++ { // 2x size to ensure wraparound
					val := testData(byte(cycle*100 + i))

					if !r.Push(val) {
						t.Fatalf("push failed at cycle %d, iteration %d", cycle, i)
					}

					got := r.Pop()
					validateData(t, got, val, fmt.Sprintf("cycle %d, iteration %d", cycle, i))
				}
			}
		})
	}
}

// TestSequenceNumberConsistency validates sequence number behavior across wraparound
func TestSequenceNumberConsistency(t *testing.T) {
	r := New(4)
	val := testData(42)

	// Track sequence numbers through operations
	initialSeqs := make([]uint64, 4)
	for i := 0; i < 4; i++ {
		initialSeqs[i] = r.buf[i].seq
	}

	// Perform operations that cause multiple wraparounds
	for i := 0; i < 20; i++ {
		if !r.Push(val) {
			t.Fatalf("push %d failed", i)
		}
		if r.Pop() == nil {
			t.Fatalf("pop %d failed", i)
		}
	}

	// Verify sequence numbers have advanced correctly
	for i := 0; i < 4; i++ {
		expected := initialSeqs[i] + 20 // Each slot used 5 times (20/4)
		if r.buf[i].seq != expected {
			t.Errorf("slot %d: seq = %d, want %d", i, r.buf[i].seq, expected)
		}
	}
}

// ============================================================================
// INTERLEAVED OPERATION TESTING
// ============================================================================

// TestPushPopInterleaved validates safety under alternating operations
func TestPushPopInterleaved(t *testing.T) {
	sizes := []int{4, 8, 16, 32}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			r := New(size)

			// Extended interleaved sequence
			for i := 0; i < size*10; i++ {
				val := testData(byte(i))

				if !r.Push(val) {
					t.Fatalf("push %d failed during interleaved test", i)
				}

				got := r.Pop()
				validateData(t, got, val, fmt.Sprintf("interleaved iteration %d", i))
			}
		})
	}
}

// TestBatchOperations validates batch push/pop patterns
func TestBatchOperations(t *testing.T) {
	r := New(8)
	batchSizes := []int{1, 2, 3, 4, 5}

	for _, batchSize := range batchSizes {
		t.Run(fmt.Sprintf("batch_size_%d", batchSize), func(t *testing.T) {
			// Clear ring
			for r.Pop() != nil {
			}

			// Perform batch operations
			for batch := 0; batch < 5; batch++ {
				// Push batch
				vals := make([]*[24]byte, batchSize)
				for i := 0; i < batchSize; i++ {
					vals[i] = testData(byte(batch*10 + i))
					if !r.Push(vals[i]) {
						t.Fatalf("batch %d, push %d failed", batch, i)
					}
				}

				// Pop batch
				for i := 0; i < batchSize; i++ {
					got := r.Pop()
					validateData(t, got, vals[i], fmt.Sprintf("batch %d, pop %d", batch, i))
				}
			}
		})
	}
}

// ============================================================================
// MEMORY SAFETY AND POINTER VALIDATION
// ============================================================================

// TestPointerLifetime validates pointer safety after operations
func TestPointerLifetime(t *testing.T) {
	r := New(4)

	// Collect pointers from multiple operations
	var pointers []*[24]byte

	for i := 0; i < 20; i++ {
		val := testData(byte(i))
		if !r.Push(val) {
			t.Fatalf("push %d failed", i)
		}

		ptr := r.Pop()
		if ptr == nil {
			t.Fatalf("pop %d returned nil", i)
		}

		// Store pointer for later validation
		pointers = append(pointers, ptr)

		// Create copy to validate data persistence
		copy := *ptr

		// Trigger potential slot reuse
		dummy := testData(255)
		r.Push(dummy)
		r.Pop()

		// Verify original data hasn't been corrupted
		validateData(t, &copy, testData(byte(i)), fmt.Sprintf("pointer lifetime %d", i))
	}

	// Validate all pointers are within buffer bounds
	bufStart := uintptr(unsafe.Pointer(&r.buf[0]))
	bufEnd := bufStart + uintptr(len(r.buf))*unsafe.Sizeof(r.buf[0])

	for i, ptr := range pointers {
		ptrAddr := uintptr(unsafe.Pointer(ptr))
		if ptrAddr < bufStart || ptrAddr >= bufEnd {
			t.Errorf("pointer %d (%p) outside buffer bounds [%p, %p)", i, ptr, unsafe.Pointer(&r.buf[0]), unsafe.Pointer(&r.buf[len(r.buf)]))
		}
	}
}

// TestSlotReuse validates proper slot reuse behavior
func TestSlotReuse(t *testing.T) {
	r := New(2) // Small ring to force frequent reuse

	// Track which slots are being used
	usedSlots := make(map[uintptr]int)

	for i := 0; i < 20; i++ {
		val := testData(byte(i))
		if !r.Push(val) {
			t.Fatalf("push %d failed", i)
		}

		ptr := r.Pop()
		if ptr == nil {
			t.Fatalf("pop %d returned nil", i)
		}

		addr := uintptr(unsafe.Pointer(ptr))
		usedSlots[addr]++

		validateData(t, ptr, val, fmt.Sprintf("slot reuse %d", i))
	}

	// Verify slots were reused multiple times
	if len(usedSlots) != 2 {
		t.Errorf("expected 2 unique slot addresses, got %d", len(usedSlots))
	}

	for addr, count := range usedSlots {
		if count != 10 {
			t.Errorf("slot %p used %d times, expected 10", unsafe.Pointer(addr), count)
		}
	}
}

// ============================================================================
// EDGE CASES AND ERROR CONDITIONS
// ============================================================================

// TestEmptyRingOperations validates repeated operations on empty ring
func TestEmptyRingOperations(t *testing.T) {
	r := New(4)

	// Multiple consecutive pops on empty ring
	for i := 0; i < 20; i++ {
		if got := r.Pop(); got != nil {
			t.Fatalf("pop %d on empty ring returned %v, want nil", i, got)
		}
	}

	// Ring should still function normally after empty operations
	val := testData(42)
	if !r.Push(val) {
		t.Fatal("push failed after empty operations")
	}

	got := r.Pop()
	validateData(t, got, val, "after empty operations")
}

// TestFullRingOperations validates repeated operations on full ring
func TestFullRingOperations(t *testing.T) {
	r := New(4)
	val := testData(99)

	// Fill ring
	for i := 0; i < 4; i++ {
		if !r.Push(val) {
			t.Fatalf("initial push %d failed", i)
		}
	}

	// Multiple consecutive pushes on full ring
	for i := 0; i < 20; i++ {
		if r.Push(val) {
			t.Fatalf("push %d on full ring should fail", i)
		}
	}

	// Ring should still function normally after full operations
	got := r.Pop()
	validateData(t, got, val, "after full operations")

	if !r.Push(val) {
		t.Fatal("push failed after full operations")
	}
}

// TestDataIntegrityUnderStress validates data integrity under rapid operations
func TestDataIntegrityUnderStress(t *testing.T) {
	r := New(16)

	// Use unique data for each operation
	dataSet := make(map[[24]byte]int)

	// Rapid push/pop cycles with unique data
	for i := 0; i < 1000; i++ {
		// Generate unique data
		val := randomData()
		dataSet[*val] = i

		if !r.Push(val) {
			t.Fatalf("push %d failed", i)
		}

		got := r.Pop()
		if got == nil {
			t.Fatalf("pop %d returned nil", i)
		}

		// Verify data matches what we pushed
		if _, exists := dataSet[*got]; !exists {
			t.Fatalf("pop %d returned unknown data %v", i, got)
		}

		// Verify it's the exact data we just pushed
		validateData(t, got, val, fmt.Sprintf("stress test %d", i))
	}
}

// TestLongIdlePeriods validates behavior after extended idle periods
func TestLongIdlePeriods(t *testing.T) {
	r := New(4)

	// Initial operation
	val1 := testData(1)
	if !r.Push(val1) {
		t.Fatal("initial push failed")
	}
	got1 := r.Pop()
	validateData(t, got1, val1, "before idle")

	// Simulate long idle period
	time.Sleep(100 * time.Millisecond)

	// Resume operations
	val2 := testData(2)
	if !r.Push(val2) {
		t.Fatal("push after idle failed")
	}
	got2 := r.Pop()
	validateData(t, got2, val2, "after idle")

	// Verify normal operation continues
	for i := 0; i < 10; i++ {
		val := testData(byte(i + 10))
		if !r.Push(val) {
			t.Fatalf("post-idle push %d failed", i)
		}
		got := r.Pop()
		validateData(t, got, val, fmt.Sprintf("post-idle %d", i))
	}
}

// ============================================================================
// ALIGNMENT AND CACHE LINE VALIDATION
// ============================================================================

// TestStructAlignment validates memory alignment for cache optimization
func TestStructAlignment(t *testing.T) {
	r := New(4)

	// Verify Ring struct alignment
	ringAddr := uintptr(unsafe.Pointer(r))
	if ringAddr%64 != 0 {
		t.Errorf("Ring not aligned to 64-byte boundary: %p", r)
	}

	// Verify head/tail separation (different cache lines)
	headAddr := uintptr(unsafe.Pointer(&r.head))
	tailAddr := uintptr(unsafe.Pointer(&r.tail))

	headCacheLine := headAddr / 64
	tailCacheLine := tailAddr / 64

	if headCacheLine == tailCacheLine {
		t.Error("head and tail are on the same cache line")
	}

	// Verify slot alignment
	for i := range r.buf {
		slotAddr := uintptr(unsafe.Pointer(&r.buf[i]))
		if slotAddr%64 != 0 {
			t.Errorf("slot %d not aligned to 64-byte boundary: %p", i, &r.buf[i])
		}
	}
}

// TestMemoryLayout validates expected memory layout
func TestMemoryLayout(t *testing.T) {
	r := New(4)

	// Verify field offsets match expected layout
	ringAddr := uintptr(unsafe.Pointer(r))
	headAddr := uintptr(unsafe.Pointer(&r.head))
	tailAddr := uintptr(unsafe.Pointer(&r.tail))
	maskAddr := uintptr(unsafe.Pointer(&r.mask))

	// Head should be at offset 64 (after padding)
	if headAddr-ringAddr != 64 {
		t.Errorf("head offset = %d, want 64", headAddr-ringAddr)
	}

	// Tail should be at offset 128 (head + 56 bytes padding)
	if tailAddr-ringAddr != 128 {
		t.Errorf("tail offset = %d, want 128", tailAddr-ringAddr)
	}

	// Verify sufficient spacing between critical fields
	if tailAddr-headAddr < 64 {
		t.Error("insufficient spacing between head and tail")
	}
}

// ============================================================================
// RACE CONDITION DETECTION (Single-threaded validation)
// ============================================================================

// TestSequentialConsistency validates operation ordering guarantees
func TestSequentialConsistency(t *testing.T) {
	r := New(8)

	// Push sequence of identifiable data
	sequence := make([]*[24]byte, 20)
	for i := 0; i < 20; i++ {
		sequence[i] = testData(byte(i))
		if !r.Push(sequence[i]) {
			t.Fatalf("push %d failed", i)
		}

		// Immediately pop to test ordering
		got := r.Pop()
		validateData(t, got, sequence[i], fmt.Sprintf("sequential consistency %d", i))
	}
}

// TestNoUnexpectedNils validates that operations never return unexpected nils
func TestNoUnexpectedNils(t *testing.T) {
	r := New(4)

	// Continuous operation cycle
	for cycle := 0; cycle < 100; cycle++ {
		// Fill ring
		for i := 0; i < 4; i++ {
			val := testData(byte(cycle*10 + i))
			if !r.Push(val) {
				t.Fatalf("cycle %d, push %d failed unexpectedly", cycle, i)
			}
		}

		// Empty ring
		for i := 0; i < 4; i++ {
			got := r.Pop()
			if got == nil {
				t.Fatalf("cycle %d, pop %d returned unexpected nil", cycle, i)
			}
		}

		// Verify empty
		if r.Pop() != nil {
			t.Fatalf("cycle %d: ring not empty after full cycle", cycle)
		}
	}
}
