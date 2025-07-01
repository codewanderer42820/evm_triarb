// ─────────────────────────────────────────────────────────────────────────────
// queue_stress_test.go — QuantumQueue vs Heap Reference Fuzz Test
//
// Purpose:
//   - Stress-test QuantumQueue against a reference Go heap
//   - Apply millions of randomized operations: push, move, pop
//
// Guarantees:
//   - Order correctness (tick and LIFO priority)
//   - Tick relocation consistency
//   - Arena reuse and freelist accuracy
//
// Notes:
//   - Uses deterministic seed for reproducibility
//   - Models CapItems handle limits and safety boundaries
//   - All corruption, misordering, or ghost state leads to test failure
//
// ─────────────────────────────────────────────────────────────────────────────

package quantumqueue

import (
	"container/heap"
	"math/rand"
	"testing"
)

/*─────────────────────────────────────────────────────────────────────────────*
 * Reference Model: Heap-backed Scheduler                                      *
 *─────────────────────────────────────────────────────────────────────────────*/

// stressItem mimics a QuantumQueue node in the reference heap.
type stressItem struct {
	h    Handle // handle into QuantumQueue arena
	tick int64  // priority key
	seq  int    // LIFO tiebreaker (higher = newer)
}

// stressHeap implements heap.Interface with LIFO tiebreaks.
type stressHeap []*stressItem

func (h stressHeap) Len() int { return len(h) }
func (h stressHeap) Less(i, j int) bool {
	if h[i].tick != h[j].tick {
		return h[i].tick < h[j].tick
	}
	return h[i].seq > h[j].seq // newer wins
}
func (h stressHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *stressHeap) Push(x interface{}) { *h = append(*h, x.(*stressItem)) }
func (h *stressHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	it := old[n]
	*h = old[:n]
	return it
}

/*─────────────────────────────────────────────────────────────────────────────*
 * Core Fuzz: Random Push / Move / Pop Loop                                    *
 *─────────────────────────────────────────────────────────────────────────────*/

// TestQueueStressRandomOperations validates QuantumQueue under chaotic ISR loads.
func TestQueueStressRandomOperations(t *testing.T) {
	const iterations = 10_000_000

	rng := rand.New(rand.NewSource(69)) // deterministic seed

	q := NewQuantumQueue() // test subject
	ref := &stressHeap{}   // reference model
	heap.Init(ref)

	// Track available + live handles
	free := make([]Handle, CapItems)
	for i := range free {
		free[i] = Handle(i)
	}
	live := make(map[Handle]bool)

	seq := 0 // global tiebreak counter

	// makeVal generates deterministic [48]byte values per seed.
	makeVal := func(seed int64) *[48]byte {
		var b [48]byte
		for i := range b {
			b[i] = byte((seed + int64(i)) & 0xFF)
		}
		return &b
	}

	// ─────────────────────────────────────────────────────────────────────────
	// Main Fuzz Loop: Random Push / MoveTick / Peep+Unlink
	// ─────────────────────────────────────────────────────────────────────────
	for i := 0; i < iterations; i++ {
		op := rng.Intn(3)
		tick := int64(rng.Intn(BucketCount))

		switch op {

		// ---------------------
		// PUSH (new handle)
		// ---------------------
		case 0:
			if len(free) == 0 {
				continue
			}
			h := free[len(free)-1]
			free = free[:len(free)-1]
			val := makeVal(int64(seq))
			q.Push(tick, h, val)
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			live[h] = true
			seq++

		// ---------------------
		// MOVE (tick relocation)
		// ---------------------
		case 1:
			if len(live) == 0 {
				continue
			}
			var h Handle
			for k := range live {
				h = k
				break
			}
			q.MoveTick(h, tick)

			// Remove stale entries in heap
			for j := len(*ref) - 1; j >= 0; j-- {
				if (*ref)[j].h == h {
					heap.Remove(ref, j)
				}
			}
			heap.Push(ref, &stressItem{h: h, tick: tick, seq: seq})
			seq++

		// ---------------------
		// POP (Peep + UnlinkMin)
		// ---------------------
		case 2:
			if q.Empty() {
				continue
			}
			h, tickGot, _ := q.PeepMin()
			exp := heap.Pop(ref).(*stressItem)
			if exp.h != h || exp.tick != tickGot {
				t.Fatalf("Mismatch at iter %d: got (h=%v,t=%d); want (h=%v,t=%d)",
					i, h, tickGot, exp.h, exp.tick)
			}
			q.UnlinkMin(h, tickGot)
			delete(live, h)
			free = append(free, h)
		}
	}

	// ─────────────────────────────────────────────────────────────────────────
	// Drain Verification: Remaining entries must match reference
	// ─────────────────────────────────────────────────────────────────────────
	for !q.Empty() {
		h, tickGot, _ := q.PeepMin()
		exp := heap.Pop(ref).(*stressItem)
		if exp.h != h || exp.tick != tickGot {
			t.Fatalf("Drain mismatch: got (h=%v,t=%d); want (%v,%d)",
				h, tickGot, exp.h, exp.tick)
		}
		q.UnlinkMin(h, tickGot)
		delete(live, h)
		free = append(free, h)
	}

	// Final check: heap must also be empty
	if ref.Len() != 0 {
		t.Fatalf("Reference heap not empty after drain: %d items left", ref.Len())
	}
}
