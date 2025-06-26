package router

import "main/bucketqueue"

// tickSoA owns three parallel slices.  Index “idx” is shared across them.
// Each slice length == capacity, so bounds checks vanish after inlining.
type tickSoA struct {
	Queue bucketqueue.Queue // SPSC queue (bucket ring) for this pair
	t0    []float64         // ticks where pair is Edge 0
	t1    []float64         // ticks where pair is Edge 1
	t2    []float64         // ticks where pair is Edge 2
}

// ensureCap grows *length* and capacity so writes are always in-bounds.
func (b *tickSoA) ensureCap(n int) {
	if len(b.t0) >= n {
		return
	}
	grow := n - len(b.t0)
	b.t0 = append(b.t0, make([]float64, grow)...)
	b.t1 = append(b.t1, make([]float64, grow)...)
	b.t2 = append(b.t2, make([]float64, grow)...)
}
