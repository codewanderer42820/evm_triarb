// ticksoa.go — per-pair storage with SoA floats.
//
// One TickBucket (alias) lives in CoreRouter.Buckets.  Each bucket owns
// its queue *value* plus three parallel slices holding the ticks for the
// three legs of all Fanouts that reference this pair.

package router

import (
	"main/bucketqueue"
)

type tickSoA struct {
	Queue bucketqueue.Queue // owned SPSC queue
	t0    []float64         // leg-0 ticks  (index == Fanout.Idx)
	t1    []float64         // leg-1 ticks
	t2    []float64         // leg-2 ticks
}

func (b *tickSoA) ensureCap(n int) {
	if cap(b.t0) >= n {
		return
	}
	grow := n - cap(b.t0)
	b.t0 = append(b.t0, make([]float64, grow)...)
	b.t1 = append(b.t1, make([]float64, grow)...)
	b.t2 = append(b.t2, make([]float64, grow)...)
}
