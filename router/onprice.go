package router

import (
	"unsafe"
)

// onPrice handles one PriceUpdate on its pinned core.
//  1. pick polarity            (fwd / rev tick)
//  2. write tick to SoA slice   (b.t0/t1/t2)
//  3. mirror tick into ArbPath  (p.Ticks[Edge])
//  4. tri-sum via SoA           (fast contiguous loads)
//  5. queue.Update( bucket, p ) (pointer to shared path)
//
//go:nosplit
func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.IsReverse {
		tick = upd.RevTick
	}

	lid, _ := rt.Local.Get(uint32(upd.Pair)) // always present
	b := &rt.Buckets[lid]
	fan := rt.Fanouts[lid] // installed at bootstrap

	legs := [...]*[]float64{&b.t0, &b.t1, &b.t2} // branch-free slice table

	for _, f := range fan {
		idx := int(f.Idx)

		(*legs[f.Edge])[idx] = tick // SoA write
		p := f.Path
		p.Ticks[f.Edge] = tick // keep ArbPath current

		sum := b.t0[idx] + b.t1[idx] + b.t2[idx]
		f.Queue.Update(l2Bucket(sum), 0, unsafe.Pointer(p))
	}
}

// l2Bucket maps a log₂ sum in ±128 into 4 096 histogram slots.
//
//	clamp = 128   (covers two full Uniswap V2 pool swings)
//	scale = 16    (4096 / (2*clamp))
//
//go:nosplit
func l2Bucket(x float64) int64 {
	const (
		clamp = 128.0
		scale = 16.0
	)
	if x > clamp {
		x = clamp
	} else if x < -clamp {
		x = -clamp
	}
	return int64((x + clamp) * scale) // (x+128)*16  →  0‥4095
}
