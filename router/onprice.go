package router

import (
	"unsafe"
)

// onPrice: write anchor tick, update leg tick in ArbPath, bucketise, fire path.
func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	// 1. polarity
	tick := upd.FwdTick
	if rt.IsReverse {
		tick = upd.RevTick
	}

	// 2. per-pair state
	lid, _ := rt.Local.Get(uint32(upd.Pair)) // guaranteed hit
	b := &rt.Buckets[lid]
	fan := rt.Fanouts[lid] // guaranteed non-empty

	// 3. peek cheapest path *before* mutation; fire if profitable (<0)
	if bucket, ptr, ok := b.PeekMin(); ok {
		p := (*ArbPath)(ptr)
		profit := tick + p.Ticks[0] + p.Ticks[1] + p.Ticks[2]
		if profit < 0 {
			onProfitablePath(p, bucket) // do NOT pop
		}
	}

	// 4. update every attached path & bucket
	for _, f := range fan {
		p := f.Path
		p.Ticks[f.Edge] = tick // store real tick for this leg

		sum := p.Ticks[0] + p.Ticks[1] + p.Ticks[2]
		f.Update(l2Bucket(sum), 0, unsafe.Pointer(p))
	}
}

// ±128 log₂  →  0‥4095 bucket range.  <2048 == negative profitability.
func l2Bucket(x float64) int64 {
	const clamp, scale = 128.0, 16.0 // scale = 4096/(2*clamp)
	if x > clamp {
		x = clamp
	} else if x < -clamp {
		x = -clamp
	}
	return int64((x + clamp) * scale)
}
