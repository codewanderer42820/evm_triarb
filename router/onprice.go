package router

import (
	"unsafe"
)

// onPrice: write anchor tick, update leg tick in ArbPath, bucketise, fire path.
func onPrice(rt *CoreRouter, upd *PriceUpdate) {
	tick := upd.FwdTick
	if rt.IsReverse {
		tick = upd.RevTick
	}

	lid, _ := rt.Local.Get(uint32(upd.Pair))
	b := &rt.Buckets[lid]
	fan := rt.Fanouts[lid]

	// anchor-pair absolute tick
	b.CommonTick = tick

	for _, f := range fan {
		p := f.Path
		p.Ticks[f.Edge] = tick // store real tick for this leg

		sum := b.CommonTick + p.Ticks[0] + p.Ticks[1] + p.Ticks[2]

		f.Queue.Update(l2Bucket(sum), 0, unsafe.Pointer(p))

		// after each update: if best bucket negative, pop & fire
		if bucket, ptr, ok := f.Queue.PeekMin(); ok && bucket < 2048 {
			f.Queue.PopMin()
			onProfitablePath((*ArbPath)(ptr), bucket)
		}
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
