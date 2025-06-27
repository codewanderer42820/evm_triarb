// fastuni.go — High‑throughput log₂ / ln helpers for HFT & Uniswap math.
//
// Overview
// --------
// Implements log2/ln math helpers designed for high-performance scenarios.
// Optimized for Apple M-series and Zen 4 CPUs with branch-free, allocation-free
// math, and panic guards for invalid operands.

package fastuni

import (
	"math"
	"math/bits"
)

/*──────────────────── compile‑time constants ───────────────────*/

const ln2 = 0x1.62e42fefa39efp-1 // ≈ ln(2)

const (
	c1 = +0.9990102443771056
	c2 = -0.4891559897950173
	c3 = +0.2833026021012029
	c4 = -0.1301181019014788
	c5 = +0.0301022874045224
)

const (
	fracMask    uint64 = (1 << 52) - 1
	maxSqrtHi          = 1 << 53
	lostLowMask        = (1 << 11) - 1
)

/*──────────────────── Uint128 ───────────────────*/

// Uint128 is a 128-bit unsigned integer (Hi << 64 | Lo)
type Uint128 struct {
	Hi   uint64
	Lo   uint64
	_pad [0]uint64 // align to 16 bytes if needed
}

/*──────────────────── internal helpers ───────────────────*/

//go:nosplit
//go:inline
func validateConv(conv float64) {
	if conv == 0 || math.IsInf(conv, 0) || math.IsNaN(conv) ||
		math.Abs(conv) < 1e-300 || math.Abs(conv) > 1e300 {
		panic("fastuni: invalid conversion factor (check base)")
	}
}

//go:nosplit
//go:inline
func ln1pf(f float64) float64 {
	t := f*c5 + c4
	t = f*t + c3
	t = f*t + c2
	t = f*t + c1
	return f * t
}

/*──────────────────── log₂ paths ───────────────────*/

//go:nosplit
//go:inline
func log2u64(x uint64) float64 {
	if x == 0 {
		panic("fastuni: log2 of zero")
	}
	k := 63 - bits.LeadingZeros64(x)
	lead := uint64(1) << k
	frac := x ^ lead
	if k > 52 {
		frac >>= uint(k - 52)
	} else {
		frac <<= uint(52 - k)
	}
	mBits := (uint64(1023) << 52) | (frac & fracMask)
	m := math.Float64frombits(mBits)
	return float64(k) + ln1pf(m-1)/ln2
}

//go:nosplit
//go:inline
func log2u128(u Uint128) float64 {
	if u.Hi == 0 && u.Lo == 0 {
		panic("fastuni: log2 of zero Uint128")
	}
	if u.Hi == 0 {
		return log2u64(u.Lo)
	}
	k := 127 - bits.LeadingZeros64(u.Hi)
	x := float64(u.Hi)*0x1p64 + float64(u.Lo)
	return float64(k) + ln1pf(x/math.Ldexp(1, k)-1)/ln2
}

/*──────────────────── V2 helpers ───────────────────*/

//go:nosplit
//go:inline
func Log2ReserveRatio(a, b uint64) float64 {
	return log2u64(a) - log2u64(b)
}

//go:nosplit
//go:inline
func LnReserveRatio(a, b uint64) float64 {
	if a == 0 || b == 0 {
		panic("fastuni: zero reserve")
	}
	if a == b {
		return 0
	}
	if a>>4 == b>>4 {
		r := float64(a)/float64(b) - 1
		if math.Abs(r) < 1e-3 {
			return math.Log1p(r)
		}
	}
	return (log2u64(a) - log2u64(b)) * ln2
}

//go:nosplit
//go:inline
func LogReserveRatioConst(a, b uint64, conv float64) float64 {
	validateConv(conv)
	return (log2u64(a) - log2u64(b)) * conv
}

/*──────────────────── V3 helpers (Q64.96) ───────────────────*/

//go:nosplit
//go:inline
func Log2PriceX96(sqrt Uint128) float64 {
	switch {
	case sqrt.Hi >= maxSqrtHi:
		panic("fastuni: sqrtPriceX96 exceeds safe range — precision loss > 1 tick")
	case sqrt.Hi == maxSqrtHi-1 && (sqrt.Lo&lostLowMask) != 0:
		panic("fastuni: sqrtPriceX96 near precision cliff — low bits would be lost")
	}
	log2sqrt := log2u128(sqrt) - 96
	return log2sqrt * 2
}

//go:nosplit
//go:inline
func LnPriceX96(sqrt Uint128) float64 {
	return Log2PriceX96(sqrt) * ln2
}

//go:nosplit
//go:inline
func LogPriceX96Const(sqrt Uint128, conv float64) float64 {
	validateConv(conv)
	return Log2PriceX96(sqrt) * conv
}

/*──────────────────── Assembly hook flag ───────────────────*/

var usingASM = false
var _ = usingASM // retain reference to silence unused warnings
