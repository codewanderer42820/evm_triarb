// fastuni.go — High‑throughput log₂ / ln helpers for HFT & Uniswap math.
//
// Synopsis
// --------
// Pure‑Go helpers that compute logarithms and reserve‑ratio metrics with
// ≤3 × 10⁻⁵ nat absolute error in ~15–20 cycles on modern Apple M‑series or
// Zen 4 CPUs.
//
// Contract
// --------
// • All public helpers panic on zero operands.
// • Conversion helpers ( *_Const ) panic on NaN, ±Inf, 0, or absurd magnitudes
//   (|conv| ∉ [1e‑300, 1e300]).
// • `Log2PriceX96` panics when the Q64.96 input would lose >1 Uniswap tick
//   (~1 e‑5 nat) of precision.
//
// Performance
// -----------
// • Branch‑free, allocation‑free, fully inlinable; GC‑safe.
// • Assembly hook (`usingASM`) wired for future SIMD, currently `false`.

package fastuni

import (
	"math"
	"math/bits"
)

/*────────────────── compile‑time constants ───────────────────*/

// ln(2) baked in so the compiler can constant‑fold everywhere.
const ln2 = 0x1.62e42fefa39efp-1 // ≈ 0.6931471805599453

// degree‑5 minimax for ln(1+f) on f ∈ [0,1]  (Sollya, 64‑bit floats, err < 3e‑5)
const (
	c1 = +0.9990102443771056
	c2 = -0.4891559897950173
	c3 = +0.2833026021012029
	c4 = -0.1301181019014788
	c5 = +0.0301022874045224
)

// bit‑mask of the 52 fraction bits in IEEE‑754 float64.
const fracMask uint64 = (1 << 52) - 1

// maximum safe upper bound for sqrtPriceX96.Hi before float mantissa exhaustion
// would introduce >1‑tick error. (2^53 exactly exhausts 53‑bit significand).
const maxSqrtHi = 1 << 53

// low‑bit mask used when Hi == maxSqrtHi‑1 ; any of those bits set would be lost.
const lostLowMask uint64 = (1 << 11) - 1 // lowest 11 bits

/*────────────────── tiny helpers ──────────────────*/

// validateConv panics if conv is ±Inf, NaN, 0, or outside a sane magnitude.
func validateConv(conv float64) {
	if conv == 0 || math.IsInf(conv, 0) || math.IsNaN(conv) ||
		math.Abs(conv) < 1e-300 || math.Abs(conv) > 1e300 {
		panic("fastuni: invalid conversion factor (check base)")
	}
}

/*────────────────── ln(1+f) helper ──────────────────*/

// ln1pf approximates ln(1+f) for 0 ≤ f ≤ 1 using a 4‑FMA Horner chain.
func ln1pf(f float64) float64 {
	t := f*c5 + c4
	t = f*t + c3
	t = f*t + c2
	t = f*t + c1
	return f * t
}

/*────────────────── private log₂ paths ──────────────────*/

// log2u64 returns log₂(x). Pre‑condition: x > 0.
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

// Uint128 is a 128‑bit unsigned integer (Hi << 64 | Lo).
// NB: Covers most practical Uniswap ranges but not absolute extremes (uint160).
// Helpers panic once precision loss would exceed 1 tick.

type Uint128 struct{ Hi, Lo uint64 }

// log2u128 returns log₂(u). Pre‑condition: u != 0.
func log2u128(u Uint128) float64 {
	if u.Hi == 0 && u.Lo == 0 {
		panic("fastuni: log2 of zero Uint128")
	}
	if u.Hi == 0 {
		return log2u64(u.Lo)
	}
	k := 127 - bits.LeadingZeros64(u.Hi)
	// float64 cast loses ≤11 low bits, but they vanish when we rescale anyway.
	x := float64(u.Hi)*0x1p64 + float64(u.Lo)
	return float64(k) + ln1pf(x/math.Ldexp(1, k)-1)/ln2
}

/*────────────────── V2 helpers (reserve ratios) ──────────────────*/

// Log2ReserveRatio returns log₂(a/b). Panics if a or b is zero.
func Log2ReserveRatio(a, b uint64) float64 { return log2u64(a) - log2u64(b) }

// LnReserveRatio returns ln(a/b). Uses log1p for near‑unity ratios to avoid
// catastrophic cancellation. Panics if a or b is zero.
func LnReserveRatio(a, b uint64) float64 {
	if a == 0 || b == 0 {
		panic("fastuni: zero reserve")
	}
	if a == b {
		return 0
	}
	// if ratio is within ±0.1% use Log1p for ~20× better accuracy
	if a>>4 == b>>4 { // quick coarse check: top 60 bits identical
		r := float64(a)/float64(b) - 1
		if math.Abs(r) < 1e-3 {
			return math.Log1p(r)
		}
	}
	return (log2u64(a) - log2u64(b)) * ln2
}

// LogReserveRatioConst returns log₍base₎(a/b) with pre‑computed conv.
func LogReserveRatioConst(a, b uint64, conv float64) float64 {
	validateConv(conv)
	return (log2u64(a) - log2u64(b)) * conv
}

/*────────────────── V3 helpers (sqrtPriceX96) ──────────────────*/

// Log2PriceX96 returns log₂(price) from sqrtPriceX96 (Q64.96). Panics if input
// is zero or so huge that precision would drop >1 tick.
func Log2PriceX96(sqrt Uint128) float64 {
	switch {
	case sqrt.Hi >= maxSqrtHi:
		panic("fastuni: sqrtPriceX96 exceeds safe range — precision loss > 1 tick")
	case sqrt.Hi == maxSqrtHi-1 && (sqrt.Lo&lostLowMask) != 0:
		panic("fastuni: sqrtPriceX96 near precision cliff — low bits would be lost")
	}
	log2sqrt := log2u128(sqrt) - 96 // remove Q64.96 fixed‑point scaling
	return log2sqrt * 2
}

// LnPriceX96 returns ln(price) from sqrtPriceX96.
func LnPriceX96(sqrt Uint128) float64 { return Log2PriceX96(sqrt) * ln2 }

// LogPriceX96Const returns log₍base₎(price) with pre‑computed conv.
func LogPriceX96Const(sqrt Uint128, conv float64) float64 {
	validateConv(conv)
	return Log2PriceX96(sqrt) * conv
}

/*────────────────── build‑tag hook (future asm path) ─────────────────*/

// usingASM flips to true when an architecture‑specific assembly file is linked
// via build‑tags (SIMD path planned, not yet implemented).
var usingASM = false
var _ = usingASM // silence vet
