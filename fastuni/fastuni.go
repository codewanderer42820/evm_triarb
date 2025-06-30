// Package fastuni provides ultra-fast, unchecked logarithm and ratio routines
// for Ethereum-compatible fixed-point arithmetic and reserve-ratio calculations.
// Footgun mode omits all safety checks; invalid inputs may produce NaNs or infinities.
// Use only when input correctness is guaranteed.
package fastuni

import (
	"math"
	"math/bits"
)

// -----------------------------------------------------------------------------
// Constants for log₂ conversion and polynomial approximation of ln(1+f).
// -----------------------------------------------------------------------------
const (
	ln2    = 0x1.62e42fefa39efp-1 // natural log of 2
	invLn2 = 1 / ln2              // reciprocal of ln2
)

const (
	// coefficients for 4th-degree ln(1+f) approximation
	c1 = +0.9990102443771056
	c2 = -0.4891559897950173
	c3 = +0.2833026021012029
	c4 = -0.1301181019014788
	c5 = +0.0301022874045224
)

const fracMask uint64 = (1<<52 - 1) // mask to extract mantissa bits

// -----------------------------------------------------------------------------
// Uint128: 128-bit unsigned integer (two 64-bit halves) for Q64.96 fixed-point.
// -----------------------------------------------------------------------------
type Uint128 struct {
	Hi uint64 // high 64 bits
	Lo uint64 // low 64 bits
}

// -----------------------------------------------------------------------------
// Internal helpers (no runtime checks).
// -----------------------------------------------------------------------------
// validateConv is a no-op placeholder for validating conversion factors.
//
//go:registerparams
//go:inline
//go:nosplit
func validateConv(conv float64) {
	// fast path skips validation
}

// ln1pf approximates ln(1+f) for -1<f<1 using Horner's method.
//
//go:registerparams
//go:inline
//go:nosplit
func ln1pf(f float64) float64 {
	t := f*c5 + c4
	t = f*t + c3
	t = f*t + c2
	t = f*t + c1
	return f * t
}

// -----------------------------------------------------------------------------
// log₂ implementations.
// -----------------------------------------------------------------------------
// log2u64 returns log₂(x) for x>0. If x==0, returns -Inf.
//
//go:registerparams
//go:inline
//go:nosplit
func log2u64(x uint64) float64 {
	if x == 0 {
		return math.Inf(-1)
	}
	// k = floor(log₂(x))
	k := 63 - bits.LeadingZeros64(x)
	// separate leading 1 and fraction bits
	lead := uint64(1) << k
	frac := x ^ lead

	// align fraction to mantissa (52 bits)
	if k > 52 {
		frac >>= uint(k - 52)
	} else {
		frac <<= uint(52 - k)
	}

	// assemble float in [1,2): exponent=1023
	mBits := (uint64(1023) << 52) | (frac & fracMask)
	m := math.Float64frombits(mBits)

	// return k + ln(m)/ln2
	return float64(k) + ln1pf(m-1)*invLn2
}

// log2u128 computes log₂ of a 128-bit unsigned integer.
// Delegates to log2u64 when the high half is zero.
//
//go:registerparams
//go:inline
//go:nosplit
func log2u128(u Uint128) float64 {
	if u.Hi == 0 {
		return log2u64(u.Lo)
	}
	// highest bit position k in [64..127]
	k := 127 - bits.LeadingZeros64(u.Hi)
	// combine high and low into float64
	x := float64(u.Hi)*0x1p64 + float64(u.Lo)
	// assemble 2^k via exponent bits
	expBits := uint64(1023+k) << 52
	expFloat := math.Float64frombits(expBits)

	// return k + ln(x/2^k)/ln2
	return float64(k) + ln1pf(x/expFloat-1)*invLn2
}

// -----------------------------------------------------------------------------
// Reserve-ratio helpers (Uniswap V2).
// -----------------------------------------------------------------------------
// Log2ReserveRatio returns log₂(a/b) without checks.
//
//go:registerparams
//go:inline
//go:nosplit
func Log2ReserveRatio(a, b uint64) float64 {
	return log2u64(a) - log2u64(b)
}

// LnReserveRatio returns ln(a/b), using log1p for ratios near 1.
//
//go:registerparams
//go:inline
//go:nosplit
func LnReserveRatio(a, b uint64) float64 {
	if a>>4 == b>>4 {
		r := float64(a)/float64(b) - 1
		if math.Abs(r) < 1e-3 {
			return math.Log1p(r)
		}
	}
	// fallback: (log₂(a)-log₂(b))*ln2
	return (log2u64(a) - log2u64(b)) * ln2
}

// LogReserveRatioConst returns ln(a/b)*conv without validating conv.
//
//go:registerparams
//go:inline
//go:nosplit
func LogReserveRatioConst(a, b uint64, conv float64) float64 {
	validateConv(conv)
	return (log2u64(a) - log2u64(b)) * conv
}

// -----------------------------------------------------------------------------
// Price helpers (Uniswap V3, Q64.96 fixed-point).
// -----------------------------------------------------------------------------
// Log2PriceX96 computes 2*log₂(sqrtPrice)-192 for full price ratio.
//
//go:registerparams
//go:inline
//go:nosplit
func Log2PriceX96(sqrt Uint128) float64 {
	return (log2u128(sqrt) - 96) * 2
}

// LnPriceX96 returns ln(price) = 2*ln(sqrtPrice).
//
//go:registerparams
//go:inline
//go:nosplit
func LnPriceX96(sqrt Uint128) float64 {
	return Log2PriceX96(sqrt) * ln2
}

// LogPriceX96Const returns ln(price)*conv without safety checks.
//
//go:registerparams
//go:inline
//go:nosplit
func LogPriceX96Const(sqrt Uint128, conv float64) float64 {
	validateConv(conv)
	return Log2PriceX96(sqrt) * conv
}
