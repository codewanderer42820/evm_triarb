// Package fastuni provides high-performance logarithm and ratio routines
// for Ethereum-compatible fixed-point arithmetic.
//
// The internal functions are tuned for latency-critical execution,
// and therefore omit safety checks. Exported wrappers should be used
// for validated, error-checked usage.
//
// ⚠️ Internal functions require the caller to uphold preconditions.
//   - Inputs must be non-zero
//   - Inputs must be within representable numeric ranges
//   - Invalid inputs may cause panics or incorrect results
package fastuni

import (
	"errors"
	"math"
	"math/bits"
)

// -----------------------------------------------------------------------------
// Error definitions
// -----------------------------------------------------------------------------

var (
	// ErrZeroValue is returned if any input is zero when non-zero is required
	ErrZeroValue = errors.New("input value must be non-zero")

	// ErrOutOfRange is returned when an input is outside a valid domain
	// For example: ln1pf(f) with f <= -1, or conv being NaN/Inf
	ErrOutOfRange = errors.New("input out of valid range")
)

// -----------------------------------------------------------------------------
// Constants and types
// -----------------------------------------------------------------------------

const (
	ln2    = 0x1.62e42fefa39efp-1 // ln(2), used to convert log₂ to natural log
	invLn2 = 1 / ln2              // 1/ln(2), used to convert ln to log₂
)

const (
	// Polynomial coefficients for ln(1+f) approximation using Horner's method
	c1 = +0.9990102443771056
	c2 = -0.4891559897950173
	c3 = +0.2833026021012029
	c4 = -0.1301181019014788
	c5 = +0.0301022874045224
)

const fracMask uint64 = (1<<52 - 1) // Mask to isolate 52-bit fraction

// Uint128 represents an unsigned 128-bit integer.
// Used to hold fixed-point Q64.96 Uniswap-style prices.
type Uint128 struct {
	Hi uint64 // high 64 bits
	Lo uint64 // low 64 bits
}

// -----------------------------------------------------------------------------
// Internal helpers (high-performance, no safety checks)
// -----------------------------------------------------------------------------
// ⚠️ Callers must ensure:
//    - All inputs are valid (e.g., x > 0, f > -1)
//    - No zero values or NaNs are passed
// Use exported wrappers for validated behavior.

// ln1pf approximates ln(1+f) for f > -1 using a 5th-degree polynomial.
// Assumes |f| ≪ 1 for accuracy.
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

// log2u64 computes log₂(x) for x > 0 using bit tricks and ln1pf polynomial.
//
//go:registerparams
//go:inline
//go:nosplit
func log2u64(x uint64) float64 {
	// Preconditions: x > 0
	k := 63 - bits.LeadingZeros64(x) // Find most significant bit
	lead := uint64(1) << k           // Isolate MSB
	frac := x ^ lead                 // Remove MSB to get fractional part

	// Normalize frac into mantissa-like float64
	if k > 52 {
		frac >>= uint(k - 52)
	} else {
		frac <<= uint(52 - k)
	}

	mBits := (uint64(1023) << 52) | (frac & fracMask) // Construct IEEE 754 float64
	m := math.Float64frombits(mBits)                  // Normalized: 1 ≤ m < 2

	return float64(k) + ln1pf(m-1)*invLn2
}

// log2u128 computes log₂(u) for u > 0 using float64 widening and polynomial approx.
//
//go:registerparams
//go:inline
//go:nosplit
func log2u128(u Uint128) float64 {
	// Preconditions: u.Hi ≠ 0 or u.Lo ≠ 0
	if u.Hi == 0 {
		return log2u64(u.Lo)
	}
	k := 127 - bits.LeadingZeros64(u.Hi)      // Find top bit in high 64 bits
	x := float64(u.Hi)*0x1p64 + float64(u.Lo) // Promote to 64-bit float
	exp := math.Float64frombits(uint64(1023+k) << 52)
	return float64(k) + ln1pf(x/exp-1)*invLn2
}

// -----------------------------------------------------------------------------
// Exported safe wrappers (error-checked)
// -----------------------------------------------------------------------------

// Log2ReserveRatio returns log₂(a / b), with safety checks for zero inputs.
//
//go:registerparams
//go:inline
//go:nosplit
func Log2ReserveRatio(a, b uint64) (float64, error) {
	if a == 0 || b == 0 {
		return 0, ErrZeroValue
	}
	return log2u64(a) - log2u64(b), nil
}

// LnReserveRatio returns ln(a / b) with fallback to log2 path for large deltas.
// Automatically uses math.Log1p for near-equal a and b.
//
//go:registerparams
//go:inline
//go:nosplit
func LnReserveRatio(a, b uint64) (float64, error) {
	if a == 0 || b == 0 {
		return 0, ErrZeroValue
	}
	r := float64(a)/float64(b) - 1
	if math.Abs(r) < 1e-3 {
		return math.Log1p(r), nil
	}
	return (log2u64(a) - log2u64(b)) * ln2, nil
}

// LogReserveRatioConst returns ln(a / b) scaled by a user-defined constant.
//
//go:registerparams
//go:inline
//go:nosplit
func LogReserveRatioConst(a, b uint64, conv float64) (float64, error) {
	if a == 0 || b == 0 {
		return 0, ErrZeroValue
	}
	if math.IsNaN(conv) || math.IsInf(conv, 0) {
		return 0, ErrOutOfRange
	}
	return (log2u64(a) - log2u64(b)) * conv, nil
}

// Log2PriceX96 computes 2·log₂(sqrtPrice) − 192, where price is Q64.96.
// Equivalent to log₂(price) for Uniswap V3 price encoding.
//
//go:registerparams
//go:inline
//go:nosplit
func Log2PriceX96(s Uint128) (float64, error) {
	if s.Hi == 0 && s.Lo == 0 {
		return 0, ErrZeroValue
	}
	return (log2u128(s) - 96) * 2, nil
}

// LnPriceX96 returns ln(price) for Q64.96 Uniswap-style prices.
//
//go:registerparams
//go:inline
//go:nosplit
func LnPriceX96(s Uint128) (float64, error) {
	v, err := Log2PriceX96(s)
	if err != nil {
		return 0, err
	}
	return v * ln2, nil
}

// LogPriceX96Const returns ln(price)·conv for price in Q64.96 format.
// Useful for converting log values into other bases or units.
//
//go:registerparams
//go:inline
//go:nosplit
func LogPriceX96Const(s Uint128, conv float64) (float64, error) {
	if math.IsNaN(conv) || math.IsInf(conv, 0) {
		return 0, ErrOutOfRange
	}
	v, err := Log2PriceX96(s)
	if err != nil {
		return 0, err
	}
	return v * conv, nil
}
