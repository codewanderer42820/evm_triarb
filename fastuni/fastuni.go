// Package fastuni provides high-performance logarithm and ratio routines
// with optional safety checks for Ethereum-compatible fixed-point arithmetic.
// Safe wrappers return errors on invalid inputs, while internal functions
// remain fast and unchecked for raw performance.
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
	// ErrZeroValue indicates a zero input where non-zero is required
	ErrZeroValue = errors.New("input value must be non-zero")
	// ErrOutOfRange indicates an input outside a valid domain (e.g. f <= -1 for ln1pf)
	ErrOutOfRange = errors.New("input out of valid range")
)

// -----------------------------------------------------------------------------
// Constants and types (unchanged)
// -----------------------------------------------------------------------------
const (
	ln2    = 0x1.62e42fefa39efp-1 // natural log of 2
	invLn2 = 1 / ln2              // reciprocal of ln2
)

const (
	c1 = +0.9990102443771056 // ln1p polynomial coeffs
	c2 = -0.4891559897950173
	c3 = +0.2833026021012029
	c4 = -0.1301181019014788
	c5 = +0.0301022874045224
)

const fracMask uint64 = (1<<52 - 1)

type Uint128 struct { // Q64.96 fixed-point
	Hi uint64
	Lo uint64
}

// -----------------------------------------------------------------------------
// Internal helpers (no runtime checks)
// -----------------------------------------------------------------------------

// ln1pf approximates ln(1+f) using Horner's method.
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

// log2u64 computes log₂(x) for x>0 using bit manipulation and polynomial approx.
//
//go:registerparams
//go:inline
//go:nosplit
func log2u64(x uint64) float64 {
	// unchecked: caller must ensure x > 0
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
	return float64(k) + ln1pf(m-1)*invLn2
}

// log2u128 computes log₂ for a 128-bit unsigned integer.
//
//go:registerparams
//go:inline
//go:nosplit
func log2u128(u Uint128) float64 {
	if u.Hi == 0 {
		return log2u64(u.Lo)
	}
	k := 127 - bits.LeadingZeros64(u.Hi)
	x := float64(u.Hi)*0x1p64 + float64(u.Lo)
	exp := math.Float64frombits(uint64(1023+k) << 52)
	return float64(k) + ln1pf(x/exp-1)*invLn2
}

// -----------------------------------------------------------------------------
// Safe public wrappers
// -----------------------------------------------------------------------------

// Log2ReserveRatio returns log₂(a/b) with error on zero inputs.
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

// LnReserveRatio returns ln(a/b), with error on zero inputs.
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
	res := log2u64(a) - log2u64(b)
	return res * ln2, nil
}

// LogReserveRatioConst returns ln(a/b)*conv, with error on zero or invalid conv.
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

// Log2PriceX96 computes 2*log₂(sqrtPrice)-192, with error on zero price.
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

// LnPriceX96 returns ln(price), with error on zero price.
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

// LogPriceX96Const returns ln(price)*conv, with error on invalid inputs.
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
