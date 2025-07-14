// fastuni - ISR-grade logarithmic utilities for Uniswap Q64.96 mathematics
package fastuni

import (
	"errors"
	"math"
	"math/bits"
)

// Error definitions
var (
	ErrZeroValue  = errors.New("input value must be non-zero")
	ErrOutOfRange = errors.New("input out of valid range")
)

// Mathematical constants
const (
	ln2    = 0x1.62e42fefa39efp-1 // Natural log of 2
	invLn2 = 1 / ln2              // Reciprocal for base conversion
)

// Polynomial coefficients for ln(1+f) approximation
const (
	c1 = +0.9990102443771056 // Linear term
	c2 = -0.4891559897950173 // Quadratic
	c3 = +0.2833026021012029 // Cubic
	c4 = -0.1301181019014788 // Quartic
	c5 = +0.0301022874045224 // Quintic
)

// IEEE 754 mantissa extraction
const fracMask uint64 = (1<<52 - 1)

// Uint128 represents 128-bit unsigned integer for Q64.96 prices
//
//go:notinheap
//go:align 16
type Uint128 struct {
	Hi uint64 // Upper 64 bits
	Lo uint64 // Lower 64 bits
}

// ln1pf computes ln(1+f) using 5th-order Horner polynomial
// ⚠️  Internal: assumes |f| < 0.5
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ln1pf(f float64) float64 {
	t := f*c5 + c4
	t = f*t + c3
	t = f*t + c2
	t = f*t + c1
	return f * t
}

// log2u64 computes log₂(x) via bit manipulation
// ⚠️  Internal: assumes x > 0
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func log2u64(x uint64) float64 {
	// Extract MSB position
	k := 63 - bits.LeadingZeros64(x)
	lead := uint64(1) << k

	// Extract fractional bits
	frac := x ^ lead

	// Normalize to mantissa precision
	if k > 52 {
		frac >>= uint(k - 52)
	} else {
		frac <<= uint(52 - k)
	}

	// Reconstruct IEEE 754 double
	mBits := (uint64(1023) << 52) | (frac & fracMask)
	m := math.Float64frombits(mBits)

	// Combine integer and fractional parts
	return float64(k) + ln1pf(m-1)*invLn2
}

// log2u128 handles 128-bit logarithm calculation
// ⚠️  Internal: assumes u ≠ 0
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func log2u128(u Uint128) float64 {
	// Optimize for values ≤ 2⁶⁴
	if u.Hi == 0 {
		return log2u64(u.Lo)
	}

	// Extract bit position from high word
	k := 127 - bits.LeadingZeros64(u.Hi)

	// Convert to double with minimal precision loss
	x := float64(u.Hi)*0x1p64 + float64(u.Lo)

	// Apply logarithmic approximation
	exp := math.Float64frombits(uint64(1023+k) << 52)
	return float64(k) + ln1pf(x/exp-1)*invLn2
}

// Log2ReserveRatio computes log₂(a/b) for reserve ratios
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Log2ReserveRatio(a, b uint64) (float64, error) {
	if a == 0 || b == 0 {
		return 0, ErrZeroValue
	}
	return log2u64(a) - log2u64(b), nil
}

// LnReserveRatio computes ln(a/b) with precision optimization
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LnReserveRatio(a, b uint64) (float64, error) {
	if a == 0 || b == 0 {
		return 0, ErrZeroValue
	}

	// High precision path for near-unity ratios
	r := float64(a)/float64(b) - 1
	if math.Abs(r) < 1e-3 {
		return math.Log1p(r), nil
	}

	// General path via log₂
	return (log2u64(a) - log2u64(b)) * ln2, nil
}

// LogReserveRatioConst computes logarithm with custom base
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LogReserveRatioConst(a, b uint64, conv float64) (float64, error) {
	if a == 0 || b == 0 {
		return 0, ErrZeroValue
	}
	if math.IsNaN(conv) || math.IsInf(conv, 0) {
		return 0, ErrOutOfRange
	}
	return (log2u64(a) - log2u64(b)) * conv, nil
}

// Log2PriceX96 extracts log₂(price) from Uniswap V3 sqrtPrice
// sqrtPrice format: Q64.96 fixed-point square root
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Log2PriceX96(s Uint128) (float64, error) {
	if s.Hi == 0 && s.Lo == 0 {
		return 0, ErrZeroValue
	}
	return (log2u128(s) - 96) * 2, nil
}

// LnPriceX96 extracts ln(price) from Q64.96 sqrtPrice
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LnPriceX96(s Uint128) (float64, error) {
	v, err := Log2PriceX96(s)
	if err != nil {
		return 0, err
	}
	return v * ln2, nil
}

// LogPriceX96Const computes custom base logarithm of price
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
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
