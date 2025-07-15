// ════════════════════════════════════════════════════════════════════════════════════════════════
// ⚡ LOGARITHMIC COMPUTATION LIBRARY
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: High-Frequency Trading System
// Component: Uniswap V3 Q64.96 Mathematics Library
//
// Description:
//   Logarithmic utilities for Uniswap V3 price calculations using Q64.96 fixed-point format.
//   Implements bit-manipulation based logarithm computation for reserve ratios and price
//   conversions with polynomial approximation for fractional components.
//
// Mathematical Foundation:
//   - Q64.96 format: 64 integer bits, 96 fractional bits for square root prices
//   - Logarithmic price ratios enable efficient arbitrage calculations
//   - Base-2 logarithms converted to natural logarithms as needed
//   - Polynomial approximation for high-precision fractional calculations
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package fastuni

import (
	"errors"
	"math"
	"math/bits"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ERROR DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 64
var (
	// ErrZeroValue indicates an invalid zero input where positive values are required.
	// This typically occurs when calculating logarithms of token reserves.
	ErrZeroValue = errors.New("input value must be non-zero")

	// ErrOutOfRange indicates the input exceeds valid computational bounds.
	// This protects against overflow conditions in fixed-point arithmetic.
	ErrOutOfRange = errors.New("input out of valid range")
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MATHEMATICAL CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// ln2 represents the natural logarithm of 2 with high precision.
	// Used for converting between base-2 and natural logarithms.
	ln2 = 0x1.62e42fefa39efp-1 // Natural log of 2

	// invLn2 is the reciprocal of ln(2) for base conversion.
	// Multiplication by reciprocal avoids division operations.
	invLn2 = 1 / ln2 // Reciprocal for base conversion
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// POLYNOMIAL APPROXIMATION COEFFICIENTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Fifth-order polynomial coefficients for ln(1+f) approximation.
// These coefficients provide accurate approximation for |f| < 0.5.
// Higher-order terms contribute progressively less to the final result.
const (
	c1 = +0.9990102443771056 // Linear term
	c2 = -0.4891559897950173 // Quadratic term
	c3 = +0.2833026021012029 // Cubic term
	c4 = -0.1301181019014788 // Quartic term
	c5 = +0.0301022874045224 // Quintic term
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// IEEE 754 CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// fracMask extracts the 52-bit mantissa from IEEE 754 double precision format.
// Used for reconstructing normalized floating-point values during logarithm computation.
const fracMask uint64 = (1<<52 - 1)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Uint128 represents 128-bit unsigned integer for Q64.96 prices.
// Uniswap V3 uses this format to store square root prices with high precision.
//
//go:notinheap
//go:align 16
type Uint128 struct {
	Hi uint64 // Upper 64 bits
	Lo uint64 // Lower 64 bits
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INTERNAL COMPUTATION FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ln1pf computes ln(1+f) using 5th-order Horner polynomial evaluation.
// This function provides accurate approximation for small values of f.
//
// MATHEMATICAL BASIS:
//
//	ln(1+f) ≈ f*(c1 + f*(c2 + f*(c3 + f*(c4 + f*c5))))
//	Horner's method reduces the number of multiplications required.
//
// CONSTRAINTS:
//   - Input f must satisfy |f| < 0.5 for accurate results
//   - Larger values of f lead to increased approximation error
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ln1pf(f float64) float64 {
	// Evaluate polynomial using Horner's method
	// Start with highest degree term and work backwards
	t := f*c5 + c4
	t = f*t + c3
	t = f*t + c2
	t = f*t + c1
	return f * t
}

// log2u64 computes log₂(x) using bit manipulation and polynomial approximation.
// This function decomposes the logarithm into integer and fractional parts.
//
// ALGORITHM:
//  1. Extract the position of the most significant bit (integer part)
//  2. Extract mantissa bits for fractional calculation
//  3. Normalize mantissa to [1, 2) range
//  4. Apply polynomial approximation for fractional part
//  5. Combine integer and fractional components
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func log2u64(x uint64) float64 {
	// Find the position of the most significant bit
	// This gives us the integer part of log₂(x)
	k := 63 - bits.LeadingZeros64(x)
	lead := uint64(1) << k

	// Extract bits after the leading bit
	// These form the fractional part of the logarithm
	frac := x ^ lead

	// Normalize fractional bits to mantissa precision
	// Shift to align with IEEE 754 mantissa position
	if k > 52 {
		frac >>= uint(k - 52)
	} else {
		frac <<= uint(52 - k)
	}

	// Reconstruct a normalized double in range [1, 2)
	// Bias of 1023 centers the exponent at zero
	mBits := (uint64(1023) << 52) | (frac & fracMask)
	m := math.Float64frombits(mBits)

	// Combine integer and fractional parts
	// The fractional part is computed as log₂(1 + (m-1))
	return float64(k) + ln1pf(m-1)*invLn2
}

// log2u128 handles 128-bit logarithm calculation for large values.
// Extended precision is required for Uniswap V3's Q64.96 format.
//
// ALGORITHM:
//   - For values ≤ 2⁶⁴: delegate to 64-bit implementation
//   - For larger values: use floating-point approximation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func log2u128(u Uint128) float64 {
	// Optimize for common case where value fits in 64 bits
	if u.Hi == 0 {
		return log2u64(u.Lo)
	}

	// Find the most significant bit position in the high word
	k := 127 - bits.LeadingZeros64(u.Hi)

	// Convert to double with controlled precision loss
	// Combine high and low words with appropriate scaling
	x := float64(u.Hi)*0x1p64 + float64(u.Lo)

	// Apply logarithmic approximation
	// Create scaling factor to normalize x to [1, 2) range
	exp := math.Float64frombits(uint64(1023+k) << 52)
	return float64(k) + ln1pf(x/exp-1)*invLn2
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Log2ReserveRatio computes log₂(a/b) for token reserve ratios.
// This is the fundamental operation for calculating price relationships.
//
// MATHEMATICAL BASIS:
//
//	log₂(a/b) = log₂(a) - log₂(b)
//	This transformation avoids division and potential precision loss.
//
// USE CASES:
//   - AMM price calculations
//   - Arbitrage opportunity detection
//   - Liquidity pool analysis
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Log2ReserveRatio(a, b uint64) (float64, error) {
	// Validate inputs to prevent undefined logarithms
	if a == 0 || b == 0 {
		return 0, ErrZeroValue
	}
	// Compute ratio using logarithm properties
	return log2u64(a) - log2u64(b), nil
}

// LnReserveRatio computes ln(a/b) with precision optimization.
// Natural logarithm is often preferred for financial calculations.
//
// OPTIMIZATION:
//
//	For ratios near 1.0, uses log1p for improved precision.
//	This handles the common case of small price movements accurately.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LnReserveRatio(a, b uint64) (float64, error) {
	// Validate inputs
	if a == 0 || b == 0 {
		return 0, ErrZeroValue
	}

	// High precision path for ratios near unity
	// This is common when prices haven't moved significantly
	r := float64(a)/float64(b) - 1
	if math.Abs(r) < 1e-3 {
		return math.Log1p(r), nil
	}

	// General path using base-2 logarithm
	// Convert to natural logarithm by multiplying by ln(2)
	return (log2u64(a) - log2u64(b)) * ln2, nil
}

// LogReserveRatioConst computes logarithm with custom base conversion factor.
// Allows efficient computation of logarithms in any base.
//
// PARAMETERS:
//   - a, b: Reserve amounts to calculate ratio
//   - conv: Conversion factor from base-2 (e.g., ln2 for natural log)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LogReserveRatioConst(a, b uint64, conv float64) (float64, error) {
	// Validate reserve inputs
	if a == 0 || b == 0 {
		return 0, ErrZeroValue
	}
	// Validate conversion factor
	if math.IsNaN(conv) || math.IsInf(conv, 0) {
		return 0, ErrOutOfRange
	}
	// Apply custom base conversion
	return (log2u64(a) - log2u64(b)) * conv, nil
}

// Log2PriceX96 extracts log₂(price) from Uniswap V3 sqrtPrice format.
// Uniswap V3 stores prices as square roots in Q64.96 fixed-point format.
//
// MATHEMATICAL BASIS:
//
//	If sqrtPrice = sqrt(price) * 2^96, then:
//	log₂(price) = 2 * (log₂(sqrtPrice) - 96)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Log2PriceX96(s Uint128) (float64, error) {
	// Check for zero price (invalid state)
	if s.Hi == 0 && s.Lo == 0 {
		return 0, ErrZeroValue
	}
	// Extract price from square root representation
	// Subtract 96 to account for Q64.96 scaling
	// Multiply by 2 to convert from sqrt(price) to price
	return (log2u128(s) - 96) * 2, nil
}

// LnPriceX96 extracts ln(price) from Q64.96 sqrtPrice format.
// Natural logarithm variant of Log2PriceX96 for compatibility.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LnPriceX96(s Uint128) (float64, error) {
	// Compute base-2 logarithm first
	v, err := Log2PriceX96(s)
	if err != nil {
		return 0, err
	}
	// Convert to natural logarithm
	return v * ln2, nil
}

// LogPriceX96Const computes custom base logarithm of price.
// Flexible variant supporting arbitrary logarithm bases.
//
// USE CASES:
//   - Custom analysis requirements
//   - Alternative base calculations
//   - Specialized financial metrics
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LogPriceX96Const(s Uint128, conv float64) (float64, error) {
	// Validate conversion factor
	if math.IsNaN(conv) || math.IsInf(conv, 0) {
		return 0, ErrOutOfRange
	}
	// Compute base-2 logarithm
	v, err := Log2PriceX96(s)
	if err != nil {
		return 0, err
	}
	// Apply custom base conversion
	return v * conv, nil
}
