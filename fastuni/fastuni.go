// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: fastuni.go — ISR-grade logarithmic utilities for EVM math
//
// Purpose:
//   - Provides high-throughput fixed-point log₂/ln computation for Uniswap-style
//     Q64.96 prices and ratios
//
// Notes:
//   - Internal functions are footgun-mode (no checks, fast paths only)
//   - All exported functions validate inputs and handle special cases (e.g. r ≈ 0)
//   - Designed for sub-10ns tick processing in ISR pipelines
//
// Compiler Directives:
//   - //go:nosplit
//   - //go:inline
//   - //go:registerparams
//   - //go:notinheap (for Uint128 struct)
//   - //go:align 64 (for alignment-critical structs)
//
// ⚠️ Internal routines assume valid input and WILL panic or misbehave otherwise.
// ─────────────────────────────────────────────────────────────────────────────

package fastuni

import (
	"errors"
	"math"
	"math/bits"
)

/*──────────────────────────────────────────────────────────────────────────────
  📦 Errors: exported sentinel values for input validation
──────────────────────────────────────────────────────────────────────────────*/

var (
	// ErrZeroValue indicates input value was zero (illegal for log)
	ErrZeroValue = errors.New("input value must be non-zero")

	// ErrOutOfRange indicates domain error (e.g. ln1pf(f) with f ≤ -1)
	ErrOutOfRange = errors.New("input out of valid range")
)

/*──────────────────────────────────────────────────────────────────────────────
  🔧 Constants: tuning values for polynomial and IEEE 754 manipulation
──────────────────────────────────────────────────────────────────────────────*/

const (
	ln2    = 0x1.62e42fefa39efp-1 // ln(2)
	invLn2 = 1 / ln2              // 1/ln(2)
)

const (
	// Horner’s method: ln(1+f) ≈ f·(c₁ + f·(c₂ + f·(…)))
	c1 = +0.9990102443771056
	c2 = -0.4891559897950173
	c3 = +0.2833026021012029
	c4 = -0.1301181019014788
	c5 = +0.0301022874045224
)

const fracMask uint64 = (1<<52 - 1) // Extracts fraction for IEEE float tricks

/*──────────────────────────────────────────────────────────────────────────────
  🧱 Type: 128-bit unsigned integer (Q64.96 fixed-point layout)
──────────────────────────────────────────────────────────────────────────────*/

// Uint128 is a high-precision unsigned integer.
// Used to represent Q64.96 fixed-point prices (Uniswap V3).
//
// Layout:
//   - Hi: high 64 bits
//   - Lo: low 64 bits
//
//go:notinheap
//go:align 64
type Uint128 struct {
	Hi uint64 // high 64 bits
	Lo uint64 // low 64 bits
}

/*──────────────────────────────────────────────────────────────────────────────
  🔩 Internal Routines: unsafe, high-performance, no checks
──────────────────────────────────────────────────────────────────────────────*/
// ⚠️ Internal routines:
//    - Assume valid non-zero, in-range input
//    - Will panic or corrupt if misused
//    - Safe wrappers follow below

// ln1pf returns ln(1+f) using a 5th-order Horner polynomial.
// Accurate for |f| ≪ 1.
//
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

// log2u64 computes log₂(x) using MSB location and ln1pf normalization.
//
//go:nosplit
//go:inline
//go:registerparams
func log2u64(x uint64) float64 {
	// ⚠️ Precondition: x > 0
	k := 63 - bits.LeadingZeros64(x)
	lead := uint64(1) << k
	frac := x ^ lead

	if k > 52 {
		frac >>= uint(k - 52)
	} else {
		frac <<= uint(52 - k)
	}

	mBits := (uint64(1023) << 52) | (frac & fracMask)
	m := math.Float64frombits(mBits) // Normalized mantissa in [1, 2)

	return float64(k) + ln1pf(m-1)*invLn2
}

// log2u128 computes log₂ of a 128-bit integer via widening and normalization.
//
//go:nosplit
//go:inline
//go:registerparams
func log2u128(u Uint128) float64 {
	// ⚠️ Precondition: u ≠ 0
	if u.Hi == 0 {
		return log2u64(u.Lo)
	}
	k := 127 - bits.LeadingZeros64(u.Hi)
	x := float64(u.Hi)*0x1p64 + float64(u.Lo)
	exp := math.Float64frombits(uint64(1023+k) << 52)
	return float64(k) + ln1pf(x/exp-1)*invLn2
}

/*──────────────────────────────────────────────────────────────────────────────
  📤 Safe Wrappers: exported, validated, panic-free
──────────────────────────────────────────────────────────────────────────────*/

// Log2ReserveRatio returns log₂(a / b), with safety validation.
//
//go:nosplit
//go:inline
//go:registerparams
func Log2ReserveRatio(a, b uint64) (float64, error) {
	if a == 0 || b == 0 {
		return 0, ErrZeroValue
	}
	return log2u64(a) - log2u64(b), nil
}

// LnReserveRatio returns ln(a / b), optimized for near-unity deltas.
// Automatically chooses Log1p or log₂ fallback.
//
//go:nosplit
//go:inline
//go:registerparams
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

// LogReserveRatioConst returns ln(a / b) * conv (user-specified base).
//
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

// Log2PriceX96 computes 2·log₂(sqrtPrice) − 192 from Q64.96 value.
// This yields log₂(price) in Uniswap V3 terms.
//
//go:nosplit
//go:inline
//go:registerparams
func Log2PriceX96(s Uint128) (float64, error) {
	if s.Hi == 0 && s.Lo == 0 {
		return 0, ErrZeroValue
	}
	return (log2u128(s) - 96) * 2, nil
}

// LnPriceX96 returns ln(price) from a Q64.96 Uint128 price.
//
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

// LogPriceX96Const returns ln(price) * conv for Q64.96 Uint128 price.
// Supports user-defined scaling factor (e.g., base conversion).
//
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
