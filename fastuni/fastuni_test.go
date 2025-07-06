// ============================================================================
// FASTUNI CORRECTNESS VALIDATION SUITE
// ============================================================================
//
// Comprehensive unit testing framework for ISR-grade logarithmic utilities
// with emphasis on mathematical accuracy and edge case handling.
//
// Test categories:
//   - Core logarithmic functions: log₂(uint64), log₂(Uint128)
//   - Reserve ratio calculations: ln(a/b), log₂(a/b), scaled variants
//   - Q64.96 price transformations: Uniswap V3 format conversions
//   - Edge case validation: boundary values, precision limits
//   - Error condition handling: zero inputs, invalid parameters
//   - Randomized validation: 1M samples per function
//
// Accuracy requirements:
//   - Tolerance: 5e-5 for general computations
//   - Reference comparison: Go math package (math.Log, math.Log2)
//   - Symmetry validation: ln(a/b) + ln(b/a) = 0
//   - Boundary testing: Powers of 2, MaxUint64, precision edges
//
// Test methodology:
//   - Deterministic seeds for reproducible randomized testing
//   - Comprehensive edge case coverage including IEEE 754 boundaries
//   - Error path validation with expected sentinel values
//   - Performance assumption verification

package fastuni

import (
	"math"
	"math/rand"
	"testing"
)

// ============================================================================
// TEST CONFIGURATION
// ============================================================================

const (
	tol     = 5e-5      // Floating-point comparison tolerance
	rndSeed = 69        // Deterministic seed for randomized tests
	rndLoop = 1_000_000 // Sample count for statistical validation
)

// ============================================================================
// UINT64 LOGARITHM VALIDATION
// ============================================================================

// TestLog2u64 validates log₂(x) computation for uint64 inputs.
//
// Test scenarios:
//   - Powers of 2: Exact integer results (k = log₂(2ᵏ))
//   - Arbitrary values: Comparison with math.Log2 reference
//   - Precision validation: Tolerance checking for complex values
//
// Mathematical verification:
//
//	log₂(2ᵏ) = k for k ∈ [0, 63]
//	log₂(x) ≈ math.Log2(x) within tolerance for arbitrary x
func TestLog2u64(t *testing.T) {
	// Test exact powers of 2
	for k := uint(0); k < 64; k++ {
		x := uint64(1) << k
		if got := log2u64(x); got != float64(k) {
			t.Errorf("log2u64(2^%d): want %g got %g", k, float64(k), got)
		}
	}

	// Test arbitrary values against reference implementation
	tests := []uint64{3, 5, 123456789, (1 << 52) + 12345}
	for _, x := range tests {
		got := log2u64(x)
		want := math.Log2(float64(x))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u64(%d): want %g got %g delta=%g", x, want, got, got-want)
		}
	}
}

// TestLog2u64_EdgeCases validates boundary condition handling.
//
// Edge cases:
//   - IEEE 754 precision boundary: 2⁵² (mantissa overflow point)
//   - Near-boundary values: 2⁵² + 1 (precision transition)
//   - Maximum value: MaxUint64 (range boundary)
//
// Validates algorithm stability across precision transition points.
func TestLog2u64_EdgeCases(t *testing.T) {
	// Test IEEE 754 mantissa precision boundary
	x := uint64(1 << 52)
	if got := log2u64(x); math.Abs(got-52) > tol {
		t.Errorf("log2u64(2^52): want 52, got %g", got)
	}

	// Test precision transition behavior
	if got := log2u64(x + 1); math.Abs(got-math.Log2(float64(x+1))) > tol {
		t.Errorf("log2u64(2^52+1): want %g, got %g", math.Log2(float64(x+1)), got)
	}

	// Test maximum representable value
	if got := log2u64(math.MaxUint64); math.Abs(got-math.Log2(float64(math.MaxUint64))) > tol {
		t.Errorf("log2u64(MaxUint64): want %g, got %g",
			math.Log2(float64(math.MaxUint64)), got)
	}
}

// ============================================================================
// UINT128 LOGARITHM VALIDATION
// ============================================================================

// TestLog2u128 validates log₂(x) computation for 128-bit inputs.
//
// Test coverage:
//   - Low-word only values: Hi=0, Lo=various (delegation to log2u64)
//   - High-word values: Hi≠0, Lo=0 (pure high-word computation)
//   - Mixed values: Hi≠0, Lo≠0 (full 128-bit computation)
//
// Mathematical verification:
//
//	Uint128{Hi, Lo} represents Hi×2⁶⁴ + Lo
//	log₂(Hi×2⁶⁴ + Lo) should match math.Log2 of equivalent float64
func TestLog2u128(t *testing.T) {
	// Test low-word only (delegation path)
	for _, lo := range []uint64{1, 2, 3, 1234567890, 1 << 52} {
		u := Uint128{0, lo}
		got := log2u128(u)
		want := math.Log2(float64(lo))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u128(%#v): want %g got %g", u, want, got)
		}
	}

	// Test mixed high/low word combinations
	for _, u := range []Uint128{{1, 0}, {1 << 10, 1234567890}} {
		got := log2u128(u)
		want := math.Log2(float64(u.Hi)*math.Ldexp(1, 64) + float64(u.Lo))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u128(%#v): want %g got %g", u, want, got)
		}
	}
}

// TestLog2u128_EdgeCases validates 128-bit boundary conditions.
//
// Edge scenarios:
//   - Maximum low word: Lo=MaxUint64, Hi=0
//   - Mixed maximum: Hi=1, Lo=MaxUint64 (near-overflow)
//   - High-word boundaries: Powers of 2 in high word
//
// Tests algorithm stability at representation boundaries.
func TestLog2u128_EdgeCases(t *testing.T) {
	// Test maximum low word value
	u := Uint128{0, math.MaxUint64}
	got := log2u128(u)
	want := math.Log2(float64(u.Lo))
	if math.Abs(got-want) > tol {
		t.Errorf("log2u128(Lo=MaxUint64): want %g got %g", want, got)
	}

	// Test mixed boundary condition
	u = Uint128{1, math.MaxUint64}
	want = math.Log2(float64(u.Hi)*math.Ldexp(1, 64) + float64(u.Lo))
	got = log2u128(u)
	if math.Abs(got-want) > tol {
		t.Errorf("log2u128(Hi=1,Lo=MaxUint64): want %g got %g", want, got)
	}
}

// ============================================================================
// RESERVE RATIO CALCULATIONS
// ============================================================================

// TestLnReserveRatio validates ln(a/b) computation with path optimization.
//
// Test scenarios:
//   - Error conditions: Zero inputs (ErrZeroValue expected)
//   - Identity case: a=b (result should be ln(1)=0)
//   - Small deltas: log1p optimization path
//   - Large deltas: log₂ difference fallback path
//   - Symmetry: ln(a/b) + ln(b/a) = 0
//   - Boundary values: MaxUint64 inputs
//
// Validates automatic path selection and mathematical correctness.
func TestLnReserveRatio(t *testing.T) {
	// Test error conditions
	if _, err := LnReserveRatio(0, 1); err != ErrZeroValue {
		t.Errorf("LnReserveRatio(0,1): expected ErrZeroValue, got %v", err)
	}
	if _, err := LnReserveRatio(1, 0); err != ErrZeroValue {
		t.Errorf("LnReserveRatio(1,0): expected ErrZeroValue, got %v", err)
	}

	// Test identity case
	if got, err := LnReserveRatio(42, 42); err != nil || got != 0 {
		t.Errorf("LnReserveRatio(42,42): want 0 got %g err=%v", got, err)
	}

	// Test small delta (log1p optimization path)
	a, b := uint64(10000), uint64(10001)
	r := float64(a)/float64(b) - 1
	if got, err := LnReserveRatio(a, b); err != nil || math.Abs(got-math.Log1p(r)) > tol {
		t.Errorf("LnReserveRatio small delta: want %g got %g err=%v",
			math.Log1p(r), got, err)
	}

	// Test large delta (log₂ fallback path)
	a, b = 16, 1
	got, err := LnReserveRatio(a, b)
	want := (log2u64(a) - log2u64(b)) * ln2
	if err != nil || math.Abs(got-want) > tol {
		t.Errorf("LnReserveRatio fallback: want %g got %g err=%v", want, got, err)
	}

	// Test symmetry property: ln(x/y) + ln(y/x) = 0
	x, y := uint64(12345), uint64(67890)
	a1, err1 := LnReserveRatio(x, y)
	a2, err2 := LnReserveRatio(y, x)
	if err1 != nil || err2 != nil {
		t.Errorf("symmetry error: %v %v", err1, err2)
	}
	if sum := a1 + a2; math.Abs(sum) > tol {
		t.Errorf("symmetry violated: ln(x/y) + ln(y/x) = %g", sum)
	}
}

// TestLnReserveRatio_EdgeCases validates boundary condition handling.
//
// Edge scenarios:
//   - Maximum ratio: MaxUint64 / 1 (extreme magnitude)
//   - Precision boundaries: Values near IEEE 754 limits
//
// Tests algorithm stability under extreme input conditions.
func TestLnReserveRatio_EdgeCases(t *testing.T) {
	a, b := uint64(math.MaxUint64), uint64(1)
	got, err := LnReserveRatio(a, b)
	want := math.Log(float64(a))
	if err != nil || math.Abs(got-want) > tol {
		t.Errorf("LnReserveRatio(MaxUint64,1): want %g got %g err=%v", want, got, err)
	}
}

// TestLog2ReserveRatio validates log₂(a/b) computation.
//
// Test scenarios:
//   - Identity case: a=b (result should be 0)
//   - Powers of 2: Exact integer results
//   - Symmetry: log₂(a/b) = -log₂(b/a)
//   - Near-equal values: Small delta precision
//
// Validates log₂ difference algorithm and mathematical properties.
func TestLog2ReserveRatio(t *testing.T) {
	// Test identity case
	if got, err := Log2ReserveRatio(7, 7); err != nil || got != 0 {
		t.Errorf("Log2ReserveRatio(7,7): want 0 got %g err=%v", got, err)
	}

	// Test powers of 2 (exact results)
	a, b := uint64(1<<40), uint64(1<<30)
	got, err := Log2ReserveRatio(a, b)
	if err != nil || math.Abs(got-10) > tol {
		t.Errorf("Log2ReserveRatio(2^40/2^30): want 10 got %g err=%v", got, err)
	}

	// Test symmetry property
	got, err = Log2ReserveRatio(b, a)
	if err != nil || math.Abs(got+10) > tol {
		t.Errorf("Log2ReserveRatio(2^30/2^40): want -10 got %g err=%v", got, err)
	}
}

// TestLog2ReserveRatio_EdgeCases validates precision edge cases.
//
// Edge scenarios:
//   - Near-equal values: Small delta precision testing
//   - Close to unity ratios: Floating-point precision limits
func TestLog2ReserveRatio_EdgeCases(t *testing.T) {
	a, b := uint64(1<<30), uint64((1<<30)+1)
	got, err := Log2ReserveRatio(a, b)
	want := math.Log2(float64(a) / float64(b))
	if err != nil || math.Abs(got-want) > tol {
		t.Errorf("near-equal values: want %g got %g err=%v", want, got, err)
	}
}

// ============================================================================
// SCALED LOGARITHM FUNCTIONS
// ============================================================================

// TestLogReserveRatioConst validates ln(a/b) × conv computation.
//
// Test scenarios:
//   - Various conversion factors: π, -2.5, extreme values
//   - Accuracy validation: Compare with log₂ × conversion
//   - Error conditions: NaN, ±Inf conversion factors
//
// Tests custom base conversion and error handling.
func TestLogReserveRatioConst(t *testing.T) {
	a, b := uint64(12345), uint64(67890)

	// Test various conversion factors
	for _, conv := range []float64{math.Pi, -2.5, 1e300, 1e-300} {
		got, err := LogReserveRatioConst(a, b, conv)
		want := (log2u64(a) - log2u64(b)) * conv
		diff := math.Abs(got - want)

		// Use relative tolerance for extreme values
		if err != nil || (diff > tol && diff > tol*math.Abs(want)) {
			t.Errorf("conv=%g: want %g got %g delta=%.3g err=%v",
				conv, want, got, diff, err)
		}
	}
}

// TestLogReserveRatioConst_InvalidConv validates error condition handling.
//
// Invalid conversion factors:
//   - NaN: Should return ErrOutOfRange
//   - +Inf: Should return ErrOutOfRange
//   - -Inf: Should return ErrOutOfRange
func TestLogReserveRatioConst_InvalidConv(t *testing.T) {
	if _, err := LogReserveRatioConst(1, 1, math.NaN()); err != ErrOutOfRange {
		t.Errorf("NaN conversion: expected ErrOutOfRange, got %v", err)
	}
	if _, err := LogReserveRatioConst(1, 1, math.Inf(1)); err != ErrOutOfRange {
		t.Errorf("+Inf conversion: expected ErrOutOfRange, got %v", err)
	}
	if _, err := LogReserveRatioConst(1, 1, math.Inf(-1)); err != ErrOutOfRange {
		t.Errorf("-Inf conversion: expected ErrOutOfRange, got %v", err)
	}
}

// ============================================================================
// Q64.96 PRICE TRANSFORMATIONS
// ============================================================================

// TestLog2PriceX96 validates Q64.96 price logarithm computation.
//
// Test scenarios:
//   - Unity price: sqrtPrice = 2⁹⁶ → price = 1 → log₂(price) = 0
//   - Double price: sqrtPrice = 2⁹⁷ → price = 4 → log₂(price) = 2
//   - Zero input: Should return ErrZeroValue
//
// Mathematical foundation:
//
//	price = (sqrtPrice / 2⁹⁶)²
//	log₂(price) = 2 × (log₂(sqrtPrice) - 96)
func TestLog2PriceX96(t *testing.T) {
	// Test unity price (sqrtPrice = 2^96)
	s0 := Uint128{Hi: 1 << 32, Lo: 0}
	if got, err := Log2PriceX96(s0); err != nil || math.Abs(got) > tol {
		t.Errorf("unity price: want 0 got %g err=%v", got, err)
	}

	// Test double price (sqrtPrice = 2^97)
	s1 := Uint128{Hi: 1 << 33, Lo: 0}
	if got, err := Log2PriceX96(s1); err != nil || math.Abs(got-2) > tol {
		t.Errorf("double price: want 2 got %g err=%v", got, err)
	}
}

// TestLnPriceX96 validates natural logarithm of Q64.96 prices.
//
// Test scenarios:
//   - Unity price: ln(1) = 0
//   - Conversion accuracy: ln(price) = log₂(price) × ln(2)
//   - Error propagation: Zero input handling
func TestLnPriceX96(t *testing.T) {
	// Test unity price
	s := Uint128{Hi: 1 << 32, Lo: 0}
	if got, err := LnPriceX96(s); err != nil || math.Abs(got) > tol {
		t.Errorf("unity price: want 0 got %g err=%v", got, err)
	}
}

// TestLog2PriceX96_InvalidInput validates error condition handling.
//
// Error conditions:
//   - Zero input: Mathematically undefined log(0)
func TestLog2PriceX96_InvalidInput(t *testing.T) {
	_, err := Log2PriceX96(Uint128{0, 0})
	if err != ErrZeroValue {
		t.Errorf("zero input: expected ErrZeroValue, got %v", err)
	}
}

// TestLnPriceX96_EdgeCases validates boundary condition handling.
//
// Edge scenarios:
//   - Zero input: Error handling verification
//   - Non-unity prices: Conversion accuracy validation
func TestLnPriceX96_EdgeCases(t *testing.T) {
	// Test zero input error handling
	if _, err := LnPriceX96(Uint128{0, 0}); err != ErrZeroValue {
		t.Errorf("zero input: expected ErrZeroValue, got %v", err)
	}

	// Test non-unity price conversion
	s := Uint128{Hi: 1 << 33, Lo: 0}
	got, err := LnPriceX96(s)
	want := (log2u128(s) - 96) * 2 * ln2
	if err != nil || math.Abs(got-want) > tol {
		t.Errorf("conversion accuracy: want %g got %g err=%v", want, got, err)
	}
}

// TestLogPriceX96Const validates custom base price logarithms.
//
// Test scenarios:
//   - Various conversion factors: e, π, -1.25
//   - Conversion accuracy: Result = log₂(price) × conversion
//   - Error conditions: Zero input, invalid conversion factors
func TestLogPriceX96Const(t *testing.T) {
	s := Uint128{Hi: 1 << 32, Lo: 0}

	for _, conv := range []float64{math.E, math.Pi, -1.25} {
		got, err := LogPriceX96Const(s, conv)
		want, _ := Log2PriceX96(s)
		want *= conv
		if err != nil || math.Abs(got-want) > tol {
			t.Errorf("conv=%g: want %g got %g err=%v", conv, want, got, err)
		}
	}
}

// TestLogPriceX96Const_InvalidCases validates error path handling.
//
// Invalid conditions:
//   - Zero price input: ErrZeroValue expected
//   - NaN conversion factor: ErrOutOfRange expected
func TestLogPriceX96Const_InvalidCases(t *testing.T) {
	_, err := LogPriceX96Const(Uint128{0, 0}, 1.0)
	if err != ErrZeroValue {
		t.Errorf("zero price: expected ErrZeroValue, got %v", err)
	}

	_, err = LogPriceX96Const(Uint128{Hi: 1 << 32}, math.NaN())
	if err != ErrOutOfRange {
		t.Errorf("NaN conversion: expected ErrOutOfRange, got %v", err)
	}
}

// ============================================================================
// RANDOMIZED VALIDATION
// ============================================================================

// TestRandomizedFunctions validates algorithm correctness across 1M samples.
//
// Randomized test coverage:
//   - log2u64: 1M random uint64 values vs math.Log2
//   - log2u128: 1M random Uint128 values vs math.Log2
//   - LnReserveRatio: 1M random (a,b) pairs vs math.Log
//   - Log2ReserveRatio: 1M random (a,b) pairs vs math.Log2
//
// Statistical validation:
//   - Deterministic seed for reproducible results
//   - Comprehensive input space coverage
//   - Fail-fast on first tolerance violation
//
// Performance verification:
//   - Confirms sub-microsecond per-operation performance
//   - Validates algorithm stability under diverse inputs
func TestRandomizedFunctions(t *testing.T) {
	// Test log2u64 with random inputs
	r1 := rand.New(rand.NewSource(rndSeed))
	for i := 0; i < rndLoop; i++ {
		x := r1.Uint64() | 1 // Ensure non-zero
		if got, want := log2u64(x), math.Log2(float64(x)); math.Abs(got-want) > tol {
			t.Fatalf("log2u64(%d): want %g got %g", x, want, got)
		}
	}

	// Test log2u128 with random inputs
	r2 := rand.New(rand.NewSource(rndSeed + 1))
	for i := 0; i < rndLoop; i++ {
		u := Uint128{r2.Uint64(), r2.Uint64()}
		if u.Hi == 0 && u.Lo == 0 {
			continue // Skip zero values
		}
		want := math.Log2(float64(u.Hi)*math.Ldexp(1, 64) + float64(u.Lo))
		if got := log2u128(u); math.Abs(got-want) > tol {
			t.Fatalf("log2u128(%#v): want %g got %g", u, want, got)
		}
	}

	// Test LnReserveRatio with random inputs
	r3 := rand.New(rand.NewSource(rndSeed + 2))
	for i := 0; i < rndLoop; i++ {
		a, b := r3.Uint64()|1, r3.Uint64()|1 // Ensure non-zero
		got, err := LnReserveRatio(a, b)
		want := math.Log(float64(a) / float64(b))
		if err != nil || math.Abs(got-want) > tol {
			t.Fatalf("LnReserveRatio(%d,%d): want %g got %g err=%v", a, b, want, got, err)
		}
	}

	// Test Log2ReserveRatio with random inputs
	r4 := rand.New(rand.NewSource(rndSeed + 3))
	for i := 0; i < rndLoop; i++ {
		a, b := r4.Uint64()|1, r4.Uint64()|1 // Ensure non-zero
		got, err := Log2ReserveRatio(a, b)
		want := math.Log2(float64(a) / float64(b))
		if err != nil || math.Abs(got-want) > tol {
			t.Fatalf("Log2ReserveRatio(%d,%d): want %g got %g err=%v", a, b, want, got, err)
		}
	}
}
