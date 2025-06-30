// fastuni_test.go — unit tests for fastuni footgun routines.
// -----------------------------------------------------------
// Validates log₂ implementations, reserve-ratio helpers, price conversions,
// and randomized correctness across core operations.
// Tolerance: |Δ| ≤ 5e-5 nat.

package fastuni

import (
	"math"
	"math/rand"
	"testing"
)

/*─────────────────── constants ───────────────────*/
const (
	tol     = 5e-5
	rndSeed = 69
	rndLoop = 1_000_000
)

/*─────────────────── log₂ (u64) tests ───────────────────*/
// TestLog2u64 checks log2u64 behavior:
// - returns -Inf for zero input,
// - exact results for powers of two,
// - approximate results within tolerance for other values.
func TestLog2u64(t *testing.T) {
	// Zero input => -Inf
	if got := log2u64(0); !math.IsInf(got, -1) {
		t.Fatalf("log2u64(0): want -Inf got %g", got)
	}

	// Powers of two: exact results
	for k := uint(0); k < 64; k++ {
		x := uint64(1) << k
		if got, want := log2u64(x), float64(k); got != want {
			t.Errorf("log2u64(%d): want %g got %g", x, want, got)
		}
	}

	// Non-powers: approximate
	tests := []uint64{3, 5, 123456789, (1<<52 + 12345)}
	for _, x := range tests {
		got := log2u64(x)
		want := math.Log2(float64(x))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u64(%d): want approx %g got %g", x, want, got)
		}
	}
}

/*─────────────────── log₂ (u128) tests ───────────────────*/
// TestLog2u128 verifies log2u128 for:
// - zero input returns -Inf,
// - 64-bit branch accuracy,
// - 128-bit branch against float-based log2.
func TestLog2u128(t *testing.T) {
	// Zero input => -Inf
	if got := log2u128(Uint128{0, 0}); !math.IsInf(got, -1) {
		t.Fatalf("log2u128(0): want -Inf got %g", got)
	}

	// Hi=0 branch
	for _, lo := range []uint64{1, 2, 3, 1234567890, 1 << 52} {
		u := Uint128{Hi: 0, Lo: lo}
		got := log2u128(u)
		want := math.Log2(float64(lo))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u128(%#v): want %g got %g", u, want, got)
		}
	}

	// Hi>0 branch
	for _, u := range []Uint128{{1, 0}, {1 << 10, 1234567890}} {
		got := log2u128(u)
		want := math.Log2(float64(u.Hi)*math.Ldexp(1, 64) + float64(u.Lo))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u128(%#v): want %g got %g", u, want, got)
		}
	}
}

/*─────────────────── ln reserve ratio tests ───────────────────*/
// TestLnReserveRatio examines LnReserveRatio, including:
// - infinite results for zero inputs,
// - zero result for equal inputs,
// - small delta branch using Log1p,
// - fallback branch via log2 conversion,
// - symmetry property.
func TestLnReserveRatio(t *testing.T) {
	// Zero numerator => -Inf
	if got := LnReserveRatio(0, 1); !math.IsInf(got, -1) {
		t.Errorf("LnReserveRatio(0,1): want -Inf got %g", got)
	}

	// Zero denominator => +Inf
	if got := LnReserveRatio(1, 0); !math.IsInf(got, 1) {
		t.Errorf("LnReserveRatio(1,0): want +Inf got %g", got)
	}

	// Equal inputs => 0
	if got := LnReserveRatio(42, 42); got != 0 {
		t.Errorf("LnReserveRatio(42,42): want 0 got %g", got)
	}

	// Small delta branch
	a, b := uint64(10000), uint64(10001)
	r := float64(a)/float64(b) - 1
	if got, want := LnReserveRatio(a, b), math.Log1p(r); math.Abs(got-want) > tol {
		t.Errorf("LnReserveRatio small delta: want %g got %g", want, got)
	}

	// Fallback branch
	a, b = 16, 1
	got2 := LnReserveRatio(a, b)
	want2 := (log2u64(a) - log2u64(b)) * ln2
	if math.Abs(got2-want2) > tol {
		t.Errorf("LnReserveRatio fallback: want %g got %g", want2, got2)
	}

	// Symmetry: Ln(a,b) + Ln(b,a) == 0
	x, y := uint64(12345), uint64(67890)
	if sum := LnReserveRatio(x, y) + LnReserveRatio(y, x); math.Abs(sum) > tol {
		t.Errorf("LnReserveRatio symmetry violated: sum=%g", sum)
	}
}

/*─────────────────── log₂ reserve ratio tests ───────────────────*/
// TestLog2ReserveRatio checks basic log2 ratio and inversion.
func TestLog2ReserveRatio(t *testing.T) {
	if got := Log2ReserveRatio(7, 7); got != 0 {
		t.Errorf("Log2ReserveRatio(7,7): want 0 got %g", got)
	}
	a, b := uint64(1<<40), uint64(1<<30)
	if got := Log2ReserveRatio(a, b); math.Abs(got-10) > tol {
		t.Errorf("Log2ReserveRatio ratio: want 10 got %g", got)
	}
	if got := Log2ReserveRatio(b, a); math.Abs(got+10) > tol {
		t.Errorf("Log2ReserveRatio inversion: want -10 got %g", got)
	}
}

/*─────────────────── ln reserve ratio const tests ───────────────────*/
// TestLogReserveRatioConst confirms scaling by conv without any validation.
func TestLogReserveRatioConst(t *testing.T) {
	a, b := uint64(12345), uint64(67890)
	for _, conv := range []float64{math.Pi, -2.5, 1e300, 1e-300} {
		got := LogReserveRatioConst(a, b, conv)
		want := (log2u64(a) - log2u64(b)) * conv
		// compare using both absolute and relative tolerance
		diff := math.Abs(got - want)
		tolAbs := tol
		tolRel := tol * math.Abs(want)
		if diff > tolAbs && diff > tolRel {
			t.Errorf("LogReserveRatioConst conv=%g: want %g got %g (diff=%g)", conv, want, got, diff)
		}
	}
}

/*─────────────────── log₂ price X96 tests ───────────────────*/
// TestLog2PriceX96 validates Log2PriceX96 for key sqrtPrice values:
// - 2^96 yields 0, 2^97 yields 2.
func TestLog2PriceX96(t *testing.T) {
	// sqrt=2^96 => 0
	s0 := Uint128{Hi: 1 << 32, Lo: 0}
	if got := Log2PriceX96(s0); math.Abs(got-0) > tol {
		t.Errorf("Log2PriceX96(2^96): want 0 got %g", got)
	}

	// sqrt=2^97 => 2
	s1 := Uint128{Hi: 1 << 33, Lo: 0}
	if got := Log2PriceX96(s1); math.Abs(got-2) > tol {
		t.Errorf("Log2PriceX96(2^97): want 2 got %g", got)
	}
}

/*─────────────────── ln price X96 tests ───────────────────*/
// TestLnPriceX96 ensures LnPriceX96 matches Log2PriceX96 * ln2.
func TestLnPriceX96(t *testing.T) {
	s := Uint128{Hi: 1 << 32, Lo: 0}
	if got := LnPriceX96(s); math.Abs(got-0) > tol {
		t.Errorf("LnPriceX96 identity: want 0 got %g", got)
	}
}

/*─────────────────── ln price X96 const tests ───────────────────*/
// TestLogPriceX96Const checks scaling of ln(price) without validation.
func TestLogPriceX96Const(t *testing.T) {
	s := Uint128{Hi: 1 << 32, Lo: 0}
	for _, conv := range []float64{math.E, math.Pi, -1.25} {
		got := LogPriceX96Const(s, conv)
		want := Log2PriceX96(s) * conv
		if math.Abs(got-want) > tol {
			t.Errorf("LogPriceX96Const conv=%g: want %g got %g", conv, want, got)
		}
	}
}

/*─────────────────── randomized tests ───────────────────*/
// TestRandomizedFunctions performs deterministic random checks for core operations.
func TestRandomizedFunctions(t *testing.T) {
	// Random log2u64
	r1 := rand.New(rand.NewSource(rndSeed))
	for i := 0; i < rndLoop; i++ {
		x := r1.Uint64() | 1
		if got, want := log2u64(x), math.Log2(float64(x)); math.Abs(got-want) > tol {
			t.Fatalf("random log2u64(%d): want %g got %g", x, want, got)
		}
	}

	// Random log2u128
	r2 := rand.New(rand.NewSource(rndSeed + 1))
	for i := 0; i < rndLoop; i++ {
		u := Uint128{Hi: r2.Uint64(), Lo: r2.Uint64()}
		if u.Hi == 0 && u.Lo == 0 {
			continue
		}
		if got, want := log2u128(u), math.Log2(float64(u.Hi)*math.Ldexp(1, 64)+float64(u.Lo)); math.Abs(got-want) > tol {
			t.Fatalf("random log2u128(%#v): want %g got %g", u, want, got)
		}
	}

	// Random LnReserveRatio
	r3 := rand.New(rand.NewSource(rndSeed + 2))
	for i := 0; i < rndLoop; i++ {
		a, b := r3.Uint64()|1, r3.Uint64()|1
		if got, want := LnReserveRatio(a, b), math.Log(float64(a)/float64(b)); math.Abs(got-want) > tol {
			t.Fatalf("random LnReserveRatio(%d,%d): want %g got %g", a, b, want, got)
		}
	}

	// Random Log2ReserveRatio
	r4 := rand.New(rand.NewSource(rndSeed + 3))
	for i := 0; i < rndLoop; i++ {
		a, b := r4.Uint64()|1, r4.Uint64()|1
		if got, want := Log2ReserveRatio(a, b), math.Log2(float64(a)/float64(b)); math.Abs(got-want) > tol {
			t.Fatalf("random Log2ReserveRatio(%d,%d): want %g got %g", a, b, want, got)
		}
	}
}
