// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: fastuni_test.go — unit tests for fastuni logarithmic utilities
//
// Purpose:
//   - Validates correctness of ISR-grade fixed-point log₂/ln implementations
//   - Covers core behavior, symmetry, fuzzing, edge cases, and invalid input
//
// Test Scope:
//   - log₂(x) over u64 and Uint128
//   - ln(a/b), log₂(a/b), log(a/b)*const
//   - log₂ and ln over Q64.96 fixed-point prices
//
// Modes:
//   - Exact constants, symmetry checks
//   - Invalid inputs (zero, NaN, Inf)
//   - MaxUint64 + rounding edge cases
//   - 1M randomized samples per function
//
// ─────────────────────────────────────────────────────────────────────────────

package fastuni

import (
	"math"
	"math/rand"
	"testing"
)

/*──────────────────────────────────────────────────────────────────────────────
  📦 Constants
──────────────────────────────────────────────────────────────────────────────*/

const (
	tol     = 5e-5
	rndSeed = 69
	rndLoop = 1_000_000
)

/*──────────────────────────────────────────────────────────────────────────────
  🔬 log₂(x) over uint64
──────────────────────────────────────────────────────────────────────────────*/

func TestLog2u64(t *testing.T) {
	for k := uint(0); k < 64; k++ {
		x := uint64(1) << k
		if got := log2u64(x); got != float64(k) {
			t.Errorf("log2u64(%d): want %g got %g", x, float64(k), got)
		}
	}

	tests := []uint64{3, 5, 123456789, (1 << 52) + 12345}
	for _, x := range tests {
		got := log2u64(x)
		want := math.Log2(float64(x))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u64(%d): want %g got %g", x, want, got)
		}
	}
}

func TestLog2u64_EdgeCases(t *testing.T) {
	x := uint64(1 << 52)
	if got := log2u64(x); math.Abs(got-52) > tol {
		t.Errorf("log2u64(2^52): want 52, got %g", got)
	}
	if got := log2u64(x + 1); math.Abs(got-math.Log2(float64(x+1))) > tol {
		t.Errorf("log2u64(2^52+1): want %g, got %g", math.Log2(float64(x+1)), got)
	}
	if got := log2u64(math.MaxUint64); math.Abs(got-math.Log2(float64(math.MaxUint64))) > tol {
		t.Errorf("log2u64(MaxUint64): want %g, got %g", math.Log2(float64(math.MaxUint64)), got)
	}
}

/*──────────────────────────────────────────────────────────────────────────────
  🔬 log₂(x) over Uint128
──────────────────────────────────────────────────────────────────────────────*/

func TestLog2u128(t *testing.T) {
	for _, lo := range []uint64{1, 2, 3, 1234567890, 1 << 52} {
		u := Uint128{0, lo}
		got := log2u128(u)
		want := math.Log2(float64(lo))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u128(%#v): want %g got %g", u, want, got)
		}
	}

	for _, u := range []Uint128{{1, 0}, {1 << 10, 1234567890}} {
		got := log2u128(u)
		want := math.Log2(float64(u.Hi)*math.Ldexp(1, 64) + float64(u.Lo))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u128(%#v): want %g got %g", u, want, got)
		}
	}
}

func TestLog2u128_EdgeCases(t *testing.T) {
	u := Uint128{0, math.MaxUint64}
	got := log2u128(u)
	want := math.Log2(float64(u.Lo))
	if math.Abs(got-want) > tol {
		t.Errorf("log2u128(Lo=Max): want %g got %g", want, got)
	}

	u = Uint128{1, math.MaxUint64}
	want = math.Log2(float64(u.Hi)*math.Ldexp(1, 64) + float64(u.Lo))
	got = log2u128(u)
	if math.Abs(got-want) > tol {
		t.Errorf("log2u128(Hi=1,Lo=Max): want %g got %g", want, got)
	}
}

/*──────────────────────────────────────────────────────────────────────────────
  🔬 LnReserveRatio
──────────────────────────────────────────────────────────────────────────────*/

func TestLnReserveRatio(t *testing.T) {
	if _, err := LnReserveRatio(0, 1); err != ErrZeroValue {
		t.Errorf("LnReserveRatio(0,1): expected ErrZeroValue, got %v", err)
	}
	if _, err := LnReserveRatio(1, 0); err != ErrZeroValue {
		t.Errorf("LnReserveRatio(1,0): expected ErrZeroValue, got %v", err)
	}
	if got, err := LnReserveRatio(42, 42); err != nil || got != 0 {
		t.Errorf("LnReserveRatio(42,42): want 0 got %g err=%v", got, err)
	}

	a, b := uint64(10000), uint64(10001)
	r := float64(a)/float64(b) - 1
	if got, err := LnReserveRatio(a, b); err != nil || math.Abs(got-math.Log1p(r)) > tol {
		t.Errorf("LnReserveRatio small delta: want %g got %g err=%v", math.Log1p(r), got, err)
	}

	a, b = 16, 1
	got, err := LnReserveRatio(a, b)
	want := (log2u64(a) - log2u64(b)) * ln2
	if err != nil || math.Abs(got-want) > tol {
		t.Errorf("LnReserveRatio fallback: want %g got %g err=%v", want, got, err)
	}

	x, y := uint64(12345), uint64(67890)
	a1, err1 := LnReserveRatio(x, y)
	a2, err2 := LnReserveRatio(y, x)
	if err1 != nil || err2 != nil {
		t.Errorf("Symmetry error: %v %v", err1, err2)
	}
	if sum := a1 + a2; math.Abs(sum) > tol {
		t.Errorf("Symmetry violated: ln(x/y) + ln(y/x) = %g", sum)
	}
}

func TestLnReserveRatio_EdgeCases(t *testing.T) {
	a, b := uint64(math.MaxUint64), uint64(1)
	got, err := LnReserveRatio(a, b)
	want := math.Log(float64(a))
	if err != nil || math.Abs(got-want) > tol {
		t.Errorf("LnReserveRatio(Max,1): want %g got %g err=%v", want, got, err)
	}
}

/*──────────────────────────────────────────────────────────────────────────────
  🔬 Log2ReserveRatio
──────────────────────────────────────────────────────────────────────────────*/

func TestLog2ReserveRatio(t *testing.T) {
	if got, err := Log2ReserveRatio(7, 7); err != nil || got != 0 {
		t.Errorf("Log2ReserveRatio(7,7): want 0 got %g err=%v", got, err)
	}

	a, b := uint64(1<<40), uint64(1<<30)
	got, err := Log2ReserveRatio(a, b)
	if err != nil || math.Abs(got-10) > tol {
		t.Errorf("Log2ReserveRatio(2^40/2^30): want 10 got %g err=%v", got, err)
	}

	got, err = Log2ReserveRatio(b, a)
	if err != nil || math.Abs(got+10) > tol {
		t.Errorf("Log2ReserveRatio(2^30/2^40): want -10 got %g err=%v", got, err)
	}
}

func TestLog2ReserveRatio_EdgeCases(t *testing.T) {
	a, b := uint64(1<<30), uint64((1<<30)+1)
	got, err := Log2ReserveRatio(a, b)
	want := math.Log2(float64(a) / float64(b))
	if err != nil || math.Abs(got-want) > tol {
		t.Errorf("Near-equal: want %g got %g err=%v", want, got, err)
	}
}

/*──────────────────────────────────────────────────────────────────────────────
  🔬 LogReserveRatioConst
──────────────────────────────────────────────────────────────────────────────*/

func TestLogReserveRatioConst(t *testing.T) {
	a, b := uint64(12345), uint64(67890)
	for _, conv := range []float64{math.Pi, -2.5, 1e300, 1e-300} {
		got, err := LogReserveRatioConst(a, b, conv)
		want := (log2u64(a) - log2u64(b)) * conv
		diff := math.Abs(got - want)
		if err != nil || (diff > tol && diff > tol*math.Abs(want)) {
			t.Errorf("conv=%g: want %g got %g Δ=%.3g err=%v", conv, want, got, diff, err)
		}
	}
}

func TestLogReserveRatioConst_InvalidConv(t *testing.T) {
	if _, err := LogReserveRatioConst(1, 1, math.NaN()); err != ErrOutOfRange {
		t.Errorf("NaN conv: expected ErrOutOfRange, got %v", err)
	}
	if _, err := LogReserveRatioConst(1, 1, math.Inf(1)); err != ErrOutOfRange {
		t.Errorf("+Inf conv: expected ErrOutOfRange, got %v", err)
	}
	if _, err := LogReserveRatioConst(1, 1, math.Inf(-1)); err != ErrOutOfRange {
		t.Errorf("-Inf conv: expected ErrOutOfRange, got %v", err)
	}
}

/*──────────────────────────────────────────────────────────────────────────────
  🔬 Log2PriceX96 / LnPriceX96 / *Const
──────────────────────────────────────────────────────────────────────────────*/

func TestLog2PriceX96(t *testing.T) {
	s0 := Uint128{Hi: 1 << 32, Lo: 0}
	if got, err := Log2PriceX96(s0); err != nil || math.Abs(got) > tol {
		t.Errorf("2^96: want 0 got %g err=%v", got, err)
	}
	s1 := Uint128{Hi: 1 << 33, Lo: 0}
	if got, err := Log2PriceX96(s1); err != nil || math.Abs(got-2) > tol {
		t.Errorf("2^97: want 2 got %g err=%v", got, err)
	}
}

func TestLnPriceX96(t *testing.T) {
	s := Uint128{Hi: 1 << 32, Lo: 0}
	if got, err := LnPriceX96(s); err != nil || math.Abs(got) > tol {
		t.Errorf("2^96: want 0 got %g err=%v", got, err)
	}
}

func TestLog2PriceX96_InvalidInput(t *testing.T) {
	_, err := Log2PriceX96(Uint128{0, 0})
	if err != ErrZeroValue {
		t.Errorf("zero: expected ErrZeroValue, got %v", err)
	}
}

func TestLnPriceX96_EdgeCases(t *testing.T) {
	if _, err := LnPriceX96(Uint128{0, 0}); err != ErrZeroValue {
		t.Errorf("zero: expected ErrZeroValue, got %v", err)
	}
	s := Uint128{Hi: 1 << 33, Lo: 0}
	got, err := LnPriceX96(s)
	want := (log2u128(s) - 96) * 2 * ln2
	if err != nil || math.Abs(got-want) > tol {
		t.Errorf("2^97: want %g got %g err=%v", want, got, err)
	}
}

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

func TestLogPriceX96Const_InvalidCases(t *testing.T) {
	_, err := LogPriceX96Const(Uint128{0, 0}, 1.0)
	if err != ErrZeroValue {
		t.Errorf("zero: expected ErrZeroValue, got %v", err)
	}
	_, err = LogPriceX96Const(Uint128{Hi: 1 << 32}, math.NaN())
	if err != ErrOutOfRange {
		t.Errorf("NaN: expected ErrOutOfRange, got %v", err)
	}
}

/*──────────────────────────────────────────────────────────────────────────────
  🎲 Randomized Tests (fuzzing)
──────────────────────────────────────────────────────────────────────────────*/

func TestRandomizedFunctions(t *testing.T) {
	r1 := rand.New(rand.NewSource(rndSeed))
	for i := 0; i < rndLoop; i++ {
		x := r1.Uint64() | 1
		if got, want := log2u64(x), math.Log2(float64(x)); math.Abs(got-want) > tol {
			t.Fatalf("log2u64(%d): want %g got %g", x, want, got)
		}
	}

	r2 := rand.New(rand.NewSource(rndSeed + 1))
	for i := 0; i < rndLoop; i++ {
		u := Uint128{r2.Uint64(), r2.Uint64()}
		if u.Hi == 0 && u.Lo == 0 {
			continue
		}
		want := math.Log2(float64(u.Hi)*math.Ldexp(1, 64) + float64(u.Lo))
		if got := log2u128(u); math.Abs(got-want) > tol {
			t.Fatalf("log2u128(%#v): want %g got %g", u, want, got)
		}
	}

	r3 := rand.New(rand.NewSource(rndSeed + 2))
	for i := 0; i < rndLoop; i++ {
		a, b := r3.Uint64()|1, r3.Uint64()|1
		got, err := LnReserveRatio(a, b)
		want := math.Log(float64(a) / float64(b))
		if err != nil || math.Abs(got-want) > tol {
			t.Fatalf("LnReserveRatio(%d,%d): want %g got %g err=%v", a, b, want, got, err)
		}
	}

	r4 := rand.New(rand.NewSource(rndSeed + 3))
	for i := 0; i < rndLoop; i++ {
		a, b := r4.Uint64()|1, r4.Uint64()|1
		got, err := Log2ReserveRatio(a, b)
		want := math.Log2(float64(a) / float64(b))
		if err != nil || math.Abs(got-want) > tol {
			t.Fatalf("Log2ReserveRatio(%d,%d): want %g got %g err=%v", a, b, want, got, err)
		}
	}
}
