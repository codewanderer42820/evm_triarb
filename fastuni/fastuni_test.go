// fastuni_test.go — unit tests for fastuni high-performance logarithmic routines.
//
// These tests validate correctness of both internal and exported functions.
// Randomized and edge-case tests are used to ensure mathematical accuracy and API behavior.

package fastuni

import (
	"math"
	"math/rand"
	"testing"
)

/*─────────────────── constants ───────────────────*/

const (
	tol     = 5e-5      // Acceptable relative/absolute tolerance for float comparisons
	rndSeed = 69        // Seed for deterministic randomized tests
	rndLoop = 1_000_000 // Number of random samples per test
)

/*─────────────────── log₂(u64) tests ───────────────────*/

func TestLog2u64(t *testing.T) {
	// Validates that log2u64 returns exact log₂(x) for powers of two
	for k := uint(0); k < 64; k++ {
		x := uint64(1) << k
		if got, want := log2u64(x), float64(k); got != want {
			t.Errorf("log2u64(%d): want %g got %g", x, want, got)
		}
	}

	// Validates approximate accuracy for non-powers
	tests := []uint64{3, 5, 123456789, (1 << 52) + 12345}
	for _, x := range tests {
		got := log2u64(x)
		want := math.Log2(float64(x))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u64(%d): want approx %g got %g", x, want, got)
		}
	}
}

/*─────────────────── log₂(u128) tests ───────────────────*/

func TestLog2u128(t *testing.T) {
	// High = 0 → behaves like log2u64
	for _, lo := range []uint64{1, 2, 3, 1234567890, 1 << 52} {
		u := Uint128{Hi: 0, Lo: lo}
		got := log2u128(u)
		want := math.Log2(float64(lo))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u128(%#v): want %g got %g", u, want, got)
		}
	}

	// High > 0 → tests 128-bit range
	for _, u := range []Uint128{{1, 0}, {1 << 10, 1234567890}} {
		got := log2u128(u)
		want := math.Log2(float64(u.Hi)*math.Ldexp(1, 64) + float64(u.Lo))
		if math.Abs(got-want) > tol {
			t.Errorf("log2u128(%#v): want %g got %g", u, want, got)
		}
	}
}

/*─────────────────── ln reserve ratio tests ───────────────────*/

func TestLnReserveRatio(t *testing.T) {
	// Zero numerator or denominator should trigger an error
	if got, err := LnReserveRatio(0, 1); err != ErrZeroValue {
		t.Errorf("LnReserveRatio(0,1): expected error, got %g err=%v", got, err)
	}
	if got, err := LnReserveRatio(1, 0); err != ErrZeroValue {
		t.Errorf("LnReserveRatio(1,0): expected error, got %g err=%v", got, err)
	}

	// Equal inputs → ln(1) = 0
	if got, err := LnReserveRatio(42, 42); err != nil || got != 0 {
		t.Errorf("LnReserveRatio(42,42): expected 0, got %g err=%v", got, err)
	}

	// Tiny delta — should use Log1p path
	a, b := uint64(10000), uint64(10001)
	r := float64(a)/float64(b) - 1
	if got, err := LnReserveRatio(a, b); err != nil || math.Abs(got-math.Log1p(r)) > tol {
		t.Errorf("LnReserveRatio small delta: expected %g got %g err=%v", math.Log1p(r), got, err)
	}

	// Large delta — should use log2-based fallback
	a, b = 16, 1
	got, err := LnReserveRatio(a, b)
	want := (log2u64(a) - log2u64(b)) * ln2
	if err != nil || math.Abs(got-want) > tol {
		t.Errorf("LnReserveRatio fallback: expected %g got %g err=%v", want, got, err)
	}

	// Symmetry: ln(x/y) + ln(y/x) ≈ 0
	x, y := uint64(12345), uint64(67890)
	a1, err1 := LnReserveRatio(x, y)
	a2, err2 := LnReserveRatio(y, x)
	if err1 != nil || err2 != nil {
		t.Errorf("LnReserveRatio symmetry: errors encountered: %v, %v", err1, err2)
	}
	if sum := a1 + a2; math.Abs(sum) > tol {
		t.Errorf("LnReserveRatio symmetry violated: sum = %g", sum)
	}
}

/*─────────────────── log₂ reserve ratio tests ───────────────────*/

func TestLog2ReserveRatio(t *testing.T) {
	// Equal inputs → log₂(1) = 0
	if got, err := Log2ReserveRatio(7, 7); err != nil || got != 0 {
		t.Errorf("Log2ReserveRatio(7,7): expected 0, got %g err=%v", got, err)
	}

	// Direct check: 2^40 / 2^30 = 2^10
	a, b := uint64(1<<40), uint64(1<<30)
	got, err := Log2ReserveRatio(a, b)
	if err != nil || math.Abs(got-10) > tol {
		t.Errorf("Log2ReserveRatio ratio: expected 10, got %g err=%v", got, err)
	}

	// Inversion: result should negate
	got, err = Log2ReserveRatio(b, a)
	if err != nil || math.Abs(got+10) > tol {
		t.Errorf("Log2ReserveRatio inversion: expected -10, got %g err=%v", got, err)
	}
}

/*─────────────────── log ratio with constant multiplier ───────────────────*/

func TestLogReserveRatioConst(t *testing.T) {
	a, b := uint64(12345), uint64(67890)
	for _, conv := range []float64{math.Pi, -2.5, 1e300, 1e-300} {
		got, err := LogReserveRatioConst(a, b, conv)
		want := (log2u64(a) - log2u64(b)) * conv
		diff := math.Abs(got - want)
		tolAbs := tol
		tolRel := tol * math.Abs(want)
		if err != nil || (diff > tolAbs && diff > tolRel) {
			t.Errorf("LogReserveRatioConst conv=%g: expected %g got %g diff=%g err=%v", conv, want, got, diff, err)
		}
	}
}

/*─────────────────── log₂ price (Q64.96) tests ───────────────────*/

func TestLog2PriceX96(t *testing.T) {
	s0 := Uint128{Hi: 1 << 32, Lo: 0} // 2^96 → log₂(price)=0
	if got, err := Log2PriceX96(s0); err != nil || math.Abs(got) > tol {
		t.Errorf("Log2PriceX96(2^96): expected 0, got %g err=%v", got, err)
	}
	s1 := Uint128{Hi: 1 << 33, Lo: 0} // 2^97 → log₂(price)=2
	if got, err := Log2PriceX96(s1); err != nil || math.Abs(got-2) > tol {
		t.Errorf("Log2PriceX96(2^97): expected 2, got %g err=%v", got, err)
	}
}

/*─────────────────── natural log price (Q64.96) tests ───────────────────*/

func TestLnPriceX96(t *testing.T) {
	s := Uint128{Hi: 1 << 32, Lo: 0} // ln(2^96) = 0
	if got, err := LnPriceX96(s); err != nil || math.Abs(got) > tol {
		t.Errorf("LnPriceX96 identity: expected 0, got %g err=%v", got, err)
	}
}

/*─────────────────── log price with constant multiplier ───────────────────*/

func TestLogPriceX96Const(t *testing.T) {
	s := Uint128{Hi: 1 << 32, Lo: 0}
	for _, conv := range []float64{math.E, math.Pi, -1.25} {
		got, err := LogPriceX96Const(s, conv)
		want, _ := Log2PriceX96(s)
		want *= conv
		if err != nil || math.Abs(got-want) > tol {
			t.Errorf("LogPriceX96Const conv=%g: expected %g got %g err=%v", conv, want, got, err)
		}
	}
}

/*─────────────────── randomized tests for all routines ───────────────────*/

func TestRandomizedFunctions(t *testing.T) {
	// log2u64 fuzz
	r1 := rand.New(rand.NewSource(rndSeed))
	for i := 0; i < rndLoop; i++ {
		x := r1.Uint64() | 1
		if got, want := log2u64(x), math.Log2(float64(x)); math.Abs(got-want) > tol {
			t.Fatalf("random log2u64(%d): expected %g got %g", x, want, got)
		}
	}

	// log2u128 fuzz
	r2 := rand.New(rand.NewSource(rndSeed + 1))
	for i := 0; i < rndLoop; i++ {
		u := Uint128{Hi: r2.Uint64(), Lo: r2.Uint64()}
		if u.Hi == 0 && u.Lo == 0 {
			continue // skip zero
		}
		want := math.Log2(float64(u.Hi)*math.Ldexp(1, 64) + float64(u.Lo))
		got := log2u128(u)
		if math.Abs(got-want) > tol {
			t.Fatalf("random log2u128(%#v): expected %g got %g", u, want, got)
		}
	}

	// LnReserveRatio fuzz
	r3 := rand.New(rand.NewSource(rndSeed + 2))
	for i := 0; i < rndLoop; i++ {
		a, b := r3.Uint64()|1, r3.Uint64()|1
		got, err := LnReserveRatio(a, b)
		want := math.Log(float64(a) / float64(b))
		if err != nil || math.Abs(got-want) > tol {
			t.Fatalf("random LnReserveRatio(%d,%d): expected %g got %g err=%v", a, b, want, got, err)
		}
	}

	// Log2ReserveRatio fuzz
	r4 := rand.New(rand.NewSource(rndSeed + 3))
	for i := 0; i < rndLoop; i++ {
		a, b := r4.Uint64()|1, r4.Uint64()|1
		got, err := Log2ReserveRatio(a, b)
		want := math.Log2(float64(a) / float64(b))
		if err != nil || math.Abs(got-want) > tol {
			t.Fatalf("random Log2ReserveRatio(%d,%d): expected %g got %g err=%v", a, b, want, got, err)
		}
	}
}
