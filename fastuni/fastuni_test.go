// fastuni_test.go — unit tests for fastuni footgun routines.
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
func TestLog2u64(t *testing.T) {
	// Do not call log2u64(0), it's explicitly unsupported
	// If you want safe behavior, use a wrapper with error checking

	for k := uint(0); k < 64; k++ {
		x := uint64(1) << k
		if got, want := log2u64(x), float64(k); got != want {
			t.Errorf("log2u64(%d): want %g got %g", x, want, got)
		}
	}

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
func TestLog2u128(t *testing.T) {
	if got := log2u128(Uint128{0, 0}); !math.IsInf(got, -1) {
		t.Fatalf("log2u128(0): want -Inf got %g", got)
	}
	for _, lo := range []uint64{1, 2, 3, 1234567890, 1 << 52} {
		u := Uint128{Hi: 0, Lo: lo}
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

/*─────────────────── ln reserve ratio tests ───────────────────*/
func TestLnReserveRatio(t *testing.T) {
	if got, err := LnReserveRatio(0, 1); err == nil || !math.IsInf(got, -1) {
		t.Errorf("LnReserveRatio(0,1): want -Inf got %g err=%v", got, err)
	}
	if got, err := LnReserveRatio(1, 0); err == nil || !math.IsInf(got, 1) {
		t.Errorf("LnReserveRatio(1,0): want +Inf got %g err=%v", got, err)
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
	a1, _ := LnReserveRatio(x, y)
	a2, _ := LnReserveRatio(y, x)
	if sum := a1 + a2; math.Abs(sum) > tol {
		t.Errorf("LnReserveRatio symmetry violated: sum=%g", sum)
	}
}

/*─────────────────── log₂ reserve ratio tests ───────────────────*/
func TestLog2ReserveRatio(t *testing.T) {
	if got, err := Log2ReserveRatio(7, 7); err != nil || got != 0 {
		t.Errorf("Log2ReserveRatio(7,7): want 0 got %g err=%v", got, err)
	}
	a, b := uint64(1<<40), uint64(1<<30)
	got, err := Log2ReserveRatio(a, b)
	if err != nil || math.Abs(got-10) > tol {
		t.Errorf("Log2ReserveRatio ratio: want 10 got %g err=%v", got, err)
	}
	got, err = Log2ReserveRatio(b, a)
	if err != nil || math.Abs(got+10) > tol {
		t.Errorf("Log2ReserveRatio inversion: want -10 got %g err=%v", got, err)
	}
}

/*─────────────────── ln reserve ratio const tests ───────────────────*/
func TestLogReserveRatioConst(t *testing.T) {
	a, b := uint64(12345), uint64(67890)
	for _, conv := range []float64{math.Pi, -2.5, 1e300, 1e-300} {
		got, err := LogReserveRatioConst(a, b, conv)
		want := (log2u64(a) - log2u64(b)) * conv
		diff := math.Abs(got - want)
		tolAbs := tol
		tolRel := tol * math.Abs(want)
		if err != nil || (diff > tolAbs && diff > tolRel) {
			t.Errorf("LogReserveRatioConst conv=%g: want %g got %g diff=%g err=%v", conv, want, got, diff, err)
		}
	}
}

/*─────────────────── log₂ price X96 tests ───────────────────*/
func TestLog2PriceX96(t *testing.T) {
	s0 := Uint128{Hi: 1 << 32, Lo: 0}
	if got, err := Log2PriceX96(s0); err != nil || math.Abs(got-0) > tol {
		t.Errorf("Log2PriceX96(2^96): want 0 got %g err=%v", got, err)
	}
	s1 := Uint128{Hi: 1 << 33, Lo: 0}
	if got, err := Log2PriceX96(s1); err != nil || math.Abs(got-2) > tol {
		t.Errorf("Log2PriceX96(2^97): want 2 got %g err=%v", got, err)
	}
}

/*─────────────────── ln price X96 tests ───────────────────*/
func TestLnPriceX96(t *testing.T) {
	s := Uint128{Hi: 1 << 32, Lo: 0}
	if got, err := LnPriceX96(s); err != nil || math.Abs(got-0) > tol {
		t.Errorf("LnPriceX96 identity: want 0 got %g err=%v", got, err)
	}
}

/*─────────────────── ln price X96 const tests ───────────────────*/
func TestLogPriceX96Const(t *testing.T) {
	s := Uint128{Hi: 1 << 32, Lo: 0}
	for _, conv := range []float64{math.E, math.Pi, -1.25} {
		got, err := LogPriceX96Const(s, conv)
		want, _ := Log2PriceX96(s)
		want *= conv
		if err != nil || math.Abs(got-want) > tol {
			t.Errorf("LogPriceX96Const conv=%g: want %g got %g err=%v", conv, want, got, err)
		}
	}
}

/*─────────────────── randomized tests ───────────────────*/
func TestRandomizedFunctions(t *testing.T) {
	r1 := rand.New(rand.NewSource(rndSeed))
	for i := 0; i < rndLoop; i++ {
		x := r1.Uint64() | 1
		if got, want := log2u64(x), math.Log2(float64(x)); math.Abs(got-want) > tol {
			t.Fatalf("random log2u64(%d): want %g got %g", x, want, got)
		}
	}
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
	r3 := rand.New(rand.NewSource(rndSeed + 2))
	for i := 0; i < rndLoop; i++ {
		a, b := r3.Uint64()|1, r3.Uint64()|1
		got, err := LnReserveRatio(a, b)
		want := math.Log(float64(a) / float64(b))
		if err != nil || math.Abs(got-want) > tol {
			t.Fatalf("random LnReserveRatio(%d,%d): want %g got %g err=%v", a, b, want, got, err)
		}
	}
	r4 := rand.New(rand.NewSource(rndSeed + 3))
	for i := 0; i < rndLoop; i++ {
		a, b := r4.Uint64()|1, r4.Uint64()|1
		got, err := Log2ReserveRatio(a, b)
		want := math.Log2(float64(a) / float64(b))
		if err != nil || math.Abs(got-want) > tol {
			t.Fatalf("random Log2ReserveRatio(%d,%d): want %g got %g err=%v", a, b, want, got, err)
		}
	}
}
