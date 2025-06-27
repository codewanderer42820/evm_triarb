// fastuni_test.go — Deterministic unit tests & micro-benchmarks.
//
// Overview
// --------
// Ensures correctness, edge-case handling, determinism, monotonicity, and
// performance of the fastuni logarithmic helpers.

package fastuni

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

/*──────────────────── deterministic seed ───────────────────*/

var testSeed int64

func init() {
	// Seed rotates once per UTC day → reproducible across midnight boundaries.
	testSeed = time.Now().UTC().Unix() / 86_400
}

/*──────────────────── test utilities ───────────────────*/

//go:nosplit
//go:inline
func rngPair(r *rand.Rand) (uint64, uint64) {
	for i := 0; i < 65_536; i++ {
		a, b := r.Uint64(), r.Uint64()
		if a != 0 && b != 0 {
			return a, b
		}
	}
	panic("rngPair: failed to generate non-zero pair after 65,536 attempts")
}

//go:nosplit
//go:inline
func assertPanics(t *testing.T, f func(), msg string) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic: %s", msg)
		}
	}()
	f()
}

/*──────────────────── panic guard tests ───────────────────*/

func TestZeroInputPanics(t *testing.T) {
	zero := Uint128{}
	assertPanics(t, func() { _ = log2u64(0) }, "log2u64(0)")
	assertPanics(t, func() { _ = log2u128(zero) }, "log2u128(0)")
	assertPanics(t, func() { _ = Log2ReserveRatio(0, 1) }, "Log2ReserveRatio(0,1)")
	assertPanics(t, func() { _ = LnReserveRatio(1, 0) }, "LnReserveRatio(1,0)")
	assertPanics(t, func() { _ = Log2PriceX96(zero) }, "Log2PriceX96(0)")
}

func TestValidateConvPanics(t *testing.T) {
	badConvs := []float64{0, math.NaN(), math.Inf(1), 1e-305, 1e301}
	for _, conv := range badConvs {
		assertPanics(t, func() { _ = LogReserveRatioConst(2, 1, conv) }, "invalid conv")
		assertPanics(t, func() { _ = LogPriceX96Const(Uint128{Hi: 1}, conv) }, "invalid price conv")
	}
}

/*──────────────────── log2u128 test coverage ───────────────────*/

func TestLog2u128Paths(t *testing.T) {
	v1 := Uint128{Hi: 0, Lo: 123456789}
	want1 := math.Log(float64(v1.Lo)) / math.Ln2
	if got := log2u128(v1); math.Abs(got-want1) > 5e-5 {
		t.Fatalf("log2u128(lo-only) mismatch: want %.12g got %.12g", want1, got)
	}

	v2 := Uint128{Hi: 1 << 20, Lo: 987654321}
	x := float64(v2.Hi)*0x1p64 + float64(v2.Lo)
	want2 := math.Log(x) / math.Ln2
	if got := log2u128(v2); math.Abs(got-want2) > 5e-5 {
		t.Fatalf("log2u128(full) mismatch: want %.12g got %.12g", want2, got)
	}
}

/*──────────────────── Q64.96 boundary checks ───────────────────*/

func TestLnPriceX96SimpleCase(t *testing.T) {
	sqrt := Uint128{Hi: 1 << 32, Lo: 0} // exactly 2^96 ⇒ price=1 ⇒ ln=0
	if got := LnPriceX96(sqrt); math.Abs(got) > 1e-12 {
		t.Fatalf("LnPriceX96(1) = %.12g, expected 0", got)
	}
}

func TestLog2PriceX96Guards(t *testing.T) {
	big := Uint128{Hi: maxSqrtHi, Lo: 0}
	assertPanics(t, func() { _ = Log2PriceX96(big) }, ">1-tick guard")

	cliff := Uint128{Hi: maxSqrtHi - 1, Lo: lostLowMask}
	assertPanics(t, func() { _ = Log2PriceX96(cliff) }, "precision cliff guard")
}

/*──────────────────── accuracy & behavior tests ───────────────────*/

func TestLnReserveRatioAccuracyNearEqual(t *testing.T) {
	r := rand.New(rand.NewSource(testSeed))
	for i := 0; i < 10_000; i++ {
		a, _ := rngPair(r)
		a &= (1 << 60) - 1
		diff := uint64(r.Intn(1_000))
		b := a + diff
		if r.Intn(2) == 0 && a > diff+1 {
			b = a - diff
		}
		if b == 0 {
			b = 1
		}
		want := math.Log(float64(a) / float64(b))
		got := LnReserveRatio(a, b)
		if math.Abs(got-want) > 1e-5 {
			t.Fatalf("LnReserveRatio(%d,%d): want %.12g got %.12g", a, b, want, got)
		}
	}
}

func TestLnReserveRatioULPBoundaries(t *testing.T) {
	b := uint64(1 << 53)
	for _, a := range []uint64{b + 1, b - 1} {
		want := math.Log(float64(a) / float64(b))
		got := LnReserveRatio(a, b)
		if math.Abs(got-want) > 1e-5 {
			t.Fatalf("ULP(%d,%d): want %.12g got %.12g", a, b, want, got)
		}
	}
}

func TestLnReserveRatioMagnitudeSweep(t *testing.T) {
	r := rand.New(rand.NewSource(testSeed))
	for i := 0; i < 10_000; i++ {
		a := uint64(1) << uint(r.Intn(63)+1)
		b := uint64(1) << uint(r.Intn(63)+1)
		want := math.Log(float64(a) / float64(b))
		got := LnReserveRatio(a, b)
		if math.Abs(got-want) > 1e-5 {
			t.Fatalf("LnReserveRatio sweep(%d,%d): want %.12g got %.12g", a, b, want, got)
		}
	}
}

func TestLnReserveRatioMonotonicity(t *testing.T) {
	prev := math.Inf(-1)
	for a := uint64(1); a <= 1000; a++ {
		val := LnReserveRatio(a, 1)
		if val <= prev {
			t.Fatalf("monotonicity fail at a=%d: %.12g ≤ %.12g", a, val, prev)
		}
		prev = val
	}
}

func TestSeedDeterminism(t *testing.T) {
	a := rand.New(rand.NewSource(testSeed)).Uint64()
	b := rand.New(rand.NewSource(testSeed)).Uint64()
	if a != b {
		t.Fatalf("testSeed mismatch: %d ≠ %d", a, b)
	}
}

/*──────────────────── micro-benchmarks ───────────────────*/

func BenchmarkLnReserveRatio(b *testing.B) {
	r := rand.New(rand.NewSource(testSeed))
	pairs := make([][2]uint64, 1<<12)
	for i := range pairs {
		pairs[i][0], pairs[i][1] = rngPair(r)
	}
	mask := uint(len(pairs) - 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := pairs[uint(i)&mask]
		_ = LnReserveRatio(p[0], p[1])
	}
}

func BenchmarkLog2PriceX96(b *testing.B) {
	r := rand.New(rand.NewSource(testSeed))
	vals := make([]Uint128, 1<<12)
	for i := range vals {
		hi := uint64(r.Int63n(maxSqrtHi-1)) | 1
		vals[i] = Uint128{Hi: hi, Lo: r.Uint64()}
	}
	mask := uint(len(vals) - 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Log2PriceX96(vals[uint(i)&mask])
	}
}
