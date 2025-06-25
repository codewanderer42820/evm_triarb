// fastuni_test.go — Deterministic unit tests & micro‑benchmarks.
//
// Synopsis
// --------
// Validates edge‑cases, panic guards, accuracy, monotonicity and provides
// micro‑benchmarks for the production helpers.
//
// Structure
// ---------
// • Daily deterministic seed → reproducible CI even across mid‑night runs.
// • Guard tests (expect panics on invalid input).
// • Accuracy sweeps (near‑equal, ULP boundaries, power‑of‑two magnitudes).
// • Monotonicity & determinism checks.
// • Benchmarks (`BenchmarkLnReserveRatio`, `BenchmarkLog2PriceX96`).
//
// Style
// -----
// Shares the same ASCII‑ruler section headers as fastuni.go for quick grepping.

package fastuni

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

/*────────────────── deterministic daily seed ──────────────────*/

var testSeed int64

func init() {
	// One seed per UTC day → reproducible CI runs even if they span midnight.
	testSeed = time.Now().UTC().Unix() / 86_400
}

/*────────────────── helpers ──────────────────*/

func rngPair(r *rand.Rand) (uint64, uint64) {
	for i := 0; i < 65_536; i++ {
		a := r.Uint64()
		b := r.Uint64()
		if a != 0 && b != 0 {
			return a, b
		}
	}
	panic("rngPair: failed to generate non‑zero pair after 65 536 tries")
}

func assertPanics(t *testing.T, f func(), msg string) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic: %s", msg)
		}
	}()
	f()
}

/*────────────────── zero / invalid input guards ──────────────────*/

func TestZeroInputPanics(t *testing.T) {
	assertPanics(t, func() { _ = log2u64(0) }, "log2u64(0)")
	assertPanics(t, func() { _ = log2u128(Uint128{}) }, "log2u128(0)")
	assertPanics(t, func() { _ = Log2ReserveRatio(0, 1) }, "Log2ReserveRatio(0,1)")
	assertPanics(t, func() { _ = LnReserveRatio(1, 0) }, "LnReserveRatio(1,0)")
	zero := Uint128{}
	assertPanics(t, func() { _ = Log2PriceX96(zero) }, "Log2PriceX96(0)")
}

func TestValidateConvPanics(t *testing.T) {
	badConvs := []float64{0, math.NaN(), math.Inf(1), 1e-305, 1e301}
	for _, c := range badConvs {
		assertPanics(t, func() { _ = LogReserveRatioConst(2, 1, c) }, "invalid conv")
		assertPanics(t, func() { _ = LogPriceX96Const(Uint128{Hi: 1, Lo: 0}, c) }, "invalid conv in price helper")
	}
}

/*────────────────── internal path coverage ──────────────────*/

func TestLog2u128Paths(t *testing.T) {
	v1 := Uint128{Hi: 0, Lo: 123456789}
	got1 := log2u128(v1)
	want1 := math.Log(float64(v1.Lo)) / math.Ln2
	if math.Abs(got1-want1) > 5e-5 {
		t.Fatalf("log2u128(Hi=0) mismatch: want %.12g got %.12g", want1, got1)
	}

	v2 := Uint128{Hi: 1 << 20, Lo: 987654321}
	x := float64(v2.Hi)*0x1p64 + float64(v2.Lo)
	got2 := log2u128(v2)
	want2 := math.Log(x) / math.Ln2
	if math.Abs(got2-want2) > 5e-5 {
		t.Fatalf("log2u128(full) mismatch: want %.12g got %.12g", want2, got2)
	}
}

/*────────────────── Log2PriceX96 & LnPriceX96 coverage ──────────────────*/

func TestLnPriceX96SimpleCase(t *testing.T) {
	sqrt := Uint128{Hi: 1 << 32, Lo: 0} // exactly 2^96 ⇒ price=1⇒ln=0
	if got := LnPriceX96(sqrt); math.Abs(got) > 1e-12 {
		t.Fatalf("LnPriceX96(1) expected 0 got %.12g", got)
	}
}

func TestLog2PriceX96Guards(t *testing.T) {
	big := Uint128{Hi: maxSqrtHi, Lo: 0}
	assertPanics(t, func() { _ = Log2PriceX96(big) }, ">1‑tick guard")

	cliff := Uint128{Hi: maxSqrtHi - 1, Lo: lostLowMask}
	assertPanics(t, func() { _ = Log2PriceX96(cliff) }, "precision cliff guard")
}

/*────────────────── LnReserveRatio accuracy tests ──────────────────*/

func TestLnReserveRatioAccuracyNearEqual(t *testing.T) {
	r := rand.New(rand.NewSource(testSeed))
	for i := 0; i < 10_000; i++ {
		a, _ := rngPair(r)
		a &= (1 << 60) - 1
		diff := uint64(r.Intn(1_000))
		var b uint64
		if r.Intn(2) == 0 {
			if a > diff+1 {
				b = a - diff
			} else {
				b = a + diff
			}
		} else {
			b = a + diff
		}
		if b == 0 {
			b = 1
		}
		want := math.Log(float64(a) / float64(b))
		got := LnReserveRatio(a, b)
		if math.Abs(got-want) > 1e-5 {
			t.Fatalf("LnReserveRatio(%d,%d): want %.12g got %.12g (Δ=%.3g)", a, b, want, got, got-want)
		}
	}
}

func TestLnReserveRatioULPBoundaries(t *testing.T) {
	b := uint64(1 << 53)
	a1 := b + 1
	a2 := b - 1
	for _, a := range []uint64{a1, a2} {
		want := math.Log(float64(a) / float64(b))
		got := LnReserveRatio(a, b)
		if math.Abs(got-want) > 1e-5 {
			t.Fatalf("LnReserveRatio(%d,%d): want %.12g got %.12g", a, b, want, got)
		}
	}
}

func TestLnReserveRatioMagnitudeSweep(t *testing.T) {
	r := rand.New(rand.NewSource(testSeed))
	for i := 0; i < 10_000; i++ {
		e1 := uint64(1) << uint(r.Intn(63)+1)
		e2 := uint64(1) << uint(r.Intn(63)+1)
		a := e1
		b := e2
		want := math.Log(float64(a) / float64(b))
		got := LnReserveRatio(a, b)
		if math.Abs(got-want) > 1e-5 {
			t.Fatalf("LnReserveRatio magnitude sweep (%d,%d): want %.12g got %.12g", a, b, want, got)
		}
	}
}

func TestLnReserveRatioMonotonicity(t *testing.T) {
	prev := math.Inf(-1)
	for a := uint64(1); a <= 1000; a++ {
		val := LnReserveRatio(a, 1)
		if val <= prev {
			t.Fatalf("LnReserveRatio not monotonic at a=%d: %.12g ≤ %.12g", a, val, prev)
		}
		prev = val
	}
}

func TestSeedDeterminism(t *testing.T) {
	a := rand.New(rand.NewSource(testSeed)).Uint64()
	b := rand.New(rand.NewSource(testSeed)).Uint64()
	if a != b {
		t.Fatalf("testSeed not deterministic: %d vs %d", a, b)
	}
}

/*────────────────── micro‑benchmarks ──────────────────*/

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
