// fastuni_stress_test.go — parallel randomized correctness sweeps for reserve-ratio routines.
// -----------------------------------------------------------
// Runs deterministic random tests across three regimes:
//   SMALL  — 1e6  samples
//   MEDIUM — 1e9  samples (skip with -short)
//   LARGE  — 1e12 samples (skip with -short)
// Tolerance: |Δ| ≤ 5e-5 nat.

package fastuni

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

/*─────────────────── constants ───────────────────*/
const (
	million  = 1_000_000
	billion  = 1_000_000_000
	trillion = 1_000_000_000_000
	seedBase = 0x5eed // base for deterministic RNG seed
)

/*─────────────────── test input generators ───────────────────*/

// drawSmall produces (a,b) with small delta variations.
// Range: base up to 1e12, delta up to 1e6.
func drawSmall(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000_000_000 + 1
	delta := r.Uint64()%1_000_000 + 1
	if r.Intn(2) == 0 {
		return base + delta, base
	}
	return base, base + delta
}

// drawMedium produces (a,b) by scaling a base up to 1e9 by factor 8..32.
func drawMedium(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000_000 + 1
	factor := uint64(r.Intn(25) + 8)
	if r.Intn(2) == 0 {
		return base * factor, base
	}
	return base, base * factor
}

// drawLarge produces bit-shifted values for large-range tests.
func drawLarge(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000 + 1
	shift := uint(r.Intn(40) + 24)
	high := base << shift
	if high == 0 {
		high = (1<<63 - 1)
	}
	if r.Intn(2) == 0 {
		return high, base
	}
	return base, high
}

/*─────────────────── parallel sweep engine ───────────────────*/

// sweepN runs n samples of regime in parallel across GOMAXPROCS workers.
// gen generates (a,b), gold computes reference, impl is the tested function.
// Fails on first sample where |got-want| > tol.
func sweepN(t *testing.T, regime string, n int,
	gen func(*rand.Rand) (uint64, uint64),
	gold func(float64) float64,
	impl func(uint64, uint64) float64,
) {
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 || n < workers {
		singleThreadSweep(t, regime, n, gen, gold, impl)
		return
	}

	chunk := n / workers
	var wg sync.WaitGroup
	var once sync.Once
	var failMsg string

	for w := 0; w < workers; w++ {
		start := w * chunk
		end := start + chunk
		if w == workers-1 {
			end = n
		}
		seed := seedBase + int64(regime[0]) + int64(n) + int64(w)
		r := rand.New(rand.NewSource(seed))

		wg.Add(1)
		go func(worker, from, to int, rng *rand.Rand) {
			defer wg.Done()
			for i := from; i < to; i++ {
				a, b := gen(rng)
				want := gold(float64(a) / float64(b))
				got := impl(a, b)
				if math.Abs(got-want) > tol {
					once.Do(func() {
						failMsg = fmt.Sprintf(
							"%s sample %d/%d (worker %d): (%d,%d) want %.12g got %.12g Δ=%.3g",
							regime, i, n, worker, a, b, want, got, got-want,
						)
					})
					return
				}
			}
		}(w, start, end, r)
	}

	wg.Wait()
	if failMsg != "" {
		t.Fatal(failMsg)
	}
}

// singleThreadSweep runs n samples serially for small workloads.
func singleThreadSweep(t *testing.T, regime string, n int,
	gen func(*rand.Rand) (uint64, uint64),
	gold func(float64) float64,
	impl func(uint64, uint64) float64,
) {
	r := rand.New(rand.NewSource(seedBase + int64(regime[0]) + int64(n)))
	for i := 0; i < n; i++ {
		a, b := gen(r)
		want := gold(float64(a) / float64(b))
		got := impl(a, b)
		if math.Abs(got-want) > tol {
			t.Fatalf(
				"%s sample %d/%d: (%d,%d) want %.12g got %.12g Δ=%.3g",
				regime, i, n, a, b, want, got, got-want,
			)
		}
	}
}

/*─────────────────── 1M sample tests ───────────────────*/

// TestLnReserveRatioSmall1M verifies LnReserveRatio over 1e6 samples in SMALL regime.
func TestLnReserveRatioSmall1M(t *testing.T) {
	sweepN(t, "SMALL", million, drawSmall, math.Log, LnReserveRatio)
}

// TestLnReserveRatioMedium1M verifies LnReserveRatio over 1e6 samples in MEDIUM regime.
func TestLnReserveRatioMedium1M(t *testing.T) {
	sweepN(t, "MEDIUM", million, drawMedium, math.Log, LnReserveRatio)
}

// TestLnReserveRatioLarge1M verifies LnReserveRatio over 1e6 samples in LARGE regime.
func TestLnReserveRatioLarge1M(t *testing.T) {
	sweepN(t, "LARGE", million, drawLarge, math.Log, LnReserveRatio)
}

// TestLog2ReserveRatioSmall1M verifies Log2ReserveRatio over 1e6 samples in SMALL regime.
func TestLog2ReserveRatioSmall1M(t *testing.T) {
	sweepN(t, "SMALL", million, drawSmall, math.Log2, Log2ReserveRatio)
}

// TestLog2ReserveRatioMedium1M verifies Log2ReserveRatio over 1e6 samples in MEDIUM regime.
func TestLog2ReserveRatioMedium1M(t *testing.T) {
	sweepN(t, "MEDIUM", million, drawMedium, math.Log2, Log2ReserveRatio)
}

// TestLog2ReserveRatioLarge1M verifies Log2ReserveRatio over 1e6 samples in LARGE regime.
func TestLog2ReserveRatioLarge1M(t *testing.T) {
	sweepN(t, "LARGE", million, drawLarge, math.Log2, Log2ReserveRatio)
}

/*─────────────────── 1B sample tests (skip in -short) ───────────────────*/

func TestLnReserveRatioSmall1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", billion, drawSmall, math.Log, LnReserveRatio)
}
func TestLnReserveRatioMedium1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", billion, drawMedium, math.Log, LnReserveRatio)
}
func TestLnReserveRatioLarge1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", billion, drawLarge, math.Log, LnReserveRatio)
}
func TestLog2ReserveRatioSmall1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", billion, drawSmall, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioMedium1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", billion, drawMedium, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioLarge1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", billion, drawLarge, math.Log2, Log2ReserveRatio)
}

/*─────────────────── 1T sample tests (opt-in only) ───────────────────*/

func TestLnReserveRatioSmallTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", trillion, drawSmall, math.Log, LnReserveRatio)
}
func TestLnReserveRatioMediumTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", trillion, drawMedium, math.Log, LnReserveRatio)
}
func TestLnReserveRatioLargeTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", trillion, drawLarge, math.Log, LnReserveRatio)
}
func TestLog2ReserveRatioSmallTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", trillion, drawSmall, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioMediumTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", trillion, drawMedium, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioLargeTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", trillion, drawLarge, math.Log2, Log2ReserveRatio)
}
