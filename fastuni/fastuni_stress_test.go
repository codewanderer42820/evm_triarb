// fastuni_stress_test.go — Parallel randomized sweeps: 1M, 1B, 1T samples.
//
// Overview
// --------
// Runs massive correctness sweeps across three magnitude regimes (SMALL, MEDIUM, LARGE).
// Uses deterministic random generation with multithreaded sweep support.
// Tolerance is fixed across all samples: |Δ| ≤ 5e‑5 nat.

package fastuni

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

/*──────────────────── constants ───────────────────*/

const (
	million  = 1_000_000
	billion  = 1_000_000_000
	trillion = 1_000_000_000_000 // 1e12; Go doesn't support float literal for this
	seedBase = 0x5eed            // base seed, regime-adjusted
)

/*──────────────────── test input generators ───────────────────*/

//go:nosplit
//go:inline
func drawSmall(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000_000_000 + 1 // up to 1e12
	delta := r.Uint64()%1_000_000 + 1        // delta in [1, 1e6]
	if r.Intn(2) == 0 {
		return base + delta, base
	}
	return base, base + delta
}

//go:nosplit
//go:inline
func drawMedium(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000_000 + 1
	factor := uint64(r.Intn(25) + 8) // scale by 8 … 32
	if r.Intn(2) == 0 {
		return base * factor, base
	}
	return base, base * factor
}

//go:nosplit
//go:inline
func drawLarge(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000 + 1
	shift := uint(r.Intn(40) + 24)
	high := base << shift
	if high == 0 {
		high = (1 << 63) - 1 // saturate to max int64 if overflowed to 0
	}
	if r.Intn(2) == 0 {
		return high, base
	}
	return base, high
}

/*──────────────────── parallel sweeping engine ───────────────────*/

func sweepN(t *testing.T, regime string, n int, gen func(*rand.Rand) (uint64, uint64), gold func(float64) float64, impl func(uint64, uint64) float64) {
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
		go func(workerID, from, to int, rng *rand.Rand) {
			defer wg.Done()
			for i := from; i < to; i++ {
				a, b := gen(rng)
				want := gold(float64(a) / float64(b))
				got := impl(a, b)
				if math.Abs(got-want) > tol {
					once.Do(func() {
						failMsg = fmt.Sprintf("%s sample %d/%d (worker %d): (%d,%d) want %.12g got %.12g (Δ=%.3g)", regime, i, n, workerID, a, b, want, got, got-want)
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

//go:nosplit
//go:inline
func singleThreadSweep(t *testing.T, regime string, n int, gen func(*rand.Rand) (uint64, uint64), gold func(float64) float64, impl func(uint64, uint64) float64) {
	r := rand.New(rand.NewSource(seedBase + int64(regime[0]) + int64(n)))
	for i := 0; i < n; i++ {
		a, b := gen(r)
		want := gold(float64(a) / float64(b))
		got := impl(a, b)
		if math.Abs(got-want) > tol {
			t.Fatalf("%s sample %d/%d: (%d,%d) want %.12g got %.12g (Δ=%.3g)", regime, i, n, a, b, want, got, got-want)
		}
	}
}

/*──────────────────── 1M tests ───────────────────*/

func TestLnReserveRatioSmall1M(t *testing.T) {
	sweepN(t, "SMALL", million, drawSmall, math.Log, LnReserveRatio)
}
func TestLnReserveRatioMedium1M(t *testing.T) {
	sweepN(t, "MEDIUM", million, drawMedium, math.Log, LnReserveRatio)
}
func TestLnReserveRatioLarge1M(t *testing.T) {
	sweepN(t, "LARGE", million, drawLarge, math.Log, LnReserveRatio)
}
func TestLog2ReserveRatioSmall1M(t *testing.T) {
	sweepN(t, "SMALL", million, drawSmall, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioMedium1M(t *testing.T) {
	sweepN(t, "MEDIUM", million, drawMedium, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioLarge1M(t *testing.T) {
	sweepN(t, "LARGE", million, drawLarge, math.Log2, Log2ReserveRatio)
}

/*──────────────────── 1B tests ───────────────────*/

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

/*──────────────────── 1T tests — opt-in only ───────────────────*/

func TestLnReserveRatioSmallTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion SMALL sweep")
	}
	sweepN(t, "SMALL", trillion, drawSmall, math.Log, LnReserveRatio)
}
func TestLnReserveRatioMediumTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion MEDIUM sweep")
	}
	sweepN(t, "MEDIUM", trillion, drawMedium, math.Log, LnReserveRatio)
}
func TestLnReserveRatioLargeTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion LARGE sweep")
	}
	sweepN(t, "LARGE", trillion, drawLarge, math.Log, LnReserveRatio)
}

func TestLog2ReserveRatioSmallTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion SMALL sweep")
	}
	sweepN(t, "SMALL", trillion, drawSmall, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioMediumTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion MEDIUM sweep")
	}
	sweepN(t, "MEDIUM", trillion, drawMedium, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioLargeTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip Trillion LARGE sweep")
	}
	sweepN(t, "LARGE", trillion, drawLarge, math.Log2, Log2ReserveRatio)
}
