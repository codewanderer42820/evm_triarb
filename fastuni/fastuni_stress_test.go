// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: fastuni_stress_test.go — parallel randomized stress validation
//
// Purpose:
//   - Validates `LnReserveRatio` and `Log2ReserveRatio` via randomized sweeping
//   - Confirms output accuracy over millions/billions/trillions of ratio pairs
//
// Modes:
//   - SMALL:    a ≈ b (tiny deltas, sensitive to float errors)
//   - MEDIUM:   a = b × [8–32] (common ratio forms)
//   - LARGE:    a ≫ b (shifted magnitude test)
//
// Infrastructure:
//   - Parallel worker threads using GOMAXPROCS
//   - Safe fail-on-first-error via `sync.Once`
//   - Uses deterministic seed per regime, thread, and size
//
// ⚠️ These tests are compute-heavy and long-running — 1B/1T tests are gated
//    behind `testing.Short()` and should be run selectively.
// ─────────────────────────────────────────────────────────────────────────────

package fastuni

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

/*──────────────────────────────────────────────────────────────────────────────
  📦 Constants and Tuning
──────────────────────────────────────────────────────────────────────────────*/

const (
	million  = 1_000_000
	billion  = 1_000_000_000
	trillion = 1_000_000_000_000

	seedBase = 0x5eed // per-regime seed base
)

/*──────────────────────────────────────────────────────────────────────────────
  🔧 Input Generators by Regime
──────────────────────────────────────────────────────────────────────────────*/

// drawSmall returns (a ≈ b), exercising log1p path
func drawSmall(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000_000_000 + 1 // avoid 0
	delta := r.Uint64()%1_000_000 + 1        // small offset
	if r.Intn(2) == 0 {
		return base + delta, base
	}
	return base, base + delta
}

// drawMedium returns (a = b × [8–32]), moderate gaps
func drawMedium(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000_000 + 1
	factor := uint64(r.Intn(25) + 8)
	if r.Intn(2) == 0 {
		return base * factor, base
	}
	return base, base * factor
}

// drawLarge returns (a ≫ b), wide magnitude ratio
func drawLarge(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000 + 1
	shift := uint(r.Intn(40) + 24)
	high := base << shift
	if high == 0 {
		high = 1<<63 - 1
	}
	if r.Intn(2) == 0 {
		return high, base
	}
	return base, high
}

/*──────────────────────────────────────────────────────────────────────────────
  🔬 Generic Parallel Stress Engine
──────────────────────────────────────────────────────────────────────────────*/

// sweepN validates impl(a, b) ≈ gold(a/b) across N samples
func sweepN(t *testing.T, regime string, n int,
	gen func(*rand.Rand) (uint64, uint64),
	gold func(float64) float64,
	impl func(uint64, uint64) (float64, error),
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
				got, err := impl(a, b)
				if err != nil || math.Abs(got-want) > tol {
					once.Do(func() {
						failMsg = fmt.Sprintf(
							"%s sample %d/%d (worker %d): (%d,%d) want %.12g got %.12g Δ=%.3g err=%v",
							regime, i, n, worker, a, b, want, got, got-want, err,
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

// singleThreadSweep is used when parallelism is not worthwhile
func singleThreadSweep(t *testing.T, regime string, n int,
	gen func(*rand.Rand) (uint64, uint64),
	gold func(float64) float64,
	impl func(uint64, uint64) (float64, error),
) {
	r := rand.New(rand.NewSource(seedBase + int64(regime[0]) + int64(n)))
	for i := 0; i < n; i++ {
		a, b := gen(r)
		want := gold(float64(a) / float64(b))
		got, err := impl(a, b)
		if err != nil || math.Abs(got-want) > tol {
			t.Fatalf(
				"%s sample %d/%d: (%d,%d) want %.12g got %.12g Δ=%.3g err=%v",
				regime, i, n, a, b, want, got, got-want, err,
			)
		}
	}
}

/*──────────────────────────────────────────────────────────────────────────────
  🧪 1M-Scale Sweeps (default)
──────────────────────────────────────────────────────────────────────────────*/

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

/*──────────────────────────────────────────────────────────────────────────────
  🧪 1B-Scale Sweeps (requires long mode)
──────────────────────────────────────────────────────────────────────────────*/

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

/*──────────────────────────────────────────────────────────────────────────────
  🧪 1T-Scale Sweeps (manual trigger only)
──────────────────────────────────────────────────────────────────────────────*/

func TestLnReserveRatioSmallTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", trillion, drawSmall, math.Log, LnReserveRatio)
}
func TestLnReserveRatioMediumTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", trillion, drawMedium, math.Log, LnReserveRatio)
}
func TestLnReserveRatioLargeTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", trillion, drawLarge, math.Log, LnReserveRatio)
}
func TestLog2ReserveRatioSmallTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", trillion, drawSmall, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioMediumTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", trillion, drawMedium, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioLargeTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", trillion, drawLarge, math.Log2, Log2ReserveRatio)
}
