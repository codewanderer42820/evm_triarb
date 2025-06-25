// fastuni_delta_test.go — Massive randomized sweeps (1 M / 1 B / 1 T).
//
// Synopsis
// --------
// Stress‑tests `LnReserveRatio` and `Log2ReserveRatio` over three magnitude
// regimes (SMALL, MEDIUM, LARGE) with fully parallel workers.
//
// Usage
// -----
//   go test -run 1B       -timeout 60h  ./…   # billion‑sample sweeps
//   go test -run Trillion -timeout 600h ./…   # trillion‑sample sweeps
//
// Notes
// -----
// • Workers = `GOMAXPROCS`; single‑thread fallback for tiny N or `testing.Short`.
// • Fixed tolerance: `tol = 5 × 10⁻⁵ nat` across all regimes.
// • Unused‑parameter warnings removed via explicit captures in goroutines.

package fastuni

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

/*──────────────────── parameters ───────────────────*/

const (
	million  = 1_000_000
	billion  = 1_000_000_000
	trillion = 1_000_000_000_000 // 1e12 (placeholder until Go int literal handles 1e12)
	tol      = 5e-5              // global error bound in nat
	seedBase = 0x5eed            // deterministic base seed, regime‑specific offset
)

/*──────────────────── helper generators ───────────────────*/

func drawSmall(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000_000_000 + 1 // up to 1e12
	delta := r.Uint64()%1_000_000 + 1        // 1 … 1e6
	if r.Intn(2) == 0 {
		return base + delta, base
	}
	return base, base + delta
}

func drawMedium(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000_000 + 1 // up to 1e9
	factor := uint64(r.Intn(25) + 8)     // 8 … 32
	if r.Intn(2) == 0 {
		return base * factor, base
	}
	return base, base * factor
}

func drawLarge(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000 + 1 // up to 1e6
	shift := uint(r.Intn(40) + 24)   // 24 … 63
	high := base << shift
	if high == 0 {
		high = (1 << 63) - 1
	}
	if r.Intn(2) == 0 {
		return high, base
	}
	return base, high
}

/*──────────────────── generic parallel sweeper ───────────────────*/

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

// singleThreadSweep is retained for the workers<2 code path.
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

/*──────────────────── 1 M sweeps ───────────────────*/

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

/*──────────────────── 1 B sweeps ───────────────────*/

func TestLnReserveRatioSmall1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1B SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", billion, drawSmall, math.Log, LnReserveRatio)
}
func TestLnReserveRatioMedium1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1B MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", billion, drawMedium, math.Log, LnReserveRatio)
}
func TestLnReserveRatioLarge1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1B LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", billion, drawLarge, math.Log, LnReserveRatio)
}

func TestLog2ReserveRatioSmall1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1B SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", billion, drawSmall, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioMedium1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1B MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", billion, drawMedium, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioLarge1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1B LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", billion, drawLarge, math.Log2, Log2ReserveRatio)
}

/*──────────────────── 1 T sweeps — opt‑in only (very long) ───────────────────*/

func TestLnReserveRatioSmallTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Trillion SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", trillion, drawSmall, math.Log, LnReserveRatio)
}
func TestLnReserveRatioMediumTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Trillion MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", trillion, drawMedium, math.Log, LnReserveRatio)
}
func TestLnReserveRatioLargeTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Trillion LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", trillion, drawLarge, math.Log, LnReserveRatio)
}

func TestLog2ReserveRatioSmallTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Trillion SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", trillion, drawSmall, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioMediumTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Trillion MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", trillion, drawMedium, math.Log2, Log2ReserveRatio)
}
func TestLog2ReserveRatioLargeTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Trillion LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", trillion, drawLarge, math.Log2, Log2ReserveRatio)
}
