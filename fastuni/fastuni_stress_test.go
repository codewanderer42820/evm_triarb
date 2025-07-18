// ============================================================================
// FASTUNI COMPREHENSIVE STRESS VALIDATION SUITE
// ============================================================================
//
// Massively parallel randomized testing framework for logarithmic utility
// validation under extreme computational stress and diverse input distributions.
//
// Validation scope:
//   - LnReserveRatio and Log2ReserveRatio accuracy across millions of samples
//   - Statistical verification over three input distribution regimes
//   - Parallel execution utilizing all available CPU cores
//   - Deterministic reproducibility via regime-specific seeding
//
// Input distribution regimes:
//   - SMALL: Near-unity ratios (a ≈ b) testing log1p precision paths
//   - MEDIUM: Moderate ratios (8× to 32× factors) covering common use cases
//   - LARGE: Extreme ratios (a ≫ b) testing algorithmic stability limits
//
// Test scales:
//   - 1M samples: Default execution for continuous integration
//   - 1B samples: Extended validation requiring long test mode
//   - 1T samples: Exhaustive validation for release qualification
//
// Infrastructure design:
//   - Worker pool architecture with GOMAXPROCS scaling
//   - Fail-fast error reporting via sync.Once coordination
//   - Deterministic seeding ensuring reproducible failure scenarios
//   - Statistical load balancing across worker threads
//
// Performance characteristics:
//   - Parallel execution scales linearly with available cores
//   - Memory usage remains constant regardless of sample count
//   - Deterministic execution time for performance benchmarking
//
// ⚠️  Computational intensity warning:
//     Billion and trillion scale tests require substantial CPU time
//     and should be executed selectively via testing.Short() gates

package fastuni

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

// ============================================================================
// SCALE CONFIGURATION
// ============================================================================

const (
	million  int64 = 1_000_000         // Standard validation scale
	billion  int64 = 1_000_000_000     // Extended validation scale
	trillion int64 = 1_000_000_000_000 // Exhaustive validation scale

	seedBase = 0x5eed // Base seed for deterministic regime generation
)

// ============================================================================
// INPUT DISTRIBUTION GENERATORS
// ============================================================================

// drawSmall generates near-unity ratio pairs (a ≈ b) for log1p path testing.
// Optimized for testing high-precision computation paths and floating-point
// edge cases around unity ratios.
//
// Distribution characteristics:
//   - Base values: [1, 1T] with uniform distribution
//   - Delta values: [1, 1M] ensuring small relative differences
//   - Ratio range: Typically [0.999999, 1.000001]
//
// Mathematical focus:
//   - Tests log1p optimization path accuracy
//   - Validates floating-point precision near unity
//   - Exercises algorithmic switching thresholds
//
// Returns: (a, b) pair where |a/b - 1| is small
func drawSmall(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000_000_000 + 1 // Base ∈ [1, 1T]
	delta := r.Uint64()%1_000_000 + 1        // Delta ∈ [1, 1M]
	if r.Intn(2) == 0 {
		return base + delta, base // a > b case
	}
	return base, base + delta // a < b case
}

// drawMedium generates moderate ratio pairs (a = b × [8, 32]) for general testing.
// Covers common ratio ranges encountered in financial and scientific applications.
//
// Distribution characteristics:
//   - Base values: [1, 1B] with uniform distribution
//   - Multiplication factors: [8, 32] covering moderate scaling
//   - Ratio range: [0.03125, 32] (5-bit dynamic range)
//
// Mathematical focus:
//   - Tests general logarithmic computation accuracy
//   - Validates algorithmic stability across common ranges
//   - Exercises typical use case performance
//
// Returns: (a, b) pair where a/b ∈ [8, 32] ∪ [1/32, 1/8]
func drawMedium(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000_000 + 1 // Base ∈ [1, 1B]
	factor := uint64(r.Intn(25) + 8)     // Factor ∈ [8, 32]
	if r.Intn(2) == 0 {
		return base * factor, base // Large ratio case
	}
	return base, base * factor // Small ratio case
}

// drawLarge generates extreme ratio pairs (a ≫ b) for stability testing.
// Tests algorithmic behavior under maximum dynamic range conditions.
//
// Distribution characteristics:
//   - Base values: [1, 1M] with uniform distribution
//   - Bit shifts: [24, 63] creating extreme magnitude differences
//   - Ratio range: [2²⁴, 2⁶³] (39-bit dynamic range)
//
// Mathematical focus:
//   - Tests numerical stability at computation limits
//   - Validates handling of extreme magnitude differences
//   - Exercises IEEE 754 boundary conditions
//
// Returns: (a, b) pair where a/b or b/a is extremely large
func drawLarge(r *rand.Rand) (a, b uint64) {
	base := r.Uint64()%1_000_000 + 1 // Base ∈ [1, 1M]
	shift := uint(r.Intn(40) + 24)   // Shift ∈ [24, 63]
	high := base << shift

	// Handle potential overflow
	if high == 0 {
		high = 1<<63 - 1 // Use maximum safe value
	}

	if r.Intn(2) == 0 {
		return high, base // Extreme large ratio case
	}
	return base, high // Extreme small ratio case
}

// ============================================================================
// PARALLEL STRESS TESTING ENGINE
// ============================================================================

// sweepN executes parallel validation of logarithmic implementations across N samples.
// Automatically scales to available CPU cores for maximum throughput while maintaining
// deterministic behavior and fail-fast error reporting.
//
// Architecture:
//   - Worker pool: Utilizes all available CPU cores via GOMAXPROCS
//   - Load balancing: Distributes samples evenly across workers
//   - Error coordination: Uses sync.Once for fail-fast behavior
//   - Deterministic seeding: Ensures reproducible test execution
//
// Parameters:
//
//	t: Testing context for error reporting
//	regime: Distribution type identifier for seeding and reporting
//	n: Total number of samples to validate
//	gen: Input pair generator function
//	gold: Reference implementation (math.Log or math.Log2)
//	impl: Implementation under test
//
// Performance characteristics:
//   - Linear scaling with CPU core count
//   - Sub-linear memory usage (constant per worker)
//   - Deterministic execution time for benchmarking
func sweepN(t *testing.T, regime string, n int64,
	gen func(*rand.Rand) (uint64, uint64),
	gold func(float64) float64,
	impl func(uint64, uint64) (float64, error),
) {
	workers := runtime.GOMAXPROCS(0)

	// Use single-threaded execution for small workloads or limited parallelism
	if workers < 2 || n < int64(workers) {
		singleThreadSweep(t, regime, n, gen, gold, impl)
		return
	}

	// Parallel execution setup
	chunk := n / int64(workers)
	var wg sync.WaitGroup
	var once sync.Once
	var failMsg string

	// Launch worker pool
	for w := 0; w < workers; w++ {
		start := int64(w) * chunk
		end := start + chunk
		if w == workers-1 {
			end = n // Last worker handles remainder
		}

		// Deterministic per-worker seeding
		seed := seedBase + int64(regime[0]) + n + int64(w)
		r := rand.New(rand.NewSource(seed))

		wg.Add(1)
		go func(worker int, from, to int64, rng *rand.Rand) {
			defer wg.Done()

			// Per-worker validation loop
			for i := from; i < to; i++ {
				a, b := gen(rng)
				want := gold(float64(a) / float64(b))
				got, err := impl(a, b)

				// Accuracy validation with tolerance checking
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

	// Synchronize worker completion and report results
	wg.Wait()
	if failMsg != "" {
		t.Fatal(failMsg)
	}
}

// singleThreadSweep provides validation for cases where parallelism is not beneficial.
// Used automatically for small workloads or systems with limited CPU cores.
//
// Execution characteristics:
//   - Sequential processing with minimal overhead
//   - Identical validation logic to parallel version
//   - Deterministic seeding matching parallel execution
func singleThreadSweep(t *testing.T, regime string, n int64,
	gen func(*rand.Rand) (uint64, uint64),
	gold func(float64) float64,
	impl func(uint64, uint64) (float64, error),
) {
	r := rand.New(rand.NewSource(seedBase + int64(regime[0]) + n))

	for i := int64(0); i < n; i++ {
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

// ============================================================================
// MILLION-SCALE VALIDATION (DEFAULT EXECUTION)
// ============================================================================

// TestLnReserveRatioSmall1M validates ln(a/b) accuracy for near-unity ratios.
// Tests log1p optimization path with 1M samples of small delta inputs.
func TestLnReserveRatioSmall1M(t *testing.T) {
	sweepN(t, "SMALL", million, drawSmall, math.Log, LnReserveRatio)
}

// TestLnReserveRatioMedium1M validates ln(a/b) accuracy for moderate ratios.
// Tests general computation path with 1M samples of typical ratio inputs.
func TestLnReserveRatioMedium1M(t *testing.T) {
	sweepN(t, "MEDIUM", million, drawMedium, math.Log, LnReserveRatio)
}

// TestLnReserveRatioLarge1M validates ln(a/b) accuracy for extreme ratios.
// Tests numerical stability with 1M samples of maximum dynamic range inputs.
func TestLnReserveRatioLarge1M(t *testing.T) {
	sweepN(t, "LARGE", million, drawLarge, math.Log, LnReserveRatio)
}

// TestLog2ReserveRatioSmall1M validates log₂(a/b) accuracy for near-unity ratios.
// Tests high-precision computation with 1M samples of small delta inputs.
func TestLog2ReserveRatioSmall1M(t *testing.T) {
	sweepN(t, "SMALL", million, drawSmall, math.Log2, Log2ReserveRatio)
}

// TestLog2ReserveRatioMedium1M validates log₂(a/b) accuracy for moderate ratios.
// Tests general algorithmic behavior with 1M samples of typical inputs.
func TestLog2ReserveRatioMedium1M(t *testing.T) {
	sweepN(t, "MEDIUM", million, drawMedium, math.Log2, Log2ReserveRatio)
}

// TestLog2ReserveRatioLarge1M validates log₂(a/b) accuracy for extreme ratios.
// Tests boundary condition handling with 1M samples of maximum range inputs.
func TestLog2ReserveRatioLarge1M(t *testing.T) {
	sweepN(t, "LARGE", million, drawLarge, math.Log2, Log2ReserveRatio)
}

// ============================================================================
// BILLION-SCALE VALIDATION (EXTENDED EXECUTION)
// ============================================================================

// TestLnReserveRatioSmall1B provides extended validation with 1B samples.
// Requires long test mode due to computational intensity.
func TestLnReserveRatioSmall1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", billion, drawSmall, math.Log, LnReserveRatio)
}

// TestLnReserveRatioMedium1B provides extended validation with 1B samples.
// Comprehensive statistical coverage for production validation.
func TestLnReserveRatioMedium1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", billion, drawMedium, math.Log, LnReserveRatio)
}

// TestLnReserveRatioLarge1B provides extended validation with 1B samples.
// Exhaustive boundary condition testing for critical applications.
func TestLnReserveRatioLarge1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", billion, drawLarge, math.Log, LnReserveRatio)
}

// TestLog2ReserveRatioSmall1B provides extended log₂ validation with 1B samples.
// High-precision verification for mission-critical deployments.
func TestLog2ReserveRatioSmall1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", billion, drawSmall, math.Log2, Log2ReserveRatio)
}

// TestLog2ReserveRatioMedium1B provides extended log₂ validation with 1B samples.
// Comprehensive algorithmic verification across typical input distributions.
func TestLog2ReserveRatioMedium1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", billion, drawMedium, math.Log2, Log2ReserveRatio)
}

// TestLog2ReserveRatioLarge1B provides extended log₂ validation with 1B samples.
// Maximum stress testing for extreme computational conditions.
func TestLog2ReserveRatioLarge1B(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1B LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", billion, drawLarge, math.Log2, Log2ReserveRatio)
}

// ============================================================================
// TRILLION-SCALE VALIDATION (EXHAUSTIVE EXECUTION)
// ============================================================================

// TestLnReserveRatioSmallTrillion provides exhaustive ln validation with 1T samples.
// Ultimate precision verification for research and certification purposes.
func TestLnReserveRatioSmallTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", trillion, drawSmall, math.Log, LnReserveRatio)
}

// TestLnReserveRatioMediumTrillion provides exhaustive ln validation with 1T samples.
// Complete statistical coverage for algorithmic certification.
func TestLnReserveRatioMediumTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", trillion, drawMedium, math.Log, LnReserveRatio)
}

// TestLnReserveRatioLargeTrillion provides exhaustive ln validation with 1T samples.
// Maximum stress testing for mission-critical system qualification.
func TestLnReserveRatioLargeTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", trillion, drawLarge, math.Log, LnReserveRatio)
}

// TestLog2ReserveRatioSmallTrillion provides exhaustive log₂ validation with 1T samples.
// Ultimate algorithmic verification for high-precision applications.
func TestLog2ReserveRatioSmallTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T SMALL sweep in short mode")
	}
	sweepN(t, "SMALL", trillion, drawSmall, math.Log2, Log2ReserveRatio)
}

// TestLog2ReserveRatioMediumTrillion provides exhaustive log₂ validation with 1T samples.
// Comprehensive verification across complete input space coverage.
func TestLog2ReserveRatioMediumTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T MEDIUM sweep in short mode")
	}
	sweepN(t, "MEDIUM", trillion, drawMedium, math.Log2, Log2ReserveRatio)
}

// TestLog2ReserveRatioLargeTrillion provides exhaustive log₂ validation with 1T samples.
// Final certification testing for production deployment qualification.
func TestLog2ReserveRatioLargeTrillion(t *testing.T) {
	if testing.Short() {
		t.Skip("skip 1T LARGE sweep in short mode")
	}
	sweepN(t, "LARGE", trillion, drawLarge, math.Log2, Log2ReserveRatio)
}
