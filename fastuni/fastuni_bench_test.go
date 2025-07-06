// ============================================================================
// FASTUNI PERFORMANCE MICROBENCHMARK SUITE
// ============================================================================
//
// Comprehensive performance measurement framework for ISR-grade logarithmic
// operations with emphasis on sub-10ns execution targets.
//
// Benchmark categories:
//   - Core logarithmic primitives: log₂(uint64), log₂(Uint128)
//   - Reserve ratio calculations: ln(a/b), log₂(a/b), scaled variants
//   - Q64.96 price transformations: Uniswap V3 format operations
//   - Input distribution analysis: Fixed, random, and edge case patterns
//
// Performance targets:
//   - log₂(uint64): <5ns per operation
//   - log₂(Uint128): <10ns per operation
//   - Reserve ratios: <15ns per operation
//   - Price transformations: <20ns per operation
//
// Test methodology:
//   - Deterministic seed (12345) for reproducible measurements
//   - Comprehensive input coverage: small, large, boundary values
//   - Separate benchmarks for algorithmic path analysis
//   - Statistical significance via large sample sizes
//
// Input characteristics:
//   - Fixed patterns: Powers of 2, known constants, edge cases
//   - Random patterns: Uniform distribution across value ranges
//   - Pathological cases: Maximum values, precision boundaries

package fastuni

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// ============================================================================
// BENCHMARK CONFIGURATION
// ============================================================================

const benchSeed = 12345 // Deterministic seed for reproducible measurements

// Pre-computed input datasets for consistent benchmarking across runs
var (
	// Sample uint64 values covering logarithmic input space
	benchLog2u64Inputs = []uint64{
		1,         // Minimum positive value
		3,         // Small prime
		1 << 10,   // Medium power of 2 (1K)
		1 << 32,   // Large power of 2 (4B)
		123456789, // Arbitrary large value
		1 << 52,   // IEEE 754 precision boundary
	}

	// Sample Uint128 values for Q64.96/log₂ benchmarking
	benchLog2u128Inputs = []Uint128{
		{0, 1},             // Low-word only (small value)
		{1 << 32, 0},       // High-word only (Q64.96 unity)
		{1 << 40, 1 << 20}, // Mixed representation
	}

	// Pre-generated random Uint128 values for statistical benchmarking
	benchRandU128 = newRandU128(1024)

	// Ratio pairs for reserve ratio benchmarking
	benchPairs = []struct{ a, b uint64 }{
		{1, 1},             // Identity ratio
		{10000, 10001},     // Small delta (log1p optimization)
		{1 << 30, 1 << 20}, // Power-of-2 ratio
		{12345, 67890},     // Arbitrary ratio
	}
)

// ============================================================================
// INPUT GENERATION UTILITIES
// ============================================================================

// newRandU128 generates deterministic Uint128 values for consistent benchmarking.
// Uses fixed seed to ensure reproducible performance measurements across runs.
//
// Parameters:
//
//	n: Number of random values to generate
//
// Returns:
//
//	Slice of n deterministic Uint128 values
func newRandU128(n int) []Uint128 {
	r := rand.New(rand.NewSource(benchSeed))
	slice := make([]Uint128, n)
	for i := range slice {
		slice[i] = Uint128{r.Uint64(), r.Uint64()}
	}
	return slice
}

// ============================================================================
// UINT64 LOGARITHM BENCHMARKS
// ============================================================================

// BenchmarkLog2u64 measures log₂(x) performance across representative inputs.
//
// Test methodology:
//   - Executes Log2ReserveRatio(x, 1) to test log₂(x) in public API context
//   - Covers small values, powers of 2, and large arbitrary numbers
//   - Individual sub-benchmarks for input-specific analysis
//
// Expected performance: <5ns per operation
// Use cases: Magnitude analysis, bit position calculations, scaling operations
func BenchmarkLog2u64(b *testing.B) {
	for _, x := range benchLog2u64Inputs {
		b.Run(fmt.Sprintf("x=%d", x), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = Log2ReserveRatio(x, 1)
			}
		})
	}
}

// ============================================================================
// UINT128 LOGARITHM BENCHMARKS
// ============================================================================

// BenchmarkLog2u128_Fixed measures log₂(Uint128) on predetermined constants.
//
// Input analysis:
//   - Low-word only: Tests 64-bit delegation optimization
//   - High-word only: Tests 128-bit computation path
//   - Mixed values: Tests full precision handling
//
// Expected performance: <10ns per operation
// Use cases: Q64.96 price analysis, large integer logarithms
func BenchmarkLog2u128_Fixed(b *testing.B) {
	for _, u := range benchLog2u128Inputs {
		name := fmt.Sprintf("Hi=%d_Lo=%d", u.Hi, u.Lo)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = log2u128(u)
			}
		})
	}
}

// BenchmarkLog2u128_Random measures performance on statistical input distribution.
//
// Test characteristics:
//   - 1024 pre-generated random values cycled through benchmark
//   - Uniform distribution across 128-bit value space
//   - Tests worst-case performance under diverse inputs
//
// Expected performance: <10ns per operation (may be higher due to cache misses)
// Use cases: Monte Carlo simulations, statistical analysis pipelines
func BenchmarkLog2u128_Random(b *testing.B) {
	n := len(benchRandU128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u := benchRandU128[i%n]
		_ = log2u128(u)
	}
}

// ============================================================================
// RESERVE RATIO BENCHMARKS
// ============================================================================

// BenchmarkLnReserveRatio measures ln(a/b) performance across ratio patterns.
//
// Algorithmic path analysis:
//   - Identity ratios: Trivial case (a=b)
//   - Small deltas: log1p optimization path
//   - Large ratios: log₂ difference fallback path
//   - Arbitrary ratios: General case performance
//
// Expected performance: <15ns per operation
// Use cases: Financial ratio analysis, reserve monitoring, price comparisons
func BenchmarkLnReserveRatio(b *testing.B) {
	for _, p := range benchPairs {
		name := fmt.Sprintf("%d_%d", p.a, p.b)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = LnReserveRatio(p.a, p.b)
			}
		})
	}
}

// BenchmarkLog2ReserveRatio measures log₂(a/b) performance.
//
// Performance characteristics:
//   - Pure log₂ difference computation
//   - No conditional path selection overhead
//   - Consistent performance across input ranges
//
// Expected performance: <10ns per operation
// Use cases: Binary scaling, magnitude comparison, bit-level analysis
func BenchmarkLog2ReserveRatio(b *testing.B) {
	for _, p := range benchPairs {
		name := fmt.Sprintf("%d_%d", p.a, p.b)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = Log2ReserveRatio(p.a, p.b)
			}
		})
	}
}

// BenchmarkLogReserveRatioConst measures ln(a/b) × constant performance.
//
// Operations tested:
//   - log₂ difference computation
//   - Floating-point multiplication by conversion factor
//   - Input validation overhead
//
// Expected performance: <15ns per operation
// Use cases: Custom base conversions, scaled logarithmic transformations
func BenchmarkLogReserveRatioConst(b *testing.B) {
	for _, p := range benchPairs {
		b.Run(fmt.Sprintf("%d_%d", p.a, p.b), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = LogReserveRatioConst(p.a, p.b, math.Pi)
			}
		})
	}
}

// ============================================================================
// Q64.96 PRICE TRANSFORMATION BENCHMARKS
// ============================================================================

// BenchmarkLog2PriceX96 measures Q64.96 → log₂(price) transformation performance.
//
// Operations measured:
//   - Uint128 logarithm computation
//   - Q64.96 format normalization (subtract 96, multiply by 2)
//   - Input validation and error handling
//
// Expected performance: <20ns per operation
// Use cases: Uniswap V3 price analysis, AMM monitoring, DeFi analytics
func BenchmarkLog2PriceX96(b *testing.B) {
	for _, u := range benchLog2u128Inputs {
		name := fmt.Sprintf("Hi=%d_Lo=%d", u.Hi, u.Lo)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = Log2PriceX96(u)
			}
		})
	}
}

// BenchmarkLnPriceX96 measures Q64.96 → ln(price) transformation performance.
//
// Operations measured:
//   - Log2PriceX96 computation
//   - Base conversion multiplication (× ln(2))
//   - Error propagation handling
//
// Expected performance: <25ns per operation
// Use cases: Natural logarithm price analysis, financial mathematics integration
func BenchmarkLnPriceX96(b *testing.B) {
	for _, u := range benchLog2u128Inputs {
		name := fmt.Sprintf("Hi=%d_Lo=%d", u.Hi, u.Lo)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = LnPriceX96(u)
			}
		})
	}
}

// BenchmarkLogPriceX96Const measures Q64.96 → log_base(price) performance.
//
// Operations measured:
//   - Log2PriceX96 computation
//   - Custom base conversion multiplication
//   - Comprehensive input validation (NaN/Inf checking)
//
// Expected performance: <30ns per operation
// Use cases: Custom base price analysis, external system integration
func BenchmarkLogPriceX96Const(b *testing.B) {
	for _, u := range benchLog2u128Inputs {
		name := fmt.Sprintf("Hi=%d_Lo=%d", u.Hi, u.Lo)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = LogPriceX96Const(u, math.Pi)
			}
		})
	}
}
