// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: fastuni_bench_test.go — microbenchmarks for fastuni log routines
//
// Purpose:
//   - Measures execution time of ISR-grade logarithmic routines and wrappers
//   - Evaluates cost of raw log₂/ln on 64-bit and 128-bit ratios
//
// Scope:
//   - log₂(x) over u64 and Uint128
//   - LnReserveRatio, Log2ReserveRatio, Log*Const
//   - Q64.96 price field access and transformations
//
// Notes:
//   - Deterministic seed (benchSeed = 12345) for repeatable fuzz inputs
//   - Benchmarks grouped by function purpose and input style
//   - Uint128 coverage includes Lo-only, Hi-only, and hybrid values
//
// ⚠️ Internal routines (e.g. log2u128) are unchecked. Use with caution.
// ─────────────────────────────────────────────────────────────────────────────

package fastuni

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

/*──────────────────────────────────────────────────────────────────────────────
  📦 Benchmark Constants and Input Sets
──────────────────────────────────────────────────────────────────────────────*/

const benchSeed = 12345 // fixed for deterministic fuzz

var (
	// Sample uint64 values for log₂ benchmarking
	benchLog2u64Inputs = []uint64{
		1,
		3,
		1 << 10,
		1 << 32,
		123456789,
		1 << 52,
	}

	// Sample Uint128 values for Q64.96/log₂ benchmarking
	benchLog2u128Inputs = []Uint128{
		{0, 1},             // Lo-only
		{1 << 32, 0},       // Hi-only
		{1 << 40, 1 << 20}, // Mixed
	}

	// Random Uint128 values (fixed across runs)
	benchRandU128 = newRandU128(1024)

	// a/b pairs for ratio benchmarks
	benchPairs = []struct{ a, b uint64 }{
		{1, 1},             // identity
		{10000, 10001},     // small delta
		{1 << 30, 1 << 20}, // power-of-2 offset
		{12345, 67890},     // arbitrary
	}
)

/*──────────────────────────────────────────────────────────────────────────────
  🔧 Input Generator
──────────────────────────────────────────────────────────────────────────────*/

// newRandU128 returns N deterministic Uint128 values using benchSeed
func newRandU128(n int) []Uint128 {
	r := rand.New(rand.NewSource(benchSeed))
	slice := make([]Uint128, n)
	for i := range slice {
		slice[i] = Uint128{r.Uint64(), r.Uint64()}
	}
	return slice
}

/*──────────────────────────────────────────────────────────────────────────────
  🔬 log₂(x) over uint64
──────────────────────────────────────────────────────────────────────────────*/

// BenchmarkLog2u64 runs Log2ReserveRatio(x,1) for small and large x
func BenchmarkLog2u64(b *testing.B) {
	for _, x := range benchLog2u64Inputs {
		b.Run(fmt.Sprintf("x=%d", x), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = Log2ReserveRatio(x, 1)
			}
		})
	}
}

/*──────────────────────────────────────────────────────────────────────────────
  🔬 log₂(x) over Uint128 (fixed + fuzz)
──────────────────────────────────────────────────────────────────────────────*/

// BenchmarkLog2u128_Fixed benchmarks log2u128() on known Uint128 constants
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

// BenchmarkLog2u128_Random benchmarks fuzzed log2u128() input from pool
func BenchmarkLog2u128_Random(b *testing.B) {
	n := len(benchRandU128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u := benchRandU128[i%n]
		_ = log2u128(u)
	}
}

/*──────────────────────────────────────────────────────────────────────────────
  🔬 Reserve Ratio: ln(a/b)
──────────────────────────────────────────────────────────────────────────────*/

// BenchmarkLnReserveRatio tests LnReserveRatio on known pairs
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

/*──────────────────────────────────────────────────────────────────────────────
  🔬 Reserve Ratio: log₂(a/b)
──────────────────────────────────────────────────────────────────────────────*/

// BenchmarkLog2ReserveRatio tests log₂(a / b)
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

/*──────────────────────────────────────────────────────────────────────────────
  🔬 Reserve Ratio: ln(a/b) * constant
──────────────────────────────────────────────────────────────────────────────*/

// BenchmarkLogReserveRatioConst tests ln(a / b) * π
func BenchmarkLogReserveRatioConst(b *testing.B) {
	for _, p := range benchPairs {
		b.Run(fmt.Sprintf("%d_%d", p.a, p.b), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = LogReserveRatioConst(p.a, p.b, math.Pi)
			}
		})
	}
}

/*──────────────────────────────────────────────────────────────────────────────
  🔬 PriceX96 (Q64.96) log₂, ln, and scaled
──────────────────────────────────────────────────────────────────────────────*/

// BenchmarkLog2PriceX96 tests 2·log₂(sqrtPrice) − 192
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

// BenchmarkLnPriceX96 tests ln(price) via sqrtPrice log₂
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

// BenchmarkLogPriceX96Const tests ln(price) * π
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
