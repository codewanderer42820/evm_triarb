// fastuni_bench_test.go — microbenchmarks for fastuni high-performance log/ratio routines.
//
// This file includes targeted performance benchmarks for both internal and exported
// functions, evaluating log₂, ln, and constant-multiplied variants under various inputs.

package fastuni

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

/*─────────────────── benchmark setup ───────────────────*/

// benchSeed ensures deterministic benchmark inputs
const benchSeed = 12345

var (
	// Sample uint64 values for benchmarking log₂(x) on 64-bit integers
	benchLog2u64Inputs = []uint64{
		1,
		3,
		1 << 10,
		1 << 32,
		123456789,
		1 << 52,
	}

	// Sample Uint128 values for benchmarking log₂(x) on 128-bit integers
	benchLog2u128Inputs = []Uint128{
		{0, 1},             // lo-only
		{1 << 32, 0},       // hi-only
		{1 << 40, 1 << 20}, // mixed
	}

	// Random Uint128 values (fixed seed) for fuzz-style benchmarking
	benchRandU128 = newRandU128(1024)

	// Pairs (a,b) for benchmarking reserve ratio computations
	benchPairs = []struct{ a, b uint64 }{
		{1, 1},             // identity
		{10000, 10001},     // small delta
		{1 << 30, 1 << 20}, // power-of-2 delta
		{12345, 67890},     // arbitrary values
	}
)

// newRandU128 returns N deterministic random Uint128 values
func newRandU128(n int) []Uint128 {
	r := rand.New(rand.NewSource(benchSeed))
	slice := make([]Uint128, n)
	for i := range slice {
		slice[i] = Uint128{r.Uint64(), r.Uint64()}
	}
	return slice
}

/*─────────────────── log₂ (u64) benchmarks ───────────────────*/

// BenchmarkLog2u64 benchmarks log₂(x) on pure 64-bit inputs
func BenchmarkLog2u64(b *testing.B) {
	for _, x := range benchLog2u64Inputs {
		b.Run(fmt.Sprintf("x=%d", x), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = Log2ReserveRatio(x, 1)
			}
		})
	}
}

/*─────────────────── log₂ (u128) benchmarks ───────────────────*/

// BenchmarkLog2u128_Fixed benchmarks fixed Uint128 inputs to log2u128
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

// BenchmarkLog2u128_Random benchmarks randomized Uint128 values (same each run)
func BenchmarkLog2u128_Random(b *testing.B) {
	n := len(benchRandU128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u := benchRandU128[i%n]
		_ = log2u128(u)
	}
}

/*─────────────────── reserve-ratio benchmarks ───────────────────*/

// BenchmarkLnReserveRatio measures performance of natural log ratio
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

// BenchmarkLog2ReserveRatio measures log₂(a/b) execution
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

// BenchmarkLogReserveRatioConst tests ln(a/b) * conv
func BenchmarkLogReserveRatioConst(b *testing.B) {
	for _, p := range benchPairs {
		b.Run(fmt.Sprintf("%d_%d", p.a, p.b), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = LogReserveRatioConst(p.a, p.b, math.Pi)
			}
		})
	}
}

/*─────────────────── price conversion benchmarks ───────────────────*/

// BenchmarkLog2PriceX96 benchmarks log₂(price) on fixed-point Q64.96
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

// BenchmarkLnPriceX96 benchmarks ln(price) on Q64.96 inputs
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

// BenchmarkLogPriceX96Const tests ln(price) * conv on Q64.96 inputs
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
