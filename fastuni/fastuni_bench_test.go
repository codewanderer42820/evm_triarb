// fastuni_bench_test.go — microbenchmarks for fastuni footgun routines.
// -----------------------------------------------------------
// Benchmarks core functions: log₂, reserve-ratio, and price conversions.
// Use `go test -bench=.` to run all benchmarks.

package fastuni

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

/*─────────────────── benchmark setup ───────────────────*/
const benchSeed = 12345

var (
	// representative inputs for log₂(u64)
	benchLog2u64Inputs = []uint64{1, 3, 1 << 10, 1 << 32, 123456789, 1 << 52}
	// representative inputs for log₂(u128)
	benchLog2u128Inputs = []Uint128{{0, 1}, {1 << 32, 0}, {1 << 40, 1 << 20}}
	// random Uint128 samples
	benchRandU128 = newRandU128(1024)
	// representative pairs for reserve-ratio
	benchPairs = []struct{ a, b uint64 }{
		{1, 1},
		{10000, 10001},
		{1 << 30, 1 << 20},
		{12345, 67890},
	}
)

// newRandU128 generates n pseudorandom Uint128 values.
func newRandU128(n int) []Uint128 {
	r := rand.New(rand.NewSource(benchSeed))
	slice := make([]Uint128, n)
	for i := range slice {
		slice[i] = Uint128{r.Uint64(), r.Uint64()}
	}
	return slice
}

/*─────────────────── log₂ (u64) benchmarks ───────────────────*/
// Benchmarks log2u64 for fixed inputs.
func BenchmarkLog2u64(b *testing.B) {
	for _, x := range benchLog2u64Inputs {
		b.Run(fmt.Sprintf("x=%d", x), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = log2u64(x)
			}
		})
	}
}

/*─────────────────── log₂ (u128) benchmarks ───────────────────*/
// Benchmarks log2u128 for fixed and random inputs.
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

func BenchmarkLog2u128_Random(b *testing.B) {
	n := len(benchRandU128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u := benchRandU128[i%n]
		_ = log2u128(u)
	}
}

/*─────────────────── reserve-ratio benchmarks ───────────────────*/
// Benchmarks LnReserveRatio, Log2ReserveRatio, and LogReserveRatioConst.
func BenchmarkLnReserveRatio(b *testing.B) {
	for _, p := range benchPairs {
		name := fmt.Sprintf("%d_%d", p.a, p.b)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = LnReserveRatio(p.a, p.b)
			}
		})
	}
}

func BenchmarkLog2ReserveRatio(b *testing.B) {
	for _, p := range benchPairs {
		name := fmt.Sprintf("%d_%d", p.a, p.b)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = Log2ReserveRatio(p.a, p.b)
			}
		})
	}
}

func BenchmarkLogReserveRatioConst(b *testing.B) {
	for _, p := range benchPairs {
		b.Run(fmt.Sprintf("%d_%d", p.a, p.b), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = LogReserveRatioConst(p.a, p.b, math.Pi)
			}
		})
	}
}

/*─────────────────── price conversion benchmarks ───────────────────*/
// Benchmarks Log2PriceX96, LnPriceX96, and LogPriceX96Const for representative sqrtPrices.
func BenchmarkLog2PriceX96(b *testing.B) {
	for _, u := range benchLog2u128Inputs {
		name := fmt.Sprintf("Hi=%d_Lo=%d", u.Hi, u.Lo)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = Log2PriceX96(u)
			}
		})
	}
}

func BenchmarkLnPriceX96(b *testing.B) {
	for _, u := range benchLog2u128Inputs {
		name := fmt.Sprintf("Hi=%d_Lo=%d", u.Hi, u.Lo)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = LnPriceX96(u)
			}
		})
	}
}

func BenchmarkLogPriceX96Const(b *testing.B) {
	for _, u := range benchLog2u128Inputs {
		name := fmt.Sprintf("Hi=%d_Lo=%d", u.Hi, u.Lo)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = LogPriceX96Const(u, math.Pi)
			}
		})
	}
}
