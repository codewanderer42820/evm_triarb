// fastuni_bench_test.go — microbenchmarks for fastuni footgun routines.
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
	benchLog2u64Inputs  = []uint64{1, 3, 1 << 10, 1 << 32, 123456789, 1 << 52}
	benchLog2u128Inputs = []Uint128{{0, 1}, {1 << 32, 0}, {1 << 40, 1 << 20}}
	benchRandU128       = newRandU128(1024)
	benchPairs          = []struct{ a, b uint64 }{
		{1, 1},
		{10000, 10001},
		{1 << 30, 1 << 20},
		{12345, 67890},
	}
)

func newRandU128(n int) []Uint128 {
	r := rand.New(rand.NewSource(benchSeed))
	slice := make([]Uint128, n)
	for i := range slice {
		slice[i] = Uint128{r.Uint64(), r.Uint64()}
	}
	return slice
}

/*─────────────────── log₂ (u64) benchmarks ───────────────────*/
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
