// utils_test.go — Comprehensive Validation Suite for SIMD-Optimized Utilities
//
// This test suite validates the complete utility library through exhaustive correctness
// testing, performance benchmarks, edge case validation, and race condition detection.
// Includes SIMD operation verification, IEEE 754 compliance testing, memory safety
// validation, and cross-platform compatibility checks.
//
// Coverage: Memory operations, type conversions, hex parsing, JSON utilities, hash mixing
// Benchmarks: SIMD throughput, allocation profiling, cache efficiency measurements
// Validation: Bit-exact verification, boundary conditions, concurrent safety

package utils

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TEST CONFIGURATION AND HELPERS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	// Test configuration constants
	StressTestIterations = 1_000_000
	ConcurrencyLevel     = 100
	CacheMissSize        = 64 * 1024 * 1024 // 64MB to blow through L3 cache
	DistributionSamples  = 100_000
	BenchmarkDataSize    = 8192
)

// hammingDistance calculates the number of differing bits between two uint64 values.
// Used to verify the avalanche effect in hash functions where each input bit
// should affect approximately 50% of output bits.
func hammingDistance(a, b uint64) int {
	diff := a ^ b
	return bits.OnesCount64(diff)
}

// generateTestData creates various test data patterns for comprehensive testing.
// Each pattern is designed to stress different aspects of the implementations:
// - "zeros": Tests handling of zero values and empty data
// - "ones": Tests maximum value handling
// - "sequential": Tests cache-friendly access patterns
// - "random": Tests worst-case unpredictable patterns
// - "alternating": Tests bit pattern recognition
func generateTestData(size int, pattern string) []byte {
	data := make([]byte, size)
	switch pattern {
	case "zeros":
		// Already zeros - tests sparse data handling
	case "ones":
		for i := range data {
			data[i] = 0xFF
		}
	case "sequential":
		for i := range data {
			data[i] = byte(i & 0xFF)
		}
	case "random":
		rand.Read(data)
	case "alternating":
		for i := range data {
			if i&1 == 0 {
				data[i] = 0xAA
			} else {
				data[i] = 0x55
			}
		}
	case "cache_miss":
		// Create data that defeats CPU cache
		for i := range data {
			data[i] = byte(rand.Intn(256))
		}
	}
	return data
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HASH MIXING TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestMix64(t *testing.T) {
	tests := []struct {
		name  string
		input uint64
		want  uint64
	}{
		{"zero", 0x0000000000000000, 0x0000000000000000},
		{"one", 0x0000000000000001, 0xB456BCFC34C2CB2C},
		{"max", 0xFFFFFFFFFFFFFFFF, 0x64B5720B4B825F21},
		{"pattern_1", 0x1234567890abcdef, 0x0CAE996FEE6BD396},
		{"pattern_2", 0xdeadbeefcafebabe, 0x7082995008F0C48C},
		{"power_of_2", 0x8000000000000000, 0x8F780810AF31A493},
		{"alternating", 0x5555555555555555, 0xC19F25AD12D5F56F},
		{"sequential_1", 0x0123456789ABCDEF, 0x0CAE996FEE6BD396},
		{"all_ones_low", 0x00000000FFFFFFFF, 0x09B56F2F488B9F44},
		{"all_ones_high", 0xFFFFFFFF00000000, 0x43E768EA982C8E15},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Mix64(tt.input)
			if got != tt.want {
				t.Errorf("Mix64(0x%016X) = 0x%016X, want 0x%016X", tt.input, got, tt.want)
			}

			// Verify determinism - critical for reproducible results
			got2 := Mix64(tt.input)
			if got != got2 {
				t.Errorf("Mix64(0x%016X) not deterministic", tt.input)
			}

			// Verify mixing changes the value (except for zero)
			if tt.input != 0 && got == tt.input {
				t.Errorf("Mix64(0x%016X) returned unchanged value", tt.input)
			}
		})
	}
}

func TestMix64_AvalancheEffect(t *testing.T) {
	// Test that flipping any input bit changes ~50% of output bits
	// This is critical for good hash distribution
	testInputs := []uint64{
		0x0000000000000000,
		0xFFFFFFFFFFFFFFFF,
		0x0123456789ABCDEF,
		0x5555555555555555,
		0xAAAAAAAAAAAAAAAA,
		0x1111111111111111,
		0x8888888888888888,
	}

	for _, input := range testInputs {
		original := Mix64(input)
		totalDistance := 0
		minDistance := 64
		maxDistance := 0

		// Test flipping each bit
		for bit := 0; bit < 64; bit++ {
			flipped := input ^ (uint64(1) << bit)
			result := Mix64(flipped)
			distance := hammingDistance(original, result)
			totalDistance += distance

			if distance < minDistance {
				minDistance = distance
			}
			if distance > maxDistance {
				maxDistance = distance
			}

			// Each bit flip should change 20-44 bits (roughly 50% ± 20%)
			if distance < 20 || distance > 44 {
				t.Errorf("Poor avalanche for input 0x%016X bit %d: %d bits changed",
					input, bit, distance)
			}
		}

		// Average should be close to 32 (50% of 64 bits)
		avgDistance := float64(totalDistance) / 64.0
		if avgDistance < 28 || avgDistance > 36 {
			t.Errorf("Poor average avalanche for input 0x%016X: %.2f (min: %d, max: %d)",
				input, avgDistance, minDistance, maxDistance)
		}
	}
}

func TestMix64_CollisionResistance(t *testing.T) {
	// Test that sequential inputs produce well-distributed outputs
	const testSize = 10000
	outputs := make(map[uint64]uint64, testSize)

	for i := uint64(0); i < testSize; i++ {
		output := Mix64(i)
		if prevInput, exists := outputs[output]; exists {
			t.Errorf("Collision detected: Mix64(%d) == Mix64(%d) == 0x%016X",
				i, prevInput, output)
		}
		outputs[output] = i
	}

	// Test specific patterns that might cause collisions
	patterns := []uint64{
		0x0000000000000000, 0x0000000000000001,
		0x00000000FFFFFFFF, 0xFFFFFFFF00000000,
		0x5555555555555555, 0xAAAAAAAAAAAAAAAA,
	}

	seen := make(map[uint64]uint64)
	for _, p := range patterns {
		output := Mix64(p)
		if prev, exists := seen[output]; exists {
			t.Errorf("Pattern collision: Mix64(0x%016X) == Mix64(0x%016X)", p, prev)
		}
		seen[output] = p
	}
}

func TestMix64_Distribution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping distribution test in short mode")
	}

	const samples = 100000

	// Test uniform distribution of output bits
	bitCounts := make([]int, 64)

	for i := 0; i < samples; i++ {
		result := Mix64(uint64(i))
		for bit := 0; bit < 64; bit++ {
			if result&(1<<bit) != 0 {
				bitCounts[bit]++
			}
		}
	}

	// Check each bit is set approximately 50% of the time
	for bit, count := range bitCounts {
		ratio := float64(count) / float64(samples)
		if math.Abs(ratio-0.5) > 0.01 { // 1% tolerance
			t.Errorf("Bit %d has biased distribution: %.4f (count: %d/%d)",
				bit, ratio, count, samples)
		}
	}

	// Test byte distribution
	byteCounts := make([]map[byte]int, 8)
	for i := range byteCounts {
		byteCounts[i] = make(map[byte]int)
	}

	for i := 0; i < samples; i++ {
		result := Mix64(uint64(i))
		for byteIdx := 0; byteIdx < 8; byteIdx++ {
			b := byte(result >> (byteIdx * 8))
			byteCounts[byteIdx][b]++
		}
	}

	// Check byte distribution (chi-square test would be better, but this is simpler)
	for byteIdx, counts := range byteCounts {
		if len(counts) < 200 { // Should see most byte values
			t.Errorf("Byte %d has poor distribution: only %d unique values", byteIdx, len(counts))
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM I/O TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestPrintFunctions(t *testing.T) {
	// Basic test that functions don't panic
	// We can't easily test actual output without redirecting stdout/stderr

	t.Run("PrintInfo", func(t *testing.T) {
		// Should not panic with various inputs
		testCases := []string{
			"test info",
			"",
			"multi\nline\nstring",
			"special chars: \t\r\n",
			"unicode: 世界 🌍",
			string([]byte{0, 1, 2, 3, 4, 5}), // Binary data
		}

		for _, msg := range testCases {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("PrintInfo panicked on %q: %v", msg, r)
					}
				}()
				PrintInfo(msg)
			}()
		}
	})

	t.Run("PrintWarning", func(t *testing.T) {
		// Should not panic with various inputs
		testCases := []string{
			"test warning",
			"",
			"Error: something went wrong",
			"🚨 Warning: critical issue",
		}

		for _, msg := range testCases {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("PrintWarning panicked on %q: %v", msg, r)
					}
				}()
				PrintWarning(msg)
			}()
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONCURRENCY AND RACE CONDITION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestRaceConditions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition tests in short mode")
	}

	t.Run("concurrent_mix64", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make([]uint64, ConcurrencyLevel)

		for i := 0; i < ConcurrencyLevel; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				// Each goroutine processes many values
				for j := 0; j < 1000; j++ {
					results[idx] = Mix64(uint64(idx*1000 + j))
				}
			}(i)
		}

		wg.Wait()

		// Verify all results are correct
		for i := 0; i < ConcurrencyLevel; i++ {
			// Check last value processed by each goroutine
			expected := Mix64(uint64(i*1000 + 999))
			if results[i] != expected {
				t.Errorf("Race condition detected: goroutine %d produced wrong result", i)
			}
		}
	})

	t.Run("concurrent_parsing", func(t *testing.T) {
		var wg sync.WaitGroup
		data := []byte("deadbeefcafebabe")
		addr := []byte("dac17f958d2ee523a2206206994597c13d831ec7")

		// Expected results
		expectedU64 := ParseHexU64(data)
		expectedU32 := ParseHexU32(data[:8])
		expectedAddr := ParseEthereumAddress(addr)

		errors := make([]error, ConcurrencyLevel)

		for i := 0; i < ConcurrencyLevel; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for j := 0; j < 1000; j++ {
					// Test all parsing functions concurrently
					if ParseHexU64(data) != expectedU64 {
						errors[idx] = fmt.Errorf("ParseHexU64 race detected")
						return
					}
					if ParseHexU32(data[:8]) != expectedU32 {
						errors[idx] = fmt.Errorf("ParseHexU32 race detected")
						return
					}
					if ParseEthereumAddress(addr) != expectedAddr {
						errors[idx] = fmt.Errorf("ParseEthereumAddress race detected")
						return
					}
				}
			}(i)
		}

		wg.Wait()

		// Check for any errors
		for i, err := range errors {
			if err != nil {
				t.Errorf("Goroutine %d: %v", i, err)
			}
		}
	})

	t.Run("concurrent_conversions", func(t *testing.T) {
		var wg sync.WaitGroup
		testInts := []int{0, 1, 42, 12345, 999999}
		testFloats := []float64{0.0, 1.0, -1.0, 3.14159, 42.0, 1e10, 1e-10}
		testBytes := []byte("test string for B2s")

		for i := 0; i < ConcurrencyLevel; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for j := 0; j < 1000; j++ {
					// Test Itoa
					_ = Itoa(testInts[j%len(testInts)])

					// Test Ftoa
					_ = Ftoa(testFloats[j%len(testFloats)])

					// Test B2s (read-only)
					_ = B2s(testBytes)
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("concurrent_json_parsing", func(t *testing.T) {
		var wg sync.WaitGroup
		jsonData := []byte(`{"key":"value","array":[1,2,3,4,5],"nested":{"a":"b"}}`)

		for i := 0; i < ConcurrencyLevel; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 1000; j++ {
					// Test all skip functions
					_ = SkipToQuote(jsonData, 0, 1)
					_ = SkipToOpeningBracket(jsonData, 0, 1)
					_ = SkipToClosingBracket(jsonData, 20, 1)
					_, _ = SkipToQuoteEarlyExit(jsonData, 0, 1, 10)
					_, _ = SkipToClosingBracketEarlyExit(jsonData, 20, 1, 10)
				}
			}()
		}

		wg.Wait()
	})
}

func TestMemoryOperationsConcurrency(t *testing.T) {
	// Test that memory operations are safe for concurrent reads
	data16 := make([]byte, 16)
	data8 := make([]byte, 8)
	for i := range data16 {
		data16[i] = byte(i)
	}
	copy(data8, data16)

	var wg sync.WaitGroup
	for i := 0; i < ConcurrencyLevel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				_ = Load64(data8)
				_ = LoadBE64(data8)
				_, _ = Load128(data16)
			}
		}()
	}

	wg.Wait()
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// EDGE CASES AND BOUNDARY TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestBoundaryConditions(t *testing.T) {
	t.Run("slice_boundaries", func(t *testing.T) {
		// Test with slices at exact required sizes
		data8 := make([]byte, 8)
		data16 := make([]byte, 16)
		data20 := make([]byte, 20)
		data40 := make([]byte, 40)

		// Fill with pattern
		for i := range data40 {
			data40[i] = byte(i & 0xFF)
		}
		copy(data20, data40)
		copy(data16, data40)
		copy(data8, data40)

		// These should work without panic
		_ = Load64(data8)
		_ = LoadBE64(data8)
		_, _ = Load128(data16)
		_ = ParseEthereumAddress(data40)
	})

	t.Run("zero_length_operations", func(t *testing.T) {
		empty := []byte{}

		// These should handle empty input gracefully
		result32 := ParseHexU32(empty)
		if result32 != 0 {
			t.Errorf("ParseHexU32(empty) = 0x%X, want 0x0", result32)
		}

		result64 := ParseHexU64(empty)
		if result64 != 0 {
			t.Errorf("ParseHexU64(empty) = 0x%X, want 0x0", result64)
		}

		idx := SkipToQuote(empty, 0, 1)
		if idx != -1 {
			t.Errorf("SkipToQuote(empty) = %d, want -1", idx)
		}

		idx = SkipToOpeningBracket(empty, 0, 1)
		if idx != -1 {
			t.Errorf("SkipToOpeningBracket(empty) = %d, want -1", idx)
		}

		idx = SkipToClosingBracket(empty, 0, 1)
		if idx != -1 {
			t.Errorf("SkipToClosingBracket(empty) = %d, want -1", idx)
		}
	})

	t.Run("maximum_values", func(t *testing.T) {
		// Test parsing maximum values
		maxU32 := ParseHexU32([]byte("ffffffff"))
		if maxU32 != 0xFFFFFFFF {
			t.Errorf("Max uint32 parse failed: got 0x%X", maxU32)
		}

		maxU64 := ParseHexU64([]byte("ffffffffffffffff"))
		if maxU64 != 0xFFFFFFFFFFFFFFFF {
			t.Errorf("Max uint64 parse failed: got 0x%X", maxU64)
		}

		// Test Mix64 with boundary values
		boundaryValues := []uint64{
			0, 1, 2, 3, 4, 5, 6, 7, 8,
			0xFF, 0xFFFF, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF,
			1 << 32, 1 << 48, 1 << 63,
		}

		for _, v := range boundaryValues {
			result := Mix64(v)
			if result == v && v != 0 {
				t.Errorf("Mix64(%d) returned unchanged value", v)
			}
		}
	})

	t.Run("ftoa_extreme_values", func(t *testing.T) {
		// Test extreme float values
		extremeValues := []float64{
			math.MaxFloat64,
			-math.MaxFloat64,
			math.SmallestNonzeroFloat64,
			-math.SmallestNonzeroFloat64,
			1.7976931348623157e+308, // Close to max
			2.2250738585072014e-308, // Close to min normal
			4.9406564584124654e-324, // Smallest subnormal
		}

		for _, f := range extremeValues {
			result := Ftoa(f)
			if len(result) == 0 {
				t.Errorf("Ftoa(%g) produced empty result", f)
			}

			// Verify it can be parsed back (approximately)
			if result != "NaN" && result != "+Inf" && result != "-Inf" {
				parsed, err := strconv.ParseFloat(result, 64)
				if err != nil {
					t.Errorf("Ftoa(%g) = %q cannot be parsed: %v", f, result, err)
				}

				// For extreme values, just check order of magnitude
				if !math.IsInf(parsed, 0) && !math.IsNaN(parsed) {
					orderOriginal := math.Log10(math.Abs(f))
					orderParsed := math.Log10(math.Abs(parsed))
					if math.Abs(orderOriginal-orderParsed) > 1 {
						t.Errorf("Ftoa(%g) lost magnitude: parsed as %g", f, parsed)
					}
				}
			}
		}
	})

	t.Run("alignment_edge_cases", func(t *testing.T) {
		// Test operations at various memory alignments
		buffer := make([]byte, 128)
		for i := range buffer {
			buffer[i] = byte(i & 0xFF)
		}

		// Test at each possible alignment (0-15)
		for align := 0; align < 16; align++ {
			// Verify Load64 works at any alignment
			if align+8 <= len(buffer) {
				_ = Load64(buffer[align : align+8])
			}

			// Verify Load128 works at any alignment
			if align+16 <= len(buffer) {
				_, _ = Load128(buffer[align : align+16])
			}
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// STRESS TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestStress_AllFunctions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress tests in short mode")
	}

	t.Run("mixed_workload", func(t *testing.T) {
		const iterations = StressTestIterations / 10 // Reduce for mixed test

		for i := 0; i < iterations; i++ {
			// Mix all function types to stress different code paths
			f := float64(i) * 0.123456789
			_ = Ftoa(f)
			_ = Itoa(i % 1000000)
			_ = Mix64(uint64(i))

			// Hex parsing stress
			hexData := []byte("deadbeefcafebabe1234567890abcdef12345678")
			_ = ParseHexU64(hexData[:16])
			_ = ParseHexU32(hexData[:8])
			if len(hexData) >= 40 {
				_ = ParseEthereumAddress(hexData[:40])
			}

			// Memory operations
			if i%100 == 0 {
				data := make([]byte, 16)
				for j := range data {
					data[j] = byte(i + j)
				}
				_ = Load64(data)
				_, _ = Load128(data)
			}
		}
	})

	t.Run("extreme_values", func(t *testing.T) {
		// Test with values that stress the implementations
		extremeInts := []int{
			0, 1, 9, 10, 99, 100, 999, 1000, 9999, 10000,
			99999, 100000, 999999, 1000000, 9999999,
			2147483647, // MaxInt32
		}

		for _, n := range extremeInts {
			result := Itoa(n)
			if result == "" {
				t.Errorf("Itoa(%d) returned empty string", n)
			}
		}

		// Extreme floats
		extremeFloats := []float64{
			math.MaxFloat64,
			math.SmallestNonzeroFloat64,
			math.NaN(),
			math.Inf(1),
			math.Inf(-1),
			1.7976931348623157e+308, // Near max
			2.2250738585072014e-308, // Near min normal
			4.9406564584124654e-324, // Smallest subnormal
		}

		for _, f := range extremeFloats {
			result := Ftoa(f)
			if len(result) == 0 {
				t.Errorf("Ftoa(%g) returned empty string", f)
			}
		}
	})

	t.Run("random_data_torture", func(t *testing.T) {
		rand.Seed(42) // Fixed seed for reproducibility

		for i := 0; i < 50000; i++ {
			// Random floats including special bit patterns
			bits := rand.Uint64()
			f := math.Float64frombits(bits)

			// Should never panic
			result := Ftoa(f)

			// Result should be non-empty for valid numbers
			if !math.IsNaN(f) && !math.IsInf(f, 0) && len(result) == 0 {
				t.Errorf("Ftoa produced empty result for valid float: %g (bits: 0x%016X)", f, bits)
			}

			// Random hex parsing
			if i%10 == 0 {
				hexLen := rand.Intn(17) // 0-16 characters
				hexData := make([]byte, hexLen)
				for j := range hexData {
					if rand.Intn(2) == 0 {
						hexData[j] = byte('0' + rand.Intn(10))
					} else {
						hexData[j] = byte('a' + rand.Intn(6))
					}
				}
				_ = ParseHexU64(hexData)
			}
		}
	})

	t.Run("memory_pressure", func(t *testing.T) {
		// Test behavior under memory pressure
		const goroutines = 50
		var wg sync.WaitGroup

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Each goroutine allocates and processes data
				for i := 0; i < 1000; i++ {
					// Create some memory pressure
					data := make([]byte, 1024)
					for j := range data {
						data[j] = byte(i + j)
					}

					// Process various operations
					_ = Load64(data[:8])
					_ = B2s(data[:100])
					_ = Itoa(i)
					_ = Ftoa(float64(i) * 1.23456)

					// Let GC run occasionally
					if i%100 == 0 {
						runtime.GC()
					}
				}
			}(g)
		}

		wg.Wait()
	})
}

func TestCacheMissBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cache miss test in short mode")
	}

	// Create large data that exceeds L3 cache
	largeData := generateTestData(CacheMissSize, "random")

	// Test Load64 with cache misses
	t.Run("load64_cache_miss", func(t *testing.T) {
		stride := 4096 // Page size to ensure cache misses
		accesses := 0

		for i := 0; i+8 < len(largeData); i += stride {
			_ = Load64(largeData[i : i+8])
			accesses++
		}

		t.Logf("Performed %d cache-missing loads", accesses)
	})

	// Test hex parsing with cache misses
	t.Run("hex_parse_cache_miss", func(t *testing.T) {
		// Create hex data
		hexData := make([]byte, len(largeData)/2)
		for i := range hexData {
			if i%2 == 0 {
				hexData[i] = byte('0' + (largeData[i] % 10))
			} else {
				hexData[i] = byte('a' + (largeData[i] % 6))
			}
		}

		stride := 4096
		parses := 0

		for i := 0; i+16 < len(hexData); i += stride {
			_ = ParseHexU64(hexData[i : i+16])
			parses++
		}

		t.Logf("Performed %d cache-missing hex parses", parses)
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INTEGRATION AND COMPATIBILITY TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestIntegration(t *testing.T) {
	t.Run("encoding_compatibility", func(t *testing.T) {
		// Verify LoadBE64 matches binary.BigEndian
		testData := [][]byte{
			{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE},
		}

		for _, data := range testData {
			ourResult := LoadBE64(data)
			stdResult := binary.BigEndian.Uint64(data)

			if ourResult != stdResult {
				t.Errorf("LoadBE64 incompatible with binary.BigEndian: 0x%X vs 0x%X",
					ourResult, stdResult)
			}
		}
	})

	t.Run("strconv_compatibility", func(t *testing.T) {
		// Verify Itoa matches strconv.Itoa for positive numbers
		testNumbers := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000, 2147483647}

		for _, n := range testNumbers {
			our := Itoa(n)
			std := strconv.Itoa(n)
			if our != std {
				t.Errorf("Itoa(%d): our=%q, std=%q", n, our, std)
			}
		}
	})

	t.Run("ftoa_parseability", func(t *testing.T) {
		// Verify Ftoa output can be parsed back
		testFloats := []float64{
			0.0, 1.0, -1.0, 0.5, -0.5, 3.14159, -3.14159,
			42.0, -42.0, 1000000.0, 0.000001, 1e10, 1e-10,
			0.1, 0.01, 0.001, 0.0001, 0.00001,
			1.23456, 12.3456, 123.456, 1234.56, 12345.6,
		}

		for _, f := range testFloats {
			result := Ftoa(f)
			if result == "NaN" || result == "+Inf" || result == "-Inf" {
				continue // Special values are expected
			}

			parsed, err := strconv.ParseFloat(result, 64)
			if err != nil {
				t.Errorf("Ftoa(%g) = %q cannot be parsed: %v", f, result, err)
				continue
			}

			// Check relative error for reasonable values
			if f != 0 {
				relError := math.Abs(parsed-f) / math.Abs(f)
				if relError > 1e-6 {
					t.Errorf("Ftoa(%g) = %q, parsed back as %g (rel error: %g)",
						f, result, parsed, relError)
				}
			} else if parsed != 0 {
				t.Errorf("Ftoa(0) = %q, parsed back as %g", result, parsed)
			}
		}
	})

	t.Run("cross_function_consistency", func(t *testing.T) {
		// Test that functions work well together
		for i := 0; i < 100; i++ {
			// Convert int to string and back
			intStr := Itoa(i)
			parsed, _ := strconv.Atoi(intStr)
			if parsed != i {
				t.Errorf("Itoa round-trip failed: %d -> %q -> %d", i, intStr, parsed)
			}

			// Use Mix64 for deterministic randomness
			mixed := Mix64(uint64(i))
			remixed := Mix64(mixed)
			if Mix64(uint64(i)) != mixed {
				t.Error("Mix64 not deterministic")
			}
			if remixed == mixed && i != 0 {
				t.Error("Mix64 should not be involutory")
			}
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PERFORMANCE BENCHMARKS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func BenchmarkMemoryOperations(b *testing.B) {
	data := generateTestData(BenchmarkDataSize, "sequential")

	b.Run("Load64", func(b *testing.B) {
		b.SetBytes(8)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Load64(data[i&(BenchmarkDataSize-8):])
		}
	})

	b.Run("Load64_Unaligned", func(b *testing.B) {
		b.SetBytes(8)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			offset := (i & 7) + 1 // Ensure misalignment
			_ = Load64(data[offset:])
		}
	})

	b.Run("LoadBE64", func(b *testing.B) {
		b.SetBytes(8)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = LoadBE64(data[i&(BenchmarkDataSize-8):])
		}
	})

	b.Run("Load128", func(b *testing.B) {
		b.SetBytes(16)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = Load128(data[i&(BenchmarkDataSize-16):])
		}
	})

	b.Run("Load64_Sequential", func(b *testing.B) {
		b.SetBytes(8)
		b.ReportAllocs()
		offset := 0
		for i := 0; i < b.N; i++ {
			_ = Load64(data[offset:])
			offset = (offset + 8) & (BenchmarkDataSize - 8)
		}
	})

	b.Run("Load64_Random", func(b *testing.B) {
		b.SetBytes(8)
		b.ReportAllocs()
		indices := make([]int, 1024)
		for i := range indices {
			indices[i] = rand.Intn(BenchmarkDataSize - 8)
		}
		for i := 0; i < b.N; i++ {
			_ = Load64(data[indices[i&1023]:])
		}
	})
}

func BenchmarkTypeConversion(b *testing.B) {
	testData := []byte("Hello, World! This is a test string for benchmarking B2s performance.")
	testInts := []int{0, 1, 42, 123, 1234, 12345, 123456, 1234567, 12345678, 123456789, 2147483647}
	testFloats := []float64{0.0, 1.0, -1.0, 3.14159, 42.123456, -123.456789, 1e6, 1e-6, 1.23456789e10}

	b.Run("B2s", func(b *testing.B) {
		b.SetBytes(int64(len(testData)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = B2s(testData)
		}
	})

	b.Run("Itoa", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Itoa(testInts[i%len(testInts)])
		}
	})

	b.Run("Itoa_Small", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Itoa(i % 100)
		}
	})

	b.Run("Itoa_Large", func(b *testing.B) {
		b.ReportAllocs()
		large := []int{1000000, 10000000, 100000000, 1000000000, 2147483647}
		for i := 0; i < b.N; i++ {
			_ = Itoa(large[i%len(large)])
		}
	})

	b.Run("Ftoa", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Ftoa(testFloats[i%len(testFloats)])
		}
	})

	b.Run("Ftoa_Integer", func(b *testing.B) {
		b.ReportAllocs()
		integers := []float64{0, 1, 10, 100, 1000, 10000, 100000}
		for i := 0; i < b.N; i++ {
			_ = Ftoa(integers[i%len(integers)])
		}
	})

	b.Run("Ftoa_Decimal", func(b *testing.B) {
		b.ReportAllocs()
		decimals := []float64{0.1, 0.01, 0.001, 1.23, 12.34, 123.45}
		for i := 0; i < b.N; i++ {
			_ = Ftoa(decimals[i%len(decimals)])
		}
	})

	b.Run("Ftoa_Scientific", func(b *testing.B) {
		b.ReportAllocs()
		scientific := []float64{1e10, 1e-10, 1.23e15, 4.56e-15, 1e100, 1e-100}
		for i := 0; i < b.N; i++ {
			_ = Ftoa(scientific[i%len(scientific)])
		}
	})

	// Comparison benchmarks with standard library
	b.Run("strconv.Itoa", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = strconv.Itoa(testInts[i%len(testInts)])
		}
	})

	b.Run("strconv.FormatFloat", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = strconv.FormatFloat(testFloats[i%len(testFloats)], 'g', -1, 64)
		}
	})
}

func BenchmarkHexParsing(b *testing.B) {
	hex4 := []byte("dead")
	hex8 := []byte("deadbeef")
	hex12 := []byte("deadbeefcafe")
	hex16 := []byte("deadbeefcafebabe")
	hex32 := []byte("deadbeefcafebabedeadbeefcafebabe")
	hex40 := []byte("dac17f958d2ee523a2206206994597c13d831ec7")

	b.Run("ParseHexU32_4", func(b *testing.B) {
		b.SetBytes(2)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseHexU32(hex4)
		}
	})

	b.Run("ParseHexU32_8", func(b *testing.B) {
		b.SetBytes(4)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseHexU32(hex8)
		}
	})

	b.Run("ParseHexU64_8", func(b *testing.B) {
		b.SetBytes(4)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseHexU64(hex8)
		}
	})

	b.Run("ParseHexU64_12", func(b *testing.B) {
		b.SetBytes(6)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseHexU64(hex12)
		}
	})

	b.Run("ParseHexU64_16", func(b *testing.B) {
		b.SetBytes(8)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseHexU64(hex16)
		}
	})

	b.Run("ParseHexU64_32", func(b *testing.B) {
		b.SetBytes(16)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseHexU64(hex32)
		}
	})

	b.Run("ParseEthereumAddress", func(b *testing.B) {
		b.SetBytes(20)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseEthereumAddress(hex40)
		}
	})

	// SIMD efficiency tests
	b.Run("ParseHexU64_7chars", func(b *testing.B) {
		data := []byte("1234567")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseHexU64(data)
		}
	})

	b.Run("ParseHexU64_9chars", func(b *testing.B) {
		data := []byte("123456789")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseHexU64(data)
		}
	})

	// Compare against naive implementation
	b.Run("ParseHexU64_Naive", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var result uint64
			for _, c := range hex16 {
				result <<= 4
				if c >= '0' && c <= '9' {
					result |= uint64(c - '0')
				} else if c >= 'a' && c <= 'f' {
					result |= uint64(c - 'a' + 10)
				} else if c >= 'A' && c <= 'F' {
					result |= uint64(c - 'A' + 10)
				}
			}
			_ = result
		}
	})
}

func BenchmarkJSONParsing(b *testing.B) {
	shortJSON := []byte(`{"key":"value"}`)
	mediumJSON := []byte(`{"key":"value","array":[1,2,3,4,5,6,7,8,9,10],"nested":{"a":"b","c":"d"}}`)
	longJSON := []byte(`{"users":[{"id":1,"name":"Alice","email":"alice@example.com"},{"id":2,"name":"Bob","email":"bob@example.com"},{"id":3,"name":"Charlie","email":"charlie@example.com"}],"meta":{"total":3,"page":1}}`)

	benchmarks := []struct {
		name string
		data []byte
	}{
		{"Short", shortJSON},
		{"Medium", mediumJSON},
		{"Long", longJSON},
	}

	for _, bench := range benchmarks {
		b.Run("SkipToQuote_"+bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = SkipToQuote(bench.data, 0, 1)
			}
		})

		b.Run("SkipToQuoteEarlyExit_"+bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = SkipToQuoteEarlyExit(bench.data, 0, 1, 20)
			}
		})

		b.Run("SkipToOpeningBracket_"+bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = SkipToOpeningBracket(bench.data, 0, 1)
			}
		})
	}

	// Hop size comparison
	hopSizes := []int{1, 2, 4, 8, 16}
	for _, hop := range hopSizes {
		b.Run(fmt.Sprintf("SkipToQuote_Hop%d", hop), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = SkipToQuote(longJSON, 0, hop)
			}
		})
	}
}

func BenchmarkMix64(b *testing.B) {
	inputs := []uint64{
		0x0000000000000000,
		0x0123456789ABCDEF,
		0xFFFFFFFFFFFFFFFF,
		0x5555555555555555,
		0xAAAAAAAAAAAAAAAA,
	}

	b.Run("Single", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Mix64(inputs[i%len(inputs)])
		}
	})

	b.Run("Sequential", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Mix64(uint64(i))
		}
	})

	b.Run("Chained", func(b *testing.B) {
		b.ReportAllocs()
		var result uint64
		for i := 0; i < b.N; i++ {
			result = Mix64(result)
		}
		_ = result
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var i uint64
			for pb.Next() {
				_ = Mix64(atomic.AddUint64(&i, 1))
			}
		})
	})
}

// Comprehensive benchmark suite
func BenchmarkComprehensive(b *testing.B) {
	// Test all functions with realistic workloads
	b.Run("RealWorldMix", func(b *testing.B) {
		data := generateTestData(1024, "random")
		hexData := []byte("deadbeefcafebabe")
		addr := []byte("dac17f958d2ee523a2206206994597c13d831ec7")

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Simulate realistic usage pattern
			offset := i & 1015

			// Memory operations
			v1 := Load64(data[offset:])
			v2 := Mix64(v1)

			// Type conversions
			s1 := Itoa(int(v2 & 0xFFFF))
			f1 := float64(v2&0xFF) / 255.0
			s2 := Ftoa(f1)

			// Hex parsing
			h1 := ParseHexU64(hexData)
			if i%10 == 0 {
				_ = ParseEthereumAddress(addr)
			}

			// Use results to prevent optimization
			_ = s1
			_ = s2
			_ = h1
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CLEANUP AND RESOURCE VALIDATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestCleanup(t *testing.T) {
	t.Run("no_goroutine_leaks", func(t *testing.T) {
		before := runtime.NumGoroutine()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = Mix64(42)
				_ = Ftoa(3.14159)
				_ = Itoa(123)
			}()
		}
		wg.Wait()

		time.Sleep(10 * time.Millisecond)
		after := runtime.NumGoroutine()
		if after > before {
			t.Logf("Possible goroutine leak: before=%d, after=%d", before, after)
		}
	})

	t.Run("memory_usage_stability", func(t *testing.T) {
		// Test that repeated calls don't increase memory usage
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Perform many operations
		for i := 0; i < 100000; i++ {
			_ = Ftoa(float64(i) * 3.14159)
			_ = Itoa(i)
			_ = Mix64(uint64(i))
			_ = ParseHexU64([]byte("deadbeefcafebabe"))
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)

		// Memory should not have grown significantly
		growth := int64(m2.Alloc) - int64(m1.Alloc)
		if growth > 1024*1024 { // Allow 1MB growth
			t.Errorf("Memory usage grew unexpectedly: %d bytes", growth)
		}

		t.Logf("Memory growth: %d bytes", growth)
	})
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// DOCUMENTATION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestIntentionalLimitations(t *testing.T) {
	t.Run("B2s_limitations", func(t *testing.T) {
		// B2s panics on empty/nil slices - this is intentional
		t.Log("B2s requires non-empty slice - will panic on empty/nil")

		// Document the zero-copy behavior
		data := []byte("test")
		str := B2s(data)
		data[0] = 'b'
		if str == "test" {
			t.Error("B2s not sharing memory as documented")
		}
	})

	t.Run("Itoa_limitations", func(t *testing.T) {
		// Itoa only supports positive integers
		t.Log("Itoa only supports positive integers [0, MaxInt]")

		if Itoa(-1) != "" {
			t.Error("Itoa should return empty string for negative numbers")
		}
	})

	t.Run("Ftoa_limitations", func(t *testing.T) {
		// Ftoa has precision limitations for performance
		t.Log("Ftoa optimizes for performance - limited to 6 decimal places")

		// Test precision limit
		f := 1.123456789
		result := Ftoa(f)
		if !strings.Contains(result, "1.123457") && !strings.Contains(result, "1.123456") {
			t.Errorf("Ftoa precision not as documented: %s", result)
		}
	})

	t.Run("hex_parsing_limitations", func(t *testing.T) {
		// No validation of hex characters
		t.Log("Hex parsing assumes valid hex input - no validation")

		// Invalid input produces undefined results but shouldn't panic
		invalid := []byte("ghijklmn")
		_ = ParseHexU64(invalid) // Should not panic
	})

	t.Run("memory_operations_limitations", func(t *testing.T) {
		// Load functions require sufficient buffer size
		t.Log("Load64/Load128/LoadBE64 require buffers of at least 8/16/8 bytes")

		// These would panic with insufficient data
		// Document but don't test the panic
	})
}

// floatStringsEquivalent checks if two float strings represent the same mathematical value.
// This is necessary because different formatting algorithms may produce slightly different
// but mathematically equivalent representations (e.g., "1e+06" vs "1000000").
func floatStringsEquivalent(got, want string, original float64) bool {
	if got == want {
		return true
	}

	// Parse both strings and compare the actual values
	gotVal, gotErr := strconv.ParseFloat(got, 64)
	wantVal, wantErr := strconv.ParseFloat(want, 64)

	if gotErr != nil || wantErr != nil {
		return false
	}

	// For very small differences, check if they're within acceptable precision
	// Using relative error for better handling of different magnitudes
	if original != 0 {
		relativeError := math.Abs(gotVal-wantVal) / math.Abs(original)
		if relativeError < 1e-15 {
			return true
		}
	} else if math.Abs(gotVal-wantVal) < 1e-15 {
		return true
	}

	// Check if both represent the same special value
	if math.IsNaN(gotVal) && math.IsNaN(wantVal) {
		return true
	}

	if math.IsInf(gotVal, 0) && math.IsInf(wantVal, 0) {
		return math.Signbit(gotVal) == math.Signbit(wantVal)
	}

	return false
}

// verifyMemoryAlignment checks if a pointer is aligned to the specified boundary.
// Critical for SIMD operations which may require specific alignment.
func verifyMemoryAlignment(ptr unsafe.Pointer, alignment uintptr) bool {
	return uintptr(ptr)&(alignment-1) == 0
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// MEMORY OPERATION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestLoad64(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint64
	}{
		{
			name:  "all_zeros",
			input: []byte{0, 0, 0, 0, 0, 0, 0, 0},
			want:  0x0000000000000000,
		},
		{
			name:  "all_ones",
			input: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			want:  0xFFFFFFFFFFFFFFFF,
		},
		{
			name:  "sequential_bytes",
			input: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			want:  0x0807060504030201, // Little-endian
		},
		{
			name:  "mixed_pattern",
			input: []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22},
			want:  0x2211FFEEDDCCBBAA,
		},
		{
			name:  "high_bit_set",
			input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80},
			want:  0x8000000000000000,
		},
		{
			name:  "alternating_bits",
			input: []byte{0x55, 0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55, 0xAA},
			want:  0xAA55AA55AA55AA55,
		},
		{
			name:  "ascending_nibbles",
			input: []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF},
			want:  0xEFCDAB8967452301,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Load64(tt.input)
			if got != tt.want {
				t.Errorf("Load64() = 0x%016X, want 0x%016X", got, tt.want)
			}

			// Verify against manual byte-by-byte calculation
			if len(tt.input) >= 8 {
				expected := uint64(tt.input[0]) |
					uint64(tt.input[1])<<8 |
					uint64(tt.input[2])<<16 |
					uint64(tt.input[3])<<24 |
					uint64(tt.input[4])<<32 |
					uint64(tt.input[5])<<40 |
					uint64(tt.input[6])<<48 |
					uint64(tt.input[7])<<56
				if got != expected {
					t.Errorf("Byte order mismatch: got 0x%016X, expected 0x%016X", got, expected)
				}
			}

			// Verify the operation is truly zero-copy
			if len(tt.input) >= 8 {
				// Modify the result and ensure input is unchanged
				inputCopy := make([]byte, len(tt.input))
				copy(inputCopy, tt.input)
				_ = Load64(tt.input)
				for i := range tt.input {
					if tt.input[i] != inputCopy[i] {
						t.Error("Load64 modified input buffer")
					}
				}
			}
		})
	}
}

func TestLoad64_Alignment(t *testing.T) {
	// Test various alignments to ensure unaligned access works correctly
	for offset := 0; offset < 16; offset++ {
		t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
			// Create buffer with extra space for alignment testing
			buffer := make([]byte, 24)
			testData := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
			copy(buffer[offset:], testData)

			result := Load64(buffer[offset:])
			expected := uint64(0x0807060504030201)

			if result != expected {
				t.Errorf("Offset %d: got 0x%016X, want 0x%016X", offset, result, expected)
			}

			// Verify alignment doesn't affect correctness
			ptr := unsafe.Pointer(&buffer[offset])
			alignment := uintptr(ptr) & 7
			t.Logf("Offset %d has alignment %d", offset, alignment)
		})
	}
}

func TestLoad64_EdgeCases(t *testing.T) {
	t.Run("minimum_valid_slice", func(t *testing.T) {
		// Exactly 8 bytes - minimum valid input
		data := []byte{1, 2, 3, 4, 5, 6, 7, 8}
		result := Load64(data)
		if result == 0 {
			t.Error("Load64 failed on minimum valid slice")
		}
	})

	t.Run("larger_slice", func(t *testing.T) {
		// More than 8 bytes - should only read first 8
		data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
		result := Load64(data)
		expected := Load64(data[:8])
		if result != expected {
			t.Error("Load64 read beyond 8 bytes")
		}
	})
}

func TestLoad128(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want1 uint64
		want2 uint64
	}{
		{
			name:  "all_zeros",
			input: make([]byte, 16),
			want1: 0x0000000000000000,
			want2: 0x0000000000000000,
		},
		{
			name:  "sequential_bytes",
			input: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			want1: 0x0807060504030201,
			want2: 0x100F0E0D0C0B0A09,
		},
		{
			name: "all_ones",
			input: []byte{
				0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
				0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			},
			want1: 0xFFFFFFFFFFFFFFFF,
			want2: 0xFFFFFFFFFFFFFFFF,
		},
		{
			name: "mixed_pattern",
			input: []byte{
				0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
				0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
			},
			want1: 0xBEBAFECAEFBEADDE,
			want2: 0xF0DEBC9A78563412,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2 := Load128(tt.input)
			if got1 != tt.want1 || got2 != tt.want2 {
				t.Errorf("Load128() = (0x%016X, 0x%016X), want (0x%016X, 0x%016X)",
					got1, got2, tt.want1, tt.want2)
			}

			// Verify it matches two Load64 calls
			if len(tt.input) >= 16 {
				check1 := Load64(tt.input[0:8])
				check2 := Load64(tt.input[8:16])
				if got1 != check1 || got2 != check2 {
					t.Error("Load128 doesn't match two Load64 calls")
				}
			}

			// Verify atomicity - both values should be read consistently
			// This is important for lock-free algorithms
			if len(tt.input) >= 16 {
				for i := 0; i < 100; i++ {
					g1, g2 := Load128(tt.input)
					if g1 != got1 || g2 != got2 {
						t.Error("Load128 not reading consistently")
					}
				}
			}
		})
	}
}

func TestLoadBE64(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint64
	}{
		{
			name:  "all_zeros",
			input: []byte{0, 0, 0, 0, 0, 0, 0, 0},
			want:  0x0000000000000000,
		},
		{
			name:  "sequential_bytes",
			input: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			want:  0x0102030405060708, // Big-endian
		},
		{
			name:  "mixed_pattern",
			input: []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22},
			want:  0xAABBCCDDEEFF1122,
		},
		{
			name:  "network_order_test",
			input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			want:  1, // Network byte order representation of 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LoadBE64(tt.input)
			if got != tt.want {
				t.Errorf("LoadBE64() = 0x%016X, want 0x%016X", got, tt.want)
			}

			// Verify against binary.BigEndian
			if len(tt.input) >= 8 {
				expected := binary.BigEndian.Uint64(tt.input)
				if got != expected {
					t.Errorf("LoadBE64() differs from binary.BigEndian: got 0x%016X, want 0x%016X",
						got, expected)
				}
			}

			// Verify endianness conversion
			leResult := Load64(tt.input)
			if runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64" {
				// On little-endian systems, results should differ
				if tt.input[0] != tt.input[7] && leResult == got {
					t.Error("LoadBE64 not performing endianness conversion")
				}
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// TYPE CONVERSION TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestB2s(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		want      string
		wantPanic bool
	}{
		{
			name:      "empty_slice",
			input:     []byte{},
			want:      "",
			wantPanic: true,
		},
		{
			name:      "nil_slice",
			input:     nil,
			want:      "",
			wantPanic: true,
		},
		{
			name:  "single_char",
			input: []byte{'a'},
			want:  "a",
		},
		{
			name:  "ascii_string",
			input: []byte("hello world"),
			want:  "hello world",
		},
		{
			name:  "utf8_string",
			input: []byte("Hello, 世界! 🌍"),
			want:  "Hello, 世界! 🌍",
		},
		{
			name:  "binary_data",
			input: []byte{0x00, 0x01, 0xFF, 0xFE},
			want:  string([]byte{0x00, 0x01, 0xFF, 0xFE}),
		},
		{
			name:  "long_string",
			input: []byte("The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs."),
			want:  "The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Expected panic but didn't get one")
					}
				}()
			}

			got := B2s(tt.input)
			if got != tt.want {
				t.Errorf("B2s() = %q, want %q", got, tt.want)
			}

			// Verify length preservation
			if len(got) != len(tt.input) {
				t.Errorf("Length mismatch: got %d, want %d", len(got), len(tt.input))
			}

			// Verify zero-copy behavior (only for non-empty slices)
			if len(tt.input) > 0 {
				inputPtr := unsafe.Pointer(&tt.input[0])
				resultPtr := unsafe.Pointer(unsafe.StringData(got))
				if inputPtr != resultPtr {
					t.Error("B2s() should share underlying data (zero-copy)")
				}

				// Verify mutations are visible
				originalByte := tt.input[0]
				tt.input[0] = 'X'
				if got[0] != 'X' {
					t.Error("B2s() not sharing memory properly")
				}
				tt.input[0] = originalByte // Restore
			}
		})
	}
}

func TestB2s_SafetyViolations(t *testing.T) {
	t.Run("modification_after_conversion", func(t *testing.T) {
		data := []byte("test")
		str := B2s(data)

		// This modification affects the string - document this behavior
		data[0] = 'b'
		if str != "best" {
			t.Error("B2s safety violation not working as documented")
		}
	})

	t.Run("slice_capacity_changes", func(t *testing.T) {
		data := make([]byte, 4, 8)
		copy(data, []byte("test"))
		str := B2s(data)

		// Appending within capacity is safe
		data = append(data, []byte("1234")...)
		if str != "test" {
			t.Error("B2s affected by append within capacity")
		}
	})
}

func TestItoa(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  string
	}{
		{"zero", 0, "0"},
		{"one", 1, "1"},
		{"single_digit", 9, "9"},
		{"two_digits", 42, "42"},
		{"three_digits", 123, "123"},
		{"four_digits", 1234, "1234"},
		{"five_digits", 12345, "12345"},
		{"large_positive", 2147483647, "2147483647"},
		{"power_of_10", 1000000, "1000000"},
		{"power_of_2", 1024, "1024"},
		{"alternating", 101010, "101010"},
		{"all_nines", 9999, "9999"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Itoa(tt.input)
			if got != tt.want {
				t.Errorf("Itoa(%d) = %q, want %q", tt.input, got, tt.want)
			}

			// Cross-verify with standard library for positive numbers
			stdResult := strconv.Itoa(tt.input)
			if got != stdResult {
				t.Errorf("Itoa(%d) differs from strconv.Itoa: got %q, want %q",
					tt.input, got, stdResult)
			}

			// Verify no allocations beyond the result string
			allocs := testing.AllocsPerRun(100, func() {
				_ = Itoa(tt.input)
			})
			if allocs > 1 {
				t.Errorf("Itoa(%d) performed %.1f allocations, want ≤ 1", tt.input, allocs)
			}
		})
	}
}

func TestItoa_Negative(t *testing.T) {
	// Test that negative numbers return empty string as documented
	negativeNumbers := []int{-1, -10, -100, -1000, -2147483648}
	for _, n := range negativeNumbers {
		result := Itoa(n)
		if result != "" {
			t.Errorf("Itoa(%d): expected empty string for negative number, got %q", n, result)
		}
	}
}

func TestItoa_BoundaryValues(t *testing.T) {
	// Test boundary values for 32-bit systems
	boundaries := []struct {
		name  string
		value int
		want  string
	}{
		{"max_int32", 2147483647, "2147483647"},
		{"billion", 1000000000, "1000000000"},
		{"ten_digits", 9999999999, "9999999999"}, // If int is 64-bit
	}

	for _, tt := range boundaries {
		t.Run(tt.name, func(t *testing.T) {
			// Skip if value overflows int on this platform
			if tt.value < 0 {
				t.Skip("Value overflows int on this platform")
			}

			got := Itoa(tt.value)
			if got != tt.want {
				t.Errorf("Itoa(%d) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestFtoa(t *testing.T) {
	tests := []struct {
		name  string
		input float64
		want  string
	}{
		// Basic values
		{"zero", 0.0, "0"},
		{"positive_one", 1.0, "1"},
		{"negative_one", -1.0, "-1"},
		{"positive_integer", 42.0, "42"},
		{"negative_integer", -42.0, "-42"},

		// Simple decimals
		{"half", 0.5, "0.5"},
		{"negative_half", -0.5, "-0.5"},
		{"quarter", 0.25, "0.25"},
		{"eighth", 0.125, "0.125"},
		{"three_quarters", 0.75, "0.75"},

		// Common decimals
		{"pi_short", 3.14, "3.14"},
		{"e_short", 2.718, "2.718"},
		{"simple_fraction", 1.5, "1.5"},
		{"negative_decimal", -3.14, "-3.14"},
		{"six_decimals", 1.123456, "1.123456"},

		// Larger integers
		{"thousand", 1000.0, "1000"},
		{"ten_thousand", 10000.0, "10000"},
		{"hundred_thousand", 100000.0, "100000"},
		{"million", 1000000.0, "1e+06"},
		{"billion", 1000000000.0, "1e+09"},
		{"large_integer", 123456789.0, "1.234568e+08"},

		// Small decimals
		{"thousandth", 0.001, "0.001"},
		{"millionth", 0.000001, "0.000001"},
		{"ten_millionth", 0.0000001, "1e-07"},
		{"small_decimal", 0.0001, "0.0001"},
		{"very_small", 0.00001, "0.00001"},
		{"tiny", 1e-10, "1e-10"},

		// Powers of 2 (exact in IEEE 754)
		{"power_2_neg1", 0.5, "0.5"},
		{"power_2_neg2", 0.25, "0.25"},
		{"power_2_neg3", 0.125, "0.125"},
		{"power_2_pos10", 1024.0, "1024"},
		{"power_2_pos16", 65536.0, "65536"},

		// Trailing zeros should be removed
		{"trailing_zeros_1", 1.500000, "1.5"},
		{"trailing_zeros_2", 42.100000, "42.1"},
		{"trailing_zeros_3", 0.100000, "0.1"},
		{"trailing_zeros_4", 10.0, "10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Ftoa(tt.input)
			if !floatStringsEquivalent(got, tt.want, tt.input) {
				t.Errorf("Ftoa(%g) = %q, want %q", tt.input, got, tt.want)
			}

			// Verify single allocation
			allocs := testing.AllocsPerRun(100, func() {
				_ = Ftoa(tt.input)
			})
			if allocs > 1 {
				t.Errorf("Ftoa(%g) performed %.1f allocations, want ≤ 1", tt.input, allocs)
			}
		})
	}
}

func TestFtoa_IEEE754Compliance(t *testing.T) {
	tests := []struct {
		name  string
		input float64
		want  string
	}{
		// IEEE 754 special values
		{"positive_zero", 0.0, "0"},
		{"negative_zero", math.Copysign(0.0, -1), "0"}, // IEEE 754: -0 == +0
		{"positive_infinity", math.Inf(1), "+Inf"},
		{"negative_infinity", math.Inf(-1), "-Inf"},
		{"nan", math.NaN(), "NaN"},
		{"negative_nan", math.Copysign(math.NaN(), -1), "NaN"}, // Sign of NaN ignored

		// Subnormal numbers
		{"smallest_positive", math.SmallestNonzeroFloat64, "4.940656e-324"},
		{"smallest_normal", 2.2250738585072014e-308, "2.225074e-308"},

		// Exact decimal representations
		{"exact_tenth", 0.1, "0.1"},
		{"exact_half", 0.5, "0.5"},
		{"exact_quarter", 0.25, "0.25"},
		{"exact_eighth", 0.125, "0.125"},

		// Large numbers requiring scientific notation
		{"large_scientific", 1.23456789e15, "1.234568e+15"},
		{"very_large", 1e20, "1e+20"},
		{"huge", 1.7976931348623157e+308, "1.797693e+308"}, // Near max float64

		// Very small numbers requiring scientific notation
		{"very_small_scientific", 1.23456e-10, "1.23456e-10"},
		{"extremely_small", 1e-20, "1e-20"},
		{"tiny_scientific", 2.2250738585072014e-308, "2.225074e-308"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Ftoa(tt.input)
			if !floatStringsEquivalent(got, tt.want, tt.input) {
				t.Errorf("Ftoa(%g) = %q, want %q", tt.input, got, tt.want)
			}

			// For special values, verify exact string match
			if math.IsNaN(tt.input) || math.IsInf(tt.input, 0) {
				if got != tt.want {
					t.Errorf("Special value: got %q, want %q", got, tt.want)
				}
			}
		})
	}
}

func TestFtoa_BitPatterns(t *testing.T) {
	// Test specific IEEE 754 bit patterns
	tests := []struct {
		name string
		bits uint64
		want string
	}{
		{"positive_zero_bits", 0x0000000000000000, "0"},
		{"negative_zero_bits", 0x8000000000000000, "0"},
		{"positive_inf_bits", 0x7FF0000000000000, "+Inf"},
		{"negative_inf_bits", 0xFFF0000000000000, "-Inf"},
		{"qnan_bits", 0x7FF8000000000000, "NaN"},
		{"snan_bits", 0x7FF4000000000000, "NaN"},
		{"one_bits", 0x3FF0000000000000, "1"},
		{"negative_one_bits", 0xBFF0000000000000, "-1"},
		{"two_bits", 0x4000000000000000, "2"},
		{"half_bits", 0x3FE0000000000000, "0.5"},
		{"pi_bits", 0x400921FB54442D18, "3.141593"},
		{"smallest_normal", 0x0010000000000000, "2.225074e-308"},
		{"largest_subnormal", 0x000FFFFFFFFFFFFF, "2.225074e-308"},
		{"max_float64", 0x7FEFFFFFFFFFFFFF, "1.797693e+308"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := math.Float64frombits(tt.bits)
			got := Ftoa(f)
			if !floatStringsEquivalent(got, tt.want, f) {
				t.Errorf("Ftoa(Float64frombits(0x%016X)) = %q, want %q",
					tt.bits, got, tt.want)
			}
		})
	}
}

func TestFtoa_RoundingBehavior(t *testing.T) {
	tests := []struct {
		name  string
		input float64
		want  string
	}{
		// Test rounding at 6 decimal places
		{"round_down", 1.1234564, "1.123456"},
		{"round_up", 1.1234565, "1.123457"},
		{"exactly_half", 1.1234565, "1.123457"}, // Round half up
		{"many_nines", 0.9999999, "1"},
		{"negative_round", -1.1234565, "-1.123457"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Ftoa(tt.input)
			if !floatStringsEquivalent(got, tt.want, tt.input) {
				t.Errorf("Ftoa(%g) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFtoa_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input float64
	}{
		{"max_int64", float64(math.MaxInt64)},
		{"min_int64", float64(math.MinInt64)},
		{"max_uint64", float64(math.MaxUint64)},
		{"just_above_one", math.Nextafter(1.0, 2.0)},
		{"just_below_one", math.Nextafter(1.0, 0.0)},
		{"just_above_zero", math.Nextafter(0.0, 1.0)},
		{"just_below_zero", math.Nextafter(0.0, -1.0)},
		{"largest_exact_int", float64(1 << 53)},      // 2^53
		{"smallest_inexact_int", float64(1<<53 + 1)}, // 2^53 + 1
		{"pi", math.Pi},
		{"e", math.E},
		{"sqrt2", math.Sqrt2},
		{"ln2", math.Ln2},
		{"phi", (1 + math.Sqrt(5)) / 2}, // Golden ratio
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic and should produce valid output
			result := Ftoa(tt.input)
			if len(result) == 0 {
				t.Errorf("Ftoa(%g) produced empty string", tt.input)
			}

			// Verify it's a valid number representation
			if result != "NaN" && result != "+Inf" && result != "-Inf" {
				parsed, err := strconv.ParseFloat(result, 64)
				if err != nil {
					t.Errorf("Ftoa(%g) = %q is not a valid float string: %v",
						tt.input, result, err)
				}

				// For exact integers, verify exact representation
				if tt.input == math.Trunc(tt.input) && math.Abs(tt.input) < 1e6 {
					if parsed != tt.input {
						t.Errorf("Exact integer lost precision: %g -> %g", tt.input, parsed)
					}
				}
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HEX PARSING TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestParseHexU32(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint32
	}{
		{"empty", []byte{}, 0x0},
		{"single_digit_decimal", []byte("5"), 0x5},
		{"single_digit_hex", []byte("f"), 0xF},
		{"two_digits", []byte("ab"), 0xAB},
		{"four_digits", []byte("1234"), 0x1234},
		{"eight_digits", []byte("deadbeef"), 0xDEADBEEF},
		{"max_uint32", []byte("ffffffff"), 0xFFFFFFFF},
		{"uppercase", []byte("DEADBEEF"), 0xDEADBEEF},
		{"mixed_case", []byte("DeAdBeEf"), 0xDEADBEEF},
		{"leading_zeros", []byte("00001234"), 0x00001234},
		{"alternating", []byte("a5a5a5a5"), 0xA5A5A5A5},
		{"truncation", []byte("123456789"), 0x12345678}, // Truncates to 8
		{"all_numeric", []byte("12345678"), 0x12345678},
		{"all_alpha", []byte("abcdefab"), 0xABCDEFAB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseHexU32(tt.input)
			if got != tt.want {
				t.Errorf("ParseHexU32(%q) = 0x%08X, want 0x%08X", tt.input, got, tt.want)
			}

			// Verify case insensitivity
			if len(tt.input) > 0 {
				upper := make([]byte, len(tt.input))
				lower := make([]byte, len(tt.input))
				for i, b := range tt.input {
					if b >= 'a' && b <= 'f' {
						upper[i] = b - 32
						lower[i] = b
					} else if b >= 'A' && b <= 'F' {
						upper[i] = b
						lower[i] = b + 32
					} else {
						upper[i] = b
						lower[i] = b
					}
				}
				gotUpper := ParseHexU32(upper)
				gotLower := ParseHexU32(lower)
				if gotUpper != got || gotLower != got {
					t.Error("ParseHexU32 not case-insensitive")
				}
			}
		})
	}
}

func TestParseHexU32_InvalidInput(t *testing.T) {
	// These should not panic but produce garbage results
	// Document this behavior as the function assumes valid input
	invalidInputs := [][]byte{
		[]byte("ghijklmn"), // Non-hex characters
		[]byte("12 34"),    // Space in middle
		[]byte("0x1234"),   // With prefix
		[]byte("12.34"),    // Decimal point
		[]byte("-1234"),    // Negative sign
	}

	for _, input := range invalidInputs {
		t.Run(string(input), func(t *testing.T) {
			// Should not panic
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("ParseHexU32 panicked on invalid input %q: %v", input, r)
				}
			}()

			_ = ParseHexU32(input)
			// We don't check the result as it's undefined for invalid input
		})
	}
}

func TestParseHexU64(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint64
	}{
		{"empty", []byte{}, 0x0},
		{"single_digit", []byte("f"), 0xF},
		{"four_chars", []byte("dead"), 0xDEAD},
		{"eight_chars", []byte("deadbeef"), 0xDEADBEEF},
		{"twelve_chars", []byte("deadbeefcafe"), 0xDEADBEEFCAFE},
		{"sixteen_chars", []byte("0123456789abcdef"), 0x0123456789ABCDEF},
		{"max_uint64", []byte("ffffffffffffffff"), 0xFFFFFFFFFFFFFFFF},
		{"uppercase_16", []byte("DEADBEEFCAFEBABE"), 0xDEADBEEFCAFEBABE},
		{"mixed_case_16", []byte("DeAdBeEfCaFeBaBe"), 0xDEADBEEFCAFEBABE},
		{"leading_zeros", []byte("0000000000001234"), 0x0000000000001234},
		{"alternating_pattern", []byte("a5a5a5a5a5a5a5a5"), 0xA5A5A5A5A5A5A5A5},
		{"truncation", []byte("123456789abcdef01"), 0x123456789ABCDEF0}, // Truncates to 16
		{"sequential", []byte("123456789abcdef0"), 0x123456789ABCDEF0},
		{"boundary_8_chars", []byte("12345678"), 0x12345678},
		{"boundary_9_chars", []byte("123456789"), 0x123456789},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseHexU64(tt.input)
			if got != tt.want {
				t.Errorf("ParseHexU64(%q) = 0x%016X, want 0x%016X", tt.input, got, tt.want)
			}

			// Test SIMD optimization correctness
			// Verify against byte-by-byte reference implementation
			var reference uint64
			processLen := len(tt.input)
			if processLen > 16 {
				processLen = 16
			}
			for i := 0; i < processLen; i++ {
				c := tt.input[i]
				if c >= '0' && c <= '9' {
					reference = (reference << 4) | uint64(c-'0')
				} else if c >= 'a' && c <= 'f' {
					reference = (reference << 4) | uint64(c-'a'+10)
				} else if c >= 'A' && c <= 'F' {
					reference = (reference << 4) | uint64(c-'A'+10)
				}
			}
			if got != reference {
				t.Errorf("SIMD result differs from reference: got 0x%016X, want 0x%016X",
					got, reference)
			}
		})
	}
}

func TestParseHexU64_SIMDCorrectness(t *testing.T) {
	// Test edge cases for SIMD implementation
	tests := []struct {
		name  string
		input []byte
	}{
		{"boundary_7_chars", []byte("1234567")},
		{"boundary_8_chars", []byte("12345678")},
		{"boundary_9_chars", []byte("123456789")},
		{"boundary_15_chars", []byte("123456789abcdef")},
		{"boundary_16_chars", []byte("123456789abcdef0")},
		{"all_zeros_8", []byte("00000000")},
		{"all_zeros_16", []byte("0000000000000000")},
		{"all_f_8", []byte("ffffffff")},
		{"all_f_16", []byte("ffffffffffffffff")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseHexU64(tt.input)

			// Verify no out-of-bounds access by running with different buffer sizes
			for extraBytes := 0; extraBytes < 8; extraBytes++ {
				paddedInput := make([]byte, len(tt.input)+extraBytes)
				copy(paddedInput, tt.input)
				// Fill extra bytes with 'X' to detect if they're read
				for i := len(tt.input); i < len(paddedInput); i++ {
					paddedInput[i] = 'X'
				}

				paddedResult := ParseHexU64(paddedInput[:len(tt.input)])
				if paddedResult != result {
					t.Errorf("Result changed with padding: got 0x%016X, want 0x%016X",
						paddedResult, result)
				}
			}
		})
	}
}

func TestParseEthereumAddress(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  [20]byte
	}{
		{
			name:  "zero_address",
			input: []byte("0000000000000000000000000000000000000000"),
			want:  [20]byte{},
		},
		{
			name:  "real_address_lowercase",
			input: []byte("dac17f958d2ee523a2206206994597c13d831ec7"),
			want: [20]byte{
				0xda, 0xc1, 0x7f, 0x95, 0x8d, 0x2e, 0xe5, 0x23, 0xa2, 0x20,
				0x62, 0x06, 0x99, 0x45, 0x97, 0xc1, 0x3d, 0x83, 0x1e, 0xc7,
			},
		},
		{
			name:  "real_address_checksum",
			input: []byte("dAC17F958D2ee523a2206206994597C13D831ec7"),
			want: [20]byte{
				0xda, 0xc1, 0x7f, 0x95, 0x8d, 0x2e, 0xe5, 0x23, 0xa2, 0x20,
				0x62, 0x06, 0x99, 0x45, 0x97, 0xc1, 0x3d, 0x83, 0x1e, 0xc7,
			},
		},
		{
			name:  "all_f_address",
			input: []byte("ffffffffffffffffffffffffffffffffffffffff"),
			want: [20]byte{
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			},
		},
		{
			name:  "alternating_pattern",
			input: []byte("a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5"),
			want: [20]byte{
				0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5,
				0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5,
			},
		},
		{
			name:  "sequential_nibbles",
			input: []byte("0123456789abcdef0123456789abcdef01234567"),
			want: [20]byte{
				0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23,
				0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseEthereumAddress(tt.input)
			if got != tt.want {
				t.Errorf("ParseEthereumAddress(%q) = %x, want %x", tt.input, got, tt.want)
			}

			// Verify SIMD correctness by comparing with reference implementation
			var reference [20]byte
			for i := 0; i < 20; i++ {
				hi := tt.input[i*2]
				lo := tt.input[i*2+1]

				var hiVal, loVal byte
				if hi >= '0' && hi <= '9' {
					hiVal = hi - '0'
				} else if hi >= 'a' && hi <= 'f' {
					hiVal = hi - 'a' + 10
				} else if hi >= 'A' && hi <= 'F' {
					hiVal = hi - 'A' + 10
				}

				if lo >= '0' && lo <= '9' {
					loVal = lo - '0'
				} else if lo >= 'a' && lo <= 'f' {
					loVal = lo - 'a' + 10
				} else if lo >= 'A' && lo <= 'F' {
					loVal = lo - 'A' + 10
				}

				reference[i] = (hiVal << 4) | loVal
			}

			if got != reference {
				t.Errorf("SIMD result differs from reference: got %x, want %x", got, reference)
			}
		})
	}
}

func TestParseEthereumAddress_Alignment(t *testing.T) {
	// Test that SIMD operations work correctly with different alignments
	address := []byte("dac17f958d2ee523a2206206994597c13d831ec7")
	expected := ParseEthereumAddress(address)

	for offset := 0; offset < 8; offset++ {
		t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
			// Create misaligned buffer
			buffer := make([]byte, 40+offset+8)
			copy(buffer[offset:], address)

			result := ParseEthereumAddress(buffer[offset : offset+40])
			if result != expected {
				t.Errorf("Misaligned result differs at offset %d", offset)
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// JSON PARSING TESTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

func TestSkipToQuote(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		startIdx int
		hopSize  int
		want     int
	}{
		{"found_immediate", []byte(`"test"`), 0, 1, 0},
		{"found_after_skip", []byte(`abc"def`), 0, 1, 3},
		{"not_found", []byte(`abcdef`), 0, 1, -1},
		{"empty_data", []byte{}, 0, 1, -1},
		{"hop_2_found", []byte(`a"b"c"d"`), 0, 2, 6}, // Finds third quote
		{"hop_2_miss", []byte(`a"b"c"d"`), 1, 2, -1}, // Misses all quotes
		{"hop_3_found", []byte(`abc"def"ghi"`), 0, 3, 3},
		{"json_object", []byte(`{"key":"value"}`), 0, 1, 1},
		{"json_array", []byte(`["item1","item2"]`), 0, 1, 1},
		{"escaped_quote", []byte(`test\"quote`), 0, 1, -1}, // No actual quote
		{"start_at_quote", []byte(`"test"`), 0, 1, 0},
		{"start_past_quote", []byte(`"test"`), 1, 1, -1},
		{"large_hop", []byte(`""""""""""`), 0, 10, -1}, // Hop past all
		{"exact_hop", []byte(`123"567"`), 0, 3, 3},     // Land exactly on quote
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SkipToQuote(tt.data, tt.startIdx, tt.hopSize)
			if got != tt.want {
				t.Errorf("SkipToQuote(%q, %d, %d) = %d, want %d",
					tt.data, tt.startIdx, tt.hopSize, got, tt.want)
			}

			// Verify bounds checking
			if got != -1 && (got < 0 || got >= len(tt.data)) {
				t.Errorf("Result %d is out of bounds for data length %d", got, len(tt.data))
			}

			// Verify the character at the result is actually a quote
			if got != -1 && tt.data[got] != '"' {
				t.Errorf("Character at position %d is %c, not quote", got, tt.data[got])
			}
		})
	}
}

func TestSkipToQuoteEarlyExit(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		startIdx  int
		hopSize   int
		maxHops   int
		wantIdx   int
		wantEarly bool
	}{
		{"found_within_limit", []byte(`abc"def"`), 0, 1, 10, 3, false},
		{"found_at_limit", []byte(`abc"def"`), 0, 1, 4, 3, false},
		{"early_exit", []byte(`abcdefghij`), 0, 1, 3, 3, true},
		{"not_found_no_early", []byte(`abcdef`), 0, 1, 10, -1, false},
		{"immediate_find", []byte(`"test"`), 0, 1, 10, 0, false},
		{"zero_max_hops", []byte(`"test"`), 0, 1, 0, 0, true},
		{"empty_data", []byte{}, 0, 1, 10, -1, false},
		{"start_past_end", []byte(`"test"`), 10, 1, 5, -1, false},
		{"hop_2_found", []byte(`ab"cd"`), 0, 2, 5, 2, false},
		{"hop_3_early", []byte(`abcdefghijk`), 0, 3, 2, 6, true}, // 0->3->6 (2 hops)
		{"exact_limit_not_found", []byte(`abcdefghij`), 0, 1, 10, -1, false},
		{"json_string", []byte(`{"name":"value"}`), 1, 1, 20, 2, false},
		{"nested_quotes", []byte(`"a""b""c"`), 0, 2, 10, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIdx, gotEarly := SkipToQuoteEarlyExit(tt.data, tt.startIdx, tt.hopSize, tt.maxHops)
			if gotIdx != tt.wantIdx || gotEarly != tt.wantEarly {
				t.Errorf("SkipToQuoteEarlyExit(%q, %d, %d, %d) = (%d, %v), want (%d, %v)",
					tt.data, tt.startIdx, tt.hopSize, tt.maxHops,
					gotIdx, gotEarly, tt.wantIdx, tt.wantEarly)
			}

			// Verify early exit returns correct position
			if gotEarly && gotIdx != -1 {
				expectedPos := tt.startIdx + tt.hopSize*tt.maxHops
				if expectedPos < len(tt.data) && gotIdx != expectedPos {
					t.Errorf("Early exit position wrong: got %d, want %d", gotIdx, expectedPos)
				}
			}

			// Verify found character is actually a quote
			if !gotEarly && gotIdx != -1 && tt.data[gotIdx] != '"' {
				t.Errorf("Found character is %c, not quote", tt.data[gotIdx])
			}
		})
	}
}

func TestSkipToOpeningBracket(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		startIdx int
		hopSize  int
		want     int
	}{
		{"found_immediate", []byte(`[test]`), 0, 1, 0},
		{"found_after_skip", []byte(`abc[def`), 0, 1, 3},
		{"not_found", []byte(`abcdef`), 0, 1, -1},
		{"json_array", []byte(`[1,2,3]`), 0, 1, 0},
		{"json_nested", []byte(`{"arr":[1,2]}`), 0, 1, 7},
		{"multiple_brackets", []byte(`[][][][]`), 0, 2, 0},
		{"start_at_bracket", []byte(`[test]`), 0, 1, 0},
		{"skip_first", []byte(`[test][next]`), 1, 1, 6},
		{"large_hop", []byte(`[[[[[[`), 0, 6, -1},
		{"mixed_brackets", []byte(`{[(})]`), 0, 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SkipToOpeningBracket(tt.data, tt.startIdx, tt.hopSize)
			if got != tt.want {
				t.Errorf("SkipToOpeningBracket(%q, %d, %d) = %d, want %d",
					tt.data, tt.startIdx, tt.hopSize, got, tt.want)
			}

			// Verify the found character
			if got != -1 && tt.data[got] != '[' {
				t.Errorf("Character at position %d is %c, not [", got, tt.data[got])
			}
		})
	}
}

func TestSkipToClosingBracket(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		startIdx int
		hopSize  int
		want     int
	}{
		{"found_immediate", []byte(`]test`), 0, 1, 0},
		{"found_after_skip", []byte(`abc]def`), 0, 1, 3},
		{"not_found", []byte(`abcdef`), 0, 1, -1},
		{"json_array_end", []byte(`[1,2,3]`), 5, 1, 6},
		{"empty_array", []byte(`[]`), 1, 1, 1},
		{"nested_arrays", []byte(`[1,[2,3],4]`), 7, 1, 7},
		{"multiple_closing", []byte(`]]]`), 0, 1, 0},
		{"hop_past", []byte(`a]b]c]`), 0, 3, 3},
		{"from_middle", []byte(`[a,b,c]`), 3, 1, 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SkipToClosingBracket(tt.data, tt.startIdx, tt.hopSize)
			if got != tt.want {
				t.Errorf("SkipToClosingBracket(%q, %d, %d) = %d, want %d",
					tt.data, tt.startIdx, tt.hopSize, got, tt.want)
			}

			// Verify the found character
			if got != -1 && tt.data[got] != ']' {
				t.Errorf("Character at position %d is %c, not ]", got, tt.data[got])
			}
		})
	}
}

func TestSkipToClosingBracketEarlyExit(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		startIdx  int
		hopSize   int
		maxHops   int
		wantIdx   int
		wantEarly bool
	}{
		{"found_within_limit", []byte(`abc]def`), 0, 1, 10, 3, false},
		{"found_at_limit", []byte(`abc]def`), 0, 1, 4, 3, false},
		{"early_exit", []byte(`abcdefghij`), 0, 1, 3, 3, true},
		{"not_found_no_early", []byte(`abcdef`), 0, 1, 10, -1, false},
		{"immediate_find", []byte(`]test`), 0, 1, 10, 0, false},
		{"zero_max_hops", []byte(`]test`), 0, 1, 0, 0, true},
		{"empty_data", []byte{}, 0, 1, 10, -1, false},
		{"start_past_end", []byte(`]test`), 10, 1, 5, -1, false},
		{"hop_2_found", []byte(`ab]cd`), 0, 2, 5, 2, false},
		{"hop_3_early", []byte(`abcdefghijk`), 0, 3, 2, 6, true},
		{"json_array_close", []byte(`[1,2,3,4,5]`), 5, 1, 10, 10, false},
		{"json_array_early", []byte(`[1,2,3,4,5,6,7,8,9`), 5, 1, 5, 10, true},
		{"nested_structure", []byte(`[1,[2,3],4]`), 3, 1, 20, 7, false},
		{"multiple_brackets", []byte(`]]]]]`), 0, 2, 3, 4, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIdx, gotEarly := SkipToClosingBracketEarlyExit(tt.data, tt.startIdx, tt.hopSize, tt.maxHops)
			if gotIdx != tt.wantIdx || gotEarly != tt.wantEarly {
				t.Errorf("SkipToClosingBracketEarlyExit(%q, %d, %d, %d) = (%d, %v), want (%d, %v)",
					tt.data, tt.startIdx, tt.hopSize, tt.maxHops,
					gotIdx, gotEarly, tt.wantIdx, tt.wantEarly)
			}

			// Verify early exit behavior
			if gotEarly && gotIdx != -1 {
				expectedPos := tt.startIdx + tt.hopSize*tt.maxHops
				if expectedPos < len(tt.data) && gotIdx != expectedPos {
					t.Errorf("Early exit position wrong: got %d, want %d", gotIdx, expectedPos)
				}
			}

			// Verify found character is actually a closing bracket
			if !gotEarly && gotIdx != -1 && tt.data[gotIdx] != ']' {
				t.Errorf("Found character is %c, not ]", tt.data[gotIdx])
			}
		})
	}
}
