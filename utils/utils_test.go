// utils_test.go — Comprehensive test suite for high-performance utility functions
package utils

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"unsafe"
)

// ============================================================================
// TEST HELPERS
// ============================================================================

func assertZeroAllocs(t *testing.T, name string, fn func()) {
	t.Helper()
	allocs := testing.AllocsPerRun(100, fn)
	if allocs > 0 {
		t.Errorf("%s allocated: %f allocs/op", name, allocs)
	}
}

// Helper function for testing hash quality
func hammingDistance(a, b uint64) int {
	diff := a ^ b
	count := 0
	for diff != 0 {
		count++
		diff &= diff - 1
	}
	return count
}

// ============================================================================
// HASH MIXING TESTS
// ============================================================================

func TestMix64(t *testing.T) {
	tests := []struct {
		name  string
		input uint64
		want  uint64
	}{
		// Known test vectors for Murmur3 finalization
		{"zero", 0x0000000000000000, 0x0000000000000000},
		{"one", 0x0000000000000001, 0x7fb5d329728ea185},
		{"max", 0xFFFFFFFFFFFFFFFF, 0x7c25d2c60fa2260f},
		{"half_max", 0x7FFFFFFFFFFFFFFF, 0x4b6d8b1a2f4c8b4e},
		{"pattern_1", 0x1234567890abcdef, 0x3fb40bbe2c544baa},
		{"pattern_2", 0xdeadbeefcafebabe, 0x8b3f8b3a8aafe23d},
		{"sequential_1", 0x0123456789abcdef, 0xd168c9fe1c1f40aa},
		{"sequential_2", 0xfedcba9876543210, 0x9e81fb80d5b39dd1},
		{"power_of_2", 0x8000000000000000, 0x3d4c7e7c4d1c3f39},
		{"mersenne_prime", 0x1fffffffffffff, 0xb0497dcc7bf7b4c4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Mix64(tt.input)
			if got != tt.want {
				t.Errorf("Mix64(0x%016X) = 0x%016X, want 0x%016X", tt.input, got, tt.want)
			}
		})
	}
}

func TestMix64_EdgeCases(t *testing.T) {
	// Test mathematical edge cases
	edgeCases := []uint64{
		0x0000000000000000, // Zero
		0x0000000000000001, // Minimal non-zero
		0x7FFFFFFFFFFFFFFF, // Maximum signed int64
		0x8000000000000000, // Minimum signed int64 (as uint64)
		0xFFFFFFFFFFFFFFFF, // Maximum uint64
		0x5555555555555555, // Alternating bits pattern
		0xAAAAAAAAAAAAAAAA, // Inverse alternating bits
		0x0F0F0F0F0F0F0F0F, // Nibble pattern
		0xF0F0F0F0F0F0F0F0, // Inverse nibble pattern
	}

	for _, input := range edgeCases {
		result := Mix64(input)
		// Just verify it doesn't panic and produces some output
		t.Logf("Mix64(0x%016X) = 0x%016X", input, result)
	}
}

func TestMix64_Properties(t *testing.T) {
	t.Run("avalanche_effect", func(t *testing.T) {
		// Test that single bit changes result in significant output changes
		input := uint64(0x1234567890abcdef)
		original := Mix64(input)

		for bit := 0; bit < 64; bit++ {
			flipped := input ^ (1 << bit)
			result := Mix64(flipped)

			// Count differing bits
			diff := original ^ result
			diffBits := 0
			for diff != 0 {
				diffBits++
				diff &= diff - 1 // Clear lowest set bit
			}

			// Good hash functions should flip ~32 bits on average for 1-bit input change
			if diffBits < 20 || diffBits > 44 {
				t.Errorf("Poor avalanche for bit %d: %d bits changed (expected ~32)", bit, diffBits)
			}
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		// Verify function is deterministic
		inputs := []uint64{0, 1, 0xFFFFFFFFFFFFFFFF, 0x1234567890abcdef}

		for _, input := range inputs {
			first := Mix64(input)
			for i := 0; i < 100; i++ {
				if Mix64(input) != first {
					t.Errorf("Mix64(0x%016X) not deterministic", input)
				}
			}
		}
	})

	t.Run("bijective", func(t *testing.T) {
		// Test that Mix64 is bijective (invertible) by checking small range
		seen := make(map[uint64]bool)

		// Test first 10000 values
		for i := uint64(0); i < 10000; i++ {
			result := Mix64(i)
			if seen[result] {
				t.Errorf("Mix64 collision: Mix64(%d) = Mix64(?)", i)
			}
			seen[result] = true
		}
	})

	t.Run("distribution", func(t *testing.T) {
		// Test bit distribution across multiple inputs
		bitCounts := make([]int, 64)
		numSamples := 10000

		for i := 0; i < numSamples; i++ {
			// Use different input patterns
			input := uint64(i)*0x9e3779b97f4a7c15 + uint64(i>>16) // Mix to avoid simple patterns
			result := Mix64(input)

			for bit := 0; bit < 64; bit++ {
				if result&(1<<bit) != 0 {
					bitCounts[bit]++
				}
			}
		}

		// Each bit should be set approximately 50% of the time
		for bit := 0; bit < 64; bit++ {
			ratio := float64(bitCounts[bit]) / float64(numSamples)
			if ratio < 0.45 || ratio > 0.55 {
				t.Errorf("Bit %d has poor distribution: %.3f (expected ~0.5)", bit, ratio)
			}
		}
	})
}

func TestMix64_ZeroAllocation(t *testing.T) {
	assertZeroAllocs(t, "Mix64()", func() {
		_ = Mix64(0x1234567890abcdef)
	})
}

// ============================================================================
// HEX PARSING TESTS
// ============================================================================

func TestParseEthereumAddress(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  [20]byte
	}{
		{
			"full_address_no_prefix",
			[]byte("1234567890abcdefABCDEF1234567890abcdefAB"),
			[20]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0xab},
		},
		{
			"real_ethereum_address",
			[]byte("dAC17F958D2ee523a2206206994597C13D831ec7"),
			[20]byte{0xda, 0xc1, 0x7f, 0x95, 0x8d, 0x2e, 0xe5, 0x23, 0xa2, 0x20, 0x62, 0x06, 0x99, 0x45, 0x97, 0xc1, 0x3d, 0x83, 0x1e, 0xc7},
		},
		{
			"zeros_address",
			[]byte("0000000000000000000000000000000000000000"),
			[20]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			"max_address",
			[]byte("ffffffffffffffffffffffffffffffffffffffff"),
			[20]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseEthereumAddress(tt.input)
			if got != tt.want {
				t.Errorf("ParseEthereumAddress(%q) = %x, want %x", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseHexU32(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint32
	}{
		{"single", []byte("f"), 15},
		{"four", []byte("1234"), 0x1234},
		{"eight", []byte("deadbeef"), 0xdeadbeef},
		{"max32", []byte("ffffffff"), 0xffffffff},
		{"mixed_case", []byte("DeAdBeEf"), 0xdeadbeef},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseHexU32(tt.input)
			if got != tt.want {
				t.Errorf("ParseHexU32(%q) = 0x%X, want 0x%X", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseHexU64(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint64
	}{
		{"single", []byte("f"), 15},
		{"no_prefix", []byte("deadbeef"), 0xdeadbeef},
		{"mixed_case", []byte("DeAdBeEf"), 0xdeadbeef},
		{"sixteen_chars", []byte("0123456789abcdef"), 0x0123456789abcdef},
		{"max64_truncated", []byte("ffffffffffffffff"), 0xffffffffffffffff},
		{"over_sixteen_chars", []byte("ffffffffffffffffdeadbeef"), 0xffffffffffffffff}, // Truncates to first 16 chars
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseHexU64(tt.input)
			if got != tt.want {
				t.Errorf("ParseHexU64(%q) = 0x%X, want 0x%X", tt.input, got, tt.want)
			}
		})
	}
}

func TestHexParsing_ZeroAllocation(t *testing.T) {
	hex32 := []byte("deadbeef")
	hex64 := []byte("deadbeef12345678")
	ethAddr := []byte("1234567890abcdefABCDEF1234567890abcdefAB")

	assertZeroAllocs(t, "ParseHexU32()", func() {
		_ = ParseHexU32(hex32)
	})

	assertZeroAllocs(t, "ParseHexU64()", func() {
		_ = ParseHexU64(hex64)
	})

	assertZeroAllocs(t, "ParseEthereumAddress()", func() {
		_ = ParseEthereumAddress(ethAddr)
	})
}

// ============================================================================
// JSON PARSING TESTS
// ============================================================================

func TestSkipToClosingBracket(t *testing.T) {
	tests := []struct {
		data  []byte
		start int
		hop   int
		want  int
	}{
		{[]byte(`]test`), 0, 1, 0},
		{[]byte(`abc]def`), 0, 1, 3},
		{[]byte(`abcdef`), 0, 1, -1},
	}

	for _, tt := range tests {
		got := SkipToClosingBracket(tt.data, tt.start, tt.hop)
		if got != tt.want {
			t.Errorf("SkipToClosingBracket(%q, %d, %d) = %d, want %d",
				tt.data, tt.start, tt.hop, got, tt.want)
		}
	}
}

func TestSkipToClosingBracketEarlyExit(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		start     int
		hop       int
		max       int
		wantIdx   int
		wantEarly bool
	}{
		{"found_bracket", []byte(`[1,2]`), 1, 1, 10, 4, false},
		{"early_exit", []byte(`[very,long`), 1, 1, 3, 4, true},
		{"not_found", []byte(`[unclosed`), 1, 1, 20, -1, false},
		{"large_hop", []byte(`[1,2,3]`), 0, 3, 10, 6, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, early := SkipToClosingBracketEarlyExit(tt.data, tt.start, tt.hop, tt.max)
			if idx != tt.wantIdx || early != tt.wantEarly {
				t.Errorf("SkipToClosingBracketEarlyExit() = (%d, %v), want (%d, %v)",
					idx, early, tt.wantIdx, tt.wantEarly)
			}
		})
	}
}

func TestSkipToOpeningBracket(t *testing.T) {
	tests := []struct {
		data  []byte
		start int
		hop   int
		want  int
	}{
		{[]byte(`[test]`), 0, 1, 0},
		{[]byte(`abc[def`), 0, 1, 3},
		{[]byte(`abcdef`), 0, 1, -1},
	}

	for _, tt := range tests {
		got := SkipToOpeningBracket(tt.data, tt.start, tt.hop)
		if got != tt.want {
			t.Errorf("SkipToOpeningBracket(%q, %d, %d) = %d, want %d",
				tt.data, tt.start, tt.hop, got, tt.want)
		}
	}
}

func TestSkipToQuote(t *testing.T) {
	tests := []struct {
		data  []byte
		start int
		hop   int
		want  int
	}{
		{[]byte(`"test"`), 0, 1, 0},
		{[]byte(`abc"def`), 0, 1, 3},
		{[]byte(`abcdef`), 0, 1, -1},
		{[]byte(`{"a":"b"}`), 0, 3, 3},
	}

	for _, tt := range tests {
		got := SkipToQuote(tt.data, tt.start, tt.hop)
		if got != tt.want {
			t.Errorf("SkipToQuote(%q, %d, %d) = %d, want %d",
				tt.data, tt.start, tt.hop, got, tt.want)
		}
	}
}

func TestSkipToQuoteEarlyExit(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		start     int
		hop       int
		max       int
		wantIdx   int
		wantEarly bool
	}{
		{"found_quote", []byte(`{"x":"y"}`), 2, 1, 10, 3, false},
		{"early_exit", []byte(`{verylong`), 1, 1, 3, 4, true},
		{"not_found", []byte(`{x:y}`), 1, 1, 10, -1, false},
		{"large_hop", []byte(`{"a":"b"}`), 0, 3, 10, 3, false},
		{"at_quote", []byte(`"test"`), 0, 1, 10, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, early := SkipToQuoteEarlyExit(tt.data, tt.start, tt.hop, tt.max)
			if idx != tt.wantIdx || early != tt.wantEarly {
				t.Errorf("SkipToQuoteEarlyExit() = (%d, %v), want (%d, %v)",
					idx, early, tt.wantIdx, tt.wantEarly)
			}
		})
	}
}

// ============================================================================
// MEMORY OPERATION TESTS
// ============================================================================

func TestLoad128(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want1 uint64
		want2 uint64
	}{
		{"zeros", make([]byte, 16), 0, 0},
		{"sequence", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			0x0807060504030201, 0x100F0E0D0C0B0A09},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2 := Load128(tt.input)
			if got1 != tt.want1 || got2 != tt.want2 {
				t.Errorf("Load128() = (0x%016X, 0x%016X), want (0x%016X, 0x%016X)",
					got1, got2, tt.want1, tt.want2)
			}
		})
	}
}

func TestLoad64(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint64
	}{
		{"zeros", []byte{0, 0, 0, 0, 0, 0, 0, 0}, 0},
		{"ones", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, 0xFFFFFFFFFFFFFFFF},
		{"sequence", []byte{1, 2, 3, 4, 5, 6, 7, 8}, 0x0807060504030201},
		{"mixed", []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22}, 0x2211FFEEDDCCBBAA},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Load64(tt.input)
			if got != tt.want {
				t.Errorf("Load64() = 0x%016X, want 0x%016X", got, tt.want)
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
		{"zeros", []byte{0, 0, 0, 0, 0, 0, 0, 0}, 0},
		{"ones", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, 0xFFFFFFFFFFFFFFFF},
		{"sequence", []byte{1, 2, 3, 4, 5, 6, 7, 8}, 0x0102030405060708},
		{"mixed", []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22}, 0xAABBCCDDEEFF1122},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LoadBE64(tt.input)
			if got != tt.want {
				t.Errorf("LoadBE64() = 0x%016X, want 0x%016X", got, tt.want)
			}
		})
	}
}

func TestMemoryOperations_ZeroAllocation(t *testing.T) {
	data := make([]byte, 16)

	assertZeroAllocs(t, "Load64()", func() {
		_ = Load64(data)
	})

	assertZeroAllocs(t, "Load128()", func() {
		_, _ = Load128(data)
	})

	assertZeroAllocs(t, "LoadBE64()", func() {
		_ = LoadBE64(data)
	})
}

// ============================================================================
// SYSTEM I/O TESTS
// ============================================================================

func TestPrintFunctions(t *testing.T) {
	// Test that functions don't panic with various inputs
	messages := []string{
		"",
		"test",
		"unicode: 测试",
		strings.Repeat("x", 100),
	}

	for _, msg := range messages {
		// These write to stdout/stderr during test
		PrintInfo(msg)
		PrintWarning(msg)
	}
}

func TestPrintFunctions_ZeroAllocation(t *testing.T) {
	msg := "test"

	assertZeroAllocs(t, "PrintInfo()", func() {
		PrintInfo(msg)
	})

	assertZeroAllocs(t, "PrintWarning()", func() {
		PrintWarning(msg)
	})
}

// ============================================================================
// TYPE CONVERSION TESTS
// ============================================================================

func TestB2s(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{"single", []byte{'a'}, "a"},
		{"ascii", []byte("hello world"), "hello world"},
		{"utf8", []byte("héllo wørld"), "héllo wørld"},
		{"binary", []byte{0x00, 0x01, 0xFF}, string([]byte{0x00, 0x01, 0xFF})},
		{"large", []byte(strings.Repeat("x", 1000)), strings.Repeat("x", 1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := B2s(tt.input)
			if got != tt.want {
				t.Errorf("B2s() = %q, want %q", got, tt.want)
			}

			// Verify zero-copy behavior
			inputPtr := unsafe.Pointer(&tt.input[0])
			resultPtr := unsafe.Pointer(unsafe.StringData(got))
			if inputPtr != resultPtr {
				t.Error("B2s() should share underlying data")
			}
		})
	}
}

func TestItoa(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  string
	}{
		{"zero", 0, "0"},
		{"single", 5, "5"},
		{"double", 42, "42"},
		{"triple", 123, "123"},
		{"large", 987654321, "987654321"},
		{"max32", 2147483647, "2147483647"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Itoa(tt.input)
			if got != tt.want {
				t.Errorf("Itoa(%d) = %q, want %q", tt.input, got, tt.want)
			}
			// Cross-verify with stdlib
			if got != strconv.Itoa(tt.input) {
				t.Errorf("Itoa(%d) differs from strconv", tt.input)
			}
		})
	}
}

func TestTypeConversion_ZeroAllocation(t *testing.T) {
	input := []byte("test")
	assertZeroAllocs(t, "B2s()", func() {
		_ = B2s(input)
	})

	assertZeroAllocs(t, "Itoa()", func() {
		_ = Itoa(12345)
	})
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

func TestEdgeCases(t *testing.T) {
	t.Run("boundaries", func(t *testing.T) {
		// Test minimum required sizes
		_ = Load64(make([]byte, 8))
		_, _ = Load128(make([]byte, 16))
		_ = LoadBE64(make([]byte, 8))
	})

	t.Run("invalid_utf8", func(t *testing.T) {
		invalid := []byte{0xFF, 0xFE, 0xFD}
		result := B2s(invalid)
		if len(result) != len(invalid) {
			t.Error("B2s should preserve byte length")
		}
	})
}

// ============================================================================
// BENCHMARKS
// ============================================================================

func BenchmarkComparison(b *testing.B) {
	testInt := 123456
	testBytes := []byte("hello world")

	b.Run("Itoa_custom", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Itoa(testInt)
		}
	})

	b.Run("Itoa_stdlib", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = strconv.Itoa(testInt)
		}
	})

	b.Run("B2s_custom", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = B2s(testBytes)
		}
	})

	b.Run("B2s_stdlib", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = string(testBytes)
		}
	})
}

func BenchmarkHexParsing(b *testing.B) {
	hex32 := []byte("deadbeef")
	hex64 := []byte("deadbeef12345678")
	ethAddr := []byte("1234567890abcdefABCDEF1234567890abcdefAB")

	b.Run("ParseHexU32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ParseHexU32(hex32)
		}
	})

	b.Run("ParseHexU64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ParseHexU64(hex64)
		}
	})

	b.Run("ParseEthereumAddress", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ParseEthereumAddress(ethAddr)
		}
	})
}

func BenchmarkJSONParsing(b *testing.B) {
	data := []byte(`{"field":"value","array":[1,2,3],"nested":{"a":"b"}}`)

	b.Run("SkipToQuote", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = SkipToQuote(data, 0, 1)
		}
	})

	b.Run("SkipToOpeningBracket", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = SkipToOpeningBracket(data, 0, 1)
		}
	})
}

func BenchmarkMemoryOps(b *testing.B) {
	data := make([]byte, 16)
	for i := range data {
		data[i] = byte(i)
	}

	b.Run("Load64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Load64(data)
		}
	})

	b.Run("Load128", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = Load128(data)
		}
	})

	b.Run("LoadBE64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = LoadBE64(data)
		}
	})
}

func BenchmarkMix64(b *testing.B) {
	input := uint64(0x1234567890abcdef)

	b.Run("single_input", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Mix64(input)
		}
	})

	b.Run("varying_inputs", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Mix64(uint64(i))
		}
	})

	b.Run("worst_case_input", func(b *testing.B) {
		// Use input that might stress the multiplication chains
		worstCase := uint64(0xFFFFFFFFFFFFFFFF)
		for i := 0; i < b.N; i++ {
			_ = Mix64(worstCase)
		}
	})
}

func BenchmarkMix64_Comparison(b *testing.B) {
	input := uint64(0x1234567890abcdef)

	b.Run("Mix64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Mix64(input)
		}
	})

	// Compare against simpler hash functions
	b.Run("simple_multiply", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = input * 0x9e3779b97f4a7c15
		}
	})

	b.Run("xor_shift", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			x := input
			x ^= x >> 33
			x ^= x << 13
			x ^= x >> 7
			_ = x
		}
	})
}

func BenchmarkMix64_Pipeline(b *testing.B) {
	// Test how well Mix64 performs in a pipeline scenario
	inputs := []uint64{
		0x1234567890abcdef,
		0xdeadbeefcafebabe,
		0x0123456789abcdef,
		0xfedcba9876543210,
	}

	b.Run("sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, input := range inputs {
				_ = Mix64(input)
			}
		}
	})

	b.Run("chained", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := inputs[0]
			for j := 1; j < len(inputs); j++ {
				result = Mix64(result ^ inputs[j])
			}
			_ = result
		}
	})
}

func BenchmarkMix64_Quality(b *testing.B) {
	// This benchmark tests the quality of the hash function
	// by measuring how much outputs differ for similar inputs

	b.Run("avalanche_quality", func(b *testing.B) {
		totalDistance := 0

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			input := uint64(i)
			original := Mix64(input)
			flipped := Mix64(input ^ 1) // Flip one bit
			totalDistance += hammingDistance(original, flipped)
		}
		b.StopTimer()

		avgDistance := float64(totalDistance) / float64(b.N)
		b.ReportMetric(avgDistance, "avg_hamming_distance")
	})
}

func BenchmarkMix64_Throughput(b *testing.B) {
	// Benchmark processing arrays of data
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("array_%d", size), func(b *testing.B) {
			data := make([]uint64, size)
			for i := range data {
				data[i] = uint64(i) * 0x9e3779b97f4a7c15
			}

			b.ResetTimer()
			b.SetBytes(int64(size * 8)) // 8 bytes per uint64

			for i := 0; i < b.N; i++ {
				for j := range data {
					data[j] = Mix64(data[j])
				}
			}
		})
	}
}

func BenchmarkTypeConversion(b *testing.B) {
	testInt := 123456
	testBytes := []byte("hello world")

	b.Run("B2s", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = B2s(testBytes)
		}
	})

	b.Run("Itoa", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Itoa(testInt)
		}
	})
}

func BenchmarkZeroAlloc(b *testing.B) {
	data := make([]byte, 1024)
	hex := []byte("deadbeef")
	ethAddr := []byte("1234567890abcdefABCDEF1234567890abcdefAB")

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = B2s(data)
		_ = Load64(data)
		_ = ParseHexU64(hex)
		_ = ParseEthereumAddress(ethAddr)
	}

	b.StopTimer()
	runtime.ReadMemStats(&m2)

	if b.N > 0 {
		allocDelta := m2.TotalAlloc - m1.TotalAlloc
		allocsPerOp := float64(allocDelta) / float64(b.N)
		b.ReportMetric(allocsPerOp, "actual_bytes/op")

		if allocsPerOp > 1 {
			b.Logf("WARNING: %.2f bytes/op allocated", allocsPerOp)
		}
	}
}
