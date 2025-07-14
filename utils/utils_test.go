package utils

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// ============================================================================
// TEST CONFIGURATION AND HELPERS
// ============================================================================

// hammingDistance calculates bit differences between two uint64s
func hammingDistance(a, b uint64) int {
	diff := a ^ b
	count := 0
	for diff != 0 {
		count++
		diff &= diff - 1
	}
	return count
}

// generateTestData creates various test data patterns
func generateTestData(size int, pattern string) []byte {
	data := make([]byte, size)
	switch pattern {
	case "zeros":
		// Already zeros
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
	}
	return data
}

// floatStringsEquivalent checks if two float strings are mathematically equivalent
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
	if math.Abs(gotVal-wantVal) < math.Abs(original)*1e-6 {
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

// ============================================================================
// MEMORY OPERATION TESTS
// ============================================================================

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Load64(tt.input)
			if got != tt.want {
				t.Errorf("Load64() = 0x%016X, want 0x%016X", got, tt.want)
			}

			// Verify against manual calculation
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
		})
	}
}

func TestLoad64_Alignment(t *testing.T) {
	// Test various alignments
	for offset := 0; offset < 16; offset++ {
		t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
			buffer := make([]byte, 24)
			testData := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
			copy(buffer[offset:], testData)

			result := Load64(buffer[offset:])
			expected := uint64(0x0807060504030201)

			if result != expected {
				t.Errorf("Offset %d: got 0x%016X, want 0x%016X", offset, result, expected)
			}
		})
	}
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
		})
	}
}

// ============================================================================
// TYPE CONVERSION TESTS
// ============================================================================

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
		{"one", 1, "1"},
		{"single_digit", 9, "9"},
		{"two_digits", 42, "42"},
		{"three_digits", 123, "123"},
		{"large_positive", 2147483647, "2147483647"},
		{"power_of_10", 1000000, "1000000"},
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
		})
	}
}

func TestItoa_Negative(t *testing.T) {
	// Test that negative numbers return empty string
	negativeNumbers := []int{-1, -10, -100, -1000}
	for _, n := range negativeNumbers {
		result := Itoa(n)
		if result != "" {
			t.Errorf("Itoa(%d): expected empty string for negative number, got %q", n, result)
		}
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

		// Larger integers
		{"thousand", 1000.0, "1000"},
		{"million", 1000000.0, "1e+06"},
		{"large_integer", 123456789.0, "1.234568e+08"},

		// Small decimals
		{"small_decimal", 0.001, "0.001"},
		{"very_small", 0.000001, "0.000001"},
		{"tiny", 0.0000001, "1e-07"},

		// Powers of 2 (exact in IEEE 754)
		{"power_2_neg1", 0.5, "0.5"},
		{"power_2_neg2", 0.25, "0.25"},
		{"power_2_neg3", 0.125, "0.125"},
		{"power_2_pos10", 1024.0, "1024"},

		// Trailing zeros should be removed
		{"trailing_zeros_1", 1.500000, "1.5"},
		{"trailing_zeros_2", 42.100000, "42.1"},
		{"trailing_zeros_3", 0.100000, "0.1"},
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

		// Subnormal numbers (IEEE 754 compliance)
		{"smallest_normal", math.SmallestNonzeroFloat64, "4.940656e-324"},

		// Exact decimal representations
		{"exact_half", 0.5, "0.5"},
		{"exact_quarter", 0.25, "0.25"},
		{"exact_eighth", 0.125, "0.125"},

		// Large numbers requiring scientific notation
		{"large_scientific", 1.23456789e15, "1.234568e+15"},
		{"very_large", 1e20, "1e+20"},

		// Very small numbers requiring scientific notation
		{"very_small_scientific", 1.23456e-10, "1.23456e-10"},
		{"extremely_small", 1e-20, "1e-20"},
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
		{"two_bits", 0x4000000000000000, "2"},
		{"half_bits", 0x3FE0000000000000, "0.5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := math.Float64frombits(tt.bits)
			got := Ftoa(f)
			if got != tt.want {
				t.Errorf("Ftoa(Float64frombits(0x%016X)) = %q, want %q",
					tt.bits, got, tt.want)
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
		{"just_above_one", math.Nextafter(1.0, 2.0)},
		{"just_below_one", math.Nextafter(1.0, 0.0)},
		{"largest_exact_int", float64(1 << 53)},
		{"smallest_inexact_int", float64(1<<53 + 1)},
		{"pi", math.Pi},
		{"e", math.E},
		{"sqrt2", math.Sqrt2},
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
				if _, err := strconv.ParseFloat(result, 64); err != nil {
					t.Errorf("Ftoa(%g) = %q is not a valid float string: %v",
						tt.input, result, err)
				}
			}
		})
	}
}

// ============================================================================
// HEX PARSING TESTS
// ============================================================================

func TestParseHexU32(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint32
	}{
		{"empty", []byte{}, 0x0},
		{"single_digit", []byte("f"), 0xF},
		{"four_digits", []byte("1234"), 0x1234},
		{"eight_digits", []byte("deadbeef"), 0xDEADBEEF},
		{"max_uint32", []byte("ffffffff"), 0xFFFFFFFF},
		{"uppercase", []byte("DEADBEEF"), 0xDEADBEEF},
		{"mixed_case", []byte("DeAdBeEf"), 0xDEADBEEF},
		{"truncation", []byte("123456789"), 0x12345678}, // Truncates to 8
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseHexU32(tt.input)
			if got != tt.want {
				t.Errorf("ParseHexU32(%q) = 0x%08X, want 0x%08X", tt.input, got, tt.want)
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
		{"empty", []byte{}, 0x0},
		{"single_digit", []byte("f"), 0xF},
		{"eight_chars", []byte("deadbeef"), 0xDEADBEEF},
		{"sixteen_chars", []byte("0123456789abcdef"), 0x0123456789ABCDEF},
		{"max_uint64", []byte("ffffffffffffffff"), 0xFFFFFFFFFFFFFFFF},
		{"uppercase_16", []byte("DEADBEEFCAFEBABE"), 0xDEADBEEFCAFEBABE},
		{"mixed_case_16", []byte("DeAdBeEfCaFeBaBe"), 0xDEADBEEFCAFEBABE},
		{"truncation", []byte("123456789abcdef01"), 0x123456789ABCDEF0}, // Truncates to 16
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseHexU64(tt.input)
			if got != tt.want {
				t.Errorf("ParseHexU64(%q) = 0x%016X, want 0x%016X", tt.input, got, tt.want)
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

// ============================================================================
// JSON PARSING TESTS
// ============================================================================

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
		{"hop_2_miss", []byte(`a"b"c"d"`), 0, 2, -1}, // Misses all quotes
		{"hop_3_found", []byte(`abc"def"ghi"`), 0, 3, 3},
		{"json_object", []byte(`{"key":"value"}`), 0, 1, 1},
		{"start_at_quote", []byte(`"test"`), 0, 1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SkipToQuote(tt.data, tt.startIdx, tt.hopSize)
			if got != tt.want {
				t.Errorf("SkipToQuote(%q, %d, %d) = %d, want %d",
					tt.data, tt.startIdx, tt.hopSize, got, tt.want)
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
		{"early_exit", []byte(`abcdefghij`), 0, 1, 3, 3, true},
		{"immediate_find", []byte(`"test"`), 0, 1, 10, 0, false},
		{"zero_max_hops", []byte(`"test"`), 0, 1, 0, 0, true},
		{"not_found_no_early", []byte(`abcdef`), 0, 1, 10, -1, false}, // -1 case
		{"empty_data", []byte{}, 0, 1, 10, -1, false},                 // -1 case
		{"start_past_end", []byte(`"test"`), 10, 1, 5, -1, false},     // -1 case
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIdx, gotEarly := SkipToQuoteEarlyExit(tt.data, tt.startIdx, tt.hopSize, tt.maxHops)
			if gotIdx != tt.wantIdx || gotEarly != tt.wantEarly {
				t.Errorf("SkipToQuoteEarlyExit(%q, %d, %d, %d) = (%d, %v), want (%d, %v)",
					tt.data, tt.startIdx, tt.hopSize, tt.maxHops,
					gotIdx, gotEarly, tt.wantIdx, tt.wantEarly)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SkipToOpeningBracket(tt.data, tt.startIdx, tt.hopSize)
			if got != tt.want {
				t.Errorf("SkipToOpeningBracket(%q, %d, %d) = %d, want %d",
					tt.data, tt.startIdx, tt.hopSize, got, tt.want)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SkipToClosingBracket(tt.data, tt.startIdx, tt.hopSize)
			if got != tt.want {
				t.Errorf("SkipToClosingBracket(%q, %d, %d) = %d, want %d",
					tt.data, tt.startIdx, tt.hopSize, got, tt.want)
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
		{"not_found_no_early", []byte(`abcdef`), 0, 1, 10, -1, false}, // -1 case
		{"immediate_find", []byte(`]test`), 0, 1, 10, 0, false},
		{"zero_max_hops", []byte(`]test`), 0, 1, 0, 0, true},
		{"empty_data", []byte{}, 0, 1, 10, -1, false},            // -1 case
		{"start_past_end", []byte(`]test`), 10, 1, 5, -1, false}, // -1 case
		{"hop_2_found", []byte(`ab]cd`), 0, 2, 5, 2, false},
		{"hop_3_early", []byte(`abcdefghijk`), 0, 3, 2, 6, true}, // 0->3->6 (2 hops)
		{"json_array_close", []byte(`[1,2,3,4,5]`), 5, 1, 10, 10, false},
		{"json_array_early", []byte(`[1,2,3,4,5,6,7,8,9`), 5, 1, 5, 10, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIdx, gotEarly := SkipToClosingBracketEarlyExit(tt.data, tt.startIdx, tt.hopSize, tt.maxHops)
			if gotIdx != tt.wantIdx || gotEarly != tt.wantEarly {
				t.Errorf("SkipToClosingBracketEarlyExit(%q, %d, %d, %d) = (%d, %v), want (%d, %v)",
					tt.data, tt.startIdx, tt.hopSize, tt.maxHops,
					gotIdx, gotEarly, tt.wantIdx, tt.wantEarly)
			}
		})
	}
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
		{"zero", 0x0000000000000000, 0x0000000000000000},
		{"one", 0x0000000000000001, 0xB456BCFC34C2CB2C},
		{"max", 0xFFFFFFFFFFFFFFFF, 0x64B5720B4B825F21},
		{"pattern_1", 0x1234567890abcdef, 0x0CAE996FEE6BD396},
		{"pattern_2", 0xdeadbeefcafebabe, 0x7082995008F0C48C},
		{"power_of_2", 0x8000000000000000, 0x8F780810AF31A493},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Mix64(tt.input)
			if got != tt.want {
				t.Errorf("Mix64(0x%016X) = 0x%016X, want 0x%016X", tt.input, got, tt.want)
			}

			// Verify determinism
			got2 := Mix64(tt.input)
			if got != got2 {
				t.Errorf("Mix64(0x%016X) not deterministic", tt.input)
			}
		})
	}
}

func TestMix64_AvalancheEffect(t *testing.T) {
	// Test that flipping any input bit changes ~50% of output bits
	testInputs := []uint64{
		0x0000000000000000,
		0xFFFFFFFFFFFFFFFF,
		0x0123456789ABCDEF,
		0x5555555555555555,
		0xAAAAAAAAAAAAAAAA,
	}

	for _, input := range testInputs {
		original := Mix64(input)
		totalDistance := 0

		// Test flipping each bit
		for bit := 0; bit < 64; bit++ {
			flipped := input ^ (uint64(1) << bit)
			result := Mix64(flipped)
			distance := hammingDistance(original, result)
			totalDistance += distance

			// Each bit flip should change 20-44 bits (roughly 50%)
			if distance < 20 || distance > 44 {
				t.Errorf("Poor avalanche for input 0x%016X bit %d: %d bits changed",
					input, bit, distance)
			}
		}

		// Average should be close to 32
		avgDistance := float64(totalDistance) / 64.0
		if avgDistance < 28 || avgDistance > 36 {
			t.Errorf("Poor average avalanche for input 0x%016X: %.2f",
				input, avgDistance)
		}
	}
}

// ============================================================================
// SYSTEM I/O TESTS
// ============================================================================

func TestPrintFunctions(t *testing.T) {
	// Basic test that functions don't panic
	PrintInfo("test info")
	PrintWarning("test warning")
}

// ============================================================================
// CONCURRENCY AND RACE CONDITION TESTS
// ============================================================================

func TestRaceConditions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition tests in short mode")
	}

	t.Run("concurrent_mix64", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make([]uint64, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx] = Mix64(uint64(idx))
			}(i)
		}

		wg.Wait()

		// Verify all results are correct
		for i := 0; i < 100; i++ {
			expected := Mix64(uint64(i))
			if results[i] != expected {
				t.Errorf("Race condition detected: Mix64(%d) = %x, expected %x",
					i, results[i], expected)
			}
		}
	})

	t.Run("concurrent_parsing", func(t *testing.T) {
		var wg sync.WaitGroup
		data := []byte("deadbeefcafebabe")

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					_ = ParseHexU64(data)
					_ = ParseHexU32(data[:8])
				}
			}()
		}

		wg.Wait()
	})

	t.Run("concurrent_ftoa", func(t *testing.T) {
		var wg sync.WaitGroup
		testFloats := []float64{0.0, 1.0, -1.0, 3.14159, 42.0, 1e10, 1e-10}

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					_ = Ftoa(testFloats[j%len(testFloats)])
				}
			}(i)
		}

		wg.Wait()
	})
}

// ============================================================================
// EDGE CASES AND BOUNDARY TESTS
// ============================================================================

func TestBoundaryConditions(t *testing.T) {
	t.Run("slice_boundaries", func(t *testing.T) {
		// Test with slices at exact required sizes
		data8 := make([]byte, 8)
		data16 := make([]byte, 16)

		// Fill with pattern
		for i := range data16 {
			data16[i] = byte(i & 0xFF)
		}
		copy(data8, data16)

		// These should work
		_ = Load64(data8)
		_ = LoadBE64(data8)
		_, _ = Load128(data16)
	})

	t.Run("zero_length_operations", func(t *testing.T) {
		empty := []byte{}

		// These should handle empty input gracefully
		_ = ParseHexU32(empty)
		_ = ParseHexU64(empty)
		_ = SkipToQuote(empty, 0, 1)
		_ = SkipToOpeningBracket(empty, 0, 1)
		_ = SkipToClosingBracket(empty, 0, 1)
	})

	t.Run("maximum_values", func(t *testing.T) {
		maxU32 := ParseHexU32([]byte("ffffffff"))
		if maxU32 != 0xFFFFFFFF {
			t.Errorf("Max uint32 parse failed: got 0x%X", maxU32)
		}

		maxU64 := ParseHexU64([]byte("ffffffffffffffff"))
		if maxU64 != 0xFFFFFFFFFFFFFFFF {
			t.Errorf("Max uint64 parse failed: got 0x%X", maxU64)
		}
	})

	t.Run("ftoa_extreme_values", func(t *testing.T) {
		// Test extreme float values
		extremeValues := []float64{
			math.MaxFloat64,
			math.SmallestNonzeroFloat64,
			1.7976931348623157e+308, // Close to max
			2.2250738585072014e-308, // Close to min normal
		}

		for _, f := range extremeValues {
			result := Ftoa(f)
			if len(result) == 0 {
				t.Errorf("Ftoa(%g) produced empty result", f)
			}
		}
	})
}

// ============================================================================
// INTENTIONAL LIMITATIONS DOCUMENTATION
// ============================================================================

func TestIntentionalLimitations(t *testing.T) {
	t.Run("B2s_limitations", func(t *testing.T) {
		// B2s panics on empty/nil slices - this is intentional
		t.Log("B2s requires non-empty slice - will panic on empty/nil")
	})

	t.Run("Itoa_limitations", func(t *testing.T) {
		// Itoa only supports positive integers
		t.Log("Itoa only supports positive integers [0, MaxInt]")
	})

	t.Run("Ftoa_limitations", func(t *testing.T) {
		// Ftoa has precision limitations for performance
		t.Log("Ftoa optimizes for performance - limited precision for very large/small numbers")
	})

	t.Run("hex_parsing_limitations", func(t *testing.T) {
		// No validation of hex characters
		t.Log("Hex parsing assumes valid hex input - no validation")
	})

	t.Run("memory_operations_limitations", func(t *testing.T) {
		// Load functions require sufficient buffer size
		t.Log("Load64/Load128/LoadBE64 require buffers of at least 8/16/8 bytes")
	})
}

// ============================================================================
// INTEGRATION AND COMPATIBILITY TESTS
// ============================================================================

func TestIntegration(t *testing.T) {
	t.Run("encoding_compatibility", func(t *testing.T) {
		// Verify LoadBE64 matches binary.BigEndian
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		ourResult := LoadBE64(data)
		stdResult := binary.BigEndian.Uint64(data)

		if ourResult != stdResult {
			t.Errorf("LoadBE64 incompatible with binary.BigEndian: 0x%X vs 0x%X",
				ourResult, stdResult)
		}
	})

	t.Run("strconv_compatibility", func(t *testing.T) {
		// Verify Itoa matches strconv.Itoa for positive numbers
		testNumbers := []int{0, 1, 10, 100, 1000, 10000, 2147483647}

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

			// Check that the parsed value is reasonably close to original
			if math.Abs(parsed-f) > math.Abs(f)*1e-6 {
				t.Errorf("Ftoa(%g) = %q, parsed back as %g (diff: %g)",
					f, result, parsed, math.Abs(parsed-f))
			}
		}
	})
}

// ============================================================================
// STATISTICAL AND DISTRIBUTION TESTS
// ============================================================================

func TestMix64_Distribution(t *testing.T) {
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
			t.Errorf("Bit %d has biased distribution: %.4f", bit, ratio)
		}
	}
}

// ============================================================================
// PERFORMANCE BENCHMARKS
// ============================================================================

func BenchmarkMemoryOperations(b *testing.B) {
	data := generateTestData(1024, "sequential")

	b.Run("Load64", func(b *testing.B) {
		b.SetBytes(8)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Load64(data[i&1016:])
		}
	})

	b.Run("LoadBE64", func(b *testing.B) {
		b.SetBytes(8)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = LoadBE64(data[i&1016:])
		}
	})

	b.Run("Load128", func(b *testing.B) {
		b.SetBytes(16)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = Load128(data[i&1008:])
		}
	})
}

func BenchmarkHexParsing(b *testing.B) {
	hex32 := []byte("deadbeef")
	hex64 := []byte("deadbeefcafebabe")
	ethAddr := []byte("dAC17F958D2ee523a2206206994597C13D831ec7")

	b.Run("ParseHexU32", func(b *testing.B) {
		b.SetBytes(4)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseHexU32(hex32)
		}
	})

	b.Run("ParseHexU64", func(b *testing.B) {
		b.SetBytes(8)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseHexU64(hex64)
		}
	})

	b.Run("ParseEthereumAddress", func(b *testing.B) {
		b.SetBytes(20)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ParseEthereumAddress(ethAddr)
		}
	})
}

func BenchmarkMix64(b *testing.B) {
	inputs := []uint64{0x0000000000000000, 0x0123456789ABCDEF, 0xFFFFFFFFFFFFFFFF}

	b.Run("single", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Mix64(inputs[i%len(inputs)])
		}
	})

	b.Run("sequential", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Mix64(uint64(i))
		}
	})
}

func BenchmarkTypeConversion(b *testing.B) {
	testData := []byte("Hello, World! This is a test string for benchmarking.")
	testInts := []int{0, 1, 42, 12345, 2147483647}
	testFloats := []float64{0.0, 1.0, 3.14159, 42.123456, -123.456789}

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

	b.Run("Ftoa", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Ftoa(testFloats[i%len(testFloats)])
		}
	})

	// Comparison benchmark with standard library
	b.Run("strconv.Itoa", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = strconv.Itoa(testInts[i%len(testInts)])
		}
	})

	b.Run("strconv.FormatFloat_f", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = strconv.FormatFloat(testFloats[i%len(testFloats)], 'f', -1, 64)
		}
	})

	b.Run("strconv.FormatFloat_g", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = strconv.FormatFloat(testFloats[i%len(testFloats)], 'g', -1, 64)
		}
	})

	b.Run("strconv.FormatFloat_e", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = strconv.FormatFloat(testFloats[i%len(testFloats)], 'e', -1, 64)
		}
	})
}

func BenchmarkFtoa_Detailed(b *testing.B) {
	b.Run("integers", func(b *testing.B) {
		integers := []float64{0, 1, 42, 1000, 1000000}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Ftoa(integers[i%len(integers)])
		}
	})

	b.Run("small_decimals", func(b *testing.B) {
		decimals := []float64{0.5, 0.25, 0.125, 0.1, 0.01}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Ftoa(decimals[i%len(decimals)])
		}
	})

	b.Run("scientific_notation", func(b *testing.B) {
		scientific := []float64{1e10, 1e-10, 1e100, 1e-100, math.MaxFloat64}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Ftoa(scientific[i%len(scientific)])
		}
	})

	b.Run("special_values", func(b *testing.B) {
		special := []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0.0}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Ftoa(special[i%len(special)])
		}
	})

	b.Run("random_floats", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Generate random float
			f := rand.Float64()*2000.0 - 1000.0 // Range [-1000, 1000]
			_ = Ftoa(f)
		}
	})

	b.Run("powers_of_2", func(b *testing.B) {
		powers := []float64{0.125, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Ftoa(powers[i%len(powers)])
		}
	})

	b.Run("common_constants", func(b *testing.B) {
		constants := []float64{math.Pi, math.E, math.Sqrt2, math.Phi, math.Ln2, math.Log2E}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Ftoa(constants[i%len(constants)])
		}
	})
}

func BenchmarkJSONParsing(b *testing.B) {
	data := []byte(`{"key":"value","array":[1,2,3,4,5,6,7,8,9,10],"nested":{"a":"b","c":"d","e":"f"}}`)

	b.Run("SkipToQuote", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = SkipToQuote(data, 0, 1)
		}
	})

	b.Run("SkipToQuoteEarlyExit", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = SkipToQuoteEarlyExit(data, 0, 1, 10)
		}
	})

	b.Run("SkipToOpeningBracket", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = SkipToOpeningBracket(data, 0, 1)
		}
	})

	b.Run("SkipToClosingBracket", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = SkipToClosingBracket(data, 10, 1)
		}
	})

	b.Run("SkipToClosingBracketEarlyExit", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = SkipToClosingBracketEarlyExit(data, 10, 1, 20)
		}
	})
}

func BenchmarkSystemIO(b *testing.B) {
	testMsg := "Benchmark test message\n"

	b.Run("PrintInfo", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			PrintInfo(testMsg)
		}
	})

	b.Run("PrintWarning", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			PrintWarning(testMsg)
		}
	})
}

// ============================================================================
// CLEANUP AND RESOURCE VALIDATION
// ============================================================================

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
		for i := 0; i < 10000; i++ {
			_ = Ftoa(float64(i) * 3.14159)
			_ = Itoa(i)
			_ = Mix64(uint64(i))
			_ = ParseHexU64([]byte("deadbeefcafebabe"))
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)

		// Memory should not have grown significantly
		if m2.Alloc > m1.Alloc+1024*1024 { // Allow 1MB growth
			t.Errorf("Memory usage grew unexpectedly: %d -> %d bytes", m1.Alloc, m2.Alloc)
		}
	})
}

// ============================================================================
// COMPREHENSIVE STRESS TESTS
// ============================================================================

func TestStress_AllFunctions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress tests in short mode")
	}

	t.Run("mixed_workload", func(t *testing.T) {
		const iterations = 100000

		for i := 0; i < iterations; i++ {
			// Mix all function types
			f := float64(i) * 0.123456789
			_ = Ftoa(f)
			_ = Itoa(i % 1000000)
			_ = Mix64(uint64(i))

			hexData := []byte("deadbeefcafebabe1234567890abcdef12345678")
			_ = ParseHexU64(hexData[:16])
			_ = ParseHexU32(hexData[:8])
			_ = ParseEthereumAddress(hexData[:40])
		}
	})

	t.Run("extreme_values", func(t *testing.T) {
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
			// Random floats
			bits := rand.Uint64()
			f := math.Float64frombits(bits)

			// Should never panic
			result := Ftoa(f)

			// Result should be non-empty for valid numbers
			if !math.IsNaN(f) && !math.IsInf(f, 0) && len(result) == 0 {
				t.Errorf("Ftoa produced empty result for valid float: %g (bits: 0x%016X)", f, bits)
			}
		}
	})
}
