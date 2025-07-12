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

// ==============================================================================
// TEST HELPERS
// ==============================================================================

// assertZeroAllocs verifies that a function allocates zero memory
func assertZeroAllocs(t *testing.T, name string, fn func()) {
	t.Helper()
	allocs := testing.AllocsPerRun(100, fn)
	if allocs > 0 {
		t.Errorf("%s allocated: %f allocs/op", name, allocs)
	}
}

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

// ==============================================================================
// MEMORY OPERATION TESTS
// ==============================================================================

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

func TestMemoryOperations_ZeroAllocation(t *testing.T) {
	data := make([]byte, 16)
	for i := range data {
		data[i] = byte(i)
	}

	assertZeroAllocs(t, "Load64", func() {
		_ = Load64(data)
	})

	assertZeroAllocs(t, "LoadBE64", func() {
		_ = LoadBE64(data)
	})

	assertZeroAllocs(t, "Load128", func() {
		_, _ = Load128(data)
	})
}

// ==============================================================================
// TYPE CONVERSION TESTS
// ==============================================================================

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

func TestTypeConversion_ZeroAllocation(t *testing.T) {
	testBytes := []byte("test string for zero allocation")
	testInt := 12345

	assertZeroAllocs(t, "B2s", func() {
		_ = B2s(testBytes)
	})

	assertZeroAllocs(t, "Itoa", func() {
		_ = Itoa(testInt)
	})
}

// ==============================================================================
// HEX PARSING TESTS
// ==============================================================================

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

func TestHexParsing_ZeroAllocation(t *testing.T) {
	hex32 := []byte("deadbeef")
	hex64 := []byte("deadbeefcafebabe")
	ethAddr := []byte("dAC17F958D2ee523a2206206994597C13D831ec7")

	assertZeroAllocs(t, "ParseHexU32", func() {
		_ = ParseHexU32(hex32)
	})

	assertZeroAllocs(t, "ParseHexU64", func() {
		_ = ParseHexU64(hex64)
	})

	assertZeroAllocs(t, "ParseEthereumAddress", func() {
		_ = ParseEthereumAddress(ethAddr)
	})
}

// ==============================================================================
// JSON PARSING TESTS
// ==============================================================================

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

func TestJSONParsing_ZeroAllocation(t *testing.T) {
	data := []byte(`{"key":"value","array":[1,2,3],"nested":{"a":"b"}}`)

	assertZeroAllocs(t, "SkipToQuote", func() {
		_ = SkipToQuote(data, 0, 1)
	})

	assertZeroAllocs(t, "SkipToQuoteEarlyExit", func() {
		_, _ = SkipToQuoteEarlyExit(data, 0, 1, 10)
	})

	assertZeroAllocs(t, "SkipToOpeningBracket", func() {
		_ = SkipToOpeningBracket(data, 0, 1)
	})

	assertZeroAllocs(t, "SkipToClosingBracket", func() {
		_ = SkipToClosingBracket(data, 0, 1)
	})
}

// ==============================================================================
// HASH MIXING TESTS
// ==============================================================================

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

func TestMix64_ZeroAllocation(t *testing.T) {
	inputs := []uint64{0, 1, 0xDEADBEEF, 0xFFFFFFFFFFFFFFFF}

	for _, input := range inputs {
		assertZeroAllocs(t, fmt.Sprintf("Mix64(0x%X)", input), func() {
			_ = Mix64(input)
		})
	}
}

// ==============================================================================
// SYSTEM I/O TESTS (Simplified)
// ==============================================================================

func TestPrintFunctions(t *testing.T) {
	// Basic test that functions don't panic
	PrintInfo("test info")
	PrintWarning("test warning")
}

// ==============================================================================
// RACE CONDITION TESTS
// ==============================================================================

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
}

// ==============================================================================
// EDGE CASES AND BOUNDARY TESTS
// ==============================================================================

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
}

// ==============================================================================
// INTENTIONAL LIMITATIONS DOCUMENTATION
// ==============================================================================

func TestIntentionalLimitations(t *testing.T) {
	t.Run("B2s_limitations", func(t *testing.T) {
		// B2s panics on empty/nil slices - this is intentional
		t.Log("B2s requires non-empty slice - will panic on empty/nil")
	})

	t.Run("Itoa_limitations", func(t *testing.T) {
		// Itoa only supports positive integers
		t.Log("Itoa only supports positive integers [0, MaxInt]")
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

// ==============================================================================
// PERFORMANCE BENCHMARKS
// ==============================================================================

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
			_ = Mix64(inputs[i&3])
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
}

// ==============================================================================
// INTEGRATION TESTS
// ==============================================================================

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
}

// ==============================================================================
// STATISTICAL TESTS
// ==============================================================================

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

// ==============================================================================
// CLEANUP AND FINAL VERIFICATION
// ==============================================================================

func TestCleanup(t *testing.T) {
	t.Run("no_goroutine_leaks", func(t *testing.T) {
		before := runtime.NumGoroutine()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = Mix64(42)
			}()
		}
		wg.Wait()

		time.Sleep(10 * time.Millisecond)
		after := runtime.NumGoroutine()
		if after > before {
			t.Logf("Possible goroutine leak: before=%d, after=%d", before, after)
		}
	})
}
