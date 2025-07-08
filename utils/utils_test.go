// utils_test.go — Comprehensive test suite for high-performance utility functions
package utils

import (
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

// ============================================================================
// MEMORY OPERATION TESTS
// ============================================================================

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
// TYPE CONVERSION TESTS
// ============================================================================

func TestB2s(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{"empty", []byte{}, ""},
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

			// Verify zero-copy behavior for non-empty slices
			if len(tt.input) > 0 {
				inputPtr := unsafe.Pointer(&tt.input[0])
				resultPtr := unsafe.Pointer(unsafe.StringData(got))
				if inputPtr != resultPtr {
					t.Error("B2s() should share underlying data")
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
// HEX PARSING TESTS
// ============================================================================

func TestParseHexU32(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  uint32
	}{
		{"zero", []byte("0"), 0},
		{"single", []byte("f"), 15},
		{"0x_prefix", []byte("0xff"), 255},
		{"0X_prefix", []byte("0XFF"), 255},
		{"four", []byte("1234"), 0x1234},
		{"eight", []byte("deadbeef"), 0xdeadbeef},
		{"max32", []byte("ffffffff"), 0xffffffff},
		{"mixed_case", []byte("DeAdBeEf"), 0xdeadbeef},
		{"invalid_stops", []byte("12g34"), 0x12},
		{"empty", []byte(""), 0},
		{"only_0x", []byte("0x"), 0},
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
		{"zero", []byte("0"), 0},
		{"single", []byte("f"), 15},
		{"0x_prefix", []byte("0xff"), 255},
		{"0X_prefix", []byte("0XFF"), 255},
		{"no_prefix", []byte("deadbeef"), 0xdeadbeef},
		{"mixed_case", []byte("DeAdBeEf"), 0xdeadbeef},
		{"sixteen_chars", []byte("0123456789abcdef"), 0x0123456789abcdef},
		{"max64_truncated", []byte("ffffffffffffffff"), 0xffffffffffffffff},
		{"invalid_stops", []byte("12g34"), 0x12},
		{"empty", []byte(""), 0},
		{"only_0x", []byte("0x"), 0},
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
			"full_address_0x_prefix",
			[]byte("0x1234567890abcdefABCDEF1234567890abcdefAB"),
			[20]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0xab},
		},
		{
			"real_ethereum_address",
			[]byte("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
			[20]byte{0xda, 0xc1, 0x7f, 0x95, 0x8d, 0x2e, 0xe5, 0x23, 0xa2, 0x20, 0x62, 0x06, 0x99, 0x45, 0x97, 0xc1, 0x3d, 0x83, 0x1e, 0xc7},
		},
		{
			"zeros_address",
			[]byte("0x0000000000000000000000000000000000000000"),
			[20]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			"max_address",
			[]byte("0xffffffffffffffffffffffffffffffffffffffff"),
			[20]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
		{
			"empty_input",
			[]byte(""),
			[20]byte{},
		},
		{
			"only_0x",
			[]byte("0x"),
			[20]byte{},
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
	hex64 := []byte("deadbeef12345678")
	ethAddr := []byte("0x1234567890abcdefABCDEF1234567890abcdefAB")

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
// EDGE CASE TESTS
// ============================================================================

func TestEdgeCases(t *testing.T) {
	t.Run("nil_safety", func(t *testing.T) {
		// Verify functions handle nil/empty inputs gracefully
		_ = B2s(nil)
		_ = ParseHexU32(nil)
		_ = ParseHexU64(nil)
		_ = ParseEthereumAddress(nil)
		PrintInfo("")
		PrintWarning("")
	})

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

func BenchmarkHexParsing(b *testing.B) {
	hex32 := []byte("deadbeef")
	hex64 := []byte("deadbeef12345678")
	ethAddr := []byte("0x1234567890abcdefABCDEF1234567890abcdefAB")

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

func BenchmarkZeroAlloc(b *testing.B) {
	data := make([]byte, 1024)
	hex := []byte("deadbeef")
	ethAddr := []byte("0x1234567890abcdefABCDEF1234567890abcdefAB")

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
