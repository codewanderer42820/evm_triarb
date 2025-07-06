package utils

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// generateRandomBytes creates a byte slice of specified length with random data
func generateRandomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// generateHexString creates a valid hex string of specified length
func generateHexString(n int) string {
	chars := "0123456789abcdef"
	result := make([]byte, n)
	for i := 0; i < n; i++ {
		result[i] = chars[i%len(chars)]
	}
	return string(result)
}

// generateJSONWithQuotes creates JSON-like data with embedded quotes at specific positions
func generateJSONWithQuotes(size int, quotePositions []int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = 'a' // Fill with 'a'
	}
	for _, pos := range quotePositions {
		if pos < size {
			data[pos] = '"'
		}
	}
	return data
}

// ============================================================================
// ZERO-ALLOCATION TYPE CONVERSION TESTS
// ============================================================================

func TestB2s(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "Empty slice",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "Single character",
			input:    []byte{'a'},
			expected: "a",
		},
		{
			name:     "ASCII string",
			input:    []byte("hello world"),
			expected: "hello world",
		},
		{
			name:     "UTF-8 string",
			input:    []byte("héllo wørld"),
			expected: "héllo wørld",
		},
		{
			name:     "Binary data",
			input:    []byte{0x00, 0x01, 0x02, 0x03, 0xFF},
			expected: string([]byte{0x00, 0x01, 0x02, 0x03, 0xFF}),
		},
		{
			name:     "Large string",
			input:    []byte(strings.Repeat("abcdefghij", 1000)),
			expected: strings.Repeat("abcdefghij", 1000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := B2s(tt.input)
			if result != tt.expected {
				t.Errorf("B2s() = %q, expected %q", result, tt.expected)
			}

			// Verify zero allocation behavior
			if len(tt.input) > 0 {
				// Check that the underlying data is shared
				inputPtr := unsafe.Pointer(&tt.input[0])
				resultPtr := unsafe.Pointer(unsafe.StringData(result))
				if inputPtr != resultPtr {
					t.Error("B2s() should share underlying data with input slice")
				}
			}
		})
	}
}

func TestB2s_ZeroAllocation(t *testing.T) {
	input := []byte("test string for allocation testing")

	allocsBefore := testing.AllocsPerRun(1000, func() {
		_ = B2s(input)
	})

	if allocsBefore > 0 {
		t.Errorf("B2s() allocated memory: %f allocs/op", allocsBefore)
	}
}

func TestItoa(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected string
	}{
		{
			name:     "Zero",
			input:    0,
			expected: "0",
		},
		{
			name:     "Single digit",
			input:    5,
			expected: "5",
		},
		{
			name:     "Two digits",
			input:    42,
			expected: "42",
		},
		{
			name:     "Three digits",
			input:    123,
			expected: "123",
		},
		{
			name:     "Large number",
			input:    987654321,
			expected: "987654321",
		},
		{
			name:     "Maximum int32",
			input:    2147483647,
			expected: "2147483647",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Itoa(tt.input)
			if result != tt.expected {
				t.Errorf("Itoa(%d) = %q, expected %q", tt.input, result, tt.expected)
			}

			// Cross-verify with standard library
			stdResult := strconv.Itoa(tt.input)
			if result != stdResult {
				t.Errorf("Itoa(%d) = %q, strconv.Itoa = %q", tt.input, result, stdResult)
			}
		})
	}
}

func TestItoa_ZeroAllocation(t *testing.T) {
	allocsBefore := testing.AllocsPerRun(1000, func() {
		_ = Itoa(12345)
	})

	if allocsBefore > 1 { // Allow one allocation for string creation
		t.Errorf("Itoa() should minimize allocations: %f allocs/op", allocsBefore)
	}
}

func TestItoa_EdgeCases(t *testing.T) {
	// Test boundary conditions
	testCases := []int{1, 9, 10, 99, 100, 999, 1000, 9999, 10000}

	for _, n := range testCases {
		t.Run(fmt.Sprintf("boundary_%d", n), func(t *testing.T) {
			result := Itoa(n)
			expected := strconv.Itoa(n)
			if result != expected {
				t.Errorf("Itoa(%d) = %q, expected %q", n, result, expected)
			}
		})
	}
}

func TestPrintWarning(t *testing.T) {
	// Note: This test doesn't capture stderr output but verifies the function doesn't panic
	testCases := []string{
		"",
		"Warning: test message",
		"Very long warning message that should still work without allocation issues",
		"Message with unicode: 测试警告消息",
		strings.Repeat("Long message ", 100),
	}

	for _, msg := range testCases {
		t.Run(fmt.Sprintf("message_len_%d", len(msg)), func(t *testing.T) {
			// Should not panic
			PrintWarning(msg)
		})
	}
}

func TestPrintWarning_ZeroAllocation(t *testing.T) {
	msg := "Test warning message"

	allocsBefore := testing.AllocsPerRun(100, func() {
		PrintWarning(msg)
	})

	if allocsBefore > 0 {
		t.Errorf("PrintWarning() allocated memory: %f allocs/op", allocsBefore)
	}
}

// ============================================================================
// JSON PARSING UTILITIES TESTS
// ============================================================================

func TestSkipToQuoteEarlyExit(t *testing.T) {
	tests := []struct {
		name              string
		data              []byte
		startIdx          int
		hopSize           int
		maxHops           int
		expectedIdx       int
		expectedEarlyExit bool
	}{
		{
			name:              "Quote found within hop limit",
			data:              []byte(`{"field":"value"}`),
			startIdx:          2,
			hopSize:           1,
			maxHops:           10,
			expectedIdx:       7,
			expectedEarlyExit: false,
		},
		{
			name:              "Early exit due to hop limit",
			data:              []byte(`{"very_long_field_name_without_quotes_for_testing"}`),
			startIdx:          2,
			hopSize:           1,
			maxHops:           5,
			expectedIdx:       7, // startIdx + (maxHops * hopSize)
			expectedEarlyExit: true,
		},
		{
			name:              "Quote not found",
			data:              []byte(`{field:value}`),
			startIdx:          2,
			hopSize:           1,
			maxHops:           20,
			expectedIdx:       -1,
			expectedEarlyExit: false,
		},
		{
			name:              "Large hop size",
			data:              []byte(`{"a":"b","c":"d","e":"f"}`),
			startIdx:          0,
			hopSize:           5,
			maxHops:           10,
			expectedIdx:       5,
			expectedEarlyExit: false,
		},
		{
			name:              "Start at quote",
			data:              []byte(`"hello"`),
			startIdx:          0,
			hopSize:           1,
			maxHops:           10,
			expectedIdx:       0,
			expectedEarlyExit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, earlyExit := SkipToQuoteEarlyExit(tt.data, tt.startIdx, tt.hopSize, tt.maxHops)

			if idx != tt.expectedIdx {
				t.Errorf("SkipToQuoteEarlyExit() idx = %d, expected %d", idx, tt.expectedIdx)
			}
			if earlyExit != tt.expectedEarlyExit {
				t.Errorf("SkipToQuoteEarlyExit() earlyExit = %v, expected %v", earlyExit, tt.expectedEarlyExit)
			}
		})
	}
}

func TestSkipToClosingBracketEarlyExit(t *testing.T) {
	tests := []struct {
		name              string
		data              []byte
		startIdx          int
		hopSize           int
		maxHops           int
		expectedIdx       int
		expectedEarlyExit bool
	}{
		{
			name:              "Bracket found within hop limit",
			data:              []byte(`["item1","item2"]`),
			startIdx:          1,
			hopSize:           1,
			maxHops:           20,
			expectedIdx:       16,
			expectedEarlyExit: false,
		},
		{
			name:              "Early exit due to hop limit",
			data:              []byte(`["very","long","array","with","many","elements"]`),
			startIdx:          1,
			hopSize:           1,
			maxHops:           5,
			expectedIdx:       6,
			expectedEarlyExit: true,
		},
		{
			name:              "Bracket not found",
			data:              []byte(`["incomplete","array"`),
			startIdx:          1,
			hopSize:           1,
			maxHops:           30,
			expectedIdx:       -1,
			expectedEarlyExit: false,
		},
		{
			name:              "Large hop size",
			data:              []byte(`[1,2,3,4,5,6,7,8,9]`),
			startIdx:          0,
			hopSize:           3,
			maxHops:           10,
			expectedIdx:       18,
			expectedEarlyExit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, earlyExit := SkipToClosingBracketEarlyExit(tt.data, tt.startIdx, tt.hopSize, tt.maxHops)

			if idx != tt.expectedIdx {
				t.Errorf("SkipToClosingBracketEarlyExit() idx = %d, expected %d", idx, tt.expectedIdx)
			}
			if earlyExit != tt.expectedEarlyExit {
				t.Errorf("SkipToClosingBracketEarlyExit() earlyExit = %v, expected %v", earlyExit, tt.expectedEarlyExit)
			}
		})
	}
}

func TestSkipToQuote(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		startIdx    int
		hopSize     int
		expectedIdx int
	}{
		{
			name:        "Quote found",
			data:        []byte(`{"field":"value"}`),
			startIdx:    2,
			hopSize:     1,
			expectedIdx: 7,
		},
		{
			name:        "Quote not found",
			data:        []byte(`{field:value}`),
			startIdx:    2,
			hopSize:     1,
			expectedIdx: -1,
		},
		{
			name:        "Multiple quotes",
			data:        []byte(`"first" "second"`),
			startIdx:    1,
			hopSize:     1,
			expectedIdx: 6,
		},
		{
			name:        "Large hop size",
			data:        []byte(`abcdefghij"klmnop`),
			startIdx:    0,
			hopSize:     5,
			expectedIdx: 10,
		},
		{
			name:        "Start at quote",
			data:        []byte(`"test"`),
			startIdx:    0,
			hopSize:     1,
			expectedIdx: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SkipToQuote(tt.data, tt.startIdx, tt.hopSize)
			if result != tt.expectedIdx {
				t.Errorf("SkipToQuote() = %d, expected %d", result, tt.expectedIdx)
			}
		})
	}
}

func TestSkipToOpeningBracket(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		startIdx    int
		hopSize     int
		expectedIdx int
	}{
		{
			name:        "Bracket found",
			data:        []byte(`{"array":[1,2,3]}`),
			startIdx:    0,
			hopSize:     1,
			expectedIdx: 9,
		},
		{
			name:        "Bracket not found",
			data:        []byte(`{"field":"value"}`),
			startIdx:    0,
			hopSize:     1,
			expectedIdx: -1,
		},
		{
			name:        "Multiple brackets",
			data:        []byte(`[1,2][3,4]`),
			startIdx:    1,
			hopSize:     1,
			expectedIdx: 5,
		},
		{
			name:        "Large hop size",
			data:        []byte(`abcdefghij[klmnop`),
			startIdx:    0,
			hopSize:     5,
			expectedIdx: 10,
		},
		{
			name:        "Start at bracket",
			data:        []byte(`[test]`),
			startIdx:    0,
			hopSize:     1,
			expectedIdx: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SkipToOpeningBracket(tt.data, tt.startIdx, tt.hopSize)
			if result != tt.expectedIdx {
				t.Errorf("SkipToOpeningBracket() = %d, expected %d", result, tt.expectedIdx)
			}
		})
	}
}

func TestSkipToClosingBracket(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		startIdx    int
		hopSize     int
		expectedIdx int
	}{
		{
			name:        "Bracket found",
			data:        []byte(`[1,2,3,4,5]`),
			startIdx:    1,
			hopSize:     1,
			expectedIdx: 10,
		},
		{
			name:        "Bracket not found",
			data:        []byte(`[1,2,3,4,5`),
			startIdx:    1,
			hopSize:     1,
			expectedIdx: -1,
		},
		{
			name:        "Nested brackets",
			data:        []byte(`[[1,2],3]`),
			startIdx:    1,
			hopSize:     1,
			expectedIdx: 5,
		},
		{
			name:        "Large hop size",
			data:        []byte(`abcdefghij]klmnop`),
			startIdx:    0,
			hopSize:     5,
			expectedIdx: 10,
		},
		{
			name:        "Start at bracket",
			data:        []byte(`]test`),
			startIdx:    0,
			hopSize:     1,
			expectedIdx: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SkipToClosingBracket(tt.data, tt.startIdx, tt.hopSize)
			if result != tt.expectedIdx {
				t.Errorf("SkipToClosingBracket() = %d, expected %d", result, tt.expectedIdx)
			}
		})
	}
}

func TestJSONParsing_StressTest(t *testing.T) {
	// Test with very large JSON-like data
	size := 100000
	data := make([]byte, size)
	for i := range data {
		data[i] = 'a'
	}
	// Add quotes at specific positions
	data[size/4] = '"'
	data[size/2] = '"'
	data[size*3/4] = '"'

	t.Run("Large data quote search", func(t *testing.T) {
		result := SkipToQuote(data, 0, 1)
		if result != size/4 {
			t.Errorf("Expected quote at %d, got %d", size/4, result)
		}
	})

	t.Run("Large data with hop size", func(t *testing.T) {
		result := SkipToQuote(data, 0, 100)
		expected := (size / 4 / 100) * 100 // Should find quote at aligned position
		if result != expected {
			t.Errorf("Expected quote at %d, got %d", expected, result)
		}
	})
}

// ============================================================================
// UNALIGNED MEMORY OPERATIONS TESTS
// ============================================================================

func TestLoad64(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected uint64
	}{
		{
			name:     "All zeros",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expected: 0x0000000000000000,
		},
		{
			name:     "All ones",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			expected: 0xFFFFFFFFFFFFFFFF,
		},
		{
			name:     "Sequential bytes",
			input:    []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			expected: 0x0807060504030201, // Little-endian
		},
		{
			name:     "Mixed pattern",
			input:    []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22},
			expected: 0x2211FFEEDDCCBBAA, // Little-endian
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Load64(tt.input)
			if result != tt.expected {
				t.Errorf("Load64() = 0x%016X, expected 0x%016X", result, tt.expected)
			}
		})
	}
}

func TestLoad128(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		expected1 uint64
		expected2 uint64
	}{
		{
			name:      "All zeros",
			input:     make([]byte, 16),
			expected1: 0x0000000000000000,
			expected2: 0x0000000000000000,
		},
		{
			name:      "Sequential bytes",
			input:     []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10},
			expected1: 0x0807060504030201,
			expected2: 0x100F0E0D0C0B0A09,
		},
		{
			name:      "Mixed pattern",
			input:     []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAB},
			expected1: 0x2211FFEEDDCCBBAA,
			expected2: 0xAB99887766554433,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result1, result2 := Load128(tt.input)
			if result1 != tt.expected1 {
				t.Errorf("Load128() first = 0x%016X, expected 0x%016X", result1, tt.expected1)
			}
			if result2 != tt.expected2 {
				t.Errorf("Load128() second = 0x%016X, expected 0x%016X", result2, tt.expected2)
			}
		})
	}
}

func TestLoadBE64(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected uint64
	}{
		{
			name:     "All zeros",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expected: 0x0000000000000000,
		},
		{
			name:     "All ones",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			expected: 0xFFFFFFFFFFFFFFFF,
		},
		{
			name:     "Sequential bytes",
			input:    []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			expected: 0x0102030405060708, // Big-endian
		},
		{
			name:     "Mixed pattern",
			input:    []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22},
			expected: 0xAABBCCDDEEFF1122, // Big-endian
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := LoadBE64(tt.input)
			if result != tt.expected {
				t.Errorf("LoadBE64() = 0x%016X, expected 0x%016X", result, tt.expected)
			}
		})
	}
}

func TestMemoryOperations_Alignment(t *testing.T) {
	// Test with unaligned memory access
	data := make([]byte, 32)
	for i := range data {
		data[i] = byte(i)
	}

	// Test Load64 at various offsets
	for offset := 0; offset < 8; offset++ {
		t.Run(fmt.Sprintf("Load64_offset_%d", offset), func(t *testing.T) {
			if offset+8 <= len(data) {
				result := Load64(data[offset:])
				// Verify it doesn't panic and returns consistent results
				if result == 0 && offset > 0 {
					t.Error("Load64 should handle unaligned access")
				}
			}
		})
	}

	// Test Load128 at various offsets
	for offset := 0; offset < 8; offset++ {
		t.Run(fmt.Sprintf("Load128_offset_%d", offset), func(t *testing.T) {
			if offset+16 <= len(data) {
				r1, r2 := Load128(data[offset:])
				// Verify it doesn't panic
				if r1 == 0 && r2 == 0 && offset > 0 {
					t.Error("Load128 should handle unaligned access")
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

	t.Run("Load64", func(t *testing.T) {
		allocs := testing.AllocsPerRun(1000, func() {
			_ = Load64(data)
		})
		if allocs > 0 {
			t.Errorf("Load64() allocated memory: %f allocs/op", allocs)
		}
	})

	t.Run("Load128", func(t *testing.T) {
		allocs := testing.AllocsPerRun(1000, func() {
			_, _ = Load128(data)
		})
		if allocs > 0 {
			t.Errorf("Load128() allocated memory: %f allocs/op", allocs)
		}
	})

	t.Run("LoadBE64", func(t *testing.T) {
		allocs := testing.AllocsPerRun(1000, func() {
			_ = LoadBE64(data)
		})
		if allocs > 0 {
			t.Errorf("LoadBE64() allocated memory: %f allocs/op", allocs)
		}
	})
}

// ============================================================================
// HEX PARSING TESTS
// ============================================================================

func TestParseHexU64(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected uint64
	}{
		{
			name:     "Zero",
			input:    []byte("0"),
			expected: 0,
		},
		{
			name:     "Single digit",
			input:    []byte("f"),
			expected: 15,
		},
		{
			name:     "0x prefix",
			input:    []byte("0xff"),
			expected: 255,
		},
		{
			name:     "0X prefix uppercase",
			input:    []byte("0XFF"),
			expected: 255,
		},
		{
			name:     "No prefix",
			input:    []byte("deadbeef"),
			expected: 0xdeadbeef,
		},
		{
			name:     "Mixed case",
			input:    []byte("DeAdBeEf"),
			expected: 0xdeadbeef,
		},
		{
			name:     "Maximum 64-bit",
			input:    []byte("ffffffffffffffff"),
			expected: 0xffffffffffffffff,
		},
		{
			name:     "With 0x prefix maximum",
			input:    []byte("0xffffffffffffffff"),
			expected: 0xffffffffffffffff,
		},
		{
			name:     "Invalid characters stop parsing",
			input:    []byte("12g34"),
			expected: 0x12,
		},
		{
			name:     "Empty string",
			input:    []byte(""),
			expected: 0,
		},
		{
			name:     "Only 0x",
			input:    []byte("0x"),
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseHexU64(tt.input)
			if result != tt.expected {
				t.Errorf("ParseHexU64(%q) = 0x%X, expected 0x%X", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseHexN(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected uint64
	}{
		{
			name:     "Single hex digit",
			input:    []byte("a"),
			expected: 0xa,
		},
		{
			name:     "Four hex digits",
			input:    []byte("1234"),
			expected: 0x1234,
		},
		{
			name:     "Eight hex digits",
			input:    []byte("deadbeef"),
			expected: 0xdeadbeef,
		},
		{
			name:     "Sixteen hex digits",
			input:    []byte("0123456789abcdef"),
			expected: 0x0123456789abcdef,
		},
		{
			name:     "Mixed case",
			input:    []byte("AbCdEf"),
			expected: 0xabcdef,
		},
		{
			name:     "All zeros",
			input:    []byte("0000"),
			expected: 0x0000,
		},
		{
			name:     "All F's",
			input:    []byte("ffff"),
			expected: 0xffff,
		},
		{
			name:     "Empty input",
			input:    []byte(""),
			expected: 0,
		},
		{
			name:     "Invalid characters ignored",
			input:    []byte("12g34"),
			expected: 0x12034, // 'g' becomes 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseHexN(tt.input)
			if result != tt.expected {
				t.Errorf("ParseHexN(%q) = 0x%X, expected 0x%X", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseHexU32(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected uint32
	}{
		{
			name:     "Four hex digits",
			input:    []byte("1234"),
			expected: 0x1234,
		},
		{
			name:     "Eight hex digits",
			input:    []byte("deadbeef"),
			expected: 0xdeadbeef,
		},
		{
			name:     "Maximum uint32",
			input:    []byte("ffffffff"),
			expected: 0xffffffff,
		},
		{
			name:     "Zero",
			input:    []byte("0"),
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseHexU32(tt.input)
			if result != tt.expected {
				t.Errorf("ParseHexU32(%q) = 0x%X, expected 0x%X", tt.input, result, tt.expected)
			}
		})
	}
}

func TestHexParsing_Performance(t *testing.T) {
	// Test with various hex string sizes
	sizes := []int{2, 4, 8, 16}

	for _, size := range sizes {
		hexStr := generateHexString(size)
		hexBytes := []byte(hexStr)

		t.Run(fmt.Sprintf("ParseHexN_size_%d", size), func(t *testing.T) {
			allocs := testing.AllocsPerRun(1000, func() {
				_ = ParseHexN(hexBytes)
			})
			if allocs > 0 {
				t.Errorf("ParseHexN() allocated memory: %f allocs/op", allocs)
			}
		})

		t.Run(fmt.Sprintf("ParseHexU64_size_%d", size), func(t *testing.T) {
			allocs := testing.AllocsPerRun(1000, func() {
				_ = ParseHexU64(hexBytes)
			})
			if allocs > 0 {
				t.Errorf("ParseHexU64() allocated memory: %f allocs/op", allocs)
			}
		})
	}
}

func TestHexParsing_EdgeCases(t *testing.T) {
	edgeCases := []struct {
		name  string
		input []byte
	}{
		{"Very long hex", []byte("0123456789abcdef0123456789abcdef0123456789abcdef")},
		{"Mixed valid/invalid", []byte("12zz34")},
		{"Only prefix", []byte("0x")},
		{"Space in hex", []byte("12 34")},
		{"Newline in hex", []byte("12\n34")},
		{"Unicode in hex", []byte("12测试34")},
	}

	for _, tc := range edgeCases {
		t.Run(tc.name, func(t *testing.T) {
			// Should not panic
			_ = ParseHexU64(tc.input)
			_ = ParseHexN(tc.input)
			_ = ParseHexU32(tc.input)
		})
	}
}

// ============================================================================
// HASHING TESTS
// ============================================================================

func TestMix64(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected uint64
	}{
		{
			name:     "Zero",
			input:    0,
			expected: 0,
		},
		{
			name:     "One",
			input:    1,
			expected: Mix64(1), // We can't predict the exact output, but it should be deterministic
		},
		{
			name:     "Maximum uint64",
			input:    0xffffffffffffffff,
			expected: Mix64(0xffffffffffffffff),
		},
		{
			name:     "Common value",
			input:    0x123456789abcdef0,
			expected: Mix64(0x123456789abcdef0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Mix64(tt.input)

			// Test deterministic behavior
			result2 := Mix64(tt.input)
			if result != result2 {
				t.Errorf("Mix64() not deterministic: first=%X, second=%X", result, result2)
			}

			// Test that different inputs produce different outputs (except for edge cases)
			if tt.input != 0 {
				differentResult := Mix64(tt.input + 1)
				if result == differentResult {
					t.Errorf("Mix64() collision: Mix64(%X) == Mix64(%X)", tt.input, tt.input+1)
				}
			}
		})
	}
}

func TestMix64_Properties(t *testing.T) {
	// Test hash function properties
	t.Run("Deterministic", func(t *testing.T) {
		input := uint64(0x123456789abcdef0)
		result1 := Mix64(input)
		result2 := Mix64(input)
		if result1 != result2 {
			t.Error("Mix64() should be deterministic")
		}
	})

	t.Run("Distribution", func(t *testing.T) {
		// Test that Mix64 produces well-distributed output
		buckets := make([]int, 256)
		for i := uint64(0); i < 10000; i++ {
			hash := Mix64(i)
			bucket := hash & 255 // Use lower 8 bits
			buckets[bucket]++
		}

		// Check that no bucket is too empty or too full
		expected := 10000 / 256
		tolerance := expected / 2 // 50% tolerance - more lenient for hash distribution
		for i, count := range buckets {
			if count < expected-tolerance || count > expected+tolerance {
				t.Errorf("Bucket %d has %d items, expected ~%d (tolerance: %d)", i, count, expected, tolerance)
			}
		}
	})

	t.Run("Avalanche", func(t *testing.T) {
		// Test avalanche effect - small input changes should cause large output changes
		input1 := uint64(0x123456789abcdef0)
		input2 := input1 ^ 1 // Flip one bit

		hash1 := Mix64(input1)
		hash2 := Mix64(input2)

		// Count different bits
		diff := hash1 ^ hash2
		bitCount := 0
		for diff != 0 {
			bitCount++
			diff &= diff - 1 // Clear lowest set bit
		}

		// Should have changed roughly half the bits
		if bitCount < 20 || bitCount > 44 {
			t.Errorf("Poor avalanche: only %d bits changed", bitCount)
		}
	})
}

func TestHash17(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected uint32
	}{
		{
			name:     "Short input",
			input:    []byte("123"),
			expected: 0, // Should return 0 for input < 6 bytes
		},
		{
			name:     "Exact 6 bytes",
			input:    []byte("123456"),
			expected: uint32(ParseHexN([]byte("123456")) & ((1 << 17) - 1)),
		},
		{
			name:     "Ethereum address prefix",
			input:    []byte("0x1234567890abcdef"),
			expected: uint32(ParseHexN([]byte("0x1234")) & ((1 << 17) - 1)),
		},
		{
			name:     "Long address",
			input:    []byte("0x1234567890abcdef1234567890abcdef12345678"),
			expected: uint32(ParseHexN([]byte("0x1234")) & ((1 << 17) - 1)),
		},
		{
			name:     "All zeros",
			input:    []byte("000000"),
			expected: 0,
		},
		{
			name:     "All F's",
			input:    []byte("ffffff"),
			expected: (1 << 17) - 1, // Maximum 17-bit value
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Hash17(tt.input)
			if result != tt.expected {
				t.Errorf("Hash17(%q) = %d, expected %d", tt.input, result, tt.expected)
			}

			// Verify result is within 17-bit range
			if result >= (1 << 17) {
				t.Errorf("Hash17(%q) = %d, exceeds 17-bit range", tt.input, result)
			}
		})
	}
}

func TestHash17_Distribution(t *testing.T) {
	// Test hash distribution for Ethereum-like addresses
	buckets := make([]int, 1<<17) // 131072 buckets

	// Generate test addresses
	for i := 0; i < 100000; i++ {
		addr := fmt.Sprintf("%06x", i)
		hash := Hash17([]byte(addr))
		buckets[hash]++
	}

	// Check distribution - should be relatively uniform
	expectedMax := 10 // No bucket should be too full

	overflowCount := 0
	for _, count := range buckets {
		if count > expectedMax {
			overflowCount++
		}
	}

	// Allow some variation but not too much
	if overflowCount > 1000 { // Allow 1% of buckets to overflow
		t.Errorf("Too many overflowing buckets: %d", overflowCount)
	}
}

func TestHashing_ZeroAllocation(t *testing.T) {
	t.Run("Mix64", func(t *testing.T) {
		allocs := testing.AllocsPerRun(1000, func() {
			_ = Mix64(0x123456789abcdef0)
		})
		if allocs > 0 {
			t.Errorf("Mix64() allocated memory: %f allocs/op", allocs)
		}
	})

	t.Run("Hash17", func(t *testing.T) {
		addr := []byte("123456789abcdef")
		allocs := testing.AllocsPerRun(1000, func() {
			_ = Hash17(addr)
		})
		if allocs > 0 {
			t.Errorf("Hash17() allocated memory: %f allocs/op", allocs)
		}
	})
}

// ============================================================================
// STRESS TESTS
// ============================================================================

func TestStressScenarios(t *testing.T) {
	t.Run("Large JSON processing", func(t *testing.T) {
		size := 1000000 // 1MB JSON-like data
		data := generateJSONWithQuotes(size, []int{size / 4, size / 2, size * 3 / 4})

		start := time.Now()
		result := SkipToQuote(data, 0, 1)
		duration := time.Since(start)

		if result != size/4 {
			t.Errorf("Expected quote at %d, got %d", size/4, result)
		}
		if duration > time.Millisecond*100 {
			t.Errorf("Processing took too long: %v", duration)
		}
	})

	t.Run("Massive hex parsing", func(t *testing.T) {
		// Test parsing many hex values
		hexValues := make([][]byte, 10000)
		for i := range hexValues {
			hexValues[i] = []byte(fmt.Sprintf("%016x", uint64(i)))
		}

		start := time.Now()
		for _, hex := range hexValues {
			_ = ParseHexU64(hex)
		}
		duration := time.Since(start)

		if duration > time.Millisecond*100 {
			t.Errorf("Hex parsing took too long: %v", duration)
		}
	})

	t.Run("Hash collision test", func(t *testing.T) {
		// Test for hash collisions in realistic scenarios
		seen := make(map[uint64]bool)
		collisions := 0

		for i := uint64(0); i < 100000; i++ {
			hash := Mix64(i)
			if seen[hash] {
				collisions++
			}
			seen[hash] = true
		}

		// Should have very few collisions
		if collisions > 10 {
			t.Errorf("Too many hash collisions: %d", collisions)
		}
	})
}

// ============================================================================
// BENCHMARK TESTS
// ============================================================================

func BenchmarkB2s(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		data := generateRandomBytes(size)
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = B2s(data)
			}
		})
	}
}

func BenchmarkItoa(b *testing.B) {
	values := []int{0, 1, 42, 123, 9999, 123456, 987654321}

	for _, val := range values {
		b.Run(fmt.Sprintf("value_%d", val), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = Itoa(val)
			}
		})
	}
}

func BenchmarkJSONParsing(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	hopSizes := []int{1, 4, 16}

	for _, size := range sizes {
		for _, hopSize := range hopSizes {
			data := generateJSONWithQuotes(size, []int{size / 2})

			b.Run(fmt.Sprintf("SkipToQuote_size_%d_hop_%d", size, hopSize), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					_ = SkipToQuote(data, 0, hopSize)
				}
			})
		}
	}
}

func BenchmarkMemoryOperations(b *testing.B) {
	data := generateRandomBytes(32)

	b.Run("Load64", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = Load64(data)
		}
	})

	b.Run("Load128", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = Load128(data)
		}
	})

	b.Run("LoadBE64", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = LoadBE64(data)
		}
	})
}

func BenchmarkHexParsing(b *testing.B) {
	hexStrings := [][]byte{
		[]byte("ff"),
		[]byte("deadbeef"),
		[]byte("0x123456789abcdef"),
		[]byte("ffffffffffffffff"),
	}

	for _, hex := range hexStrings {
		b.Run(fmt.Sprintf("ParseHexU64_len_%d", len(hex)), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = ParseHexU64(hex)
			}
		})

		b.Run(fmt.Sprintf("ParseHexN_len_%d", len(hex)), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = ParseHexN(hex)
			}
		})
	}
}

func BenchmarkHashing(b *testing.B) {
	values := []uint64{
		0,
		1,
		0xdeadbeef,
		0x123456789abcdef0,
		0xffffffffffffffff,
	}

	for _, val := range values {
		b.Run(fmt.Sprintf("Mix64_0x%x", val), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = Mix64(val)
			}
		})
	}

	addresses := [][]byte{
		[]byte("123456"),
		[]byte("deadbeef123456"),
		[]byte("0x123456789abcdef"),
	}

	for _, addr := range addresses {
		b.Run(fmt.Sprintf("Hash17_len_%d", len(addr)), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = Hash17(addr)
			}
		})
	}
}

func BenchmarkComparison(b *testing.B) {
	// Compare with standard library functions where applicable
	testInt := 123456
	testBytes := []byte("hello world")

	b.Run("Itoa_vs_strconv", func(b *testing.B) {
		b.Run("custom", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = Itoa(testInt)
			}
		})

		b.Run("stdlib", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = strconv.Itoa(testInt)
			}
		})
	})

	b.Run("B2s_vs_string", func(b *testing.B) {
		b.Run("custom", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = B2s(testBytes)
			}
		})

		b.Run("stdlib", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = string(testBytes)
			}
		})
	})
}

// ============================================================================
// MEMORY PRESSURE TESTS
// ============================================================================

func BenchmarkMemoryPressure(b *testing.B) {
	data := generateRandomBytes(1024 * 1024) // 1MB
	runtime.GC()

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	b.ReportAllocs()
	b.SetBytes(1024 * 1024)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = B2s(data)
		_ = Load64(data)
		_ = ParseHexU64(data[:16])
		_ = Mix64(uint64(i))
	}

	b.StopTimer()
	runtime.ReadMemStats(&m2)

	if b.N > 0 {
		allocDelta := m2.TotalAlloc - m1.TotalAlloc
		b.ReportMetric(float64(allocDelta)/float64(b.N), "actual_bytes/op")

		if allocDelta > 0 {
			b.Logf("WARNING: %d bytes allocated over %d iterations (%.2f bytes/op)",
				allocDelta, b.N, float64(allocDelta)/float64(b.N))
		} else {
			b.Logf("PERFECT: Zero allocations confirmed over %d iterations", b.N)
		}
	}
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

func BenchmarkConcurrency(b *testing.B) {
	data := generateRandomBytes(1000)
	hexData := []byte("deadbeef123456")

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = B2s(data)
			_ = Load64(data)
			_ = ParseHexU64(hexData)
			_ = Mix64(uint64(12345))
			_ = Hash17(hexData)
		}
	})
}

// ============================================================================
// EDGE CASE AND SAFETY TESTS
// ============================================================================

func TestEdgeCaseSafety(t *testing.T) {
	t.Run("Nil and empty inputs", func(t *testing.T) {
		// Test with nil slices - should not panic
		_ = B2s(nil)
		_ = Hash17(nil)
		_ = ParseHexU64(nil)
		_ = ParseHexN(nil)
	})

	t.Run("Boundary conditions", func(t *testing.T) {
		// Test exact boundary conditions
		data6 := generateRandomBytes(6)
		data8 := generateRandomBytes(8)
		data16 := generateRandomBytes(16)

		_ = Hash17(data6)
		_ = Load64(data8)
		_, _ = Load128(data16)
		_ = LoadBE64(data8)
	})

	t.Run("Very large inputs", func(t *testing.T) {
		largeData := generateRandomBytes(1000000)

		// Should handle large inputs gracefully
		_ = B2s(largeData)
		_ = SkipToQuote(largeData, 0, 1000)
		_ = Hash17(largeData)
	})

	t.Run("Invalid UTF-8 in B2s", func(t *testing.T) {
		invalidUTF8 := []byte{0xFF, 0xFE, 0xFD}
		result := B2s(invalidUTF8)

		// Should not panic, even with invalid UTF-8
		if len(result) != len(invalidUTF8) {
			t.Error("B2s should preserve byte length even with invalid UTF-8")
		}
	})
}

func TestDataIntegrity(t *testing.T) {
	t.Run("B2s data sharing", func(t *testing.T) {
		original := []byte("test data")
		str := B2s(original)

		// Verify the string shares data with original slice
		if len(str) > 0 {
			strData := unsafe.StringData(str)
			sliceData := &original[0]
			if strData != sliceData {
				t.Error("B2s should share data with input slice")
			}
		}
	})

	t.Run("Memory operation consistency", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}

		// Load64 and LoadBE64 should produce different results for same data
		le := Load64(data)
		be := LoadBE64(data)

		if le == be && le != 0 {
			t.Error("Load64 and LoadBE64 should produce different results for non-zero data")
		}

		// Test Load128 properly
		_, _ = Load128(data)
	})

	t.Run("Hash consistency", func(t *testing.T) {
		// Same input should always produce same hash
		input := uint64(0x123456789abcdef0)
		hash1 := Mix64(input)
		hash2 := Mix64(input)

		if hash1 != hash2 {
			t.Error("Mix64 should be deterministic")
		}

		// Different inputs should (usually) produce different hashes
		hash3 := Mix64(input + 1)
		if hash1 == hash3 {
			t.Error("Mix64 should produce different outputs for different inputs")
		}
	})
}
