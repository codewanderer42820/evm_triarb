// utils.go — High-performance utilities for zero-allocation operations
package utils

import (
	"syscall"
	"unsafe"
)

// ============================================================================
// MEMORY OPERATIONS
// ============================================================================

// Load64 loads 8 bytes as uint64 from unaligned memory
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Load64(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

// Load128 loads 16 bytes as two uint64s
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Load128(b []byte) (uint64, uint64) {
	p := (*[2]uint64)(unsafe.Pointer(&b[0]))
	return p[0], p[1]
}

// LoadBE64 loads 8 bytes as big-endian uint64
// Note: Go compiler recognizes this pattern and compiles to single BSWAP instruction on ARM64/x86
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LoadBE64(b []byte) uint64 {
	_ = b[7] // Bounds check hint
	return uint64(b[0])<<56 | uint64(b[1])<<48 |
		uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 |
		uint64(b[6])<<8 | uint64(b[7])
}

// ============================================================================
// TYPE CONVERSION UTILITIES
// ============================================================================

// B2s converts byte slice to string without allocation
// ⚠️  WARNING: Input slice must not be modified after conversion
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func B2s(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// Itoa converts integer to string without heap allocation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Itoa(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [10]byte // Max 10 digits for 32-bit int
	i := len(buf)

	// Convert digits in reverse order
	for n > 0 {
		i--
		buf[i] = byte(n%10 + '0')
		n /= 10
	}

	return string(buf[i:])
}

// ============================================================================
// HEX PARSING UTILITIES
// ============================================================================

// ParseHexU32 parses hex to uint32 with branchless optimization
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU32(b []byte) uint32 {
	if len(b) == 0 {
		return 0
	}

	j := 0
	// Skip 0x prefix
	if len(b) >= 2 && b[0] == '0' && (b[1]|0x20) == 'x' {
		j = 2
	}

	var result uint32

	// Handle up to 8 hex chars for uint32
	for ; j < len(b) && j < 10; j++ { // 10 = 2 (0x) + 8 (hex chars)
		c := b[j] | 0x20
		v := c - '0' - ((c&0x40)>>6)*39

		if v > 15 {
			break
		}

		result = (result << 4) | uint32(v)
	}

	return result
}

// ParseHexU64 parses hex string to uint64 using SIMD optimization with efficient padding
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU64(b []byte) uint64 {
	// Process up to 16 chars, truncate if longer
	processLen := len(b)
	if processLen > 16 {
		processLen = 16
	}

	// For inputs up to 8 chars, use single SIMD operation
	if processLen <= 8 {
		padded := [8]byte{'0', '0', '0', '0', '0', '0', '0', '0'}
		copy(padded[8-processLen:], b[:processLen])

		chunk := Load64(padded[:])

		// Convert ASCII to nibbles
		chunk |= 0x2020202020202020                            // Force lowercase
		letterMask := (chunk & 0x4040404040404040) >> 6        // Detect letters
		chunk = chunk - 0x3030303030303030 - (letterMask * 39) // Convert to nibbles

		// SIMD nibble compaction
		extracted := chunk & 0x000F000F000F000F
		chunk ^= extracted
		chunk |= extracted << 12

		extracted = chunk & 0xFF000000FF000000
		chunk ^= extracted
		chunk |= extracted >> 24

		extracted = chunk & 0x000000000000FFFF
		chunk ^= extracted
		chunk |= extracted << 48

		return chunk >> 32
	}

	// For inputs 9-16 chars, use two SIMD operations
	var result uint64

	// First 8 chars
	chunk1 := Load64(b[:8])

	// Convert ASCII to nibbles
	chunk1 |= 0x2020202020202020                              // Force lowercase
	letterMask1 := (chunk1 & 0x4040404040404040) >> 6         // Detect letters
	chunk1 = chunk1 - 0x3030303030303030 - (letterMask1 * 39) // Convert to nibbles

	// SIMD nibble compaction
	extracted := chunk1 & 0x000F000F000F000F
	chunk1 ^= extracted
	chunk1 |= extracted << 12

	extracted = chunk1 & 0xFF000000FF000000
	chunk1 ^= extracted
	chunk1 |= extracted >> 24

	extracted = chunk1 & 0x000000000000FFFF
	chunk1 ^= extracted
	chunk1 |= extracted << 48

	result = chunk1 >> 32

	// Second chunk (remaining chars) - efficient padding
	remaining2 := processLen - 8
	padded := [8]byte{'0', '0', '0', '0', '0', '0', '0', '0'}
	copy(padded[8-remaining2:], b[8:8+remaining2])

	chunk2 := Load64(padded[:])

	// Convert ASCII to nibbles
	chunk2 |= 0x2020202020202020                              // Force lowercase
	letterMask2 := (chunk2 & 0x4040404040404040) >> 6         // Detect letters
	chunk2 = chunk2 - 0x3030303030303030 - (letterMask2 * 39) // Convert to nibbles

	// SIMD nibble compaction
	extracted = chunk2 & 0x000F000F000F000F
	chunk2 ^= extracted
	chunk2 |= extracted << 12

	extracted = chunk2 & 0xFF000000FF000000
	chunk2 ^= extracted
	chunk2 |= extracted >> 24

	extracted = chunk2 & 0x000000000000FFFF
	chunk2 ^= extracted
	chunk2 |= extracted << 48

	// Combine results: first chunk shifted left, second chunk in lower bits
	secondValue := chunk2 >> 32
	result = (result << (remaining2 * 4)) | secondValue

	return result
}

// ParseEthereumAddress parses 40-char Ethereum address to [20]byte using SIMD optimization
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseEthereumAddress(b []byte) [20]byte {
	var result [20]byte

	if len(b) == 0 {
		return result
	}

	j := 0
	// Skip 0x prefix
	if len(b) >= 2 && b[0] == '0' && (b[1]|0x20) == 'x' {
		j = 2
	}

	byteIdx := 0

	// Process exactly 5 iterations of 8 chars each = 40 chars = 20 bytes
	for byteIdx < 20 && j+7 < len(b) {
		chunk := Load64(b[j:])

		// Convert ASCII to nibbles
		chunk |= 0x2020202020202020                            // Force lowercase
		letterMask := (chunk & 0x4040404040404040) >> 6        // Detect letters
		chunk = chunk - 0x3030303030303030 - (letterMask * 39) // Convert to nibbles

		// SIMD nibble compaction with endian correction
		extracted := chunk & 0x000F000F000F000F
		chunk ^= extracted
		chunk |= extracted << 12

		extracted = chunk & 0xFF000000FF000000
		chunk ^= extracted
		chunk |= extracted >> 24

		extracted = chunk & 0x000000000000FFFF
		chunk ^= extracted
		chunk |= extracted << 48

		// Extract 4 bytes from compacted result
		packed := chunk >> 32
		result[byteIdx] = byte(packed >> 24)
		result[byteIdx+1] = byte(packed >> 16)
		result[byteIdx+2] = byte(packed >> 8)
		result[byteIdx+3] = byte(packed)

		byteIdx += 4
		j += 8
	}

	return result
}

// ============================================================================
// JSON PARSING UTILITIES
// ============================================================================

// SkipToQuote finds next quote with hop-based traversal
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SkipToQuote(p []byte, startIdx int, hopSize int) int {
	for i := startIdx; i < len(p); i += hopSize {
		if p[i] == '"' {
			return i
		}
	}
	return -1
}

// SkipToOpeningBracket finds [ character with hop-based traversal
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SkipToOpeningBracket(p []byte, startIdx int, hopSize int) int {
	for i := startIdx; i < len(p); i += hopSize {
		if p[i] == '[' {
			return i
		}
	}
	return -1
}

// SkipToClosingBracket finds ] character with hop-based traversal
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SkipToClosingBracket(p []byte, startIdx int, hopSize int) int {
	for i := startIdx; i < len(p); i += hopSize {
		if p[i] == ']' {
			return i
		}
	}
	return -1
}

// SkipToQuoteEarlyExit finds quote with hop limit for bounded parsing
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SkipToQuoteEarlyExit(p []byte, startIdx int, hopSize int, maxHops int) (int, bool) {
	i := startIdx
	hops := 0

	for ; i < len(p); i += hopSize {
		hops++
		if hops > maxHops {
			return i, true // Early exit
		}
		if p[i] == '"' {
			return i, false
		}
	}

	return -1, false
}

// SkipToClosingBracketEarlyExit finds ] with hop limit for bounded parsing
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SkipToClosingBracketEarlyExit(p []byte, startIdx int, hopSize int, maxHops int) (int, bool) {
	i := startIdx
	hops := 0

	for ; i < len(p); i += hopSize {
		hops++
		if hops > maxHops {
			return i, true // Early exit
		}
		if p[i] == ']' {
			return i, false
		}
	}

	return -1, false
}

// ============================================================================
// SYSTEM I/O UTILITIES
// ============================================================================

// PrintInfo writes to stdout via direct syscall
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func PrintInfo(msg string) {
	msgBytes := *(*[]byte)(unsafe.Pointer(&msg))
	syscall.Write(1, msgBytes)
}

// PrintWarning writes to stderr via direct syscall
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func PrintWarning(msg string) {
	msgBytes := *(*[]byte)(unsafe.Pointer(&msg))
	syscall.Write(2, msgBytes)
}
