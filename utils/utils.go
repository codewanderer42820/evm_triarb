// utils.go — High-performance utilities for zero-allocation operations
package utils

import (
	"syscall"
	"unsafe"
)

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

	// Convert digits in reverse
	for n > 0 {
		i--
		buf[i] = byte(n%10 + '0')
		n /= 10
	}

	return string(buf[i:])
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

// SkipToQuoteEarlyExit finds quote with hop limit
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

// SkipToClosingBracketEarlyExit finds ] with hop limit
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

// SkipToOpeningBracket finds [ character
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

// SkipToClosingBracket finds ] character
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

// LoadBE64 parses 8 bytes as big-endian uint64
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

// ParseHexU64 parses hex string to uint64 with big-endian order
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU64(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}

	j := 0
	// Skip 0x prefix
	if len(b) >= 2 && b[0] == '0' && (b[1]|0x20) == 'x' {
		j = 2
	}

	var result uint64

	// Process 8 bytes at a time with SIMD algorithm
	for j+7 < len(b) && j < 18 {
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

		// Extract final result from upper 32 bits where compaction placed it
		result = (result << 32) | (chunk >> 32)
		j += 8
	}

	// Handle remaining bytes with branchless optimization
	for ; j < len(b) && j < 18; j++ {
		c := b[j] | 0x20
		v := c - '0' - ((c&0x40)>>6)*39

		// Branchless exit: if invalid, v will be > 15
		if v > 15 {
			break
		}

		result = (result << 4) | uint64(v)
	}

	return result
}

// ParseHexN parses up to 16 hex chars with bit manipulation
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseHexN(b []byte) uint64 {
	var result uint64

	for _, c := range b {
		c |= 0x20 // Force lowercase
		isLetter := (c & 0x40) >> 6
		v := c - '0' - (isLetter * 39)

		if v > 15 {
			break
		}
		result = (result << 4) | uint64(v)
	}

	return result
}

// ParseHexU32 parses hex to uint32
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU32(b []byte) uint32 {
	return uint32(ParseHexN(b))
}

// Mix64 applies Murmur3 finalization
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// Hash17 reduces address to 17-bit hash
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Hash17(addr []byte) uint32 {
	if len(addr) < 6 {
		return 0
	}
	raw := ParseHexN(addr[:6])
	return uint32(raw) & ((1 << 17) - 1)
}
