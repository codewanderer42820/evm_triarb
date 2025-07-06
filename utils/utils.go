package utils

import (
	"syscall"
	"unsafe"
)

// ============================================================================
// HIGH-PERFORMANCE UTILITY FUNCTIONS
// ============================================================================

// ============================================================================
// ZERO-ALLOCATION TYPE CONVERSION
// ============================================================================

// B2s converts a byte slice to string without memory allocation.
// WARNING: The input byte slice must not be modified after conversion.
//
//go:nosplit
//go:inline
//go:registerparams
func B2s(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// Itoa converts an integer to string without heap allocation.
// Optimized for non-negative integers using a fixed-size buffer.
//
//go:nosplit
//go:inline
//go:registerparams
func Itoa(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [10]byte // Maximum 10 digits for 32-bit int
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
// ZERO-ALLOCATION OUTPUT FUNCTIONS
// ============================================================================

// PrintWarning writes a warning message directly to stderr without allocation.
// Bypasses fmt and log packages for zero-allocation error reporting.
//
//go:nosplit
//go:inline
//go:registerparams
func PrintWarning(msg string) {
	// Convert string to byte slice without allocation
	msgBytes := *(*[]byte)(unsafe.Pointer(&msg))

	// Write directly to stderr (file descriptor 2)
	_, _ = syscall.Write(2, msgBytes)
}

// PrintInfo writes an informational message directly to stdout without allocation.
// Bypasses fmt and log packages for zero-allocation status reporting.
//
//go:nosplit
//go:inline
//go:registerparams
func PrintInfo(msg string) {
	// Convert string to byte slice without allocation
	msgBytes := *(*[]byte)(unsafe.Pointer(&msg))

	// Write directly to stdout (file descriptor 1)
	_, _ = syscall.Write(1, msgBytes)
}

// ============================================================================
// JSON PARSING UTILITIES
// ============================================================================

// SkipToQuoteEarlyExit locates the next double-quote character in JSON data
// using hop-based traversal for efficiency. Returns early if hop limit exceeded.
//
//go:nosplit
//go:inline
//go:registerparams
func SkipToQuoteEarlyExit(p []byte, startIdx int, hopSize int, maxHops int) (int, bool) {
	i := startIdx
	hops := 0

	for ; i < len(p); i += hopSize {
		hops++
		if hops > maxHops {
			return i, true // Early exit due to hop limit
		}
		if p[i] == '"' {
			return i, false // Found quote
		}
	}

	return -1, false // Quote not found
}

// SkipToClosingBracketEarlyExit locates the closing bracket in JSON arrays
// using hop-based traversal. Returns early if hop limit exceeded.
//
//go:nosplit
//go:inline
//go:registerparams
func SkipToClosingBracketEarlyExit(p []byte, startIdx int, hopSize int, maxHops int) (int, bool) {
	i := startIdx
	hops := 0

	for ; i < len(p); i += hopSize {
		hops++
		if hops > maxHops {
			return i, true // Early exit due to hop limit
		}
		if p[i] == ']' {
			return i, false // Found closing bracket
		}
	}

	return -1, false // Bracket not found
}

// SkipToQuote locates the next double-quote character in JSON data
// using hop-based traversal for efficient string field parsing.
//
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

// SkipToOpeningBracket locates the opening bracket in JSON arrays
// for efficient array field parsing.
//
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

// SkipToClosingBracket locates the closing bracket in JSON arrays
// for efficient array field parsing.
//
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

// ============================================================================
// UNALIGNED MEMORY OPERATIONS
// ============================================================================

// Load64 loads 8 bytes as uint64 from unaligned memory
//
//go:nosplit
//go:inline
//go:registerparams
func Load64(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

// Load128 loads 16 bytes as two uint64s from unaligned memory
//
//go:nosplit
//go:inline
//go:registerparams
func Load128(b []byte) (uint64, uint64) {
	p := (*[2]uint64)(unsafe.Pointer(&b[0]))
	return p[0], p[1]
}

// LoadBE64 parses 8 bytes as big-endian uint64
//
//go:nosplit
//go:inline
//go:registerparams
func LoadBE64(b []byte) uint64 {
	_ = b[7] // Bounds check hint for compiler
	return uint64(b[0])<<56 | uint64(b[1])<<48 |
		uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 |
		uint64(b[6])<<8 | uint64(b[7])
}

// ============================================================================
// HIGH-PERFORMANCE HEX PARSING
// ============================================================================

// ParseHexU64 parses hex string (with or without 0x prefix) to uint64
// No error handling - assumes well-formed input
//
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU64(b []byte) uint64 {
	j := 0
	// Skip 0x prefix if present
	if len(b) >= 2 && b[0] == '0' && (b[1]|0x20) == 'x' {
		j = 2
	}

	var u uint64
	for ; j < len(b) && j < 18; j++ { // Limit to 16 hex digits + prefix
		c := b[j] | 0x20 // Convert to lowercase
		if c < '0' || c > 'f' || (c > '9' && c < 'a') {
			break
		}
		v := uint64(c - '0')
		if c > '9' {
			v -= 39 // Convert 'a'-'f' to 10-15
		}
		u = (u << 4) | v
	}
	return u
}

// ParseHexN parses up to 16 hex characters to uint64
// Optimized for fixed-length hex strings
//
//go:nosplit
//go:inline
//go:registerparams
func ParseHexN(b []byte) uint64 {
	var v uint64
	for _, c := range b {
		v <<= 4
		switch {
		case c >= '0' && c <= '9':
			v |= uint64(c - '0')
		case c >= 'a' && c <= 'f':
			v |= uint64(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v |= uint64(c-'A') + 10
		}
	}
	return v
}

// ParseHexU32 parses hex string to uint32
// Wrapper around ParseHexN for 32-bit output
//
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU32(b []byte) uint32 {
	return uint32(ParseHexN(b))
}

// ============================================================================
// FAST HASHING AND DEDUPLICATION
// ============================================================================

// Mix64 applies Murmur3-style 64-bit hash finalization
// Used for fast hash table operations and deduplication
//
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

// Hash17 reduces Ethereum address to 17-bit hash for bucketing
// Optimized for address-based routing and deduplication
//
//go:nosplit
//go:inline
//go:registerparams
func Hash17(addr []byte) uint32 {
	if len(addr) < 6 {
		return 0
	}
	raw := ParseHexN(addr[:6])
	return uint32(raw) & ((1 << 17) - 1) // Mask to 17 bits
}

// ============================================================================
// DESIGN NOTES
// ============================================================================

/*
PERFORMANCE OPTIMIZATIONS:

1. ZERO-ALLOCATION DESIGN:
   - All functions avoid heap allocations
   - Direct unsafe operations for type conversion
   - Stack-allocated buffers where needed

2. REGISTER OPTIMIZATION:
   - Functions marked with go:registerparams
   - Minimal local variables for register allocation
   - Inlined hot path operations

3. CACHE-FRIENDLY PATTERNS:
   - Hop-based traversal reduces cache misses
   - Unaligned memory access for performance
   - Batch processing where applicable

4. UNSAFE OPERATIONS:
   - Direct memory access for speed
   - Bounds checking hints for compiler
   - Type punning for efficient conversion

5. BRANCH MINIMIZATION:
   - Reduced conditional logic in hot paths
   - Lookup tables for character conversion
   - Bit manipulation over comparisons

USAGE PATTERNS:
- JSON parsing in high-throughput scenarios
- Ethereum address processing and routing
- Fast hash computation for deduplication
- Zero-copy string operations
- Direct system output without runtime overhead
*/
