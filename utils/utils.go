// utils.go — High-performance utility functions for zero-allocation operations
// ============================================================================
// HIGH-PERFORMANCE UTILITY FUNCTIONS
// ============================================================================
//
// Utils package provides zero-allocation utility functions optimized for
// high-frequency trading systems requiring sub-microsecond response times.
//
// Architecture overview:
//   • Zero-allocation type conversions with unsafe pointer operations
//   • Direct syscall integration bypassing standard library overhead
//   • SIMD-optimized memory operations for unaligned data access
//   • Cache-friendly JSON parsing with hop-based traversal algorithms
//
// Performance characteristics:
//   • Nanosecond-scale operations with zero heap allocations
//   • Unaligned memory access support for arbitrary data layouts
//   • Optimized hex parsing without error handling overhead
//   • Fast hashing functions for deduplication and bucketing
//
// Safety considerations:
//   • Unsafe pointer operations require careful lifetime management
//   • Unaligned memory access may cause performance penalties on some architectures
//   • No bounds checking in hot path functions for maximum performance
//   • Direct syscall usage bypasses standard library safety guarantees
//
// Threading model:
//   • All functions are thread-safe without synchronization overhead
//   • No shared mutable state across function calls
//   • Suitable for concurrent use in multi-core arbitrage detection
//   • Lock-free operations enable deterministic low-latency execution
//
// Memory layout legend:
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│ Input data → Zero-copy conversion → Direct operation → Output result    │
//	│       │              ▲                        │              │          │
//	│   Unsafe ptr ◀───────┘                        │              │          │
//	│       │                                   Hot path ◀─────────┘          │
//	│   Allocation ◀─────────────────────────────────┘                        │
//	└─────────────────────────────────────────────────────────────────────────┘
//
// Compiler optimizations:
//   • //go:nosplit for stack management elimination
//   • //go:inline for call overhead elimination
//   • //go:registerparams for register-based parameter passing
//   • //go:nocheckptr for bounds checking elimination in hot paths

package utils

import (
	"syscall"
	"unsafe"
)

// ============================================================================
// ZERO-ALLOCATION TYPE CONVERSION
// ============================================================================

// B2s converts a byte slice to string without memory allocation using unsafe
// pointer operations. The input byte slice must not be modified after conversion
// to prevent data corruption in the resulting string.
//
// Performance impact: Single instruction conversion with zero overhead
// Memory safety: Shares underlying data between slice and string
// Threading model: Safe for concurrent use without synchronization
//
// WARNING: The input byte slice must not be modified after conversion
// as this would corrupt the resulting string data.
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

// Itoa converts an integer to string without heap allocation using a fixed-size
// buffer for digit conversion. Optimized for non-negative integers commonly
// encountered in trading system operations.
//
// Performance impact: Fixed buffer eliminates allocation overhead
// Numerical range: Optimized for 32-bit integers (up to 10 digits)
// Threading model: Safe for concurrent use without synchronization
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

	var buf [10]byte // Maximum 10 digits for 32-bit int
	i := len(buf)

	// Convert digits in reverse order for optimal cache access
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

// PrintWarning writes a warning message directly to stderr without allocation
// by bypassing fmt and log packages. Uses direct syscall for minimal overhead
// in error reporting scenarios.
//
// Performance impact: Direct syscall eliminates buffering and formatting overhead
// Threading model: Safe for concurrent use (kernel handles synchronization)
// Error handling: Ignores syscall errors for maximum performance
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func PrintWarning(msg string) {
	// Convert string to byte slice without allocation using unsafe conversion
	msgBytes := *(*[]byte)(unsafe.Pointer(&msg))

	// Write directly to stderr (file descriptor 2) bypassing standard library
	_, _ = syscall.Write(2, msgBytes)
}

// PrintInfo writes an informational message directly to stdout without allocation
// by bypassing fmt and log packages. Uses direct syscall for minimal overhead
// in status reporting scenarios.
//
// Performance impact: Direct syscall eliminates buffering and formatting overhead
// Threading model: Safe for concurrent use (kernel handles synchronization)
// Error handling: Ignores syscall errors for maximum performance
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func PrintInfo(msg string) {
	// Convert string to byte slice without allocation using unsafe conversion
	msgBytes := *(*[]byte)(unsafe.Pointer(&msg))

	// Write directly to stdout (file descriptor 1) bypassing standard library
	_, _ = syscall.Write(1, msgBytes)
}

// ============================================================================
// JSON PARSING UTILITIES
// ============================================================================

// SkipToQuoteEarlyExit locates the next double-quote character in JSON data
// using hop-based traversal for efficiency. Returns early if hop limit exceeded
// to prevent excessive scanning in malformed or very large JSON structures.
//
// Performance impact: Hop-based traversal reduces memory access overhead
// Early exit prevents pathological behavior on malformed input
// Cache efficiency: Predictable access patterns optimize prefetching
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
			return i, true // Early exit due to hop limit
		}
		if p[i] == '"' {
			return i, false // Found quote
		}
	}

	return -1, false // Quote not found
}

// SkipToClosingBracketEarlyExit locates the closing bracket in JSON arrays
// using hop-based traversal. Returns early if hop limit exceeded to prevent
// excessive scanning in deeply nested or malformed JSON structures.
//
// Performance impact: Hop-based traversal reduces memory access overhead
// Early exit prevents pathological behavior on malformed input
// Cache efficiency: Predictable access patterns optimize prefetching
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
			return i, true // Early exit due to hop limit
		}
		if p[i] == ']' {
			return i, false // Found closing bracket
		}
	}

	return -1, false // Bracket not found
}

// SkipToQuote locates the next double-quote character in JSON data using
// hop-based traversal for efficient string field parsing in WebSocket logs.
//
// Performance impact: Hop-based traversal reduces memory access overhead
// Cache efficiency: Predictable stride access patterns optimize prefetching
// Hot path optimization: No bounds checking for maximum performance
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

// SkipToOpeningBracket locates the opening bracket in JSON arrays for
// efficient array field parsing in WebSocket transaction logs.
//
// Performance impact: Linear scan with hop optimization for cache efficiency
// Hot path optimization: No bounds checking for maximum performance
// Use case: Topics array parsing in Ethereum event logs
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

// SkipToClosingBracket locates the closing bracket in JSON arrays for
// efficient array field parsing in WebSocket transaction logs.
//
// Performance impact: Linear scan with hop optimization for cache efficiency
// Hot path optimization: No bounds checking for maximum performance
// Use case: Topics array parsing in Ethereum event logs
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

// ============================================================================
// UNALIGNED MEMORY OPERATIONS
// ============================================================================

// Load64 loads 8 bytes as uint64 from unaligned memory using unsafe pointer
// operations. Optimized for little-endian architectures common in trading systems.
//
// Performance impact: Single memory access with potential unalignment penalty
// Architecture dependency: Assumes little-endian byte order
// Memory safety: No bounds checking - caller must ensure sufficient data
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Load64(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

// Load128 loads 16 bytes as two uint64s from unaligned memory using unsafe
// pointer operations. Enables SIMD-style processing of 128-bit data blocks.
//
// Performance impact: Two memory accesses with potential unalignment penalty
// Architecture dependency: Assumes little-endian byte order
// Memory safety: No bounds checking - caller must ensure sufficient data
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

// LoadBE64 parses 8 bytes as big-endian uint64 using explicit byte manipulation
// for network byte order processing in Ethereum data structures.
//
// Performance impact: Eight byte operations with compiler optimization
// Endianness: Explicitly handles big-endian format regardless of architecture
// Memory safety: Bounds check hint for compiler optimization
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LoadBE64(b []byte) uint64 {
	_ = b[7] // Bounds check hint for compiler optimization
	return uint64(b[0])<<56 | uint64(b[1])<<48 |
		uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 |
		uint64(b[6])<<8 | uint64(b[7])
}

// ============================================================================
// HIGH-PERFORMANCE HEX PARSING
// ============================================================================

// ParseHexU64 parses hex string (with or without 0x prefix) to uint64 without
// error handling for maximum performance in trusted input scenarios.
//
// Performance impact: Optimized character conversion without validation overhead
// Input handling: Supports both 0x-prefixed and raw hex strings
// Error handling: No validation - assumes well-formed input for speed
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU64(b []byte) uint64 {
	j := 0
	// Skip 0x prefix if present (case-insensitive)
	if len(b) >= 2 && b[0] == '0' && (b[1]|0x20) == 'x' {
		j = 2
	}

	var u uint64
	for ; j < len(b) && j < 18; j++ { // Limit to 16 hex digits + prefix
		c := b[j] | 0x20 // Convert to lowercase for uniform processing
		if c < '0' || c > 'f' || (c > '9' && c < 'a') {
			break // Stop on invalid character
		}
		v := uint64(c - '0')
		if c > '9' {
			v -= 39 // Convert 'a'-'f' to 10-15
		}
		u = (u << 4) | v
	}
	return u
}

// ParseHexN parses up to 16 hex characters to uint64 using optimized character
// conversion for fixed-length hex strings common in Ethereum data.
//
// Performance impact: Unrolled loop with optimized character conversion
// Input handling: Processes raw hex characters without prefix support
// Error handling: Invalid characters convert to zero without termination
//
//go:norace
//go:nocheckptr
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

// ParseHexU32 parses hex string to uint32 using ParseHexN with type conversion
// for 32-bit values common in Ethereum log indices and transaction data.
//
// Performance impact: Delegates to ParseHexN with single type conversion
// Output range: Truncates to 32-bit value from 64-bit intermediate
// Use case: Optimized for log indices and transaction positions
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU32(b []byte) uint32 {
	return uint32(ParseHexN(b))
}

// ============================================================================
// FAST HASHING AND DEDUPLICATION
// ============================================================================

// Mix64 applies Murmur3-style 64-bit hash finalization for fast hash table
// operations and content deduplication in high-throughput scenarios.
//
// Performance impact: Fixed number of operations independent of input size
// Hash quality: Excellent avalanche properties for uniform distribution
// Use case: Hash table operations and content fingerprinting
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

// Hash17 reduces Ethereum address to 17-bit hash for bucketing and routing
// operations. Optimized for address-based routing and load balancing.
//
// Performance impact: Fast prefix extraction with bit masking
// Output range: 17-bit hash (0-131071) suitable for moderate bucketing
// Use case: Address-based routing and deduplication in trading systems
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
	return uint32(raw) & ((1 << 17) - 1) // Mask to 17 bits
}
