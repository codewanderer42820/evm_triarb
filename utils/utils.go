// ════════════════════════════════════════════════════════════════════════════════════════════════
// Memory Operations and Utility Functions
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Core Utility Functions
//
// Description:
//   Utility functions for memory operations, type conversion, hex parsing, and JSON processing.
//   Optimized for zero-allocation operation and efficient memory access patterns.
//
// Features:
//   - Memory load operations with direct pointer access
//   - Type conversion utilities with zero-copy semantics
//   - SIMD-optimized hex parsing functions
//   - JSON field detection and system I/O utilities
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package utils

import (
	"math/bits"
	"syscall"
	"unsafe"
)

// ============================================================================
// MEMORY OPERATIONS
// ============================================================================

// Load64 performs unaligned 8-byte load from memory as little-endian uint64.
// Bypasses Go's alignment requirements using unsafe pointers for direct CPU access.
//
// Safety: Caller must ensure at least 8 bytes are available.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Load64(b []byte) uint64 {
	// Direct memory load - compiles to single MOV instruction
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

// Load128 performs unaligned 16-byte load from memory as two uint64 values.
// Optimized for SIMD operations that process 128-bit values in parallel.
//
// Returns: (first 8 bytes, second 8 bytes) in little-endian order
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Load128(b []byte) (uint64, uint64) {
	// Cast to array pointer for efficient dual load
	p := (*[2]uint64)(unsafe.Pointer(&b[0]))
	return p[0], p[1]
}

// LoadBE64 performs 8-byte load from memory as big-endian uint64.
// Used for network protocols and cryptographic operations requiring
// big-endian byte order regardless of host architecture.
// Go compiler optimizes this pattern to a single BSWAP instruction.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LoadBE64(b []byte) uint64 {
	// Manual byte assembly for big-endian conversion
	return uint64(b[0])<<56 | uint64(b[1])<<48 |
		uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 |
		uint64(b[6])<<8 | uint64(b[7])
}

// ============================================================================
// TYPE CONVERSION UTILITIES
// ============================================================================

// B2s converts byte slice to string without memory allocation.
// Creates string header pointing to same underlying memory as byte slice.
//
// Safety Warning:
//   - Byte slice must not be modified after conversion
//   - Byte slice must remain valid for string's lifetime
//   - Violating these rules causes undefined behavior
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func B2s(b []byte) string {
	// unsafe.String creates string header pointing to byte slice data
	return unsafe.String(&b[0], len(b))
}

// Itoa converts non-negative integer to decimal string representation.
// Optimized for small positive integers with single stack allocation.
//
// Limitations:
//   - Only handles non-negative integers (returns empty string for negative)
//   - Maximum value: 2^31-1 (uses 10-digit buffer)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Itoa(n int) string {
	// Fast path for zero
	if n == 0 {
		return "0"
	}

	// Stack-allocated buffer for maximum 32-bit integer
	var buf [10]byte
	i := len(buf)

	// Extract digits from least to most significant
	for n > 0 {
		i--
		buf[i] = byte(n%10 + '0')
		n /= 10
	}

	return string(buf[i:])
}

// Ftoa converts float64 to string representation with optimized formatting.
// Uses IEEE 754 bit manipulation and stack-based formatting for single allocation.
//
// Format selection:
//   - Integers < 1e6: Plain decimal (e.g., "42")
//   - Small decimals: Fixed-point up to 6 digits (e.g., "3.14159")
//   - Large/small values: Scientific notation (e.g., "1.23e+10")
//   - Special values: "NaN", "+Inf", "-Inf"
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Ftoa(f float64) string {
	// Stack buffer for worst-case scientific notation
	var buf [32]byte
	i := len(buf)

	// Extract IEEE 754 bit representation for special value detection
	bits := *(*uint64)(unsafe.Pointer(&f))

	// IEEE 754 special values check via exponent field
	exp := (bits >> 52) & 0x7FF
	if exp == 0x7FF {
		if bits&0xFFFFFFFFFFFFF != 0 {
			// NaN: exponent = all 1s, mantissa != 0
			i -= 3
			buf[i] = 'N'
			buf[i+1] = 'a'
			buf[i+2] = 'N'
			return string(buf[i:])
		}
		// Infinity: exponent = all 1s, mantissa = 0
		if bits&0x8000000000000000 != 0 {
			i -= 4
			buf[i] = '-'
			buf[i+1] = 'I'
			buf[i+2] = 'n'
			buf[i+3] = 'f'
		} else {
			i -= 4
			buf[i] = '+'
			buf[i+1] = 'I'
			buf[i+2] = 'n'
			buf[i+3] = 'f'
		}
		return string(buf[i:])
	}

	// Handle zero (both +0 and -0 map to "0")
	if bits&0x7FFFFFFFFFFFFFFF == 0 {
		i--
		buf[i] = '0'
		return string(buf[i:])
	}

	// Extract and handle sign bit
	negative := bits&0x8000000000000000 != 0

	// Work with absolute value for conversion
	absF := f
	if negative {
		absF = -absF
	}

	// Fast path for integers in reasonable range
	if absF >= 1.0 && absF < 1e6 && absF == float64(uint64(absF)) {
		// Convert as integer - faster than general float conversion
		intVal := uint64(absF)

		// Extract digits (guaranteed to execute at least once since intVal >= 1)
		for {
			i--
			buf[i] = byte(intVal%10 + '0')
			intVal /= 10
			if intVal == 0 {
				break
			}
		}

		// Add sign if negative
		if negative {
			i--
			buf[i] = '-'
		}
		return string(buf[i:])
	}

	// Determine if scientific notation is needed
	if absF >= 1e6 || (absF < 1e-4 && absF != 0) {
		// Scientific notation format: -1.234567e+123
		scientificExp := 0
		normalizedF := absF

		// Normalize to range [1, 10)
		if normalizedF >= 10.0 {
			for normalizedF >= 10.0 {
				normalizedF /= 10.0
				scientificExp++
			}
		} else if normalizedF < 1.0 {
			for normalizedF < 1.0 {
				normalizedF *= 10.0
				scientificExp--
			}
		}

		// Format exponent part (right to left)
		expAbs := scientificExp
		if expAbs < 0 {
			expAbs = -expAbs
		}

		// Add exponent digits
		if expAbs >= 100 {
			i--
			buf[i] = byte(expAbs%10 + '0')
			expAbs /= 10
		}
		if expAbs >= 10 {
			i--
			buf[i] = byte(expAbs%10 + '0')
			expAbs /= 10
		}
		i--
		buf[i] = byte(expAbs + '0')

		// Add exponent sign
		i--
		if scientificExp >= 0 {
			buf[i] = '+'
		} else {
			buf[i] = '-'
		}

		// Add 'e' separator
		i--
		buf[i] = 'e'

		// Format mantissa fractional part
		fracPart := uint64((normalizedF-float64(uint64(normalizedF)))*1000000 + 0.5)

		// Remove trailing zeros for cleaner output
		if fracPart > 0 {
			for fracPart > 0 && fracPart%10 == 0 {
				fracPart /= 10
			}

			// Add fractional digits
			for fracPart > 0 {
				i--
				buf[i] = byte(fracPart%10 + '0')
				fracPart /= 10
			}

			// Add decimal point
			i--
			buf[i] = '.'
		}

		// Add mantissa integer part
		i--
		buf[i] = byte(uint64(normalizedF) + '0')

	} else {
		// Regular decimal notation for moderate values
		intPart := uint64(absF)

		// Handle fractional part with precision control
		fracF := absF - float64(intPart)
		if fracF > 0 {
			// Extract fractional digits
			tempF := fracF
			fracDigits := [6]byte{}
			fracCount := 0

			// Extract up to 6 fractional digits
			for fracCount < 6 && tempF > 1e-15 {
				tempF *= 10
				digit := int(tempF)
				fracDigits[fracCount] = byte(digit + '0')
				tempF -= float64(digit)
				fracCount++
			}

			// Remove trailing zeros
			for fracCount > 0 && fracDigits[fracCount-1] == '0' {
				fracCount--
			}

			// Add fractional digits to buffer
			if fracCount > 0 {
				for j := fracCount - 1; j >= 0; j-- {
					i--
					buf[i] = fracDigits[j]
				}

				// Add decimal point
				i--
				buf[i] = '.'
			}
		}

		// Add integer part
		if intPart == 0 {
			i--
			buf[i] = '0'
		} else {
			// Standard digit extraction
			for intPart > 0 {
				i--
				buf[i] = byte(intPart%10 + '0')
				intPart /= 10
			}
		}
	}

	// Add sign for negative numbers
	if negative {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}

// ============================================================================
// HEX PARSING UTILITIES
// ============================================================================

// ParseHexU32 converts up to 8 hexadecimal characters to uint32.
// Uses branchless ASCII-to-nibble conversion for consistent performance.
//
// Input requirements:
//   - Valid hex characters only (0-9, a-f, A-F)
//   - No validation performed - garbage in, garbage out
//   - Longer inputs are truncated to 8 characters
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU32(b []byte) uint32 {
	var result uint32

	// Process up to 8 hex characters for uint32
	for j := 0; j < len(b) && j < 8; j++ {
		// Branchless hex char to nibble conversion
		c := b[j] | 0x20                   // Force lowercase
		v := c - '0' - ((c&0x40)>>6)*39    // Convert to nibble value
		result = (result << 4) | uint32(v) // Shift and combine
	}

	return result
}

// ParseHexU64 converts up to 16 hexadecimal characters to uint64 using SIMD operations.
// Processes 8 characters simultaneously using 64-bit SIMD-style operations.
//
// Algorithm:
//  1. Load 8 ASCII characters as single 64-bit value
//  2. Convert all characters to nibbles in parallel
//  3. Compact nibbles using bit manipulation
//  4. Repeat for second 8 characters if needed
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU64(b []byte) uint64 {
	// Limit processing to 16 characters
	processLen := len(b)
	if processLen > 16 {
		processLen = 16
	}

	// Fast path for inputs up to 8 characters
	if processLen <= 8 {
		// Pad input to 8 bytes for SIMD processing
		padded := [8]byte{'0', '0', '0', '0', '0', '0', '0', '0'}
		copy(padded[8-processLen:], b[:processLen])

		// Load 8 bytes as single 64-bit value
		chunk := Load64(padded[:])

		// Parallel ASCII to nibble conversion
		chunk |= 0x2020202020202020                            // Force lowercase
		letterMask := (chunk & 0x4040404040404040) >> 6        // Detect letters
		chunk = chunk - 0x3030303030303030 - (letterMask * 39) // Convert to nibbles

		// SIMD-style nibble compaction using bit manipulation
		// Step 1: Gather alternating nibbles
		extracted := chunk & 0x000F000F000F000F
		chunk ^= extracted
		chunk |= extracted << 12

		// Step 2: Gather alternating bytes
		extracted = chunk & 0xFF000000FF000000
		chunk ^= extracted
		chunk |= extracted >> 24

		// Step 3: Gather final 16-bit groups
		extracted = chunk & 0x000000000000FFFF
		chunk ^= extracted
		chunk |= extracted << 48

		// Result is in upper 32 bits after compaction
		return chunk >> 32
	}

	// Path for 9-16 character inputs - process in two chunks
	var result uint64

	// Process first 8 characters
	chunk1 := Load64(b[:8])

	// Same SIMD conversion as above
	chunk1 |= 0x2020202020202020
	letterMask1 := (chunk1 & 0x4040404040404040) >> 6
	chunk1 = chunk1 - 0x3030303030303030 - (letterMask1 * 39)

	// Nibble compaction
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

	// Process remaining characters (1-8 chars)
	remaining2 := processLen - 8
	padded := [8]byte{'0', '0', '0', '0', '0', '0', '0', '0'}
	copy(padded[8-remaining2:], b[8:8+remaining2])

	chunk2 := Load64(padded[:])

	// SIMD conversion for second chunk
	chunk2 |= 0x2020202020202020
	letterMask2 := (chunk2 & 0x4040404040404040) >> 6
	chunk2 = chunk2 - 0x3030303030303030 - (letterMask2 * 39)

	// Nibble compaction
	extracted = chunk2 & 0x000F000F000F000F
	chunk2 ^= extracted
	chunk2 |= extracted << 12

	extracted = chunk2 & 0xFF000000FF000000
	chunk2 ^= extracted
	chunk2 |= extracted >> 24

	extracted = chunk2 & 0x000000000000FFFF
	chunk2 ^= extracted
	chunk2 |= extracted << 48

	// Combine both chunks with appropriate shift
	secondValue := chunk2 >> 32
	result = (result << (remaining2 * 4)) | secondValue

	return result
}

// ParseEthereumAddress converts 40-character hex string to 20-byte Ethereum address.
// Optimized specifically for Ethereum's 160-bit addresses using SIMD operations
// to process 8 hex characters (4 bytes) per iteration.
//
// Algorithm:
//  1. Load 8 ASCII characters as single 64-bit value
//  2. Convert all characters to nibbles in parallel
//  3. Compact nibbles using bit manipulation
//  4. Store resulting 4 bytes directly in little-endian order
//
// Input requirements:
//   - Exactly 40 hex characters (no 0x prefix)
//   - Valid hex characters only (0-9, a-f, A-F)
//   - No validation performed - garbage in, garbage out
//
// Returns: 20-byte Ethereum address
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ParseEthereumAddress(b []byte) [20]byte {
	var result [20]byte
	byteIdx := 0

	// Process 40 hex chars as 5 groups of 8 chars
	for byteIdx < 20 {
		// Load 8 hex characters as 64-bit value
		chunk := Load64(b[byteIdx<<1:])

		// Parallel ASCII to nibble conversion
		chunk |= 0x2020202020202020                            // Force lowercase
		letterMask := (chunk & 0x4040404040404040) >> 6        // Detect letters
		chunk = chunk - 0x3030303030303030 - (letterMask * 39) // Convert to nibbles

		// SIMD-style nibble compaction using bit manipulation
		// Step 1: Gather alternating nibbles
		extracted := chunk & 0x000F000F000F000F
		chunk ^= extracted
		chunk |= extracted << 12

		// Step 2: Gather alternating bytes
		extracted = chunk & 0xFF000000FF000000
		chunk ^= extracted
		chunk |= extracted >> 8

		// Step 3: Gather final 16-bit groups with adjusted mask
		extracted = chunk & 0x0000000000FFFF00
		chunk ^= extracted
		chunk |= extracted << 16

		// Store 4 compacted bytes directly as little-endian uint32
		*(*uint32)(unsafe.Pointer(&result[byteIdx])) = uint32(chunk >> 24)

		byteIdx += 4
	}

	return result
}

// CountHexLeadingZeros efficiently counts leading zero characters in hex-encoded data.
// Uses SIMD-style operations to process 32 hex characters in parallel, optimized for
// cryptocurrency address validation and numeric analysis.
//
// Algorithm:
//  1. Load 32 hex characters as four 8-byte chunks
//  2. XOR each chunk with ASCII '0' pattern to identify zero characters
//  3. Create bitmask indicating which chunks contain non-zero characters
//  4. Use bit manipulation to find first non-zero position
//
// Input requirements:
//   - Exactly 32 hex characters (typical for 128-bit values)
//   - Valid ASCII hex characters only
//   - No validation performed for maximum performance
//
// Returns: Number of leading zero characters (0-32)
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func CountHexLeadingZeros(segment []byte) int {
	// Define the 64-bit pattern representing eight consecutive ASCII '0' characters
	const ZERO_PATTERN = 0x3030303030303030

	// Process the 32-byte hex segment in four 8-byte chunks simultaneously
	// XOR with ZERO_PATTERN converts zero bytes to 0x00 and non-zero bytes to non-zero values
	c0 := Load64(segment[0:8]) ^ ZERO_PATTERN   // Process bytes 0-7
	c1 := Load64(segment[8:16]) ^ ZERO_PATTERN  // Process bytes 8-15
	c2 := Load64(segment[16:24]) ^ ZERO_PATTERN // Process bytes 16-23
	c3 := Load64(segment[24:32]) ^ ZERO_PATTERN // Process bytes 24-31

	// Create a bitmask indicating which 8-byte chunks contain only zero characters
	// The expression (x|(^x+1))>>63 produces 1 if any byte in x is non-zero, 0 if all bytes are zero
	mask := ((c0|(^c0+1))>>63)<<0 | ((c1|(^c1+1))>>63)<<1 |
		((c2|(^c2+1))>>63)<<2 | ((c3|(^c3+1))>>63)<<3

	// Find the first chunk that contains a non-zero character
	firstChunk := bits.TrailingZeros64(mask)

	// Handle the special case where all 32 hex characters are zeros
	if firstChunk == 64 {
		return 32 // All characters in the segment are zeros
	}

	// Create an array to access the processed chunks by index
	chunks := [4]uint64{c0, c1, c2, c3}

	// Within the first non-zero chunk, find the first non-zero byte
	// Divide by 8 to convert bit position to byte position within the chunk
	firstByte := bits.TrailingZeros64(chunks[firstChunk]) >> 3

	// Calculate the total number of leading zero characters
	return (firstChunk << 3) + firstByte
}

// ============================================================================
// JSON PARSING UTILITIES
// ============================================================================

// SkipToQuote finds next double quote character using hop-based traversal.
// Hop size allows skipping characters when approximate location is known.
//
// Use cases:
//   - hopSize=1: Linear search when location unknown
//   - hopSize>1: Skip known fixed-size fields in JSON
//
// Returns: Index of quote or -1 if not found
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SkipToQuote(p []byte, startIdx int, hopSize int) int {
	// Hop-based search allows efficient traversal of structured data
	for i := startIdx; i < len(p); i += hopSize {
		if p[i] == '"' {
			return i
		}
	}
	return -1
}

// SkipToOpeningBracket finds next '[' character using hop-based traversal.
// Optimized for parsing JSON arrays where approximate position is known.
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

// SkipToClosingBracket finds next ']' character using hop-based traversal.
// Used for efficiently locating the end of JSON arrays.
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

// SkipToQuoteEarlyExit finds quotes with bounded search for latency control.
// Limits maximum search distance to prevent unbounded latency in real-time systems.
//
// Returns:
//   - (index, false): Quote found at index
//   - (position, true): Search limit reached at position
//   - (-1, false): End of data reached without finding quote
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SkipToQuoteEarlyExit(p []byte, startIdx int, hopSize int, maxHops int) (int, bool) {
	i := startIdx
	hops := 0

	// Bounded search prevents worst-case latency spikes
	for ; i < len(p); i += hopSize {
		hops++
		if hops > maxHops {
			return i, true // Early exit with current position
		}
		if p[i] == '"' {
			return i, false // Found target
		}
	}

	return -1, false // Reached end without finding
}

// SkipToClosingBracketEarlyExit finds ']' with bounded search for latency control.
// Provides same early-exit mechanism as SkipToQuoteEarlyExit for
// consistent bounded-latency parsing across all JSON structure elements.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SkipToClosingBracketEarlyExit(p []byte, startIdx int, hopSize int, maxHops int) (int, bool) {
	i := startIdx
	hops := 0

	// Same bounded search pattern for consistency
	for ; i < len(p); i += hopSize {
		hops++
		if hops > maxHops {
			return i, true // Early exit
		}
		if p[i] == ']' {
			return i, false // Found
		}
	}

	return -1, false // Not found
}

// ============================================================================
// SYSTEM I/O UTILITIES
// ============================================================================

// PrintInfo writes informational messages directly to stdout using syscalls.
// Bypasses Go's fmt package and buffered I/O for minimal overhead.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func PrintInfo(msg string) {
	// Convert string to byte slice without allocation
	msgBytes := *(*[]byte)(unsafe.Pointer(&msg))
	syscall.Write(1, msgBytes) // fd=1 is stdout
}

// PrintWarning writes warning messages directly to stderr using syscalls.
// Similar to PrintInfo but targets stderr for error/warning output.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func PrintWarning(msg string) {
	// Same zero-copy conversion as PrintInfo
	msgBytes := *(*[]byte)(unsafe.Pointer(&msg))
	syscall.Write(2, msgBytes) // fd=2 is stderr
}

// ============================================================================
// FAST HASHING AND DEDUPLICATION
// ============================================================================

// Mix64 applies Murmur3-style 64-bit hash finalization for uniform bit distribution.
// Ensures small changes in input create large, unpredictable changes in output.
//
// Mathematical properties:
//   - Full avalanche: Each input bit affects all output bits
//   - No collisions for sequential integers
//   - Uniform distribution of output values
//
// Use cases:
//   - Converting sequential IDs to uniformly distributed values
//   - Creating hash table indices from packed data
//   - Pseudo-random number generation from deterministic seeds
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Mix64(x uint64) uint64 {
	// First round: spread high bits down
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd

	// Second round: spread low bits up
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53

	// Final round: ensure final mixing
	x ^= x >> 33

	// Constants are prime numbers chosen for optimal bit mixing
	return x
}
