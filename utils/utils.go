package utils

import "unsafe"

///////////////////////////////////////////////////////////////////////////////
// Conversion Utilities — Zero-Alloc Casts
///////////////////////////////////////////////////////////////////////////////

// B2s converts a []byte to string with zero allocations.
// ⚠️ Caller must ensure the byte slice is immutable while in use.
//
// Used for fast log printing in emitLog().
//
//go:nosplit
//go:inline
func B2s(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

///////////////////////////////////////////////////////////////////////////////
// JSON Micro-Scanners — For Field Detection & Slice Extraction
///////////////////////////////////////////////////////////////////////////////

// FindQuote locates the index of the next '"' after a ':'.
// This helps extract `"field":"value"` spans.
//
// Returns index of the opening quote, or -1 if malformed.
//
//go:nosplit
//go:inline
func FindQuote(b []byte) int {
	for i := 0; i < len(b)-1; i++ {
		if b[i] == ':' {
			for j := i + 1; j < len(b); j++ {
				switch c := b[j]; {
				case c == '"':
					return j
				case c > ' ':
					return -1 // invalid char
				}
			}
		}
	}
	return -1
}

// FindBracket finds the first '[' in a JSON field like `"topics":[...`.
//
//go:nosplit
//go:inline
func FindBracket(b []byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == '[' {
			return i
		}
	}
	return -1
}

// SliceASCII returns the quoted string between `"..."` at index i.
// Expects that b[i] == '"'. Returns nil if malformed.
//
//go:nosplit
//go:inline
func SliceASCII(b []byte, i int) []byte {
	if i < 0 || i >= len(b) || b[i] != '"' {
		return nil
	}
	for j := i + 1; j < len(b); j++ {
		if b[j] == '"' {
			return b[i+1 : j]
		}
	}
	return nil
}

// SliceJSONArray extracts the bytes between `[` and `]` starting at b[i].
//
//go:nosplit
//go:inline
func SliceJSONArray(b []byte, i int) []byte {
	if i < 0 || i >= len(b) || b[i] != '[' {
		return nil
	}
	for j := i; j < len(b); j++ {
		if b[j] == ']' {
			return b[i+1 : j]
		}
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// Fast Loaders — Unaligned 64/128-Bit Reads
///////////////////////////////////////////////////////////////////////////////

// Load64 reads 8 bytes as uint64 from a byte slice.
// No alignment or bounds checks.
//
//go:nosplit
//go:inline
func Load64(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

// Load128 reads 16 bytes as two uint64s from b[0]..b[15].
//
//go:nosplit
//go:inline
func Load128(b []byte) (uint64, uint64) {
	p := (*[2]uint64)(unsafe.Pointer(&b[0]))
	return p[0], p[1]
}

// LoadBE64 manually parses a big-endian uint64 from b[0..7].
//
//go:nosplit
//go:inline
func LoadBE64(b []byte) uint64 {
	_ = b[7] // hint to compiler: bounds safe
	return uint64(b[0])<<56 | uint64(b[1])<<48 |
		uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 |
		uint64(b[6])<<8 | uint64(b[7])
}

///////////////////////////////////////////////////////////////////////////////
// Hex Decoders — No Allocation, Early Exit on Malformed Input
///////////////////////////////////////////////////////////////////////////////

// ParseHexU64 parses an ASCII-encoded hexadecimal number into uint64.
// Accepts optional `0x` prefix. Fast-paths Uniswap logs (0x...).
//
// ~5× faster than strconv.ParseUint. Tolerates garbage suffix.
//
//go:nosplit
//go:inline
func ParseHexU64(b []byte) uint64 {
	j := 0
	if len(b) >= 2 && b[0] == '0' && (b[1]|0x20) == 'x' {
		j = 2
	}
	var u uint64
	for ; j < len(b) && j < 18; j++ {
		c := b[j] | 0x20
		if c < '0' || c > 'f' || (c > '9' && c < 'a') {
			break
		}
		v := uint64(c - '0')
		if c > '9' {
			v -= 39 // 'a' → 10
		}
		u = (u << 4) | v
	}
	return u
}

// ParseHexN parses arbitrary-length lowercase or uppercase hex into uint64.
// Caller guarantees length ≤ 16. Garbage is silently ignored.
//
//go:nosplit
//go:inline
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

// ParseHexU32 converts hex → uint32 using ParseHexN.
//
//go:nosplit
//go:inline
func ParseHexU32(b []byte) uint32 {
	return uint32(ParseHexN(b))
}

///////////////////////////////////////////////////////////////////////////////
// Hash & Mixers — For Dedupe Indexing & Key Routing
///////////////////////////////////////////////////////////////////////////////

// Mix64 applies a Murmur3-style finalizer to x.
// Used to randomize deduper ring index.
//
//go:nosplit
//go:inline
func Mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// Hash17 reduces a 40-character lowercase Ethereum address to a 17-bit bucket index.
// Uses only the first 6 characters (3 bytes entropy).
//
//go:nosplit
//go:inline
func Hash17(addr []byte) uint32 {
	if len(addr) < 6 {
		return 0
	}
	raw := ParseHexN(addr[:6])
	return uint32(raw) & ((1 << 17) - 1)
}
