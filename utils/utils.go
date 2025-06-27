package utils

import "unsafe"

///////////////////////////////////////////////////////////////////////////////
// Conversion Utilities — Zero-Alloc Casts
///////////////////////////////////////////////////////////////////////////////

// B2s converts a []byte to a string **without** allocation.
// ⚠️ Caller must ensure the input slice remains valid and unchanged.
// Used for human-readable print paths.
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

// FindQuote locates the next '"' after a ':' in `"field":"value"` patterns.
// It rejects malformed cases with non-space garbage after ':'.
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
				case c > ' ': // malformed — non-whitespace, non-quote
					return -1
				}
			}
		}
	}
	return -1
}

// FindBracket returns the first index of '[' used in JSON arrays (e.g. topics).
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

// SliceASCII returns the quoted string value starting at index i.
// It expects b[i] to be a '"' and returns the span between opening/closing quotes.
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

// SliceJSONArray extracts the payload between `[` and `]` starting at index i.
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

// Load64 reads an unaligned 64-bit word from a byte slice.
//
//go:nosplit
//go:inline
func Load64(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

// Load128 performs two consecutive unaligned 64-bit reads for fingerprinting.
//
//go:nosplit
//go:inline
func Load128(b []byte) (uint64, uint64) {
	p := (*[2]uint64)(unsafe.Pointer(&b[0]))
	return p[0], p[1]
}

// LoadBE64 performs a manual big-endian 64-bit read, avoiding dependency on binary.BigEndian.
//
//go:nosplit
//go:inline
func LoadBE64(b []byte) uint64 {
	_ = b[7] // bounds check hint
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 |
		uint64(b[3])<<32 | uint64(b[4])<<24 | uint64(b[5])<<16 |
		uint64(b[6])<<8 | uint64(b[7])
}

///////////////////////////////////////////////////////////////////////////////
// Hex Decoders — No Allocation, Early Exit on Malformed Input
///////////////////////////////////////////////////////////////////////////////

// ParseHexU64 parses a 64-bit uint from a (0x-optional) ASCII hex string.
// Stops at first non-nibble. ~5x faster than strconv.ParseUint.
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
			v -= 39 // a/A -> 10
		}
		u = (u << 4) | v
	}
	return u
}

// ParseHexN parses arbitrary-length hex input into a uint64.
// Useful for short hash-based fingerprints (like address prefix).
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

// ParseHexU32 is a wrapper to convert hex → uint32 directly.
//
//go:nosplit
//go:inline
func ParseHexU32(b []byte) uint32 {
	return uint32(ParseHexN(b))
}

///////////////////////////////////////////////////////////////////////////////
// Hash & Mixers — For Dedupe Indexing & Key Rotation
///////////////////////////////////////////////////////////////////////////////

// Mix64 applies a Murmur3-style avalanche to a 64-bit value.
// Used to randomize index mapping inside dedupe ring.
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

// Hash17 reduces a lowercase Ethereum address (40 ASCII hex bytes) to a 17-bit bucket index.
// Uses the first 6 hex chars (3 bytes entropy), parsed and masked to 17 bits.
//
//go:nosplit
//go:inline
func Hash17(addr []byte) uint32 {
	if len(addr) < 6 {
		return 0
	}
	raw := ParseHexN(addr[:6])           // parse first 3 bytes
	return uint32(raw) & ((1 << 17) - 1) // mask to 17 bits
}
