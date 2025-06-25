// utils/utils.go — low-level helpers shared by parser, router, deduper & WS I/O.
package utils

import "unsafe"

///////////////////////////////////////////////////////////////////////////////
// Tiny zero-alloc conversions & key probes
///////////////////////////////////////////////////////////////////////////////

// B2s converts a []byte to string without an allocation.
//
//go:nosplit
//go:inline
func B2s(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b)) // caller must keep b immutable
}

///////////////////////////////////////////////////////////////////////////////
// Micro-scanners used by the JSON fast path
///////////////////////////////////////////////////////////////////////////////

// FindQuote returns the index of the next '"' after the ':' in `"field":"`.
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
				case c > ' ': // anything non-whitespace ⇒ malformed
					return -1
				}
			}
		}
	}
	return -1
}

// FindBracket returns the index of the first '[' (for JSON topic arrays).
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

// SliceASCII returns bytes between the two '"' that start at b[i].
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

// SliceJSONArray returns the bytes inside [...] (without the brackets).
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
// Unaligned word loads (little-endian)
///////////////////////////////////////////////////////////////////////////////

//go:nosplit
//go:inline
func Load64(b []byte) uint64 { return *(*uint64)(unsafe.Pointer(&b[0])) }

//go:nosplit
//go:inline
func Load128(b []byte) (uint64, uint64) {
	p := (*[2]uint64)(unsafe.Pointer(&b[0]))
	return p[0], p[1]
}

///////////////////////////////////////////////////////////////////////////////
// Fast hex decoders (no allocations, stop at first invalid nibble)
///////////////////////////////////////////////////////////////////////////////

// ParseHexU64 parses a (0x-optional) hex string into uint64.
//
//go:nosplit
//go:inline
func ParseHexU64(b []byte) uint64 {
	j := 0
	if len(b) >= 2 && b[0] == '0' && (b[1]|0x20) == 'x' {
		j = 2
	}
	var u uint64
	for ; j < len(b) && j < 18; j++ { // max 16 nibbles = 64 bits
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

// ParseHexN handles any length hex string.
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

//go:nosplit
//go:inline
func ParseHexU32(b []byte) uint32 { return uint32(ParseHexN(b)) }

///////////////////////////////////////////////////////////////////////////////
// Misc – 64-bit avalanche mixer (MurmurHash3 finalizer)
///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////
// Address hash → 17-bit bucket index (array lookup)
///////////////////////////////////////////////////////////////////////////////

// Hash17 maps the first 6 hex chars of a 40-byte lowercase address into 17 bits.
// It is deterministic, zero-alloc, and <10 ns on modern CPUs.
//
//go:nosplit
//go:inline
func Hash17(addr []byte) uint32 {
	if len(addr) < 6 {
		return 0
	}
	raw := ParseHexN(addr[:6])           // 24-bit value
	return uint32(raw) & ((1 << 17) - 1) // keep low 17 bits
}
