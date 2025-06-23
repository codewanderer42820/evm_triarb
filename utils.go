// utils.go — low-level helpers shared by parser, deduper & WS I/O.
package main

import "unsafe"

///////////////////////////////////////////////////////////////////////////////
// Tiny zero-alloc conversions & key probes
///////////////////////////////////////////////////////////////////////////////

// b2s converts a []byte to string without an allocation.
//
//go:nosplit
//go:inline
func b2s(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b)) // caller must keep b immutable
}

///////////////////////////////////////////////////////////////////////////////
// Micro-scanners used by the JSON fast path
///////////////////////////////////////////////////////////////////////////////

// findQuote returns the index of the next '"' after the ':' in `"field":"`.
//
//go:nosplit
//go:inline
func findQuote(b []byte) int {
	for i := 0; i < len(b)-1; i++ {
		if b[i] == ':' {
			for j := i + 1; j < len(b); j++ {
				switch c := b[j]; {
				case c == '"':
					return j
				case c > ' ': // hit anything non-whitespace ⇒ malformed
					return -1
				}
			}
		}
	}
	return -1
}

// findBracket returns the index of the first '[' (for JSON topic arrays).
//
//go:nosplit
//go:inline
func findBracket(b []byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == '[' {
			return i
		}
	}
	return -1
}

// sliceASCII returns bytes between the two '"' that start at b[i].
//
//go:nosplit
//go:inline
func sliceASCII(b []byte, i int) []byte {
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

// sliceJSONArray returns the bytes inside [...] (without the brackets).
//
//go:nosplit
//go:inline
func sliceJSONArray(b []byte, i int) []byte {
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
func load64(b []byte) uint64 { return *(*uint64)(unsafe.Pointer(&b[0])) }

//go:nosplit
//go:inline
func load128(b []byte) (uint64, uint64) {
	p := (*[2]uint64)(unsafe.Pointer(&b[0]))
	return p[0], p[1]
}

///////////////////////////////////////////////////////////////////////////////
// Fast hex decoders (no allocations, stop at first invalid nibble)
///////////////////////////////////////////////////////////////////////////////

// parseHexU64 parses a (0x-optional) hex string into uint64.
//
//go:nosplit
//go:inline
func parseHexU64(b []byte) uint64 {
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

// parseHexN handles any length (used by uint32 helper below).
//
//go:nosplit
//go:inline
func parseHexN(b []byte) uint64 {
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
func parseHexU32(b []byte) uint32 { return uint32(parseHexN(b)) }

///////////////////////////////////////////////////////////////////////////////
// Misc – 64-bit avalanche mixer (MurmurHash3 finalizer)
///////////////////////////////////////////////////////////////////////////////

//go:nosplit
//go:inline
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
