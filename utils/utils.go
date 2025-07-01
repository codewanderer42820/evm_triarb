// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: utils.go — ISR-grade zero-alloc utilities for JSON, hex, and hashing
//
// Purpose:
//   - Provides unsafe low-level helpers for parsing, casting, and hashing
//   - Used throughout parser, deduper, and log emitter paths
//
// Notes:
//   - All functions are branch-minimized, registerparam-optimized, and inlined
//   - No allocations, no reflection, no strings — everything byte-level and fused
//   - Unsafe use is deliberate for ISR-class log ingestion
//
// Compiler Directives:
//   - //go:nosplit
//   - //go:inline
//   - //go:registerparams
//
// ⚠️ All inputs must be bounds-safe and well-formed — no fallback handling
// ─────────────────────────────────────────────────────────────────────────────

package utils

import "unsafe"

// ───────────────────── Zero-Alloc Type Coercion ─────────────────────

// B2s converts []byte → string with zero alloc.
// ⚠️ Input must not be mutated after conversion.
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

// ────────────── JSON Field Probes (Unsafe, Fixed Pattern) ──────────────

// FindQuote finds next '"' after a ':' → used for string fields
//
//go:nosplit
//go:inline
//go:registerparams
func FindQuote(b []byte) int {
	for i := 0; i < len(b)-1; i++ {
		if b[i] == ':' {
			for j := i + 1; j < len(b); j++ {
				switch c := b[j]; {
				case c == '"':
					return j
				case c > ' ':
					return -1 // malformed
				}
			}
		}
	}
	return -1
}

// FindBracket finds first '[' (for topics array)
//
//go:nosplit
//go:inline
//go:registerparams
func FindBracket(b []byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == '[' {
			return i
		}
	}
	return -1
}

// SliceASCII extracts quoted "..." string
//
//go:nosplit
//go:inline
//go:registerparams
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

// SliceJSONArray extracts [...] region from start of array
//
//go:nosplit
//go:inline
//go:registerparams
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

// ───────────────────── Unaligned Memory Loaders ─────────────────────

// Load64 loads 8 bytes as uint64 from b[0:8]
//
//go:nosplit
//go:inline
//go:registerparams
func Load64(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

// Load128 loads 16 bytes as two uint64s from b[0:16]
//
//go:nosplit
//go:inline
//go:registerparams
func Load128(b []byte) (uint64, uint64) {
	p := (*[2]uint64)(unsafe.Pointer(&b[0]))
	return p[0], p[1]
}

// LoadBE64 parses b[0:8] as big-endian uint64
//
//go:nosplit
//go:inline
//go:registerparams
func LoadBE64(b []byte) uint64 {
	_ = b[7] // bounds hint
	return uint64(b[0])<<56 | uint64(b[1])<<48 |
		uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 |
		uint64(b[6])<<8 | uint64(b[7])
}

// ───────────────────── Hex Parsers (No Error Path) ─────────────────────

// ParseHexU64 parses 0x-prefixed or raw hex → uint64
//
//go:nosplit
//go:inline
//go:registerparams
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

// ParseHexN parses ≤16 chars of hex (0-9a-fA-F) into uint64
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

// ParseHexU32 is a wrapper around ParseHexN for 32-bit output
//
//go:nosplit
//go:inline
//go:registerparams
func ParseHexU32(b []byte) uint32 {
	return uint32(ParseHexN(b))
}

// ───────────────────── Fast Hashes (Dedup & Routing) ─────────────────────

// Mix64 is a Murmur3-style 64-bit finalizer hash
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

// Hash17 reduces a 0x-prefixed Ethereum address to 17-bit bucket
//
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
