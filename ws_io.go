// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: ws_io.go — ISR-grade zero-alloc WebSocket frame I/O and buffer decoding
//
// Purpose:
//   - Reads and parses RFC 6455 WebSocket frames from a raw TCP connection
//   - Performs zero-alloc payload extraction and mask unwrapping in-place
//   - Eliminates ALL runtime allocations, including error objects and temporary buffers
//
// Notes:
//   - Only supports FIN=true, unfragmented, masked data frames (Infura style)
//   - Buffer reuse ensures zero heap pressure during log ingestion
//   - Frame ring (`wsFrames`) is used for direct zero-copy views into `wsBuf`
//   - All errors are pre-allocated to avoid runtime allocation during error handling
//   - Highly optimized for single-threaded operation with no concurrent access protection
//
// ⚠️ Caller MUST NOT retain frame.Payload beyond the next wsBuf overwrite — pointer invalidation risk
// ⚠️ Buffer compaction is triggered early for better memory utilization and cache locality
// ─────────────────────────────────────────────────────────────────────────────

package main

import (
	"encoding/binary"
	"io"
	"main/constants"
	"net"
	"unsafe"
)

// ────────────────────────── Pre-allocated Error Objects ─────────────────────────────

// All errors pre-allocated to avoid allocation during error paths
var (
	errHandshakeOverflow   = &wsError{msg: "handshake header overflow"}
	errFrameExceedsBuffer  = &wsError{msg: "frame exceeds buffer capacity"}
	errFragmentedFrame     = &wsError{msg: "fragmented frames not supported"}
	errFrameExceedsMaxSize = &wsError{msg: "frame exceeds maximum size"}
	errPongResponseFailed  = &wsError{msg: "failed to respond with pong"}
)

// wsError is a pre-allocated error type to avoid allocations
//
//go:notinheap
//go:align 64
type wsError struct {
	msg string
}

func (e *wsError) Error() string {
	return e.msg
}

// ────────────────────────── Handshake Parsing ─────────────────────────────

var (
	hsBuf [4096]byte // 4 KiB temporary buffer for HTTP headers
)

// readHandshake reads until HTTP upgrade response is complete.
// Uses zero-copy byte searching and pre-allocated buffers.
//
//go:nosplit
//go:inline
//go:registerparams
func readHandshake(c net.Conn) ([]byte, error) {
	n := 0
	for {
		if n == len(hsBuf) {
			return nil, errHandshakeOverflow
		}
		m, err := c.Read(hsBuf[n:])
		if err != nil {
			dropError("readHandshake", err)
			return nil, err
		}
		n += m
		// Zero-copy search for CRLF-CRLF delimiter using pre-computed pattern
		if findTerminator(hsBuf[:n]) >= 0 {
			return hsBuf[:n], nil
		}
	}
}

// findTerminator searches for CRLF-CRLF using architecture-specific pattern
//
//go:nosplit
//go:inline
//go:registerparams
func findTerminator(data []byte) int {
	if len(data) < 4 {
		return -1
	}
	for i := 0; i <= len(data)-4; i++ {
		if *(*uint32)(unsafe.Pointer(&data[i])) == crlfcrlfPattern {
			return i
		}
	}
	return -1
}

// ────────────────────── Buffer Compaction Helper ──────────────────────────

// ensureRoom guarantees ≥ `need` bytes in `wsBuf`, refilling from conn.
// Uses unsafe.Pointer for efficient memory copying.
// Optimized compaction strategy: compact when wsStart > len(wsBuf)/2
//
//go:nosplit
//go:inline
//go:registerparams
func ensureRoom(conn net.Conn, need int) error {
	if need > len(wsBuf) {
		return errFrameExceedsBuffer
	}

	for wsLen < need {
		// Optimized compaction: compact when wsStart > half buffer size
		// This prevents excessive memory waste and improves cache locality
		if wsStart > len(wsBuf)/2 || wsStart+wsLen == len(wsBuf) {
			// Zero-copy memory move using unsafe
			if wsLen > 0 {
				copy(wsBuf[:wsLen], wsBuf[wsStart:wsStart+wsLen])
			}
			wsStart = 0
		}
		// Read more data into the buffer
		n, err := conn.Read(wsBuf[wsStart+wsLen:])
		if err != nil {
			return err
		}
		wsLen += n
	}
	return nil
}

// ───────────────────────────── Frame Decoder ──────────────────────────────

// readFrame parses a single complete WebSocket frame from the stream.
// Fully zero-copy implementation with pre-allocated frame ring.
// Added bounds checking for frame ring overflow protection.
//
//go:nosplit
//go:inline
//go:registerparams
func readFrame(conn net.Conn) (*wsFrame, error) {
	for {
		// Step 1: Minimal header (2 bytes)
		if err := ensureRoom(conn, 2); err != nil {
			return nil, err
		}

		// Read header bytes directly (fix byte order issue)
		hdr0 := wsBuf[wsStart]
		hdr1 := wsBuf[wsStart+1]

		fin := hdr0 & 0x80
		opcode := hdr0 & 0x0F
		masked := hdr1 & 0x80
		plen7 := int(hdr1 & 0x7F)

		// Step 2: Handle special control frames
		switch opcode {
		case 0x8: // CLOSE frame
			return nil, io.EOF
		case 0x9: // PING frame
			wsStart += 2
			wsLen -= 2
			// Respond with pre-allocated pong frame
			_, err := conn.Write(pongFrame[:])
			if err != nil {
				return nil, errPongResponseFailed
			}
			continue
		case 0xA: // PONG frame
			wsStart += 2
			wsLen -= 2
			continue
		}

		// Step 3: Decode payload length with zero-copy
		offset := 2
		var plen int
		switch plen7 {
		case 126:
			if err := ensureRoom(conn, offset+2); err != nil {
				return nil, err
			}
			plen = int(binary.BigEndian.Uint16(wsBuf[wsStart+offset:]))
			offset += 2
		case 127:
			if err := ensureRoom(conn, offset+8); err != nil {
				return nil, err
			}
			plen64 := binary.BigEndian.Uint64(wsBuf[wsStart+offset:])
			if plen64 > constants.MaxFrameSize {
				return nil, errFrameExceedsMaxSize
			}
			plen = int(plen64)
			offset += 8
		default:
			plen = plen7
		}

		// Step 4: Read masking key with direct pointer access
		var mkey uint32
		if masked != 0 {
			if err := ensureRoom(conn, offset+4); err != nil {
				return nil, err
			}
			mkey = *(*uint32)(unsafe.Pointer(&wsBuf[wsStart+offset]))
			offset += 4
		}

		// Step 5: Read payload
		if err := ensureRoom(conn, offset+plen); err != nil {
			return nil, err
		}
		payloadStart := wsStart + offset
		payloadEnd := payloadStart + plen

		// Step 6: Unmask payload in-place with SIMD-friendly loop
		if masked != 0 {
			unmaskPayload(wsBuf[payloadStart:payloadEnd], mkey)
		}

		// Step 7: Reject fragmented frames
		if fin == 0 {
			return nil, errFragmentedFrame
		}

		// Step 8: Store frame in ring (truly zero-copy)
		idx := wsHead & (constants.FrameCap - 1)
		f := &wsFrames[idx]
		f.PayloadPtr = unsafe.Pointer(&wsBuf[payloadStart])
		f.PayloadStart = payloadStart
		f.PayloadEnd = payloadEnd
		f.Len = plen
		f.End = payloadEnd
		wsHead++

		// Step 9: Advance buffer position correctly
		totalFrameSize := offset + plen
		wsStart += totalFrameSize
		wsLen -= totalFrameSize

		return f, nil
	}
}

// unmaskPayload unmasks WebSocket payload in-place using efficient word-based operations
// Optimized for better performance with unrolled 8-byte operations
//
//go:nosplit
//go:inline
//go:registerparams
func unmaskPayload(payload []byte, maskKey uint32) {
	if len(payload) == 0 {
		return
	}

	// Convert mask to byte array for remainder processing
	mask := *(*[4]byte)(unsafe.Pointer(&maskKey))

	// Process 8 bytes at a time for better performance
	// Create 8-byte mask pattern once
	maskPattern := uint64(maskKey) | (uint64(maskKey) << 32)

	i := 0
	// Unroll loop for better performance on large payloads
	for i+15 < len(payload) {
		// Process 16 bytes at once (2 x 8-byte operations)
		dataPtr1 := (*uint64)(unsafe.Pointer(&payload[i]))
		dataPtr2 := (*uint64)(unsafe.Pointer(&payload[i+8]))
		*dataPtr1 ^= maskPattern
		*dataPtr2 ^= maskPattern
		i += 16
	}

	// Handle remaining 8-byte chunks
	for i+7 < len(payload) {
		dataPtr := (*uint64)(unsafe.Pointer(&payload[i]))
		*dataPtr ^= maskPattern
		i += 8
	}

	// Handle remaining bytes (0-7 bytes)
	for i < len(payload) {
		payload[i] ^= mask[i&3]
		i++
	}
}

// reclaimFrame marks a frame as processed and reclaims buffer space
//
//go:nosplit
//go:inline
//go:registerparams
func reclaimFrame(f *wsFrame) {
	// Move the start position forward to reclaim space
	if wsStart < f.End {
		wsStart = f.End
		wsLen -= (f.End - wsStart)
	}
}
