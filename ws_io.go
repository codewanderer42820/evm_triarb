// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: ws_io.go — Zero-alloc WebSocket frame I/O and buffer decoding
//
// Purpose:
//   - Reads and parses RFC 6455 WebSocket frames from a raw TCP connection
//   - Performs zero-alloc payload extraction and mask unwrapping
//   - Eliminates ALL runtime allocations including error objects
//
// Notes:
//   - Supports only FIN=true, unfragmented, masked data frames (Infura style)
//   - Buffer reuse guarantees zero heap pressure during all log ingestion
//   - Frame ring (`wsFrames`) is used to queue zero-copy views into `wsBuf`
//   - All errors are pre-allocated to avoid allocation during error handling
//
// ⚠️ Caller MUST not retain frame.Payload past next wsBuf overwrite
// ─────────────────────────────────────────────────────────────────────────────

package main

import (
	"encoding/binary"
	"io"
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
type wsError struct {
	msg string
}

func (e *wsError) Error() string {
	return e.msg
}

// ────────────────────────── Handshake Parsing ─────────────────────────────

var (
	hsBuf  [4096]byte                        // 4 KiB temporary buffer for HTTP headers
	hsTerm = [4]byte{'\r', '\n', '\r', '\n'} // CRLF–CRLF delimiter as array
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
		// Zero-copy search for CRLF-CRLF delimiter
		if findTerminator(hsBuf[:n]) >= 0 {
			return hsBuf[:n], nil
		}
	}
}

// findTerminator searches for CRLF-CRLF without allocating
//
//go:nosplit
//go:inline
func findTerminator(data []byte) int {
	if len(data) < 4 {
		return -1
	}
	for i := 0; i <= len(data)-4; i++ {
		if *(*uint32)(unsafe.Pointer(&data[i])) == *(*uint32)(unsafe.Pointer(&hsTerm[0])) {
			return i
		}
	}
	return -1
}

// ────────────────────── Buffer Compaction Helper ──────────────────────────

// ensureRoom guarantees ≥ `need` bytes in `wsBuf`, refilling from conn.
// Uses unsafe.Pointer for efficient memory copying.
//
//go:nosplit
//go:inline
//go:registerparams
func ensureRoom(conn net.Conn, need int) error {
	if need > len(wsBuf) {
		return errFrameExceedsBuffer
	}

	for wsLen < need {
		// Compact if necessary: if the buffer is full, move data to the beginning
		if wsStart+wsLen == len(wsBuf) {
			// Zero-copy memory move using unsafe
			if wsLen > 0 {
				memmove(unsafe.Pointer(&wsBuf[0]),
					unsafe.Pointer(&wsBuf[wsStart]),
					uintptr(wsLen))
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

// memmove efficiently moves memory without allocation
//
//go:nosplit
//go:inline
func memmove(dst, src unsafe.Pointer, n uintptr) {
	copy(unsafe.Slice((*byte)(dst), n), unsafe.Slice((*byte)(src), n))
}

// ───────────────────────────── Frame Decoder ──────────────────────────────

// readFrame parses a single complete WebSocket frame from the stream.
// Fully zero-copy implementation with pre-allocated frame ring.
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
			if plen64 > maxFrameSize {
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
		idx := wsHead & (frameCap - 1)
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
//
//go:nosplit
//go:inline
func unmaskPayload(payload []byte, maskKey uint32) {
	if len(payload) == 0 {
		return
	}

	// Convert mask to byte array
	mask := *(*[4]byte)(unsafe.Pointer(&maskKey))

	// Process 8 bytes at a time for better performance
	i := 0
	for i+7 < len(payload) {
		// Create 8-byte mask pattern
		maskPattern := uint64(maskKey) | (uint64(maskKey) << 32)

		// Apply mask to 8 bytes at once
		dataPtr := (*uint64)(unsafe.Pointer(&payload[i]))
		*dataPtr ^= maskPattern
		i += 8
	}

	// Handle remaining bytes
	for i < len(payload) {
		payload[i] ^= mask[i&3]
		i++
	}
}

// getUpgradeRequest returns the pre-built upgrade request without allocation
//
//go:nosplit
//go:inline
func getUpgradeRequest() []byte {
	return upgradeRequest[:upgradeLen]
}

// getSubscribePacket returns the pre-built subscribe packet without allocation
//
//go:nosplit
//go:inline
func getSubscribePacket() []byte {
	return subscribePacket[:subscribeLen]
}

// Example usage in your main connection code:
// Replace the problematic lines with:
//
// Step 4: Perform WebSocket upgrade handshake
// if _, err := conn.Write(getUpgradeRequest()); err != nil {
//     dropError("ws upgrade write", err)
//     return err
// }
//
// Read the WebSocket handshake response from the server
// if _, err := readHandshake(conn); err != nil {
//     dropError("ws handshake", err)
//     return err
// }
//
// Send subscribe packet
// if _, err := conn.Write(getSubscribePacket()); err != nil {
//     dropError("subscribe write", err)
//     return err
// }

// reclaimFrame marks a frame as processed and reclaims buffer space
//
//go:nosplit
//go:inline
func reclaimFrame(f *wsFrame) {
	// Move the start position forward to reclaim space
	if wsStart < f.End {
		wsStart = f.End
		wsLen -= (f.End - wsStart)
	}
}
