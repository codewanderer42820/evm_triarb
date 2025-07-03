// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: ws_io.go — ISR-grade WebSocket frame I/O and buffer decoding
//
// Purpose:
//   - Reads and parses RFC 6455 WebSocket frames from a raw TCP connection
//   - Performs zero-alloc payload extraction and mask unwrapping
//
// Notes:
//   - Supports only FIN=true, unfragmented, masked data frames (Infura style)
//   - Buffer reuse guarantees zero heap pressure during all log ingestion
//   - Frame ring (`wsFrames`) is used to queue zero-copy views into `wsBuf`
//
// ⚠️ Caller MUST not retain frame.Payload past next wsBuf overwrite
// ─────────────────────────────────────────────────────────────────────────────

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"unsafe"
)

// ────────────────────────── Predefined Error Objects ─────────────────────────────

// Generic error for overflow during handshake
var ErrHandshakeOverflow = fmt.Errorf("handshake header overflow")

// Generic error for frame exceeding buffer capacity
var ErrFrameExceedsBuffer = fmt.Errorf("frame exceeds buffer capacity")

// Generic error for unsupported fragmented WebSocket frames
var ErrFragmentedFrame = fmt.Errorf("fragmented frames not supported")

// Generic error for exceeding maximum allowed frame size
var ErrFrameExceedsMaxSize = fmt.Errorf("frame exceeds maximum size")

// Generic error for failure to respond to Pong frame
var ErrPongResponseFailed = fmt.Errorf("failed to respond with pong")

// ────────────────────────── Handshake Parsing ─────────────────────────────

var (
	hsBuf  [4096]byte                       // 4 KiB temporary buffer for HTTP headers
	hsTerm = []byte{'\r', '\n', '\r', '\n'} // CRLF–CRLF delimiter to mark end of HTTP header
)

// readHandshake reads until HTTP upgrade response is complete.
//
// This function reads the WebSocket handshake response from the server and ensures that it follows
// the expected HTTP protocol for WebSocket connection upgrade.
//
//go:nosplit
//go:inline
//go:registerparams
func readHandshake(c net.Conn) ([]byte, error) {
	n := 0
	for {
		if n == len(hsBuf) {
			// If the buffer is full, return an error indicating that the header size exceeded the limit
			return nil, ErrHandshakeOverflow
		}
		m, err := c.Read(hsBuf[n:])
		if err != nil {
			// Log and return the error if the read operation fails
			dropError("readHandshake", err)
			return nil, err
		}
		n += m
		// Look for the CRLF-CRLF delimiter to mark the end of the handshake response
		if bytes.Index(hsBuf[:n], hsTerm) >= 0 {
			return hsBuf[:n], nil
		}
	}
}

// ────────────────────── Buffer Compaction Helper ──────────────────────────

// ensureRoom guarantees ≥ `need` bytes in `wsBuf`, refilling from conn.
//
// This function ensures that the WebSocket buffer has enough space to store a frame's data.
// It reads from the connection if needed and compacts the buffer if necessary to maintain low memory pressure.
//
//go:nosplit
//go:inline
//go:registerparams
func ensureRoom(conn net.Conn, need int) error {
	if need > len(wsBuf) {
		// If the required bytes exceed the buffer capacity, return an error
		return ErrFrameExceedsBuffer
	}

	for wsLen < need {
		// Compact if necessary: if the buffer is full, move data to the beginning
		if wsStart+wsLen == len(wsBuf) {
			copy(wsBuf[0:], wsBuf[wsStart:wsStart+wsLen])
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

// Pre-allocated Pong frame for responding to Ping frames
var pongFrame = []byte{0x8A, 0x00} // Pong frame (FIN=1, Opcode=0xA)

// readFrame parses a single complete WebSocket frame from the stream.
//
// This function decodes a WebSocket frame by processing its header, payload length, and mask, if present.
// It then stores the frame in the frame ring for further processing.
//
//go:nosplit
//go:inline
//go:registerparams
func readFrame(conn net.Conn) (*wsFrame, error) {
	for {
		// Step 1: Minimal header (2 bytes) — Read the WebSocket frame header
		if err := ensureRoom(conn, 2); err != nil {
			return nil, err
		}
		hdr0 := wsBuf[wsStart]
		hdr1 := wsBuf[wsStart+1]

		fin := hdr0 & 0x80
		opcode := hdr0 & 0x0F
		masked := hdr1 & 0x80
		plen7 := int(hdr1 & 0x7F)

		// Step 2: Handle special control frames: Ping and Pong
		switch opcode {
		case 0x8: // CLOSE frame
			// Return EOF to signal closure
			return nil, io.EOF
		case 0x9: // PING frame (opcode 0x9)
			// Handle Ping frame by responding with Pong
			wsStart += 2
			wsLen -= 2
			// Respond to ping with pong frame
			_, err := conn.Write(pongFrame)
			if err != nil {
				return nil, ErrPongResponseFailed
			}
			continue
		case 0xA: // PONG frame (opcode 0xA)
			// Just skip the Pong frame and continue to the next frame
			wsStart += 2
			wsLen -= 2
			continue
		}

		// Step 3: Decode payload length
		offset := 2
		var plen int
		switch plen7 {
		case 126:
			// 16-bit extended length
			if err := ensureRoom(conn, offset+2); err != nil {
				return nil, err
			}
			plen = int(binary.BigEndian.Uint16(wsBuf[wsStart+offset:]))
			offset += 2
		case 127:
			// 64-bit extended length
			if err := ensureRoom(conn, offset+8); err != nil {
				return nil, err
			}
			plen64 := binary.BigEndian.Uint64(wsBuf[wsStart+offset:])
			if plen64 > maxFrameSize {
				return nil, ErrFrameExceedsMaxSize
			}
			plen = int(plen64)
			offset += 8
		default:
			// 7-bit length
			plen = plen7
		}

		// Step 4: Read masking key
		var mkey uint32
		if masked != 0 {
			// If the frame is masked, read the 4-byte masking key
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

		// Step 6: Unmask the payload if needed
		if masked != 0 {
			key := [4]byte{}
			*(*uint32)(unsafe.Pointer(&key[0])) = mkey
			for i := 0; i < plen; i++ {
				wsBuf[payloadStart+i] ^= key[i&3] // Apply the mask to each byte
			}
		}

		// Step 7: Reject fragmented frames
		if fin == 0 {
			return nil, ErrFragmentedFrame
		}

		// Step 8: Store the parsed frame in the ring
		idx := wsHead & (frameCap - 1)
		f := &wsFrames[idx]
		f.Payload = wsBuf[payloadStart:payloadEnd]
		f.Len = plen
		f.End = payloadEnd
		wsHead++
		return f, nil
	}
}
