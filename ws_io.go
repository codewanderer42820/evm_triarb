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

// ────────────────────────── Handshake Parsing ─────────────────────────────

var (
	hsBuf  [4096]byte                       // 4 KiB temporary buffer for HTTP headers
	hsTerm = []byte{'\r', '\n', '\r', '\n'} // CRLF–CRLF delimiter
)

// readHandshake reads until HTTP upgrade response is complete.
//
//go:nosplit
//go:inline
//go:registerparams
func readHandshake(c net.Conn) ([]byte, error) {
	n := 0
	for {
		if n == len(hsBuf) {
			return nil, fmt.Errorf("handshake overflow: header exceeds %d bytes", len(hsBuf))
		}
		m, err := c.Read(hsBuf[n:])
		if err != nil {
			dropError("readHandshake", err)
			return nil, err
		}
		n += m
		if bytes.Index(hsBuf[:n], hsTerm) >= 0 {
			return hsBuf[:n], nil
		}
	}
}

// ────────────────────── Buffer Compaction Helper ──────────────────────────

// ensureRoom guarantees ≥ `need` bytes in `wsBuf`, refilling from conn.
//
//go:nosplit
//go:inline
//go:registerparams
func ensureRoom(conn net.Conn, need int) error {
	if need > len(wsBuf) {
		return fmt.Errorf("frame %d exceeds wsBuf capacity %d", need, len(wsBuf))
	}

	for wsLen < need {
		// compact if needed
		if wsStart+wsLen == len(wsBuf) {
			copy(wsBuf[0:], wsBuf[wsStart:wsStart+wsLen])
			wsStart = 0
		}
		n, err := conn.Read(wsBuf[wsStart+wsLen:])
		if err != nil {
			return err
		}
		wsLen += n
	}
	return nil
}

// ───────────────────────────── Frame Decoder ──────────────────────────────

// readFrame parses a single complete WebSocket frame from stream.
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
		hdr0 := wsBuf[wsStart]
		hdr1 := wsBuf[wsStart+1]

		fin := hdr0 & 0x80
		opcode := hdr0 & 0x0F
		masked := hdr1 & 0x80
		plen7 := int(hdr1 & 0x7F)

		// Step 2: Handle control frames (skip)
		switch opcode {
		case 0x8:
			return nil, io.EOF // CLOSE
		case 0x9, 0xA:
			wsStart += 2
			wsLen -= 2
			continue // PING / PONG
		}

		// Step 3: Decode payload length
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
				return nil, fmt.Errorf("frame %d exceeds cap", plen64)
			}
			plen = int(plen64)
			offset += 8
		default:
			plen = plen7
		}

		// Step 4: Read masking key
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

		// Step 6: Unmask
		if masked != 0 {
			key := [4]byte{}
			*(*uint32)(unsafe.Pointer(&key[0])) = mkey
			for i := 0; i < plen; i++ {
				wsBuf[payloadStart+i] ^= key[i&3]
			}
		}

		// Step 7: Reject fragmented frames
		if fin == 0 {
			return nil, fmt.Errorf("fragmented frames not supported")
		}

		// Step 8: Store parsed frame in ring
		idx := wsHead & (frameCap - 1)
		f := &wsFrames[idx]
		f.Payload = wsBuf[payloadStart:payloadEnd]
		f.Len = plen
		f.End = payloadEnd
		wsHead++
		return f, nil
	}
}
