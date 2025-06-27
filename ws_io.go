// ws_io.go — raw I/O helpers for the WebSocket transport.
// Handles buffer management, handshake parsing, and zero-copy frame decoding.

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

// hsBuf is a temporary 4 KiB buffer used to read the WebSocket upgrade response.
// hsTerm is the CRLF–CRLF delimiter that terminates the HTTP header.
var (
	hsBuf  [4096]byte                       // 4 KiB scratch space
	hsTerm = []byte{'\r', '\n', '\r', '\n'} // "\r\n\r\n"
)

// readHandshake reads from the socket until the HTTP upgrade response is complete.
// It returns the raw header bytes or an error if the header overflows or fails.
//
//go:nosplit
func readHandshake(c net.Conn) ([]byte, error) {
	n := 0
	for {
		if n == len(hsBuf) {
			err := fmt.Errorf("handshake overflow: header exceeds %d bytes", len(hsBuf))
			dropError("handshake overflow", err)
			return nil, err
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

// ───────────────────────────── Frame Decoder ──────────────────────────────

// ensureRoom guarantees at least `need` bytes are readable in `wsBuf`.
// It compacts the buffer in-place if needed and refills via conn.Read().
func ensureRoom(conn net.Conn, need int) error {
	// Reject frames larger than the buffer
	if need > len(wsBuf) {
		return fmt.Errorf("frame %d exceeds wsBuf capacity %d", need, len(wsBuf))
	}

	// Refill until buffer holds `need` bytes
	for wsLen < need {
		// Compact buffer to start if we hit the end
		if wsStart+wsLen == len(wsBuf) {
			copy(wsBuf[0:wsLen], wsBuf[wsStart:wsStart+wsLen])
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

// readFrame parses the next WebSocket frame from the TCP stream.
// It supports only non-fragmented, masked, data frames (as per Infura).
// Control frames (PING, PONG, CLOSE) are skipped.
//
//go:nosplit
func readFrame(conn net.Conn) (*wsFrame, error) {
	for {
		// ───── Step 1: Minimal 2-byte header ─────
		if err := ensureRoom(conn, 2); err != nil {
			return nil, err
		}
		hdr0 := wsBuf[wsStart]
		hdr1 := wsBuf[wsStart+1]

		fin := hdr0 & 0x80    // final frame flag
		opcode := hdr0 & 0x0F // 0x1 = text, 0x2 = binary, 0x9 = ping, etc.
		masked := hdr1 & 0x80 // must be set for client → server
		plen7 := int(hdr1 & 0x7F)

		// ───── Step 2: Skip control frames ─────
		switch opcode {
		case 0x8: // CLOSE
			return nil, io.EOF
		case 0x9, 0xA: // PING / PONG
			wsStart += 2
			wsLen -= 2
			continue
		}

		// ───── Step 3: Decode payload length ─────
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

		// ───── Step 4: Extract masking key ─────
		var mkey uint32
		if masked != 0 {
			if err := ensureRoom(conn, offset+4); err != nil {
				return nil, err
			}
			mkey = *(*uint32)(unsafe.Pointer(&wsBuf[wsStart+offset]))
			offset += 4
		}

		// ───── Step 5: Read payload ─────
		if err := ensureRoom(conn, offset+plen); err != nil {
			return nil, err
		}
		payloadStart := wsStart + offset
		payloadEnd := payloadStart + plen

		// ───── Step 6: Unmask payload (RFC 6455) ─────
		if masked != 0 {
			key := [4]byte{}
			*(*uint32)(unsafe.Pointer(&key[0])) = mkey
			for i := 0; i < plen; i++ {
				wsBuf[payloadStart+i] ^= key[i&3]
			}
		}

		// Fragmentation not supported
		if fin == 0 {
			return nil, fmt.Errorf("fragmented frames not supported")
		}

		// ───── Step 7: Register frame in ring ─────
		idx := wsHead & (frameCap - 1)
		f := &wsFrames[idx]
		f.Payload = wsBuf[payloadStart:payloadEnd]
		f.Len = plen
		f.End = payloadEnd
		wsHead++
		return f, nil
	}
}
