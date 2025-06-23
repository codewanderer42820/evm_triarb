// ws_io.go — raw I/O helpers sitting directly on the TCP/TLS socket.
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"unsafe"
)

// ───────────────────────── handshake helpers ──────────────────────────────

// hsBuf/hsTerm are static workspace for reading the HTTP upgrade response.
var (
	hsBuf  [4096]byte                       // 4 KiB scratch
	hsTerm = []byte{'\r', '\n', '\r', '\n'} // "\r\n\r\n"
)

// readHandshake fills hsBuf until the CRLF-CRLF terminator is seen.
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

// ───────────────────── streaming frame decoder ────────────────────────────

// ensureRoom guarantees wsLen ≥ need by reading from the socket, compacting
// the circular buffer when necessary.
func ensureRoom(conn net.Conn, need int) error {
	// hard cap — reject frames bigger than the buffer itself
	if need > len(wsBuf) {
		return fmt.Errorf("frame %d exceeds wsBuf capacity %d", need, len(wsBuf))
	}
	for wsLen < need {
		// out of capacity? — compact in-place
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

// readFrame decodes the next WebSocket data frame **in-place**.
// Control frames are skipped. Fragmentation is rejected for simplicity.
func readFrame(conn net.Conn) (*wsFrame, error) {
	for {
		if err := ensureRoom(conn, 2); err != nil {
			return nil, err
		}
		hdr0 := wsBuf[wsStart]
		hdr1 := wsBuf[wsStart+1]

		fin := hdr0 & 0x80
		opcode := hdr0 & 0x0F
		masked := hdr1 & 0x80
		plen7 := int(hdr1 & 0x7F)

		// skip control frames
		switch opcode {
		case 0x8: // CLOSE
			return nil, io.EOF
		case 0x9, 0xA: // PING / PONG
			wsStart += 2
			wsLen -= 2
			continue
		}

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

		// masking key
		var mkey uint32
		if masked != 0 {
			if err := ensureRoom(conn, offset+4); err != nil {
				return nil, err
			}
			mkey = *(*uint32)(unsafe.Pointer(&wsBuf[wsStart+offset]))
			offset += 4
		}

		// payload
		if err := ensureRoom(conn, offset+plen); err != nil {
			return nil, err
		}
		payloadStart := wsStart + offset
		payloadEnd := payloadStart + plen

		// unmask in-place (branch-free)
		if masked != 0 {
			key := [4]byte{}
			*(*uint32)(unsafe.Pointer(&key[0])) = mkey
			for i := 0; i < plen; i++ {
				wsBuf[payloadStart+i] ^= key[i&3]
			}
		}

		if fin == 0 {
			return nil, fmt.Errorf("fragmented frames not supported")
		}

		// register frame view in the ring
		idx := wsHead & (frameCap - 1)
		f := &wsFrames[idx]
		f.Payload = wsBuf[payloadStart:payloadEnd]
		f.Len = plen
		f.End = payloadEnd
		wsHead++
		return f, nil
	}
}
