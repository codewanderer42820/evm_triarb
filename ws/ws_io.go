// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: ws_io.go — Ultra-lean ISR-grade zero-alloc WebSocket frame I/O
//
// Purpose:
//   - Reads and parses RFC 6455 WebSocket frames from a raw TCP connection
//   - Single-frame streaming parser with eliminated allocations
//   - Optimized for maximum throughput with minimal overhead
//
// Notes:
//   - Error codes instead of error objects for critical paths
//   - Aggressive inlining and optimized buffer management
//   - Direct streaming processing with no frame buffering
//
// ⚠️ Caller MUST process frame immediately — no retention allowed
// ─────────────────────────────────────────────────────────────────────────────

package ws

import (
	"encoding/binary"
	"io"
	"main/constants"
	"main/debug"
	"net"
	"unsafe"
)

// Error codes (no allocation)
const (
	ErrHandshakeOverflow = iota + 1
	ErrFrameExceedsBuffer
	ErrFragmentedFrame
	ErrFrameExceedsMaxSize
	ErrPongResponseFailed
)

// Ultra-lean error for critical paths only
var criticalErr = &wsError{msg: "critical error"}

type wsError struct {
	msg string
}

func (e *wsError) Error() string { return e.msg }

var hsBuf [4096]byte // 4 KiB temporary buffer for HTTP headers

// ReadHandshake reads until HTTP upgrade response is complete.
//
//go:nosplit
//go:inline
//go:registerparams
func ReadHandshake(c net.Conn) ([]byte, error) {
	n := 0
	for {
		if n == len(hsBuf) {
			return nil, criticalErr
		}
		m, err := c.Read(hsBuf[n:])
		if err != nil {
			debug.DropError("ReadHandshake", err)
			return nil, err
		}
		n += m
		if findTerminator(hsBuf[:n]) >= 0 {
			return hsBuf[:n], nil
		}
	}
}

// findTerminator searches for CRLF-CRLF using architecture-specific pattern.
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

// ensureRoom guarantees ≥ `need` bytes in `wsBuf` with aggressive compaction.
//
//go:nosplit
//go:inline
//go:registerparams
func ensureRoom(conn net.Conn, need int) error {
	if need > len(wsBuf) {
		return criticalErr
	}

	for wsLen < need {
		// Aggressive compaction: compact when wsStart > quarter buffer
		if wsStart > len(wsBuf)/4 || wsStart+wsLen == len(wsBuf) {
			if wsLen > 0 {
				copy(wsBuf[:wsLen], wsBuf[wsStart:wsStart+wsLen])
			}
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

// ReadFrame parses a single complete WebSocket frame from the stream.
// Ultra-lean: single frame reuse, no ring buffer overhead.
//
//go:nosplit
//go:inline
//go:registerparams
func ReadFrame(conn net.Conn) (*wsFrame, error) {
	for {
		// Inline buffer check for critical 2-byte header
		if wsLen < 2 {
			if err := ensureRoom(conn, 2); err != nil {
				return nil, err
			}
		}

		hdr0 := wsBuf[wsStart]
		hdr1 := wsBuf[wsStart+1]
		fin := hdr0 & 0x80
		opcode := hdr0 & 0x0F
		masked := hdr1 & 0x80
		plen7 := int(hdr1 & 0x7F)

		// Handle control frames with minimal overhead
		switch opcode {
		case 0x8: // CLOSE
			return nil, io.EOF
		case 0x9: // PING - respond immediately
			wsStart += 2
			wsLen -= 2
			if _, err := conn.Write(pongFrame[:]); err != nil {
				return nil, criticalErr
			}
			continue
		case 0xA: // PONG - ignore
			wsStart += 2
			wsLen -= 2
			continue
		}

		// Decode payload length with optimized branching
		offset := 2
		var plen int
		if plen7 < 126 {
			plen = plen7
		} else if plen7 == 126 {
			if err := ensureRoom(conn, offset+2); err != nil {
				return nil, err
			}
			plen = int(binary.BigEndian.Uint16(wsBuf[wsStart+offset:]))
			offset += 2
		} else {
			if err := ensureRoom(conn, offset+8); err != nil {
				return nil, err
			}
			plen64 := binary.BigEndian.Uint64(wsBuf[wsStart+offset:])
			if plen64 > constants.MaxFrameSize {
				return nil, criticalErr
			}
			plen = int(plen64)
			offset += 8
		}

		// Read masking key if present
		var mkey uint32
		if masked != 0 {
			if err := ensureRoom(conn, offset+4); err != nil {
				return nil, err
			}
			mkey = *(*uint32)(unsafe.Pointer(&wsBuf[wsStart+offset]))
			offset += 4
		}

		// Ensure payload is available
		if err := ensureRoom(conn, offset+plen); err != nil {
			return nil, err
		}

		payloadStart := wsStart + offset
		payloadEnd := payloadStart + plen

		// Unmask payload in-place
		if masked != 0 {
			unmaskPayloadFast(wsBuf[payloadStart:payloadEnd], mkey)
		}

		// Reject fragmented frames
		if fin == 0 {
			return nil, criticalErr
		}

		// Reuse single frame struct
		currentFrame.PayloadPtr = unsafe.Pointer(&wsBuf[payloadStart])
		currentFrame.Len = plen
		currentFrame.End = payloadEnd

		// Advance buffer position
		totalFrameSize := offset + plen
		wsStart += totalFrameSize
		wsLen -= totalFrameSize

		return &currentFrame, nil
	}
}

// unmaskPayloadFast - ultra-optimized unmasking with 32-byte unrolling
//
//go:nosplit
//go:inline
//go:registerparams
func unmaskPayloadFast(payload []byte, maskKey uint32) {
	if len(payload) == 0 {
		return
	}

	// Create 8-byte mask pattern
	maskPattern := uint64(maskKey) | (uint64(maskKey) << 32)
	mask := *(*[4]byte)(unsafe.Pointer(&maskKey))

	i := 0
	// Unroll 32 bytes at once for large payloads
	for i+31 < len(payload) {
		dataPtr1 := (*uint64)(unsafe.Pointer(&payload[i]))
		dataPtr2 := (*uint64)(unsafe.Pointer(&payload[i+8]))
		dataPtr3 := (*uint64)(unsafe.Pointer(&payload[i+16]))
		dataPtr4 := (*uint64)(unsafe.Pointer(&payload[i+24]))
		*dataPtr1 ^= maskPattern
		*dataPtr2 ^= maskPattern
		*dataPtr3 ^= maskPattern
		*dataPtr4 ^= maskPattern
		i += 32
	}

	// Handle remaining 8-byte chunks
	for i+7 < len(payload) {
		*(*uint64)(unsafe.Pointer(&payload[i])) ^= maskPattern
		i += 8
	}

	// Handle remaining bytes
	for i < len(payload) {
		payload[i] ^= mask[i&3]
		i++
	}
}

// ReclaimFrame is now a no-op since we advance buffer position immediately
//
//go:nosplit
//go:inline
//go:registerparams
func ReclaimFrame(f *wsFrame) {
	// No-op: buffer position already advanced in ReadFrame
}
