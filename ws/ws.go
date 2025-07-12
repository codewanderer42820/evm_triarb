package ws

import (
	"errors"
	"main/constants"
	"net"
	"unsafe"
)

// Pre-computed WebSocket protocol strings
const upgradeRequestTemplate = "GET " + constants.WsPath + " HTTP/1.1\r\n" +
	"Host: " + constants.WsHost + "\r\n" +
	"Upgrade: websocket\r\n" +
	"Connection: Upgrade\r\n" +
	"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
	"Sec-WebSocket-Version: 13\r\n\r\n"

const subscribePayload = `{"jsonrpc":"2.0","method":"eth_subscribe","params":["logs",{}],"id":1}`

// Compile-time length calculations
const upgradeRequestLen = len(upgradeRequestTemplate)
const subscribePayloadLen = len(subscribePayload)
const subscribeFrameLen = 8 + subscribePayloadLen

// Pre-allocated error instances
var (
	errUpgradeFailed    = errors.New("upgrade failed")
	errHandshakeTimeout = errors.New("handshake timeout")
	errFrameTooLarge    = errors.New("frame too large")
	errMessageTooLarge  = errors.New("message too large")
	errBoundsViolation  = errors.New("bounds violation")
)

// WebSocketProcessor - Cache-optimized message processor
//
//go:notinheap
//go:align 64
type WebSocketProcessor struct {
	// Hot: Main message buffer (16MB, page-aligned)
	//go:align 16384
	buffer [BufferSize]byte // 16MB

	// Cold: Pre-built protocol frames
	upgradeRequest [256]byte // HTTP upgrade
	subscribeFrame [128]byte // Subscribe frame
}

// Global processor instance
//
//go:notinheap
//go:align 64
var processor WebSocketProcessor

const BufferSize = 16777216     // 16MB buffer
const HandshakeBufferSize = 512 // Handshake buffer

// init builds pre-computed protocol frames
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func init() {
	// Copy upgrade request
	copy(processor.upgradeRequest[:], upgradeRequestTemplate)

	// Build WebSocket frame
	processor.subscribeFrame[0] = 0x81                           // FIN=1, TEXT
	processor.subscribeFrame[1] = 0x80 | 126                     // MASK=1, 16-bit len
	processor.subscribeFrame[2] = byte(subscribePayloadLen >> 8) // Length high
	processor.subscribeFrame[3] = byte(subscribePayloadLen)      // Length low

	// Masking key
	processor.subscribeFrame[4] = 0x12
	processor.subscribeFrame[5] = 0x34
	processor.subscribeFrame[6] = 0x56
	processor.subscribeFrame[7] = 0x78

	// Apply XOR masking
	for i := 0; i < subscribePayloadLen; i++ {
		processor.subscribeFrame[8+i] = subscribePayload[i] ^ processor.subscribeFrame[4+(i&3)]
	}
}

// Handshake performs WebSocket upgrade
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Handshake(conn net.Conn) error {
	// Send upgrade request
	_, err := conn.Write(processor.upgradeRequest[:upgradeRequestLen])
	if err != nil {
		return err
	}

	// Read response
	var buf [HandshakeBufferSize]byte
	total := 0

	for total < 500 {
		n, err := conn.Read(buf[total:])
		if err != nil {
			return err
		}
		total += n

		// Scan for \r\n\r\n
		if total >= 16 {
			end := total - 3
			for i := 0; i < end; i++ {
				if *(*uint32)(unsafe.Pointer(&buf[i])) == 0x0A0D0A0D {
					// Validate HTTP 101
					if *(*uint64)(unsafe.Pointer(&buf[0])) == 0x312E312F50545448 &&
						buf[8] == ' ' && buf[9] == '1' && buf[10] == '0' && buf[11] == '1' {
						return nil
					}
					return errUpgradeFailed
				}
			}
		}
	}
	return errHandshakeTimeout
}

// SendSubscription sends pre-built subscription frame
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SendSubscription(conn net.Conn) error {
	_, err := conn.Write(processor.subscribeFrame[:subscribeFrameLen])
	return err
}

// SpinUntilCompleteMessage processes frames until complete
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SpinUntilCompleteMessage(conn net.Conn) ([]byte, error) {
	msgEnd := 0
	var payloadLen uint64
	var opcode uint8

	for {
		headerBuf := processor.buffer[msgEnd:]

		// Read frame header
		_, err := conn.Read(headerBuf[:2])
		if err != nil {
			return nil, err
		}

		// Signal activity
		// control.SignalActivity()

		// Extract frame info
		opcode = headerBuf[0] & 0x0F
		payloadLen = uint64(headerBuf[1] & 0x7F)

		// Handle extended length
		switch payloadLen {
		case 126:
			_, err = conn.Read(headerBuf[2:4])
			if err != nil {
				return nil, err
			}
			payloadLen = uint64(headerBuf[2])<<8 | uint64(headerBuf[3])

		case 127:
			_, err = conn.Read(headerBuf[2:10])
			if err != nil {
				return nil, err
			}

			// Fast endian conversion
			v := *(*uint64)(unsafe.Pointer(&headerBuf[2]))
			payloadLen = ((v & 0xFF) << 56) | ((v & 0xFF00) << 40) | ((v & 0xFF0000) << 24) | ((v & 0xFF000000) << 8) |
				((v & 0xFF00000000) >> 8) | ((v & 0xFF0000000000) >> 24) | ((v & 0xFF000000000000) >> 40) | ((v & 0xFF00000000000000) >> 56)

			if payloadLen > uint64(BufferSize) {
				return nil, errFrameTooLarge
			}
		}

		// Handle control frames
		isControlFrame := (opcode >> 3) & 1
		if isControlFrame != 0 {
			if payloadLen > 0 {
				// Discard control payload
				for remaining := payloadLen; remaining > 0; {
					toRead := remaining
					if toRead > 16 {
						toRead = 16
					}
					bytesRead, err := conn.Read(headerBuf[:toRead])
					if err != nil {
						return nil, err
					}
					remaining -= uint64(bytesRead)
				}
			}
			continue
		}

		// Check buffer space
		if uint64(msgEnd)+payloadLen > uint64(BufferSize) {
			return nil, errMessageTooLarge
		}

		// Check FIN bit
		isLastFrame := headerBuf[0]&0x80 != 0

		// Read payload
		remaining := payloadLen
		for remaining > 0 {
			toRead := remaining
			if toRead > uint64(BufferSize-msgEnd) {
				toRead = uint64(BufferSize - msgEnd)
			}
			if toRead > 65536 {
				toRead = 65536 // 64KB chunks
			}

			bytesRead, err := conn.Read(processor.buffer[msgEnd : msgEnd+int(toRead)])
			if err != nil {
				return nil, err
			}

			msgEnd += bytesRead
			remaining -= uint64(bytesRead)
		}

		// Return if final frame
		if isLastFrame {
			if msgEnd > BufferSize {
				return nil, errBoundsViolation
			}
			return processor.buffer[:msgEnd], nil
		}
	}
}
