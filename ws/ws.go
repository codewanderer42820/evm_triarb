// ════════════════════════════════════════════════════════════════════════════════════════════════
// WebSocket Frame Processor
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: WebSocket Message Processor
//
// Description:
//   WebSocket client implementing RFC 6455 compliant frame parsing for processing
//   Ethereum event streams with minimal memory allocation.
//
// Features:
//   - Zero-allocation frame parsing
//   - Pre-allocated buffer pool
//   - Connection establishment and subscription management
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package ws

import (
	"errors"
	"main/constants"
	"main/utils"
	"net"
	"unsafe"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PROTOCOL CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Pre-computed WebSocket protocol strings for connection establishment.
const upgradeRequestTemplate = "GET " + constants.WsPath + " HTTP/1.1\r\n" +
	"Host: " + constants.WsHost + "\r\n" +
	"Upgrade: websocket\r\n" +
	"Connection: Upgrade\r\n" +
	"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
	"Sec-WebSocket-Version: 13\r\n\r\n"

// JSON-RPC request for Ethereum log subscriptions.
const subscribePayload = `{"jsonrpc":"2.0","method":"eth_subscribe","params":["logs",{}],"id":1}`

// Pre-calculated lengths for buffer operations.
const upgradeRequestLen = len(upgradeRequestTemplate)
const subscribePayloadLen = len(subscribePayload)
const subscribeFrameLen = 8 + subscribePayloadLen

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ERROR DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// WebSocket error instances with cache line isolation to avoid heap allocations during
// error handling. Aligned to prevent false sharing with other global state and ensure
// predictable memory access patterns during exceptional conditions.
//
//go:notinheap
//go:align 64
var (
	// WebSocket protocol errors (accessed during connection failures)
	errUpgradeFailed    = errors.New("upgrade failed")    // Error for WebSocket upgrade failures
	errHandshakeTimeout = errors.New("handshake timeout") // Error for connection timeout
	errFrameTooLarge    = errors.New("frame too large")   // Error for oversized frames
	errMessageTooLarge  = errors.New("message too large") // Error for oversized messages
	errBoundsViolation  = errors.New("bounds violation")  // Error for buffer overflows
	_                   [24]byte                          // Padding to complete cache line
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE PROCESSOR STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// WebSocketProcessor maintains state required for WebSocket message processing.
// Structure is cache-aligned and organized to minimize memory access patterns.
//
//go:notinheap
//go:align 64
type WebSocketProcessor struct {
	// Main message buffer for accumulating WebSocket frames
	// Aligned to page boundaries for optimal memory access
	//go:align 16384
	buffer [constants.BufferSize]byte

	// Pre-built protocol frames for connection establishment
	upgradeRequest [256]byte
	subscribeFrame [128]byte
}

// WebSocket processor global instance with page alignment for optimal cache utilization.
// Page-aligned allocation ensures processor buffers don't cross page boundaries and
// maximizes memory bandwidth during frame processing operations.
//
//go:notinheap
//go:align 16384
var processor WebSocketProcessor

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// init constructs pre-computed protocol frames during program initialization.
// This eliminates runtime frame construction overhead.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func init() {
	// Copy HTTP upgrade request template
	copy(processor.upgradeRequest[:], upgradeRequestTemplate)

	// Construct WebSocket frame for subscription requests
	// Frame format: [FIN|RSV|OPCODE] [MASK|LENGTH] [EXTENDED_LENGTH] [MASK_KEY] [PAYLOAD]
	processor.subscribeFrame[0] = 0x81                           // FIN=1, RSV=0, OPCODE=1 (text frame)
	processor.subscribeFrame[1] = 0x80 | 126                     // MASK=1, LENGTH=126 (16-bit extended)
	processor.subscribeFrame[2] = byte(subscribePayloadLen >> 8) // Length high byte
	processor.subscribeFrame[3] = byte(subscribePayloadLen)      // Length low byte

	// Static masking key for client-to-server frames
	processor.subscribeFrame[4] = 0x12
	processor.subscribeFrame[5] = 0x34
	processor.subscribeFrame[6] = 0x56
	processor.subscribeFrame[7] = 0x78

	// Apply XOR masking to payload as required by WebSocket protocol
	for i := 0; i < subscribePayloadLen; i++ {
		processor.subscribeFrame[8+i] = subscribePayload[i] ^ processor.subscribeFrame[4+(i&3)]
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONNECTION ESTABLISHMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Handshake performs WebSocket upgrade protocol with minimal overhead.
// Validates essential response elements for successful connection establishment.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Handshake(conn net.Conn) error {
	// Send pre-computed upgrade request
	_, err := conn.Write(processor.upgradeRequest[:upgradeRequestLen])
	if err != nil {
		return err
	}

	// Read HTTP response
	var buf [constants.HandshakeBufferSize]byte
	total := 0

	// Continue reading until header terminator or limit exceeded
	for total < 500 {
		n, err := conn.Read(buf[total:])
		if err != nil {
			return err
		}
		total += n

		// Scan for HTTP header terminator sequence "\r\n\r\n"
		if total >= 16 {
			end := total - 3
			for i := 0; i < end; i++ {
				// Check for CRLF sequence using direct memory comparison
				if *(*uint32)(unsafe.Pointer(&buf[i])) == 0x0A0D0A0D {
					// Validate HTTP status line for successful upgrade
					if *(*uint64)(unsafe.Pointer(&buf[0])) == 0x312E312F50545448 && // "HTTP/1.1"
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

// SendSubscription transmits pre-built subscription frame to establish event streaming.
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

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// FRAME PROCESSING ENGINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// SpinUntilCompleteMessage accumulates WebSocket frames until a complete message is received.
// Handles frame fragmentation, control frames, and variable-length encoding.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SpinUntilCompleteMessage(conn net.Conn) ([]byte, error) {
	msgEnd := 0           // Current position in message buffer
	var payloadLen uint64 // Length of current frame's payload
	var opcode uint8      // WebSocket opcode for frame type identification

	for {
		// Verify buffer capacity before reading frame header
		if msgEnd > constants.BufferSize-constants.MaxFrameHeaderSize {
			return nil, errMessageTooLarge
		}

		// Create slice view into buffer for frame data
		headerBuf := processor.buffer[msgEnd:]

		// Read minimal 2-byte WebSocket frame header
		_, err := conn.Read(headerBuf[:2])
		if err != nil {
			return nil, err
		}

		// Extract frame metadata from header bytes
		opcode = headerBuf[0] & 0x0F             // Lower 4 bits contain opcode
		payloadLen = uint64(headerBuf[1] & 0x7F) // Lower 7 bits contain base length

		// Handle extended payload length encoding
		switch payloadLen {
		case 126:
			// 16-bit extended payload length
			_, err = conn.Read(headerBuf[2:4])
			if err != nil {
				return nil, err
			}
			payloadLen = uint64(headerBuf[2])<<8 | uint64(headerBuf[3])

		case 127:
			// 64-bit extended payload length
			_, err = conn.Read(headerBuf[2:10])
			if err != nil {
				return nil, err
			}

			// Convert from network byte order to host byte order
			payloadLen = utils.LoadBE64(headerBuf[2:10])
		}

		// Validate frame size against buffer capacity
		if payloadLen >= uint64(constants.BufferSize) {
			return nil, errFrameTooLarge
		}

		// Process control frames by discarding their payload
		isControlFrame := (opcode >> 3) & 1
		if isControlFrame != 0 {
			if payloadLen > 0 {
				// Read and discard control frame payload
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

		// Verify complete message will fit in buffer
		if uint64(msgEnd)+payloadLen > uint64(constants.BufferSize) {
			return nil, errMessageTooLarge
		}

		// Check if this is the final frame of the message
		isLastFrame := headerBuf[0]&0x80 != 0

		// Read frame payload into message buffer
		remaining := payloadLen
		for remaining > 0 {
			toRead := remaining
			if toRead > uint64(constants.BufferSize-msgEnd) {
				toRead = uint64(constants.BufferSize - msgEnd)
			}
			if toRead > 65536 {
				toRead = 65536
			}

			bytesRead, err := conn.Read(processor.buffer[msgEnd : msgEnd+int(toRead)])
			if err != nil {
				return nil, err
			}

			msgEnd += bytesRead
			remaining -= uint64(bytesRead)
		}

		// Return complete message if this was the final frame
		if isLastFrame {
			// Perform final bounds validation
			if msgEnd > constants.BufferSize {
				return nil, errBoundsViolation
			}
			return processor.buffer[:msgEnd], nil
		}
	}
}
