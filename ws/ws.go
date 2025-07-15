// ws.go — WebSocket Client Library for High-Throughput Message Processing
//
// This library implements a specialized WebSocket client optimized for processing
// high-volume message streams from Ethereum nodes. It provides minimal-overhead
// frame parsing, efficient handshake negotiation, and direct memory management
// to achieve consistent low-latency message delivery.
//
// Architecture: Single-threaded design, global buffer management, zero-allocation frame parsing
// Protocol: WebSocket RFC 6455 compliant with selective feature implementation
// Memory Model: Pre-allocated 128MB buffer, unsafe pointer operations, manual frame assembly

package ws

import (
	"errors"
	"main/constants"
	"net"
	"unsafe"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PROTOCOL CONSTANTS AND TEMPLATES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Pre-computed WebSocket protocol strings for efficient connection establishment.
// These templates are built at compile time to eliminate runtime string concatenation.
const upgradeRequestTemplate = "GET " + constants.WsPath + " HTTP/1.1\r\n" +
	"Host: " + constants.WsHost + "\r\n" +
	"Upgrade: websocket\r\n" +
	"Connection: Upgrade\r\n" +
	"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
	"Sec-WebSocket-Version: 13\r\n\r\n"

// subscribePayload contains the JSON-RPC request for Ethereum log subscriptions.
// This subscribes to all contract events without filtering for maximum throughput.
const subscribePayload = `{"jsonrpc":"2.0","method":"eth_subscribe","params":["logs",{}],"id":1}`

// Compile-time length calculations eliminate runtime overhead
const upgradeRequestLen = len(upgradeRequestTemplate)
const subscribePayloadLen = len(subscribePayload)
const subscribeFrameLen = 8 + subscribePayloadLen // Frame header (8 bytes) + payload

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ERROR DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

//go:notinheap
//go:align 64
var (
	// Pre-allocated error instances to avoid heap allocations during error handling.
	// These are created once at startup and reused throughout the application lifetime.
	errUpgradeFailed    = errors.New("upgrade failed")
	errHandshakeTimeout = errors.New("handshake timeout")
	errFrameTooLarge    = errors.New("frame too large")
	errMessageTooLarge  = errors.New("message too large")
	errBoundsViolation  = errors.New("bounds violation")
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE PROCESSOR STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// WebSocketProcessor maintains all state required for WebSocket message processing.
// The structure is cache-aligned and organized to minimize memory access patterns.
//
//go:notinheap
//go:align 64
type WebSocketProcessor struct {
	// Hot: Main message buffer for accumulating WebSocket frames
	// Aligned to 16KB page boundaries for optimal memory access on modern CPUs
	//go:align 16384
	buffer [constants.BufferSize]byte // 128MB pre-allocated buffer

	// Cold: Pre-built protocol frames used only during connection establishment
	upgradeRequest [256]byte // HTTP upgrade request with headers
	subscribeFrame [128]byte // WebSocket frame containing subscription request
}

// Global processor instance eliminates allocation overhead and pointer indirection.
// The page alignment ensures optimal CPU cache line usage during frame processing.
//
//go:notinheap
//go:align 16384
var processor WebSocketProcessor

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// init constructs pre-computed protocol frames during program initialization.
// This eliminates all runtime frame construction overhead.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func init() {
	// Copy the HTTP upgrade request template into the processor's buffer
	copy(processor.upgradeRequest[:], upgradeRequestTemplate)

	// Construct the WebSocket frame for subscription requests
	// Frame format: [FIN|RSV|OPCODE] [MASK|LENGTH] [EXTENDED_LENGTH] [MASK_KEY] [PAYLOAD]
	processor.subscribeFrame[0] = 0x81                           // FIN=1, RSV=0, OPCODE=1 (text frame)
	processor.subscribeFrame[1] = 0x80 | 126                     // MASK=1, LENGTH=126 (16-bit extended)
	processor.subscribeFrame[2] = byte(subscribePayloadLen >> 8) // Length high byte
	processor.subscribeFrame[3] = byte(subscribePayloadLen)      // Length low byte

	// Static masking key (acceptable for client-to-server frames)
	processor.subscribeFrame[4] = 0x12
	processor.subscribeFrame[5] = 0x34
	processor.subscribeFrame[6] = 0x56
	processor.subscribeFrame[7] = 0x78

	// Apply XOR masking to the payload as required by WebSocket protocol
	// Each payload byte is XORed with the corresponding mask key byte
	for i := 0; i < subscribePayloadLen; i++ {
		processor.subscribeFrame[8+i] = subscribePayload[i] ^ processor.subscribeFrame[4+(i&3)]
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CONNECTION ESTABLISHMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Handshake performs the WebSocket upgrade protocol with minimal overhead.
// The function validates only essential response elements to minimize processing time.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Handshake(conn net.Conn) error {
	// Send the pre-computed upgrade request in a single write operation
	_, err := conn.Write(processor.upgradeRequest[:upgradeRequestLen])
	if err != nil {
		return err
	}

	// Read the HTTP response into a stack-allocated buffer
	var buf [constants.HandshakeBufferSize]byte
	total := 0

	// Continue reading until we find the end of HTTP headers or exceed limits
	for total < 500 {
		n, err := conn.Read(buf[total:])
		if err != nil {
			return err
		}
		total += n

		// Scan for the HTTP header terminator sequence "\r\n\r\n"
		// We start checking once we have at least 16 bytes to avoid bounds checks
		if total >= 16 {
			end := total - 3
			for i := 0; i < end; i++ {
				// Check for CRLF sequence using direct memory comparison
				// The pattern 0x0A0D0A0D represents "\r\n\r\n" in little-endian
				if *(*uint32)(unsafe.Pointer(&buf[i])) == 0x0A0D0A0D {
					// Validate the HTTP status line for successful upgrade
					// Expected format: "HTTP/1.1 101 Switching Protocols"
					if *(*uint64)(unsafe.Pointer(&buf[0])) == 0x312E312F50545448 && // "HTTP/1.1"
						buf[8] == ' ' && buf[9] == '1' && buf[10] == '0' && buf[11] == '1' {
						return nil // Successful WebSocket upgrade
					}
					return errUpgradeFailed
				}
			}
		}
	}
	return errHandshakeTimeout
}

// SendSubscription transmits the pre-built subscription frame to establish event streaming.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SendSubscription(conn net.Conn) error {
	// Write the entire pre-computed subscription frame in one operation
	_, err := conn.Write(processor.subscribeFrame[:subscribeFrameLen])
	return err
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// FRAME PROCESSING ENGINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// SpinUntilCompleteMessage accumulates WebSocket frames until a complete message is received.
// This function handles frame fragmentation, control frames, and variable-length encoding.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func SpinUntilCompleteMessage(conn net.Conn) ([]byte, error) {
	msgEnd := 0           // Current position in the message buffer
	var payloadLen uint64 // Length of the current frame's payload
	var opcode uint8      // WebSocket opcode for frame type identification

	for {
		// Verify buffer capacity before reading frame header
		if msgEnd > constants.BufferSize-constants.MaxFrameHeaderSize {
			return nil, errMessageTooLarge
		}

		// Create a slice view into the buffer for this frame's data
		headerBuf := processor.buffer[msgEnd:]

		// Read the minimal 2-byte WebSocket frame header
		// Format: [FIN|RSV|OPCODE] [MASK|PAYLOAD_LENGTH]
		_, err := conn.Read(headerBuf[:2])
		if err != nil {
			return nil, err
		}

		// Extract frame metadata from the header bytes
		opcode = headerBuf[0] & 0x0F             // Lower 4 bits contain the opcode
		payloadLen = uint64(headerBuf[1] & 0x7F) // Lower 7 bits contain base length

		// Handle extended payload length encoding as per WebSocket specification
		switch payloadLen {
		case 126:
			// 16-bit extended payload length follows the base header
			_, err = conn.Read(headerBuf[2:4])
			if err != nil {
				return nil, err
			}
			// Combine the two bytes into a 16-bit length value
			payloadLen = uint64(headerBuf[2])<<8 | uint64(headerBuf[3])

		case 127:
			// 64-bit extended payload length follows the base header
			_, err = conn.Read(headerBuf[2:10])
			if err != nil {
				return nil, err
			}

			// Convert 8 bytes from network byte order to host byte order
			// This performs big-endian to little-endian conversion
			v := *(*uint64)(unsafe.Pointer(&headerBuf[2]))
			payloadLen = ((v & 0xFF) << 56) | ((v & 0xFF00) << 40) | ((v & 0xFF0000) << 24) | ((v & 0xFF000000) << 8) |
				((v & 0xFF00000000) >> 8) | ((v & 0xFF0000000000) >> 24) | ((v & 0xFF000000000000) >> 40) | ((v & 0xFF00000000000000) >> 56)
		}

		// Validate frame size against buffer capacity
		if payloadLen >= uint64(constants.BufferSize) {
			return nil, errFrameTooLarge
		}

		// Process control frames (ping, pong, close) by discarding their payload
		// Control frames have the 4th bit of the opcode set
		isControlFrame := (opcode >> 3) & 1
		if isControlFrame != 0 {
			if payloadLen > 0 {
				// Read and discard control frame payload in small chunks
				for remaining := payloadLen; remaining > 0; {
					toRead := remaining
					if toRead > 16 {
						toRead = 16 // Limit read size for control frames
					}
					bytesRead, err := conn.Read(headerBuf[:toRead])
					if err != nil {
						return nil, err
					}
					remaining -= uint64(bytesRead)
				}
			}
			continue // Skip to next frame
		}

		// Verify the complete message will fit in the buffer
		if uint64(msgEnd)+payloadLen > uint64(constants.BufferSize) {
			return nil, errMessageTooLarge
		}

		// Check if this is the final frame of the message
		isLastFrame := headerBuf[0]&0x80 != 0

		// Read the frame payload into the message buffer
		remaining := payloadLen
		for remaining > 0 {
			// Calculate optimal read size for this iteration
			toRead := remaining
			if toRead > uint64(constants.BufferSize-msgEnd) {
				toRead = uint64(constants.BufferSize - msgEnd)
			}
			if toRead > 65536 {
				toRead = 65536 // Limit individual reads to 64KB for efficiency
			}

			// Read payload data directly into the message buffer
			bytesRead, err := conn.Read(processor.buffer[msgEnd : msgEnd+int(toRead)])
			if err != nil {
				return nil, err
			}

			msgEnd += bytesRead
			remaining -= uint64(bytesRead)
		}

		// Return the complete message if this was the final frame
		if isLastFrame {
			// Perform final bounds validation
			if msgEnd > constants.BufferSize {
				return nil, errBoundsViolation
			}
			return processor.buffer[:msgEnd], nil
		}
		// Continue to next frame if message is fragmented
	}
}
