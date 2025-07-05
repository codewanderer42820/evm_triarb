// ─────────────────────────────────────────────────────────────────────────────
// Package ws: True Zero-Copy Zero-Allocation WebSocket Client for DEX Arbitrage
//
// Architecture: Zero-allocation, zero-copy, direct memory access frame processor
// Target: Sub-50ns frame processing latency on Apple Silicon M4 Pro
//
// Design Principles:
//   - TRUE ZERO-COPY: Direct pointers to network buffer, no payload copying
//   - TRUE ZERO-ALLOCATION: Single allocation at init, zero heap allocations during operation
//   - Cache-line aligned data structures for maximum memory bandwidth
//   - Branch predictor friendly hot paths with minimal conditional logic
//   - Aggressive compiler optimizations with manual loop unrolling
//   - Direct unsafe memory operations for maximum speed
//
// Performance Targets:
//   - Frame parsing: <25ns for typical 1KB DEX events
//   - Memory bandwidth: Direct access to L1 cache at full speed
//   - CPU utilization: <3% on dedicated core for 100K frames/sec
//
// CRITICAL ZERO-COPY CONTRACT:
//   - Payload data is ONLY valid until the next IngestFrame() call
//   - Must process payload immediately - cannot store pointers
//   - Buffer compaction will invalidate all previous payload pointers
//   - Single-threaded only - no concurrent access allowed
//
// CRITICAL SAFETY WARNINGS:
//   - Extensive unsafe pointer arithmetic - bounds checking disabled
//   - Manual memory management - buffer corruption will cause crashes
//   - Apple Silicon optimized - may underperform on other architectures
// ─────────────────────────────────────────────────────────────────────────────

package ws

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"io"
	"main/constants"
	"main/debug"
	"net"
	"unsafe"
)

// ───────────────────────────── Performance-Critical Constants ─────────────────────────────

const (
	// Buffer sizing for optimal cache behavior and zero-copy access
	RingBufSize  = 32768 // Primary ring buffer (32KB - fits in L1/L2 cache)
	HandshakeBuf = 1024  // HTTP handshake buffer

	// Performance thresholds for zero-copy operation
	CompactionThreshold = 24576 // Trigger compaction when readPos exceeds this (75% of buffer)
	FrameSizeLimit      = 8192  // Maximum single frame size we'll process
	MinReadSpace        = 4096  // Minimum space required before compaction

	// Error codes for ultra-fast path selection
	Success       = 0
	NeedMoreData  = 1
	CloseFrame    = 2
	PingFrame     = 3
	PongFrame     = 4
	OversizeFrame = 5
	FragmentFrame = 6
	ParseError    = 7
)

// ───────────────────────────── Zero-Copy Data Structures ─────────────────────────────

// Frame represents a parsed WebSocket frame with DIRECT zero-copy payload access
// WARNING: Payload is only valid until next IngestFrame() call
//
//go:notinheap
//go:align 64
type Frame struct {
	PayloadPtr unsafe.Pointer // DIRECT pointer to payload in ring buffer (ZERO-COPY)
	Size       int            // Payload size in bytes
	_          [56]byte       // Cache line padding
}

// RingBufferState contains all ring buffer state in a single cache line
//
//go:notinheap
//go:align 64
type RingBufferState struct {
	ReadCursor  int      // Current read position in ring buffer
	WriteCursor int      // Current write position in ring buffer
	DataSize    int      // Total valid data in ring buffer
	FrameCount  uint64   // Total frames processed (for debugging)
	_           [32]byte // Cache line padding
}

// ───────────────────────────── Global Zero-Allocation State ─────────────────────────────

// High-performance ring buffer - allocated once at init, never reallocated
//
//go:notinheap
//go:align 64
var ringBuffer [RingBufSize]byte

// HTTP handshake buffer - separate from main data path
//
//go:notinheap
//go:align 64
var handshakeBuffer [HandshakeBuf]byte

// Ring buffer state - single cache line for maximum access speed
//
//go:notinheap
//go:align 64
var ring RingBufferState

// Pre-built protocol packets - constructed once at init
//
//go:notinheap
//go:align 64
var protocolData struct {
	upgradeRequest  [256]byte // HTTP WebSocket upgrade request
	subscribePacket [96]byte  // WebSocket subscribe frame
	pongResponse    [2]byte   // WebSocket pong response
	upgradeSize     int       // Size of upgrade request
	subscribeSize   int       // Size of subscribe packet
	_               [32]byte  // Cache line padding
}

// Current frame instance - reused for zero allocation
//
//go:notinheap
//go:align 64
var currentFrame Frame

// ───────────────────────────── Initialization ─────────────────────────────

// init performs one-time initialization of all WebSocket protocol data
func init() {
	// Generate cryptographically secure WebSocket key
	var keyBytes [16]byte
	if _, err := rand.Read(keyBytes[:]); err != nil {
		panic("WebSocket key generation failed: " + err.Error())
	}

	var encodedKey [24]byte
	base64.StdEncoding.Encode(encodedKey[:], keyBytes[:])

	// Build HTTP upgrade request with zero allocations
	upgradeTemplate := [][]byte{
		[]byte("GET "), []byte(constants.WsPath), []byte(" HTTP/1.1\r\n"),
		[]byte("Host: "), []byte(constants.WsHost), []byte("\r\n"),
		[]byte("Upgrade: websocket\r\n"),
		[]byte("Connection: Upgrade\r\n"),
		[]byte("Sec-WebSocket-Key: "), encodedKey[:], []byte("\r\n"),
		[]byte("Sec-WebSocket-Version: 13\r\n\r\n"),
	}

	pos := 0
	for _, chunk := range upgradeTemplate {
		pos += copy(protocolData.upgradeRequest[pos:], chunk)
	}
	protocolData.upgradeSize = pos

	// Build pre-masked subscribe frame
	payload := []byte(`{"id":1,"method":"eth_subscribe","params":["logs",{}],"jsonrpc":"2.0"}`)

	// Generate masking key
	var maskKey [4]byte
	rand.Read(maskKey[:])

	// Construct WebSocket frame header
	protocolData.subscribePacket[0] = 0x81                      // FIN=1, TEXT frame
	protocolData.subscribePacket[1] = 0x80 | byte(len(payload)) // MASKED + payload length
	copy(protocolData.subscribePacket[2:6], maskKey[:])

	// Apply XOR masking to payload
	for i, b := range payload {
		protocolData.subscribePacket[6+i] = b ^ maskKey[i&3]
	}
	protocolData.subscribeSize = 6 + len(payload)

	// Build pong response frame
	protocolData.pongResponse[0] = 0x8A // FIN=1, PONG frame
	protocolData.pongResponse[1] = 0x00 // No payload

	debug.DropMessage("WebSocket", "zero-copy initialization complete")
}

// ───────────────────────────── True Zero-Copy Memory Operations ─────────────────────────────

// compactRingBuffer performs zero-copy buffer compaction with bulk memory move
// CRITICAL: This invalidates ALL existing payload pointers
//
//go:nosplit
//go:inline
//go:registerparams
func compactRingBuffer() {
	if ring.ReadCursor == 0 {
		return // Already compacted
	}

	remainingBytes := ring.DataSize - ring.ReadCursor
	if remainingBytes > 0 {
		// Use bulk memory move for maximum speed - this is the ONLY copy operation
		copy(ringBuffer[:remainingBytes], ringBuffer[ring.ReadCursor:ring.ReadCursor+remainingBytes])
	}

	// Reset cursors to beginning
	ring.WriteCursor = remainingBytes
	ring.ReadCursor = 0
	ring.DataSize = remainingBytes
}

// ingestNetworkData reads from network connection directly into ring buffer
// TRUE ZERO-COPY: Network data goes directly into ring buffer
//
//go:nosplit
//go:inline
//go:registerparams
func ingestNetworkData(conn net.Conn) bool {
	// Ensure buffer space is available for network ingestion
	availableSpace := RingBufSize - ring.WriteCursor
	if availableSpace < MinReadSpace {
		compactRingBuffer()
		availableSpace = RingBufSize - ring.WriteCursor
		if availableSpace < MinReadSpace {
			return false // Buffer completely full
		}
	}

	// Read directly into ring buffer - TRUE ZERO-COPY from network
	bytesRead, err := conn.Read(ringBuffer[ring.WriteCursor:])
	if err != nil {
		debug.DropError("network ingestion failed", err)
		return false
	}

	// Update cursors - no data copying involved
	ring.WriteCursor += bytesRead
	ring.DataSize += bytesRead

	return true
}

// ───────────────────────────── Hyper-Optimized Zero-Copy Frame Parser ─────────────────────────────

// parseFrameHeaderZeroCopy extracts WebSocket frame metadata with zero memory copying
// Returns DIRECT pointers into ring buffer for true zero-copy access
//
//go:nosplit
//go:inline
//go:registerparams
func parseFrameHeaderZeroCopy() (headerBytes, payloadBytes int, resultCode int) {
	dataAvailable := ring.DataSize - ring.ReadCursor
	if dataAvailable < 2 {
		return 0, 0, NeedMoreData
	}

	// Read frame header as single 16-bit operation for speed
	headerWord := *(*uint16)(unsafe.Pointer(&ringBuffer[ring.ReadCursor]))
	firstByte := byte(headerWord)
	secondByte := byte(headerWord >> 8)

	// Extract frame metadata with bit operations
	finFlag := firstByte & 0x80
	opcode := firstByte & 0x0F
	maskFlag := secondByte & 0x80
	initialLength := int(secondByte & 0x7F)

	// Fast path rejection for fragmented frames
	if finFlag == 0 || opcode == 0 {
		// Still need to calculate frame size to properly skip fragment
		// Continue with size calculation to skip entire fragment properly
	}

	// Control frame fast path
	if opcode >= 8 {
		switch opcode {
		case 0x8:
			return 2, 0, CloseFrame
		case 0x9:
			return 2, 0, PingFrame
		case 0xA:
			return 2, 0, PongFrame
		}
	}

	// Calculate total header size and payload length
	headerSize := 2
	var payloadSize int

	if initialLength < 126 {
		payloadSize = initialLength
	} else if initialLength == 126 {
		if dataAvailable < 4 {
			return 0, 0, NeedMoreData
		}
		payloadSize = int(binary.BigEndian.Uint16(ringBuffer[ring.ReadCursor+2:]))
		headerSize = 4
	} else {
		if dataAvailable < 10 {
			return 0, 0, NeedMoreData
		}
		length64 := binary.BigEndian.Uint64(ringBuffer[ring.ReadCursor+2:])
		if length64 > FrameSizeLimit {
			return 0, 0, OversizeFrame
		}
		payloadSize = int(length64)
		headerSize = 10
	}

	// Account for masking key
	if maskFlag != 0 {
		headerSize += 4
	}

	// Verify complete frame is available
	totalFrameSize := headerSize + payloadSize
	if dataAvailable < totalFrameSize {
		return 0, 0, NeedMoreData
	}

	// Check if this is a fragment after size calculation
	isFragment := (finFlag == 0 || opcode == 0)

	// Unmask payload IN-PLACE in ring buffer - no copying
	if maskFlag != 0 {
		maskKey := *(*uint32)(unsafe.Pointer(&ringBuffer[ring.ReadCursor+headerSize-4]))
		payloadStart := ring.ReadCursor + headerSize
		unmaskPayloadInPlace(ringBuffer[payloadStart:payloadStart+payloadSize], maskKey)
	}

	// Return appropriate result code
	if isFragment {
		return headerSize, payloadSize, FragmentFrame
	}

	return headerSize, payloadSize, Success
}

// unmaskPayloadInPlace performs in-place payload unmasking directly in ring buffer
// TRUE ZERO-COPY: Unmasks data in original location, no memory copying
//
//go:nosplit
//go:inline
//go:registerparams
func unmaskPayloadInPlace(payload []byte, maskKey uint32) {
	if len(payload) == 0 {
		return
	}

	// Create 64-bit mask for word-level operations
	mask64 := uint64(maskKey) | (uint64(maskKey) << 32)

	i := 0
	payloadLen := len(payload)

	// Unroll loop for 64-byte chunks (optimal for Apple Silicon cache lines)
	for i+63 < payloadLen {
		ptr := unsafe.Pointer(&payload[i])
		// Process 8 uint64 values (64 bytes) per iteration
		*(*uint64)(ptr) ^= mask64
		*(*uint64)(unsafe.Add(ptr, 8)) ^= mask64
		*(*uint64)(unsafe.Add(ptr, 16)) ^= mask64
		*(*uint64)(unsafe.Add(ptr, 24)) ^= mask64
		*(*uint64)(unsafe.Add(ptr, 32)) ^= mask64
		*(*uint64)(unsafe.Add(ptr, 40)) ^= mask64
		*(*uint64)(unsafe.Add(ptr, 48)) ^= mask64
		*(*uint64)(unsafe.Add(ptr, 56)) ^= mask64
		i += 64
	}

	// Process remaining 8-byte chunks
	for i+7 < payloadLen {
		*(*uint64)(unsafe.Pointer(&payload[i])) ^= mask64
		i += 8
	}

	// Handle final bytes individually
	mask4 := *(*[4]byte)(unsafe.Pointer(&maskKey))
	for i < payloadLen {
		payload[i] ^= mask4[i&3]
		i++
	}
}

// ───────────────────────────── Public True Zero-Copy API ─────────────────────────────

// ProcessHandshake reads and validates WebSocket upgrade response
//
//go:inline
//go:registerparams
func ProcessHandshake(conn net.Conn) error {
	bytesRead := 0

	for bytesRead < HandshakeBuf {
		n, err := conn.Read(handshakeBuffer[bytesRead:])
		if err != nil {
			debug.DropError("handshake read failed", err)
			return err
		}
		bytesRead += n

		// Search for HTTP terminator sequence
		if bytesRead >= 4 {
			searchLimit := bytesRead - 3
			for i := 0; i < searchLimit; i++ {
				// Check for CRLF CRLF sequence
				if *(*uint32)(unsafe.Pointer(&handshakeBuffer[i])) == 0x0A0D0A0D {
					debug.DropMessage("WebSocket", "handshake successful")
					return nil
				}
			}
		}
	}

	debug.DropError("handshake buffer overflow", nil)
	return io.ErrUnexpectedEOF
}

// IngestFrame reads and parses the next WebSocket frame with TRUE ZERO-COPY access
// CRITICAL: Returned payload is ONLY valid until the next IngestFrame() call
//
//go:inline
//go:registerparams
func IngestFrame(conn net.Conn) (*Frame, error) {
	ring.FrameCount++

	for {
		// Parse frame from current buffer contents with zero-copy
		headerSize, payloadSize, resultCode := parseFrameHeaderZeroCopy()

		switch resultCode {
		case NeedMoreData:
			// Compact buffer if read cursor is high - this invalidates previous payloads
			if ring.ReadCursor > CompactionThreshold {
				compactRingBuffer()
			}
			// Ingest more network data directly into ring buffer
			if !ingestNetworkData(conn) {
				debug.DropError("data ingestion failed", nil)
				return nil, io.ErrUnexpectedEOF
			}
			continue

		case Success:
			// Set up frame with DIRECT pointer to ring buffer - TRUE ZERO-COPY
			payloadStart := ring.ReadCursor + headerSize
			currentFrame.PayloadPtr = unsafe.Pointer(&ringBuffer[payloadStart])
			currentFrame.Size = payloadSize

			// Advance read cursor past this frame
			ring.ReadCursor += headerSize + payloadSize

			// Trigger compaction if cursor is high (this will invalidate returned payload eventually)
			if ring.ReadCursor > CompactionThreshold {
				// Don't compact now - that would invalidate the payload we're about to return
				// Compaction will happen on next IngestFrame() call
			}

			return &currentFrame, nil

		case CloseFrame:
			debug.DropMessage("WebSocket", "connection closed by peer")
			return nil, io.EOF

		case PingFrame:
			// Respond to ping immediately
			if _, err := conn.Write(protocolData.pongResponse[:]); err != nil {
				debug.DropError("pong response failed", err)
				return nil, err
			}
			ring.ReadCursor += 2 // Skip ping frame
			continue

		case PongFrame:
			ring.ReadCursor += 2 // Skip pong frame
			continue

		case FragmentFrame:
			// Skip entire fragment (header + payload) to maintain buffer integrity
			totalFrameSize := headerSize + payloadSize
			ring.ReadCursor += totalFrameSize
			continue

		case OversizeFrame:
			debug.DropError("frame exceeds size limit", nil)
			ring.ReadCursor += 2 // Skip frame header, let connection reset
			continue

		default:
			debug.DropError("frame parse error", nil)
			ring.ReadCursor += 1 // Advance by one byte to resync
			continue
		}
	}
}

// GetUpgradeRequest returns pre-built HTTP upgrade request
//
//go:nosplit
//go:inline
//go:registerparams
func GetUpgradeRequest() []byte {
	return protocolData.upgradeRequest[:protocolData.upgradeSize]
}

// GetSubscribePacket returns pre-built subscribe frame
//
//go:nosplit
//go:inline
//go:registerparams
func GetSubscribePacket() []byte {
	return protocolData.subscribePacket[:protocolData.subscribeSize]
}

// ExtractPayload returns DIRECT zero-copy payload slice pointing to ring buffer
// CRITICAL: Data is ONLY valid until next IngestFrame() call - process immediately
//
//go:nosplit
//go:inline
//go:registerparams
func (f *Frame) ExtractPayload() []byte {
	if f.Size <= 0 {
		return nil
	}
	return unsafe.Slice((*byte)(f.PayloadPtr), f.Size)
}

// GetProcessingStats returns current performance statistics
//
//go:nosplit
//go:inline
//go:registerparams
func GetProcessingStats() (totalFrames uint64, bufferUtilization int) {
	return ring.FrameCount, (ring.DataSize * 100) / RingBufSize
}
