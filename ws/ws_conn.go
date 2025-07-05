// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: ws_conn.go — Ultra-lean ISR-grade zero-alloc WebSocket setup
//
// Purpose:
//   - Constructs immutable WebSocket upgrade request and subscription frame
//   - Eliminates ALL allocations during runtime operation
//   - Single-frame streaming parser for maximum performance
//
// Notes:
//   - All variables are initialized exactly once in init()
//   - Shared runtime state is immutable post-init — guarantees no heap pressure
//   - No frame ring - direct streaming processing only
//   - Single-threaded, zero-copy, no interfaces
//
// ⚠️ NEVER mutate shared state after init
// ⚠️ SINGLE-THREADED ONLY — no concurrent access protection
// ─────────────────────────────────────────────────────────────────────────────

package ws

import (
	"crypto/rand"
	"encoding/base64"
	"main/constants"
	"unsafe"
)

// ───────────────────────────── Shared Runtime State ─────────────────────────────

var (
	// HTTP upgrade handshake payload (static pre-allocated)
	upgradeRequest [512]byte // Fixed-size buffer to avoid allocation
	upgradeLen     int       // Actual length of upgrade request

	// RFC 6455-masked subscribe payload (precomputed, fixed-size)
	subscribePacket [128]byte // Fixed-size buffer for subscribe frame
	subscribeLen    int       // Actual length of subscribe packet

	// Raw TCP buffer backing WebSocket read loop
	wsBuf          [constants.MaxFrameSize]byte
	wsStart, wsLen int

	// Single reusable frame (no ring buffer overhead)
	currentFrame wsFrame

	// Pre-allocated buffers for string operations (zero-copy)
	keyBuf     [24]byte  // Base64-encoded key buffer
	payloadBuf [256]byte // JSON payload buffer

	// Pre-allocated Pong frame for responding to Ping frames
	pongFrame [2]byte // = {0x8A, 0x00}

	// Architecture-specific CRLF-CRLF pattern (computed at init)
	crlfcrlfPattern uint32
)

// wsFrame holds a parsed WebSocket payload (direct view into wsBuf).
// Ultra-lean: removed redundant fields, optimized for cache alignment.
//
//go:notinheap
//go:align 64
type wsFrame struct {
	PayloadPtr unsafe.Pointer // Direct pointer to payload in wsBuf
	Len        int            // payload length
	End        int            // wsBuf offset for reclaim
	_          [5]uint64      // Padding to maintain 64B alignment
}

// GetPayload returns payload as []byte without allocating slice header.
//
//go:nosplit
//go:inline
//go:registerparams
func (f *wsFrame) GetPayload() []byte {
	return unsafe.Slice((*byte)(f.PayloadPtr), f.Len)
}

// GetUpgradeRequest returns the pre-built upgrade request without allocation.
//
//go:nosplit
//go:inline
//go:registerparams
func GetUpgradeRequest() []byte {
	return upgradeRequest[:upgradeLen]
}

// GetSubscribePacket returns the pre-built subscribe packet without allocation.
//
//go:nosplit
//go:inline
//go:registerparams
func GetSubscribePacket() []byte {
	return subscribePacket[:subscribeLen]
}

// init prebuilds the upgrade request and masked subscribe frame.
//
//go:nosplit
//go:inline
//go:registerparams
func init() {
	// Initialize CRLF-CRLF pattern for current architecture
	hsTerm := [4]byte{'\r', '\n', '\r', '\n'}
	crlfcrlfPattern = *(*uint32)(unsafe.Pointer(&hsTerm[0]))

	// Generate Sec-WebSocket-Key (zero-copy)
	var keyBytes [16]byte
	_, _ = rand.Read(keyBytes[:])
	base64.StdEncoding.Encode(keyBuf[:], keyBytes[:])

	// Build the upgrade request directly into fixed buffer
	upgradeLen = 0
	upgradeLen += copy(upgradeRequest[upgradeLen:], []byte("GET "))
	upgradeLen += copy(upgradeRequest[upgradeLen:], []byte(constants.WsPath))
	upgradeLen += copy(upgradeRequest[upgradeLen:], []byte(" HTTP/1.1\r\nHost: "))
	upgradeLen += copy(upgradeRequest[upgradeLen:], []byte(constants.WsHost))
	upgradeLen += copy(upgradeRequest[upgradeLen:], []byte("\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: "))
	upgradeLen += copy(upgradeRequest[upgradeLen:], keyBuf[:24])
	upgradeLen += copy(upgradeRequest[upgradeLen:], []byte("\r\nSec-WebSocket-Version: 13\r\n\r\n"))

	// Masked subscribe payload (zero-copy)
	jsonPayload := []byte(`{"id":1,"method":"eth_subscribe","params":["logs",{}],"jsonrpc":"2.0"}`)
	payloadLen := copy(payloadBuf[:], jsonPayload)

	subscribePacket[0] = 0x81                    // FIN|TEXT
	subscribePacket[1] = 0x80 | byte(payloadLen) // MASKED | length

	var maskBytes [4]byte
	_, _ = rand.Read(maskBytes[:])
	copy(subscribePacket[2:6], maskBytes[:])

	for i := 0; i < payloadLen; i++ {
		subscribePacket[6+i] = payloadBuf[i] ^ maskBytes[i&3]
	}
	subscribeLen = 6 + payloadLen

	pongFrame[0] = 0x8A // FIN=1, Opcode=0xA (Pong)
	pongFrame[1] = 0x00 // No payload
}
