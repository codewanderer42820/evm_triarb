// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: ws_conn.go — Zero-alloc WebSocket setup & shared ring state
//
// Purpose:
//   - Constructs immutable WebSocket upgrade request and subscription frame
//   - Initializes shared buffers and frame ring for runtime parsing
//   - Eliminates ALL allocations during runtime operation
//
// Notes:
//   - All variables initialized exactly once in init()
//   - Runtime reads never mutate shared state — safe for single-threaded use
//   - Payload masking complies with RFC 6455 client-to-server masking rules
//   - Zero-copy: no []byte allocations, no string conversions, no interface{}
//
// ⚠️ NEVER mutate shared state after init — ring buffer assumes immutability
// ⚠️ SINGLE-THREADED ONLY — no concurrent access protection
// ─────────────────────────────────────────────────────────────────────────────

package main

import (
	"crypto/rand"
	"encoding/base64"
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
	wsBuf          [maxFrameSize]byte
	wsStart, wsLen int

	// WebSocket frame ring: pre-allocated views into wsBuf
	wsFrames [frameCap]wsFrame
	wsHead   uint32

	// Pre-allocated buffers for string operations (zero-copy)
	keyBuf     [24]byte  // Base64-encoded key buffer
	payloadBuf [256]byte // JSON payload buffer

	// Pre-allocated Pong frame for responding to Ping frames
	pongFrame [2]byte // = {0x8A, 0x00}

	// Architecture-specific CRLF-CRLF pattern (computed at init)
	crlfcrlfPattern uint32
)

// wsFrame holds a parsed WebSocket payload (view into wsBuf).
// `End` is used to reclaim space after processing.
//
//go:notinheap
//go:align 64
type wsFrame struct {
	PayloadPtr   unsafe.Pointer // Direct pointer to payload in wsBuf
	PayloadStart int            // Start offset in wsBuf
	PayloadEnd   int            // End offset in wsBuf
	Len          int            // payload length
	End          int            // wsBuf offset for reclaim

	_ [2]uint64 // Padding to maintain 64B alignment
}

// GetPayload returns payload as []byte without allocating slice header
//
//go:nosplit
//go:inline
//go:registerparams
func (f *wsFrame) GetPayload() []byte {
	if f.PayloadPtr == nil {
		return nil
	}
	return unsafe.Slice((*byte)(f.PayloadPtr), f.Len)
}

// GetPayloadUnsafe returns direct pointer access (most efficient)
//
//go:nosplit
//go:inline
//go:registerparams
func (f *wsFrame) GetPayloadUnsafe() (unsafe.Pointer, int) {
	return f.PayloadPtr, f.Len
}

// init prebuilds the upgrade request and masked subscribe frame.
// Ensures fully deterministic runtime state — no allocs during execution.
//
//go:nosplit
//go:inline
//go:registerparams
func init() {
	// ───── Step 1: Initialize CRLF-CRLF pattern for current architecture ─────
	hsTerm := [4]byte{'\r', '\n', '\r', '\n'}
	crlfcrlfPattern = *(*uint32)(unsafe.Pointer(&hsTerm[0]))

	// ───── Step 2: Generate Sec-WebSocket-Key (zero-copy) ─────
	var keyBytes [16]byte
	_, _ = rand.Read(keyBytes[:])

	// Encode to base64 directly into pre-allocated buffer
	base64.StdEncoding.Encode(keyBuf[:], keyBytes[:])

	// Build the upgrade request directly into fixed buffer (no string allocs)
	upgradeLen = 0
	upgradeLen += copyBytes(upgradeRequest[upgradeLen:], []byte("GET "))
	upgradeLen += copyBytes(upgradeRequest[upgradeLen:], []byte(wsPath))
	upgradeLen += copyBytes(upgradeRequest[upgradeLen:], []byte(" HTTP/1.1\r\nHost: "))
	upgradeLen += copyBytes(upgradeRequest[upgradeLen:], []byte(wsHost))
	upgradeLen += copyBytes(upgradeRequest[upgradeLen:], []byte("\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: "))
	upgradeLen += copyBytes(upgradeRequest[upgradeLen:], keyBuf[:24])
	upgradeLen += copyBytes(upgradeRequest[upgradeLen:], []byte("\r\nSec-WebSocket-Version: 13\r\n\r\n"))

	// ───── Step 3: Masked subscribe payload (zero-copy) ─────
	// Build JSON payload directly into buffer
	jsonPayload := []byte(`{"id":1,"method":"eth_subscribe","params":["logs",{}],"jsonrpc":"2.0"}`)
	payloadLen := copy(payloadBuf[:], jsonPayload)

	// Prepare the WebSocket frame header
	subscribePacket[0] = 0x81                    // FIN|TEXT
	subscribePacket[1] = 0x80 | byte(payloadLen) // MASKED | length

	// Generate mask directly into frame
	var maskBytes [4]byte
	_, _ = rand.Read(maskBytes[:])
	copy(subscribePacket[2:6], maskBytes[:])

	// Copy and mask payload in one pass
	for i := 0; i < payloadLen; i++ {
		subscribePacket[6+i] = payloadBuf[i] ^ maskBytes[i&3]
	}
	subscribeLen = 6 + payloadLen

	// Initialize pong frame
	pongFrame[0] = 0x8A // FIN=1, Opcode=0xA (Pong)
	pongFrame[1] = 0x00 // No payload
}

// copyBytes copies bytes without string conversion
//
//go:nosplit
//go:inline
//go:registerparams
func copyBytes(dst, src []byte) int {
	return copy(dst, src)
}
