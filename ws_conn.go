// ─────────────────────────────────────────────────────────────────────────────
// [Filename]: ws_conn.go — ISR-class WebSocket setup & shared ring state
//
// Purpose:
//   - Constructs immutable WebSocket upgrade request and subscription frame
//   - Initializes shared buffers and frame ring for runtime parsing
//
// Notes:
//   - All variables initialized exactly once in init()
//   - Runtime reads never mutate shared state — safe for no-lock use
//   - Payload masking complies with RFC 6455 client-to-server masking rules
//
// ⚠️ NEVER mutate shared state after init — ring buffer assumes immutability
// ─────────────────────────────────────────────────────────────────────────────

package main

import (
	"crypto/rand"
	"encoding/base64"
)

// ───────────────────────────── Shared Runtime State ─────────────────────────────

var (
	// HTTP upgrade handshake payload (static)
	// This is the request used to upgrade the connection from HTTP to WebSocket.
	upgradeRequest []byte

	// RFC 6455-masked subscribe payload (precomputed)
	// This payload is pre-masked according to the WebSocket protocol and sent when subscribing.
	subscribePacket []byte

	// Raw TCP buffer backing WebSocket read loop
	// The buffer stores WebSocket frames for processing.
	wsBuf          [maxFrameSize]byte
	wsStart, wsLen int

	// WebSocket frame ring: pre-allocated views into wsBuf
	// The ring holds pre-allocated frames for efficient WebSocket frame parsing.
	wsFrames [frameCap]wsFrame
	wsHead   uint32
)

// wsFrame holds a parsed WebSocket payload (view into wsBuf).
// `End` is used to reclaim space after processing.
//
//go:notinheap
//go:align 64
type wsFrame struct {
	Payload []byte // zero-copy payload view into wsBuf
	Len     int    // payload length
	End     int    // wsBuf offset for reclaim

	_ [3]uint64 // 24-byte pad to bring struct to 64B
}

// init prebuilds the upgrade request and masked subscribe frame.
// Ensures fully deterministic runtime state — no allocs during execution.
//
//go:nosplit
//go:inline
//go:registerparams
func init() {
	// ───── Step 1: Generate Sec-WebSocket-Key ─────
	// Generate a random 16-byte Sec-WebSocket-Key for the upgrade request.
	key := make([]byte, 16)
	_, _ = rand.Read(key)

	// Build the upgrade request with necessary headers.
	// This request is sent to initiate the WebSocket connection.
	upgradeRequest = []byte("GET " + wsPath + " HTTP/1.1\r\n" +
		"Host: " + wsHost + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(key) + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n")

	// ───── Step 2: Masked subscribe payload ─────
	// This is the subscribe payload for the WebSocket. It's used to subscribe to log events.
	const payload = `{"id":1,"method":"eth_subscribe","params":["logs",{}],"jsonrpc":"2.0"}`

	// Prepare the WebSocket frame header
	hdr := []byte{0x81, 0x80 | byte(len(payload))} // FIN|TEXT, MASKED

	var mask [4]byte
	_, _ = rand.Read(mask[:])     // Generate a random mask for payload
	hdr = append(hdr, mask[:]...) // Append the mask to the header

	// Append the payload, applying the mask as required by RFC 6455
	frame := append(hdr, payload...)
	for i := range payload {
		// XOR mask per RFC 6455
		frame[len(hdr)+i] ^= mask[i&3]
	}

	// Finalize the subscribe frame
	subscribePacket = frame
}
