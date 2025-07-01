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
// Compiler Directives:
//   - //go:nosplit
//   - //go:registerparams
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
	upgradeRequest []byte

	// RFC 6455-masked subscribe payload (precomputed)
	subscribePacket []byte

	// Raw TCP buffer backing WebSocket read loop
	wsBuf          [maxFrameSize]byte
	wsStart, wsLen int

	// WebSocket frame ring: pre-allocated views into wsBuf
	wsFrames [frameCap]wsFrame
	wsHead   uint32
)

// wsFrame holds a parsed WebSocket payload (view into wsBuf).
// `End` is used to reclaim space after processing.
type wsFrame struct {
	Payload []byte // log payload (zero-copy)
	Len     int    // byte length
	End     int    // end offset in wsBuf
}

// init prebuilds the upgrade request and masked subscribe frame.
// Ensures fully deterministic runtime state — no allocs during execution.
//
//go:nosplit
//go:registerparams
func init() {
	// ───── Step 1: Generate Sec-WebSocket-Key ─────
	key := make([]byte, 16)
	_, _ = rand.Read(key)

	upgradeRequest = []byte("GET " + wsPath + " HTTP/1.1\r\n" +
		"Host: " + wsHost + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(key) + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n")

	// ───── Step 2: Masked subscribe payload ─────
	const payload = `{"id":1,"method":"eth_subscribe","params":["logs",{}],"jsonrpc":"2.0"}`

	hdr := []byte{0x81, 0x80 | byte(len(payload))} // FIN|TEXT, MASKED

	var mask [4]byte
	_, _ = rand.Read(mask[:])
	hdr = append(hdr, mask[:]...)

	frame := append(hdr, payload...)
	for i := range payload {
		frame[len(hdr)+i] ^= mask[i&3] // XOR mask per RFC 6455
	}

	subscribePacket = frame
}
