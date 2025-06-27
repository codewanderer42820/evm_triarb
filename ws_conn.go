// ws_conn.go — WebSocket setup: HTTP upgrade, frame mask prep, and buffer initialization.
// Runs once at startup. All outputs are immutable and reused by the runtime.

package main

import (
	"crypto/rand"
	"encoding/base64"
)

// ───────────────────────────── Shared Runtime State ─────────────────────────────
//
// Everything below is initialized once in `init()` and used read-only thereafter.
// This avoids allocation or mutation during the hot-path WebSocket loop.

var (
	// upgradeRequest is a raw HTTP 1.1 request to initiate WebSocket protocol upgrade.
	// It includes a 16-byte random Sec-WebSocket-Key encoded in base64.
	upgradeRequest []byte

	// subscribePacket is a fully-formed WebSocket frame that initiates a log subscription.
	// It is pre-masked (RFC 6455) with a random 4-byte masking key at startup.
	subscribePacket []byte

	// wsBuf is a circular byte buffer holding incoming TCP stream data.
	// wsStart/wsLen track the unread region: [wsStart : wsStart+wsLen)
	wsBuf          [maxFrameSize]byte
	wsStart, wsLen int

	// wsFrames is a fixed ring of parsed WebSocket frame views.
	// Each frame is a pointerless view into wsBuf, valid until overwritten.
	wsFrames [frameCap]wsFrame
	wsHead   uint32 // atomic-free index for write position
)

// wsFrame represents a parsed WebSocket payload slice.
// All fields point directly into wsBuf and must not outlive it.
type wsFrame struct {
	Payload []byte // application-level payload slice (e.g. JSON)
	Len     int    // length of payload (≤ maxFrameSize)
	End     int    // end offset in wsBuf (used to advance wsStart)
}

// init builds the WebSocket upgrade request and subscribe frame once.
// Called automatically by Go runtime before main().
//
//go:nosplit
func init() {
	// 1. Generate random 16-byte Sec-WebSocket-Key
	key := make([]byte, 16)
	_, _ = rand.Read(key)

	upgradeRequest = []byte("GET " + wsPath + " HTTP/1.1\r\n" +
		"Host: " + wsHost + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(key) + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n")

	// 2. Prepare subscription payload (eth_subscribe: logs)
	const payload = `{"id":1,"method":"eth_subscribe","params":["logs",{}],"jsonrpc":"2.0"}`

	// Create WebSocket frame header: FIN + MASK + payload length (≤125 bytes)
	hdr := []byte{0x81, 0x80 | byte(len(payload))}

	// 3. Generate random 4-byte masking key
	var mask [4]byte
	_, _ = rand.Read(mask[:])
	hdr = append(hdr, mask[:]...)

	// 4. Apply masking to payload in-place (RFC 6455)
	frame := append(hdr, payload...)
	for i := range payload {
		frame[len(hdr)+i] ^= mask[i&3]
	}
	subscribePacket = frame
}
