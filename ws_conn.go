// ws_conn.go — one-off WebSocket setup and buffer plumbing.
package main

import (
	"crypto/rand"
	"encoding/base64"
)

// ───────────────────────────── shared state ────────────────────────────────
// Everything below is initialised once at program start and then treated as
// read-only by the hot paths.

var (
	// upgradeRequest: the RFC-6455 HTTP request that turns a TCP/TLS socket
	// into a WebSocket connection. Built at init() with a random key.
	upgradeRequest []byte

	// subscribePacket: a **masked** client-to-server frame that subscribes
	// to Ethereum `eth_subscribe: logs`. Also built at init().
	subscribePacket []byte

	// wsBuf / wsStart / wsLen: circular buffer holding TCP stream bytes.
	wsBuf          [maxFrameSize]byte // max 64 KiB in this build
	wsStart, wsLen int                // unread window [start : start+len)

	// Decoded data-frames (views into wsBuf) live in this ring.
	wsFrames [frameCap]wsFrame
	wsHead   uint32
)

// wsFrame is a light view into wsBuf — NO copies or allocations.
type wsFrame struct {
	Payload []byte // valid until the next readFrame()
	Len     int
	End     int // absolute end offset inside wsBuf
}

// init builds the upgrade request and a single subscribe frame.
func init() {
	// ── 1. build HTTP upgrade request ──
	key := make([]byte, 16)
	_, _ = rand.Read(key)

	upgradeRequest = []byte("GET " + wsPath + " HTTP/1.1\r\n" +
		"Host: " + wsHost + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(key) + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n")

	// ── 2. pre-mask the subscribe payload ──
	const payload = `{"id":1,"method":"eth_subscribe","params":["logs",{}],"jsonrpc":"2.0"}`
	hdr := []byte{0x81, 0x80 | byte(len(payload))} // FIN | MASK | len≤125

	var mask [4]byte
	_, _ = rand.Read(mask[:])
	hdr = append(hdr, mask[:]...)

	frame := append(hdr, payload...)
	for i := range payload {
		frame[len(hdr)+i] ^= mask[i&3] // RFC 6455 masking
	}
	subscribePacket = frame
}
