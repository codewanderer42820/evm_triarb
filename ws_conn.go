// ws_conn.go — WebSocket setup: HTTP upgrade, frame mask prep, and buffer initialization.
// Runs once at startup. All outputs are immutable and reused by the runtime.

package main

import (
	"crypto/rand"
	"encoding/base64"
)

// ───────────────────────────── Shared Runtime State ─────────────────────────────
//
// All variables below are initialized once during program `init()`.
// None are mutated during the hot path (readFrame/handleFrame).
// This ensures:
//   - No GC pressure from log loop
//   - Zero heap allocation during operation
//   - Deterministic memory footprint for circular buffers

var (
	// upgradeRequest is a raw WebSocket upgrade request (HTTP/1.1 format).
	// It includes a randomized Sec-WebSocket-Key (base64-encoded, 16 bytes raw).
	upgradeRequest []byte

	// subscribePacket is a pre-masked WebSocket frame containing:
	//   {"id":1,"method":"eth_subscribe","params":["logs",{}],"jsonrpc":"2.0"}
	// The masking is RFC 6455-compliant and done once up front.
	subscribePacket []byte

	// wsBuf is the ring buffer used for the raw TCP stream from WebSocket conn.
	// wsStart/wsLen track the valid unread window [start : start+len)
	wsBuf          [maxFrameSize]byte
	wsStart, wsLen int

	// wsFrames is a circular ring of views into parsed WebSocket payloads.
	// Each frame is a pointerless view over wsBuf, valid until overwritten.
	wsFrames [frameCap]wsFrame
	wsHead   uint32 // write pointer into wsFrames (incremented on every frame)
)

// wsFrame represents one parsed WebSocket payload slice.
// Payload points directly into `wsBuf` and MUST NOT outlive it.
type wsFrame struct {
	Payload []byte // raw log JSON payload (zero-copy view)
	Len     int    // length of payload (≤ maxFrameSize)
	End     int    // end offset in wsBuf (used to reclaim space)
}

// init constructs the handshake and subscribe frames once, before `main()`.
//
// Compiler Directives:
//   - nosplit         → ensures early init path has no preemption
//
//go:nosplit
func init() {
	// ───── Step 1: Generate Sec-WebSocket-Key (16 random bytes) ─────
	key := make([]byte, 16)
	_, _ = rand.Read(key)

	upgradeRequest = []byte("GET " + wsPath + " HTTP/1.1\r\n" +
		"Host: " + wsHost + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(key) + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n")

	// ───── Step 2: Prepare subscription JSON payload ─────
	const payload = `{"id":1,"method":"eth_subscribe","params":["logs",{}],"jsonrpc":"2.0"}`

	// Header: FIN (0x80) | OPCODE (0x1 = text) | MASK (0x80) | len
	hdr := []byte{0x81, 0x80 | byte(len(payload))}

	// ───── Step 3: Generate 4-byte random mask ─────
	var mask [4]byte
	_, _ = rand.Read(mask[:])
	hdr = append(hdr, mask[:]...) // append masking key to header

	// ───── Step 4: Mask payload in-place per RFC 6455 ─────
	frame := append(hdr, payload...)
	for i := range payload {
		frame[len(hdr)+i] ^= mask[i&3]
	}

	// Final prebuilt frame: sent once after handshake
	subscribePacket = frame
}
