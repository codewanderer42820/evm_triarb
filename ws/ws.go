package ws

import (
	"encoding/binary"
	"fmt"
	"main/constants"
	"net"
)

const BufferSize = 16777216 // 16MB

//go:notinheap
//go:align 16384
var buffer [BufferSize]byte

//go:notinheap
//go:align 16
var headerBuf [16]byte

var upgradeRequest []byte
var subscribeFrame []byte

func init() {
	upgradeRequest = []byte(
		"GET " + constants.WsPath + " HTTP/1.1\r\n" +
			"Host: " + constants.WsHost + "\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
			"Sec-WebSocket-Version: 13\r\n\r\n")

	payload := `{"jsonrpc":"2.0","method":"eth_subscribe","params":["logs",{}],"id":1}`
	payloadBytes := []byte(payload)
	plen := len(payloadBytes)
	mask := [4]byte{0x12, 0x34, 0x56, 0x78}

	frame := make([]byte, 0, 512)
	frame = append(frame, 0x81) // FIN=1, TEXT frame

	if plen > 125 {
		frame = append(frame, 0x80|126) // MASK=1, indicates 16-bit length follows
		frame = append(frame, byte(plen>>8), byte(plen))
	} else {
		frame = append(frame, 0x80|byte(plen)) // MASK=1, 7-bit length
	}

	frame = append(frame, mask[:]...)
	for i, b := range payloadBytes {
		frame = append(frame, b^mask[i&3])
	}
	subscribeFrame = frame
}

func Handshake(conn net.Conn) error {
	if _, err := conn.Write(upgradeRequest); err != nil {
		return err
	}

	buf := make([]byte, 1024)
	total := 0

	for total < 1000 {
		n, err := conn.Read(buf[total:])
		if err != nil {
			return err
		}
		total += n

		if total >= 4 {
			for i := 0; i <= total-4; i++ {
				if buf[i] == '\r' && buf[i+1] == '\n' &&
					buf[i+2] == '\r' && buf[i+3] == '\n' {
					if total >= 12 && string(buf[:12]) == "HTTP/1.1 101" {
						return nil
					}
					return fmt.Errorf("upgrade failed")
				}
			}
		}
	}
	return fmt.Errorf("handshake timeout")
}

func SendSubscription(conn net.Conn) error {
	_, err := conn.Write(subscribeFrame)
	return err
}

func SpinUntilCompleteMessage(conn net.Conn) ([]byte, error) {
	msgEnd := 0

	for {
		// Read 16 bytes for header parsing
		n, err := conn.Read(headerBuf[:])
		if err != nil {
			return nil, err
		}

		// Parse header - hot path optimized
		b1 := headerBuf[1]
		payloadLen := int(b1 & 0x7F)
		headerLen := 2

		if payloadLen > 125 {
			if payloadLen == 126 {
				headerLen = 4
				payloadLen = int(binary.BigEndian.Uint16(headerBuf[2:4]))
			} else { // payloadLen == 127
				headerLen = 10
				payloadLen = int(binary.BigEndian.Uint64(headerBuf[2:10]))
			}
		}

		// Skip control frames (opcode >= 8)
		if headerBuf[0]&0x0F >= 8 {
			if payloadLen > 0 {
				// Read and discard control payload
				for remaining := payloadLen; remaining > 0; {
					toRead := remaining
					if toRead > 16 {
						toRead = 16
					}
					bytesRead, err := conn.Read(headerBuf[:toRead])
					if err != nil {
						return nil, err
					}
					remaining -= bytesRead
				}
			}
			continue
		}

		// Copy payload from header buffer
		payloadInHeader := n - headerLen
		if payloadInHeader > 0 {
			copy(buffer[msgEnd:], headerBuf[headerLen:headerLen+payloadInHeader])
			msgEnd += payloadInHeader
		}

		// Read remaining payload
		remaining := payloadLen - payloadInHeader
		for remaining > 0 {
			bytesRead, err := conn.Read(buffer[msgEnd : msgEnd+remaining])
			if err != nil {
				return nil, err
			}
			msgEnd += bytesRead
			remaining -= bytesRead
		}

		// Check FIN bit
		if headerBuf[0]&0x80 != 0 {
			return buffer[:msgEnd], nil
		}
	}
}
