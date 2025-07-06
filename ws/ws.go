package ws

import (
	"fmt"
	"main/constants"
	"net"
	"unsafe"
)

const BufferSize = 16777216     // 16MB
const HandshakeBufferSize = 512 // Handshake response buffer size

//go:notinheap
//go:align 16384
var buffer [BufferSize]byte

//go:notinheap
//go:align 16
var headerBuf [16]byte

//go:notinheap
var upgradeRequest [256]byte
var upgradeRequestLen int

//go:notinheap
var subscribeFrame [128]byte
var subscribeFrameLen int

func init() {
	// Build upgrade request directly into fixed buffer
	req := "GET " + constants.WsPath + " HTTP/1.1\r\n" +
		"Host: " + constants.WsHost + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	upgradeRequestLen = copy(upgradeRequest[:], req)

	// Build subscribe frame directly into fixed buffer
	payload := `{"jsonrpc":"2.0","method":"eth_subscribe","params":["logs",{}],"id":1}`
	plen := len(payload)

	subscribeFrame[0] = 0x81       // FIN=1, TEXT frame
	subscribeFrame[1] = 0x80 | 126 // MASK=1, 16-bit length
	subscribeFrame[2] = byte(plen >> 8)
	subscribeFrame[3] = byte(plen)
	subscribeFrame[4] = 0x12 // mask bytes
	subscribeFrame[5] = 0x34
	subscribeFrame[6] = 0x56
	subscribeFrame[7] = 0x78

	// XOR mask payload directly into buffer
	for i := 0; i < plen; i++ {
		subscribeFrame[8+i] = payload[i] ^ subscribeFrame[4+(i&3)]
	}
	subscribeFrameLen = 8 + plen
}

//go:noinline
func Handshake(conn net.Conn) error {
	_, err := conn.Write(upgradeRequest[:upgradeRequestLen])
	if err != nil {
		return err
	}

	// Stack-allocated buffer - no heap allocation
	var buf [HandshakeBufferSize]byte
	total := 0

	for total < 500 {
		n, err := conn.Read(buf[total:])
		if err != nil {
			return err
		}
		total += n

		if total >= 16 {
			// Fast scan for \r\n\r\n using 32-bit reads
			end := total - 3
			for i := 0; i < end; i++ {
				if *(*uint32)(unsafe.Pointer(&buf[i])) == 0x0A0D0A0D {
					// Check "HTTP/1.1 101" - fastest possible validation
					if *(*uint64)(unsafe.Pointer(&buf[0])) == 0x312E312F50545448 &&
						buf[8] == ' ' && buf[9] == '1' && buf[10] == '0' && buf[11] == '1' {
						return nil
					}
					return fmt.Errorf("upgrade failed")
				}
			}
		}
	}
	return fmt.Errorf("handshake timeout")
}

//go:noinline
func SendSubscription(conn net.Conn) error {
	_, err := conn.Write(subscribeFrame[:subscribeFrameLen])
	return err
}

//go:noinline
func SpinUntilCompleteMessage(conn net.Conn) ([]byte, error) {
	msgEnd := 0

	for {
		// Read minimum header (2 bytes)
		_, err := conn.Read(headerBuf[:2])
		if err != nil {
			return nil, err
		}

		opcode := headerBuf[0] & 0x0F
		payloadLen := uint64(headerBuf[1] & 0x7F)

		// Read extended length if needed
		if payloadLen == 126 {
			_, err = conn.Read(headerBuf[2:4])
			if err != nil {
				return nil, err
			}
			payloadLen = uint64(headerBuf[2])<<8 | uint64(headerBuf[3])
		} else if payloadLen == 127 {
			_, err = conn.Read(headerBuf[2:10])
			if err != nil {
				return nil, err
			}
			v := *(*uint64)(unsafe.Pointer(&headerBuf[2]))
			payloadLen = ((v & 0xFF) << 56) | ((v & 0xFF00) << 40) | ((v & 0xFF0000) << 24) | ((v & 0xFF000000) << 8) |
				((v & 0xFF00000000) >> 8) | ((v & 0xFF0000000000) >> 24) | ((v & 0xFF000000000000) >> 40) | ((v & 0xFF00000000000000) >> 56)
		}

		// Skip control frames
		if opcode >= 8 {
			if payloadLen > 0 {
				// Discard payload - use larger chunks when possible
				for remaining := payloadLen; remaining > 0; {
					toRead := remaining
					if toRead > 16 {
						toRead = 16
					}
					bytesRead, err := conn.Read(headerBuf[:toRead])
					if err != nil {
						return nil, err
					}
					remaining -= uint64(bytesRead)
				}
			}
			continue
		}

		// Read payload directly
		remaining := payloadLen
		for remaining > 0 {
			toRead := remaining
			if toRead > uint64(BufferSize-msgEnd) {
				toRead = uint64(BufferSize - msgEnd)
			}
			if toRead > 65536 {
				toRead = 65536 // 64KB chunks
			}

			bytesRead, err := conn.Read(buffer[msgEnd : msgEnd+int(toRead)])
			if err != nil {
				return nil, err
			}
			msgEnd += bytesRead
			remaining -= uint64(bytesRead)
		}

		// Check FIN bit
		if headerBuf[0]&0x80 != 0 {
			return buffer[:msgEnd], nil
		}
	}
}
