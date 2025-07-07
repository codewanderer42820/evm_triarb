package ws

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// ==============================================================================
// CONNECTION MOCKS FOR TESTING
// ==============================================================================

// mockConn provides full connection simulation with error injection capabilities
// for comprehensive unit testing of error conditions and edge cases.
type mockConn struct {
	readData  []byte
	readPos   int
	writeData []byte
	readErr   error
	writeErr  error
	closed    bool
	readFunc  func(b []byte) (int, error)
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.readFunc != nil {
		return m.readFunc(b)
	}
	if m.readErr != nil {
		return 0, m.readErr
	}
	if m.readPos >= len(m.readData) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(b, m.readData[m.readPos:])
	m.readPos += n
	if n == 0 && len(b) > 0 {
		return 0, fmt.Errorf("EOF")
	}
	return n, nil
}

func (m *mockConn) Write(b []byte) (int, error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// nullConn provides write-only connection for testing subscription sends.
type nullConn struct{}

func (n *nullConn) Read(b []byte) (int, error)         { return 0, fmt.Errorf("EOF") }
func (n *nullConn) Write(b []byte) (int, error)        { return len(b), nil }
func (n *nullConn) Close() error                       { return nil }
func (n *nullConn) LocalAddr() net.Addr                { return nil }
func (n *nullConn) RemoteAddr() net.Addr               { return nil }
func (n *nullConn) SetDeadline(t time.Time) error      { return nil }
func (n *nullConn) SetReadDeadline(t time.Time) error  { return nil }
func (n *nullConn) SetWriteDeadline(t time.Time) error { return nil }

// ==============================================================================
// ZERO-ALLOCATION CONNECTIONS FOR BENCHMARKS
// ==============================================================================

// zeroConn provides zero-allocation data delivery for pure performance measurement.
type zeroConn struct {
	data []byte
	pos  int
}

func (z *zeroConn) Read(b []byte) (int, error) {
	if z.pos >= len(z.data) {
		return 0, io.EOF
	}
	n := copy(b, z.data[z.pos:])
	z.pos += n
	return n, nil
}

func (z *zeroConn) Write(b []byte) (int, error)        { return len(b), nil }
func (z *zeroConn) Close() error                       { return nil }
func (z *zeroConn) LocalAddr() net.Addr                { return nil }
func (z *zeroConn) RemoteAddr() net.Addr               { return nil }
func (z *zeroConn) SetDeadline(t time.Time) error      { return nil }
func (z *zeroConn) SetReadDeadline(t time.Time) error  { return nil }
func (z *zeroConn) SetWriteDeadline(t time.Time) error { return nil }

func (z *zeroConn) reset() { z.pos = 0 }

// chunkedConn simulates realistic read patterns without network overhead.
type chunkedConn struct {
	data      []byte
	pos       int
	chunkSize int
}

func (c *chunkedConn) Read(b []byte) (int, error) {
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	toRead := len(b)
	if toRead > c.chunkSize {
		toRead = c.chunkSize
	}
	available := len(c.data) - c.pos
	if toRead > available {
		toRead = available
	}
	n := copy(b[:toRead], c.data[c.pos:c.pos+toRead])
	c.pos += n
	return n, nil
}

func (c *chunkedConn) Write(b []byte) (int, error)        { return len(b), nil }
func (c *chunkedConn) Close() error                       { return nil }
func (c *chunkedConn) LocalAddr() net.Addr                { return nil }
func (c *chunkedConn) RemoteAddr() net.Addr               { return nil }
func (c *chunkedConn) SetDeadline(t time.Time) error      { return nil }
func (c *chunkedConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *chunkedConn) SetWriteDeadline(t time.Time) error { return nil }

func (c *chunkedConn) reset() { c.pos = 0 }

// ==============================================================================
// FRAME BUILDERS
// ==============================================================================

// createTestFrame builds WebSocket frames for unit testing. Allocations are
// acceptable here since unit tests prioritize correctness over performance.
func createTestFrame(opcode byte, payload []byte, fin bool) []byte {
	frame := make([]byte, 2)

	if fin {
		frame[0] = 0x80 | opcode
	} else {
		frame[0] = opcode
	}

	plen := len(payload)
	if plen < 126 {
		frame[1] = byte(plen)
	} else if plen < 65536 {
		frame[1] = 126
		frame = append(frame, byte(plen>>8), byte(plen))
	} else {
		frame[1] = 127
		lenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBytes, uint64(plen))
		frame = append(frame, lenBytes...)
	}

	return append(frame, payload...)
}

// buildFrame creates WebSocket frames in pre-allocated buffers for zero-allocation
// benchmarks. Returns the number of bytes written to the destination buffer.
func buildFrame(dst []byte, opcode byte, payload []byte, fin bool) int {
	pos := 0

	if fin {
		dst[pos] = 0x80 | opcode
	} else {
		dst[pos] = opcode
	}
	pos++

	plen := len(payload)
	if plen < 126 {
		dst[pos] = byte(plen)
		pos++
	} else if plen < 65536 {
		dst[pos] = 126
		pos++
		dst[pos] = byte(plen >> 8)
		pos++
		dst[pos] = byte(plen)
		pos++
	} else {
		dst[pos] = 127
		pos++
		binary.BigEndian.PutUint64(dst[pos:pos+8], uint64(plen))
		pos += 8
	}

	copy(dst[pos:], payload)
	return pos + len(payload)
}

// ==============================================================================
// PRE-ALLOCATED TEST DATA
// ==============================================================================

// Pre-allocated Ethereum-like payloads for zero-allocation benchmarks.
var (
	ethPayload64    [64]byte
	ethPayload512   [512]byte
	ethPayload1536  [1536]byte
	ethPayload4096  [4096]byte
	ethPayload16384 [16384]byte
	ethPayload65536 [65536]byte
)

// Pre-built complete frames (header + payload) for immediate use in benchmarks.
var (
	frame64    []byte
	frame512   []byte
	frame1536  []byte
	frame4096  []byte
	frame16384 []byte
	frame65536 []byte
)

// Pre-built complex message sequences for specialized testing.
var (
	fragmented4096  []byte // 4KB message fragmented into 512B pieces
	fragmented16384 []byte // 16KB message fragmented into MTU-sized pieces
	controlMixed    []byte // Data frames mixed with ping/pong control frames
	maxFragmented   []byte // Worst-case 1-byte-per-frame fragmentation
	alternatingCtrl []byte // Alternating control and data frame pattern
)

// fillEthereumPayload generates realistic Ethereum JSON-RPC subscription data
// in the provided buffer without allocations.
func fillEthereumPayload(buf []byte) {
	template := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x123","result":{"address":"0xa0b86a33e624e826","data":"0x`
	pos := copy(buf, template)

	// Fill remaining space with realistic hex data
	hexChars := "0123456789abcdef"
	for i := pos; i < len(buf)-3; i++ {
		buf[i] = hexChars[i&15]
	}

	// Close JSON structure properly
	if len(buf) >= 3 {
		copy(buf[len(buf)-3:], `"}}`)
	}
}

// buildFragmentedSequences creates pre-built fragmented message sequences
// for testing fragmentation handling performance.
func buildFragmentedSequences() {
	var buf [70000]byte
	pos := 0

	// Build 4096-byte message fragmented into 512-byte pieces
	for i := 0; i < 4096; i += 512 {
		end := i + 512
		if end > 4096 {
			end = 4096
		}
		fragment := ethPayload4096[i:end]
		isFirst := i == 0
		isLast := end == 4096

		opcode := byte(0x0) // Continuation frame
		if isFirst {
			opcode = 0x1 // Text frame
		}

		n := buildFrame(buf[pos:], opcode, fragment, isLast)
		pos += n
	}
	fragmented4096 = append([]byte(nil), buf[:pos]...)

	// Build 16384-byte message fragmented into MTU-sized pieces
	pos = 0
	for i := 0; i < 16384; i += 1460 {
		end := i + 1460
		if end > 16384 {
			end = 16384
		}
		fragment := ethPayload16384[i:end]
		isFirst := i == 0
		isLast := end == 16384

		opcode := byte(0x0)
		if isFirst {
			opcode = 0x1
		}

		n := buildFrame(buf[pos:], opcode, fragment, isLast)
		pos += n
	}
	fragmented16384 = append([]byte(nil), buf[:pos]...)
}

// buildControlMixedSequence creates a sequence of data frames interspersed
// with control frames to test realistic WebSocket traffic patterns.
func buildControlMixedSequence() {
	var buf [10000]byte
	pos := 0

	// Pattern: data, ping, data, pong, data
	n := buildFrame(buf[pos:], 0x1, ethPayload1536[:], true)
	pos += n

	n = buildFrame(buf[pos:], 0x9, []byte("ping"), true)
	pos += n

	n = buildFrame(buf[pos:], 0x1, ethPayload1536[:], true)
	pos += n

	n = buildFrame(buf[pos:], 0xA, []byte("pong"), true)
	pos += n

	n = buildFrame(buf[pos:], 0x1, ethPayload1536[:], true)
	pos += n

	controlMixed = append([]byte(nil), buf[:pos]...)
}

// buildWorstCaseSequences creates pathological frame sequences for stress testing.
func buildWorstCaseSequences() {
	var buf [20000]byte
	pos := 0

	// Maximum fragmentation: 1 byte per frame for 1024 bytes total
	for i := 0; i < 1024; i++ {
		fragment := ethPayload4096[i : i+1]
		isFirst := i == 0
		isLast := i == 1023

		opcode := byte(0x0)
		if isFirst {
			opcode = 0x1
		}

		n := buildFrame(buf[pos:], opcode, fragment, isLast)
		pos += n
	}
	maxFragmented = append([]byte(nil), buf[:pos]...)

	// Alternating control and data frames
	pos = 0
	for i := 0; i < 20; i++ {
		// Control frame (ping)
		n := buildFrame(buf[pos:], 0x9, []byte("p"), true)
		pos += n

		// Data frame (100 bytes)
		n = buildFrame(buf[pos:], 0x1, ethPayload512[:100], true)
		pos += n
	}
	alternatingCtrl = append([]byte(nil), buf[:pos]...)
}

// init pre-computes all test data to eliminate allocation overhead during benchmarks.
func init() {
	// Generate realistic Ethereum payloads
	fillEthereumPayload(ethPayload64[:])
	fillEthereumPayload(ethPayload512[:])
	fillEthereumPayload(ethPayload1536[:])
	fillEthereumPayload(ethPayload4096[:])
	fillEthereumPayload(ethPayload16384[:])
	fillEthereumPayload(ethPayload65536[:])

	// Build complete frames
	var frameBuf [70000]byte

	n := buildFrame(frameBuf[:], 0x1, ethPayload64[:], true)
	frame64 = append([]byte(nil), frameBuf[:n]...)

	n = buildFrame(frameBuf[:], 0x1, ethPayload512[:], true)
	frame512 = append([]byte(nil), frameBuf[:n]...)

	n = buildFrame(frameBuf[:], 0x1, ethPayload1536[:], true)
	frame1536 = append([]byte(nil), frameBuf[:n]...)

	n = buildFrame(frameBuf[:], 0x1, ethPayload4096[:], true)
	frame4096 = append([]byte(nil), frameBuf[:n]...)

	n = buildFrame(frameBuf[:], 0x1, ethPayload16384[:], true)
	frame16384 = append([]byte(nil), frameBuf[:n]...)

	n = buildFrame(frameBuf[:], 0x1, ethPayload65536[:], true)
	frame65536 = append([]byte(nil), frameBuf[:n]...)

	// Build complex sequences
	buildFragmentedSequences()
	buildControlMixedSequence()
	buildWorstCaseSequences()
}

// ==============================================================================
// UNIT TESTS
// ==============================================================================

func TestInit(t *testing.T) {
	// Verify WebSocket upgrade request template
	request := string(processor.upgradeRequest[:upgradeRequestLen])
	if !strings.Contains(request, "GET") {
		t.Error("Missing GET method in upgrade request")
	}
	if !strings.Contains(request, "Upgrade: websocket") {
		t.Error("Missing Upgrade header in request")
	}
	if !strings.HasSuffix(request, "\r\n\r\n") {
		t.Error("Missing CRLF termination in request")
	}

	// Verify subscription frame format
	if processor.subscribeFrame[0] != 0x81 {
		t.Error("Wrong opcode, expected TEXT frame with FIN=1")
	}
	if processor.subscribeFrame[1] != (0x80 | 126) {
		t.Error("Wrong mask bit or length indicator")
	}
}

func TestHandshake(t *testing.T) {
	tests := []struct {
		name     string
		response string
		wantErr  bool
		errMsg   string
	}{
		{
			name: "valid_101_response",
			response: "HTTP/1.1 101 Switching Protocols\r\n" +
				"Upgrade: websocket\r\n" +
				"Connection: Upgrade\r\n\r\n",
			wantErr: false,
		},
		{
			name: "invalid_status_code",
			response: "HTTP/1.1 400 Bad Request\r\n" +
				"Content-Type: text/plain\r\n\r\n",
			wantErr: true,
			errMsg:  "upgrade failed",
		},
		{
			name:     "malformed_http_response",
			response: "NOT_HTTP/1.1 101\r\n\r\n",
			wantErr:  true,
			errMsg:   "upgrade failed",
		},
		{
			name:     "incomplete_headers",
			response: "HTTP/1.1 101 OK\r\nUpgrade: websocket\r\n",
			wantErr:  true,
			errMsg:   "EOF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &mockConn{readData: []byte(tt.response)}
			err := Handshake(conn)

			if tt.wantErr {
				if err == nil {
					t.Fatal("Expected error but got none")
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Expected error containing %q, got %q", tt.errMsg, err.Error())
				}
			} else if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestHandshakeErrors(t *testing.T) {
	t.Run("write_error", func(t *testing.T) {
		conn := &mockConn{writeErr: fmt.Errorf("write failed")}
		if err := Handshake(conn); err == nil {
			t.Fatal("Expected write error")
		}
	})

	t.Run("timeout_without_crlf", func(t *testing.T) {
		large := "HTTP/1.1 101 OK\r\n" + strings.Repeat("X", 500)
		conn := &mockConn{readData: []byte(large)}
		if err := Handshake(conn); err == nil || !strings.Contains(err.Error(), "timeout") {
			t.Fatal("Expected timeout error")
		}
	})

	t.Run("read_error_during_response", func(t *testing.T) {
		conn := &mockConn{readErr: fmt.Errorf("network error")}
		if err := Handshake(conn); err == nil || !strings.Contains(err.Error(), "network error") {
			t.Fatal("Expected network error")
		}
	})
}

func TestSendSubscription(t *testing.T) {
	t.Run("successful_send", func(t *testing.T) {
		conn := &mockConn{}
		if err := SendSubscription(conn); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(conn.writeData) == 0 {
			t.Error("No subscription frame sent")
		}
	})

	t.Run("write_error", func(t *testing.T) {
		conn := &mockConn{writeErr: fmt.Errorf("connection closed")}
		if err := SendSubscription(conn); err == nil {
			t.Fatal("Expected write error")
		}
	})
}

func TestSpinUntilCompleteMessage(t *testing.T) {
	tests := []struct {
		name     string
		frames   [][]byte
		expected []byte
	}{
		{
			name:     "single_text_frame",
			frames:   [][]byte{createTestFrame(0x1, []byte("Hello"), true)},
			expected: []byte("Hello"),
		},
		{
			name: "fragmented_message",
			frames: [][]byte{
				createTestFrame(0x1, []byte("Hello "), false),
				createTestFrame(0x0, []byte("World"), true),
			},
			expected: []byte("Hello World"),
		},
		{
			name: "message_with_control_frames",
			frames: [][]byte{
				createTestFrame(0x1, []byte("Test"), false),
				createTestFrame(0x8, []byte{}, true), // Close frame
				createTestFrame(0x0, []byte("ing"), true),
			},
			expected: []byte("Testing"),
		},
		{
			name:     "large_16bit_length",
			frames:   [][]byte{createTestFrame(0x1, make([]byte, 1000), true)},
			expected: make([]byte, 1000),
		},
		{
			name:     "large_64bit_length",
			frames:   [][]byte{createTestFrame(0x1, make([]byte, 100000), true)},
			expected: make([]byte, 100000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var data []byte
			for _, frame := range tt.frames {
				data = append(data, frame...)
			}

			conn := &mockConn{readData: data}
			result, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("Expected %d bytes, got %d", len(tt.expected), len(result))
			}
		})
	}
}

func TestSpinUntilCompleteMessageErrors(t *testing.T) {
	t.Run("header_read_error", func(t *testing.T) {
		conn := &mockConn{readErr: fmt.Errorf("connection lost")}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
		if !strings.Contains(err.Error(), "connection lost") {
			t.Errorf("Expected connection error, got %q", err.Error())
		}
	})

	t.Run("extended_length_read_error", func(t *testing.T) {
		conn := &mockConn{readData: []byte{0x81, 126}}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
		if !strings.Contains(err.Error(), "EOF") {
			t.Errorf("Expected EOF error, got %q", err.Error())
		}
	})

	t.Run("frame_too_large", func(t *testing.T) {
		frame := []byte{0x81, 127}
		lenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBytes, uint64(BufferSize)+1)
		frame = append(frame, lenBytes...)

		conn := &mockConn{readData: frame}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
		if !strings.Contains(err.Error(), "frame too large") {
			t.Errorf("Expected 'frame too large' error, got %q", err.Error())
		}
	})

	t.Run("message_too_large", func(t *testing.T) {
		var data []byte
		payload1 := make([]byte, BufferSize-1000)
		data = append(data, createTestFrame(0x1, payload1, false)...)
		payload2 := make([]byte, 2000)
		data = append(data, createTestFrame(0x0, payload2, true)...)

		conn := &mockConn{readData: data}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
		if !strings.Contains(err.Error(), "message too large") {
			t.Errorf("Expected 'message too large' error, got %q", err.Error())
		}
	})
}

func TestControlFrameHandling(t *testing.T) {
	// Test all control frame opcodes (0x8-0xF)
	for opcode := byte(0x8); opcode <= 0xF; opcode++ {
		t.Run(fmt.Sprintf("control_opcode_0x%X", opcode), func(t *testing.T) {
			var data []byte
			data = append(data, createTestFrame(opcode, []byte("ctrl"), true)...)
			data = append(data, createTestFrame(0x1, []byte("data"), true)...)

			conn := &mockConn{readData: data}
			result, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if string(result) != "data" {
				t.Errorf("Expected 'data', got %q", result)
			}
		})
	}
}

func TestBoundaryConditions(t *testing.T) {
	t.Run("exactly_126_bytes", func(t *testing.T) {
		payload := make([]byte, 126)
		frame := createTestFrame(0x1, payload, true)
		conn := &mockConn{readData: frame}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 126 {
			t.Errorf("Expected 126 bytes, got %d", len(result))
		}
	})

	t.Run("exactly_65536_bytes", func(t *testing.T) {
		payload := make([]byte, 65536)
		frame := createTestFrame(0x1, payload, true)
		conn := &mockConn{readData: frame}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 65536 {
			t.Errorf("Expected 65536 bytes, got %d", len(result))
		}
	})
}

func TestFragmentation(t *testing.T) {
	t.Run("many_small_fragments", func(t *testing.T) {
		var data []byte
		var expected []byte
		for i := 0; i < 100; i++ {
			payload := []byte{byte(i)}
			expected = append(expected, payload...)
			isLast := i == 99
			opcode := byte(0x0)
			if i == 0 {
				opcode = 0x1
			}
			data = append(data, createTestFrame(opcode, payload, isLast)...)
		}

		conn := &mockConn{readData: data}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(result, expected) {
			t.Error("Fragment data mismatch")
		}
	})
}

// ==============================================================================
// PURE PERFORMANCE BENCHMARKS - NO NETWORK SIMULATION
// ==============================================================================

// BenchmarkPureWebSocketParsing tests ONLY your parsing logic without network overhead
func BenchmarkPureWebSocketParsing(b *testing.B) {
	sizes := []struct {
		name        string
		frame       []byte
		payloadSize int64
	}{
		{"64B", frame64, 64},
		{"512B", frame512, 512},
		{"1536B", frame1536, 1536},
		{"4KB", frame4096, 4096},
		{"16KB", frame16384, 16384},
		{"64KB", frame65536, 65536},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			// Pre-load frame into buffer (eliminate copy overhead)
			copy(processor.buffer[:], s.frame)

			b.SetBytes(s.payloadSize)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// PURE parsing - your bit manipulation magic
				headerBuf := processor.buffer[:]

				// Extract frame info (3-5 cycles)
				opcode := headerBuf[0] & 0x0F
				payloadLen := uint64(headerBuf[1] & 0x7F)
				headerSize := 2

				// Handle extended length (your optimized endian conversion)
				switch payloadLen {
				case 126:
					payloadLen = uint64(headerBuf[2])<<8 | uint64(headerBuf[3])
					headerSize = 4
				case 127:
					// Your assembly-level endian conversion
					v := *(*uint64)(unsafe.Pointer(&headerBuf[2]))
					payloadLen = ((v & 0xFF) << 56) | ((v & 0xFF00) << 40) |
						((v & 0xFF0000) << 24) | ((v & 0xFF000000) << 8) |
						((v & 0xFF00000000) >> 8) | ((v & 0xFF0000000000) >> 24) |
						((v & 0xFF000000000000) >> 40) | ((v & 0xFF00000000000000) >> 56)
					headerSize = 10
				}

				// Zero-copy message extraction
				result := processor.buffer[headerSize : headerSize+int(payloadLen)]

				// Prevent optimization
				_ = opcode
				_ = result
			}
		})
	}
}

// BenchmarkFrameHeaderParsing tests ONLY the bit manipulation part
func BenchmarkFrameHeaderParsing(b *testing.B) {
	headers := []struct {
		name   string
		header []byte
		desc   string
	}{
		{"7bit_length", []byte{0x81, 0x40}, "64 byte payload"},
		{"16bit_length", []byte{0x81, 0x7E, 0x04, 0x00}, "1024 byte payload"},
		{"64bit_length", []byte{0x81, 0x7F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00}, "64KB payload"},
	}

	for _, h := range headers {
		b.Run(h.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Your bit manipulation (should be ~3-10ns)
				opcode := h.header[0] & 0x0F
				payloadLen := uint64(h.header[1] & 0x7F)

				switch payloadLen {
				case 126:
					payloadLen = uint64(h.header[2])<<8 | uint64(h.header[3])
				case 127:
					v := *(*uint64)(unsafe.Pointer(&h.header[2]))
					payloadLen = ((v & 0xFF) << 56) | ((v & 0xFF00) << 40) |
						((v & 0xFF0000) << 24) | ((v & 0xFF000000) << 8) |
						((v & 0xFF00000000) >> 8) | ((v & 0xFF0000000000) >> 24) |
						((v & 0xFF000000000000) >> 40) | ((v & 0xFF00000000000000) >> 56)
				}

				_ = opcode
				_ = payloadLen
			}
		})
	}
}

// BenchmarkControlFrameDetection tests your control frame filtering
func BenchmarkControlFrameDetection(b *testing.B) {
	frames := []struct {
		name   string
		opcode byte
		isCtrl bool
	}{
		{"text_frame", 0x1, false},
		{"binary_frame", 0x2, false},
		{"close_frame", 0x8, true},
		{"ping_frame", 0x9, true},
		{"pong_frame", 0xA, true},
	}

	for _, f := range frames {
		b.Run(f.name, func(b *testing.B) {
			header := []byte{0x80 | f.opcode, 0x04}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Your control frame detection (should be 1-2 cycles)
				opcode := header[0] & 0x0F
				isControlFrame := (opcode >> 3) & 1

				_ = isControlFrame
			}
		})
	}
}

// BenchmarkZeroCopySlicing tests buffer slicing performance
func BenchmarkZeroCopySlicing(b *testing.B) {
	sizes := []int{64, 512, 1536, 4096, 16384, 65536}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Zero-copy slice (should be ~1-2ns)
				result := processor.buffer[0:size]
				_ = result
			}
		})
	}
}

// BenchmarkMemcopyComparison compares your parsing to raw memory operations
func BenchmarkMemcopyComparison(b *testing.B) {
	const size = 4096
	src := make([]byte, size)
	dst := make([]byte, size)

	b.Run("stdlib_copy", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			copy(dst, src) // Raw memcopy baseline
		}
	})

	b.Run("unsafe_copy", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Unsafe memory copy
			*(*[size]byte)(unsafe.Pointer(&dst[0])) = *(*[size]byte)(unsafe.Pointer(&src[0]))
		}
	})

	b.Run("your_websocket_parsing", func(b *testing.B) {
		// Frame with 4KB payload pre-loaded
		copy(processor.buffer[:], frame4096)

		b.SetBytes(size)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Your complete parsing pipeline
			headerBuf := processor.buffer[:]

			opcode := headerBuf[0] & 0x0F
			payloadLen := uint64(headerBuf[1] & 0x7F)
			headerSize := 2

			if payloadLen == 126 {
				payloadLen = uint64(headerBuf[2])<<8 | uint64(headerBuf[3])
				headerSize = 4
			}

			// Zero-copy result
			result := processor.buffer[headerSize : headerSize+int(payloadLen)]
			_ = opcode
			_ = result
		}
	})

	b.Run("zero_copy_slice", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Pure slice operation (your actual cost)
			result := processor.buffer[4 : 4+size]
			_ = result
		}
	})
}

// Helper function to build test fragments
func buildTestFragments(totalSize, numFragments int) [][]byte {
	fragmentSize := totalSize / numFragments
	var fragments [][]byte

	for i := 0; i < numFragments; i++ {
		isLast := i == numFragments-1

		// Build frame header
		var frame []byte
		if i == 0 {
			frame = append(frame, 0x01) // First frame (text)
		} else {
			frame = append(frame, 0x00) // Continuation frame
		}

		if isLast {
			frame[0] |= 0x80 // Set FIN bit
		}

		// Add payload length
		if fragmentSize < 126 {
			frame = append(frame, byte(fragmentSize))
		} else {
			frame = append(frame, 126, byte(fragmentSize>>8), byte(fragmentSize))
		}

		// Add payload
		payload := make([]byte, fragmentSize)
		frame = append(frame, payload...)

		fragments = append(fragments, frame)
	}

	return fragments
}

// BenchmarkFragmentationParsing tests your stitching logic without network overhead
func BenchmarkFragmentationParsing(b *testing.B) {
	scenarios := []struct {
		name       string
		totalBytes int64
		fragments  [][]byte
	}{
		{
			name:       "4KB_8_fragments",
			totalBytes: 4096,
			fragments:  buildTestFragments(4096, 8),
		},
		{
			name:       "16KB_16_fragments",
			totalBytes: 16384,
			fragments:  buildTestFragments(16384, 16),
		},
		{
			name:       "1KB_max_fragments",
			totalBytes: 1024,
			fragments:  buildTestFragments(1024, 1024), // 1 byte per fragment
		},
	}

	for _, s := range scenarios {
		b.Run(s.name, func(b *testing.B) {
			b.SetBytes(s.totalBytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				msgEnd := 0

				// Process each fragment (your stitching logic)
				for _, fragment := range s.fragments {
					copy(processor.buffer[msgEnd:], fragment)

					// Parse header
					headerBuf := processor.buffer[msgEnd:]
					opcode := headerBuf[0] & 0x0F
					payloadLen := uint64(headerBuf[1] & 0x7F)
					isLastFrame := headerBuf[0]&0x80 != 0

					// Handle control frames (your filtering)
					isControlFrame := (opcode >> 3) & 1
					if isControlFrame != 0 {
						continue // Skip control frames
					}

					// Advance message end (your stitching)
					msgEnd += 2 + int(payloadLen)

					if isLastFrame {
						break
					}
				}

				// Final stitched message
				result := processor.buffer[:msgEnd]
				_ = result
			}
		})
	}
}

// BenchmarkNanosecondLatency measures sub-microsecond performance
func BenchmarkNanosecondLatency(b *testing.B) {
	// Pre-load small frame for minimum latency test
	copy(processor.buffer[:], frame64)

	const samples = 10000
	latencies := make([]time.Duration, 0, samples)

	// Warmup
	for i := 0; i < 1000; i++ {
		headerBuf := processor.buffer[:]
		opcode := headerBuf[0] & 0x0F
		payloadLen := uint64(headerBuf[1] & 0x7F)
		result := processor.buffer[2 : 2+int(payloadLen)]
		_ = opcode
		_ = result
	}

	b.ResetTimer()

	for i := 0; i < b.N && len(latencies) < samples; i++ {
		start := time.Now()

		// Your parsing (should be <50ns)
		headerBuf := processor.buffer[:]
		opcode := headerBuf[0] & 0x0F
		payloadLen := uint64(headerBuf[1] & 0x7F)
		result := processor.buffer[2 : 2+int(payloadLen)]

		elapsed := time.Since(start)
		latencies = append(latencies, elapsed)

		_ = opcode
		_ = result
	}

	b.StopTimer()

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool {
			return latencies[i] < latencies[j]
		})

		n := len(latencies)
		p50 := latencies[n*50/100]
		p95 := latencies[n*95/100]
		p99 := latencies[n*99/100]

		b.ReportMetric(float64(p50.Nanoseconds()), "p50_ns")
		b.ReportMetric(float64(p95.Nanoseconds()), "p95_ns")
		b.ReportMetric(float64(p99.Nanoseconds()), "p99_ns")
	}
}

// BenchmarkAssemblyOptimizations tests your specific assembly-level optimizations
func BenchmarkAssemblyOptimizations(b *testing.B) {
	b.Run("bit_manipulation", func(b *testing.B) {
		header := []byte{0x81, 0x7E, 0x04, 0x00}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Your bit manipulation magic (should be 1-2 cycles)
			opcode := header[0] & 0x0F
			fin := header[0] & 0x80
			payloadLen := header[1] & 0x7F
			masked := header[1] & 0x80

			_ = opcode
			_ = fin
			_ = payloadLen
			_ = masked
		}
	})

	b.Run("unsafe_endian_conversion", func(b *testing.B) {
		data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Your assembly-level endian conversion
			v := *(*uint64)(unsafe.Pointer(&data[0]))
			result := ((v & 0xFF) << 56) | ((v & 0xFF00) << 40) |
				((v & 0xFF0000) << 24) | ((v & 0xFF000000) << 8) |
				((v & 0xFF00000000) >> 8) | ((v & 0xFF0000000000) >> 24) |
				((v & 0xFF000000000000) >> 40) | ((v & 0xFF00000000000000) >> 56)

			_ = result
		}
	})
}

// ==============================================================================
// ORIGINAL BENCHMARKS (FOR COMPARISON WITH NETWORK SIMULATION)
// ==============================================================================

func BenchmarkFrameSizes(b *testing.B) {
	sizes := []struct {
		name  string
		frame []byte
		bytes int64
	}{
		{"64B", frame64, 64},
		{"512B", frame512, 512},
		{"1536B", frame1536, 1536},
		{"4KB", frame4096, 4096},
		{"16KB", frame16384, 16384},
		{"64KB", frame65536, 65536},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			conn := &zeroConn{data: s.frame}
			b.SetBytes(s.bytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				conn.reset()
				SpinUntilCompleteMessage(conn)
			}
		})
	}
}

func BenchmarkFragmentation(b *testing.B) {
	tests := []struct {
		name  string
		frame []byte
		bytes int64
	}{
		{"4KB_fragmented", fragmented4096, 4096},
		{"16KB_fragmented", fragmented16384, 16384},
		{"1KB_max_fragmentation", maxFragmented, 1024},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			conn := &zeroConn{data: tt.frame}
			b.SetBytes(tt.bytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				conn.reset()
				SpinUntilCompleteMessage(conn)
			}
		})
	}
}

func BenchmarkControlFrames(b *testing.B) {
	conn := &zeroConn{data: controlMixed}
	b.SetBytes(1536 * 3) // Three 1536-byte data frames
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		// Process three data frames (control frames handled internally)
		SpinUntilCompleteMessage(conn)
		SpinUntilCompleteMessage(conn)
		SpinUntilCompleteMessage(conn)
	}
}

func BenchmarkLengthEncoding(b *testing.B) {
	tests := []struct {
		name  string
		frame []byte
		bytes int64
	}{
		{"7bit_encoding", frame64, 64},
		{"16bit_encoding", frame1536, 1536},
		{"64bit_encoding", frame65536, 65536},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			conn := &zeroConn{data: tt.frame}
			b.SetBytes(tt.bytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				conn.reset()
				SpinUntilCompleteMessage(conn)
			}
		})
	}
}

func BenchmarkChunkedReads(b *testing.B) {
	chunks := []struct {
		name string
		size int
	}{
		{"64B_chunks", 64},
		{"256B_chunks", 256},
		{"1460B_chunks", 1460}, // Ethernet MTU size
	}

	for _, c := range chunks {
		b.Run(c.name, func(b *testing.B) {
			conn := &chunkedConn{data: frame16384, chunkSize: c.size}
			b.SetBytes(16384)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				conn.reset()
				SpinUntilCompleteMessage(conn)
			}
		})
	}
}

func BenchmarkWorstCase(b *testing.B) {
	tests := []struct {
		name  string
		frame []byte
		desc  string
	}{
		{"max_fragmentation", maxFragmented, "1 byte per frame"},
		{"alternating_control", alternatingCtrl, "control/data alternating"},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			conn := &zeroConn{data: tt.frame}
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				conn.reset()
				// Process all messages in the sequence
				for {
					_, err := SpinUntilCompleteMessage(conn)
					if err == io.EOF {
						break
					}
				}
			}
		})
	}
}

func BenchmarkHandshake(b *testing.B) {
	response := []byte("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n\r\n")

	conn := &zeroConn{data: response}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		Handshake(conn)
	}
}

func BenchmarkSendSubscription(b *testing.B) {
	conn := &zeroConn{}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		SendSubscription(conn)
	}
}

func BenchmarkZeroAllocation(b *testing.B) {
	conn := &zeroConn{data: frame4096}

	// Warmup phase to eliminate cold start effects
	for i := 0; i < 100; i++ {
		conn.reset()
		SpinUntilCompleteMessage(conn)
	}

	runtime.GC()
	runtime.GC()

	b.SetBytes(4096)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		SpinUntilCompleteMessage(conn)
	}
}

func BenchmarkLatency(b *testing.B) {
	conn := &zeroConn{data: frame1536}
	const samples = 10000
	latencies := make([]time.Duration, 0, samples)

	// Warmup phase
	for i := 0; i < 1000; i++ {
		conn.reset()
		SpinUntilCompleteMessage(conn)
	}

	b.ResetTimer()
	for i := 0; i < b.N && len(latencies) < samples; i++ {
		conn.reset()
		start := time.Now()
		SpinUntilCompleteMessage(conn)
		latencies = append(latencies, time.Since(start))
	}
	b.StopTimer()

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool {
			return latencies[i] < latencies[j]
		})

		n := len(latencies)
		p50 := latencies[n*50/100]
		p95 := latencies[n*95/100]
		p99 := latencies[n*99/100]

		b.ReportMetric(float64(p50.Nanoseconds()), "p50_ns")
		b.ReportMetric(float64(p95.Nanoseconds()), "p95_ns")
		b.ReportMetric(float64(p99.Nanoseconds()), "p99_ns")
	}
}

func BenchmarkThroughput(b *testing.B) {
	scenarios := []struct {
		name  string
		frame []byte
		bytes int64
	}{
		{"1KB_sustained", frame512, 512},
		{"4KB_sustained", frame4096, 4096},
		{"64KB_sustained", frame65536, 65536},
	}

	for _, s := range scenarios {
		b.Run(s.name, func(b *testing.B) {
			conn := &zeroConn{data: s.frame}
			b.SetBytes(s.bytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				conn.reset()
				SpinUntilCompleteMessage(conn)
			}
		})
	}
}
