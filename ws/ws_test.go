package ws

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

// ============================================================================
// TEST CONFIGURATION
// ============================================================================

var (
	// Pre-built test payloads for realistic Ethereum JSON-RPC data
	ethPayload512   [512]byte
	ethPayload4096  [4096]byte
	ethPayload16384 [16384]byte
	ethPayload65536 [65536]byte

	// Pre-built WebSocket frames for benchmarking
	frame512   []byte
	frame4096  []byte
	frame16384 []byte
	frame65536 []byte
)

func init() {
	// Initialize test data with realistic Ethereum payloads
	fillEthereumPayload(ethPayload512[:])
	fillEthereumPayload(ethPayload4096[:])
	fillEthereumPayload(ethPayload16384[:])
	fillEthereumPayload(ethPayload65536[:])

	// Pre-build frames for zero-allocation benchmarks
	frame512 = createTestFrame(0x1, ethPayload512[:], true)
	frame4096 = createTestFrame(0x1, ethPayload4096[:], true)
	frame16384 = createTestFrame(0x1, ethPayload16384[:], true)
	frame65536 = createTestFrame(0x1, ethPayload65536[:], true)
}

// ============================================================================
// TEST UTILITIES
// ============================================================================

// mockConn provides controllable network connection behavior for testing
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
		return 0, io.EOF
	}
	n := copy(b, m.readData[m.readPos:])
	m.readPos += n
	return n, nil
}

func (m *mockConn) Write(b []byte) (int, error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error                       { m.closed = true; return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// benchConn provides zero-allocation connection for benchmarks
type benchConn struct {
	data []byte
	pos  int
}

func (z *benchConn) Read(b []byte) (int, error) {
	if z.pos >= len(z.data) {
		return 0, io.EOF
	}
	n := copy(b, z.data[z.pos:])
	z.pos += n
	return n, nil
}

func (z *benchConn) Write(b []byte) (int, error)        { return len(b), nil }
func (z *benchConn) Close() error                       { return nil }
func (z *benchConn) LocalAddr() net.Addr                { return nil }
func (z *benchConn) RemoteAddr() net.Addr               { return nil }
func (z *benchConn) SetDeadline(t time.Time) error      { return nil }
func (z *benchConn) SetReadDeadline(t time.Time) error  { return nil }
func (z *benchConn) SetWriteDeadline(t time.Time) error { return nil }
func (z *benchConn) reset()                             { z.pos = 0 }

// createTestFrame builds a WebSocket frame with given parameters
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

// fillEthereumPayload creates realistic Ethereum JSON-RPC payload
func fillEthereumPayload(buf []byte) {
	template := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x123","result":{"address":"0xa0b86a33e624e826","data":"0x`
	pos := copy(buf, template)

	hexChars := "0123456789abcdef"
	for i := pos; i < len(buf)-3; i++ {
		buf[i] = hexChars[i&15]
	}

	if len(buf) >= 3 {
		copy(buf[len(buf)-3:], `"}}`)
	}
}

// ============================================================================
// INITIALIZATION TESTS
// ============================================================================

// TestProcessorInitialization validates proper static initialization
func TestProcessorInitialization(t *testing.T) {
	t.Run("upgrade_request_structure", func(t *testing.T) {
		request := string(processor.upgradeRequest[:upgradeRequestLen])

		expectedHeaders := []string{
			"GET", "HTTP/1.1", "Upgrade: websocket",
			"Connection: Upgrade", "Sec-WebSocket-Key:",
			"Sec-WebSocket-Version: 13",
		}

		for _, header := range expectedHeaders {
			if !strings.Contains(request, header) {
				t.Errorf("Missing header: %s", header)
			}
		}

		if !strings.HasSuffix(request, "\r\n\r\n") {
			t.Error("Missing CRLF termination")
		}
	})

	t.Run("subscribe_frame_structure", func(t *testing.T) {
		if processor.subscribeFrame[0] != 0x81 {
			t.Errorf("Wrong opcode: expected 0x81, got 0x%02X", processor.subscribeFrame[0])
		}

		if processor.subscribeFrame[1]&0x80 == 0 {
			t.Error("Missing mask bit")
		}

		// Verify payload is properly masked
		maskKey := processor.subscribeFrame[4:8]
		maskedPayload := processor.subscribeFrame[8:subscribeFrameLen]

		for i := 0; i < len(subscribePayload); i++ {
			unmasked := maskedPayload[i] ^ maskKey[i&3]
			if unmasked != subscribePayload[i] {
				t.Errorf("Payload masking error at position %d", i)
				break
			}
		}
	})

	t.Run("buffer_size", func(t *testing.T) {
		if len(processor.buffer) != BufferSize {
			t.Errorf("Buffer size incorrect: expected %d, got %d", BufferSize, len(processor.buffer))
		}
	})
}

// ============================================================================
// HANDSHAKE TESTS
// ============================================================================

// TestHandshake validates WebSocket upgrade negotiation
func TestHandshake(t *testing.T) {
	validTests := []struct {
		name     string
		response string
	}{
		{
			name: "standard_101_response",
			response: "HTTP/1.1 101 Switching Protocols\r\n" +
				"Upgrade: websocket\r\n" +
				"Connection: Upgrade\r\n" +
				"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n",
		},
		{
			name:     "minimal_101",
			response: "HTTP/1.1 101 OK\r\n\r\n",
		},
		{
			name:     "with_extra_headers",
			response: "HTTP/1.1 101 OK\r\n" + strings.Repeat("X-Header: value\r\n", 10) + "\r\n",
		},
	}

	for _, tt := range validTests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &mockConn{readData: []byte(tt.response)}
			if err := Handshake(conn); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Verify upgrade request was sent
			if !bytes.Equal(conn.writeData, processor.upgradeRequest[:upgradeRequestLen]) {
				t.Error("Incorrect upgrade request sent")
			}
		})
	}

	invalidTests := []struct {
		name     string
		response string
		errMsg   string
	}{
		{
			name:     "wrong_status_code",
			response: "HTTP/1.1 200 OK\r\n\r\n",
			errMsg:   "upgrade failed",
		},
		{
			name:     "malformed_http",
			response: "NOT_HTTP/1.1 101\r\n\r\n",
			errMsg:   "upgrade failed",
		},
		{
			name:     "timeout_no_termination",
			response: "HTTP/1.1 101 OK\r\n" + strings.Repeat("X", 500),
			errMsg:   "timeout",
		},
	}

	for _, tt := range invalidTests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &mockConn{readData: []byte(tt.response)}
			err := Handshake(conn)
			if err == nil || !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("Expected error containing %q, got %v", tt.errMsg, err)
			}
		})
	}

	t.Run("write_error", func(t *testing.T) {
		conn := &mockConn{writeErr: fmt.Errorf("write failed")}
		if err := Handshake(conn); err == nil {
			t.Error("Expected write error")
		}
	})

	t.Run("read_error", func(t *testing.T) {
		conn := &mockConn{readErr: fmt.Errorf("network error")}
		if err := Handshake(conn); err == nil {
			t.Error("Expected read error")
		}
	})
}

// ============================================================================
// SUBSCRIPTION TESTS
// ============================================================================

// TestSendSubscription validates subscription frame transmission
func TestSendSubscription(t *testing.T) {
	t.Run("successful_send", func(t *testing.T) {
		conn := &mockConn{}
		if err := SendSubscription(conn); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if len(conn.writeData) != subscribeFrameLen {
			t.Errorf("Expected %d bytes, got %d", subscribeFrameLen, len(conn.writeData))
		}

		if !bytes.Equal(conn.writeData, processor.subscribeFrame[:subscribeFrameLen]) {
			t.Error("Incorrect subscription frame sent")
		}
	})

	t.Run("write_error", func(t *testing.T) {
		conn := &mockConn{writeErr: fmt.Errorf("connection closed")}
		if err := SendSubscription(conn); err == nil {
			t.Error("Expected write error")
		}
	})
}

// ============================================================================
// WEBSOCKET FRAME PARSING TESTS
// ============================================================================

// TestSpinUntilCompleteMessage validates WebSocket frame parsing
func TestSpinUntilCompleteMessage(t *testing.T) {
	t.Run("basic_frames", func(t *testing.T) {
		tests := []struct {
			name     string
			frames   [][]byte
			expected string
		}{
			{
				name:     "single_text_frame",
				frames:   [][]byte{createTestFrame(0x1, []byte("Hello"), true)},
				expected: "Hello",
			},
			{
				name:     "empty_frame",
				frames:   [][]byte{createTestFrame(0x1, []byte{}, true)},
				expected: "",
			},
			{
				name: "fragmented_message",
				frames: [][]byte{
					createTestFrame(0x1, []byte("Hello "), false),
					createTestFrame(0x0, []byte("World"), true),
				},
				expected: "Hello World",
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
					t.Errorf("Unexpected error: %v", err)
				}
				if string(result) != tt.expected {
					t.Errorf("Expected %q, got %q", tt.expected, string(result))
				}
			})
		}
	})

	t.Run("payload_length_encoding", func(t *testing.T) {
		tests := []struct {
			name   string
			length int
		}{
			{"7bit_max", 125},
			{"16bit_min", 126},
			{"16bit_max", 65535},
			{"64bit_min", 65536},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				payload := make([]byte, tt.length)
				frame := createTestFrame(0x1, payload, true)

				conn := &mockConn{readData: frame}
				result, err := SpinUntilCompleteMessage(conn)
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if len(result) != tt.length {
					t.Errorf("Expected %d bytes, got %d", tt.length, len(result))
				}
			})
		}
	})
}

// ============================================================================
// CONTROL FRAME TESTS
// ============================================================================

// TestControlFrames validates control frame handling
func TestControlFrames(t *testing.T) {
	t.Run("control_frames_skipped", func(t *testing.T) {
		// Test control frames are properly skipped
		var data []byte
		data = append(data, createTestFrame(0x1, []byte("Start"), false)...)
		data = append(data, createTestFrame(0x9, []byte("ping"), true)...) // Ping
		data = append(data, createTestFrame(0x0, []byte("End"), true)...)

		conn := &mockConn{readData: data}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if string(result) != "StartEnd" {
			t.Errorf("Expected 'StartEnd', got %q", string(result))
		}
	})

	t.Run("control_frame_payload_handling", func(t *testing.T) {
		tests := []struct {
			name        string
			payloadSize int
			description string
		}{
			{"empty_payload", 0, "Control frame with no payload"},
			{"small_payload", 10, "Control frame with small payload"},
			{"exact_16_bytes", 16, "Control frame with exactly 16 bytes"},
			{"over_16_bytes", 50, "Control frame requiring multiple 16-byte reads"},
			{"max_control_payload", 125, "Control frame with maximum allowed payload"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var data []byte
				// Create control frame with specific payload size
				controlPayload := bytes.Repeat([]byte("X"), tt.payloadSize)
				data = append(data, createTestFrame(0x9, controlPayload, true)...) // Ping frame
				data = append(data, createTestFrame(0x1, []byte("data"), true)...) // Data frame

				conn := &mockConn{readData: data}
				result, err := SpinUntilCompleteMessage(conn)
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if string(result) != "data" {
					t.Errorf("Expected 'data', got %q", string(result))
				}
			})
		}
	})
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

// TestSpinUntilCompleteMessageErrors validates error conditions
func TestSpinUntilCompleteMessageErrors(t *testing.T) {
	t.Run("header_read_error", func(t *testing.T) {
		conn := &mockConn{readErr: fmt.Errorf("connection lost")}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil || !strings.Contains(err.Error(), "connection lost") {
			t.Errorf("Expected connection lost error, got: %v", err)
		}
	})

	t.Run("header_incomplete", func(t *testing.T) {
		// When trying to read 2 bytes but only 1 available and then EOF
		conn := &mockConn{
			readFunc: func(b []byte) (int, error) {
				if len(b) >= 2 {
					// Only fill 1 byte then return EOF
					b[0] = 0x81
					return 1, io.EOF
				}
				return 0, io.EOF
			},
		}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Error("Expected error but got none")
		}
	})

	t.Run("extended_length_16bit_error", func(t *testing.T) {
		callCount := 0
		conn := &mockConn{
			readFunc: func(b []byte) (int, error) {
				callCount++
				switch callCount {
				case 1:
					// First call: return header indicating 16-bit length
					if len(b) >= 2 {
						b[0] = 0x81
						b[1] = 126
						return 2, nil
					}
					return 0, fmt.Errorf("buffer too small")
				case 2:
					// Second call: fail when trying to read 16-bit length
					return 0, fmt.Errorf("connection interrupted")
				default:
					return 0, fmt.Errorf("too many calls")
				}
			},
		}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil || !strings.Contains(err.Error(), "connection interrupted") {
			t.Errorf("Expected connection interrupted error, got: %v", err)
		}
	})

	t.Run("extended_length_64bit_error", func(t *testing.T) {
		callCount := 0
		conn := &mockConn{
			readFunc: func(b []byte) (int, error) {
				callCount++
				switch callCount {
				case 1:
					// First call: return header indicating 64-bit length
					if len(b) >= 2 {
						b[0] = 0x81
						b[1] = 127
						return 2, nil
					}
					return 0, fmt.Errorf("buffer too small")
				case 2:
					// Second call: fail when trying to read 64-bit length
					return 0, fmt.Errorf("connection interrupted")
				default:
					return 0, fmt.Errorf("too many calls")
				}
			},
		}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil || !strings.Contains(err.Error(), "connection interrupted") {
			t.Errorf("Expected connection interrupted error, got: %v", err)
		}
	})

	t.Run("payload_read_error", func(t *testing.T) {
		callCount := 0
		conn := &mockConn{
			readFunc: func(b []byte) (int, error) {
				callCount++
				switch callCount {
				case 1:
					// First call: return header for 10-byte payload
					if len(b) >= 2 {
						b[0] = 0x81 // FIN=1, opcode=1 (text frame)
						b[1] = 10   // 10 bytes payload
						return 2, nil
					}
					return 0, fmt.Errorf("buffer too small")
				case 2:
					// Second call: fail during payload read
					return 0, fmt.Errorf("payload read failed")
				default:
					return 0, fmt.Errorf("too many calls")
				}
			},
		}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil || !strings.Contains(err.Error(), "payload read failed") {
			t.Errorf("Expected payload read failed error, got: %v", err)
		}
	})

	t.Run("control_frame_read_errors", func(t *testing.T) {
		t.Run("control_payload_read_error", func(t *testing.T) {
			callCount := 0
			conn := &mockConn{
				readFunc: func(b []byte) (int, error) {
					callCount++
					switch callCount {
					case 1:
						// Return header for ping frame with 20-byte payload
						if len(b) >= 2 {
							b[0] = 0x89 // FIN=1, opcode=9 (ping)
							b[1] = 20   // 20 bytes payload
							return 2, nil
						}
					case 2:
						// First control payload read (16 bytes) succeeds
						if len(b) >= 16 {
							for i := 0; i < 16; i++ {
								b[i] = byte('X')
							}
							return 16, nil
						}
					case 3:
						// Second control payload read (remaining 4 bytes) fails
						return 0, fmt.Errorf("control payload read failed")
					}
					return 0, fmt.Errorf("unexpected call")
				},
			}
			_, err := SpinUntilCompleteMessage(conn)
			if err == nil || !strings.Contains(err.Error(), "control payload read failed") {
				t.Errorf("Expected control payload read error, got: %v", err)
			}
		})

		t.Run("control_payload_partial_read", func(t *testing.T) {
			callCount := 0
			conn := &mockConn{
				readFunc: func(b []byte) (int, error) {
					callCount++
					switch callCount {
					case 1:
						// Return header for ping frame with 30-byte payload
						if len(b) >= 2 {
							b[0] = 0x89 // FIN=1, opcode=9 (ping)
							b[1] = 30   // 30 bytes payload
							return 2, nil
						}
					case 2:
						// First read returns only 10 bytes instead of requested 16
						if len(b) >= 10 {
							for i := 0; i < 10; i++ {
								b[i] = byte('X')
							}
							return 10, nil
						}
					case 3:
						// Second read fails
						return 0, fmt.Errorf("partial read failed")
					}
					return 0, fmt.Errorf("unexpected call")
				},
			}
			_, err := SpinUntilCompleteMessage(conn)
			if err == nil || !strings.Contains(err.Error(), "partial read failed") {
				t.Errorf("Expected partial read error, got: %v", err)
			}
		})
	})

	t.Run("size_limit_errors", func(t *testing.T) {
		t.Run("frame_too_large", func(t *testing.T) {
			frame := []byte{0x81, 127}
			lenBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(lenBytes, uint64(BufferSize)+1)
			frame = append(frame, lenBytes...)

			conn := &mockConn{readData: frame}
			_, err := SpinUntilCompleteMessage(conn)
			if err == nil || !strings.Contains(err.Error(), "frame too large") {
				t.Error("Expected 'frame too large' error")
			}
		})

		t.Run("message_too_large", func(t *testing.T) {
			var data []byte
			// First fragment takes most of buffer
			payload1 := make([]byte, BufferSize-1000)
			data = append(data, createTestFrame(0x1, payload1, false)...)
			// Second fragment exceeds buffer
			payload2 := make([]byte, 2000)
			data = append(data, createTestFrame(0x0, payload2, true)...)

			conn := &mockConn{readData: data}
			_, err := SpinUntilCompleteMessage(conn)
			if err == nil || !strings.Contains(err.Error(), "message too large") {
				t.Error("Expected 'message too large' error")
			}
		})
	})
}

// ============================================================================
// DATA INTEGRITY TESTS
// ============================================================================

// TestDataIntegrity validates data is not corrupted during processing
func TestDataIntegrity(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"binary_zeros", bytes.Repeat([]byte{0x00}, 1000)},
		{"binary_ones", bytes.Repeat([]byte{0xFF}, 1000)},
		{"alternating", bytes.Repeat([]byte{0xAA, 0x55}, 500)},
		{"sequential", func() []byte {
			b := make([]byte, 1000)
			for i := range b {
				b[i] = byte(i & 0xFF)
			}
			return b
		}()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			frame := createTestFrame(0x2, tc.data, true) // Binary frame
			conn := &mockConn{readData: frame}

			result, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !bytes.Equal(result, tc.data) {
				t.Error("Data corruption detected")
			}
		})
	}
}

// ============================================================================
// BENCHMARKS
// ============================================================================

// BenchmarkHandshake measures handshake operation performance
func BenchmarkHandshake(b *testing.B) {
	response := []byte("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n")

	conn := &benchConn{data: response}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		Handshake(conn)
	}
}

// BenchmarkSendSubscription measures subscription frame transmission performance
func BenchmarkSendSubscription(b *testing.B) {
	conn := &benchConn{}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		SendSubscription(conn)
	}
}

// BenchmarkFrameSizes measures frame parsing performance at different sizes
func BenchmarkFrameSizes(b *testing.B) {
	sizes := []struct {
		name  string
		frame []byte
		bytes int64
	}{
		{"512B", frame512, 512},
		{"4KB", frame4096, 4096},
		{"16KB", frame16384, 16384},
		{"64KB", frame65536, 65536},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			conn := &benchConn{data: s.frame}
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

// BenchmarkZeroAllocation validates zero-allocation message processing
func BenchmarkZeroAllocation(b *testing.B) {
	conn := &benchConn{data: frame4096}

	// Warmup and force GC
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
