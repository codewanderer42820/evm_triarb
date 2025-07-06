package ws

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// Mock connection for testing
type mockConn struct {
	readData  []byte
	readPos   int
	writeData []byte
	readErr   error
	writeErr  error
	closed    bool
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.readErr != nil {
		return 0, m.readErr
	}
	if m.readPos >= len(m.readData) {
		return 0, fmt.Errorf("EOF")
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

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// Helper to create WebSocket frame
func createFrame(opcode byte, payload []byte, fin bool) []byte {
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

func TestHandshake(t *testing.T) {
	tests := []struct {
		name        string
		response    string
		shouldError bool
		errorMsg    string
	}{
		{
			name: "Valid handshake",
			response: "HTTP/1.1 101 Switching Protocols\r\n" +
				"Upgrade: websocket\r\n" +
				"Connection: Upgrade\r\n" +
				"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n",
			shouldError: false,
		},
		{
			name: "Invalid status code",
			response: "HTTP/1.1 400 Bad Request\r\n" +
				"Content-Type: text/plain\r\n\r\n",
			shouldError: true,
			errorMsg:    "upgrade failed",
		},
		{
			name:        "Malformed HTTP",
			response:    "NOT_HTTP/1.1 101\r\n\r\n",
			shouldError: true,
			errorMsg:    "upgrade failed",
		},
		{
			name:        "Timeout (no CRLF)",
			response:    "HTTP/1.1 101 Switching Protocols",
			shouldError: true,
			errorMsg:    "handshake timeout",
		},
		{
			name:        "Read error",
			response:    "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &mockConn{
				readData: []byte(tt.response),
			}

			if tt.name == "Read error" {
				conn.readErr = fmt.Errorf("network error")
			}

			err := Handshake(conn)

			if tt.shouldError {
				if err == nil {
					t.Fatal("Expected error but got none")
				}
				if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}

			// Verify upgrade request was sent
			if !tt.shouldError || tt.name == "Read error" {
				if len(conn.writeData) == 0 {
					t.Error("No upgrade request sent")
				}
			}
		})
	}
}

func TestSendSubscription(t *testing.T) {
	tests := []struct {
		name      string
		writeErr  error
		shouldErr bool
	}{
		{
			name:      "Successful send",
			writeErr:  nil,
			shouldErr: false,
		},
		{
			name:      "Write error",
			writeErr:  fmt.Errorf("connection closed"),
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &mockConn{
				writeErr: tt.writeErr,
			}

			err := SendSubscription(conn)

			if tt.shouldErr {
				if err == nil {
					t.Fatal("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// Verify subscription frame was sent
				if len(conn.writeData) == 0 {
					t.Error("No subscription frame sent")
				}
			}
		})
	}
}

func TestSpinUntilCompleteMessage(t *testing.T) {
	tests := []struct {
		name        string
		frames      [][]byte
		expected    []byte
		shouldError bool
		errorMsg    string
	}{
		{
			name: "Single text frame",
			frames: [][]byte{
				createFrame(0x1, []byte("Hello World"), true),
			},
			expected: []byte("Hello World"),
		},
		{
			name: "Fragmented message",
			frames: [][]byte{
				createFrame(0x1, []byte("Hello "), false), // First fragment
				createFrame(0x0, []byte("World"), true),   // Final fragment
			},
			expected: []byte("Hello World"),
		},
		{
			name: "Message with control frame interleaved",
			frames: [][]byte{
				createFrame(0x1, []byte("Hello "), false), // First fragment
				createFrame(0x8, []byte{}, true),          // Close frame (control)
				createFrame(0x0, []byte("World"), true),   // Final fragment
			},
			expected: []byte("Hello World"),
		},
		{
			name: "Large message (16-bit length)",
			frames: [][]byte{
				createFrame(0x1, make([]byte, 1000), true),
			},
			expected: make([]byte, 1000),
		},
		{
			name: "Very large message (64-bit length)",
			frames: [][]byte{
				createFrame(0x1, make([]byte, 100000), true),
			},
			expected: make([]byte, 100000),
		},
		{
			name: "Ping frame only",
			frames: [][]byte{
				createFrame(0x9, []byte("ping"), true), // Ping frame
				createFrame(0x1, []byte("data"), true), // Actual data
			},
			expected: []byte("data"),
		},
		{
			name: "Pong frame with payload",
			frames: [][]byte{
				createFrame(0xA, []byte("pong payload"), true), // Pong frame
				createFrame(0x1, []byte("message"), true),      // Actual data
			},
			expected: []byte("message"),
		},
		{
			name: "Control frame with large payload",
			frames: [][]byte{
				createFrame(0x8, make([]byte, 500), true), // Close with large payload
				createFrame(0x1, []byte("test"), true),    // Actual data
			},
			expected: []byte("test"),
		},
		{
			name: "Multiple control frames",
			frames: [][]byte{
				createFrame(0x9, []byte("ping1"), true),
				createFrame(0xA, []byte("pong1"), true),
				createFrame(0x8, []byte{}, true),
				createFrame(0x1, []byte("final"), true),
			},
			expected: []byte("final"),
		},
		{
			name:        "Read error on header",
			frames:      [][]byte{},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var frameData []byte
			for _, frame := range tt.frames {
				frameData = append(frameData, frame...)
			}

			conn := &mockConn{
				readData: frameData,
			}

			if tt.name == "Read error on header" {
				conn.readErr = fmt.Errorf("connection closed")
			}

			result, err := SpinUntilCompleteMessage(conn)

			if tt.shouldError {
				if err == nil {
					t.Fatal("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !bytes.Equal(result, tt.expected) {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestSpinUntilCompleteMessage_EdgeCases(t *testing.T) {
	t.Run("Read error on extended length", func(t *testing.T) {
		// Create frame header with 16-bit length indicator but incomplete length
		frame := []byte{0x81, 126} // Missing the 2 length bytes

		conn := &mockConn{
			readData: frame,
		}

		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error for incomplete extended length")
		}
	})

	t.Run("Read error on 64-bit length", func(t *testing.T) {
		// Create frame header with 64-bit length indicator but incomplete length
		frame := []byte{0x81, 127, 0x00, 0x00} // Missing 4 more length bytes

		conn := &mockConn{
			readData: frame,
		}

		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error for incomplete 64-bit length")
		}
	})

	t.Run("Read error during payload", func(t *testing.T) {
		// Create valid header but incomplete payload
		frame := createFrame(0x1, []byte("Hello World"), true)
		frame = frame[:len(frame)-5] // Truncate payload

		conn := &mockConn{
			readData: frame,
		}

		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error for incomplete payload")
		}
	})

	t.Run("Buffer overflow protection", func(t *testing.T) {
		// This test ensures we don't write beyond buffer bounds
		largePayload := make([]byte, BufferSize+1000) // Larger than buffer
		frame := createFrame(0x1, largePayload, true)

		conn := &mockConn{
			readData: frame,
		}

		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Should only read up to buffer size
		if len(result) > BufferSize {
			t.Errorf("Result larger than buffer: %d > %d", len(result), BufferSize)
		}
	})
}

func TestInit(t *testing.T) {
	t.Run("Upgrade request format", func(t *testing.T) {
		request := string(upgradeRequest)

		// Verify required headers
		if !strings.Contains(request, "GET") {
			t.Error("Missing GET method")
		}
		if !strings.Contains(request, "Upgrade: websocket") {
			t.Error("Missing Upgrade header")
		}
		if !strings.Contains(request, "Connection: Upgrade") {
			t.Error("Missing Connection header")
		}
		if !strings.Contains(request, "Sec-WebSocket-Key:") {
			t.Error("Missing WebSocket key")
		}
		if !strings.Contains(request, "Sec-WebSocket-Version: 13") {
			t.Error("Missing WebSocket version")
		}
		if !strings.HasSuffix(request, "\r\n\r\n") {
			t.Error("Missing proper CRLF termination")
		}
	})

	t.Run("Subscribe frame format", func(t *testing.T) {
		if len(subscribeFrame) < 8 {
			t.Error("Subscribe frame too short")
		}

		// Verify frame structure
		if subscribeFrame[0] != 0x81 {
			t.Error("Wrong opcode, expected TEXT frame with FIN=1")
		}
		if subscribeFrame[1] != (0x80 | 126) {
			t.Error("Wrong mask bit or length indicator")
		}

		// Verify payload length
		expectedLen := len(`{"jsonrpc":"2.0","method":"eth_subscribe","params":["logs",{}],"id":1}`)
		actualLen := int(subscribeFrame[2])<<8 | int(subscribeFrame[3])
		if actualLen != expectedLen {
			t.Errorf("Wrong payload length: expected %d, got %d", expectedLen, actualLen)
		}
	})
}

// Benchmarks
func BenchmarkHandshake(b *testing.B) {
	validResponse := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := &mockConn{
			readData: []byte(validResponse),
		}
		err := Handshake(conn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSendSubscription(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := &mockConn{}
		err := SendSubscription(conn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSpinUntilCompleteMessage(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			payload := make([]byte, size)
			rand.Read(payload)
			frame := createFrame(0x1, payload, true)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				conn := &mockConn{
					readData: frame,
				}
				_, err := SpinUntilCompleteMessage(conn)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSpinUntilCompleteMessage_Fragmented(b *testing.B) {
	// Test fragmented message performance
	payload1 := make([]byte, 5000)
	payload2 := make([]byte, 5000)
	rand.Read(payload1)
	rand.Read(payload2)

	var frameData []byte
	frameData = append(frameData, createFrame(0x1, payload1, false)...)
	frameData = append(frameData, createFrame(0x0, payload2, true)...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := &mockConn{
			readData: frameData,
		}
		_, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSpinUntilCompleteMessage_WithControlFrames(b *testing.B) {
	payload := make([]byte, 1000)
	rand.Read(payload)

	var frameData []byte
	frameData = append(frameData, createFrame(0x9, []byte("ping"), true)...) // Ping
	frameData = append(frameData, createFrame(0xA, []byte("pong"), true)...) // Pong
	frameData = append(frameData, createFrame(0x1, payload, true)...)        // Data

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := &mockConn{
			readData: frameData,
		}
		_, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryOperations(b *testing.B) {
	b.Run("unsafe_pointer_cast", func(b *testing.B) {
		data := []byte("HTTP/1.1")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Benchmark the unsafe pointer operation
			_ = *(*uint64)(unsafe.Pointer(&data[0]))
		}
	})

	b.Run("manual_endian_conversion", func(b *testing.B) {
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v := *(*uint64)(unsafe.Pointer(&data[0]))
			_ = ((v & 0xFF) << 56) | ((v & 0xFF00) << 40) | ((v & 0xFF0000) << 24) | ((v & 0xFF000000) << 8) |
				((v & 0xFF00000000) >> 8) | ((v & 0xFF0000000000) >> 24) | ((v & 0xFF000000000000) >> 40) | ((v & 0xFF00000000000000) >> 56)
		}
	})

	b.Run("buffer_slice_creation", func(b *testing.B) {
		msgEnd := 1000
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = buffer[:msgEnd]
		}
	})
}

// Memory allocation benchmark
func BenchmarkMemoryAllocations(b *testing.B) {
	b.Run("zero_allocations", func(b *testing.B) {
		payload := make([]byte, 1000)
		frame := createFrame(0x1, payload, true)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			conn := &mockConn{
				readData: frame,
			}
			_, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Edge case coverage
func TestCoverageEdgeCases(t *testing.T) {
	t.Run("Exactly 126 byte payload", func(t *testing.T) {
		payload := make([]byte, 126)
		frame := createFrame(0x1, payload, true)

		conn := &mockConn{readData: frame}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 126 {
			t.Errorf("Expected 126 bytes, got %d", len(result))
		}
	})

	t.Run("Exactly 65536 byte payload", func(t *testing.T) {
		payload := make([]byte, 65536)
		frame := createFrame(0x1, payload, true)

		conn := &mockConn{readData: frame}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 65536 {
			t.Errorf("Expected 65536 bytes, got %d", len(result))
		}
	})

	t.Run("All control frame opcodes", func(t *testing.T) {
		controlOpcodes := []byte{0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF}

		for _, opcode := range controlOpcodes {
			t.Run(fmt.Sprintf("opcode_0x%X", opcode), func(t *testing.T) {
				var frameData []byte
				frameData = append(frameData, createFrame(opcode, []byte("control"), true)...)
				frameData = append(frameData, createFrame(0x1, []byte("data"), true)...)

				conn := &mockConn{readData: frameData}
				result, err := SpinUntilCompleteMessage(conn)
				if err != nil {
					t.Fatal(err)
				}
				if string(result) != "data" {
					t.Errorf("Expected 'data', got %q", result)
				}
			})
		}
	})
}
