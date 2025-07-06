package ws

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// ============================================================================
// MOCK CONNECTION FOR TESTING
// ============================================================================

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

	if m.readPos >= len(m.readData) && n == 0 {
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

// ============================================================================
// ZERO-ALLOCATION CONNECTIONS FOR TRUE PERFORMANCE MEASUREMENT
// ============================================================================

// discardConn - Discards all writes, returns EOF on reads
type discardConn struct{}

func (d *discardConn) Read(b []byte) (int, error)         { return 0, fmt.Errorf("EOF") }
func (d *discardConn) Write(b []byte) (int, error)        { return len(b), nil }
func (d *discardConn) Close() error                       { return nil }
func (d *discardConn) LocalAddr() net.Addr                { return nil }
func (d *discardConn) RemoteAddr() net.Addr               { return nil }
func (d *discardConn) SetDeadline(t time.Time) error      { return nil }
func (d *discardConn) SetReadDeadline(t time.Time) error  { return nil }
func (d *discardConn) SetWriteDeadline(t time.Time) error { return nil }

// hybridConn - Provides pre-allocated read data, discards writes (zero allocation)
type hybridConn struct {
	readData []byte
	readPos  int
}

func (h *hybridConn) Write(b []byte) (int, error) { return len(b), nil } // Zero allocation discard

func (h *hybridConn) Read(b []byte) (int, error) {
	if h.readPos >= len(h.readData) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(b, h.readData[h.readPos:])
	h.readPos += n
	return n, nil
}

func (h *hybridConn) Close() error                       { return nil }
func (h *hybridConn) LocalAddr() net.Addr                { return nil }
func (h *hybridConn) RemoteAddr() net.Addr               { return nil }
func (h *hybridConn) SetDeadline(t time.Time) error      { return nil }
func (h *hybridConn) SetReadDeadline(t time.Time) error  { return nil }
func (h *hybridConn) SetWriteDeadline(t time.Time) error { return nil }

// reusableConn - Pre-allocated connection that can be reset without allocation
type reusableConn struct {
	readData    []byte
	readPos     int
	writeBuffer []byte // Pre-allocated write buffer
	writePos    int
	readErr     error
	writeErr    error
}

func newReusableConn(readData []byte, writeCapacity int) *reusableConn {
	return &reusableConn{
		readData:    readData,
		writeBuffer: make([]byte, 0, writeCapacity),
	}
}

func (r *reusableConn) reset() {
	r.readPos = 0
	r.writePos = 0
	// Don't reallocate writeBuffer, just reset length
	r.writeBuffer = r.writeBuffer[:0]
}

func (r *reusableConn) Read(b []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	if r.readPos >= len(r.readData) {
		return 0, fmt.Errorf("EOF")
	}

	n := copy(b, r.readData[r.readPos:])
	r.readPos += n
	return n, nil
}

func (r *reusableConn) Write(b []byte) (int, error) {
	if r.writeErr != nil {
		return 0, r.writeErr
	}
	// Use pre-allocated buffer to avoid allocation
	if len(r.writeBuffer)+len(b) <= cap(r.writeBuffer) {
		r.writeBuffer = append(r.writeBuffer, b...)
	}
	return len(b), nil
}

func (r *reusableConn) Close() error                       { return nil }
func (r *reusableConn) LocalAddr() net.Addr                { return nil }
func (r *reusableConn) RemoteAddr() net.Addr               { return nil }
func (r *reusableConn) SetDeadline(t time.Time) error      { return nil }
func (r *reusableConn) SetReadDeadline(t time.Time) error  { return nil }
func (r *reusableConn) SetWriteDeadline(t time.Time) error { return nil }

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

// ============================================================================
// UNIT TESTS - UPDATED FOR STRUCT-BASED DESIGN
// ============================================================================

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
			response:    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n",
			shouldError: true,
			errorMsg:    "EOF",
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
				createFrame(0x1, []byte("Hello "), false),
				createFrame(0x0, []byte("World"), true),
			},
			expected: []byte("Hello World"),
		},
		{
			name: "Message with control frame interleaved",
			frames: [][]byte{
				createFrame(0x1, []byte("Hello "), false),
				createFrame(0x8, []byte{}, true),
				createFrame(0x0, []byte("World"), true),
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
			name: "All control frame opcodes",
			frames: [][]byte{
				createFrame(0x8, []byte("close"), true),
				createFrame(0x9, []byte("ping"), true),
				createFrame(0xA, []byte("pong"), true),
				createFrame(0xB, []byte("reserved"), true),
				createFrame(0x1, []byte("data"), true),
			},
			expected: []byte("data"),
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
				t.Errorf("Expected %d bytes, got %d bytes", len(tt.expected), len(result))
			}
		})
	}
}

func TestSpinUntilCompleteMessage_EdgeCases(t *testing.T) {
	t.Run("Read error on extended length", func(t *testing.T) {
		frame := []byte{0x81, 126}
		conn := &mockConn{readData: frame}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error for incomplete extended length")
		}
	})

	t.Run("Read error on 64-bit length", func(t *testing.T) {
		frame := []byte{0x81, 127, 0x00, 0x00}
		conn := &mockConn{readData: frame}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error for incomplete 64-bit length")
		}
	})

	t.Run("Read error during payload", func(t *testing.T) {
		frame := createFrame(0x1, []byte("Hello World"), true)
		frame = frame[:len(frame)-5]
		conn := &mockConn{readData: frame}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error for incomplete payload")
		}
	})

	t.Run("Boundary conditions", func(t *testing.T) {
		// Test exactly 126 bytes (boundary for 16-bit length)
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

		// Test exactly 65536 bytes (boundary for 64-bit length)
		payload = make([]byte, 65536)
		frame = createFrame(0x1, payload, true)
		conn = &mockConn{readData: frame}
		result, err = SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 65536 {
			t.Errorf("Expected 65536 bytes, got %d", len(result))
		}
	})
}

func TestInit(t *testing.T) {
	t.Run("Upgrade request format", func(t *testing.T) {
		// FIXED: Use compile-time constant instead of struct field
		request := string(processor.upgradeRequest[:upgradeRequestLen])

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
		// FIXED: Use compile-time constant instead of struct field
		if len(processor.subscribeFrame) < 8 {
			t.Error("Subscribe frame too short")
		}

		if processor.subscribeFrame[0] != 0x81 {
			t.Error("Wrong opcode, expected TEXT frame with FIN=1")
		}
		if processor.subscribeFrame[1] != (0x80 | 126) {
			t.Error("Wrong mask bit or length indicator")
		}

		expectedLen := len(`{"jsonrpc":"2.0","method":"eth_subscribe","params":["logs",{}],"id":1}`)
		actualLen := int(processor.subscribeFrame[2])<<8 | int(processor.subscribeFrame[3])
		if actualLen != expectedLen {
			t.Errorf("Wrong payload length: expected %d, got %d", expectedLen, actualLen)
		}
	})
}

// ============================================================================
// STRESS TESTS
// ============================================================================

func TestStressScenarios(t *testing.T) {
	t.Run("Large message processing", func(t *testing.T) {
		payload := make([]byte, 1024*1024) // 1MB
		rand.Read(payload)
		frame := createFrame(0x1, payload, true)

		conn := &mockConn{readData: frame}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != len(payload) {
			t.Errorf("Expected %d bytes, got %d", len(payload), len(result))
		}
	})

	t.Run("Multiple fragments", func(t *testing.T) {
		var frameData []byte
		expectedData := make([]byte, 0, 100)

		// Create 100 small fragments
		for i := 0; i < 99; i++ {
			payload := []byte{byte(i % 256)}
			expectedData = append(expectedData, payload...)
			frameData = append(frameData, createFrame(0x0, payload, false)...)
		}

		// Final fragment
		payload := []byte{255}
		expectedData = append(expectedData, payload...)
		frameData = append(frameData, createFrame(0x0, payload, true)...)

		conn := &mockConn{readData: frameData}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != len(expectedData) {
			t.Errorf("Expected %d bytes, got %d", len(expectedData), len(result))
		}
	})

	t.Run("Many control frames", func(t *testing.T) {
		var frameData []byte

		// Add 100 control frames
		for i := 0; i < 100; i++ {
			opcode := byte(0x8 + (i % 8))
			payload := []byte(fmt.Sprintf("control_%d", i))
			frameData = append(frameData, createFrame(opcode, payload, true)...)
		}

		// Finally add data frame
		frameData = append(frameData, createFrame(0x1, []byte("final_data"), true)...)

		conn := &mockConn{readData: frameData}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if string(result) != "final_data" {
			t.Errorf("Expected 'final_data', got %q", result)
		}
	})
}

// ============================================================================
// TRUE ZERO-ALLOCATION BENCHMARKS
// ============================================================================

func BenchmarkZeroAllocation(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Pre-allocate everything OUTSIDE timer
			payload := make([]byte, size)
			rand.Read(payload)
			frame := createFrame(0x1, payload, true)
			conn := newReusableConn(frame, 0) // No write buffer needed

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer() // Start timing HERE

			for i := 0; i < b.N; i++ {
				conn.reset() // Reset without reallocation
				_, err := SpinUntilCompleteMessage(conn)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkZeroAllocationFragmented(b *testing.B) {
	// Pre-create fragmented frame data
	chunkSize := 1000
	numChunks := 10
	var frameData []byte

	for i := 0; i < numChunks-1; i++ {
		payload := make([]byte, chunkSize)
		rand.Read(payload)
		frameData = append(frameData, createFrame(0x0, payload, false)...)
	}

	payload := make([]byte, chunkSize)
	rand.Read(payload)
	frameData = append(frameData, createFrame(0x0, payload, true)...)

	conn := newReusableConn(frameData, 0)

	b.ReportAllocs()
	b.SetBytes(int64(chunkSize * numChunks))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		_, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZeroAllocationControlFrames(b *testing.B) {
	var frameData []byte

	// Add control frames
	for i := 0; i < 10; i++ {
		frameData = append(frameData, createFrame(0x8+byte(i%4), []byte("control"), true)...)
	}

	payload := make([]byte, 1000)
	rand.Read(payload)
	frameData = append(frameData, createFrame(0x1, payload, true)...)

	conn := newReusableConn(frameData, 0)

	b.ReportAllocs()
	b.SetBytes(1000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		_, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================================
// TRUE PERFORMANCE BENCHMARKS FOR HANDSHAKE & SUBSCRIPTION
// ============================================================================

func BenchmarkSendSubscriptionTrue(b *testing.B) {
	conn := &discardConn{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := SendSubscription(conn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHandshakeTrue(b *testing.B) {
	// Pre-allocate valid response OUTSIDE benchmark
	validResponse := []byte("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n")

	conn := &hybridConn{readData: validResponse}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.readPos = 0 // Reset without allocation
		err := Handshake(conn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHandshakeWriteOnly(b *testing.B) {
	conn := &discardConn{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// FIXED: Use compile-time constant instead of struct field
		_, err := conn.Write(processor.upgradeRequest[:upgradeRequestLen])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================================
// CORE OPERATION BENCHMARKS
// ============================================================================

func BenchmarkCoreOperations(b *testing.B) {
	b.Run("unsafe_pointer_magic", func(b *testing.B) {
		data := []byte("HTTP/1.1 101 Switching Protocols")

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = *(*uint64)(unsafe.Pointer(&data[0])) == 0x312E312F50545448
		}
	})

	b.Run("endian_conversion", func(b *testing.B) {
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			v := *(*uint64)(unsafe.Pointer(&data[0]))
			_ = ((v & 0xFF) << 56) | ((v & 0xFF00) << 40) | ((v & 0xFF0000) << 24) | ((v & 0xFF000000) << 8) |
				((v & 0xFF00000000) >> 8) | ((v & 0xFF0000000000) >> 24) | ((v & 0xFF000000000000) >> 40) | ((v & 0xFF00000000000000) >> 56)
		}
	})

	b.Run("frame_header_parsing", func(b *testing.B) {
		header := []byte{0x81, 0x7E, 0x03, 0xE8}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			opcode := header[0] & 0x0F
			payloadLen := uint64(header[1] & 0x7F)
			if payloadLen == 126 {
				payloadLen = uint64(header[2])<<8 | uint64(header[3])
			}
			_ = opcode
			_ = payloadLen
		}
	})

	b.Run("buffer_slice", func(b *testing.B) {
		msgEnd := 1000

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// FIXED: Access struct buffer instead of global variable
			_ = processor.buffer[:msgEnd]
		}
	})
}

// ============================================================================
// MEMORY PRESSURE ANALYSIS
// ============================================================================

func BenchmarkMemoryPressure(b *testing.B) {
	payload := make([]byte, 1024*1024) // 1MB
	rand.Read(payload)
	frame := createFrame(0x1, payload, true)
	conn := newReusableConn(frame, 0)

	runtime.GC()

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	b.ReportAllocs()
	b.SetBytes(1024 * 1024)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		_, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	runtime.ReadMemStats(&m2)

	if b.N > 0 {
		allocDelta := m2.TotalAlloc - m1.TotalAlloc
		b.ReportMetric(float64(allocDelta)/float64(b.N), "actual_bytes/op")

		if allocDelta > 0 {
			b.Logf("WARNING: %d bytes allocated over %d iterations (%.2f bytes/op)",
				allocDelta, b.N, float64(allocDelta)/float64(b.N))
		} else {
			b.Logf("PERFECT: Zero allocations confirmed over %d iterations", b.N)
		}
	}
}

// ============================================================================
// LATENCY ANALYSIS
// ============================================================================

func BenchmarkLatencyAnalysis(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("latency_%dbytes", size), func(b *testing.B) {
			payload := make([]byte, size)
			rand.Read(payload)
			frame := createFrame(0x1, payload, true)
			conn := newReusableConn(frame, 0)

			times := make([]time.Duration, b.N)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				conn.reset()

				start := time.Now()
				_, err := SpinUntilCompleteMessage(conn)
				end := time.Now()

				if err != nil {
					b.Fatal(err)
				}
				times[i] = end.Sub(start)
			}
			b.StopTimer()

			if b.N > 0 {
				var total time.Duration
				min := times[0]
				max := times[0]

				for _, t := range times {
					total += t
					if t < min {
						min = t
					}
					if t > max {
						max = t
					}
				}

				avg := total / time.Duration(b.N)
				b.ReportMetric(float64(avg.Nanoseconds()), "avg_ns/op")
				b.ReportMetric(float64(min.Nanoseconds()), "min_ns/op")
				b.ReportMetric(float64(max.Nanoseconds()), "max_ns/op")
			}
		})
	}
}

// ============================================================================
// EDGE CASE PERFORMANCE
// ============================================================================

func BenchmarkEdgeCases(b *testing.B) {
	b.Run("boundary_126", func(b *testing.B) {
		payload := make([]byte, 126)
		frame := createFrame(0x1, payload, true)
		conn := newReusableConn(frame, 0)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			conn.reset()
			_, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("boundary_65536", func(b *testing.B) {
		payload := make([]byte, 65536)
		frame := createFrame(0x1, payload, true)
		conn := newReusableConn(frame, 0)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			conn.reset()
			_, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("max_control_frames", func(b *testing.B) {
		var frameData []byte

		// 100 control frames before data
		for i := 0; i < 100; i++ {
			frameData = append(frameData, createFrame(0x8, []byte{}, true)...)
		}
		frameData = append(frameData, createFrame(0x1, []byte("data"), true)...)

		conn := newReusableConn(frameData, 0)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			conn.reset()
			_, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ============================================================================
// CONCURRENCY STRESS TEST
// ============================================================================

func BenchmarkConcurrency(b *testing.B) {
	payload := make([]byte, 10000)
	rand.Read(payload)
	frame := createFrame(0x1, payload, true)

	b.RunParallel(func(pb *testing.PB) {
		conn := newReusableConn(frame, 0)

		for pb.Next() {
			conn.reset()
			_, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ============================================================================
// COMPARISON BENCHMARKS
// ============================================================================

func BenchmarkComparison(b *testing.B) {
	size := 10000
	src := make([]byte, size)
	dst := make([]byte, size)
	rand.Read(src)

	b.Run("memcpy", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(size))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			copy(dst, src)
		}
	})

	b.Run("websocket", func(b *testing.B) {
		payload := make([]byte, size)
		rand.Read(payload)
		frame := createFrame(0x1, payload, true)
		conn := newReusableConn(frame, 0)

		b.ReportAllocs()
		b.SetBytes(int64(size))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			conn.reset()
			_, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ============================================================================
// LEGACY BENCHMARKS (for comparison with old test infrastructure)
// ============================================================================

func BenchmarkHandshakeLegacy(b *testing.B) {
	validResponse := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"

	b.ReportAllocs()
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

func BenchmarkSendSubscriptionLegacy(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn := &mockConn{}
		err := SendSubscription(conn)
		if err != nil {
			b.Fatal(err)
		}
	}
}
