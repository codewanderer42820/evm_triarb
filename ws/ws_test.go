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
)

// mockConn simulates a network connection for testing
type mockConn struct {
	readData  []byte
	readPos   int
	writeData []byte
	readErr   error
	writeErr  error
	closed    bool
	// Special behavior for testing edge cases
	readFunc func(b []byte) (int, error)
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

// reusableConn provides a resettable connection for benchmarks
type reusableConn struct {
	readData []byte
	readPos  int
	readErr  error
}

func newReusableConn(data []byte) *reusableConn {
	return &reusableConn{readData: data}
}

func (r *reusableConn) reset() {
	r.readPos = 0
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

func (r *reusableConn) Write(b []byte) (int, error)        { return len(b), nil }
func (r *reusableConn) Close() error                       { return nil }
func (r *reusableConn) LocalAddr() net.Addr                { return nil }
func (r *reusableConn) RemoteAddr() net.Addr               { return nil }
func (r *reusableConn) SetDeadline(t time.Time) error      { return nil }
func (r *reusableConn) SetReadDeadline(t time.Time) error  { return nil }
func (r *reusableConn) SetWriteDeadline(t time.Time) error { return nil }

// createFrame builds a WebSocket frame
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

// TestInit verifies protocol frame initialization
func TestInit(t *testing.T) {
	// Verify upgrade request
	request := string(processor.upgradeRequest[:upgradeRequestLen])
	if !strings.Contains(request, "GET") {
		t.Error("Missing GET method")
	}
	if !strings.Contains(request, "Upgrade: websocket") {
		t.Error("Missing Upgrade header")
	}
	if !strings.HasSuffix(request, "\r\n\r\n") {
		t.Error("Missing CRLF termination")
	}

	// Verify subscribe frame
	if processor.subscribeFrame[0] != 0x81 {
		t.Error("Wrong opcode, expected TEXT frame with FIN=1")
	}
	if processor.subscribeFrame[1] != (0x80 | 126) {
		t.Error("Wrong mask bit or length indicator")
	}
}

// TestHandshake verifies WebSocket upgrade negotiation
func TestHandshake(t *testing.T) {
	tests := []struct {
		name     string
		response string
		wantErr  bool
		errMsg   string
	}{
		{
			name: "valid_101",
			response: "HTTP/1.1 101 Switching Protocols\r\n" +
				"Upgrade: websocket\r\n" +
				"Connection: Upgrade\r\n\r\n",
			wantErr: false,
		},
		{
			name: "invalid_status",
			response: "HTTP/1.1 400 Bad Request\r\n" +
				"Content-Type: text/plain\r\n\r\n",
			wantErr: true,
			errMsg:  "upgrade failed",
		},
		{
			name:     "malformed_http",
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

// TestHandshakeErrors verifies error handling
func TestHandshakeErrors(t *testing.T) {
	// Write error
	conn := &mockConn{writeErr: fmt.Errorf("write failed")}
	if err := Handshake(conn); err == nil {
		t.Fatal("Expected write error")
	}

	// Timeout on large response without CRLF
	large := "HTTP/1.1 101 OK\r\n" + strings.Repeat("X", 500)
	conn = &mockConn{readData: []byte(large)}
	if err := Handshake(conn); err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatal("Expected timeout error")
	}

	// Read error during response
	conn = &mockConn{
		readFunc: func(b []byte) (int, error) {
			return 0, fmt.Errorf("network error")
		},
	}
	if err := Handshake(conn); err == nil || !strings.Contains(err.Error(), "network error") {
		t.Fatal("Expected network error")
	}
}

// TestSendSubscription verifies subscription frame transmission
func TestSendSubscription(t *testing.T) {
	conn := &mockConn{}
	if err := SendSubscription(conn); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(conn.writeData) == 0 {
		t.Error("No subscription frame sent")
	}

	// Write error
	conn = &mockConn{writeErr: fmt.Errorf("closed")}
	if err := SendSubscription(conn); err == nil {
		t.Fatal("Expected write error")
	}
}

// TestSpinUntilCompleteMessage verifies frame processing
func TestSpinUntilCompleteMessage(t *testing.T) {
	tests := []struct {
		name     string
		frames   [][]byte
		expected []byte
	}{
		{
			name:     "single_text",
			frames:   [][]byte{createFrame(0x1, []byte("Hello"), true)},
			expected: []byte("Hello"),
		},
		{
			name: "fragmented",
			frames: [][]byte{
				createFrame(0x1, []byte("Hello "), false),
				createFrame(0x0, []byte("World"), true),
			},
			expected: []byte("Hello World"),
		},
		{
			name: "with_control",
			frames: [][]byte{
				createFrame(0x1, []byte("Test"), false),
				createFrame(0x8, []byte{}, true), // Close frame
				createFrame(0x0, []byte("ing"), true),
			},
			expected: []byte("Testing"),
		},
		{
			name:     "large_16bit",
			frames:   [][]byte{createFrame(0x1, make([]byte, 1000), true)},
			expected: make([]byte, 1000),
		},
		{
			name:     "large_64bit",
			frames:   [][]byte{createFrame(0x1, make([]byte, 100000), true)},
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

// TestSpinUntilCompleteMessageErrors verifies error conditions
func TestSpinUntilCompleteMessageErrors(t *testing.T) {
	// Test header read error
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

	// Test incomplete header
	t.Run("incomplete_header", func(t *testing.T) {
		conn := &mockConn{readData: []byte{0x81}}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
		if !strings.Contains(err.Error(), "EOF") {
			t.Errorf("Expected EOF error, got %q", err.Error())
		}
	})

	// Test extended length read error
	t.Run("extended_length_error", func(t *testing.T) {
		conn := &mockConn{readData: []byte{0x81, 126, 0x00}}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
		if !strings.Contains(err.Error(), "EOF") {
			t.Errorf("Expected EOF error, got %q", err.Error())
		}
	})

	// Test 64-bit length read error
	t.Run("64bit_length_error", func(t *testing.T) {
		conn := &mockConn{readData: []byte{0x81, 127, 0x00, 0x00, 0x00, 0x00}}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
		if !strings.Contains(err.Error(), "EOF") {
			t.Errorf("Expected EOF error, got %q", err.Error())
		}
	})

	// Test payload read error
	t.Run("payload_read_error", func(t *testing.T) {
		conn := &mockConn{readData: []byte{0x81, 0x05, 0x01, 0x02}}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
		if !strings.Contains(err.Error(), "EOF") {
			t.Errorf("Expected EOF error, got %q", err.Error())
		}
	})

	// Test control frame payload read error
	t.Run("control_frame_error", func(t *testing.T) {
		conn := &mockConn{readData: []byte{0x89, 0x05, 0x01}}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
		if !strings.Contains(err.Error(), "EOF") {
			t.Errorf("Expected EOF error, got %q", err.Error())
		}
	})

	// Test frame too large
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

	// Test message too large (accumulated fragments)
	t.Run("message_too_large", func(t *testing.T) {
		var data []byte
		// First fragment takes most of buffer
		payload1 := make([]byte, BufferSize-1000)
		data = append(data, createFrame(0x1, payload1, false)...)
		// Second fragment exceeds buffer
		payload2 := make([]byte, 2000)
		data = append(data, createFrame(0x0, payload2, true)...)

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

// TestControlFrameHandling verifies all control opcodes
func TestControlFrameHandling(t *testing.T) {
	for opcode := byte(0x8); opcode <= 0xF; opcode++ {
		t.Run(fmt.Sprintf("opcode_0x%X", opcode), func(t *testing.T) {
			var data []byte
			data = append(data, createFrame(opcode, []byte("ctrl"), true)...)
			data = append(data, createFrame(0x1, []byte("data"), true)...)

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

// TestBoundaryConditions verifies edge cases
func TestBoundaryConditions(t *testing.T) {
	// Exactly 126 bytes (16-bit length boundary)
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

	// Exactly 65536 bytes (64-bit length boundary)
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

	// Test toRead clamping to 65536
	largePayload := make([]byte, 100000)
	frame = createFrame(0x1, largePayload, true)
	conn = &mockConn{readData: frame}
	result, err = SpinUntilCompleteMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 100000 {
		t.Errorf("Expected 100000 bytes, got %d", len(result))
	}

	// Test near buffer limit
	nearLimitPayload := make([]byte, BufferSize-100)
	frame = createFrame(0x1, nearLimitPayload, true)
	conn = &mockConn{readData: frame}
	result, err = SpinUntilCompleteMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != len(nearLimitPayload) {
		t.Errorf("Expected %d bytes, got %d", len(nearLimitPayload), len(result))
	}
}

// TestControlFrameWithPayload verifies control frames with payloads
func TestControlFrameWithPayload(t *testing.T) {
	// Control frame with large payload that requires chunked reading
	var data []byte
	controlPayload := make([]byte, 50)
	for i := range controlPayload {
		controlPayload[i] = byte(i)
	}
	data = append(data, createFrame(0x8, controlPayload, true)...)
	data = append(data, createFrame(0x1, []byte("test"), true)...)

	conn := &mockConn{readData: data}
	result, err := SpinUntilCompleteMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "test" {
		t.Errorf("Expected 'test', got %q", result)
	}

	// Control frame with empty payload
	data = nil
	data = append(data, createFrame(0x9, []byte{}, true)...)
	data = append(data, createFrame(0x1, []byte("ping"), true)...)

	conn = &mockConn{readData: data}
	result, err = SpinUntilCompleteMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "ping" {
		t.Errorf("Expected 'ping', got %q", result)
	}
}

// TestPayloadReadChunking verifies chunked payload reading
func TestPayloadReadChunking(t *testing.T) {
	// Test payload that requires multiple reads
	payload := make([]byte, 70000) // Larger than 65536 chunk size
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	frame := createFrame(0x1, payload, true)

	// Mock connection that returns data in small chunks
	conn := &mockConn{
		readData: frame,
	}

	result, err := SpinUntilCompleteMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != len(payload) {
		t.Errorf("Expected %d bytes, got %d", len(payload), len(result))
	}
	// Verify data integrity
	for i := range payload {
		if result[i] != payload[i] {
			t.Errorf("Data mismatch at position %d", i)
			break
		}
	}
}

// TestEdgeCaseReadErrors verifies read errors at various points
func TestEdgeCaseReadErrors(t *testing.T) {
	// Test read error during control frame payload discard
	t.Run("control_frame_payload_read_error", func(t *testing.T) {
		// Control frame with payload but incomplete data
		data := []byte{0x88, 0x20}               // Close frame with 32 byte payload
		data = append(data, make([]byte, 10)...) // Only 10 bytes available

		conn := &mockConn{readData: data}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
	})

	// Test read error during large payload read
	t.Run("payload_chunked_read_error", func(t *testing.T) {
		// Large payload but incomplete data
		frame := []byte{0x81, 127}
		lenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBytes, 100000)
		frame = append(frame, lenBytes...)
		frame = append(frame, make([]byte, 50000)...) // Only half the payload

		conn := &mockConn{readData: frame}
		_, err := SpinUntilCompleteMessage(conn)
		if err == nil {
			t.Fatal("Expected error but got none")
		}
	})
}

// TestFragmentation verifies complex fragmentation scenarios
func TestFragmentation(t *testing.T) {
	// Many small fragments
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
		data = append(data, createFrame(opcode, payload, isLast)...)
	}

	conn := &mockConn{readData: data}
	result, err := SpinUntilCompleteMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result, expected) {
		t.Error("Fragment data mismatch")
	}

	// Test fragmentation that fills buffer exactly
	data = nil
	fragmentSize := 1000000
	numFragments := BufferSize / fragmentSize

	for i := 0; i < numFragments; i++ {
		payload := make([]byte, fragmentSize)
		isLast := i == numFragments-1
		opcode := byte(0x0)
		if i == 0 {
			opcode = 0x1
		}
		data = append(data, createFrame(opcode, payload, isLast)...)
	}

	conn = &mockConn{readData: data}
	result, err = SpinUntilCompleteMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != fragmentSize*numFragments {
		t.Errorf("Expected %d bytes, got %d", fragmentSize*numFragments, len(result))
	}
}

// BenchmarkHandshake measures handshake performance
func BenchmarkHandshake(b *testing.B) {
	response := []byte("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n\r\n")
	conn := newReusableConn(response)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		if err := Handshake(conn); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSendSubscription measures subscription performance
func BenchmarkSendSubscription(b *testing.B) {
	conn := &mockConn{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.writeData = conn.writeData[:0]
		if err := SendSubscription(conn); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSpinUntilCompleteMessage measures frame processing performance
func BenchmarkSpinUntilCompleteMessage(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			payload := make([]byte, size)
			rand.Read(payload)
			frame := createFrame(0x1, payload, true)
			conn := newReusableConn(frame)

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				conn.reset()
				if _, err := SpinUntilCompleteMessage(conn); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFragmented measures fragmented message performance
func BenchmarkFragmented(b *testing.B) {
	var data []byte
	chunkSize := 1000
	numChunks := 10

	for i := 0; i < numChunks; i++ {
		payload := make([]byte, chunkSize)
		rand.Read(payload)
		isLast := i == numChunks-1
		opcode := byte(0x0)
		if i == 0 {
			opcode = 0x1
		}
		data = append(data, createFrame(opcode, payload, isLast)...)
	}

	conn := newReusableConn(data)

	b.ReportAllocs()
	b.SetBytes(int64(chunkSize * numChunks))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		if _, err := SpinUntilCompleteMessage(conn); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkControlFrames measures control frame overhead
func BenchmarkControlFrames(b *testing.B) {
	var data []byte

	// Add 10 control frames
	for i := 0; i < 10; i++ {
		data = append(data, createFrame(0x8+byte(i%4), []byte("ctrl"), true)...)
	}

	// Add data frame
	payload := make([]byte, 1000)
	rand.Read(payload)
	data = append(data, createFrame(0x1, payload, true)...)

	conn := newReusableConn(data)

	b.ReportAllocs()
	b.SetBytes(1000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		if _, err := SpinUntilCompleteMessage(conn); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemoryPressure verifies zero allocations
func BenchmarkMemoryPressure(b *testing.B) {
	payload := make([]byte, 1024*1024)
	rand.Read(payload)
	frame := createFrame(0x1, payload, true)
	conn := newReusableConn(frame)

	runtime.GC()
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	b.ReportAllocs()
	b.SetBytes(1024 * 1024)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.reset()
		if _, err := SpinUntilCompleteMessage(conn); err != nil {
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

// BenchmarkLatency measures operation latency
func BenchmarkLatency(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
			payload := make([]byte, size)
			rand.Read(payload)
			frame := createFrame(0x1, payload, true)
			conn := newReusableConn(frame)

			times := make([]time.Duration, b.N)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				conn.reset()

				start := time.Now()
				_, err := SpinUntilCompleteMessage(conn)
				times[i] = time.Since(start)

				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			if b.N > 0 {
				var total time.Duration
				min, max := times[0], times[0]

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
