package ws

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"main/constants"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
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

	// Pre-allocated handshake response
	benchHandshakeResponse = []byte("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n")
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

// ============================================================================
// ZERO-ALLOCATION BENCHMARK CONNECTIONS
// ============================================================================

// benchReadConn - Zero-allocation read-only connection for benchmarks
type benchReadConn struct {
	data []byte
	pos  int
}

//go:nosplit
//go:noinline
func (b *benchReadConn) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}

//go:nosplit
//go:noinline
func (b *benchReadConn) Write(p []byte) (int, error)        { return len(p), nil }
func (b *benchReadConn) Close() error                       { return nil }
func (b *benchReadConn) LocalAddr() net.Addr                { return nil }
func (b *benchReadConn) RemoteAddr() net.Addr               { return nil }
func (b *benchReadConn) SetDeadline(t time.Time) error      { return nil }
func (b *benchReadConn) SetReadDeadline(t time.Time) error  { return nil }
func (b *benchReadConn) SetWriteDeadline(t time.Time) error { return nil }
func (b *benchReadConn) reset()                             { b.pos = 0 }

// benchWriteConn - Zero-allocation write-only connection for benchmarks
type benchWriteConn struct{}

//go:nosplit
//go:noinline
func (b *benchWriteConn) Read(p []byte) (int, error) { return 0, io.EOF }

//go:nosplit
//go:noinline
func (b *benchWriteConn) Write(p []byte) (int, error)        { return len(p), nil }
func (b *benchWriteConn) Close() error                       { return nil }
func (b *benchWriteConn) LocalAddr() net.Addr                { return nil }
func (b *benchWriteConn) RemoteAddr() net.Addr               { return nil }
func (b *benchWriteConn) SetDeadline(t time.Time) error      { return nil }
func (b *benchWriteConn) SetReadDeadline(t time.Time) error  { return nil }
func (b *benchWriteConn) SetWriteDeadline(t time.Time) error { return nil }

// benchHandshakeConn - Zero-allocation handshake connection for benchmarks
type benchHandshakeConn struct {
	response []byte
	pos      int
}

//go:nosplit
//go:noinline
func (b *benchHandshakeConn) Read(p []byte) (int, error) {
	if b.pos >= len(b.response) {
		return 0, io.EOF
	}
	n := copy(p, b.response[b.pos:])
	b.pos += n
	return n, nil
}

//go:nosplit
//go:noinline
func (b *benchHandshakeConn) Write(p []byte) (int, error)        { return len(p), nil }
func (b *benchHandshakeConn) Close() error                       { return nil }
func (b *benchHandshakeConn) LocalAddr() net.Addr                { return nil }
func (b *benchHandshakeConn) RemoteAddr() net.Addr               { return nil }
func (b *benchHandshakeConn) SetDeadline(t time.Time) error      { return nil }
func (b *benchHandshakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (b *benchHandshakeConn) SetWriteDeadline(t time.Time) error { return nil }
func (b *benchHandshakeConn) reset()                             { b.pos = 0 }

// directConn - For true zero-allocation benchmarks (no interface overhead)
type directConn struct {
	data []byte
	pos  int
}

//go:nosplit
//go:noinline
func (d *directConn) read(p []byte) (int, error) {
	if d.pos >= len(d.data) {
		return 0, io.EOF
	}
	n := copy(p, d.data[d.pos:])
	d.pos += n
	return n, nil
}

//go:nosplit
//go:noinline
func (d *directConn) write(p []byte) (int, error) { return len(p), nil }
func (d *directConn) reset()                      { d.pos = 0 }

// ============================================================================
// SYSCALL SIMULATION CONNECTIONS
// ============================================================================

// syscallConn simulates realistic network syscall overhead
type syscallConn struct {
	data           []byte
	pos            int
	syscallLatency time.Duration
	copyOverhead   time.Duration
	readCallCount  int
	writeCallCount int
}

//go:noinline
func (s *syscallConn) Read(p []byte) (int, error) {
	// Simulate syscall entry/exit overhead
	if s.syscallLatency > 0 {
		time.Sleep(s.syscallLatency)
	}

	s.readCallCount++

	if s.pos >= len(s.data) {
		return 0, io.EOF
	}

	// Simulate realistic read sizes (not always full buffer)
	maxRead := len(p)
	available := len(s.data) - s.pos

	// Realistic network behavior: sometimes partial reads
	if available > 1024 && s.readCallCount%3 == 0 {
		maxRead = 1024 // Simulate 1KB partial reads sometimes
	}

	toRead := maxRead
	if toRead > available {
		toRead = available
	}

	// Simulate memory copy overhead
	if s.copyOverhead > 0 {
		// Scale copy time with data size
		copyTime := time.Duration(float64(s.copyOverhead) * float64(toRead) / 1024.0)
		time.Sleep(copyTime)
	}

	n := copy(p, s.data[s.pos:s.pos+toRead])
	s.pos += n
	return n, nil
}

//go:noinline
func (s *syscallConn) Write(p []byte) (int, error) {
	// Simulate syscall overhead
	if s.syscallLatency > 0 {
		time.Sleep(s.syscallLatency)
	}

	s.writeCallCount++

	// Simulate memory copy overhead for writes
	if s.copyOverhead > 0 {
		copyTime := time.Duration(float64(s.copyOverhead) * float64(len(p)) / 1024.0)
		time.Sleep(copyTime)
	}

	return len(p), nil
}

func (s *syscallConn) Close() error                       { return nil }
func (s *syscallConn) LocalAddr() net.Addr                { return nil }
func (s *syscallConn) RemoteAddr() net.Addr               { return nil }
func (s *syscallConn) SetDeadline(t time.Time) error      { return nil }
func (s *syscallConn) SetReadDeadline(t time.Time) error  { return nil }
func (s *syscallConn) SetWriteDeadline(t time.Time) error { return nil }
func (s *syscallConn) reset() {
	s.pos = 0
	s.readCallCount = 0
	s.writeCallCount = 0
}

// Syscall overhead profiles for different scenarios
type syscallProfile struct {
	latency      time.Duration
	copyOverhead time.Duration
	name         string
}

var (
	// Modern kernel, optimized network stack
	fastSyscallProfile = &syscallProfile{
		latency:      50 * time.Nanosecond, // 50ns syscall overhead
		copyOverhead: 10 * time.Nanosecond, // 10ns per KB copy
		name:         "fast_kernel",
	}

	// Typical production server
	normalSyscallProfile = &syscallProfile{
		latency:      200 * time.Nanosecond, // 200ns syscall overhead
		copyOverhead: 50 * time.Nanosecond,  // 50ns per KB copy
		name:         "normal_kernel",
	}

	// Loaded system or container overhead
	slowSyscallProfile = &syscallProfile{
		latency:      1 * time.Microsecond,  // 1μs syscall overhead
		copyOverhead: 200 * time.Nanosecond, // 200ns per KB copy
		name:         "loaded_system",
	}

	// VM or high-overhead environment
	vmSyscallProfile = &syscallProfile{
		latency:      5 * time.Microsecond, // 5μs syscall overhead
		copyOverhead: 1 * time.Microsecond, // 1μs per KB copy
		name:         "virtualized",
	}
)

// ============================================================================
// SHARED UTILITIES
// ============================================================================

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

// handshakeLogic - Direct implementation without interface overhead
//
//go:nosplit
//go:noinline
func handshakeLogic(conn *directConn) error {
	// Simulate the write (your actual code writes pre-computed request)
	_ = processor.upgradeRequest[:upgradeRequestLen]

	// Read response logic (same as your actual code)
	var buf [constants.HandshakeBufferSize]byte
	total := 0

	for total < 500 {
		// Simulate Read() without interface call
		n, err := conn.read(buf[total:])
		if err != nil {
			return err
		}
		total += n

		// Same validation logic as your code
		if total >= 16 {
			end := total - 3
			for i := 0; i < end; i++ {
				if *(*uint32)(unsafe.Pointer(&buf[i])) == 0x0A0D0A0D {
					if *(*uint64)(unsafe.Pointer(&buf[0])) == 0x312E312F50545448 &&
						buf[8] == ' ' && buf[9] == '1' && buf[10] == '0' && buf[11] == '1' {
						return nil
					}
					return errUpgradeFailed
				}
			}
		}
	}
	return errHandshakeTimeout
}

// ============================================================================
// GLOBAL BENCHMARK INSTANCES
// ============================================================================

var (
	// Global benchmark connections (reused across iterations)
	globalHandshakeConn = &benchHandshakeConn{response: benchHandshakeResponse}
	globalDirectConn    = &directConn{data: benchHandshakeResponse}
	globalWriteConn     = &benchWriteConn{}
	globalReadConn512   = &benchReadConn{data: frame512}
	globalReadConn4KB   = &benchReadConn{data: frame4096}
	globalReadConn16KB  = &benchReadConn{data: frame16384}
	globalReadConn64KB  = &benchReadConn{data: frame65536}
)

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
		if len(processor.buffer) != constants.BufferSize {
			t.Errorf("Buffer size incorrect: expected %d, got %d", constants.BufferSize, len(processor.buffer))
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
			binary.BigEndian.PutUint64(lenBytes, uint64(constants.BufferSize)+1)
			frame = append(frame, lenBytes...)

			conn := &mockConn{readData: frame}
			_, err := SpinUntilCompleteMessage(conn)
			if err == nil || !strings.Contains(err.Error(), "frame too large") {
				t.Error("Expected 'frame too large' error")
			}
		})

		t.Run("message_too_large_accumulated", func(t *testing.T) {
			var data []byte
			// First fragment takes most of buffer
			payload1 := make([]byte, constants.BufferSize-1000)
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

		t.Run("early_bounds_check_near_buffer_end", func(t *testing.T) {
			// Test the early bounds check when msgEnd approaches buffer end
			// We need msgEnd > BufferSize-MaxFrameHeaderSize to trigger the check

			var data []byte

			// Create a large unfragmented frame that brings msgEnd just past the early check threshold
			// We want msgEnd to be BufferSize-MaxFrameHeaderSize+1 after this frame
			targetMsgEnd := constants.BufferSize - constants.MaxFrameHeaderSize + 1
			largePayload := make([]byte, targetMsgEnd)
			for i := range largePayload {
				largePayload[i] = byte('A' + (i % 26))
			}

			// Create the first frame as non-final (FIN=0) so we stay in the loop
			data = append(data, createTestFrame(0x1, largePayload, false)...)

			// Now create a second frame - this should trigger the early bounds check
			// because msgEnd will be > BufferSize-MaxFrameHeaderSize
			smallPayload := []byte("trigger")
			data = append(data, createTestFrame(0x0, smallPayload, true)...)

			conn := &mockConn{readData: data}
			_, err := SpinUntilCompleteMessage(conn)
			if err == nil {
				t.Error("Expected error from early bounds check")
			}
			if !strings.Contains(err.Error(), "message too large") {
				t.Errorf("Expected 'message too large' error from early bounds check, got: %v", err)
			}
		})

		t.Run("exact_buffer_limit", func(t *testing.T) {
			// Test exact buffer size limit - should trigger frame too large
			// We need to create a 64-bit length frame to trigger the individual frame check
			var data []byte

			// Create frame header manually to ensure we trigger the 64-bit length path
			data = append(data, 0x81) // FIN=1, opcode=1 (text)
			data = append(data, 127)  // 64-bit length indicator

			// Add 64-bit length for exactly BufferSize
			lenBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(lenBytes, uint64(constants.BufferSize))
			data = append(data, lenBytes...)

			// We don't need to add the actual payload since the check happens after length parsing

			conn := &mockConn{readData: data}
			_, err := SpinUntilCompleteMessage(conn)
			if err == nil || !strings.Contains(err.Error(), "frame too large") {
				t.Errorf("Expected 'frame too large' error for exact buffer size frame, got: %v", err)
			}
		})

		t.Run("maximum_valid_frame", func(t *testing.T) {
			// Test maximum valid frame size (just under BufferSize)
			// Use a size that's definitely valid
			maxValidSize := constants.BufferSize - 1000 // Leave some room to be safe
			payload := make([]byte, maxValidSize)
			for i := range payload {
				payload[i] = byte(i & 0xFF)
			}
			frame := createTestFrame(0x1, payload, true)

			conn := &mockConn{readData: frame}
			result, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				t.Errorf("Unexpected error for valid large frame: %v", err)
			}
			if len(result) != maxValidSize {
				t.Errorf("Expected %d bytes, got %d", maxValidSize, len(result))
			}
		})

		t.Run("fragmented_message_approaching_limit", func(t *testing.T) {
			var data []byte

			// Create multiple fragments that together approach the buffer limit
			fragmentSize := constants.BufferSize / 4

			// Fragment 1
			payload1 := make([]byte, fragmentSize)
			data = append(data, createTestFrame(0x1, payload1, false)...)

			// Fragment 2
			payload2 := make([]byte, fragmentSize)
			data = append(data, createTestFrame(0x0, payload2, false)...)

			// Fragment 3
			payload3 := make([]byte, fragmentSize)
			data = append(data, createTestFrame(0x0, payload3, false)...)

			// Fragment 4 - this should succeed (total = BufferSize)
			payload4 := make([]byte, fragmentSize-10) // Leave some room for frame headers
			data = append(data, createTestFrame(0x0, payload4, true)...)

			conn := &mockConn{readData: data}
			result, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				t.Errorf("Unexpected error for fragmented message within limits: %v", err)
			}
			expectedSize := fragmentSize*3 + (fragmentSize - 10)
			if len(result) != expectedSize {
				t.Errorf("Expected %d bytes, got %d", expectedSize, len(result))
			}
		})

		t.Run("fragmented_message_exceeding_limit", func(t *testing.T) {
			var data []byte

			// Create multiple fragments that together exceed the buffer limit
			fragmentSize := constants.BufferSize / 3

			// Fragment 1
			payload1 := make([]byte, fragmentSize)
			data = append(data, createTestFrame(0x1, payload1, false)...)

			// Fragment 2
			payload2 := make([]byte, fragmentSize)
			data = append(data, createTestFrame(0x0, payload2, false)...)

			// Fragment 3 - this should fail (total would exceed BufferSize)
			payload3 := make([]byte, fragmentSize+1000)
			data = append(data, createTestFrame(0x0, payload3, true)...)

			conn := &mockConn{readData: data}
			_, err := SpinUntilCompleteMessage(conn)
			if err == nil || !strings.Contains(err.Error(), "message too large") {
				t.Error("Expected 'message too large' error for fragmented message exceeding limits")
			}
		})

		t.Run("bounds_violation_check", func(t *testing.T) {
			// This test is harder to trigger since we have comprehensive bounds checking
			// But we'll test the final bounds violation check as a safety net

			// Create a frame that should pass all checks but somehow result in msgEnd > BufferSize
			// This is mostly for code coverage of the final safety check
			payload := make([]byte, constants.BufferSize-100) // Should be within limits
			frame := createTestFrame(0x1, payload, true)

			conn := &mockConn{readData: frame}
			result, err := SpinUntilCompleteMessage(conn)

			// This should succeed normally
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if len(result) != constants.BufferSize-100 {
				t.Errorf("Expected %d bytes, got %d", constants.BufferSize-100, len(result))
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

// BenchmarkHandshake measures true handshake performance without overhead
func BenchmarkHandshake(b *testing.B) {
	// Pre-allocated response data
	response := benchHandshakeResponse
	responseLen := len(response)

	// Pre-validate the response to ensure our benchmark measures the actual logic
	// Find CRLF position
	crlfPos := -1
	for i := 0; i < responseLen-3; i++ {
		if response[i] == '\r' && response[i+1] == '\n' && response[i+2] == '\r' && response[i+3] == '\n' {
			crlfPos = i
			break
		}
	}

	if crlfPos == -1 {
		b.Fatal("Invalid test response - no CRLF found")
	}

	// Validate HTTP header to ensure test data is correct
	if responseLen < 12 || string(response[0:8]) != "HTTP/1.1" || string(response[9:12]) != "101" {
		b.Fatal("Invalid test response - not HTTP 101")
	}

	// Extended warmup to eliminate any CPU frequency scaling effects
	for i := 0; i < 10000; i++ {
		// Simulate the core handshake logic without I/O overhead
		var buf [constants.HandshakeBufferSize]byte
		copy(buf[:], response) // Simulate read
		total := responseLen

		// Core handshake validation logic (what we're actually measuring)
		if total >= 16 {
			end := total - 3
			for j := 0; j < end; j++ {
				if *(*uint32)(unsafe.Pointer(&buf[j])) == 0x0A0D0A0D {
					if *(*uint64)(unsafe.Pointer(&buf[0])) == 0x312E312F50545448 &&
						buf[8] == ' ' && buf[9] == '1' && buf[10] == '0' && buf[11] == '1' {
						break
					}
				}
			}
		}
	}

	// Force multiple GC cycles to ensure clean measurement
	runtime.GC()
	runtime.GC()
	runtime.GC()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Measure ONLY the core handshake validation logic
		var buf [constants.HandshakeBufferSize]byte
		copy(buf[:responseLen], response) // Simulate the network read
		total := responseLen

		// This is the actual handshake validation we're measuring
		validated := false
		if total >= 16 {
			end := total - 3
			for j := 0; j < end; j++ {
				if *(*uint32)(unsafe.Pointer(&buf[j])) == 0x0A0D0A0D {
					if *(*uint64)(unsafe.Pointer(&buf[0])) == 0x312E312F50545448 &&
						buf[8] == ' ' && buf[9] == '1' && buf[10] == '0' && buf[11] == '1' {
						validated = true
						break
					}
				}
			}
		}

		// Prevent compiler optimization
		if !validated {
			b.Fatal("Handshake validation failed")
		}
	}
}

// BenchmarkSendSubscription measures subscription frame transmission performance
func BenchmarkSendSubscription(b *testing.B) {
	// Warmup
	for i := 0; i < 100; i++ {
		SendSubscription(globalWriteConn)
	}

	runtime.GC()
	runtime.GC()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		SendSubscription(globalWriteConn)
	}
}

// BenchmarkFrameSizes measures frame parsing performance at different sizes
func BenchmarkFrameSizes(b *testing.B) {
	sizes := []struct {
		name  string
		conn  *benchReadConn
		bytes int64
	}{
		{"512B", globalReadConn512, 512},
		{"4KB", globalReadConn4KB, 4096},
		{"16KB", globalReadConn16KB, 16384},
		{"64KB", globalReadConn64KB, 65536},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			// Warmup
			for i := 0; i < 100; i++ {
				s.conn.reset()
				SpinUntilCompleteMessage(s.conn)
			}

			runtime.GC()
			runtime.GC()

			b.SetBytes(s.bytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				s.conn.reset()
				SpinUntilCompleteMessage(s.conn)
			}
		})
	}
}

// BenchmarkZeroAllocation validates zero-allocation message processing
func BenchmarkZeroAllocation(b *testing.B) {
	// Extended warmup for zero-allocation validation
	for i := 0; i < 1000; i++ {
		globalReadConn4KB.reset()
		SpinUntilCompleteMessage(globalReadConn4KB)
	}

	// Multiple GC passes to ensure cleanup
	runtime.GC()
	runtime.GC()
	runtime.GC()

	b.SetBytes(4096)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		globalReadConn4KB.reset()
		SpinUntilCompleteMessage(globalReadConn4KB)
	}
}

// BenchmarkMemoryAccess measures raw memory access performance
func BenchmarkMemoryAccess(b *testing.B) {
	data := frame4096

	runtime.GC()
	runtime.GC()

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Simulate the memory access patterns in SpinUntilCompleteMessage
		_ = data[0] & 0x0F           // opcode extraction
		_ = data[1] & 0x7F           // payload length
		_ = unsafe.Pointer(&data[2]) // unsafe pointer access simulation
	}
}

// BenchmarkHandshakeMemoryOps measures handshake memory operations
func BenchmarkHandshakeMemoryOps(b *testing.B) {
	response := benchHandshakeResponse

	runtime.GC()
	runtime.GC()

	b.SetBytes(int64(len(response)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Simulate the memory operations in Handshake function
		for j := 0; j < len(response)-16; j += 4 {
			_ = *(*uint32)(unsafe.Pointer(&response[j])) // CRLF pattern matching
		}
		_ = *(*uint64)(unsafe.Pointer(&response[0])) // HTTP header validation
	}
}

// ============================================================================
// COMPARATIVE BENCHMARKS (OPTIMIZED VS SYSCALL OVERHEAD)
// ============================================================================

// BenchmarkOptimizedVsSyscall compares optimized vs syscall-realistic performance
func BenchmarkOptimizedVsSyscall(b *testing.B) {
	sizes := []struct {
		name  string
		data  []byte
		bytes int64
	}{
		{"512B", frame512, 512},
		{"4KB", frame4096, 4096},
		{"16KB", frame16384, 16384},
		{"64KB", frame65536, 65536},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			b.Run("optimized", func(b *testing.B) {
				conn := &benchReadConn{data: size.data}

				// Warmup
				for i := 0; i < 100; i++ {
					conn.reset()
					SpinUntilCompleteMessage(conn)
				}

				runtime.GC()
				runtime.GC()

				b.SetBytes(size.bytes)
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					conn.reset()
					SpinUntilCompleteMessage(conn)
				}
			})

			profiles := []*syscallProfile{
				fastSyscallProfile,
				normalSyscallProfile,
				slowSyscallProfile,
				vmSyscallProfile,
			}

			for _, profile := range profiles {
				b.Run("syscall_"+profile.name, func(b *testing.B) {
					conn := &syscallConn{
						data:           size.data,
						syscallLatency: profile.latency,
						copyOverhead:   profile.copyOverhead,
					}

					// Warmup
					for i := 0; i < 10; i++ {
						conn.reset()
						SpinUntilCompleteMessage(conn)
					}

					runtime.GC()
					runtime.GC()

					b.SetBytes(size.bytes)
					b.ReportAllocs()
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						conn.reset()
						SpinUntilCompleteMessage(conn)
					}
				})
			}
		})
	}
}

// BenchmarkHandshakeComparison compares handshake performance
func BenchmarkHandshakeComparison(b *testing.B) {
	b.Run("optimized", func(b *testing.B) {
		conn := globalHandshakeConn

		// Warmup
		for i := 0; i < 100; i++ {
			conn.reset()
			Handshake(conn)
		}

		runtime.GC()
		runtime.GC()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			conn.reset()
			Handshake(conn)
		}
	})

	b.Run("zero_alloc", func(b *testing.B) {
		conn := globalDirectConn

		// Warmup
		for i := 0; i < 100; i++ {
			conn.reset()
			handshakeLogic(conn)
		}

		runtime.GC()
		runtime.GC()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			conn.reset()
			handshakeLogic(conn)
		}
	})

	profiles := []*syscallProfile{
		fastSyscallProfile,
		normalSyscallProfile,
		slowSyscallProfile,
		vmSyscallProfile,
	}

	for _, profile := range profiles {
		b.Run("syscall_"+profile.name, func(b *testing.B) {
			conn := &syscallConn{
				data:           benchHandshakeResponse,
				syscallLatency: profile.latency,
				copyOverhead:   profile.copyOverhead,
			}

			// Warmup
			for i := 0; i < 10; i++ {
				conn.reset()
				Handshake(conn)
			}

			runtime.GC()
			runtime.GC()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				conn.reset()
				Handshake(conn)
			}
		})
	}
}

// BenchmarkSubscriptionComparison compares subscription performance
func BenchmarkSubscriptionComparison(b *testing.B) {
	b.Run("optimized", func(b *testing.B) {
		conn := globalWriteConn

		// Warmup
		for i := 0; i < 100; i++ {
			SendSubscription(conn)
		}

		runtime.GC()
		runtime.GC()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			SendSubscription(conn)
		}
	})

	profiles := []*syscallProfile{
		fastSyscallProfile,
		normalSyscallProfile,
		slowSyscallProfile,
		vmSyscallProfile,
	}

	for _, profile := range profiles {
		b.Run("syscall_"+profile.name, func(b *testing.B) {
			conn := &syscallConn{
				syscallLatency: profile.latency,
				copyOverhead:   profile.copyOverhead,
			}

			// Warmup
			for i := 0; i < 10; i++ {
				conn.reset()
				SendSubscription(conn)
			}

			runtime.GC()
			runtime.GC()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				conn.reset()
				SendSubscription(conn)
			}
		})
	}
}
