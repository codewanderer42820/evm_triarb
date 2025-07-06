package ws

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"testing"
	"time"
	"unsafe"
)

// Stress tests to push the code to its limits
func TestStressMaxBuffer(t *testing.T) {
	t.Run("Maximum buffer size", func(t *testing.T) {
		// Test with exactly BufferSize payload
		payload := make([]byte, BufferSize)
		rand.Read(payload)
		frame := createFrame(0x1, payload, true)

		conn := &mockConn{readData: frame}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != BufferSize {
			t.Errorf("Expected %d bytes, got %d", BufferSize, len(result))
		}
	})

	t.Run("Fragmented message filling buffer", func(t *testing.T) {
		// Create multiple fragments that together fill the buffer
		chunkSize := BufferSize / 4
		var frameData []byte

		// First 3 fragments (not final)
		for i := 0; i < 3; i++ {
			payload := make([]byte, chunkSize)
			rand.Read(payload)
			frameData = append(frameData, createFrame(0x0, payload, false)...)
		}

		// Final fragment
		payload := make([]byte, chunkSize)
		rand.Read(payload)
		frameData = append(frameData, createFrame(0x0, payload, true)...)

		conn := &mockConn{readData: frameData}
		result, err := SpinUntilCompleteMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != BufferSize {
			t.Errorf("Expected %d bytes, got %d", BufferSize, len(result))
		}
	})
}

func TestStressControlFrames(t *testing.T) {
	t.Run("Many interleaved control frames", func(t *testing.T) {
		var frameData []byte

		// Add 100 control frames
		for i := 0; i < 100; i++ {
			opcode := byte(0x8 + (i % 8)) // Cycle through control opcodes
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

	t.Run("Control frames with maximum payload", func(t *testing.T) {
		var frameData []byte

		// Large control frame payload (125 bytes max for control frames in spec)
		controlPayload := make([]byte, 125)
		rand.Read(controlPayload)
		frameData = append(frameData, createFrame(0x8, controlPayload, true)...)

		// Data frame
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

func TestStressFragmentation(t *testing.T) {
	t.Run("Maximum fragmentation", func(t *testing.T) {
		// Create 1000 tiny fragments
		var frameData []byte
		expectedData := make([]byte, 0, 1000)

		for i := 0; i < 999; i++ {
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
}

// Performance benchmarks for different scenarios
func BenchmarkPerformanceScenarios(b *testing.B) {
	scenarios := []struct {
		name          string
		payloadSize   int
		fragments     int
		controlFrames int
	}{
		{"tiny_message", 10, 1, 0},
		{"small_message", 100, 1, 0},
		{"medium_message", 1000, 1, 0},
		{"large_message", 10000, 1, 0},
		{"huge_message", 100000, 1, 0},
		{"fragmented_small", 1000, 10, 0},
		{"fragmented_large", 10000, 100, 0},
		{"with_control_frames", 1000, 1, 5},
		{"complex_scenario", 5000, 10, 3},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			frameData := createComplexFrame(scenario.payloadSize, scenario.fragments, scenario.controlFrames)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				conn := &mockConn{readData: frameData}
				_, err := SpinUntilCompleteMessage(conn)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func createComplexFrame(totalSize, fragments, controlFrames int) []byte {
	var frameData []byte
	payloadPerFragment := totalSize / fragments

	for i := 0; i < fragments; i++ {
		// Add control frames randomly
		if controlFrames > 0 && i%(fragments/controlFrames+1) == 0 {
			controlPayload := []byte(fmt.Sprintf("control_%d", i))
			frameData = append(frameData, createFrame(0x8+byte(i%4), controlPayload, true)...)
		}

		// Add data fragment
		payload := make([]byte, payloadPerFragment)
		rand.Read(payload)

		isFinal := i == fragments-1
		frameData = append(frameData, createFrame(0x0, payload, isFinal)...)
	}

	return frameData
}

// Memory benchmarks to verify zero-allocation claims
func BenchmarkMemoryProfile(b *testing.B) {
	b.Run("allocation_profile", func(b *testing.B) {
		payload := make([]byte, 10000)
		rand.Read(payload)
		frame := createFrame(0x1, payload, true)

		// Force GC before measurement
		runtime.GC()

		var m1, m2 runtime.MemStats
		runtime.ReadMemStats(&m1)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			conn := &mockConn{readData: frame}
			_, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
		runtime.ReadMemStats(&m2)

		if b.N > 0 {
			allocPerOp := float64(m2.TotalAlloc-m1.TotalAlloc) / float64(b.N)
			b.ReportMetric(allocPerOp, "alloc/op")
		}
	})
}

// Latency measurement benchmarks
func BenchmarkLatencyAnalysis(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("latency_%dbytes", size), func(b *testing.B) {
			payload := make([]byte, size)
			rand.Read(payload)
			frame := createFrame(0x1, payload, true)

			times := make([]time.Duration, b.N)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				conn := &mockConn{readData: frame}

				start := time.Now()
				_, err := SpinUntilCompleteMessage(conn)
				end := time.Now()

				if err != nil {
					b.Fatal(err)
				}
				times[i] = end.Sub(start)
			}
			b.StopTimer()

			// Calculate statistics
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

// CPU instruction counting (approximate)
func BenchmarkInstructionCount(b *testing.B) {
	b.Run("unsafe_operations", func(b *testing.B) {
		data := []byte("HTTP/1.1 101 Switching Protocols")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Benchmark the critical unsafe operations
			_ = *(*uint32)(unsafe.Pointer(&data[0]))
			_ = *(*uint64)(unsafe.Pointer(&data[0]))
		}
	})

	b.Run("endian_conversion", func(b *testing.B) {
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v := *(*uint64)(unsafe.Pointer(&data[0]))
			_ = ((v & 0xFF) << 56) | ((v & 0xFF00) << 40) | ((v & 0xFF0000) << 24) | ((v & 0xFF000000) << 8) |
				((v & 0xFF00000000) >> 8) | ((v & 0xFF0000000000) >> 24) | ((v & 0xFF000000000000) >> 40) | ((v & 0xFF00000000000000) >> 56)
		}
	})

	b.Run("frame_parsing", func(b *testing.B) {
		header := []byte{0x81, 0x7E, 0x03, 0xE8} // TEXT frame, 1000 bytes

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
}

// Throughput benchmarks
func BenchmarkThroughput(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("throughput_%dbytes", size), func(b *testing.B) {
			payload := make([]byte, size)
			rand.Read(payload)
			frame := createFrame(0x1, payload, true)

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				conn := &mockConn{readData: frame}
				_, err := SpinUntilCompleteMessage(conn)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Edge case performance
func BenchmarkEdgeCasePerformance(b *testing.B) {
	b.Run("boundary_126", func(b *testing.B) {
		payload := make([]byte, 126) // Exactly at 16-bit length boundary
		frame := createFrame(0x1, payload, true)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn := &mockConn{readData: frame}
			_, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("boundary_65536", func(b *testing.B) {
		payload := make([]byte, 65536) // Exactly at 64-bit length boundary
		frame := createFrame(0x1, payload, true)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn := &mockConn{readData: frame}
			_, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("max_control_frames", func(b *testing.B) {
		var frameData []byte

		// 1000 control frames before data
		for i := 0; i < 1000; i++ {
			frameData = append(frameData, createFrame(0x8, []byte{}, true)...)
		}
		frameData = append(frameData, createFrame(0x1, []byte("data"), true)...)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn := &mockConn{readData: frameData}
			_, err := SpinUntilCompleteMessage(conn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Concurrency stress test (even though the code isn't concurrent)
func BenchmarkConcurrencyStress(b *testing.B) {
	b.Run("parallel_processing", func(b *testing.B) {
		payload := make([]byte, 10000)
		rand.Read(payload)
		frame := createFrame(0x1, payload, true)

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				conn := &mockConn{readData: frame}
				_, err := SpinUntilCompleteMessage(conn)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
