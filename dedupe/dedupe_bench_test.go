package dedupe

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"main/constants"
	"testing"
	"time"
)

// BenchmarkDeduper_BasicOperations benchmarks fundamental operations
func BenchmarkDeduper_BasicOperations(b *testing.B) {
	d := Deduper{}

	// Pre-generate test data to avoid allocation overhead in benchmark
	testData := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
		currentBlock     uint32
	}, 1000)

	for i := range testData {
		testData[i] = struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
			currentBlock     uint32
		}{
			block:        uint32(i + 1000),
			tx:           uint32(i % 65536),
			log:          uint32(i % 32),
			topicHi:      uint64(i) << 32,
			topicLo:      uint64(i + 1000000),
			currentBlock: uint32(i + 1000),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := &testData[i%len(testData)]
		d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.currentBlock)
	}
}

// BenchmarkDeduper_NewEntries benchmarks processing all new entries (cache misses)
func BenchmarkDeduper_NewEntries(b *testing.B) {
	d := Deduper{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Each entry is unique, so all are cache misses
		block := uint32(i + 1000)
		tx := uint32(i % 65536)
		log := uint32(i % 32)
		topicHi := uint64(i) << 32
		topicLo := uint64(i + 1000000)

		result := d.Check(block, tx, log, topicHi, topicLo, block)
		if !result {
			b.Fatal("New entry should be accepted")
		}
	}

	b.Logf("Processed %d unique entries", b.N)
}

// BenchmarkDeduper_Duplicates benchmarks processing duplicate entries (cache hits)
func BenchmarkDeduper_Duplicates(b *testing.B) {
	d := Deduper{}

	// Insert one entry
	const block, tx, log = 1000, 5, 2
	const topicHi, topicLo = 0x1234567890abcdef, 0xfedcba0987654321
	d.Check(block, tx, log, topicHi, topicLo, block)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Same entry every time - always a cache hit
		result := d.Check(block, tx, log, topicHi, topicLo, block)
		if result {
			b.Fatal("Duplicate entry should be rejected")
		}
	}

	b.Logf("Processed %d duplicate lookups", b.N)
}

// BenchmarkDeduper_MixedWorkload benchmarks realistic mixed workload
func BenchmarkDeduper_MixedWorkload(b *testing.B) {
	d := Deduper{}

	// Pre-populate with some entries to simulate realistic cache state
	for i := 0; i < 1000; i++ {
		block := uint32(i + 1000)
		tx := uint32(i % 100)
		log := uint32(i % 10)
		topicHi := uint64(i) << 32
		topicLo := uint64(i)
		d.Check(block, tx, log, topicHi, topicLo, block)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 70% new entries, 30% duplicates (realistic ratio)
		if i%10 < 7 {
			// New entry
			block := uint32(i + 2000)
			tx := uint32(i % 65536)
			log := uint32(i % 32)
			topicHi := uint64(i) << 32
			topicLo := uint64(i + 2000000)
			d.Check(block, tx, log, topicHi, topicLo, block)
		} else {
			// Duplicate entry (from pre-populated set)
			idx := i % 1000
			block := uint32(idx + 1000)
			tx := uint32(idx % 100)
			log := uint32(idx % 10)
			topicHi := uint64(idx) << 32
			topicLo := uint64(idx)
			d.Check(block, tx, log, topicHi, topicLo, block)
		}
	}
}

// BenchmarkDeduper_HighFrequency benchmarks high-frequency blockchain scenarios
func BenchmarkDeduper_HighFrequency(b *testing.B) {
	d := Deduper{}

	// Simulate high-frequency chain like Solana
	const logsPerBlock = 5000
	const blocksToSimulate = 100

	b.ResetTimer()
	b.ReportAllocs()

	blockCounter := 0
	logCounter := 0

	for i := 0; i < b.N; i++ {
		block := uint32(1000 + blockCounter)
		tx := uint32(logCounter % 1000)
		log := uint32(logCounter % 50)
		topicHi := uint64(block)<<32 | uint64(tx)
		topicLo := uint64(log)<<32 | uint64(blockCounter)

		d.Check(block, tx, log, topicHi, topicLo, block)

		logCounter++
		if logCounter >= logsPerBlock {
			logCounter = 0
			blockCounter++
			if blockCounter >= blocksToSimulate {
				blockCounter = 0
			}
		}
	}

	b.Logf("Simulated %d blocks with %d logs each", blocksToSimulate, logsPerBlock)
}

// BenchmarkDeduper_CryptoRandomness benchmarks with cryptographically random data
func BenchmarkDeduper_CryptoRandomness(b *testing.B) {
	d := Deduper{}

	// Pre-generate random data to avoid crypto overhead in benchmark
	randomData := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
	}, 10000)

	for i := range randomData {
		var buf [32]byte
		rand.Read(buf[:])

		randomData[i] = struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
		}{
			block:   binary.BigEndian.Uint32(buf[0:4]),
			tx:      binary.BigEndian.Uint32(buf[4:8]) & 0xFFFF,
			log:     binary.BigEndian.Uint32(buf[8:12]) & 0xFFFF,
			topicHi: binary.BigEndian.Uint64(buf[12:20]),
			topicLo: binary.BigEndian.Uint64(buf[20:28]),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := &randomData[i%len(randomData)]
		d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.block)
	}

	b.Logf("Processed %d cryptographically random entries", b.N)
}

// BenchmarkDeduper_ZeroValues benchmarks zero value handling
func BenchmarkDeduper_ZeroValues(b *testing.B) {
	d := Deduper{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i == 0 {
			// First zero entry should be accepted
			result := d.Check(0, 0, 0, 0, 0, 0)
			if !result {
				b.Fatal("First zero entry should be accepted")
			}
		} else {
			// Subsequent zero entries should be rejected
			result := d.Check(0, 0, 0, 0, 0, 0)
			if result {
				b.Fatal("Duplicate zero entry should be rejected")
			}
		}
	}
}

// BenchmarkDeduper_MaxValues benchmarks maximum value handling
func BenchmarkDeduper_MaxValues(b *testing.B) {
	d := Deduper{}

	const maxBlk = uint32(0xFFFFFFFF)
	const maxTx = uint32(0xFFFF)
	const maxLog = uint32(0xFFFF)
	const maxTopicHi = uint64(0xFFFFFFFFFFFFFFFF)
	const maxTopicLo = uint64(0xFFFFFFFFFFFFFFFF)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i == 0 {
			// First max entry should be accepted
			result := d.Check(maxBlk, maxTx, maxLog, maxTopicHi, maxTopicLo, maxBlk)
			if !result {
				b.Fatal("First max entry should be accepted")
			}
		} else {
			// Subsequent max entries should be rejected
			result := d.Check(maxBlk, maxTx, maxLog, maxTopicHi, maxTopicLo, maxBlk)
			if result {
				b.Fatal("Duplicate max entry should be rejected")
			}
		}
	}
}

// BenchmarkDeduper_HashCollisions benchmarks hash collision scenarios
func BenchmarkDeduper_HashCollisions(b *testing.B) {
	d := Deduper{}

	// Pre-generate data that might cause hash collisions
	collisionData := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
	}, 1000)

	for i := range collisionData {
		// Use patterns that might hash to similar values
		collisionData[i] = struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
		}{
			block:   uint32(i << 16), // Spacing to encourage collisions
			tx:      uint32(i % 256),
			log:     uint32(i % 16),
			topicHi: uint64(i) << 32,
			topicLo: uint64(i) ^ 0xAAAAAAAAAAAAAAAA,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := &collisionData[i%len(collisionData)]
		d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.block)
	}
}

// BenchmarkDeduper_StalenessChecks benchmarks staleness detection
func BenchmarkDeduper_StalenessChecks(b *testing.B) {
	d := Deduper{}

	// Insert entry at old block
	const oldBlock = 1000
	d.Check(oldBlock, 5, 2, 0x1234567890abcdef, 0xfedcba0987654321, oldBlock)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Use different coordinates to avoid interference, but test staleness concept
		block := uint32(oldBlock + (i % 100))
		tx := uint32(5 + (i % 10))
		log := uint32(2 + (i % 5))
		currentBlock := uint32(oldBlock + constants.MaxReorg + 1 + (i % 100))
		d.Check(block, tx, log, 0x1234567890abcdef+uint64(i), 0xfedcba0987654321+uint64(i), currentBlock)
	}
}

// BenchmarkDeduper_ThroughputTest measures maximum throughput
func BenchmarkDeduper_ThroughputTest(b *testing.B) {
	d := Deduper{}

	// Pre-generate diverse test data
	const dataSize = 100000
	testData := make([]struct {
		block, tx, log   uint32
		topicHi, topicLo uint64
	}, dataSize)

	for i := range testData {
		seed := make([]byte, 8)
		binary.LittleEndian.PutUint64(seed, uint64(i))
		hash := sha256.Sum256(seed)

		testData[i] = struct {
			block, tx, log   uint32
			topicHi, topicLo uint64
		}{
			block:   (binary.LittleEndian.Uint32(hash[0:4]) % 10000) + 1000,
			tx:      binary.LittleEndian.Uint32(hash[4:8]) % 65536,
			log:     binary.LittleEndian.Uint32(hash[8:12]) % 100,
			topicHi: binary.LittleEndian.Uint64(hash[12:20]),
			topicLo: binary.LittleEndian.Uint64(hash[20:28]),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()

	for i := 0; i < b.N; i++ {
		data := &testData[i%dataSize]
		d.Check(data.block, data.tx, data.log, data.topicHi, data.topicLo, data.block)
	}

	elapsed := time.Since(start)
	opsPerSecond := float64(b.N) / elapsed.Seconds()
	nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)

	b.Logf("Throughput: %.0f ops/sec", opsPerSecond)
	b.Logf("Latency: %.2f ns/op", nsPerOp)
	b.Logf("Total operations: %d", b.N)
}

// BenchmarkDeduper_CacheUtilization benchmarks different cache utilization levels
func BenchmarkDeduper_CacheUtilization(b *testing.B) {
	cacheSize := 1 << constants.RingBits

	utilizationLevels := []struct {
		name string
		pct  float64
	}{
		{"1pct", 0.01},
		{"5pct", 0.05},
		{"10pct", 0.10},
		{"25pct", 0.25},
		{"50pct", 0.50},
		{"75pct", 0.75},
		{"90pct", 0.90},
	}

	for _, level := range utilizationLevels {
		b.Run(level.name, func(b *testing.B) {
			d := Deduper{}
			numEntries := int(float64(cacheSize) * level.pct)

			// Pre-populate cache to target utilization
			for i := 0; i < numEntries; i++ {
				block := uint32(i + 1000)
				tx := uint32(i % 65536)
				log := uint32(i % 100)
				topicHi := uint64(i) << 32
				topicLo := uint64(i + 1000000)
				d.Check(block, tx, log, topicHi, topicLo, block)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Mix of new entries and lookups of existing entries
				if i%2 == 0 {
					// New entry
					block := uint32(i + numEntries + 1000)
					tx := uint32(i % 65536)
					log := uint32(i % 100)
					topicHi := uint64(i+numEntries) << 32
					topicLo := uint64(i + numEntries + 1000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				} else {
					// Lookup existing entry
					idx := i % numEntries
					block := uint32(idx + 1000)
					tx := uint32(idx % 65536)
					log := uint32(idx % 100)
					topicHi := uint64(idx) << 32
					topicLo := uint64(idx + 1000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				}
			}

			b.Logf("Cache utilization: %.1f%% (%d entries)", level.pct*100, numEntries)
		})
	}
}

// BenchmarkDeduper_BitPatterns benchmarks different bit patterns
func BenchmarkDeduper_BitPatterns(b *testing.B) {
	patterns := []struct {
		name    string
		block   uint32
		tx      uint32
		log     uint32
		topicHi uint64
		topicLo uint64
	}{
		{"AllOnes", 0xFFFFFFFF, 0xFFFF, 0xFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
		{"AllZeros", 0, 0, 0, 0, 0},
		{"Alternating", 0xAAAAAAAA, 0xAAAA, 0xAAAA, 0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA},
		{"PowersOf2", 0x80000000, 0x8000, 0x8000, 0x8000000000000000, 0x8000000000000000},
		{"Sequential", 0x01234567, 0x0123, 0x4567, 0x0123456789ABCDEF, 0xFEDCBA9876543210},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			d := Deduper{}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if i == 0 {
					// First should be accepted
					result := d.Check(pattern.block, pattern.tx, pattern.log, pattern.topicHi, pattern.topicLo, pattern.block)
					if !result {
						b.Fatalf("First %s pattern should be accepted", pattern.name)
					}
				} else {
					// Subsequent should be duplicates
					result := d.Check(pattern.block, pattern.tx, pattern.log, pattern.topicHi, pattern.topicLo, pattern.block)
					if result {
						b.Fatalf("Duplicate %s pattern should be rejected", pattern.name)
					}
				}
			}
		})
	}
}

// BenchmarkDeduper_MemoryAccess benchmarks memory access patterns
func BenchmarkDeduper_MemoryAccess(b *testing.B) {
	d := Deduper{}

	b.Run("Sequential", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			block := uint32(i + 1000)
			d.Check(block, 0, 0, uint64(i), uint64(i), block)
		}
	})

	b.Run("Random", func(b *testing.B) {
		// Pre-generate random access pattern
		randomOrder := make([]int, 10000)
		for i := range randomOrder {
			randomOrder[i] = i
		}
		// Simple shuffle
		for i := range randomOrder {
			j := (i*1103515245 + 12345) % len(randomOrder)
			randomOrder[i], randomOrder[j] = randomOrder[j], randomOrder[i]
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			idx := randomOrder[i%len(randomOrder)]
			block := uint32(idx + 1000)
			d.Check(block, 0, 0, uint64(idx), uint64(idx), block)
		}
	})

	b.Run("Clustered", func(b *testing.B) {
		b.ReportAllocs()
		clusterSize := 64
		for i := 0; i < b.N; i++ {
			// Access in clusters to simulate cache locality
			clusterBase := (i / clusterSize) * clusterSize * 1000
			offset := i % clusterSize
			block := uint32(clusterBase + offset + 1000)
			d.Check(block, 0, 0, uint64(clusterBase+offset), uint64(clusterBase+offset), block)
		}
	})
}

// BenchmarkDeduper_ScalabilityTest tests performance at different scales
func BenchmarkDeduper_ScalabilityTest(b *testing.B) {
	scales := []struct {
		name       string
		numEntries int
	}{
		{"1K", 1000},
		{"10K", 10000},
		{"100K", 100000},
		{"1M", 1000000},
	}

	for _, scale := range scales {
		b.Run(scale.name, func(b *testing.B) {
			if b.N < scale.numEntries {
				b.Skip("Not enough iterations for this scale")
			}

			d := Deduper{}

			b.ResetTimer()
			b.ReportAllocs()

			// Process entries up to the scale limit
			processed := 0
			for i := 0; i < b.N && processed < scale.numEntries; i++ {
				block := uint32(i + 1000)
				tx := uint32(i % 65536)
				log := uint32(i % 100)
				topicHi := uint64(i) << 32
				topicLo := uint64(i + 1000000)

				d.Check(block, tx, log, topicHi, topicLo, block)
				processed++
			}

			opsPerSecond := float64(processed) / b.Elapsed().Seconds()
			b.Logf("Scale: %s, Ops/sec: %.0f", scale.name, opsPerSecond)
		})
	}
}

// BenchmarkDeduper_ComparativeAnalysis provides detailed performance analysis
func BenchmarkDeduper_ComparativeAnalysis(b *testing.B) {
	b.Run("PeakPerformance", func(b *testing.B) {
		d := Deduper{}

		// Optimal case: all unique entries, no collisions
		b.ResetTimer()
		b.ReportAllocs()

		start := time.Now()
		for i := 0; i < b.N; i++ {
			// Widely spaced entries to minimize collisions
			block := uint32(i*1000 + 1000)
			tx := uint32(i % 65536)
			log := uint32(i % 32)
			topicHi := uint64(i) << 32
			topicLo := uint64(i + 1000000)

			result := d.Check(block, tx, log, topicHi, topicLo, block)
			if !result {
				b.Fatal("New entry should be accepted")
			}
		}
		elapsed := time.Since(start)

		nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)
		opsPerSec := float64(b.N) / elapsed.Seconds()

		b.Logf("Peak Performance Analysis:")
		b.Logf("  Latency: %.2f ns/op", nsPerOp)
		b.Logf("  Throughput: %.0f ops/sec", opsPerSec)
		b.Logf("  Total ops: %d", b.N)

		// Performance categories
		if nsPerOp < 2.0 {
			b.Logf("  Performance: EXCEPTIONAL (sub-2ns)")
		} else if nsPerOp < 5.0 {
			b.Logf("  Performance: EXCELLENT (sub-5ns)")
		} else if nsPerOp < 10.0 {
			b.Logf("  Performance: GOOD (sub-10ns)")
		} else {
			b.Logf("  Performance: NEEDS_OPTIMIZATION (>10ns)")
		}

		// Throughput categories
		if opsPerSec > 500_000_000 {
			b.Logf("  Throughput: ULTRA_HIGH (>500M ops/sec)")
		} else if opsPerSec > 100_000_000 {
			b.Logf("  Throughput: HIGH (>100M ops/sec)")
		} else if opsPerSec > 10_000_000 {
			b.Logf("  Throughput: MODERATE (>10M ops/sec)")
		} else {
			b.Logf("  Throughput: LOW (<10M ops/sec)")
		}
	})
}

// BenchmarkDeduper_RealWorldSimulation simulates real-world blockchain workloads
func BenchmarkDeduper_RealWorldSimulation(b *testing.B) {
	scenarios := []struct {
		name          string
		duplicateRate float64 // Percentage of entries that are duplicates
		burstiness    int     // How bursty the traffic is
		description   string
	}{
		{"Ethereum", 0.05, 1, "Low duplicate rate, steady traffic"},
		{"Solana", 0.15, 3, "Medium duplicate rate, bursty traffic"},
		{"Polygon", 0.25, 2, "High duplicate rate, moderately bursty"},
		{"Arbitrum", 0.10, 1, "Low-medium duplicate rate, steady traffic"},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			d := Deduper{}

			// Pre-populate with some entries for realistic duplicate testing
			prePopSize := 10000
			for i := 0; i < prePopSize; i++ {
				block := uint32(i + 1000)
				tx := uint32(i % 1000)
				log := uint32(i % 50)
				topicHi := uint64(i) << 32
				topicLo := uint64(i + 1000000)
				d.Check(block, tx, log, topicHi, topicLo, block)
			}

			b.ResetTimer()
			b.ReportAllocs()

			burstCounter := 0
			for i := 0; i < b.N; i++ {
				// Simulate burstiness
				if burstCounter < scenario.burstiness {
					burstCounter++
				} else {
					burstCounter = 0
					continue // Skip this iteration to simulate burst gaps
				}

				// Determine if this should be a duplicate
				isDuplicate := float64(i%100) < scenario.duplicateRate*100

				if isDuplicate && i > 100 {
					// Use existing entry from pre-populated set
					idx := i % prePopSize
					block := uint32(idx + 1000)
					tx := uint32(idx % 1000)
					log := uint32(idx % 50)
					topicHi := uint64(idx) << 32
					topicLo := uint64(idx + 1000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				} else {
					// New entry
					block := uint32(i + prePopSize + 1000)
					tx := uint32(i % 5000)
					log := uint32(i % 100)
					topicHi := uint64(i+prePopSize) << 32
					topicLo := uint64(i + prePopSize + 1000000)
					d.Check(block, tx, log, topicHi, topicLo, block)
				}
			}

			b.Logf("%s: %s", scenario.name, scenario.description)
		})
	}
}
