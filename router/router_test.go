// Realistic production benchmarks for triangular arbitrage router

// BenchmarkAddressLookup - Realistic production address lookup patterns
func BenchmarkAddressLookup(b *testing.B) {
	// Production-scale: 50K DEX pairs, ~80% load factor for optimal Robin Hood performance
	const addressCount = 50000
	const lookupRatio = 0.8 // 80% hit rate, 20% misses (realistic for production)

	addresses := make([][40]byte, addressCount)

	// Pre-populate with realistic Ethereum address patterns
	for i := 0; i < addressCount; i++ {
		// Simulate real Ethereum addresses with proper entropy distribution
		addr := generateMockAddress(uint64(i*1337 + 42)) // More realistic seed variation
		addresses[i] = addr
		RegisterPairAddress(addr[:], PairID(i+100000))
	}

	// Add some unknown addresses for cache miss simulation
	unknownAddresses := make([][40]byte, addressCount/5)
	for i := range unknownAddresses {
		unknownAddresses[i] = generateMockAddress(uint64(addressCount*2 + i*7919))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var addr [40]byte
		if i%10 < 8 { // 80% hit rate
			addr = addresses[i%addressCount]
		} else { // 20% miss rate
			addr = unknownAddresses[i%len(unknownAddresses)]
		}
		_ = lookupPairIDByAddress(addr[:])
	}
}

// BenchmarkTickQuantization - Real market tick value distributions
func BenchmarkTickQuantization(b *testing.B) {
	// Realistic tick distribution based on DeFi market analysis
	// Most ticks cluster around small values with occasional large moves
	inputs := []float64{
		// Common small movements (70% of traffic)
		-0.001, -0.0005, -0.0001, 0.0, 0.0001, 0.0005, 0.001,
		-0.005, -0.001, 0.001, 0.005,
		-0.01, -0.005, 0.005, 0.01,

		// Medium movements (25% of traffic)
		-0.1, -0.05, -0.02, 0.02, 0.05, 0.1,
		-0.5, -0.2, 0.2, 0.5,
		-1.0, -0.5, 0.5, 1.0,

		// Large movements (5% of traffic) - flash crashes, major news
		-10.0, -5.0, -2.0, 2.0, 5.0, 10.0,
		-50.0, -25.0, 25.0, 50.0,
	}

	// Weight distribution to match realistic patterns
	weights := []int{
		7, 7, 7, 10, 7, 7, 7, // Small movements
		5, 5, 5, 5, 4, 4, 4, 4,
		3, 3, 3, 3, 3, 3, // Medium movements
		2, 2, 2, 2, 2, 2,
		1, 1, 1, 1, 1, 1, // Large movements
		1, 1, 1, 1,
	}

	// Build weighted input slice
	weightedInputs := make([]float64, 0, 1000)
	for i, input := range inputs {
		for j := 0; j < weights[i]; j++ {
			weightedInputs = append(weightedInputs, input)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		input := weightedInputs[i%len(weightedInputs)]
		_ = quantizeTickToInt64(input)
	}
}

// BenchmarkTickUpdateDispatch - Production dispatch patterns
func BenchmarkTickUpdateDispatch(b *testing.B) {
	// Realistic production scale: 10K active pairs, 16 cores
	const activePairCount = 10000
	const totalPairCount = 50000 // Many registered but only subset active
	const coreCount = 16

	// Register all pairs but make only subset active
	allAddresses := make([][40]byte, totalPairCount)
	for i := 0; i < totalPairCount; i++ {
		addr := generateMockAddress(uint64(i * 2654435761)) // Good distribution
		allAddresses[i] = addr
		pairID := PairID(i)
		RegisterPairAddress(addr[:], pairID)

		// Realistic core assignment: most pairs on 2-4 cores, some on more
		numCores := 2 + (i % 3) // 2, 3, or 4 cores typically
		if i%50 == 0 {
			numCores = 8 // 2% of pairs on many cores (popular pairs)
		}

		for j := 0; j < numCores; j++ {
			core := uint8((i + j*17) % coreCount)
			RegisterPairToCore(pairID, core)
		}
	}

	// Initialize rings for all cores
	for i := 0; i < coreCount; i++ {
		if coreRings[i] == nil {
			coreRings[i] = ring24.New(2048) // Larger buffers for production
		}
	}

	// Pre-generate realistic log views with Pareto distribution
	// 80% of updates come from 20% of pairs (realistic trading pattern)
	logViews := make([]*types.LogView, 1000)
	for i := range logViews {
		var pairIdx int
		if i%10 < 8 {
			// 80% from top 20% of pairs (hot pairs)
			pairIdx = i % (activePairCount / 5)
		} else {
			// 20% from remaining pairs
			pairIdx = (activePairCount / 5) + (i % (activePairCount * 4 / 5))
		}

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		addr := allAddresses[pairIdx]
		copy(logView.Addr[2:42], addr[:])

		// Realistic reserve ranges: $1K to $100M TVL
		baseReserve := uint64(1000000) // $1M base
		scale := uint64(1 + i%100)     // 1x to 100x multiplier

		reserve0 := baseReserve * scale
		reserve1 := baseReserve * scale * uint64(90+i%20) / 100 // 90-110% ratio variance

		for j := 0; j < 8; j++ {
			logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
			logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
		}

		logViews[i] = logView
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logView := logViews[i%len(logViews)]
		DispatchTickUpdate(logView)
	}
}

// BenchmarkRobinHoodOperations - Realistic hash table operations with production patterns
func BenchmarkRobinHoodOperations(b *testing.B) {
	const dataSize = 20000 // Realistic active address set

	addresses := make([][40]byte, dataSize)
	pairIDs := make([]PairID, dataSize)

	// Generate addresses with realistic clustering (same few bytes for contract families)
	for i := 0; i < dataSize; i++ {
		seed := uint64(i * 16777619) // FNV prime for good distribution
		addresses[i] = generateMockAddress(seed)
		pairIDs[i] = PairID(i + 1000000)
	}

	b.Run("Registration", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			idx := i % dataSize
			RegisterPairAddress(addresses[idx][:], pairIDs[idx])
		}
	})

	// Pre-populate for realistic lookup testing
	for i := 0; i < dataSize; i++ {
		RegisterPairAddress(addresses[i][:], pairIDs[i])
	}

	b.Run("Lookup", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			idx := i % dataSize
			_ = lookupPairIDByAddress(addresses[idx][:])
		}
	})

	b.Run("MixedOperations", func(b *testing.B) {
		// Realistic ratio: 90% lookups, 10% registrations
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if i%10 == 0 {
				// 10% registrations (new pairs being added)
				idx := (i + dataSize) % (dataSize * 2)
				addr := generateMockAddress(uint64(idx * 1000003))
				RegisterPairAddress(addr[:], PairID(idx+2000000))
			} else {
				// 90% lookups (normal operation)
				idx := i % dataSize
				_ = lookupPairIDByAddress(addresses[idx][:])
			}
		}
	})

	b.Run("HighCollisionScenario", func(b *testing.B) {
		// Test with addresses that intentionally collide more
		collisionAddresses := make([][40]byte, 1000)
		for i := 0; i < len(collisionAddresses); i++ {
			addr := generateMockAddress(uint64(i))
			// Force similar hash prefixes to test Robin Hood efficiency
			addr[0] = byte(i % 256)
			addr[1] = byte(i % 256)
			collisionAddresses[i] = addr
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			idx := i % len(collisionAddresses)
			RegisterPairAddress(collisionAddresses[idx][:], PairID(i+3000000))
		}
	})
}

// BenchmarkProcessTickUpdate - Realistic arbitrage processing workloads
func BenchmarkProcessTickUpdate(b *testing.B) {
	// Production-scale executor with realistic cycle distribution
	const queueCount = 20     // Typical queue count per core
	const cyclesPerQueue = 50 // Average cycles per queue
	const totalCycles = queueCount * cyclesPerQueue

	executor := &ArbitrageCoreExecutor{
		isReverseDirection: false,
		priorityQueues:     make([]quantumqueue64.QuantumQueue64, queueCount),
		fanoutTables:       make([][]FanoutEntry, queueCount),
		pairToQueueIndex:   localidx.New(2048),
		cycleStates:        make([]ArbitrageCycleState, totalCycles),
	}

	// Initialize queues and map pairs
	for i := 0; i < queueCount; i++ {
		executor.priorityQueues[i] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(uint32(i+1000), uint32(i))
	}

	// Track handles per queue for fanout setup
	queueHandles := make([][]quantumqueue64.Handle, queueCount)
	for i := range queueHandles {
		queueHandles[i] = make([]quantumqueue64.Handle, 0, cyclesPerQueue)
	}

	// Setup cycles with realistic profitability distribution
	for i := 0; i < totalCycles; i++ {
		queueIdx := i / cyclesPerQueue

		// Realistic tick value distribution:
		// - Most cycles slightly unprofitable (sum > 0)
		// - Few cycles near breakeven
		// - Rare profitable opportunities (sum < 0)
		var baseProfitability float64
		switch i % 100 {
		case 0, 1: // 2% highly profitable
			baseProfitability = -0.05
		case 2, 3, 4, 5, 6: // 5% marginally profitable
			baseProfitability = -0.001
		case 7, 8, 9, 10, 11, 12, 13, 14, 15, 16: // 10% near breakeven
			baseProfitability = 0.0001
		default: // 83% unprofitable
			baseProfitability = 0.01 + float64(i%50)*0.001
		}

		// Add realistic variance
		variance := float64(i%7) * 0.0001

		executor.cycleStates[i] = ArbitrageCycleState{
			tickValues: [3]float64{
				baseProfitability/3 + variance,
				baseProfitability/3 - variance,
				baseProfitability / 3,
			},
			pairIDs: [3]PairID{
				PairID(i * 3),
				PairID(i*3 + 1),
				PairID(i*3 + 2),
			},
		}

		// Add to appropriate queue
		handle, _ := executor.priorityQueues[queueIdx].BorrowSafe()
		priority := quantizeTickToInt64(
			executor.cycleStates[i].tickValues[0] +
				executor.cycleStates[i].tickValues[1] +
				executor.cycleStates[i].tickValues[2])
		executor.priorityQueues[queueIdx].Push(priority, handle, uint64(i))

		queueHandles[queueIdx] = append(queueHandles[queueIdx], handle)
	}

	// Setup realistic fanout tables (edges that affect multiple cycles)
	for queueIdx := 0; queueIdx < queueCount; queueIdx++ {
		fanoutSize := len(queueHandles[queueIdx])
		if fanoutSize > 10 {
			fanoutSize = 10 // Limit fanout for realism
		}

		executor.fanoutTables[queueIdx] = make([]FanoutEntry, fanoutSize)

		for j := 0; j < fanoutSize; j++ {
			cycleIdx := queueIdx*cyclesPerQueue + j

			executor.fanoutTables[queueIdx][j] = FanoutEntry{
				queueHandle:     queueHandles[queueIdx][j],
				edgeIndex:       uint16(j % 3),
				cycleStateIndex: CycleStateIndex(cycleIdx),
				queue:           &executor.priorityQueues[queueIdx],
			}
		}
	}

	// Pre-generate realistic tick updates with market-like patterns
	updates := make([]*TickUpdate, 200)
	for i := range updates {
		// Simulate market microstructure: small random walk with occasional jumps
		baseMove := (float64(i%11) - 5) * 0.0001 // Small random walk
		if i%20 == 0 {
			baseMove *= 10 // Occasional larger moves
		}
		if i%100 == 0 {
			baseMove *= 50 // Rare large moves
		}

		updates[i] = &TickUpdate{
			pairID:      PairID((i % queueCount) + 1000),
			forwardTick: baseMove,
			reverseTick: -baseMove,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		update := updates[i%len(updates)]
		processTickUpdate(executor, update)
	}
}

// BenchmarkArbitrageDetection - Realistic arbitrage opportunity detection
func BenchmarkArbitrageDetection(b *testing.B) {
	const queueCount = 10
	const cyclesPerQueue = 100

	executor := &ArbitrageCoreExecutor{
		isReverseDirection: false,
		priorityQueues:     make([]quantumqueue64.QuantumQueue64, queueCount),
		fanoutTables:       make([][]FanoutEntry, queueCount),
		pairToQueueIndex:   localidx.New(64),
		cycleStates:        make([]ArbitrageCycleState, queueCount*cyclesPerQueue),
	}

	for i := 0; i < queueCount; i++ {
		executor.priorityQueues[i] = *quantumqueue64.New()
		executor.pairToQueueIndex.Put(uint32(i), uint32(i))
		executor.fanoutTables[i] = []FanoutEntry{} // Minimal fanout for this benchmark
	}

	// Create realistic cycle distribution for arbitrage detection
	for i := 0; i < queueCount*cyclesPerQueue; i++ {
		queueIdx := i / cyclesPerQueue

		// Create distribution where most cycles are close to breakeven
		// This tests performance when many cycles need to be checked
		distanceFromBreakeven := float64(i%21-10) * 0.001 // -0.01 to +0.01

		executor.cycleStates[i] = ArbitrageCycleState{
			tickValues: [3]float64{
				distanceFromBreakeven / 3,
				distanceFromBreakeven / 3,
				distanceFromBreakeven / 3,
			},
			pairIDs: [3]PairID{
				PairID(i * 3),
				PairID(i*3 + 1),
				PairID(i*3 + 2),
			},
		}

		handle, _ := executor.priorityQueues[queueIdx].BorrowSafe()
		priority := quantizeTickToInt64(distanceFromBreakeven)
		executor.priorityQueues[queueIdx].Push(priority, handle, uint64(i))
	}

	// Updates that will trigger arbitrage detection across profitability threshold
	updates := make([]*TickUpdate, 100)
	for i := range updates {
		// Updates that push cycles across the profitability threshold
		updates[i] = &TickUpdate{
			pairID:      PairID(i % queueCount),
			forwardTick: -0.005 - float64(i%5)*0.001, // Makes some cycles profitable
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		update := updates[i%len(updates)]
		processTickUpdate(executor, update)
	}
}

// BenchmarkCoreAssignmentLookup - Realistic core assignment patterns
func BenchmarkCoreAssignmentLookup(b *testing.B) {
	const pairCount = 50000 // Production scale
	const coreCount = 64    // Max supported

	// Setup realistic core assignments based on pair importance
	for i := 0; i < pairCount; i++ {
		pairID := PairID(i)

		// Realistic assignment patterns:
		var numCores int
		switch {
		case i < pairCount/100: // Top 1%: major pairs (ETH/USDC etc)
			numCores = 16
		case i < pairCount/20: // Top 5%: popular pairs
			numCores = 8
		case i < pairCount/5: // Top 20%: active pairs
			numCores = 4
		default: // Remaining 80%: long tail
			numCores = 2
		}

		// Assign to spread cores for load balancing
		for j := 0; j < numCores; j++ {
			coreID := uint8((i*17 + j*23) % coreCount) // Good distribution
			RegisterPairToCore(pairID, coreID)
		}
	}

	b.Run("PopularPairs", func(b *testing.B) {
		// Benchmark popular pairs (more cores assigned)
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			pairID := PairID(i % (pairCount / 20)) // Top 5% pairs
			coreAssignments := pairToCoreAssignment[pairID]

			// Simulate realistic dispatch loop
			for coreAssignments != 0 {
				coreID := bits.TrailingZeros64(uint64(coreAssignments))
				coreAssignments &^= 1 << coreID
				// In production: coreRings[coreID].Push(message)
				_ = coreID
			}
		}
	})

	b.Run("LongTailPairs", func(b *testing.B) {
		// Benchmark long tail pairs (fewer cores)
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			pairID := PairID((pairCount / 5) + (i % (pairCount * 4 / 5))) // Bottom 80%
			coreAssignments := pairToCoreAssignment[pairID]

			for coreAssignments != 0 {
				coreID := bits.TrailingZeros64(uint64(coreAssignments))
				coreAssignments &^= 1 << coreID
				_ = coreID
			}
		}
	})

	b.Run("MixedWorkload", func(b *testing.B) {
		// Realistic mix: 80/20 distribution
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var pairID PairID
			if i%10 < 8 {
				// 80% from popular pairs
				pairID = PairID(i % (pairCount / 5))
			} else {
				// 20% from long tail
				pairID = PairID((pairCount / 5) + (i % (pairCount * 4 / 5)))
			}

			coreAssignments := pairToCoreAssignment[pairID]
			for coreAssignments != 0 {
				coreID := bits.TrailingZeros64(uint64(coreAssignments))
				coreAssignments &^= 1 << coreID
				_ = coreID
			}
		}
	})
}

// BenchmarkSystemIntegration - End-to-end realistic workload
func BenchmarkSystemIntegration(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping intensive integration benchmark in short mode")
	}

	// Realistic production scale but manageable for benchmarking
	const triangleCount = 500 // Reasonable for testing
	const coreCount = 8       // Typical server setup

	// Clear state
	for i := range pairAddressKeys[:1000] { // Clear subset for benchmark
		pairAddressKeys[i] = AddressKey{}
		addressToPairID[i] = 0
	}
	for i := range pairToCoreAssignment[:triangleCount*3] {
		pairToCoreAssignment[i] = 0
	}

	// Generate realistic triangles (common tokens forming triangles)
	cycles := make([]ArbitrageTriplet, triangleCount)
	for i := 0; i < triangleCount; i++ {
		baseID := uint32(i * 3)
		cycles[i] = ArbitrageTriplet{
			PairID(baseID),
			PairID(baseID + 1),
			PairID(baseID + 2),
		}
	}

	// Initialize system
	InitializeArbitrageSystem(cycles)
	runtime.Gosched() // Let goroutines start

	// Register addresses with realistic patterns
	for i := 0; i < triangleCount*3; i++ {
		addr := generateMockAddress(uint64(i * 2654435761)) // Good distribution
		RegisterPairAddress(addr[:], PairID(i))
	}

	// Generate realistic workload with market patterns
	workloadSize := 500
	logViews := make([]*types.LogView, workloadSize)

	for i := 0; i < workloadSize; i++ {
		// 80/20 distribution: 80% from hot pairs, 20% from others
		var pairID PairID
		if i%10 < 8 {
			pairID = PairID(i % (triangleCount / 5)) // Hot pairs
		} else {
			pairID = PairID((triangleCount / 5) + (i % (triangleCount * 4 / 5))) // Cold pairs
		}

		addr := generateMockAddress(uint64(pairID * 2654435761))

		logView := &types.LogView{
			Addr: make([]byte, 64),
			Data: make([]byte, 128),
		}

		logView.Addr[0] = '0'
		logView.Addr[1] = 'x'
		copy(logView.Addr[2:42], addr[:])

		// Realistic reserve values: $10K to $100M pools
		baseReserve := uint64(10000000) // $10M base
		multiplier := uint64(1 + i%100) // 1x to 100x

		reserve0 := baseReserve * multiplier
		reserve1 := baseReserve * multiplier * uint64(95+i%10) / 100 // 95-105% variance

		for j := 0; j < 8; j++ {
			logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
			logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
		}

		logViews[i] = logView
	}

	// Let system stabilize
	runtime.Gosched()

	b.ResetTimer()
	b.ReportAllocs()

	// Run realistic workload
	for i := 0; i < b.N; i++ {
		logView := logViews[i%workloadSize]
		DispatchTickUpdate(logView)

		// Simulate realistic batching - not every iteration
		if i%10 == 0 {
			runtime.Gosched() // Yield occasionally like real workload
		}
	}

	b.StopTimer()

	// Clean shutdown
	control.Shutdown()
	runtime.Gosched()
}

// BenchmarkMemoryAccessPatterns - Cache-friendly vs cache-hostile patterns
func BenchmarkMemoryAccessPatterns(b *testing.B) {
	const cycleCount = 1000

	cycles := make([]ArbitrageCycleState, cycleCount)
	for i := range cycles {
		cycles[i] = ArbitrageCycleState{
			tickValues: [3]float64{
				float64(i) * 0.001,
				float64(i) * 0.002,
				float64(i) * 0.003,
			},
			pairIDs: [3]PairID{
				PairID(i * 3),
				PairID(i*3 + 1),
				PairID(i*3 + 2),
			},
		}
	}

	b.Run("SequentialAccess", func(b *testing.B) {
		// Cache-friendly sequential access
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		for i := 0; i < b.N; i++ {
			cycle := &cycles[i%cycleCount]
			sum += cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		}
		_ = sum
	})

	b.Run("RandomAccess", func(b *testing.B) {
		// Cache-hostile random access (realistic for hash table lookups)
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		for i := 0; i < b.N; i++ {
			// Simulate hash-based access pattern
			idx := (i * 2654435761) % cycleCount // Good pseudo-random distribution
			cycle := &cycles[idx]
			sum += cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		}
		_ = sum
	})

	b.Run("StridedAccess", func(b *testing.B) {
		// Strided access pattern (realistic for fanout processing)
		b.ResetTimer()
		b.ReportAllocs()

		sum := 0.0
		stride := 17 // Prime number for good coverage
		for i := 0; i < b.N; i++ {
			idx := (i * stride) % cycleCount
			cycle := &cycles[idx]
			sum += cycle.tickValues[0] + cycle.tickValues[1] + cycle.tickValues[2]
		}
		_ = sum
	})
}

// BenchmarkNetworkTrafficSimulation - Realistic update frequency patterns
func BenchmarkNetworkTrafficSimulation(b *testing.B) {
	const pairCount = 1000

	// Setup pairs with realistic address distribution
	addresses := make([][40]byte, pairCount)
	for i := 0; i < pairCount; i++ {
		addr := generateMockAddress(uint64(i * 16777619))
		addresses[i] = addr
		RegisterPairAddress(addr[:], PairID(i))
		RegisterPairToCore(PairID(i), uint8(i%8))
	}

	// Initialize rings
	for i := 0; i < 8; i++ {
		if coreRings[i] == nil {
			coreRings[i] = ring24.New(1024)
		}
	}

	b.Run("BurstyTraffic", func(b *testing.B) {
		// Simulate burst traffic patterns (realistic for DEX updates)
		logViews := make([]*types.LogView, 100)

		// Create burst of updates for hot pairs
		for i := range logViews {
			hotPairIdx := i % 20 // Focus on top 20 pairs
			addr := addresses[hotPairIdx]

			logView := &types.LogView{
				Addr: make([]byte, 64),
				Data: make([]byte, 128),
			}

			logView.Addr[0] = '0'
			logView.Addr[1] = 'x'
			copy(logView.Addr[2:42], addr[:])

			// Simulate volatile period with large reserve changes
			base := uint64(1000000000)                  // $1B base
			variance := uint64(i%20) * uint64(10000000) // High variance

			reserve0 := base + variance
			reserve1 := base - variance

			for j := 0; j < 8; j++ {
				logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
				logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
			}

			logViews[i] = logView
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			logView := logViews[i%len(logViews)]
			DispatchTickUpdate(logView)
		}
	})

	b.Run("SteadyTraffic", func(b *testing.B) {
		// Simulate steady-state traffic distribution
		logViews := make([]*types.LogView, 200)

		for i := range logViews {
			// More uniform distribution across pairs
			pairIdx := i % pairCount
			addr := addresses[pairIdx]

			logView := &types.LogView{
				Addr: make([]byte, 64),
				Data: make([]byte, 128),
			}

			logView.Addr[0] = '0'
			logView.Addr[1] = 'x'
			copy(logView.Addr[2:42], addr[:])

			// Smaller, more typical reserve changes
			base := uint64(50000000)                   // $50M typical pool
			variance := uint64(i%10) * uint64(1000000) // Smaller variance

			reserve0 := base + variance
			reserve1 := base + variance

			for j := 0; j < 8; j++ {
				logView.Data[24+j] = byte(reserve0 >> (8 * (7 - j)))
				logView.Data[56+j] = byte(reserve1 >> (8 * (7 - j)))
			}

			logViews[i] = logView
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			logView := logViews[i%len(logViews)]
			DispatchTickUpdate(logView)
		}
	})
}