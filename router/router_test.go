// ============================================================================
// FIXED TEST UTILITIES WITH PROPER HANDLE MANAGEMENT
// ============================================================================

// TestExecutorBuilder helps create properly initialized executors for tests
type TestExecutorBuilder struct {
	poolSize       int
	numQueues      int
	cyclesPerQueue int
}

func NewTestExecutorBuilder() *TestExecutorBuilder {
	return &TestExecutorBuilder{
		poolSize:       1000,
		numQueues:      1,
		cyclesPerQueue: 10,
	}
}

func (b *TestExecutorBuilder) WithPoolSize(size int) *TestExecutorBuilder {
	b.poolSize = size
	return b
}

func (b *TestExecutorBuilder) WithQueues(numQueues int) *TestExecutorBuilder {
	b.numQueues = numQueues
	return b
}

func (b *TestExecutorBuilder) WithCyclesPerQueue(cycles int) *TestExecutorBuilder {
	b.cyclesPerQueue = cycles
	return b
}

func (b *TestExecutorBuilder) Build() *ArbitrageCoreExecutor {
	// PHASE 1: ALLOCATE AND INITIALIZE SHARED POOL
	pool := make([]pooledquantumqueue.Entry, b.poolSize)
	for i := range pool {
		pool[i].Tick = -1
		pool[i].Prev = pooledquantumqueue.Handle(^uint64(0))
		pool[i].Next = pooledquantumqueue.Handle(^uint64(0))
		pool[i].Data = 0
	}

	// PHASE 2: CREATE EXECUTOR WITH HANDLE ALLOCATOR
	executor := &ArbitrageCoreExecutor{
		pairToQueueIndex:   localidx.New(constants.DefaultLocalIdxSize),
		isReverseDirection: false,
		cycleStates:        make([]ArbitrageCycleState, 0),
		fanoutTables:       make([][]FanoutEntry, b.numQueues),
		priorityQueues:     make([]pooledquantumqueue.PooledQuantumQueue, b.numQueues),
		sharedArena:        pool,
		handleAllocator:    NewHandleAllocator(pool), // FIXED: Proper initialization
	}

	// PHASE 3: CALCULATE ARENA PARTITIONS
	entriesPerQueue := b.poolSize / b.numQueues
	if entriesPerQueue < 64 {
		entriesPerQueue = 64
	}

	// PHASE 4: INITIALIZE QUEUES WITH PROPER ARENA PARTITIONING
	for i := 0; i < b.numQueues; i++ {
		arenaOffset := i * entriesPerQueue
		if arenaOffset >= len(pool) {
			arenaOffset = 0 // Fallback to shared arena start
		}

		arenaPtr := unsafe.Pointer(&pool[arenaOffset])
		executor.priorityQueues[i] = *pooledquantumqueue.New(arenaPtr)

		// PHASE 5: POPULATE QUEUE WITH TEST CYCLES
		for j := 0; j < b.cyclesPerQueue; j++ {
			// FIXED: Proper handle allocation
			globalHandle, available := executor.handleAllocator.AllocateHandle()
			if !available {
				break // Pool exhausted
			}

			// Create cycle state
			cycleIndex := len(executor.cycleStates)
			executor.cycleStates = append(executor.cycleStates, ArbitrageCycleState{
				pairIDs:    [3]PairID{PairID(i*3 + 1), PairID(i*3 + 2), PairID(i*3 + 3)},
				tickValues: [3]float64{0.0, 0.0, 0.0},
			})

			// FIXED: Calculate relative handle for this queue's arena
			relativeHandle := pooledquantumqueue.Handle(uint64(globalHandle) - uint64(arenaOffset))

			// Generate distributed priority to avoid clustering
			cycleHash := utils.Mix64(uint64(cycleIndex))
			randBits := cycleHash & 0xFFFF
			initPriority := int64(196608 + randBits)

			// Insert into queue
			executor.priorityQueues[i].Push(initPriority, relativeHandle, uint64(cycleIndex))

			// Setup fanout entries
			if len(executor.fanoutTables[i]) < 5 { // Limit fanout entries
				executor.fanoutTables[i] = append(executor.fanoutTables[i], FanoutEntry{
					cycleStateIndex: uint64(cycleIndex),
					edgeIndex:       uint64(j % 3),
					queue:           &executor.priorityQueues[i],
					queueHandle:     relativeHandle, // FIXED: Use relative handle
				})
			}
		}

		// Setup pair mapping
		executor.pairToQueueIndex.Put(uint32(i+1), uint32(i))
	}

	return executor
}

// TestExecutorCleaner helps clean up test executors properly
type TestExecutorCleaner struct {
	executor *ArbitrageCoreExecutor
}

func NewTestCleaner(executor *ArbitrageCoreExecutor) *TestExecutorCleaner {
	return &TestExecutorCleaner{executor: executor}
}

func (c *TestExecutorCleaner) Cleanup() {
	if c.executor == nil || c.executor.handleAllocator == nil {
		return
	}

	// FIXED: Return all allocated handles to free pool
	// Note: In a real cleanup, we'd track which handles were allocated
	// For tests, we can reset the entire allocator state
	c.executor.handleAllocator.nextHandle = 0
	c.executor.handleAllocator.freeHandles = make([]pooledquantumqueue.Handle, len(c.executor.sharedArena))
	for i := range c.executor.handleAllocator.freeHandles {
		c.executor.handleAllocator.freeHandles[i] = pooledquantumqueue.Handle(i)
	}

	// Reset pool entries
	for i := range c.executor.sharedArena {
		c.executor.sharedArena[i].Tick = -1
		c.executor.sharedArena[i].Prev = pooledquantumqueue.Handle(^uint64(0))
		c.executor.sharedArena[i].Next = pooledquantumqueue.Handle(^uint64(0))
		c.executor.sharedArena[i].Data = 0
	}
}

// FIXED: Example of corrected test using proper utilities
func TestCoreProcessingLogicFixed(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("ForwardDirectionProcessingFixed", func(t *testing.T) {
		// FIXED: Use proper test builder
		executor := NewTestExecutorBuilder().
			WithPoolSize(1000).
			WithQueues(1).
			WithCyclesPerQueue(3).
			Build()

		defer NewTestCleaner(executor).Cleanup()

		// Test processing
		pairID := PairID(1)
		update := &TickUpdate{
			pairID:      pairID,
			forwardTick: 1.5,
			reverseTick: -1.5,
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			processTickUpdate(executor, update)
		})

		// Verify queue is not empty after processing
		fixture.EXPECT_FALSE(executor.priorityQueues[0].Empty(), "Queue should have cycles after processing")
	})

	t.Run("ReverseDirectionProcessingFixed", func(t *testing.T) {
		// FIXED: Use proper test builder with reverse direction
		executor := NewTestExecutorBuilder().
			WithPoolSize(1000).
			WithQueues(1).
			WithCyclesPerQueue(3).
			Build()

		executor.isReverseDirection = true // Set reverse direction
		defer NewTestCleaner(executor).Cleanup()

		pairID := PairID(1)
		update := &TickUpdate{
			pairID:      pairID,
			forwardTick: 2.0,
			reverseTick: -2.0,
		}

		fixture.EXPECT_NO_FATAL_FAILURE(func() {
			processTickUpdate(executor, update)
		})

		// Verify processing completed
		fixture.EXPECT_FALSE(executor.priorityQueues[0].Empty(), "Queue should have cycles after processing")
	})
}

// FIXED: Example of corrected fanout test
func TestFanoutProcessingLogicFixed(t *testing.T) {
	fixture := NewRouterTestFixture(t)
	fixture.SetUp()

	t.Run("FanoutTickValueUpdatesFixed", func(t *testing.T) {
		// FIXED: Use proper test builder
		executor := NewTestExecutorBuilder().
			WithPoolSize(2000).
			WithQueues(1).
			WithCyclesPerQueue(0). // Don't auto-populate, we'll do it manually
			Build()

		defer NewTestCleaner(executor).Cleanup()

		// FIXED: Create cycles manually with known initial values
		executor.cycleStates = make([]ArbitrageCycleState, 3)
		executor.cycleStates[0] = ArbitrageCycleState{
			pairIDs:    [3]PairID{1, 2, 3},
			tickValues: [3]float64{1.0, 2.0, 3.0}, // Initial sum = 6.0
		}
		executor.cycleStates[1] = ArbitrageCycleState{
			pairIDs:    [3]PairID{1, 4, 5},
			tickValues: [3]float64{-1.0, 0.5, 1.5}, // Initial sum = 1.0
		}
		executor.cycleStates[2] = ArbitrageCycleState{
			pairIDs:    [3]PairID{1, 6, 7},
			tickValues: [3]float64{0.0, -2.0, 1.0}, // Initial sum = -1.0
		}

		// FIXED: Allocate handles properly
		handle0, _ := executor.handleAllocator.AllocateHandle()
		handle1, _ := executor.handleAllocator.AllocateHandle()
		handle2, _ := executor.handleAllocator.AllocateHandle()

		// FIXED: Set up fanout table with proper handles
		executor.fanoutTables[0] = []FanoutEntry{
			{
				cycleStateIndex: 0,
				edgeIndex:       0,
				queue:           &executor.priorityQueues[0],
				queueHandle:     handle0, // FIXED: Use allocated handle
			},
			{
				cycleStateIndex: 1,
				edgeIndex:       0,
				queue:           &executor.priorityQueues[0],
				queueHandle:     handle1, // FIXED: Use allocated handle
			},
			{
				cycleStateIndex: 2,
				edgeIndex:       0,
				queue:           &executor.priorityQueues[0],
				queueHandle:     handle2, // FIXED: Use allocated handle
			},
		}

		// Set up pair mapping
		executor.pairToQueueIndex.Put(1, 0)

		// Add cycles to queue with initial priorities
		executor.priorityQueues[0].Push(quantizeTickToInt64(6.0), handle0, 0)
		executor.priorityQueues[0].Push(quantizeTickToInt64(1.0), handle1, 1)
		executor.priorityQueues[0].Push(quantizeTickToInt64(-1.0), handle2, 2)

		// Process an update
		update := &TickUpdate{
			pairID:      1,
			forwardTick: 5.0,
			reverseTick: -5.0,
		}

		processTickUpdate(executor, update)

		// Verify tick values were updated
		fixture.EXPECT_EQ(5.0, executor.cycleStates[0].tickValues[0], "Cycle 0 edge 0 should be updated")
		fixture.EXPECT_EQ(2.0, executor.cycleStates[0].tickValues[1], "Cycle 0 edge 1 should be unchanged")
		fixture.EXPECT_EQ(5.0, executor.cycleStates[1].tickValues[0], "Cycle 1 edge 0 should be updated")
		fixture.EXPECT_EQ(5.0, executor.cycleStates[2].tickValues[0], "Cycle 2 edge 0 should be updated")
	})
}

// FIXED: Example of corrected benchmark
func BenchmarkProcessTickUpdateFixed(b *testing.B) {
	// FIXED: Use proper test builder for benchmarks
	executor := NewTestExecutorBuilder().
		WithPoolSize(10000).
		WithQueues(10).
		WithCyclesPerQueue(10).
		Build()

	defer NewTestCleaner(executor).Cleanup()

	updates := make([]*TickUpdate, 10)
	for i := range updates {
		updates[i] = &TickUpdate{
			pairID:      PairID(i + 1),
			forwardTick: float64(i) * 0.1,
			reverseTick: -float64(i) * 0.1,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processTickUpdate(executor, updates[i%len(updates)])
	}
}