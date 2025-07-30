// ════════════════════════════════════════════════════════════════════════════════════════════════
// Multi-Core Arbitrage Opportunity Aggregator
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: Opportunity Aggregator & Bundle Extractor
//
// Description:
//   Lock-free dual-layer priority sorting system using hash table deduplication and zero-allocation
//   queue operations. Aggregates profitable opportunities across router cores for optimal execution.
//
// Features:
//   - Multi-core opportunity collection with lock-free coordination
//   - Hash table deduplication using deterministic cycle hashing
//   - Dual-layer priority sorting by liquidity and profitability
//   - Adaptive block finalization with core count scaling
//   - Bit shift optimizations for all hot paths
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package aggregator

import (
	"runtime"
	"time"
	"unsafe"

	"main/compactqueue128"
	"main/constants"
	"main/ring56"
	"main/types"
	"main/utils"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// AGGREGATOR STATE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// AggregatorState encapsulates core system state for arbitrage opportunity aggregation.
// Memory layout optimized for cache efficiency with fields ordered by access frequency
// in the primary processing loop. Only persistent state remains in struct.
//
// Access Pattern Optimization:
//   - Hot path data (coreRings) in first cache line
//   - Opportunity processing data (occupied, opportunities) grouped together
//   - Queue management data (queues, stratumTracker) grouped together
//   - Shared memory arena isolated for queue operations
//
//go:notinheap
//go:align 64
type AggregatorState struct {
	// ═══════════════════════════════════════════════════════════════════════════════════════════════
	// CACHE LINE 1: HOT PATH - ACCESSED EVERY ITERATION
	// ═══════════════════════════════════════════════════════════════════════════════════════════════

	// Primary communication interface accessed every processing iteration.
	// Ring buffers provide lock-free message passing between router cores and aggregator.
	// Frequency: Accessed once per core per iteration regardless of data availability.
	coreRings [constants.AggregatorMaxSupportedCores]ring56.Ring

	// ═══════════════════════════════════════════════════════════════════════════════════════════════
	// CACHE LINE 2: OPPORTUNITY PROCESSING - ACCESSED WHEN DATA AVAILABLE
	// ═══════════════════════════════════════════════════════════════════════════════════════════════

	// Hash table occupancy tracking for efficient slot management and bulk operations.
	// Bitmap enables rapid occupancy testing and provides efficient bulk reset capability
	// during block finalization. Each bit represents one hash table slot.
	// Frequency: Accessed twice per opportunity (read for collision detection, write for marking).
	occupied [constants.AggregatorBitmapWords]uint64

	// Primary opportunity storage indexed by cycle hash for deduplication.
	// Hash table stores unique arbitrage cycles with collision handling via replacement.
	// Replacement strategy prioritizes most recent opportunity data for execution relevance.
	// Frequency: Accessed once per opportunity for storage and retrieval operations.
	opportunities [constants.AggregatorTableCapacity]types.ArbitrageOpportunity

	// ═══════════════════════════════════════════════════════════════════════════════════════════════
	// CACHE LINE 3: QUEUE MANAGEMENT - ACCESSED PER OPPORTUNITY AND EXTRACTION
	// ═══════════════════════════════════════════════════════════════════════════════════════════════

	// Liquidity-stratified priority queues for dual-layer opportunity ordering.
	// Each queue contains opportunities within a specific liquidity tier, ordered by profitability.
	// Queue selection by leading zero count ensures execution risk assessment precedence.
	// Frequency: Accessed once per opportunity for queue selection and priority operations.
	queues [constants.AggregatorMaxStrata]compactqueue128.CompactQueue128

	// Meta-queue tracking active liquidity strata for efficient bundle extraction.
	// Maintains sorted list of strata containing opportunities, enabling rapid identification
	// of highest-liquidity tiers during execution bundle construction.
	// Frequency: Accessed during new opportunity insertion and bundle extraction phases.
	stratumTracker compactqueue128.CompactQueue128

	// ═══════════════════════════════════════════════════════════════════════════════════════════════
	// CACHE LINE 4: SHARED MEMORY ARENA - ACCESSED DURING QUEUE OPERATIONS
	// ═══════════════════════════════════════════════════════════════════════════════════════════════

	// Shared memory arena for all priority queue operations and data storage.
	// All CompactQueue128 instances allocate entries from this unified memory pool
	// to ensure cache locality and minimize memory fragmentation during operation.
	// Frequency: Accessed during every queue operation (Push, Pop, MoveTick).
	sharedArena [constants.AggregatorTableCapacity]compactqueue128.Entry

	// ═══════════════════════════════════════════════════════════════════════════════════════════════
	// CACHE LINE 5: SYNCHRONIZATION - ACCESSED DURING PHASE TRANSITIONS
	// ═══════════════════════════════════════════════════════════════════════════════════════════════

	// Initialization coordination channel for two-phase system startup.
	// Enables coordinated transition between initialization and operational
	// phases across multiple concurrent goroutines without race conditions.
	// Cold - only used during initialization coordination
	gcComplete chan struct{} // 8B - Channel used for two-stage initialization coordination
	_          [56]byte      // 56B - Padding to reach cache line boundary (8 + 56 = 64)

	// ═══════════════════════════════════════════════════════════════════════════════════════════════
	// CACHE LINE 6: COLD PATH - INFREQUENT ACCESS
	// ═══════════════════════════════════════════════════════════════════════════════════════════════

	// Sequential handle allocator for queue entry management.
	// Provides unique identifiers for queue entries to enable efficient priority updates
	// and entry lifecycle management across queue operations.
	// Frequency: Accessed only during new stratum activation (subset of new opportunities).
	nextHandle compactqueue128.Handle
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL STATE AND FUNCTION-LOCAL VARIABLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Primary aggregator instance with optimized memory layout and cache alignment.
// Single global instance eliminates pointer indirection and enables direct
// memory access in performance-critical code paths.
//
//go:notinheap
//go:align 64
var Aggregator AggregatorState

// Initialize the aggregator state during package initialization
func init() {
	Aggregator.gcComplete = make(chan struct{})
}

// SpinUntilAllCoreRingsPopulated spins hot until all core rings are initialized.
// Blocks until all expected rings are ready for message dispatch.
func SpinUntilAllCoreRingsPopulated(expectedCoreCount int) [constants.AggregatorMaxSupportedCores]*ring56.Ring {
	for {
		i := 0

		// Check all expected cores for ring initialization
		for ; i < expectedCoreCount; i++ {
			if !Aggregator.coreRings[i].IsReady() {
				break
			}
		}

		if i == expectedCoreCount {
			break
		}

		// Hot spin - no yielding for minimum latency coordination
	}

	// Build and return pointers to all rings
	var ringPointers [constants.AggregatorMaxSupportedCores]*ring56.Ring
	for i := 0; i < constants.AggregatorMaxSupportedCores; i++ {
		ringPointers[i] = &Aggregator.coreRings[i]
	}
	return ringPointers
}

// processOpportunity ingests and prioritizes arbitrage opportunities using hash deduplication.
func (agg *AggregatorState) processOpportunity(opp *types.ArbitrageOpportunity) {
	// Compute cycle hash: XOR for order-independence, Mix64 for distribution with sequential IDs
	h := uint16(utils.Mix64(uint64(opp.CyclePairs[0])^
		uint64(opp.CyclePairs[1])^
		uint64(opp.CyclePairs[2])) & constants.AggregatorTableMask)

	// Calculate bitmap position for occupancy tracking
	w := h >> 6
	b := h & 63
	mask := uint64(1) << b
	wasOccupied := (agg.occupied[w] & mask) >> b

	// Update hash table with current opportunity data
	agg.opportunities[h] = *opp
	agg.occupied[w] |= mask

	// Transform profitability to priority queue tick
	tick := int64((opp.TotalProfitability + constants.AggregatorProfitabilityClampingBound) * constants.AggregatorProfitabilityScale)
	queue := &agg.queues[opp.LeadingZeroCount]
	handle := compactqueue128.Handle(h)

	if wasOccupied != 0 {
		// Update existing opportunity priority
		queue.MoveTick(handle, tick)
	} else {
		// Insert new opportunity and track stratum activity
		queue.Push(tick, handle, uint64(h))

		size := uint64(queue.Size())
		isFirst := (uint64(int64(size-1)) >> 63) & 1

		nextHandle := agg.nextHandle
		agg.nextHandle += compactqueue128.Handle(isFirst)
		trackerHandle := nextHandle
		stratumTick := int64(opp.LeadingZeroCount) * int64(isFirst)
		stratumData := opp.LeadingZeroCount * isFirst
		agg.stratumTracker.Push(stratumTick, trackerHandle, stratumData)
	}
}

// extractOpportunityBundle constructs execution bundles from highest-priority opportunities.
// Extracts from highest-liquidity strata first, then by profitability within each stratum.
func (agg *AggregatorState) extractOpportunityBundle() {
	for bundleCount := 0; bundleCount < constants.AggregatorMaxBundleSize; bundleCount++ {
		if agg.stratumTracker.Empty() {
			break
		}

		_, _, stratumID := agg.stratumTracker.PeepMin()
		queue := &agg.queues[stratumID]

		if queue.Empty() {
			th, _, _ := agg.stratumTracker.PeepMin()
			agg.stratumTracker.UnlinkMin(th)
			continue
		}

		oh, _, _ := queue.PeepMin()
		opportunity := &agg.opportunities[oh]

		// TODO: Send opportunity to execution system
		_ = opportunity

		queue.UnlinkMin(oh)

		if queue.Empty() {
			th, _, _ := agg.stratumTracker.PeepMin()
			agg.stratumTracker.UnlinkMin(th)
		}
	}
}

// reset clears all aggregation state for next processing cycle using bulk memory operations.
func (agg *AggregatorState) reset() {
	// Clear occupancy bitmap
	for i := 0; i < constants.AggregatorBitmapWords; i++ {
		agg.occupied[i] = 0
	}

	// Clear opportunity storage
	oppPtr := (*[constants.AggregatorTableCapacity * 7]uint64)(unsafe.Pointer(&agg.opportunities[0]))
	for i := 0; i < constants.AggregatorTableCapacity*7; i++ {
		oppPtr[i] = 0
	}

	// Clear shared memory arena
	arenaPtr := (*[constants.AggregatorTableCapacity * 4]uint64)(unsafe.Pointer(&agg.sharedArena[0]))
	for i := 0; i < constants.AggregatorTableCapacity*4; i++ {
		arenaPtr[i] = 0
	}

	// Clear priority queues (preserve arena field at index 2)
	for i := 0; i < constants.AggregatorMaxStrata; i++ {
		qPtr := (*[144]uint64)(unsafe.Pointer(&agg.queues[i]))
		// Clear fields 0-1 (summary, size)
		qPtr[0] = 0
		qPtr[1] = 0
		// Skip index 2 (arena field)
		// Clear remaining fields 3-143
		for j := 3; j < 144; j++ {
			qPtr[j] = 0
		}
	}

	// Clear meta-queue
	tPtr := (*[144]uint64)(unsafe.Pointer(&agg.stratumTracker))
	for i := 0; i < 144; i++ {
		tPtr[i] = 0
	}
}

// InitializeAggregatorSystem orchestrates complete system initialization with empirical
// performance calibration and multi-phase startup coordination.
func InitializeAggregatorSystem() {
	// Determine optimal core allocation
	coreCount := runtime.NumCPU() - 4
	if coreCount > constants.AggregatorMaxSupportedCores {
		coreCount = constants.AggregatorMaxSupportedCores
	}
	coreCount &^= 1

	// Empirical performance measurement for timing calibration
	benchmarkRings := make([]ring56.Ring, coreCount)
	for i := 0; i < coreCount; i++ {
		benchmarkRings[i] = *ring56.New(constants.RouterDefaultRingSize)
	}

	const benchmarkIterations = 1000000
	start := time.Now()

	for iteration := 0; iteration < benchmarkIterations; iteration++ {
		dataCount := uint64(0)
		for i := 0; i < coreCount; i++ {
			p := benchmarkRings[i].Pop()
			hasData := uint64(uintptr(unsafe.Pointer(p))>>63) ^ 1
			dataCount += hasData
		}
	}

	elapsed := time.Since(start)
	iterationsPerMs := benchmarkIterations / int(elapsed/constants.AggregatorTargetFinalizationTime)
	finalizationThreshold := uint64(iterationsPerMs / coreCount)

	Aggregator.gcComplete = make(chan struct{})

	go func() {
		runtime.LockOSThread()

		// Initialize ring buffers
		for i := 0; i < coreCount; i++ {
			Aggregator.coreRings[i] = *ring56.New(constants.RouterDefaultRingSize)
		}

		// Configure shared memory arena
		arenaPtr := unsafe.Pointer(&Aggregator.sharedArena[0])
		Aggregator.stratumTracker = *compactqueue128.New(arenaPtr)

		// Initialize priority queues
		for i := range Aggregator.queues {
			Aggregator.queues[i] = *compactqueue128.New(arenaPtr)
		}

		Aggregator.nextHandle = 0

	cooperativeDrainSpin:
		for {
			allEmpty := true
			for i := 0; i < coreCount; i++ {
				if Aggregator.coreRings[i].Pop() != nil {
					allEmpty = false
				}
			}

			if allEmpty {
				select {
				case <-Aggregator.gcComplete:
					break cooperativeDrainSpin
				default:
				}
			}
			runtime.Gosched()
		}

		// Primary operational loop
		emptySpins := uint64(0)
		for {
			dataCount := uint64(0)

			for i := 0; i < coreCount; i++ {
				p := Aggregator.coreRings[i].Pop()
				hasData := uint64(uintptr(unsafe.Pointer(p))>>63) ^ 1
				dataCount += hasData

				if hasData != 0 {
					opp := (*types.ArbitrageOpportunity)(unsafe.Pointer(p))
					Aggregator.processOpportunity(opp)
				}
			}

			hasData := (dataCount - 1) >> 63
			emptySpins = (emptySpins + 1) & hasData
			shouldFinalize := ((emptySpins - finalizationThreshold) >> 63) ^ 1

			if shouldFinalize != 0 {
				Aggregator.extractOpportunityBundle()
				Aggregator.reset()
				emptySpins = 0
			}
		}
	}()
}

// SignalGCComplete signals all system components that initialization is complete.
func SignalGCComplete() {
	close(Aggregator.gcComplete)
}
