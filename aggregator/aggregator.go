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
	coreRings [constants.RouterMaxSupportedCores]ring56.Ring

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
	// CACHE LINE 5: COLD PATH - INFREQUENT ACCESS
	// ═══════════════════════════════════════════════════════════════════════════════════════════════

	// Sequential handle allocator for queue entry management.
	// Provides unique identifiers for queue entries to enable efficient priority updates
	// and entry lifecycle management across queue operations.
	// Frequency: Accessed only during new stratum activation (subset of new opportunities).
	nextHandle compactqueue128.Handle

	// Initialization coordination channel for two-phase system startup.
	// Enables coordinated transition between initialization and operational
	// phases across multiple concurrent goroutines without race conditions.
	// Cold - only used during initialization coordination
	gcComplete chan struct{} // 8B - Channel used for two-stage initialization coordination
	_          [48]byte      // 48B - Padding to reach cache line boundary (8 + 8 + 48 = 64)
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

// SpinUntilAllCoreRingsPopulated spins hot until all core rings are initialized and populated.
// This function blocks the router from proceeding until all destination rings are ready.
// Returns pointers to the populated core rings once all entries are non-nil.
//
// Design rationale:
//   - Hot spinning provides minimum latency for startup coordination
//   - Validation ensures no nil rings before router begins message dispatch
//   - Pointer return avoids copying large ring structures
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func SpinUntilAllCoreRingsPopulated(expectedCoreCount int) [constants.RouterMaxSupportedCores]*ring56.Ring {
	for {
		i := 0

		// Check all expected cores for ring readiness
		// Use IsReady() method to check if ring has been properly set up
		// An unready ring will return false, ready rings return true
		for ; i < expectedCoreCount; i++ {
			if !Aggregator.coreRings[i].IsReady() {
				break // Found unready ring, exit inner loop
			}
		}

		// If i == expectedCoreCount, all cores were checked without breaking (all ready)
		// If i < expectedCoreCount, we broke early (found unready ring)
		if i == expectedCoreCount {
			break // All rings are ready, exit outer loop
		}

		// Hot spin - no yielding for minimum latency coordination
		// This is startup-only code, so CPU usage is acceptable
	}

	// All cores are populated, build and return pointers to the rings
	var ringPointers [constants.RouterMaxSupportedCores]*ring56.Ring
	for i := 0; i < constants.RouterMaxSupportedCores; i++ {
		ringPointers[i] = &Aggregator.coreRings[i]
	}
	return ringPointers
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE OPPORTUNITY PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processOpportunity implements the primary opportunity ingestion and prioritization logic.
// Handles hash table deduplication, dual-layer queue management, and stratum tracking
// with optimized memory access patterns and branchless arithmetic for consistent performance.
//
// Processing Flow:
//  1. Compute deterministic hash from cycle pairs for deduplication
//  2. Check hash table occupancy and update opportunity storage
//  3. Transform profitability to priority queue tick value
//  4. Execute queue update (existing) or insertion (new) path
//  5. Manage stratum activity tracking for new opportunities
//
// Performance Characteristics:
//   - O(1) hash table operations with collision handling via replacement
//   - O(log n) priority queue operations with shared memory allocation
//   - Branchless arithmetic for predictable execution timing
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func (agg *AggregatorState) processOpportunity(opp *types.ArbitrageOpportunity) {
	// Compute cycle hash using XOR operation for order-independent deduplication.
	// XOR ensures identical cycles produce identical hashes regardless of pair ordering,
	// enabling efficient cycle identification and replacement-based collision handling.
	h := uint16((uint64(opp.CyclePairs[0]) ^
		uint64(opp.CyclePairs[1]) ^
		uint64(opp.CyclePairs[2])) & constants.AggregatorTableMask)

	// Calculate bitmap position using bit manipulation for efficient occupancy tracking.
	// Bitmap organization: 64 slots per uint64 word, requiring word and bit index calculation.
	w := h >> 6                                  // Word index: hash ÷ 64
	b := h & 63                                  // Bit index: hash mod 64
	mask := uint64(1) << b                       // Bit mask for specific slot
	wasOccupied := (agg.occupied[w] & mask) >> b // Extract occupancy status

	// Update hash table with current opportunity data and mark slot as occupied.
	// Replacement strategy ensures most recent opportunity data is preserved
	// for execution relevance and price accuracy.
	agg.opportunities[h] = *opp
	agg.occupied[w] |= mask

	// Transform profitability to priority queue tick using linear scaling.
	// Formula: tick = (profitability + bound) × scale
	// Matches router.go quantization pattern for consistency.
	tick := int64((opp.TotalProfitability + constants.AggregatorProfitabilityClampingBound) * constants.AggregatorProfitabilityScale)
	queue := &agg.queues[opp.LeadingZeroCount]
	handle := compactqueue128.Handle(h)

	if wasOccupied != 0 {
		// Update path: Existing opportunity requires priority adjustment in queue.
		// MoveTick operation maintains queue ordering while updating opportunity priority
		// based on latest profitability calculation from router cores.
		queue.MoveTick(handle, tick)
	} else {
		// Insertion path: New opportunity requires queue insertion and stratum tracking.
		// Queue insertion maintains priority ordering within liquidity tier.
		queue.Push(tick, handle, uint64(h))

		// Stratum activity management using branchless first-opportunity detection.
		// Tracks whether this is the first opportunity in its liquidity stratum
		// for efficient meta-queue management during bundle extraction.
		size := uint64(queue.Size())

		// Branchless first-opportunity detection using arithmetic right shift.
		// Expression: (size-1) >> 63 produces 0 for size≥1, but for size=1:
		// (1-1) = 0, and 0-1 = 0xFFFFFFFFFFFFFFFF (max uint64)
		// Right shift of max uint64 by 63 positions yields 1
		isFirst := (uint64(int64(size-1)) >> 63) & 1

		// Handle allocation and stratum tracking with conditional execution.
		// Operations scaled by isFirst flag to execute only for first opportunities.
		nextHandle := agg.nextHandle
		agg.nextHandle += compactqueue128.Handle(isFirst)
		trackerHandle := nextHandle
		stratumTick := int64(opp.LeadingZeroCount) * int64(isFirst)
		stratumData := opp.LeadingZeroCount * isFirst
		agg.stratumTracker.Push(stratumTick, trackerHandle, stratumData)
	}
}

// extractOpportunityBundle constructs execution bundles from highest-priority opportunities.
// Implements liquidity-first extraction strategy: selects highest-liquidity strata first,
// then extracts most profitable opportunities within each stratum until bundle capacity reached.
//
// Extraction Strategy:
//  1. Identify active liquidity strata via meta-queue traversal
//  2. Extract most profitable opportunity from highest-liquidity stratum
//  3. Remove empty strata from tracking to maintain extraction efficiency
//  4. Repeat until bundle capacity reached or no opportunities remain
//
// Bundle Composition Optimization:
//   - Prioritizes execution certainty through liquidity-first selection
//   - Maximizes profitability within liquidity constraints
//   - Maintains bundle size limits for gas efficiency and inclusion probability
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func (agg *AggregatorState) extractOpportunityBundle() {
	for bundleCount := 0; bundleCount < constants.AggregatorMaxBundleSize; bundleCount++ {
		// Terminate extraction when no active strata remain.
		// Empty meta-queue indicates all opportunities have been extracted
		// or no profitable opportunities are currently available.
		if agg.stratumTracker.Empty() {
			break
		}

		// Identify highest-liquidity stratum (lowest leading zero count).
		// Meta-queue maintains strata sorted by liquidity tier for optimal
		// execution risk management and slippage minimization.
		_, _, stratumID := agg.stratumTracker.PeepMin()
		queue := &agg.queues[stratumID]

		// Handle empty stratum cleanup during extraction process.
		// Empty strata can occur due to opportunity expiration or previous extraction,
		// requiring removal from meta-queue to maintain extraction efficiency.
		if queue.Empty() {
			th, _, _ := agg.stratumTracker.PeepMin()
			agg.stratumTracker.UnlinkMin(th)
			continue
		}

		// Extract most profitable opportunity from current liquidity stratum.
		// Opportunity data retrieved from hash table using queue handle for
		// integration into execution bundle construction process.
		oh, _, _ := queue.PeepMin()
		// Integration point: agg.opportunities[oh] contains opportunity data for execution
		opportunity := &agg.opportunities[oh]

		// TODO: Send opportunity to execution system
		// Format: types.ArbitrageOpportunity with cycle pairs, profitability, and liquidity data
		_ = opportunity // Placeholder to prevent unused variable error

		queue.UnlinkMin(oh)

		// Remove stratum from meta-queue tracking when depleted.
		// Cleanup maintains meta-queue accuracy and prevents unnecessary
		// iteration over empty strata in subsequent extraction cycles.
		if queue.Empty() {
			th, _, _ := agg.stratumTracker.PeepMin()
			agg.stratumTracker.UnlinkMin(th)
		}
	}
}

// reset performs comprehensive system state cleanup for next processing cycle.
// Implements optimized bulk memory operations to clear all aggregation state
// efficiently while maintaining cache-friendly access patterns for subsequent operations.
//
// Reset Operations:
//  1. Occupancy bitmap bulk clearing using direct iteration
//  2. Opportunity storage bulk zeroing using unsafe pointer arithmetic
//  3. Priority queue state clearing via memory layout exploitation
//  4. Meta-queue state clearing using identical memory operations
//
// Performance Optimization:
//   - Bulk memory operations minimize system call overhead
//   - Sequential access patterns optimize cache utilization
//   - Unsafe operations eliminate bounds checking for maximum throughput
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func (agg *AggregatorState) reset() {
	// Clear occupancy bitmap using direct iteration for optimal cache utilization.
	// Linear access pattern ensures efficient cache line utilization and
	// predictable memory bandwidth consumption during bulk clearing operations.
	for i := 0; i < constants.AggregatorBitmapWords; i++ {
		agg.occupied[i] = 0
	}

	// Clear opportunity storage using unsafe bulk memory operations.
	// Memory layout exploitation: ArbitrageOpportunity = 56 bytes = 7 uint64 words.
	// Bulk clearing via uint64 array access eliminates per-struct overhead
	// and maximizes memory bandwidth utilization for clearing operations.
	oppPtr := (*[constants.AggregatorTableCapacity * 7]uint64)(unsafe.Pointer(&agg.opportunities[0]))
	for i := 0; i < constants.AggregatorTableCapacity*7; i++ {
		oppPtr[i] = 0
	}

	// Clear shared memory arena used by all priority queues.
	// Arena reset ensures clean entry allocation state for subsequent processing cycles.
	// Entry structure size: 32 bytes = 4 uint64 words.
	arenaPtr := (*[constants.AggregatorTableCapacity * 4]uint64)(unsafe.Pointer(&agg.sharedArena[0]))
	for i := 0; i < constants.AggregatorTableCapacity*4; i++ {
		arenaPtr[i] = 0
	}

	// Clear priority queue state using memory layout exploitation.
	// CompactQueue128 structure size: 1,152 bytes = 144 uint64 words.
	// Bulk clearing maintains queue initialization state for subsequent
	// operation without requiring complex reinitialization procedures.
	for i := 0; i < constants.AggregatorMaxStrata; i++ {
		qPtr := (*[144]uint64)(unsafe.Pointer(&agg.queues[i]))
		for j := 0; j < 144; j++ {
			qPtr[j] = 0
		}
	}

	// Clear meta-queue state using identical memory layout operations.
	// Consistent clearing approach across all queue structures ensures
	// uniform performance characteristics and predictable reset timing.
	tPtr := (*[144]uint64)(unsafe.Pointer(&agg.stratumTracker))
	for i := 0; i < 144; i++ {
		tPtr[i] = 0
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION AND LIFECYCLE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// InitializeAggregatorSystem orchestrates complete system initialization with empirical
// performance calibration and multi-phase startup coordination. Implements adaptive
// configuration based on hardware capabilities and core count optimization.
//
// Initialization Phases:
//  1. Hardware capability assessment and core count determination
//  2. Empirical performance measurement for timing calibration
//  3. Memory structure initialization with shared arena allocation
//  4. Cooperative initialization coordination with router systems
//  5. Transition to operational processing with optimized hot loops
//
// Performance Calibration:
//   - Runtime measurement of actual loop performance for timing accuracy
//   - Adaptive threshold calculation based on empirical hardware characteristics
//   - Core count scaling for consistent timing across diverse configurations
func InitializeAggregatorSystem() {
	// Determine optimal core allocation using router allocation strategy.
	// Core count calculation reserves system cores for OS operations while
	// maximizing utilization for arbitrage processing workloads.
	coreCount := runtime.NumCPU() - 4
	if coreCount > constants.RouterMaxSupportedCores {
		coreCount = constants.RouterMaxSupportedCores
	}
	coreCount &^= 1 // Ensure even count for forward/reverse core pairing

	// Empirical performance measurement for adaptive timing calibration.
	// Benchmark measures actual main loop performance under realistic conditions
	// to calculate accurate finalization thresholds for consistent timing.
	benchmarkRings := make([]ring56.Ring, coreCount)
	for i := 0; i < coreCount; i++ {
		benchmarkRings[i] = *ring56.New(constants.RouterDefaultRingSize)
	}

	// Execute 1,000,000 iterations of exact polling loop for timing measurement.
	// Benchmark runs for at least 1ms to establish bare minimum workload baseline
	// that represents typical empty-spin conditions during low activity periods.
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

	// Calculate adaptive finalization threshold for consistent timing.
	// Threshold calculation ensures AggregatorTargetFinalizationTime timing regardless
	// of hardware performance variations or core count differences.
	iterationsPerMs := benchmarkIterations / int(elapsed/constants.AggregatorTargetFinalizationTime)
	finalizationThreshold := uint64(iterationsPerMs / coreCount)

	// Initialize global coordination channel for multi-phase startup synchronization.
	// Channel enables coordinated transition between initialization and operational
	// phases across multiple concurrent goroutines without race conditions.
	Aggregator.gcComplete = make(chan struct{})

	go func() {
		// Establish OS thread affinity for consistent NUMA performance characteristics.
		// Thread locking eliminates scheduler-induced performance variability and
		// ensures predictable memory access patterns for cache optimization.
		runtime.LockOSThread()

		// Initialize inter-core communication infrastructure with optimized ring buffers.
		// Ring buffer configuration matches router core expectations for message
		// size and capacity to ensure efficient lock-free communication.
		for i := 0; i < coreCount; i++ {
			Aggregator.coreRings[i] = *ring56.New(constants.RouterDefaultRingSize)
		}

		// Configure shared memory arena for unified queue memory management.
		// Shared arena eliminates memory fragmentation and ensures cache locality
		// across all priority queue operations for optimal performance.
		arenaPtr := unsafe.Pointer(&Aggregator.sharedArena[0])
		Aggregator.stratumTracker = *compactqueue128.New(arenaPtr)

		// Initialize liquidity-stratified priority queue system.
		// Queue initialization establishes memory layout for efficient
		// dual-layer priority sorting and bundle extraction operations.
		for i := range Aggregator.queues {
			Aggregator.queues[i] = *compactqueue128.New(arenaPtr)
		}

		// Reset counter for handle allocation
		Aggregator.nextHandle = 0

		// Cooperative initialization coordination with systematic state draining.
		// Coordination phase ensures all system components reach operational
		// readiness before processing activation to prevent race conditions.
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

		// Primary operational loop with optimized opportunity processing.
		// Loop implements adaptive block finalization, efficient opportunity ingestion,
		// and automatic state management for continuous high-frequency operation.
		// All finalization-related variables are now local to this function.
		emptySpins := uint64(0)
		for {
			dataCount := uint64(0)

			// Poll all router core communication channels for opportunity data.
			// Polling strategy balances latency minimization with CPU efficiency
			// through direct ring buffer access without blocking operations.
			for i := 0; i < coreCount; i++ {
				p := Aggregator.coreRings[i].Pop()
				hasData := uint64(uintptr(unsafe.Pointer(p))>>63) ^ 1
				dataCount += hasData

				if hasData != 0 {
					opp := (*types.ArbitrageOpportunity)(unsafe.Pointer(p))
					Aggregator.processOpportunity(opp)
				}
			}

			// Adaptive block finalization using branchless empty iteration counting.
			// Finalization detection enables timely opportunity extraction while
			// preventing premature finalization during active trading periods.
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

// SignalGCComplete notifies all system components of initialization completion.
// Signal coordination enables synchronized transition to operational processing
// across all concurrent goroutines participating in system initialization.
//
// Coordination Protocol:
//   - Called after all initialization operations complete successfully
//   - Triggers transition from cooperative to high-performance processing modes
//   - Ensures consistent system state before operational activation
func SignalGCComplete() {
	close(Aggregator.gcComplete)
}
