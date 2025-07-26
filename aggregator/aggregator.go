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
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

const (
	TableCapacity = 1 << 16           // 65536 hash table slots
	TableMask     = TableCapacity - 1 // 65535 mask for hash indexing
	MaxStrata     = 96                // Liquidity strata count (hex leading zeros 0-95)
	BitmapWords   = 1 << 10           // 1024 uint64 words for occupancy bitmap
	MaxBundleSize = 32                // Maximum opportunities per execution bundle
)

type TradingPairID uint64

// ArbitrageOpportunity represents a profitable three-pair arbitrage cycle.
// Exactly 56 bytes for ring56 compatibility with cache-optimized field ordering.
//
//go:notinheap
type ArbitrageOpportunity struct {
	totalProfitability float64          // 8B - Logarithmic profit calculation for the complete cycle
	leadingZeroCount   uint64           // 8B - Liquidity assessment via hex leading zeros
	cyclePairs         [3]TradingPairID // 24B - Three trading pairs forming arbitrage cycle
	isReverseDirection bool             // 1B - Execution direction flag for optimal trading path
	_                  [15]byte         // 15B - Padding to reach exactly 56 bytes for ring56
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// AGGREGATOR STATE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// AggregatorState manages all global state for the arbitrage opportunity aggregation system.
// Fields ordered by access frequency in the hot path with cache line optimization.
// Each cache line group represents related data accessed together during processing.
//
//go:notinheap
//go:align 64
type AggregatorState struct {
	// CACHE LINE GROUP 1: Core communication (accessed every iteration)
	coreRings [constants.MaxSupportedCores]ring56.Ring

	// CACHE LINE GROUP 2: Queue memory arena (accessed during all queue operations)
	sharedArena [TableCapacity]compactqueue128.Entry

	// CACHE LINE GROUP 3: Hash table occupancy tracking (accessed with every opportunity)
	occupied [BitmapWords]uint64

	// CACHE LINE GROUP 4: Opportunity storage (accessed with every opportunity)
	opportunities [TableCapacity]ArbitrageOpportunity

	// CACHE LINE GROUP 5: Priority queue system (accessed with every opportunity)
	queues [MaxStrata]compactqueue128.CompactQueue128

	// CACHE LINE GROUP 6: Stratum management (accessed for new opportunities)
	stratumTracker compactqueue128.CompactQueue128
	nextHandle     uint64

	// COLD: Initialization synchronization only
	gcComplete chan struct{}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL STATE VARIABLES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Global aggregator instance with cache line isolation and access frequency optimization.
// Single point of truth for all aggregation state to prevent data fragmentation.
//
//go:notinheap
//go:align 64
var Aggregator AggregatorState

// Adaptive finalization threshold calculated at runtime based on core count.
// Maintains consistent ~1ms timing across different hardware configurations.
var FinalizationThreshold uint64

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE PROCESSING PIPELINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processOpportunity handles incoming arbitrage opportunities with hash table deduplication
// and dual-layer priority queue management for optimal execution ordering.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func (agg *AggregatorState) processOpportunity(opp *ArbitrageOpportunity) {
	// Compute deterministic hash from cycle pairs for deduplication
	h := uint16((uint64(opp.cyclePairs[0]) ^
		uint64(opp.cyclePairs[1]) ^
		uint64(opp.cyclePairs[2])) & TableMask)

	// Calculate bitmap position and create bit mask for occupancy tracking
	w := h >> 6
	b := h & 63
	mask := uint64(1) << b
	wasOccupied := (agg.occupied[w] & mask) >> b

	// Store opportunity in hash table and mark slot as occupied
	agg.opportunities[h] = *opp
	agg.occupied[w] |= mask

	// Transform profitability to priority queue tick value for ordering
	tick := int64((opp.totalProfitability + 192.0) * 126.0 / 384.0)
	queue := &agg.queues[opp.leadingZeroCount]
	handle := compactqueue128.Handle(h)

	if wasOccupied != 0 {
		// UPDATE PATH: Existing opportunity, adjust priority in queue
		queue.MoveTick(handle, tick)
	} else {
		// INSERT PATH: New opportunity, add to queue and track stratum activity
		queue.Push(tick, handle, uint64(h))

		// Stratum activity tracking with branchless first-opportunity detection
		size := uint64(queue.Size())
		isFirst := (uint64(int64(size-1)) >> 63) & 1
		trackerHandle := compactqueue128.Handle(agg.nextHandle)
		agg.nextHandle += isFirst
		stratumTick := int64(opp.leadingZeroCount) * int64(isFirst)
		stratumData := opp.leadingZeroCount * isFirst
		agg.stratumTracker.Push(stratumTick, trackerHandle, stratumData)
	}
}

// extractOpportunityBundle creates execution bundles from the most profitable opportunities
// across all liquidity strata with preference for higher liquidity execution.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func (agg *AggregatorState) extractOpportunityBundle() {
	for bundleCount := 0; bundleCount < MaxBundleSize; bundleCount++ {
		if agg.stratumTracker.Empty() {
			break
		}

		// Extract best liquidity stratum (lowest leading zero count)
		_, _, stratumID := agg.stratumTracker.PeepMin()
		queue := &agg.queues[stratumID]

		if queue.Empty() {
			// Remove empty stratum from tracker and continue
			th, _, _ := agg.stratumTracker.PeepMin()
			agg.stratumTracker.UnlinkMin(th)
			continue
		}

		// Extract most profitable opportunity from this stratum
		oh, _, _ := queue.PeepMin()
		// TODO: Add agg.opportunities[oh] to execution bundle
		queue.UnlinkMin(oh)

		// Clean up empty stratum from tracker
		if queue.Empty() {
			th, _, _ := agg.stratumTracker.PeepMin()
			agg.stratumTracker.UnlinkMin(th)
		}
	}
}

// reset clears all aggregation state for the next processing block using
// optimized bulk memory operations for maximum performance.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
func (agg *AggregatorState) reset() {
	agg.nextHandle = 0

	// Clear occupancy bitmap using direct iteration
	for i := 0; i < BitmapWords; i++ {
		agg.occupied[i] = 0
	}

	// Clear opportunities array using unsafe bulk memory operations
	oppPtr := (*[TableCapacity * 7]uint64)(unsafe.Pointer(&agg.opportunities[0]))
	for i := 0; i < TableCapacity*7; i++ {
		oppPtr[i] = 0
	}

	// Clear priority queues using bulk memory zeroing
	for i := 0; i < MaxStrata; i++ {
		qPtr := (*[24]uint64)(unsafe.Pointer(&agg.queues[i]))
		for j := 0; j < 24; j++ {
			qPtr[j] = 0
		}
	}

	// Clear stratum tracker using bulk memory operations
	tPtr := (*[24]uint64)(unsafe.Pointer(&agg.stratumTracker))
	for i := 0; i < 24; i++ {
		tPtr[i] = 0
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SYSTEM INITIALIZATION AND COORDINATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// InitializeAggregatorSystem orchestrates complete system bootstrap with adaptive configuration
// based on core count and two-phase initialization for optimal memory layout.
func InitializeAggregatorSystem() {
	// Determine optimal number of CPU cores using same algorithm as router
	coreCount := runtime.NumCPU() - 4
	if coreCount > constants.MaxSupportedCores {
		coreCount = constants.MaxSupportedCores
	}
	coreCount &^= 1 // Ensure even number for paired forward/reverse processing

	// Benchmark actual loop performance to calculate accurate finalization threshold
	benchmarkRings := make([]ring56.Ring, coreCount)
	for i := 0; i < coreCount; i++ {
		benchmarkRings[i] = *ring56.New(constants.DefaultRingSize)
	}

	// Measure 1,000,000 iterations of the exact polling loop
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

	// Calculate threshold for 1ms timing: iterations_per_ms / cores
	// elapsed.Milliseconds() gives total ms, benchmarkIterations/elapsed_ms gives iterations/ms
	iterationsPerMs := benchmarkIterations / int(elapsed.Milliseconds())
	FinalizationThreshold = uint64(iterationsPerMs / coreCount)
	if FinalizationThreshold < 100 {
		FinalizationThreshold = 100 // Minimum safety threshold
	}

	Aggregator.gcComplete = make(chan struct{})

	go func() {
		// Lock to OS thread for consistent NUMA performance characteristics
		runtime.LockOSThread()

		// Initialize ring buffers for inter-core communication
		for i := 0; i < coreCount; i++ {
			Aggregator.coreRings[i] = *ring56.New(constants.DefaultRingSize)
		}

		// Set up shared memory arena for all priority queue operations
		arenaPtr := unsafe.Pointer(&Aggregator.sharedArena[0])
		Aggregator.stratumTracker = *compactqueue128.New(arenaPtr)

		// Initialize all liquidity stratum priority queues
		for i := range Aggregator.queues {
			Aggregator.queues[i] = *compactqueue128.New(arenaPtr)
		}

		// COOPERATIVE INITIALIZATION DRAIN PHASE
		// Continue processing until GC operations complete and all rings are empty
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

		// MAIN EVENT PROCESSING LOOP
		// Process opportunities and detect block finalization using adaptive timing
		emptySpins := uint64(0)
		for {
			dataCount := uint64(0)

			// Poll all router core ring buffers for new opportunities
			for i := 0; i < coreCount; i++ {
				p := Aggregator.coreRings[i].Pop()
				hasData := uint64(uintptr(unsafe.Pointer(p))>>63) ^ 1
				dataCount += hasData

				if hasData != 0 {
					opp := (*ArbitrageOpportunity)(unsafe.Pointer(p))
					Aggregator.processOpportunity(opp)
				}
			}

			// Block finalization detection using branchless empty spin counting
			hasData := (dataCount - 1) >> 63
			emptySpins = (emptySpins + 1) & hasData
			shouldFinalize := ((emptySpins - FinalizationThreshold) >> 63) ^ 1

			if shouldFinalize != 0 {
				Aggregator.extractOpportunityBundle()
				Aggregator.reset()
				emptySpins = 0
			}
		}
	}()
}

// signalGCComplete signals all worker processes that garbage collection and
// memory optimization operations have completed and normal processing can begin.
func signalGCComplete() {
	close(Aggregator.gcComplete)
}
