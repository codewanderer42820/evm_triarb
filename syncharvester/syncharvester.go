// ════════════════════════════════════════════════════════════════════════════════════════════════
// Historical Synchronization Harvester
// ────────────────────────────────────────────────────────────────────────────────────────────────
// Project: Arbitrage Detection System
// Component: CSV Reserve Harvester
//
// Description:
//   Historical data extraction system for building complete Uniswap V2 reserve state.
//   Uses optimized parsing with zero-copy operations for high-performance data processing.
//
// Features:
//   - Zero-allocation data processing with pre-allocated buffers
//   - SIMD-optimized hex parsing operations for maximum throughput
//   - Dynamic batch sizing with convergence for optimal performance
//   - Multi-connection parallel harvesting with intelligent load balancing
//   - Bit shift optimizations for all hot paths
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"main/constants"
	"main/debug"
	"main/router"
	"main/types"
	"main/utils"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sugawarayuuta/sonnet"
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CORE DATA STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// EthereumLog represents a single log entry from Ethereum JSON-RPC responses.
// Fields ordered by access frequency during JSON unmarshaling and processing.
//
//go:notinheap
//go:align 64
type EthereumLog struct {
	Data        string   // 16B - Hex-encoded event data payload (accessed first during processing)
	Address     string   // 16B - Contract address for the log event (accessed for pair lookup)
	BlockNumber string   // 16B - Block number in hex format (accessed for deduplication)
	_           [16]byte // 16B - Padding to complete cache line alignment
}

// EthereumLogsResponse encapsulates JSON-RPC log query responses.
// Fields ordered by processing priority: Result array accessed first, Error only on failure.
//
//go:notinheap
//go:align 64
type EthereumLogsResponse struct {
	Result []EthereumLog // 24B - Array of log entries matching query criteria (hot path)
	Error  *struct {     // 8B - Error information from RPC provider (cold path)
		Message string // Error message if request failed
	}
	_ [32]byte // 32B - Padding to complete cache line alignment
}

// EthereumBlockResponse handles block number queries from JSON-RPC.
// Single field structure optimized for quick block number extraction.
//
//go:notinheap
//go:align 64
type EthereumBlockResponse struct {
	Result string   // 16B - Current block number in hex format (only field accessed)
	_      [48]byte // 48B - Padding to complete cache line alignment
}

// ProcessedReserveEntry represents a single reserve state after parsing.
// Fields ordered by processing sequence: address lookup first, then data parsing, then block tracking.
//
//go:notinheap
//go:align 64
type ProcessedReserveEntry struct {
	contractAddress string   // 16B - Uniswap V2 pair contract address (used first for router lookup)
	eventData       string   // 16B - Raw hex data from Sync event (used second for reserve parsing)
	blockHeight     string   // 16B - Block number in hex format (used third for deduplication)
	_               [16]byte // 16B - Padding to complete cache line alignment
}

// SynchronizationHarvester orchestrates historical data extraction.
// Fields organized by access frequency and cache line boundaries for maximum efficiency.
//
//go:notinheap
//go:align 64
type SynchronizationHarvester struct {
	// Cache Line 1: Nuclear hot - accessed during every operation (64B)
	totalEvents int64      // 8B - Atomic counter for processed events (read every batch)
	rpcEndpoint string     // 16B - RPC endpoint URL for HTTP requests (used every request)
	outputFile  *os.File   // 8B - CSV output file handle (written every flush)
	fileMutex   sync.Mutex // 8B - Protects concurrent file writes (locked every flush)
	batchSizes  []uint64   // 24B - Dynamic batch sizes per connection (read every batch)

	// Cache Line 2: Very hot - accessed frequently during processing (64B)
	csvBufferSizes       []int  // 24B - Current buffer sizes for CSV data (updated every record)
	consecutiveSuccesses []int  // 24B - Success counters for batch adaptation (updated every success)
	outputPath           string // 16B - Output file path for this harvester instance (read during init)

	// Cache Line 3: Hot - accessed during batch operations (64B)
	currentBlocks []uint64       // 24B - Current block heights per connection (updated every batch)
	httpClients   []*http.Client // 24B - HTTP clients for parallel requests (used every request)
	_             [16]byte       // 16B - Padding to complete cache line alignment

	// Cache Line 4: Warm - accessed during coordination (64B)
	syncTarget        uint64             // 8B - Target block height for synchronization (read at start/end)
	lastProcessed     uint64             // 8B - Last successfully processed block (read at start)
	startTime         time.Time          // 24B - Processing start timestamp (read for reporting)
	processingContext context.Context    // 8B - Context for cancellation handling (checked periodically)
	cancelFunc        context.CancelFunc // 8B - Function to cancel processing context (called at cleanup)
	_                 [8]byte            // 8B - Padding to complete cache line alignment
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL PROCESSING BUFFERS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Global buffers eliminate allocation overhead during processing operations.
// Ordered by access frequency: response buffers used most, CSV builders least frequently.
//
//go:notinheap
//go:align 64
var (
	// Cache Line 1: Ultra hot - accessed during every HTTP request (64B)
	responseBuffers [][]byte                // 24B - HTTP response buffers (read/written every request)
	processedLogs   []ProcessedReserveEntry // 24B - Log processing buffer (accessed every batch)
	_               [16]byte                // 16B - Padding to complete cache line alignment

	// Cache Line 2: Hot - accessed during CSV operations (64B)
	csvOutputBuffers  [][]byte          // 24B - CSV output buffers for batched writes (written every record)
	csvStringBuilders []strings.Builder // 24B - String builders for zero-allocation CSV construction (used every record)
	_                 [16]byte          // 16B - Padding to complete cache line alignment
)

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// METADATA MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// LoadMetadata reads the last processed block height from binary metadata file.
// Returns deployment block if metadata file is missing or corrupted.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func LoadMetadata() uint64 {
	var buf [8]byte
	file, err := os.Open(constants.HarvesterMetadataPath)
	if err != nil {
		return constants.HarvesterDeploymentBlock
	}
	defer file.Close()

	if n, err := file.Read(buf[:]); err != nil || n != 8 {
		return constants.HarvesterDeploymentBlock
	}

	return utils.Load64(buf[:])
}

// saveMetadata writes the last processed block height to binary metadata file.
// Creates new file if it doesn't exist, overwrites existing content.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func saveMetadata(block uint64) error {
	buf := [8]byte{
		byte(block), byte(block >> 8), byte(block >> 16), byte(block >> 24),
		byte(block >> 32), byte(block >> 40), byte(block >> 48), byte(block >> 56),
	}

	file, err := os.OpenFile(constants.HarvesterMetadataPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(buf[:])
	return err
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HTTP TRANSPORT OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// buildHTTPTransport creates an optimized HTTP transport for maximum throughput operations.
// Configuration fields ordered by performance impact and access frequency.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func buildHTTPTransport() *http.Transport {
	return &http.Transport{
		// Connection establishment (ultra hot - every new connection)
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,  // Connection establishment timeout (checked every dial)
			KeepAlive: 60 * time.Second, // Connection keep-alive duration (affects pool efficiency)
			DualStack: true,             // Enable IPv4/IPv6 fallback support (affects connection success)
		}).DialContext,

		// Connection pooling (very hot - accessed during every request)
		MaxIdleConns:        800, // Global connection pool size (checked on every pool operation)
		MaxIdleConnsPerHost: 150, // Per-host connection limit (checked per host)
		MaxConnsPerHost:     300, // Maximum concurrent connections per host (checked per connection)

		// Timeout configuration (hot - checked during request lifecycle)
		IdleConnTimeout:       120 * time.Second,      // Idle connection timeout (affects pool cleanup)
		TLSHandshakeTimeout:   4 * time.Second,        // TLS negotiation timeout (HTTPS only)
		ResponseHeaderTimeout: 12 * time.Second,       // Response header timeout (checked per request)
		ExpectContinueTimeout: 500 * time.Millisecond, // Continue expectation timeout (HTTP/1.1)

		// Performance flags (warm - affect request processing)
		DisableCompression: true,  // Disable compression for speed (CPU vs bandwidth tradeoff)
		DisableKeepAlives:  false, // Enable connection reuse (essential for performance)
		ForceAttemptHTTP2:  true,  // Use HTTP/2 when available (better multiplexing)

		// Buffer configuration (warm - affect memory allocation patterns)
		WriteBufferSize: 128 * 1024, // Write buffer size (affects memory usage)
		ReadBufferSize:  128 * 1024, // Read buffer size (affects read performance)

		// Environment configuration (cold - read once during initialization)
		Proxy: http.ProxyFromEnvironment, // Environment proxy settings (rarely used)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SIMD-OPTIMIZED HEX PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// parseReservesToZeroTrimmed extracts and trims reserve values from Sync event data.
// Processing variables ordered by usage sequence: dataBytes first, then reserve processing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func parseReservesToZeroTrimmed(eventData string) (string, string) {
	// Convert string to byte slice for SIMD processing efficiency (used immediately)
	dataBytes := unsafe.Slice(unsafe.StringData(eventData), len(eventData))

	// Parse reserve0 from bytes 32-64 (processed first)
	leadingZeros0 := utils.CountHexLeadingZeros(dataBytes[32:64])
	// Optimization: for all-zeros case, keep one zero to avoid "0" string allocation
	if leadingZeros0 == 32 {
		leadingZeros0 = 31 // Keep last zero character
	}
	reserve0Start := 32 + leadingZeros0
	reserve0 := eventData[reserve0Start:64]

	// Parse reserve1 from bytes 96-128 (processed second)
	leadingZeros1 := utils.CountHexLeadingZeros(dataBytes[96:128])
	// Optimization: for all-zeros case, keep one zero to avoid "0" string allocation
	if leadingZeros1 == 32 {
		leadingZeros1 = 31 // Keep last zero character
	}
	reserve1Start := 96 + leadingZeros1
	reserve1 := eventData[reserve1Start:128]

	return reserve0, reserve1
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HARVESTER INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// newSynchronizationHarvester creates a fully initialized harvester with optimized buffers and transport.
// Initialization variables ordered by dependency and usage frequency.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func newSynchronizationHarvester(connectionCount int, outputPath string) *SynchronizationHarvester {
	// Dependency order: context needed first, then metadata for initialization decisions
	ctx, cancel := context.WithCancel(context.Background())
	lastProcessed := LoadMetadata()

	harvester := &SynchronizationHarvester{
		// Cache Line 1: Nuclear hot fields (access frequency order)
		totalEvents: 0,                                                              // 8B - Will be incremented constantly
		rpcEndpoint: "https://" + constants.HarvesterHost + constants.HarvesterPath, // 16B - Used in every HTTP request
		outputFile:  nil,                                                            // 8B - Will be set below, used in every flush
		fileMutex:   sync.Mutex{},                                                   // 8B - Locked during every flush operation
		batchSizes:  make([]uint64, connectionCount),                                // 24B - Read during every batch operation

		// Cache Line 2: Very hot fields (processing frequency order)
		csvBufferSizes:       make([]int, connectionCount), // 24B - Updated with every CSV record
		consecutiveSuccesses: make([]int, connectionCount), // 24B - Updated after every successful batch
		outputPath:           outputPath,                   // 16B - Used during initialization only

		// Cache Line 3: Hot fields (batch operation frequency)
		currentBlocks: make([]uint64, connectionCount),       // 24B - Updated once per batch
		httpClients:   make([]*http.Client, connectionCount), // 24B - Used for every HTTP request

		// Cache Line 4: Warm fields (coordination frequency)
		syncTarget:        0,             // 8B - Set once at start, read at end
		lastProcessed:     lastProcessed, // 8B - Read at start for resumption logic
		startTime:         time.Now(),    // 24B - Set once, read periodically for reporting
		processingContext: ctx,           // 8B - Checked periodically for cancellation
		cancelFunc:        cancel,        // 8B - Called once during cleanup
	}

	// Initialize batch sizes to optimal default values (hot path initialization)
	for i := range harvester.batchSizes {
		harvester.batchSizes[i] = constants.OptimalBatchSize
	}

	// Initialize global buffers in access frequency order
	responseBuffers = make([][]byte, connectionCount)                        // Used every HTTP request
	processedLogs = make([]ProcessedReserveEntry, constants.MaxLogSliceSize) // Used every batch
	csvOutputBuffers = make([][]byte, connectionCount)                       // Used every CSV write
	csvStringBuilders = make([]strings.Builder, connectionCount)             // Used every CSV construction

	// Create shared HTTP transport for connection pooling efficiency
	sharedTransport := buildHTTPTransport()
	for i := 0; i < connectionCount; i++ {
		// Initialize buffers in usage order: response buffer used first, then CSV operations
		responseBuffers[i] = make([]byte, constants.ResponseBufferSize)
		csvOutputBuffers[i] = make([]byte, constants.CSVBufferSize)
		csvStringBuilders[i].Grow(constants.CSVBufferSize)
		harvester.httpClients[i] = &http.Client{
			Timeout:   30 * time.Second, // Checked during every request
			Transport: sharedTransport,  // Used for connection pooling
		}
	}

	// File mode calculation based on resumption state (dependency order)
	var err error
	fileMode := os.O_CREATE | os.O_WRONLY
	if lastProcessed == constants.HarvesterDeploymentBlock {
		fileMode |= os.O_TRUNC // Start fresh
	} else {
		fileMode |= os.O_APPEND // Resume existing file
	}

	harvester.outputFile, err = os.OpenFile(outputPath, fileMode, 0644)
	if err != nil {
		panic(err)
	}

	// Write CSV header for new files only (conditional based on file mode)
	if lastProcessed == constants.HarvesterDeploymentBlock {
		harvester.outputFile.WriteString("address,block,reserve0,reserve1\n")
	}

	// Start background reporting goroutine (least critical initialization)
	go harvester.reportStatistics()
	return harvester
}

// reportStatistics provides periodic progress updates during harvesting operation.
// Local variables ordered by access frequency within the reporting loop.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) reportStatistics() {
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Most frequently accessed: atomic load of total events (every reporting cycle)
			events := atomic.LoadInt64(&harvester.totalEvents)
			debug.DropMessage("HARVEST", utils.Itoa(int(events))+" events processed")

			// Less frequently accessed: iterate through sector progress (per sector per cycle)
			for sectorID := 0; sectorID < len(harvester.currentBlocks); sectorID++ {
				// Racey read of current block height for this sector
				currentBlock := harvester.currentBlocks[sectorID]
				if currentBlock > 0 {
					debug.DropMessage("HARVEST", "Sector "+utils.Itoa(sectorID)+" at block "+utils.Itoa(int(currentBlock)))
				}
			}

		case <-harvester.processingContext.Done():
			return
		}
	}
}

// cleanup ensures all resources are properly released and data is flushed.
// Cleanup operations ordered by criticality: data safety first, then resource cleanup.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) cleanup() {
	// Most critical: ensure all data is written to disk
	harvester.flushAllBuffers()

	// File cleanup (data safety)
	if harvester.outputFile != nil {
		harvester.outputFile.Close()
	}

	// Memory cleanup (resource management, done last)
	responseBuffers = nil
	processedLogs = nil
	csvOutputBuffers = nil
	csvStringBuilders = nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ETHEREUM JSON-RPC OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// getCurrentBlockNumber queries the latest block number from the Ethereum node.
// Local variables ordered by usage: requestJSON used first, then response processing variables.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) getCurrentBlockNumber() uint64 {
	requestJSON := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}` // Used immediately in request
	maxRetries := 3                                                                  // Used in loop condition

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Request creation (dependency order: context→request→header)
		ctx, cancel := context.WithTimeout(harvester.processingContext, 8*time.Second)
		req, err := http.NewRequestWithContext(ctx, "POST", harvester.rpcEndpoint, strings.NewReader(requestJSON))
		if err != nil {
			cancel()
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		// HTTP execution (using first client for block queries)
		response, err := harvester.httpClients[0].Do(req)
		if err != nil {
			cancel()
			time.Sleep(25 * time.Millisecond)
			continue
		}

		// Response processing (order: read→close→cancel to ensure proper resource cleanup)
		bytesRead, _ := response.Body.Read(responseBuffers[0][:512])
		response.Body.Close()
		cancel() // Safe to cancel after response processing complete

		if bytesRead == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// JSON parsing and validation
		var blockResponse EthereumBlockResponse
		err = sonnet.Unmarshal(responseBuffers[0][:bytesRead], &blockResponse)
		if err != nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		// Block number extraction with minimal validation
		if len(blockResponse.Result) > 2 {
			blockNumber := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
			return blockNumber // Block 0 is valid (genesis block)
		}

		time.Sleep(5 * time.Millisecond)
	}

	panic("Failed to get current block number after retries")
}

// extractLogBatch retrieves and processes logs for a specific block range.
// Parameters ordered by access frequency: blocks used first for JSON, connectionID used throughout.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) extractLogBatch(fromBlock, toBlock uint64, connectionID int) (int, error) {
	// Most frequently accessed: string builder for JSON construction
	builder := &csvStringBuilders[connectionID]
	builder.Reset()

	// Block conversion for JSON construction (dependency order)
	fromHex := fmt.Sprintf("%x", fromBlock)
	toHex := fmt.Sprintf("%x", toBlock)

	// Construct JSON-RPC request manually for optimal performance
	builder.WriteString(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x`)
	builder.WriteString(fromHex)
	builder.WriteString(`","toBlock":"0x`)
	builder.WriteString(toHex)
	builder.WriteString(`","topics":["`)
	builder.WriteString(constants.SyncEventSignature)
	builder.WriteString(`"]}],"id":1}`)

	requestJSON := builder.String()

	// Request creation with timeout context
	ctx, cancel := context.WithTimeout(harvester.processingContext, 25*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", harvester.rpcEndpoint, strings.NewReader(requestJSON))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute HTTP request using connection-specific client
	response, err := harvester.httpClients[connectionID].Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()

	// Validate response status
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d", response.StatusCode)
	}

	// Response reading with cache-aligned operations (access frequency order)
	totalBytes := 0
	responseBuffer := responseBuffers[connectionID]   // Most frequently accessed
	maxReadSize := (len(responseBuffer) - 1024) &^ 63 // Calculated once, used in loop

	for totalBytes < maxReadSize {
		// Align read sizes to cache boundaries
		readSize := constants.ReadBufferSize &^ 15
		if totalBytes+readSize > maxReadSize {
			readSize = (maxReadSize - totalBytes) &^ 15
		}

		bytesRead, err := response.Body.Read(responseBuffer[totalBytes : totalBytes+readSize])
		totalBytes += bytesRead

		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return 0, err
		}

		if bytesRead == 0 {
			break
		}
	}

	if totalBytes == 0 {
		return 0, fmt.Errorf("empty response")
	}

	return harvester.parseLogsWithSonnet(responseBuffer[:totalBytes], connectionID)
}

// parseLogsWithSonnet processes JSON-RPC log responses using optimized parsing.
// Variables ordered by computation dependency: counts first, then buffer calculations.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) parseLogsWithSonnet(jsonData []byte, connectionID int) (int, error) {
	// Buffer partitioning calculations (dependency order)
	connectionCount := len(harvester.httpClients)
	bufferOffset := connectionID * (len(processedLogs) / connectionCount)
	maxLogsPerConnection := len(processedLogs) / connectionCount
	logCount := 0

	// Parse JSON response using high-performance sonnet library
	var logsResponse EthereumLogsResponse
	err := sonnet.Unmarshal(jsonData, &logsResponse)
	if err != nil {
		return 0, err
	}

	// Check for RPC errors in response
	if logsResponse.Error != nil {
		return 0, fmt.Errorf("RPC error: %s", logsResponse.Error.Message)
	}

	// Process each log entry with minimal validation for maximum speed
	for _, ethereumLog := range logsResponse.Result {
		if logCount >= maxLogsPerConnection {
			break // Prevent buffer overflow
		}

		bufferPosition := bufferOffset + logCount
		if bufferPosition >= len(processedLogs) {
			break // Additional safety check for buffer bounds
		}

		// Store parsed log data in partitioned global buffer (access frequency order)
		logEntry := &processedLogs[bufferPosition]
		logEntry.contractAddress = ethereumLog.Address[2:] // Remove 0x prefix (accessed first for lookup)
		logEntry.eventData = ethereumLog.Data[2:]          // Remove 0x prefix (accessed second for parsing)
		logEntry.blockHeight = ethereumLog.BlockNumber[2:] // Remove 0x prefix (accessed third for deduplication)

		logCount++
	}

	return logCount, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CSV OUTPUT OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// writeCSVRecord constructs and buffers a CSV record for batch writing.
// Parameters ordered by CSV construction sequence: address first, then block, then reserves.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) writeCSVRecord(address, blockHeight, reserve0, reserve1 string, connectionID int) {
	// Most frequently accessed: string builder for CSV construction
	builder := &csvStringBuilders[connectionID]
	builder.Reset()

	// CSV field construction in output order
	builder.WriteString(address)
	builder.WriteByte(',')
	builder.WriteString(blockHeight)
	builder.WriteByte(',')
	builder.WriteString(reserve0)
	builder.WriteByte(',')
	builder.WriteString(reserve1)
	builder.WriteByte('\n')

	csvRecord := builder.String()

	// Buffer management (access frequency order)
	currentBufferSize := harvester.csvBufferSizes[connectionID] // Read frequently
	newBufferSize := currentBufferSize + len(csvRecord)         // Calculated once

	// Use bit operations for threshold calculation
	threshold := constants.CSVBufferSize - (constants.CSVBufferSize >> 3)
	if newBufferSize >= threshold {
		harvester.flushCSVBuffer(connectionID)
		currentBufferSize = 0
		newBufferSize = len(csvRecord)
	}

	copy(csvOutputBuffers[connectionID][currentBufferSize:], csvRecord)
	harvester.csvBufferSizes[connectionID] = newBufferSize
	atomic.AddInt64(&harvester.totalEvents, 1)
}

// flushCSVBuffer writes accumulated CSV data to disk with mutex protection.
// Early return for empty buffers to avoid unnecessary lock contention.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) flushCSVBuffer(connectionID int) {
	if harvester.csvBufferSizes[connectionID] == 0 {
		return
	}

	harvester.fileMutex.Lock()
	harvester.outputFile.Write(csvOutputBuffers[connectionID][:harvester.csvBufferSizes[connectionID]])
	harvester.fileMutex.Unlock()

	harvester.csvBufferSizes[connectionID] = 0
}

// flushAllBuffers ensures all pending CSV data is written to disk and synced.
// Iterates through all connections to ensure complete data safety.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) flushAllBuffers() {
	for i := 0; i < len(harvester.httpClients); i++ {
		harvester.flushCSVBuffer(i)
	}
	harvester.outputFile.Sync()
}

// processLogFromGlobalBuffer converts parsed log data into CSV format and tracks block progress.
// Processing order: reserve parsing first (most CPU intensive), then CSV writing, then block tracking.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) processLogFromGlobalBuffer(logEntry *ProcessedReserveEntry, connectionID int) {
	// Most CPU-intensive operation first: reserve parsing
	reserve0, reserve1 := parseReservesToZeroTrimmed(logEntry.eventData)

	// CSV writing (I/O operation)
	harvester.writeCSVRecord(logEntry.contractAddress, logEntry.blockHeight, reserve0, reserve1, connectionID)

	// Block tracking (least frequent operation - only when block advances)
	blockNumber := utils.ParseHexU64([]byte(logEntry.blockHeight))
	if blockNumber > harvester.currentBlocks[connectionID] {
		harvester.currentBlocks[connectionID] = blockNumber
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HARVESTING COORDINATION ENGINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// executeHarvesting orchestrates the complete harvesting process with intelligent work distribution.
// Processing variables ordered by calculation dependency and usage frequency.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) executeHarvesting() error {
	harvester.syncTarget = harvester.getCurrentBlockNumber()

	if harvester.lastProcessed >= harvester.syncTarget {
		debug.DropMessage("HARVEST", "Already synced to latest block")
		return nil
	}

	// Work distribution calculations (dependency order)
	totalBlocks := harvester.syncTarget - harvester.lastProcessed // Used for logging and calculations
	connectionCount := len(harvester.httpClients)                 // Used throughout distribution logic
	blocksPerSector := totalBlocks / uint64(connectionCount)      // Base blocks per connection
	extraBlocks := totalBlocks % uint64(connectionCount)          // Remainder blocks to distribute
	sectorStart := harvester.lastProcessed + 1                    // Starting block for distribution

	debug.DropMessage("HARVEST", fmt.Sprintf("Resuming from block %d, %d blocks remaining, %d connections",
		harvester.lastProcessed, totalBlocks, connectionCount))

	// Calculate work distribution across connections with load balancing
	workSectors := make([][2]uint64, connectionCount)
	for i := 0; i < connectionCount; i++ {
		// Sector calculation (dependency order: fromBlock→sectorSize→toBlock→next sectorStart)
		fromBlock := sectorStart
		sectorSize := blocksPerSector
		if uint64(i) < extraBlocks {
			sectorSize++
		}
		toBlock := fromBlock + sectorSize - 1
		if i == connectionCount-1 {
			toBlock = harvester.syncTarget // Ensure last sector reaches target
		}

		workSectors[i] = [2]uint64{fromBlock, toBlock}
		sectorStart = toBlock + 1
	}

	// Start periodic buffer flush goroutine for data safety
	flushTicker := time.NewTicker(2500 * time.Millisecond)
	go func() {
		defer flushTicker.Stop()
		for {
			select {
			case <-flushTicker.C:
				harvester.flushAllBuffers()
			case <-harvester.processingContext.Done():
				return
			}
		}
	}()

	// Launch all worker goroutines with proper synchronization
	var workerWG sync.WaitGroup
	for connectionID := 0; connectionID < connectionCount; connectionID++ {
		workerWG.Add(1)
		go func(id int, sectorRange [2]uint64) {
			defer workerWG.Done()
			harvester.harvestSector(sectorRange[0], sectorRange[1], id)
		}(connectionID, workSectors[connectionID])
	}

	// Wait for all workers to complete processing
	workerWG.Wait()

	// Cleanup operations (order: stop background tasks→flush data→report completion)
	harvester.cancelFunc()
	harvester.flushAllBuffers()

	events := atomic.LoadInt64(&harvester.totalEvents)
	debug.DropMessage("HARVEST", utils.Itoa(int(events))+" events complete")

	return nil
}

// harvestSector processes a continuous range of blocks for a specific connection.
// Local variables ordered by usage frequency: currentBlock used in loop, bufferOffset calculated once.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) harvestSector(fromBlock, toBlock uint64, connectionID int) {
	// Pre-calculate buffer partition once for this connection (used in every log processing)
	bufferOffset := connectionID * (len(processedLogs) / len(harvester.httpClients))
	currentBlock := fromBlock // Most frequently accessed variable in the loop

	for currentBlock <= toBlock {
		// Batch size management (access frequency order)
		batchSize := harvester.batchSizes[connectionID] // Read every iteration
		batchEnd := currentBlock + batchSize - 1        // Calculated every iteration
		if batchEnd > toBlock {
			batchEnd = toBlock
		}

		logCount, err := harvester.extractLogBatch(currentBlock, batchEnd, connectionID)
		if err != nil {
			// Handle "too many results" error by reducing batch size
			if strings.Contains(err.Error(), "more than 10000 results") {
				harvester.batchSizes[connectionID] = harvester.batchSizes[connectionID] >> 1
				if harvester.batchSizes[connectionID] < constants.MinBatchSize {
					harvester.batchSizes[connectionID] = constants.MinBatchSize
				}
				harvester.consecutiveSuccesses[connectionID] = 0
				continue
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Process retrieved logs using pre-calculated buffer offset
		for i := 0; i < logCount; i++ {
			logPosition := bufferOffset + i
			if logPosition >= len(processedLogs) {
				break
			}
			harvester.processLogFromGlobalBuffer(&processedLogs[logPosition], connectionID)
		}

		// Batch completion and adaptive sizing (access frequency order)
		currentBlock = batchEnd + 1                    // Most critical: advance to next batch
		harvester.consecutiveSuccesses[connectionID]++ // Track success for adaptation

		// Increase batch size after consecutive successes for adaptive optimization
		if harvester.consecutiveSuccesses[connectionID] >= 3 {
			harvester.batchSizes[connectionID] = harvester.batchSizes[connectionID] << 1
			harvester.consecutiveSuccesses[connectionID] = 0
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ROUTER STATE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// processLine parses a single CSV line and updates the router with optimized zero-copy operations.
// Parameters ordered by usage frequency: line used immediately, processed and blocks accessed frequently.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processLine(line []byte, processed *int, offset int64, blocks []uint64, addressBuf, dataBuf []byte, v *types.LogView) {
	// CSV delimiter discovery using linear scan (dependency order: c1→c2→c3)
	c1, c2, c3 := -1, -1, -1
	for i, b := range line {
		if b == ',' {
			if c1 == -1 {
				c1 = i // First comma: separates address from block
			} else if c2 == -1 {
				c2 = i // Second comma: separates block from reserve0
			} else if c3 == -1 {
				c3 = i // Third comma: separates reserve0 from reserve1
				break  // Early termination after finding all required delimiters
			}
		}
	}

	// Header detection and validation (fastest check first)
	if c1 == 7 {
		return // Skip header row to avoid processing metadata as data
	}

	// Validate CSV structure integrity with detailed error reporting
	if c1 == -1 || c2 == -1 || c3 == -1 {
		panic(fmt.Sprintf("malformed CSV line at offset %d (after %d events): expected 3 commas, found: comma1=%d, comma2=%d, comma3=%d, line: %q", offset, *processed, c1, c2, c3, string(line)))
	}

	// Pair existence verification using zero-copy address lookup (hot path check)
	pairID := router.LookupPairByAddress(line[0:c1])
	if pairID == 0 {
		return // Skip unknown pairs to avoid processing irrelevant data
	}

	// Block freshness validation for deduplication (dependency order: parse→compare→update)
	block := utils.ParseHexU64(line[c1+1 : c2])
	// Skip stale or duplicate block data to maintain state consistency
	if block <= blocks[pairID] {
		return // Ignore older blocks to preserve latest state
	}
	blocks[pairID] = block // Update block tracking for this pair

	// Address buffer preparation using SIMD-style operations (access pattern order)
	addrWords := (*[5]uint64)(unsafe.Pointer(&addressBuf[2]))
	addrWords[0] = utils.Load64(line[0:8])   // Characters 0-7
	addrWords[1] = utils.Load64(line[8:16])  // Characters 8-15
	addrWords[2] = utils.Load64(line[16:24]) // Characters 16-23
	addrWords[3] = utils.Load64(line[24:32]) // Characters 24-31
	addrWords[4] = utils.Load64(line[32:40]) // Characters 32-39

	// Reserve data buffer initialization with pattern fills (memory layout order)
	r0 := (*[4]uint64)(unsafe.Pointer(&dataBuf[34])) // Reserve0 data region
	r1 := (*[4]uint64)(unsafe.Pointer(&dataBuf[98])) // Reserve1 data region
	for i := 0; i < 4; i++ {
		r0[i] = 0x3030303030303030 // Eight ASCII '0' characters per 64-bit word
		r1[i] = 0x3030303030303030 // Ensures consistent zero-padding format
	}

	// Reserve value extraction from CSV fields (parsing order)
	res0 := line[c2+1 : c3] // Reserve0 field from CSV
	res1 := line[c3+1:]     // Reserve1 field from CSV (to end of line)

	// Right-aligned hex placement for standardized formatting (layout order)
	copy(dataBuf[66-len(res0):66], res0)   // Right-align reserve0 in its 32-char field
	copy(dataBuf[130-len(res1):130], res1) // Right-align reserve1 in its 32-char field

	// Router dispatch and progress tracking (operation order: dispatch first, then count)
	router.DispatchPriceUpdate(v) // Trigger price recalculation and arbitrage detection
	*processed++                  // Increment processed event counter for progress reporting
}

// flushHarvestedReservesToRouterFromFile performs backwards streaming CSV ingestion for router state initialization.
// Variables ordered by initialization dependency and usage frequency.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func flushHarvestedReservesToRouterFromFile(filePath string) error {
	// Database setup for pair count (dependency order: open→query→close)
	db, err := sql.Open("sqlite3", "uniswap_pairs.db")
	if err != nil {
		return err
	}
	defer db.Close()

	// Retrieve exact pair count for buffer sizing
	var totalPairs int
	if err := db.QueryRow("SELECT COUNT(*) FROM pools").Scan(&totalPairs); err != nil {
		return err
	}
	if totalPairs == 0 {
		return fmt.Errorf("no pairs found in database")
	}

	// File system initialization (dependency order: open→stat for size calculation)
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// LogView structure initialization for router communication (usage frequency order)
	var logView types.LogView

	// Address buffer preparation with Ethereum formatting (hot path setup)
	addressBuffer := make([]byte, 42)
	addressBuffer[0], addressBuffer[1] = '0', 'x' // Ethereum address prefix

	// Sync event data buffer allocation with Uniswap V2 format (hot path setup)
	dataBuffer := make([]byte, 130)
	dataBuffer[0], dataBuffer[1] = '0', 'x' // Event data prefix

	// Reserve data pre-initialization using 64-bit pattern fills (memory layout order)
	zeros1 := (*[4]uint64)(unsafe.Pointer(&dataBuffer[34])) // Reserve0 data region
	zeros2 := (*[4]uint64)(unsafe.Pointer(&dataBuffer[98])) // Reserve1 data region
	for i := 0; i < 4; i++ {
		zeros1[i] = 0x3030303030303030 // Eight ASCII '0' characters per 64-bit word
		zeros2[i] = 0x3030303030303030 // Ensures consistent zero-padding format
	}

	// LogView buffer binding for zero-copy router communication
	logView.Addr = addressBuffer // Bind address buffer for contract identification
	logView.Data = dataBuffer    // Bind data buffer for reserve value transmission

	// Processing state initialization (access frequency order)
	blockHeights := make([]uint64, totalPairs+1) // Array index corresponds to pair ID (accessed frequently)
	processedEvents := 0                         // Progress counter (incremented every record)

	// Backwards file reading infrastructure (buffer size calculations)
	const bufferSize = 8192
	readBuffer := make([]byte, bufferSize)       // Read buffer for file chunks
	workingBuffer := make([]byte, bufferSize<<1) // Combined buffer for chunk+leftover processing
	lineBuffer := make([]byte, bufferSize)       // Leftover line data storage
	lineBufferUsed := 0                          // Track leftover data length
	currentPosition := stat.Size()               // Start from end of file

	// Backwards streaming loop with chunk-based processing
	for currentPosition > 0 {
		// Calculate optimal read size for current chunk (dependency order)
		readSize := bufferSize
		if currentPosition < bufferSize {
			readSize = int(currentPosition) // Handle final chunk at beginning of file
		}
		currentPosition -= int64(readSize) // Update position for next iteration

		// Perform positioned file read for current chunk
		_, err := file.ReadAt(readBuffer[:readSize], currentPosition)
		if err != nil {
			return err
		}

		// Chunk combination with leftover line data (buffer management order)
		var processingData []byte
		if lineBufferUsed > 0 {
			// Combine buffers: current chunk + leftover from previous iteration
			copy(workingBuffer, readBuffer[:readSize])                  // Copy current chunk
			copy(workingBuffer[readSize:], lineBuffer[:lineBufferUsed]) // Append leftover data
			processingData = workingBuffer[:readSize+lineBufferUsed]    // Create combined slice
			lineBufferUsed = 0                                          // Reset line buffer for next iteration
		} else {
			// Direct processing: no leftover data from previous iteration
			processingData = readBuffer[:readSize] // Process current chunk directly
		}

		// Backwards line extraction with newline boundary detection
		endPosition := len(processingData) // Start scanning from end of data
		for scanIndex := len(processingData) - 1; scanIndex >= 0; scanIndex-- {
			if processingData[scanIndex] == '\n' {
				// Found newline: extract complete line for processing
				if scanIndex+1 < endPosition {
					lineData := processingData[scanIndex+1 : endPosition] // Extract line content
					if len(lineData) > 0 {
						// Process complete line with router integration
						fileOffset := currentPosition + int64(scanIndex+1)
						processLine(lineData, &processedEvents, fileOffset, blockHeights, addressBuffer, dataBuffer, &logView)
					}
				}
				endPosition = scanIndex // Update end position for next line extraction
			}
		}

		// Partial line preservation for next iteration
		if endPosition > 0 {
			copy(lineBuffer, processingData[:endPosition]) // Preserve partial line
			lineBufferUsed = endPosition                   // Track preserved data length
		}
	}

	// Final line processing for file beginning
	if lineBufferUsed > 0 {
		// Process final line with zero file offset (beginning of file)
		processLine(lineBuffer[:lineBufferUsed], &processedEvents, 0, blockHeights, addressBuffer, dataBuffer, &logView)
	}

	// Completion reporting and resource cleanup
	debug.DropMessage("HARVEST", utils.Itoa(processedEvents)+" reserve states loaded into router")
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// PUBLIC API OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// CheckHarvestingRequirement determines if harvesting is needed by comparing block heights.
// Variables ordered by usage: client used for request, endpoint used for connection, then response processing.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func CheckHarvestingRequirement() (bool, uint64, uint64, error) {
	// HTTP client setup (used immediately for request)
	client := &http.Client{Timeout: 10 * time.Second, Transport: buildHTTPTransport()}
	rpcEndpoint := "https://" + constants.HarvesterHost + constants.HarvesterPath

	// Request creation and execution (dependency order)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", rpcEndpoint, strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
	if err != nil {
		return false, 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	response, err := client.Do(req)
	if err != nil {
		return false, 0, 0, err
	}
	defer response.Body.Close()

	// Response processing (buffer size based on expected response)
	var buf [128]byte
	n, _ := response.Body.Read(buf[:])

	var blockResponse EthereumBlockResponse
	if err := sonnet.Unmarshal(buf[:n], &blockResponse); err != nil {
		return false, 0, 0, err
	}

	// Block comparison logic (dependency order: parse current→load last→compare)
	if len(blockResponse.Result) > 2 {
		currentHeight := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
		lastProcessed := LoadMetadata()
		return lastProcessed < currentHeight, lastProcessed, currentHeight, nil
	}

	return false, 0, 0, fmt.Errorf("invalid response format")
}

// CheckHarvestingRequirementFromBlock determines if harvesting is needed using provided last block.
// Variables ordered identically to CheckHarvestingRequirement for consistency.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func CheckHarvestingRequirementFromBlock(lastProcessedBlock uint64) (bool, uint64, uint64, error) {
	// HTTP client setup (used immediately for request)
	client := &http.Client{Timeout: 10 * time.Second, Transport: buildHTTPTransport()}
	rpcEndpoint := "https://" + constants.HarvesterHost + constants.HarvesterPath

	// Request creation and execution (dependency order)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", rpcEndpoint, strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
	if err != nil {
		return false, 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	response, err := client.Do(req)
	if err != nil {
		return false, 0, 0, err
	}
	defer response.Body.Close()

	// Response processing (buffer size based on expected response)
	var buf [128]byte
	n, _ := response.Body.Read(buf[:])

	var blockResponse EthereumBlockResponse
	if err := sonnet.Unmarshal(buf[:n], &blockResponse); err != nil {
		return false, 0, 0, err
	}

	// Block comparison using provided parameter instead of loading metadata
	if len(blockResponse.Result) > 2 {
		currentHeight := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
		return lastProcessedBlock < currentHeight, lastProcessedBlock, currentHeight, nil
	}

	return false, 0, 0, fmt.Errorf("invalid response format")
}

// ExecuteHarvesting starts the harvesting process with default connection settings.
// Simple wrapper for consistency with connection-specific version.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ExecuteHarvesting() error {
	return ExecuteHarvestingWithConnections(constants.DefaultConnections)
}

// ExecuteHarvestingWithConnections starts the harvesting process with specified connection count.
// Error handling order: execute first, then save metadata only on success.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func ExecuteHarvestingWithConnections(connectionCount int) error {
	harvester := newSynchronizationHarvester(connectionCount, constants.HarvesterOutputPath)
	defer harvester.cleanup()

	err := harvester.executeHarvesting()
	if err == nil {
		// Save metadata only after successful harvesting
		saveMetadata(harvester.syncTarget)
	}
	return err
}

// ExecuteHarvestingToTemp performs temp harvesting and returns the last processed block.
// Processing order: execute→cleanup→return target (does not update metadata file).
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func ExecuteHarvestingToTemp(connectionCount int) (uint64, error) {
	harvester := newSynchronizationHarvester(connectionCount, constants.HarvesterTempPath)
	defer harvester.cleanup()

	err := harvester.executeHarvesting()
	if err != nil {
		return 0, err
	}

	return harvester.syncTarget, nil
}

// FlushHarvestedReservesToRouter performs backwards streaming CSV ingestion for router state initialization.
// Simple wrapper for the main file with standard path.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func FlushHarvestedReservesToRouter() error {
	return flushHarvestedReservesToRouterFromFile(constants.HarvesterOutputPath)
}

// FlushHarvestedReservesToRouterFromTemp performs the same operation but reads from the temp file.
// Simple wrapper for the main function with temp path.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func FlushHarvestedReservesToRouterFromTemp() error {
	return flushHarvestedReservesToRouterFromFile(constants.HarvesterTempPath)
}
