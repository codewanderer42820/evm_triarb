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
//
// ════════════════════════════════════════════════════════════════════════════════════════════════

package syncharvester

import (
	"context"
	"database/sql"
	"fmt"
	"math/bits"
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
// Cache-aligned for optimal memory access patterns during high-frequency processing.
//
//go:notinheap
//go:align 64
type EthereumLog struct {
	Address     string   // Contract address for the log event
	Data        string   // Hex-encoded event data payload
	BlockNumber string   // Block number in hex format
	_           [16]byte // Padding to complete cache line alignment
}

// EthereumLogsResponse encapsulates JSON-RPC log query responses.
// Optimized layout for efficient JSON unmarshaling and error handling.
//
//go:notinheap
//go:align 64
type EthereumLogsResponse struct {
	Result []EthereumLog // Array of log entries matching query criteria
	Error  *struct {
		Message string // API error message if request failed
	} // Error information from RPC provider
	_ [32]byte // Padding to complete cache line alignment
}

// EthereumBlockResponse handles block number queries from JSON-RPC.
// Used for determining current blockchain state and synchronization targets.
//
//go:notinheap
//go:align 64
type EthereumBlockResponse struct {
	Result string   // Current block number in hex format
	_      [48]byte // Padding to complete cache line alignment
}

// ProcessedReserveEntry represents a single reserve state after parsing.
// Optimized for high-throughput processing of Uniswap V2 Sync events.
//
//go:notinheap
//go:align 64
type ProcessedReserveEntry struct {
	contractAddress string   // Uniswap V2 pair contract address (without 0x prefix)
	eventData       string   // Raw hex data from Sync event (128 hex characters)
	blockHeight     string   // Block number in hex format (without 0x prefix)
	_               [16]byte // Padding to complete cache line alignment
}

// SynchronizationHarvester orchestrates historical data extraction.
// Memory layout optimized based on access frequency for maximum cache efficiency.
//
//go:notinheap
//go:align 64
type SynchronizationHarvester struct {
	// Cache Line 1: Hottest fields accessed during every operation
	totalEvents int64      // Atomic counter for processed events (8B)
	rpcEndpoint string     // RPC endpoint URL for HTTP requests (16B)
	outputFile  *os.File   // CSV output file handle (8B)
	fileMutex   sync.Mutex // Protects concurrent file writes (8B)
	batchSizes  []uint64   // Dynamic batch sizes per connection (24B)

	// Cache Line 2: Hot fields accessed frequently during processing
	csvBufferSizes       []int  // Current buffer sizes for CSV data (24B)
	consecutiveSuccesses []int  // Success counters for batch size adaptation (24B)
	outputPath           string // Output file path for this harvester instance (16B)

	// Cache Line 3: Warm fields accessed during batch completion
	currentBlocks []uint64       // Current block heights per connection
	httpClients   []*http.Client // HTTP clients for parallel requests
	_             [16]byte       // Padding to complete cache line alignment

	// Cache Line 4: Cold fields accessed infrequently
	syncTarget        uint64             // Target block height for synchronization
	lastProcessed     uint64             // Last successfully processed block
	startTime         time.Time          // Processing start timestamp
	processingContext context.Context    // Context for cancellation handling
	cancelFunc        context.CancelFunc // Function to cancel processing context
	_                 [8]byte            // Padding to complete cache line alignment
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// GLOBAL PROCESSING BUFFERS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// Global buffers eliminate allocation overhead during processing operations.
// Dynamically sized based on connection count for optimal memory usage patterns.
// These buffers are shared across all worker goroutines for maximum efficiency.
//
//go:notinheap
//go:align 64
var (
	// Cache Line 1: Most frequently accessed buffers during request processing
	responseBuffers [][]byte                // HTTP response buffers for parallel request handling
	processedLogs   []ProcessedReserveEntry // Log processing buffer for parsed events
	_               [16]byte                // Padding to complete cache line alignment

	// Cache Line 2: Less frequently accessed buffers for output formatting
	csvOutputBuffers  [][]byte          // CSV output buffers for batched writes
	csvStringBuilders []strings.Builder // String builders for zero-allocation CSV construction
	_                 [16]byte          // Padding to complete cache line alignment
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
// Configuration tuned for high-frequency blockchain data requests with connection pooling.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func buildHTTPTransport() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,  // Connection establishment timeout
			KeepAlive: 60 * time.Second, // Connection keep-alive duration
			DualStack: true,             // Enable IPv4/IPv6 fallback support
		}).DialContext,
		MaxIdleConns:          800,                       // Global connection pool size
		MaxIdleConnsPerHost:   150,                       // Per-host connection limit
		MaxConnsPerHost:       300,                       // Maximum concurrent connections per host
		IdleConnTimeout:       120 * time.Second,         // Idle connection timeout
		TLSHandshakeTimeout:   4 * time.Second,           // TLS negotiation timeout
		ResponseHeaderTimeout: 12 * time.Second,          // Response header timeout
		ExpectContinueTimeout: 500 * time.Millisecond,    // Continue expectation timeout
		DisableCompression:    true,                      // Disable compression for speed
		DisableKeepAlives:     false,                     // Enable connection reuse
		ForceAttemptHTTP2:     true,                      // Use HTTP/2 when available
		WriteBufferSize:       128 * 1024,                // Write buffer size
		ReadBufferSize:        128 * 1024,                // Read buffer size
		Proxy:                 http.ProxyFromEnvironment, // Environment proxy settings
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// SIMD-OPTIMIZED HEX PROCESSING
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// countHexLeadingZeros performs leading zero counting using SIMD-style operations.
// Optimized for processing 32-character hex segments from Ethereum event data.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func countHexLeadingZeros(hexSegment []byte) int {
	// 64-bit pattern representing eight consecutive ASCII '0' characters
	const ZERO_PATTERN = 0x3030303030303030

	// Process four 8-byte chunks simultaneously using SIMD-style operations
	chunk0 := utils.Load64(hexSegment[0:8]) ^ ZERO_PATTERN   // XOR reveals non-zero bytes
	chunk1 := utils.Load64(hexSegment[8:16]) ^ ZERO_PATTERN  // Process second chunk
	chunk2 := utils.Load64(hexSegment[16:24]) ^ ZERO_PATTERN // Process third chunk
	chunk3 := utils.Load64(hexSegment[24:32]) ^ ZERO_PATTERN // Process fourth chunk

	// Create bitmask indicating which chunks contain non-zero characters
	chunkMask := ((chunk0|(^chunk0+1))>>63)<<0 | ((chunk1|(^chunk1+1))>>63)<<1 |
		((chunk2|(^chunk2+1))>>63)<<2 | ((chunk3|(^chunk3+1))>>63)<<3

	// Find first chunk containing non-zero character using bit operations
	firstNonZeroChunk := bits.TrailingZeros64(chunkMask)
	if firstNonZeroChunk == 64 {
		return 32 // All 32 characters are zeros
	}

	// Within the first non-zero chunk, locate first non-zero byte
	chunks := [4]uint64{chunk0, chunk1, chunk2, chunk3}
	firstNonZeroByte := bits.TrailingZeros64(chunks[firstNonZeroChunk]) >> 3

	// Calculate total leading zero count across all processed chunks
	return (firstNonZeroChunk << 3) + firstNonZeroByte
}

// parseReservesToZeroTrimmed extracts and trims reserve values from Sync event data.
// Processes 128-character hex strings representing reserve0 and reserve1 values.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func parseReservesToZeroTrimmed(eventData string) (string, string) {
	// Convert string to byte slice for SIMD processing efficiency
	dataBytes := unsafe.Slice(unsafe.StringData(eventData), len(eventData))

	// Parse reserve0 from bytes 32-64 (32 hex characters representing first reserve)
	leadingZeros0 := countHexLeadingZeros(dataBytes[32:64])
	reserve0Start := 32 + leadingZeros0
	var reserve0 string
	if reserve0Start >= 64 {
		reserve0 = "0" // Handle all-zeros case efficiently
	} else {
		reserve0 = eventData[reserve0Start:64]
	}

	// Parse reserve1 from bytes 96-128 (32 hex characters representing second reserve)
	leadingZeros1 := countHexLeadingZeros(dataBytes[96:128])
	reserve1Start := 96 + leadingZeros1
	var reserve1 string
	if reserve1Start >= 128 {
		reserve1 = "0" // Handle all-zeros case efficiently
	} else {
		reserve1 = eventData[reserve1Start:128]
	}

	return reserve0, reserve1
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HARVESTER INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// newSynchronizationHarvester creates a fully initialized harvester with optimized buffers and transport.
// Implements intelligent resource allocation based on connection count for optimal performance.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func newSynchronizationHarvester(connectionCount int, outputPath string) *SynchronizationHarvester {
	ctx, cancel := context.WithCancel(context.Background())
	lastProcessed := LoadMetadata()

	harvester := &SynchronizationHarvester{
		// Cache Line 1: Hottest fields accessed during every operation
		totalEvents: 0,
		rpcEndpoint: "https://" + constants.HarvesterHost + constants.HarvesterPath,
		outputFile:  nil, // Will be set below
		fileMutex:   sync.Mutex{},
		batchSizes:  make([]uint64, connectionCount),

		// Cache Line 2: Hot fields accessed frequently during processing
		csvBufferSizes:       make([]int, connectionCount),
		consecutiveSuccesses: make([]int, connectionCount),
		outputPath:           outputPath,

		// Cache Line 3: Warm fields accessed during batch completion
		currentBlocks: make([]uint64, connectionCount),
		httpClients:   make([]*http.Client, connectionCount),

		// Cache Line 4: Cold fields accessed infrequently
		syncTarget:        0, // Will be set in executeHarvesting()
		lastProcessed:     lastProcessed,
		startTime:         time.Now(),
		processingContext: ctx,
		cancelFunc:        cancel,
	}

	// Initialize batch sizes to optimal default values
	for i := range harvester.batchSizes {
		harvester.batchSizes[i] = constants.OptimalBatchSize
	}

	// Initialize global buffers in the order they are declared
	responseBuffers = make([][]byte, connectionCount)
	processedLogs = make([]ProcessedReserveEntry, constants.MaxLogSliceSize)
	csvOutputBuffers = make([][]byte, connectionCount)
	csvStringBuilders = make([]strings.Builder, connectionCount)

	// Create shared HTTP transport for connection pooling efficiency
	sharedTransport := buildHTTPTransport()
	for i := 0; i < connectionCount; i++ {
		responseBuffers[i] = make([]byte, constants.ResponseBufferSize)
		csvOutputBuffers[i] = make([]byte, constants.CSVBufferSize)
		csvStringBuilders[i].Grow(constants.CSVBufferSize)
		harvester.httpClients[i] = &http.Client{Timeout: 30 * time.Second, Transport: sharedTransport}
	}

	// Initialize output file with appropriate mode based on resume state
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

	// Write CSV header for new files only
	if lastProcessed == constants.HarvesterDeploymentBlock {
		harvester.outputFile.WriteString("address,block,reserve0,reserve1\n")
	}

	// Start background reporting goroutine
	go harvester.reportStatistics()
	return harvester
}

// reportStatistics provides periodic progress updates during harvesting operation.
// Displays event count and block progress for each connection sector.
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
			events := atomic.LoadInt64(&harvester.totalEvents)
			debug.DropMessage("HARVEST", utils.Itoa(int(events))+" events processed")

			// Report progress for each sector with racey access
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
// Called during harvester shutdown to prevent data loss.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) cleanup() {
	harvester.flushAllBuffers()

	if harvester.outputFile != nil {
		harvester.outputFile.Close()
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// ETHEREUM JSON-RPC OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// getCurrentBlockNumber queries the latest block number from the Ethereum node.
// Implements retry logic with exponential backoff for reliable network operations.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) getCurrentBlockNumber() uint64 {
	requestJSON := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Create request with timeout context
		ctx, cancel := context.WithTimeout(harvester.processingContext, 8*time.Second)
		req, err := http.NewRequestWithContext(ctx, "POST", harvester.rpcEndpoint, strings.NewReader(requestJSON))
		if err != nil {
			cancel()
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		response, err := harvester.httpClients[0].Do(req)
		if err != nil {
			cancel()
			time.Sleep(25 * time.Millisecond)
			continue
		}

		// Read response with size limit for safety
		bytesRead, _ := response.Body.Read(responseBuffers[0][:512])
		response.Body.Close()
		cancel() // Safe to cancel after response processing complete

		if bytesRead == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Parse JSON response to extract block number
		var blockResponse EthereumBlockResponse
		err = sonnet.Unmarshal(responseBuffers[0][:bytesRead], &blockResponse)
		if err != nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		// Parse hex block number with minimal validation
		if len(blockResponse.Result) > 2 {
			blockNumber := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
			return blockNumber // Block 0 is valid (genesis block)
		}

		time.Sleep(5 * time.Millisecond)
	}

	panic("Failed to get current block number after retries")
}

// extractLogBatch retrieves and processes logs for a specific block range.
// Uses zero-allocation JSON construction and connection-specific HTTP clients.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) extractLogBatch(fromBlock, toBlock uint64, connectionID int) (int, error) {
	// Use pre-allocated string builder for zero-allocation JSON construction
	builder := &csvStringBuilders[connectionID]
	builder.Reset()

	// Convert block numbers to hex format for JSON-RPC request
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

	// Create request with timeout context
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

	// Read response data with size limits for safety
	totalBytes := 0
	responseBuffer := responseBuffers[connectionID]
	maxReadSize := len(responseBuffer) - 1024 // Reserve space for safety

	for totalBytes < maxReadSize {
		readSize := constants.ReadBufferSize
		if totalBytes+readSize > maxReadSize {
			readSize = maxReadSize - totalBytes
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
// Implements buffer partitioning to prevent memory conflicts between connections.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) parseLogsWithSonnet(jsonData []byte, connectionID int) (int, error) {
	// Calculate buffer partitioning for this connection to prevent conflicts
	connectionCount := len(harvester.httpClients)
	bufferOffset := connectionID * (len(processedLogs) / connectionCount)
	logCount := 0
	maxLogsPerConnection := len(processedLogs) / connectionCount

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

		// Store parsed log data in partitioned global buffer
		logEntry := &processedLogs[bufferPosition]
		logEntry.contractAddress = ethereumLog.Address[2:] // Remove 0x prefix for efficiency
		logEntry.eventData = ethereumLog.Data[2:]          // Remove 0x prefix for processing
		logEntry.blockHeight = ethereumLog.BlockNumber[2:] // Store hex without 0x prefix

		logCount++
	}

	return logCount, nil
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// CSV OUTPUT OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// writeCSVRecord constructs and buffers a CSV record for batch writing.
// Automatically flushes buffer when approaching capacity limits.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) writeCSVRecord(address string, blockHeight string,
	reserve0, reserve1 string, connectionID int) {

	builder := &csvStringBuilders[connectionID]
	builder.Reset()

	builder.WriteString(address)
	builder.WriteByte(',')
	builder.WriteString(blockHeight)
	builder.WriteByte(',')
	builder.WriteString(reserve0)
	builder.WriteByte(',')
	builder.WriteString(reserve1)
	builder.WriteByte('\n')

	csvRecord := builder.String()

	currentBufferSize := harvester.csvBufferSizes[connectionID]
	newBufferSize := currentBufferSize + len(csvRecord)

	if newBufferSize >= (constants.CSVBufferSize * 9 / 10) {
		harvester.flushCSVBuffer(connectionID)
		currentBufferSize = 0
		newBufferSize = len(csvRecord)
	}

	copy(csvOutputBuffers[connectionID][currentBufferSize:], csvRecord)
	harvester.csvBufferSizes[connectionID] = newBufferSize
	atomic.AddInt64(&harvester.totalEvents, 1)
}

// flushCSVBuffer writes accumulated CSV data to disk with mutex protection.
// Thread-safe operation for concurrent buffer flushing.
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
// Flushes all connection buffers and forces filesystem synchronization.
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
// Optimized for high-frequency processing with minimal overhead per log entry.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) processLogFromGlobalBuffer(logEntry *ProcessedReserveEntry, connectionID int) {
	reserve0, reserve1 := parseReservesToZeroTrimmed(logEntry.eventData)
	harvester.writeCSVRecord(logEntry.contractAddress, logEntry.blockHeight, reserve0, reserve1, connectionID)

	blockNumber := utils.ParseHexU64([]byte(logEntry.blockHeight))
	if blockNumber > harvester.currentBlocks[connectionID] {
		harvester.currentBlocks[connectionID] = blockNumber
	}
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════
// HARVESTING COORDINATION ENGINE
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// executeHarvesting orchestrates the complete harvesting process with intelligent work distribution.
// Implements dynamic sector allocation and parallel processing for maximum throughput.
// Metadata saving is the responsibility of the caller.
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

	totalBlocks := harvester.syncTarget - harvester.lastProcessed
	connectionCount := len(harvester.httpClients)

	debug.DropMessage("HARVEST", fmt.Sprintf("Resuming from block %d, %d blocks remaining, %d connections",
		harvester.lastProcessed, totalBlocks, connectionCount))

	// Calculate work distribution across connections with load balancing
	workSectors := make([][2]uint64, connectionCount)
	blocksPerSector := totalBlocks / uint64(connectionCount)
	extraBlocks := totalBlocks % uint64(connectionCount)
	sectorStart := harvester.lastProcessed + 1

	for i := 0; i < connectionCount; i++ {
		fromBlock := sectorStart
		sectorSize := blocksPerSector
		if uint64(i) < extraBlocks {
			sectorSize++ // Distribute remainder blocks to first connections
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

	// Stop the reporting goroutine
	harvester.cancelFunc()

	// Ensure all buffered data is written to disk
	harvester.flushAllBuffers()

	events := atomic.LoadInt64(&harvester.totalEvents)
	debug.DropMessage("HARVEST", utils.Itoa(int(events))+" events complete")

	return nil
}

// harvestSector processes a continuous range of blocks for a specific connection.
// Implements adaptive batch sizing and intelligent error handling for optimal performance.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func (harvester *SynchronizationHarvester) harvestSector(fromBlock, toBlock uint64, connectionID int) {
	// Calculate buffer partition once for this connection
	bufferOffset := connectionID * (len(processedLogs) / len(harvester.httpClients))
	currentBlock := fromBlock

	for currentBlock <= toBlock {
		batchSize := harvester.batchSizes[connectionID]
		batchEnd := currentBlock + batchSize - 1
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

		currentBlock = batchEnd + 1
		harvester.consecutiveSuccesses[connectionID]++

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
// Parameters are ordered by usage frequency for optimal register allocation and cache efficiency.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func processLine(line []byte, processed *int, offset int64, blocks []uint64, addressBuf, dataBuf []byte, v *types.LogView) {
	// CSV delimiter discovery using linear scan
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

	// Header detection and validation
	if c1 == 7 {
		return // Skip header row to avoid processing metadata as data
	}

	// Validate CSV structure integrity with detailed error reporting
	if c1 == -1 || c2 == -1 || c3 == -1 {
		panic(fmt.Sprintf("malformed CSV line at offset %d (after %d events): expected 3 commas, found: comma1=%d, comma2=%d, comma3=%d, line: %q", offset, *processed, c1, c2, c3, string(line)))
	}

	// Pair existence verification using zero-copy address lookup
	pairID := router.LookupPairByAddress(line[0:c1])
	if pairID == 0 {
		return // Skip unknown pairs to avoid processing irrelevant data
	}

	// Block freshness validation for deduplication
	block := utils.ParseHexU64(line[c1+1 : c2])

	// Skip stale or duplicate block data to maintain state consistency
	if block <= blocks[pairID] {
		return // Ignore older blocks to preserve latest state
	}
	blocks[pairID] = block // Update block tracking for this pair

	// Address buffer preparation using SIMD-style operations
	addrWords := (*[5]uint64)(unsafe.Pointer(&addressBuf[2]))
	addrWords[0] = utils.Load64(line[0:8])   // Characters 0-7
	addrWords[1] = utils.Load64(line[8:16])  // Characters 8-15
	addrWords[2] = utils.Load64(line[16:24]) // Characters 16-23
	addrWords[3] = utils.Load64(line[24:32]) // Characters 24-31
	addrWords[4] = utils.Load64(line[32:40]) // Characters 32-39

	// Reserve data buffer initialization with pattern fills
	r0 := (*[4]uint64)(unsafe.Pointer(&dataBuf[34])) // Reserve0 data region
	r1 := (*[4]uint64)(unsafe.Pointer(&dataBuf[98])) // Reserve1 data region
	for i := 0; i < 4; i++ {
		r0[i] = 0x3030303030303030 // Eight ASCII '0' characters per 64-bit word
		r1[i] = 0x3030303030303030 // Ensures consistent zero-padding format
	}

	// Reserve value extraction from CSV fields
	res0 := line[c2+1 : c3] // Reserve0 field from CSV
	res1 := line[c3+1:]     // Reserve1 field from CSV (to end of line)

	// Right-aligned hex placement for standardized formatting
	copy(dataBuf[66-len(res0):66], res0)   // Right-align reserve0 in its 32-char field
	copy(dataBuf[130-len(res1):130], res1) // Right-align reserve1 in its 32-char field

	// Router dispatch and progress tracking
	router.DispatchPriceUpdate(v) // Trigger price recalculation and arbitrage detection
	*processed++                  // Increment processed event counter for progress reporting
}

// flushHarvestedReservesToRouterFromFile performs backwards streaming CSV ingestion for router state initialization.
// Processes historical reserve data in reverse chronological order to ensure latest state consistency.
//
//go:norace
//go:nocheckptr
//go:inline
//go:registerparams
func flushHarvestedReservesToRouterFromFile(filePath string) error {
	// Database capacity planning and resource allocation
	db, err := sql.Open("sqlite3", "uniswap_pairs.db")
	if err != nil {
		return err
	}
	defer db.Close()

	// Retrieve exact pair count for buffer sizing and processing termination
	var totalPairs int
	if err := db.QueryRow("SELECT COUNT(*) FROM pools").Scan(&totalPairs); err != nil {
		return err
	}
	if totalPairs == 0 {
		return fmt.Errorf("no pairs found in database")
	}

	// File system initialization and metadata extraction
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Extract file size for backwards reading position calculations
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// LogView structure initialization for router communication
	var logView types.LogView

	// Address buffer preparation with Ethereum formatting
	addressBuffer := make([]byte, 42)
	addressBuffer[0], addressBuffer[1] = '0', 'x' // Ethereum address prefix

	// Sync event data buffer allocation with Uniswap V2 format
	dataBuffer := make([]byte, 130)
	dataBuffer[0], dataBuffer[1] = '0', 'x' // Event data prefix

	// Reserve data pre-initialization using 64-bit pattern fills
	zeros1 := (*[4]uint64)(unsafe.Pointer(&dataBuffer[34])) // Reserve0 data region
	zeros2 := (*[4]uint64)(unsafe.Pointer(&dataBuffer[98])) // Reserve1 data region
	for i := 0; i < 4; i++ {
		zeros1[i] = 0x3030303030303030 // Eight ASCII '0' characters per 64-bit word
		zeros2[i] = 0x3030303030303030 // Ensures consistent zero-padding format
	}

	// LogView buffer binding for zero-copy router communication
	logView.Addr = addressBuffer // Bind address buffer for contract identification
	logView.Data = dataBuffer    // Bind data buffer for reserve value transmission

	// Deduplication infrastructure with block height tracking
	blockHeights := make([]uint64, totalPairs+1) // Array index corresponds to pair ID
	processedEvents := 0                         // Progress counter for logging and debugging

	// Backwards file reading infrastructure with zero-allocation management
	const bufferSize = 8192                     // Primary read buffer size for I/O optimization
	readBuffer := make([]byte, bufferSize)      // Primary file reading buffer
	workingBuffer := make([]byte, bufferSize*2) // Pre-allocated for chunk combination
	lineBuffer := make([]byte, bufferSize)      // Pre-allocated for partial line storage
	lineBufferUsed := 0                         // Track occupied bytes in line buffer
	currentPosition := stat.Size()              // Start from end of file for reverse traversal

	// Backwards streaming loop with chunk-based processing
	for currentPosition > 0 {
		// Calculate optimal read size for current chunk
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

		// Chunk combination with leftover line data
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
// Provides a quick check to avoid unnecessary harvesting operations.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func CheckHarvestingRequirement() (bool, uint64, uint64, error) {
	client := &http.Client{Timeout: 10 * time.Second, Transport: buildHTTPTransport()}
	rpcEndpoint := "https://" + constants.HarvesterHost + constants.HarvesterPath

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

	var buf [128]byte
	n, _ := response.Body.Read(buf[:])

	var blockResponse EthereumBlockResponse
	if err := sonnet.Unmarshal(buf[:n], &blockResponse); err != nil {
		return false, 0, 0, err
	}

	if len(blockResponse.Result) > 2 {
		currentHeight := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
		lastProcessed := LoadMetadata()
		return lastProcessed < currentHeight, lastProcessed, currentHeight, nil
	}

	return false, 0, 0, fmt.Errorf("invalid response format")
}

// CheckHarvestingRequirementFromBlock determines if harvesting is needed using provided last block.
// Allows temp harvesting to track its own progress independently.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func CheckHarvestingRequirementFromBlock(lastProcessedBlock uint64) (bool, uint64, uint64, error) {
	client := &http.Client{Timeout: 10 * time.Second, Transport: buildHTTPTransport()}
	rpcEndpoint := "https://" + constants.HarvesterHost + constants.HarvesterPath

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

	var buf [128]byte
	n, _ := response.Body.Read(buf[:])

	var blockResponse EthereumBlockResponse
	if err := sonnet.Unmarshal(buf[:n], &blockResponse); err != nil {
		return false, 0, 0, err
	}

	if len(blockResponse.Result) > 2 {
		currentHeight := utils.ParseHexU64([]byte(blockResponse.Result[2:]))
		return lastProcessedBlock < currentHeight, lastProcessedBlock, currentHeight, nil
	}

	return false, 0, 0, fmt.Errorf("invalid response format")
}

// ExecuteHarvesting starts the harvesting process with default connection settings.
// Uses standard configuration for routine synchronization operations.
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
// Updates metadata file upon successful completion.
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
// Does not update metadata file to maintain isolation.
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
// Processes historical reserve data in reverse chronological order to ensure latest state consistency.
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
// Allows testing and validation without affecting production router state.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func FlushHarvestedReservesToRouterFromTemp() error {
	return flushHarvestedReservesToRouterFromFile(constants.HarvesterTempPath)
}
