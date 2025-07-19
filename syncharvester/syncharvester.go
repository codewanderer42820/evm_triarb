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
	batchSizes  []uint64   // Dynamic batch sizes per connection (24B) - moved here for hot access

	// Cache Line 2: Hot fields accessed frequently during processing
	csvBufferSizes       []int    // Current buffer sizes for CSV data (24B)
	consecutiveSuccesses []int    // Success counters for batch size adaptation (24B)
	_                    [16]byte // Padding to complete cache line alignment

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
// METADATA MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// loadMetadata reads the last processed block height from binary metadata file.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func loadMetadata() uint64 {
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
func newSynchronizationHarvester(connectionCount int) *SynchronizationHarvester {
	ctx, cancel := context.WithCancel(context.Background())
	lastProcessed := loadMetadata()

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

	harvester.outputFile, err = os.OpenFile(constants.HarvesterOutputPath, fileMode, 0644)
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
// CSV OUTPUT OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// writeCSVRecord constructs and buffers a CSV record for batch writing.
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

	saveMetadata(harvester.syncTarget)

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

// cleanup ensures all resources are properly released and data is flushed.
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
// PUBLIC API OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ExecuteHarvesting starts the harvesting process with default connection settings.
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
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func ExecuteHarvestingWithConnections(connectionCount int) error {
	harvester := newSynchronizationHarvester(connectionCount)
	defer harvester.cleanup()

	return harvester.executeHarvesting()
}

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
		lastProcessed := loadMetadata()
		return lastProcessed < currentHeight, lastProcessed, currentHeight, nil
	}

	return false, 0, 0, fmt.Errorf("invalid response format")
}

// FlushHarvestedReservesToRouter loads CSV data and populates the router's reserve state.
// Processes historical reserve data by streaming backwards through the CSV file to get the
// latest state for each pair without loading the entire 23GB file into memory.
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func FlushHarvestedReservesToRouter() error {
	// Open database connection to get total pair count
	db, err := sql.Open("sqlite3", "uniswap_pairs.db")
	if err != nil {
		return err
	}
	defer db.Close()

	// Get total number of pairs for allocation and termination logic
	var totalPairs int
	if err := db.QueryRow("SELECT COUNT(*) FROM pools").Scan(&totalPairs); err != nil {
		return err
	}
	if totalPairs == 0 {
		return fmt.Errorf("no pairs found")
	}

	// Open CSV file for backwards streaming
	file, err := os.Open(constants.HarvesterOutputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size for backwards reading position tracking
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// ═══════════════════════════════════════════════════════════════════════════════════════════
	// REUSABLE BUFFER SETUP - Zero Allocation Processing
	// ═══════════════════════════════════════════════════════════════════════════════════════════

	// Initialize single LogView structure that will be reused for all events
	var v types.LogView

	// Pre-allocated address buffer: "0x" + 40 hex characters = 42 bytes total
	// This buffer will hold contract addresses in the format expected by the router
	addressBuffer := make([]byte, 42)
	addressBuffer[0], addressBuffer[1] = '0', 'x' // Set permanent "0x" prefix

	// Pre-allocated Sync event data buffer: "0x" + 128 hex characters = 130 bytes total
	// Uniswap V2 Sync event layout: 0x + 32 zeros + 32 reserve0 + 32 zeros + 32 reserve1
	// Memory layout: [0x][00000...][reserve0][00000...][reserve1]
	//                 0-1  2-33     34-65    66-97     98-129
	syncDataBuffer := make([]byte, 130)
	syncDataBuffer[0], syncDataBuffer[1] = '0', 'x' // Set permanent "0x" prefix

	// Initialize the zero-filled sections of the Sync event data buffer
	// We pre-fill sections that should always be zeros to avoid clearing them repeatedly
	// Section 1: Bytes 2-33 (32 bytes of zeros before reserve0)
	zeroSection1 := (*[4]uint64)(unsafe.Pointer(&syncDataBuffer[2]))
	// Section 2: Bytes 66-97 (32 bytes of zeros before reserve1)
	zeroSection2 := (*[4]uint64)(unsafe.Pointer(&syncDataBuffer[66]))

	// Fill both zero sections with ASCII '0' characters (0x3030303030303030 = "00000000")
	for i := 0; i < 4; i++ {
		zeroSection1[i] = 0x3030303030303030 // 8 ASCII '0' characters
		zeroSection2[i] = 0x3030303030303030 // 8 ASCII '0' characters
	}

	// Connect the pre-allocated buffers to the LogView structure
	// These pointers will remain constant throughout processing
	v.Addr = addressBuffer  // Address field points to our reusable address buffer
	v.Data = syncDataBuffer // Data field points to our reusable Sync event buffer

	// ═══════════════════════════════════════════════════════════════════════════════════════════
	// PROCESSING STATE TRACKING
	// ═══════════════════════════════════════════════════════════════════════════════════════════

	// Track the latest block number seen for each pair to ensure we only keep the newest state
	// Index 0 is unused since pair IDs start from 1, matching the database schema
	latestBlockNumbers := make([]uint64, totalPairs+1)

	// Count of pairs successfully processed and dispatched to router
	eventsProcessed := 0

	// ═══════════════════════════════════════════════════════════════════════════════════════════
	// BACKWARDS FILE STREAMING - Constant Memory Usage
	// ═══════════════════════════════════════════════════════════════════════════════════════════

	// Use small buffer size for streaming to minimize memory usage
	// 8KB is optimal for most filesystems while keeping memory footprint low
	const chunkBufferSize = 8192
	chunkBuffer := make([]byte, chunkBufferSize)
	currentPosition := stat.Size() // Start reading from end of file

	// Stream through file backwards in chunks
	for currentPosition > 0 {
		// Calculate how much to read in this iteration
		readSize := chunkBufferSize
		if currentPosition < chunkBufferSize {
			readSize = int(currentPosition) // Handle final chunk smaller than buffer
		}
		currentPosition -= int64(readSize)

		// Read chunk from file at calculated position
		_, err := file.ReadAt(chunkBuffer[:readSize], currentPosition)
		if err != nil {
			return err
		}

		// ═══════════════════════════════════════════════════════════════════════════════════════
		// LINE PARSING - Process Lines Backwards Within Chunk
		// ═══════════════════════════════════════════════════════════════════════════════════════

		// Process lines backwards within this chunk
		lineEnd := readSize
		for i := readSize - 1; i >= 0; i-- {
			if chunkBuffer[i] == '\n' {
				// Found line boundary, process the line if it has content
				if i+1 < lineEnd {
					currentLine := chunkBuffer[i+1 : lineEnd]

					// ═══════════════════════════════════════════════════════════════════════════════
					// CSV FIELD EXTRACTION - Find Comma Positions
					// ═══════════════════════════════════════════════════════════════════════════════

					// Locate the three commas that separate our four CSV fields
					// Expected format: address,block,reserve0,reserve1
					comma1, comma2, comma3 := -1, -1, -1
					for j, b := range currentLine {
						if b == ',' {
							if comma1 == -1 {
								comma1 = j // First comma: separates address from block
							} else if comma2 == -1 {
								comma2 = j // Second comma: separates block from reserve0
							} else if comma3 == -1 {
								comma3 = j // Third comma: separates reserve0 from reserve1
								break      // Found all required commas, stop searching
							}
						}
					}

					// Validate CSV format - all three commas must be present
					// In a well-formed harvester CSV, this should never fail
					if comma1 == -1 || comma2 == -1 || comma3 == -1 {
						panic(fmt.Sprintf("malformed CSV line: expected 3 commas, found: comma1=%d, comma2=%d, comma3=%d, line: %q", comma1, comma2, comma3, string(currentLine)))
					}

					// ═══════════════════════════════════════════════════════════════════════════════
					// OPTIMIZATION: Process Fields in Order of Computational Cost
					// ═══════════════════════════════════════════════════════════════════════════════

					// 1. BLOCK NUMBER PARSING - Cheapest operation, eliminates most entries early
					// Parse hex block number to compare against latest seen for this pair
					blockNumber := utils.ParseHexU64(currentLine[comma1+1 : comma2])

					// 2. ADDRESS SETUP - Load address into buffer for pair lookup
					// Copy 40-character hex address into our reusable buffer (after "0x" prefix)
					// Use 64-bit word copying for maximum efficiency on 64-bit systems
					addressWords := (*[5]uint64)(unsafe.Pointer(&addressBuffer[2]))
					addressWords[0] = utils.Load64(currentLine[0:8])   // Characters 0-7
					addressWords[1] = utils.Load64(currentLine[8:16])  // Characters 8-15
					addressWords[2] = utils.Load64(currentLine[16:24]) // Characters 16-23
					addressWords[3] = utils.Load64(currentLine[24:32]) // Characters 24-31
					addressWords[4] = utils.Load64(currentLine[32:40]) // Characters 32-39

					// 3. PAIR EXISTENCE CHECK - Early exit if pair not found in router
					// Look up pair ID using the address we just loaded into the buffer
					pairID := router.LookupPairByAddress(currentLine[0:comma1])
					if pairID == 0 {
						continue // Pair not found in router, skip this line
					}

					// 4. BLOCK RECENCY CHECK - Skip if we already have newer or equal data
					// Since we're reading backwards, newer blocks appear first in the file
					if blockNumber <= latestBlockNumbers[pairID] {
						continue // Already processed newer state for this pair
					}
					latestBlockNumbers[pairID] = blockNumber // Update latest block for this pair

					// ═══════════════════════════════════════════════════════════════════════════════
					// SYNC EVENT DATA CONSTRUCTION - Build Router-Compatible Format
					// ═══════════════════════════════════════════════════════════════════════════════

					// 5. CLEAR RESERVE FIELDS - Reset reserve areas to zeros before writing new data
					// Reserve0 area: bytes 34-65 (32 characters for reserve0 value)
					reserve0Area := (*[4]uint64)(unsafe.Pointer(&syncDataBuffer[34]))
					// Reserve1 area: bytes 98-129 (32 characters for reserve1 value)
					reserve1Area := (*[4]uint64)(unsafe.Pointer(&syncDataBuffer[98]))

					// Fill both reserve areas with ASCII '0' characters
					for k := 0; k < 4; k++ {
						reserve0Area[k] = 0x3030303030303030 // 8 ASCII '0' characters
						reserve1Area[k] = 0x3030303030303030 // 8 ASCII '0' characters
					}

					// 6. PLACE RESERVE DATA - Right-align reserve values in their designated areas
					// Extract reserve values from CSV line
					reserve0Data := currentLine[comma2+1 : comma3] // Reserve0: between comma2 and comma3
					reserve1Data := currentLine[comma3+1:]         // Reserve1: after comma3 to end of line

					// Copy reserve data right-aligned into buffer (hex values are right-aligned)
					// Reserve0 goes into bytes 34-65, right-aligned from position 66
					copy(syncDataBuffer[66-len(reserve0Data):66], reserve0Data)
					// Reserve1 goes into bytes 98-129, right-aligned from position 130
					copy(syncDataBuffer[130-len(reserve1Data):130], reserve1Data)

					// ═══════════════════════════════════════════════════════════════════════════════
					// ROUTER DISPATCH - Send Processed Event to Arbitrage System
					// ═══════════════════════════════════════════════════════════════════════════════

					// 7. DISPATCH TO ROUTER - Send completed LogView to arbitrage detection system
					// The LogView now contains properly formatted address and Sync event data
					router.DispatchPriceUpdate(&v)
					eventsProcessed++
				}
				lineEnd = i // Move to next line boundary
			}
		}
	}

	// Report processing completion statistics
	debug.DropMessage("HARVEST", utils.Itoa(eventsProcessed)+" states loaded")
	return nil
}
